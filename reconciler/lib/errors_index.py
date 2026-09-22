import json
import logging
import boto3

logger = logging.getLogger()
_s3 = boto3.client("s3", region_name="eu-west-1")

# Codici per cui ha senso riprovare: nessuno status (timeout o errore di rete),
# 502, 503, 504. Un 500 non è incluso: il consumer lo tratta già come
# recuperabile e SQS ha quindi esaurito i tre tentativi senza successo, quindi
# ritentarlo qui significherebbe quasi sempre rifare la stessa chiamata a vuoto.
RETRYABLE_STATUS_CODES = frozenset({None, 502, 503, 504})


def load_error_index(bucket: str, run_id: str, has_time) -> tuple[dict, bool]:
    """
    Costruisce decoder_id_original -> ultimo errore registrato, leggendo
    audit/{run_id}/errors/.

    Restituisce (indice, completo). `completo` è False se il tempo è finito
    prima di leggere tutti i file: in quel caso i decoder non trovati vengono
    mandati in revisione manuale anziché ritentati alla cieca.
    """
    prefix = f"audit/{run_id}/errors/"
    index: dict[str, dict] = {}
    files_read = 0

    paginator = _s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if not has_time():
                logger.warning(
                    "Tempo esaurito durante la lettura degli errori: %d file letti, indice parziale",
                    files_read,
                )
                return index, False

            try:
                body = _s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read()
            except Exception as e:
                logger.error("Lettura fallita per %s: %s", obj["Key"], e)
                continue

            files_read += 1
            for line in body.decode().splitlines():
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue

                decoder = record.get("decoder_id_original")
                if not decoder:
                    continue

                previous = index.get(decoder)
                if previous is None or record.get("ts", "") >= previous.get("ts", ""):
                    index[decoder] = record

    logger.info("Indice errori: %d decoder distinti da %d file", len(index), files_read)
    return index, True


def is_retryable(error_record: dict | None) -> bool:
    """Un decoder senza errore registrato va in revisione manuale, non ritentato."""
    if error_record is None:
        return False
    return error_record.get("status_code") in RETRYABLE_STATUS_CODES
