import json
import uuid
import logging
import boto3

logger = logging.getLogger()
_s3 = boto3.client("s3", region_name="eu-west-1")


def write_outcomes(bucket: str, run_id: str, outcomes: list[dict]) -> None:
    """
    Write batch outcomes to S3 as JSONL.
    One file per consumer invocation (avoids concurrent-write conflicts).

    Each outcome record:
    {
        "decoder_id_original": str,
        "hasoptedoutrecommendation__c": str,
        "status": "ok" | "error" | "discarded",
        "status_code": int | None,
        "error": str | None,
        "run_id": str,
        "ts": str (ISO-8601)
    }
    """
    if not outcomes:
        return
    _put_jsonl(bucket, f"audit/{run_id}/outcomes/{uuid.uuid4()}.jsonl", outcomes)


def write_errors(bucket: str, run_id: str, outcomes: list[dict]) -> None:
    """
    Flusso compatto dei soli errori, in audit/{run_id}/errors/.

    Duplica un sottoinsieme di write_outcomes di proposito: il reconciler deve
    poter risalire alla causa di ogni fallimento senza scorrere un file di
    outcome per ogni invocazione del consumer (su 1.8M record sarebbero 180.000
    oggetti, illeggibili nel tempo di una Lambda).
    """
    errors = [o for o in outcomes if o.get("status") == "error"]
    if not errors:
        return
    _put_jsonl(bucket, f"audit/{run_id}/errors/{uuid.uuid4()}.jsonl", errors)


def _put_jsonl(bucket: str, key: str, records: list[dict]) -> None:
    body = "\n".join(json.dumps(r, default=str) for r in records) + "\n"
    try:
        _s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=body.encode(),
            ContentType="application/x-ndjson",
        )
    except Exception as e:
        # Audit write failure must not block message processing
        logger.error("Audit S3 write failed (key=%s): %s", key, e)
