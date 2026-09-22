"""
Lambda di riconciliazione — da invocare manualmente a campagna conclusa.

Lavora sulla DLQ, non sui file di outcome: la DLQ contiene per costruzione
esattamente i messaggi falliti in via definitiva, uno per decoder, senza i
tentativi intermedi di messaggi poi andati a buon fine. Gli outcome su S3
restano come traccia completa della campagna, da interrogare con Athena.

Event:
{
    "runId":  "riallineamento-2026-09",   # per risalire alla causa di ogni errore
    "dryRun": false                       # true = analizza e riporta, non tocca le code
}

Per ogni messaggio in DLQ:
  - causa recuperabile (timeout, 502, 503, 504) -> ripubblicato sulla coda principale
  - tutto il resto                              -> report per revisione manuale

Report scritti in reconcile/{reconcileRunId}/ su S3.
"""

import json
import os
import uuid
import logging
from datetime import datetime, timezone

import boto3

from lib.dlq import receive_batch, delete_batch, approximate_depth
from lib.errors_index import load_error_index, is_retryable
from lib.sqs import publish_batch

logger = logging.getLogger()
logger.setLevel(logging.INFO)

AUDIT_BUCKET = os.environ["AUDIT_BUCKET"]
SQS_QUEUE_URL = os.environ["SQS_QUEUE_URL"]
DLQ_URL = os.environ["DLQ_URL"]

_s3 = boto3.client("s3", region_name="eu-west-1")

SAFETY_MARGIN_MS = 30_000
DLQ_VISIBILITY_TIMEOUT = 300


def handler(event, context):
    event = event or {}
    run_id = event.get("runId")
    dry_run = bool(event.get("dryRun", False))

    if not run_id:
        raise ValueError("L'evento deve contenere 'runId'")

    def has_time():
        return context.get_remaining_time_in_millis() > SAFETY_MARGIN_MS

    reconcile_run_id = (
        f"reconcile-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
        f"-{uuid.uuid4().hex[:6]}"
    )
    logger.info(
        "Avvio riconciliazione. runId=%s reconcileRunId=%s dryRun=%s",
        run_id, reconcile_run_id, dry_run,
    )

    error_index, index_complete = load_error_index(AUDIT_BUCKET, run_id, has_time)

    retryable: list[dict] = []
    manual_review: list[dict] = []
    drained = 0

    while has_time():
        messages = receive_batch(DLQ_URL, DLQ_VISIBILITY_TIMEOUT)
        if not messages:
            break

        batch_retryable: list[dict] = []
        batch_manual: list[dict] = []
        for message in messages:
            record = _classify(message, error_index)
            if record["retryable"]:
                batch_retryable.append(record)
            else:
                batch_manual.append(record)

        if not dry_run:
            # Ripubblicare prima di cancellare: l'ordine inverso perderebbe i
            # messaggi se la pubblicazione fallisse. Al più si duplicano, e le
            # chiamate opt-in/opt-out sono idempotenti.
            _republish(batch_retryable, reconcile_run_id)
            delete_batch(DLQ_URL, messages)

        retryable.extend(batch_retryable)
        manual_review.extend(batch_manual)
        drained += len(messages)

    dlq_svuotata = approximate_depth(DLQ_URL) == 0 if not dry_run else None

    summary = {
        "reconcileRunId": reconcile_run_id,
        "sourceRunId": run_id,
        "dryRun": dry_run,
        "lettiDaDlq": drained,
        "ripubblicati": len(retryable),
        "revisioneManuale": len(manual_review),
        "indiceErroriCompleto": index_complete,
        "dlqSvuotata": dlq_svuotata,
        "ts": datetime.now(timezone.utc).isoformat(),
    }

    _write_reports(reconcile_run_id, retryable, manual_review, summary)

    if not index_complete:
        logger.warning(
            "Indice errori incompleto: i decoder senza causa nota sono finiti in "
            "revisione manuale invece di essere ritentati. Rilanciare per completare."
        )
    if not dlq_svuotata and drained:
        logger.warning("DLQ non svuotata nel tempo disponibile — rilanciare la Lambda.")

    logger.info("Riconciliazione conclusa: %s", summary)
    return summary


def _classify(message: dict, error_index: dict) -> dict:
    """Associa al messaggio in DLQ la causa registrata dal consumer."""
    body_raw = message.get("Body", "")
    try:
        payload = json.loads(body_raw)
    except (json.JSONDecodeError, ValueError):
        payload = {}

    decoder = payload.get("decoder_id_original")
    error = error_index.get(decoder) if decoder else None

    return {
        "decoder_id_original": decoder,
        "hasoptedoutrecommendation__c": payload.get("hasoptedoutrecommendation__c"),
        "status_code": error.get("status_code") if error else None,
        "error": error.get("error") if error else "causa non trovata nell'indice errori",
        "retryable": is_retryable(error),
        "body": body_raw,
    }


def _republish(records: list[dict], reconcile_run_id: str) -> None:
    if not records:
        return
    publish_batch(
        [
            {
                "decoder_id_original": r["decoder_id_original"],
                "hasoptedoutrecommendation__c": r["hasoptedoutrecommendation__c"],
                "run_id": reconcile_run_id,
            }
            for r in records
        ],
        SQS_QUEUE_URL,
    )


def _write_reports(reconcile_run_id, retryable, manual_review, summary) -> None:
    prefix = f"reconcile/{reconcile_run_id}"

    def put(key, body, content_type):
        _s3.put_object(Bucket=AUDIT_BUCKET, Key=key, Body=body.encode(), ContentType=content_type)

    def jsonl(records):
        return "\n".join(json.dumps(r, default=str) for r in records) + "\n"

    try:
        if retryable:
            put(f"{prefix}/retried.jsonl", jsonl(retryable), "application/x-ndjson")
        if manual_review:
            # Include il body originale: basta ripubblicarlo per rilavorare a mano
            put(f"{prefix}/manual_review.jsonl", jsonl(manual_review), "application/x-ndjson")
        put(f"{prefix}/summary.json", json.dumps(summary, default=str, indent=2), "application/json")
    except Exception as e:
        logger.error("Scrittura report di riconciliazione fallita: %s", e)
