"""
Lambda di riconciliazione — va invocata manualmente dopo un run del producer/consumer.

Event input:
{
    "runId": "20240101-120000",   # run_id da riconciliare
    "dryRun": false               # opzionale: true = analisi senza ri-pubblicare su SQS
}

Logica:
1. Legge tutti i file audit/{runId}/outcomes/*.jsonl
2. Separa:
   - retryable: status_code in {None, 502, 503, 504} → ri-pubblica su SQS
   - manual_review: tutti gli altri errori (4xx, 500) → solo log e report su S3
3. Scrive su S3:
   - reconcile/{reconcile_run_id}/retried.jsonl
   - reconcile/{reconcile_run_id}/manual_review.jsonl
   - reconcile/{reconcile_run_id}/summary.json
"""

import json
import os
import uuid
import logging
from datetime import datetime, timezone

import boto3

from lib.audit_reader import list_outcome_keys, read_outcomes
from lib.sqs import publish_batch

logger = logging.getLogger()
logger.setLevel(logging.INFO)

AUDIT_BUCKET = os.environ["AUDIT_BUCKET"]
SQS_QUEUE_URL = os.environ["SQS_QUEUE_URL"]

_s3 = boto3.client("s3", region_name="eu-west-1")


def handler(event, context):
    run_id: str = event.get("runId", "")
    dry_run: bool = bool(event.get("dryRun", False))

    if not run_id:
        raise ValueError("Event must contain 'runId'")

    reconcile_run_id = f"reconcile-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:6]}"
    logger.info("Starting reconciliation. runId=%s reconcileRunId=%s dryRun=%s", run_id, reconcile_run_id, dry_run)

    # 1. Carica tutti i file outcomes del run originale
    keys = list_outcome_keys(AUDIT_BUCKET, run_id)
    logger.info("Found %d outcome file(s) for runId=%s", len(keys), run_id)

    if not keys:
        logger.warning("No outcome files found for runId=%s — nothing to reconcile", run_id)
        return {"reconcileRunId": reconcile_run_id, "retried": 0, "manualReview": 0}

    # 2. Classifica errori
    retryable, manual_review = read_outcomes(AUDIT_BUCKET, keys)
    logger.info("Retryable: %d | Manual review: %d", len(retryable), len(manual_review))

    # 3. Ri-pubblica su SQS i record retryable (con nuovo run_id per tracciare il retry)
    retried_count = 0
    if retryable and not dry_run:
        sqs_records = [
            {
                "decoder_id_original": r["decoder_id_original"],
                "hasoptedoutrecommendation__c": r["hasoptedoutrecommendation__c"],
                "run_id": reconcile_run_id,
            }
            for r in retryable
        ]
        retried_count = publish_batch(sqs_records, SQS_QUEUE_URL)
        logger.info("Re-published %d records to SQS", retried_count)

    # 4. Scrivi report su S3
    _write_report(reconcile_run_id, run_id, retryable, manual_review, dry_run)

    return {
        "reconcileRunId": reconcile_run_id,
        "sourceRunId": run_id,
        "dryRun": dry_run,
        "retried": retried_count,
        "manualReview": len(manual_review),
    }


def _write_report(reconcile_run_id, source_run_id, retryable, manual_review, dry_run):
    prefix = f"reconcile/{reconcile_run_id}"

    def _put(key, data):
        _s3.put_object(
            Bucket=AUDIT_BUCKET,
            Key=key,
            Body=data.encode(),
            ContentType="application/x-ndjson" if key.endswith(".jsonl") else "application/json",
        )

    try:
        if retryable:
            _put(
                f"{prefix}/retried.jsonl",
                "\n".join(json.dumps(r, default=str) for r in retryable) + "\n",
            )
        if manual_review:
            _put(
                f"{prefix}/manual_review.jsonl",
                "\n".join(json.dumps(r, default=str) for r in manual_review) + "\n",
            )
        _put(
            f"{prefix}/summary.json",
            json.dumps({
                "reconcileRunId": reconcile_run_id,
                "sourceRunId": source_run_id,
                "dryRun": dry_run,
                "retryableCount": len(retryable),
                "manualReviewCount": len(manual_review),
                "ts": datetime.now(timezone.utc).isoformat(),
            }, indent=2),
        )
    except Exception as e:
        logger.error("Failed to write reconciliation report to S3: %s", e)
