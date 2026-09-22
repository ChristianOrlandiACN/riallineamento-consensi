import json
import os
import logging
from datetime import datetime, timezone

from lib.hermes import update_consent, HermesError
from lib.audit import write_outcomes

logger = logging.getLogger()
logger.setLevel(logging.INFO)

AUDIT_BUCKET = os.environ.get("AUDIT_BUCKET", "")


def handler(event, context):
    batch_item_failures = []
    outcomes = []

    for record in event.get("Records", []):
        message_id = record["messageId"]
        body_raw = record.get("body", "")

        try:
            payload = json.loads(body_raw)
        except (json.JSONDecodeError, ValueError):
            logger.error(
                "Non-parseable message body, discarding. messageId=%s body=%s",
                message_id, body_raw[:200],
            )
            continue

        run_id = payload.get("run_id", "unknown")
        decoder_id = payload.get("decoder_id_original", "")
        consent_val = payload.get("hasoptedoutrecommendation__c", "")
        ts = datetime.now(timezone.utc).isoformat()

        try:
            status_code = update_consent(payload)
            outcomes.append({
                "decoder_id_original": decoder_id,
                "hasoptedoutrecommendation__c": consent_val,
                "status": "ok",
                "status_code": status_code,
                "error": None,
                "run_id": run_id,
                "ts": ts,
            })
        except HermesError as e:
            outcomes.append({
                "decoder_id_original": decoder_id,
                "hasoptedoutrecommendation__c": consent_val,
                "status": "error",
                "status_code": e.status_code,
                "error": str(e),
                "run_id": run_id,
                "ts": ts,
            })
            if e.is_recoverable:
                logger.warning(
                    "Recoverable Hermes error for messageId=%s: %s — added to batchItemFailures",
                    message_id, e,
                )
                batch_item_failures.append({"itemIdentifier": message_id})
            else:
                logger.error(
                    "Non-recoverable Hermes error for messageId=%s (HTTP %s): %s — discarding",
                    message_id, e.status_code, e,
                )
        except Exception as e:
            outcomes.append({
                "decoder_id_original": decoder_id,
                "hasoptedoutrecommendation__c": consent_val,
                "status": "error",
                "status_code": None,
                "error": str(e),
                "run_id": run_id,
                "ts": ts,
            })
            logger.exception(
                "Unexpected error for messageId=%s — added to batchItemFailures", message_id
            )
            batch_item_failures.append({"itemIdentifier": message_id})

    _write_audit(outcomes)
    return {"batchItemFailures": batch_item_failures}


def _write_audit(outcomes: list[dict]) -> None:
    if not AUDIT_BUCKET or not outcomes:
        return
    # Group by run_id (a batch could theoretically mix runs, but normally all same)
    by_run: dict[str, list[dict]] = {}
    for o in outcomes:
        by_run.setdefault(o["run_id"], []).append(o)
    for run_id, run_outcomes in by_run.items():
        write_outcomes(AUDIT_BUCKET, run_id, run_outcomes)
