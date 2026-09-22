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
    key = f"audit/{run_id}/outcomes/{uuid.uuid4()}.jsonl"
    body = "\n".join(json.dumps(o, default=str) for o in outcomes) + "\n"
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
