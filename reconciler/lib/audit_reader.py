import json
import logging
import boto3

logger = logging.getLogger()
_s3 = boto3.client("s3", region_name="eu-west-1")

# Errori per cui ha senso riprovare: assenza di status (timeout/network), 502, 503, 504
RETRYABLE_STATUS_CODES: frozenset = frozenset({None, 502, 503, 504})


def list_outcome_keys(bucket: str, run_id: str) -> list[str]:
    """Lista tutti i file outcomes sotto audit/{run_id}/outcomes/."""
    prefix = f"audit/{run_id}/outcomes/"
    keys = []
    paginator = _s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def read_outcomes(bucket: str, keys: list[str]) -> tuple[list[dict], list[dict]]:
    """
    Legge i file JSONL e separa:
    - retryable: status='error' e status_code in RETRYABLE_STATUS_CODES
    - manual_review: tutti gli altri errori (4xx, 500, ecc.)

    Restituisce (retryable, manual_review).
    """
    retryable: list[dict] = []
    manual_review: list[dict] = []

    for key in keys:
        try:
            obj = _s3.get_object(Bucket=bucket, Key=key)
            lines = obj["Body"].read().decode().strip().splitlines()
        except Exception as e:
            logger.error("Cannot read S3 key %s: %s", key, e)
            continue

        for line in lines:
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                logger.warning("Non-parseable line in %s, skipping", key)
                continue

            if record.get("status") != "error":
                continue

            status_code = record.get("status_code")
            if status_code in RETRYABLE_STATUS_CODES:
                retryable.append(record)
            else:
                manual_review.append(record)

    return retryable, manual_review
