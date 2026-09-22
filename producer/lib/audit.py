import json
import uuid
import boto3

_s3 = boto3.client("s3", region_name="eu-west-1")


def write_published(bucket: str, run_id: str, records: list[dict]) -> str:
    """Write all published records to S3 as JSONL. Returns the S3 key."""
    key = f"audit/{run_id}/published-{uuid.uuid4()}.jsonl"
    body = "\n".join(json.dumps(r, default=str) for r in records) + "\n"
    _s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode(),
        ContentType="application/x-ndjson",
    )
    return key


def write_summary(bucket: str, run_id: str, summary: dict) -> str:
    """Write run summary (no personal data) to S3. Returns the S3 key."""
    key = f"audit/{run_id}/summary-{uuid.uuid4()}.json"
    _s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(summary, default=str, indent=2).encode(),
        ContentType="application/json",
    )
    return key
