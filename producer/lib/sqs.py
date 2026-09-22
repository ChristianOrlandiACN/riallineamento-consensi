import json
import uuid
import boto3

_client = boto3.client("sqs", region_name="eu-west-1")


def publish_batch(records: list[dict], queue_url: str) -> int:
    """Publish records to SQS in chunks of 10 (AWS hard limit). Returns total sent."""
    total = 0
    for i in range(0, len(records), 10):
        chunk = records[i : i + 10]
        entries = [
            {"Id": str(uuid.uuid4()), "MessageBody": json.dumps(record)}
            for record in chunk
        ]
        response = _client.send_message_batch(QueueUrl=queue_url, Entries=entries)
        failed = response.get("Failed", [])
        if failed:
            raise RuntimeError(f"SQS batch send partial failure: {failed}")
        total += len(chunk)
    return total
