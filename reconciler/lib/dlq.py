import boto3

_sqs = boto3.client("sqs", region_name="eu-west-1")

SQS_MAX_BATCH = 10


def receive_batch(queue_url: str, visibility_timeout: int) -> list[dict]:
    """
    Legge fino a 10 messaggi dalla DLQ.

    Il visibility timeout copre l'intera durata della Lambda: così un messaggio
    già letto non viene riproposto a noi stessi più avanti nello stesso giro.
    """
    response = _sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=SQS_MAX_BATCH,
        VisibilityTimeout=visibility_timeout,
        WaitTimeSeconds=1,
    )
    return response.get("Messages", [])


def approximate_depth(queue_url: str) -> int:
    response = _sqs.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["ApproximateNumberOfMessages"]
    )
    return int(response["Attributes"]["ApproximateNumberOfMessages"])


def delete_batch(queue_url: str, messages: list[dict]) -> None:
    """Rimuove definitivamente i messaggi dalla DLQ."""
    if not messages:
        return
    entries = [
        {"Id": str(i), "ReceiptHandle": m["ReceiptHandle"]}
        for i, m in enumerate(messages)
    ]
    response = _sqs.delete_message_batch(QueueUrl=queue_url, Entries=entries)
    failed = response.get("Failed", [])
    if failed:
        raise RuntimeError(f"Cancellazione dalla DLQ fallita: {failed}")
