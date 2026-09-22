import os
import logging
from datetime import datetime, timezone

from lib.secrets import get_db_credentials
from lib.db import create_connection, query_page
from lib.sqs import publish_batch
from lib.audit import write_published, write_summary

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DB_SECRET_NAME = os.environ["DB_SECRET_NAME"]
DB_HOST = os.environ["DB_HOST"]
DB_PORT = int(os.environ.get("DB_PORT", "6543"))
DB_NAME = os.environ["DB_NAME"]
DB_SCHEMA = os.environ.get("DB_SCHEMA", "AUDIT")
DB_TABLE = os.environ["DB_TABLE"]
SQS_QUEUE_URL = os.environ["SQS_QUEUE_URL"]
PAGE_SIZE = int(os.environ.get("PAGE_SIZE", "500"))
AUDIT_BUCKET = os.environ.get("AUDIT_BUCKET", "")

SAFETY_MARGIN_MS = 30_000


def handler(event, context):
    # run_id: shared across invocations della stessa campagna.
    # Prima invocazione: non presente → generato. Invocazioni successive: passare il
    # run_id restituito dalla precedente per raggruppare i file di audit in un'unica folder.
    run_id: str = event.get("runId") or datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    last_cursor: str = event.get("startCursor", "")
    total_published = 0
    page_count = 0
    published_records: list[dict] = []

    credentials = get_db_credentials(DB_SECRET_NAME)
    conn = create_connection(credentials, DB_HOST, DB_PORT, DB_NAME)

    stopped_early = False
    try:
        while True:
            if context.get_remaining_time_in_millis() < SAFETY_MARGIN_MS:
                logger.warning(
                    "Safety timeout reached — stopping. runId=%s lastCursor=%s",
                    run_id, last_cursor,
                )
                stopped_early = True
                break

            rows = query_page(conn, DB_SCHEMA, DB_TABLE, last_cursor, PAGE_SIZE)
            if not rows:
                break

            # Embed run_id in each SQS message so the consumer knows the audit folder
            rows_with_meta = [{**row, "run_id": run_id} for row in rows]
            count = publish_batch(rows_with_meta, SQS_QUEUE_URL)
            published_records.extend(rows_with_meta)
            total_published += count
            page_count += 1
            last_cursor = rows[-1]["decoder_id_original"]

            logger.info(
                "Page %d: published %d records, lastCursor=%s",
                page_count, count, last_cursor,
            )
    finally:
        conn.close()

    _write_audit(run_id, published_records, total_published, page_count, last_cursor, stopped_early)

    logger.info(
        "runId=%s completed=%s totalPublished=%d pageCount=%d",
        run_id, not stopped_early, total_published, page_count,
    )
    return {
        "completed": not stopped_early,
        "runId": run_id,
        "totalPublished": total_published,
        "pageCount": page_count,
        "lastCursor": last_cursor,
    }


def _write_audit(run_id, records, total_published, page_count, last_cursor, stopped_early):
    if not AUDIT_BUCKET:
        return
    try:
        if records:
            write_published(AUDIT_BUCKET, run_id, records)
        write_summary(AUDIT_BUCKET, run_id, {
            "runId": run_id,
            "completed": not stopped_early,
            "totalPublished": total_published,
            "pageCount": page_count,
            "lastCursor": last_cursor,
        })
    except Exception as e:
        logger.error("Audit S3 write failed: %s", e)
