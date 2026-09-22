import os
import logging

from lib.secrets import get_db_credentials
from lib.db import create_connection, query_page
from lib.sqs import publish_batch
from lib.audit import write_published, write_summary
from lib.state import acquire_lease, save_cursor, release

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DB_SECRET_NAME = os.environ["DB_SECRET_NAME"]
DB_HOST = os.environ["DB_HOST"]
DB_PORT = int(os.environ.get("DB_PORT", "6543"))
DB_NAME = os.environ["DB_NAME"]
DB_SCHEMA = os.environ.get("DB_SCHEMA", "audit")
DB_TABLE = os.environ["DB_TABLE"]
SQS_QUEUE_URL = os.environ["SQS_QUEUE_URL"]
PAGE_SIZE = int(os.environ.get("PAGE_SIZE", "500"))
AUDIT_BUCKET = os.environ.get("AUDIT_BUCKET", "")
RUN_ID = os.environ.get("RUN_ID", "riallineamento")
LEASE_SECONDS = int(os.environ.get("LEASE_SECONDS", "360"))

SAFETY_MARGIN_MS = 30_000


def handler(event, context):
    """
    Invocata ripetutamente da EventBridge finché la campagna non è completa.
    Il punto di ripresa è su audit.consent_realign_state, non nell'evento.
    """
    run_id = (event or {}).get("runId") or RUN_ID

    credentials = get_db_credentials(DB_SECRET_NAME)
    conn = create_connection(credentials, DB_HOST, DB_PORT, DB_NAME)

    try:
        last_cursor = acquire_lease(conn, run_id, LEASE_SECONDS)
        if last_cursor is None:
            logger.info(
                "Lease non acquisito per runId=%s: invocazione precedente ancora in corso "
                "oppure campagna già completata. Esco.", run_id,
            )
            return {"skipped": True, "runId": run_id}

        logger.info(
            "Lease acquisito. runId=%s ripresa da cursore=%s",
            run_id, last_cursor or "(inizio tabella)",
        )

        result = _process_pages(conn, context, run_id, last_cursor)

        # Rilascio solo sul percorso pulito: in caso di eccezione il lease scade
        # da solo dopo LEASE_SECONDS e la schedulazione successiva riprende,
        # il che fa da backoff naturale se il problema è persistente.
        release(conn, run_id, result["completed"])
        return result
    finally:
        conn.close()


def _process_pages(conn, context, run_id: str, last_cursor: str) -> dict:
    total_published = 0
    page_count = 0
    completed = False

    while True:
        # Uscire prima del timeout duro garantisce che il segnaposto e il
        # riepilogo di audit vengano scritti.
        if context.get_remaining_time_in_millis() < SAFETY_MARGIN_MS:
            logger.warning(
                "Margine di sicurezza raggiunto. runId=%s cursore=%s — riprenderà alla prossima schedulazione",
                run_id, last_cursor,
            )
            break

        rows = query_page(conn, DB_SCHEMA, DB_TABLE, last_cursor, PAGE_SIZE)
        if not rows:
            completed = True
            break

        rows_with_meta = [{**row, "run_id": run_id} for row in rows]
        published = publish_batch(rows_with_meta, SQS_QUEUE_URL)

        last_cursor = rows[-1]["decoder_id_original"]
        save_cursor(conn, run_id, last_cursor, published, LEASE_SECONDS)
        _write_published(run_id, rows_with_meta)

        total_published += published
        page_count += 1
        logger.info(
            "Pagina %d: %d record pubblicati, cursore=%s", page_count, published, last_cursor,
        )

    summary = {
        "completed": completed,
        "runId": run_id,
        "totalPublished": total_published,
        "pageCount": page_count,
        "lastCursor": last_cursor,
    }
    _write_summary(run_id, summary)
    logger.info("Fine invocazione: %s", summary)
    return summary


def _write_published(run_id: str, records: list[dict]) -> None:
    """Un file S3 per pagina: evita di tenere l'intera campagna in memoria."""
    if not AUDIT_BUCKET:
        return
    try:
        write_published(AUDIT_BUCKET, run_id, records)
    except Exception as e:
        logger.error("Scrittura audit published fallita: %s", e)


def _write_summary(run_id: str, summary: dict) -> None:
    if not AUDIT_BUCKET:
        return
    try:
        write_summary(AUDIT_BUCKET, run_id, summary)
    except Exception as e:
        logger.error("Scrittura audit summary fallita: %s", e)
