import time
import psycopg2
import psycopg2.extras


def create_connection(credentials: dict, host: str, port: int, db_name: str):
    """PostgreSQL connection with exponential backoff (3 attempts: 0.5s, 1s)."""
    delays = [0.5, 1.0, None]  # None = ultimo tentativo, nessuna attesa prima di sollevare
    last_err: Exception | None = None
    for delay in delays:
        try:
            conn = psycopg2.connect(
                host=host,
                port=port,
                dbname=db_name,
                user=credentials["dmf_db_user"],
                password=credentials["dmf_db_pass"],
                connect_timeout=10,
            )
            # Le scritture sul segnaposto devono essere visibili subito alle altre
            # invocazioni: con una transazione implicita il lock non funzionerebbe.
            conn.autocommit = True
            return conn
        except psycopg2.Error as e:
            last_err = e
            if delay is not None:
                time.sleep(delay)
    raise last_err  # type: ignore[misc]


def query_page(conn, schema: str, table: str, last_cursor: str, page_size: int) -> list[dict]:
    """Cursor-based pagination on decoder_id_original (ascending)."""
    sql = f"""
        SELECT decoder_id_original, hasoptedoutrecommendation__c
        FROM {schema}.{table}
        WHERE decoder_id_original > %s
        ORDER BY decoder_id_original ASC
        LIMIT %s
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, (last_cursor, page_size))
        return [dict(row) for row in cur.fetchall()]
