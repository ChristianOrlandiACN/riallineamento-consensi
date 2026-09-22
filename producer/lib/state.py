"""
Segnaposto del producer su tabella audit.consent_realign_state.

Il producer viene rilanciato da EventBridge finché la campagna non è completa.
Questo modulo gestisce il punto di ripresa e il lock tra invocazioni sovrapposte.
"""

STATE_TABLE = "audit.consent_realign_state"


def acquire_lease(conn, run_id: str, lease_seconds: int) -> str | None:
    """
    Prende possesso del segnaposto con una singola UPDATE condizionale, quindi
    senza race condition tra invocazioni concorrenti.

    Restituisce last_cursor da cui ripartire, oppure None se il lease è già
    detenuto da un'altra invocazione o se la campagna è completata.
    """
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO {STATE_TABLE} (run_id) VALUES (%s) ON CONFLICT (run_id) DO NOTHING",
            (run_id,),
        )
        cur.execute(
            f"""
            UPDATE {STATE_TABLE}
            SET status      = 'running',
                lease_until = now() + make_interval(secs => %s),
                updated_at  = now()
            WHERE run_id = %s
              AND status <> 'completed'
              AND (status = 'idle' OR lease_until IS NULL OR lease_until < now())
            RETURNING last_cursor
            """,
            (lease_seconds, run_id),
        )
        row = cur.fetchone()
        return row[0] if row else None


def save_cursor(conn, run_id: str, cursor: str, published: int, lease_seconds: int) -> None:
    """
    Avanza il segnaposto e prolunga il lease.

    Va chiamata solo dopo che SQS ha accettato la pagina: se il cursore avanzasse
    prima, un fallimento di pubblicazione farebbe saltare definitivamente dei clienti.
    """
    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE {STATE_TABLE}
            SET last_cursor     = %s,
                total_published = total_published + %s,
                lease_until     = now() + make_interval(secs => %s),
                updated_at      = now()
            WHERE run_id = %s
            """,
            (cursor, published, lease_seconds, run_id),
        )


def release(conn, run_id: str, completed: bool) -> None:
    """Rilascia il lease: 'completed' chiude la campagna, 'idle' la lascia riprendibile."""
    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE {STATE_TABLE}
            SET status      = %s,
                lease_until = NULL,
                updated_at  = now()
            WHERE run_id = %s
            """,
            ("completed" if completed else "idle", run_id),
        )
