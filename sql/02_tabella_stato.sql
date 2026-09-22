-- Tabella di stato del producer ("segnaposto").
--
-- Il producer non riesce a leggere 1.8M record in una sola invocazione (timeout
-- Lambda 5 minuti), quindi viene rilanciato da EventBridge finché non ha finito.
-- Questa tabella conserva il punto in cui si era fermato e fa da lock tra
-- invocazioni che si sovrappongono.
--
-- Richiede permesso di SCRITTURA per il ruolo della Lambda producer
-- (la tabella sorgente resta in sola lettura).
--
-- Colonne:
--   run_id          identificativo della campagna; raggruppa anche l'audit su S3
--   last_cursor     ultimo decoder_id_original pubblicato con successo su SQS
--   status          idle = riprendibile | running = in corso | completed = finita
--   total_published contatore cumulativo dei record pubblicati
--   lease_until     scadenza del lock; se passata, un'altra invocazione può subentrare
--                   (evita il blocco permanente se una Lambda muore senza rilasciare)

CREATE TABLE IF NOT EXISTS audit.consent_realign_state (
    run_id          varchar(64) PRIMARY KEY,
    last_cursor     varchar(64) NOT NULL DEFAULT '',
    status          varchar(16) NOT NULL DEFAULT 'idle',
    total_published bigint      NOT NULL DEFAULT 0,
    lease_until     timestamp,
    updated_at      timestamp   NOT NULL DEFAULT now()
);

-- Avanzamento della campagna:
--   SELECT * FROM audit.consent_realign_state;
--
-- Ripartire da zero (solo a campagna ferma):
--   UPDATE audit.consent_realign_state
--   SET last_cursor = '', status = 'idle', total_published = 0, lease_until = NULL
--   WHERE run_id = '<run_id>';
