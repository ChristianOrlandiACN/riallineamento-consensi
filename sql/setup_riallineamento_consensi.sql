-- =============================================================================
-- RIALLINEAMENTO MASSIVO CONSENSI CRM/Rex
-- Modifiche richieste sul DB HOT — schema audit
--
-- Contesto: riallineamento di 1.800.000 consensi privacy/recommendation
-- disallineati tra CRM (Salesforce) e sistema Rex. Il processo legge
-- audit.audit_sky_recs_recommendation a pagine e invoca le API Hermes per
-- ogni decoder.
--
-- QUANDO: prima del caricamento dati. Su tabella vuota l'esecuzione è
-- immediata; dopo il caricamento delle 1.8M righe il punto 2 richiede
-- tempo e prende lock sulla tabella.
--
-- PRIMA DI ESEGUIRE: sostituire <UTENTE_LAMBDA> con il nome dell'utenza
-- applicativa (compare nei punti 1, 4 e 5).
-- =============================================================================


-- ─────────────────────────────────────────────────────────────────────────────
-- 1. UTENZA APPLICATIVA DELLA LAMBDA
--
-- Utenza dedicata al processo di riallineamento, distinta da quelle esistenti
-- per poterne tracciare e revocare l'accesso in modo indipendente.
-- La usa la sola Lambda producer: è l'unico componente che si connette al DB
-- (consumer e reconciler lavorano su SQS, S3 e API Hermes).
--
-- Richiede privilegio CREATEROLE o superuser.
--
-- >>> SICUREZZA: non scrivere qui la password reale e non salvarla in questo
-- >>> file. Generarla al momento dell'esecuzione e riporla su AWS Secrets
-- >>> Manager nel secret prod/dmf/db/hot (campi dmf_db_user / dmf_db_pass).
-- ─────────────────────────────────────────────────────────────────────────────

CREATE ROLE <UTENTE_LAMBDA> WITH LOGIN PASSWORD '<PASSWORD_DA_GENERARE>';


-- ─────────────────────────────────────────────────────────────────────────────
-- 2. INDICE SULLA TABELLA SORGENTE
--
-- audit_sky_recs_recommendation non ha attualmente alcun indice né vincolo.
-- Il producer la legge a pagine ripetendo:
--     WHERE decoder_id_original > :cursore ORDER BY decoder_id_original LIMIT 500
-- Senza indice ogni pagina costringe a scandire e ordinare l'intera tabella:
-- su 1.8M righe e ~3600 pagine il processo non rientra nella finestra prevista.
--
-- Composito di proposito: la prima colonna serve alla paginazione, la seconda
-- permette di selezionare il consenso più recente per decoder nel caso la
-- tabella contenga lo storico eventi anziché una riga per decoder (vedi
-- domanda A in fondo).
--
-- Richiede di essere proprietari della tabella.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_audit_sky_recs_decoder_dt
    ON audit.audit_sky_recs_recommendation (decoder_id_original, dt DESC);


-- ─────────────────────────────────────────────────────────────────────────────
-- 3. TABELLA DI STATO DEL PROCESSO
--
-- Il producer non riesce a leggere 1.8M record in una sola esecuzione (limite
-- di 5 minuti per invocazione), quindi viene rilanciato automaticamente finché
-- non ha finito. Questa tabella conserva il punto in cui si era fermato e fa da
-- lock per impedire che due esecuzioni sovrapposte elaborino gli stessi record.
--
-- Contiene solo stato di avanzamento: nessun dato personale.
-- Dimensione: una riga per campagna.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS audit.consent_realign_state (
    run_id          varchar(64) PRIMARY KEY,   -- identificativo campagna
    last_cursor     varchar(64) NOT NULL DEFAULT '',  -- ultimo decoder elaborato
    status          varchar(16) NOT NULL DEFAULT 'idle',  -- idle | running | completed
    total_published bigint      NOT NULL DEFAULT 0,
    lease_until     timestamp,                 -- scadenza lock, evita blocchi permanenti
    updated_at      timestamp   NOT NULL DEFAULT now()
);


-- ─────────────────────────────────────────────────────────────────────────────
-- 4. PERMESSI DELL'UTENZA
--
-- Scrittura richiesta SOLO sulla tabella di stato del punto 3.
-- La tabella dei consensi resta in sola lettura.
-- ─────────────────────────────────────────────────────────────────────────────

GRANT USAGE  ON SCHEMA audit                                TO <UTENTE_LAMBDA>;
GRANT SELECT ON audit.audit_sky_recs_recommendation         TO <UTENTE_LAMBDA>;
GRANT SELECT, INSERT, UPDATE ON audit.consent_realign_state TO <UTENTE_LAMBDA>;


-- ─────────────────────────────────────────────────────────────────────────────
-- 5. VERIFICA — eseguire dopo i punti precedenti
-- ─────────────────────────────────────────────────────────────────────────────

-- L'indice esiste?
SELECT indexname, indexdef
FROM pg_indexes
WHERE schemaname = 'audit'
  AND tablename  = 'audit_sky_recs_recommendation';

-- I permessi dell'utenza sono corretti? Attesi: true, true, true
SELECT
    has_table_privilege('<UTENTE_LAMBDA>', 'audit.audit_sky_recs_recommendation', 'SELECT') AS legge_sorgente,
    has_table_privilege('<UTENTE_LAMBDA>', 'audit.consent_realign_state', 'INSERT')         AS scrive_stato,
    has_table_privilege('<UTENTE_LAMBDA>', 'audit.consent_realign_state', 'UPDATE')         AS aggiorna_stato;

-- L'utenza NON deve poter scrivere sulla sorgente. Atteso: false
SELECT has_table_privilege('<UTENTE_LAMBDA>', 'audit.audit_sky_recs_recommendation', 'UPDATE') AS scrive_sorgente;


-- =============================================================================
-- DUE DOMANDE APERTE
-- =============================================================================
--
-- A) audit_sky_recs_recommendation conterrà UNA RIGA PER DECODER oppure lo
--    STORICO COMPLETO degli eventi di modifica del consenso?
--
--    È la domanda che determina se il codice è corretto. La tabella non ha
--    vincoli di unicità su decoder_id_original, quindi al momento non possiamo
--    dedurlo. Se ci fossero più righe per decoder, la query attuale ne
--    restituirebbe una arbitraria invece della più recente per dt: rischieremmo
--    di riscrivere consensi obsoleti, cioè esattamente il problema che stiamo
--    correggendo. In quel caso la query va cambiata in DISTINCT ON.
--
-- B) Il refresh dei dati post-deploy è ADDITIVO oppure fa DROP/TRUNCATE
--    a livello di schema?
--
--    Se è invasivo, la tabella del punto 3 verrebbe azzerata a metà lavorazione
--    e il processo ripartirebbe da capo ripubblicando tutto. In quel caso va
--    collocata in uno schema diverso da audit.
--
-- =============================================================================
