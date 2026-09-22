-- Indice sulla tabella sorgente dei consensi.
--
-- ESEGUIRE PRIMA DEL CARICAMENTO DATI: su tabella vuota è istantaneo,
-- su 1.8M righe richiede tempo e prende lock.
--
-- Serve alla paginazione a cursore del producer, che esegue ripetutamente:
--   WHERE decoder_id_original > :cursore ORDER BY decoder_id_original LIMIT :n
-- Senza indice ogni pagina costringe a scandire e ordinare l'intera tabella.
--
-- Composito di proposito: la prima colonna serve alla paginazione, la seconda
-- permette di prendere l'ultimo consenso per decoder (DISTINCT ON ... ORDER BY
-- decoder_id_original, dt DESC) nel caso la tabella contenga lo storico eventi
-- invece di una riga per decoder.

CREATE INDEX IF NOT EXISTS idx_audit_sky_recs_decoder_dt
    ON audit.audit_sky_recs_recommendation (decoder_id_original, dt DESC);
