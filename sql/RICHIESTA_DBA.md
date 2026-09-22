# Richiesta modifiche DB — riallineamento consensi

Testo da inviare al referente del DB interno (Antonio Croce), allegando
[`setup_riallineamento_consensi.sql`](setup_riallineamento_consensi.sql).

---

**Oggetto:** Riallineamento consensi — modifiche richieste sul DB HOT (schema audit)

Ciao Antonio,

per il riallineamento massivo dei consensi ci servono alcune modifiche sul DB HOT.
Ho preparato uno script unico con tutto quanto, in allegato: ogni sezione spiega il
motivo della modifica, così puoi valutarle senza bisogno di altro contesto.

In sintesi:

1. **Un indice** su `audit.audit_sky_recs_recommendation`. Oggi la tabella non ha
   alcun indice: il nostro processo la legge a pagine ordinando per
   `decoder_id_original`, e senza indice ogni pagina riordina l'intera tabella. Su
   1.8M righe il job non rientrerebbe nella finestra prevista.

2. **Una tabella di appoggio** (`audit.consent_realign_state`) dove il processo salva
   il punto a cui è arrivato, dato che deve girare in più riprese. Contiene solo stato
   di avanzamento, nessun dato personale, una riga per campagna.

3. **Le GRANT** per l'utenza applicativa della Lambda: lettura sulla tabella dei
   consensi, scrittura solo sulla tabella al punto 2.

**Fammi sapere se preferisci eseguirlo tu o autorizzare noi** — in entrambi i casi lo
script è pronto da lanciare dall'inizio alla fine. Nel caso lo esegua tu, va sostituito
`<UTENTE_LAMBDA>` con l'utenza della Lambda (compare in quattro punti); se l'hai creata
tu dovresti avere il nome sottomano, altrimenti dimmelo e te lo recupero.

**Sulla tempistica:** meglio farlo prima del caricamento dati. Adesso che la tabella è
vuota la creazione dell'indice è istantanea, mentre su 1.8M righe richiede tempo e
prende lock.

Infine due domande a cui non riusciamo a rispondere guardando il DB:

- **`audit_sky_recs_recommendation` conterrà una riga per decoder o lo storico completo
  degli eventi?** È la cosa più importante: non ci sono vincoli di unicità su
  `decoder_id_original`, quindi se ci fossero più righe per decoder la nostra query ne
  prenderebbe una a caso invece della più recente per `dt`. Rischieremmo di riscrivere
  consensi obsoleti, cioè proprio il problema che stiamo correggendo.

- **Il refresh dati post-deploy è additivo o fa drop/truncate dello schema?** Se è
  invasivo, la tabella al punto 2 verrebbe azzerata a metà lavorazione e dovremmo
  spostarla altrove.

Grazie,
Christian

---

## Stato della richiesta

| Punto | Stato |
|---|---|
| Indice su tabella sorgente | da richiedere |
| Tabella `consent_realign_state` | da richiedere |
| GRANT utenza Lambda | da richiedere |
| Risposta domanda A (riga per decoder o storico) | in attesa |
| Risposta domanda B (refresh additivo o distruttivo) | in attesa |

La risposta alla domanda A determina se la query in
[`producer/lib/db.py`](../producer/lib/db.py) va cambiata in `DISTINCT ON`.
