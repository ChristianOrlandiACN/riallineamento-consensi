'use strict';

const { getDbCredentials } = require('./lib/secrets');
const { createConnection, queryPage } = require('./lib/db');
const { publishBatch } = require('./lib/sqs');

const {
  DB_SECRET_NAME,
  DB_NAME,
  DB_TABLE = 'audit_sky_recs_recommendation',
  SQS_QUEUE_URL,
  PAGE_SIZE = '500',
} = process.env;

// Margine di sicurezza prima della scadenza del timeout Lambda.
// Se il tempo residuo scende sotto questa soglia la Lambda si ferma e restituisce
// lastCursor, da passare come startCursor all'invocazione successiva.
const TIMEOUT_SAFETY_MS = 30_000;

exports.handler = async (event, context) => {
  if (!DB_SECRET_NAME || !DB_NAME || !SQS_QUEUE_URL) {
    throw new Error('Variabili di ambiente mancanti: DB_SECRET_NAME, DB_NAME, SQS_QUEUE_URL');
  }

  const pageSize = parseInt(PAGE_SIZE, 10);
  // startCursor può essere passato nell'event per riprendere da dove si era fermati
  let lastCursor = typeof event?.startCursor === 'string' ? event.startCursor : '';

  const credentials = await getDbCredentials(DB_SECRET_NAME);
  const connection = await createConnection(credentials, DB_NAME);

  let totalPublished = 0;
  let pageCount = 0;
  let completed = false;

  try {
    while (true) {
      if (context.getRemainingTimeInMillis() < TIMEOUT_SAFETY_MS) {
        console.warn(`[PRODUCER] timeout imminente, fermo a lastCursor="${lastCursor}". Rilanciare con startCursor="${lastCursor}"`);
        break;
      }

      const records = await queryPage(connection, DB_TABLE, lastCursor, pageSize);

      if (records.length === 0) {
        completed = true;
        break;
      }

      await publishBatch(records, SQS_QUEUE_URL);

      lastCursor = records[records.length - 1].decoder_id_original;
      totalPublished += records.length;
      pageCount++;

      console.log(`[PRODUCER] pagina ${pageCount}: +${records.length} record (totale=${totalPublished}, lastCursor="${lastCursor}")`);

      if (records.length < pageSize) {
        completed = true;
        break;
      }
    }
  } finally {
    await connection.end();
  }

  const summary = { completed, totalPublished, pageCount, lastCursor };
  console.log('[PRODUCER] fine esecuzione:', JSON.stringify(summary));
  return summary;
};
