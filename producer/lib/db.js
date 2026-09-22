'use strict';

const mysql = require('mysql2/promise');

const MAX_RETRIES = 3;
const BASE_DELAY_MS = 500;

async function createConnection(credentials, dbName) {
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      const connection = await mysql.createConnection({
        host: credentials.host,
        port: credentials.port || 3306,
        user: credentials.username,
        password: credentials.password,
        database: dbName,
        connectTimeout: 10_000,
      });
      return connection;
    } catch (err) {
      if (attempt === MAX_RETRIES) throw err;
      const delay = BASE_DELAY_MS * Math.pow(2, attempt - 1);
      console.warn(`[DB] tentativo ${attempt} fallito, retry tra ${delay}ms: ${err.message}`);
      await sleep(delay);
    }
  }
}

/**
 * Cursor-based pagination su decoder_id_original (varchar).
 * Inizializzare lastCursor a '' (stringa vuota) per partire dall'inizio.
 */
async function queryPage(connection, table, lastCursor, pageSize) {
  const [rows] = await connection.execute(
    `SELECT decoder_id_original, hasoptedoutrecommendation__c
     FROM \`${table}\`
     WHERE decoder_id_original > ?
     ORDER BY decoder_id_original ASC
     LIMIT ?`,
    [lastCursor, pageSize]
  );
  return rows;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

module.exports = { createConnection, queryPage };
