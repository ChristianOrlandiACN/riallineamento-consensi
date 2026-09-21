'use strict';

// Swap 'mysql2/promise' with 'pg' if the DB is PostgreSQL.
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
 * Cursor-based pagination: efficiente su grandi dataset perché non usa OFFSET.
 * Richiede che la tabella abbia una colonna 'id' numerica e un indice su di essa.
 * Aggiornare la query quando Antonio Croce fornirà la struttura della tabella.
 */
async function queryPage(connection, table, lastId, pageSize) {
  const [rows] = await connection.execute(
    `SELECT * FROM \`${table}\` WHERE id > ? ORDER BY id ASC LIMIT ?`,
    [lastId, pageSize]
  );
  return rows;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

module.exports = { createConnection, queryPage };
