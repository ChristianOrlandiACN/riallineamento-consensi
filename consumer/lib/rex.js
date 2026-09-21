'use strict';

// TODO: impostare quando Alessandro Paggio / Gianluca De Gennaro forniscono i dettagli Rex
const REX_ENDPOINT_URL = process.env.REX_ENDPOINT_URL;
const REX_TIMEOUT_MS = parseInt(process.env.REX_TIMEOUT_MS || '10000', 10);

class RexError extends Error {
  constructor(message, statusCode) {
    super(message);
    this.name = 'RexError';
    this.statusCode = statusCode;
    // 5xx e timeout = recuperabile (retry SQS); 4xx = non recuperabile (scartat con log)
    this.isRecoverable = statusCode == null || statusCode >= 500;
  }
}

/**
 * Aggiorna il consenso su Rex per il record ricevuto.
 * Il payload verrà adattato alla struttura dell'API Rex quando disponibile.
 */
async function updateConsent(record) {
  if (!REX_ENDPOINT_URL) {
    throw new Error('REX_ENDPOINT_URL non configurato');
  }

  // TODO: aggiungere header di autenticazione quando disponibili
  let response;
  try {
    response = await fetch(REX_ENDPOINT_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        // 'Authorization': `Bearer ${token}`,  // TODO
      },
      body: JSON.stringify(record),
      signal: AbortSignal.timeout(REX_TIMEOUT_MS),
    });
  } catch (err) {
    // Timeout o errore di rete: recuperabile
    throw new RexError(`Rex network error: ${err.message}`, null);
  }

  if (!response.ok) {
    const body = await response.text().catch(() => '');
    throw new RexError(`Rex ${response.status}: ${body}`, response.status);
  }

  return response.json().catch(() => null);
}

module.exports = { updateConsent, RexError };
