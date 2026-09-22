'use strict';

// Base URL configurabile per prod/collaudo tramite variabile d'ambiente
// Prod:     https://servizi.sky.it/hermes/v0
// Collaudo: https://servizicollaudo.sky.it/hermes-st/v0
const REX_BASE_URL = process.env.REX_BASE_URL;
const REX_TIMEOUT_MS = parseInt(process.env.REX_TIMEOUT_MS || '10000', 10);

// TODO: aggiungere autenticazione e certificato MDW quando disponibili lato ACN
// Il secret Secrets Manager conterrà le credenziali di connessione MDW

class RexError extends Error {
  constructor(message, statusCode) {
    super(message);
    this.name = 'RexError';
    this.statusCode = statusCode;
    // 5xx e timeout = recuperabile (retry SQS); 4xx = non recuperabile (scartato con log)
    this.isRecoverable = statusCode == null || statusCode >= 500;
  }
}

/**
 * Chiama l'API Hermes per aggiornare il consenso del decoder.
 * hasoptedoutrecommendation__c === 'true'  → opt-out
 * hasoptedoutrecommendation__c === 'false' → opt-in
 */
async function updateConsent(record) {
  if (!REX_BASE_URL) {
    throw new Error('REX_BASE_URL non configurato');
  }

  const { decoder_id_original, hasoptedoutrecommendation__c } = record;

  if (!decoder_id_original) {
    throw Object.assign(new RexError('decoder_id_original mancante nel record', 400), { isRecoverable: false });
  }

  const action = hasoptedoutrecommendation__c === 'true' ? 'opt-out' : 'opt-in';
  const url = `${REX_BASE_URL}/devices/${encodeURIComponent(decoder_id_original)}/${action}`;

  let response;
  try {
    response = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        // TODO: 'Authorization': `Bearer ${token}`,        // credenziali MDW
        // TODO: aggiungere client certificate TLS se richiesto da MDW
      },
      signal: AbortSignal.timeout(REX_TIMEOUT_MS),
    });
  } catch (err) {
    // Timeout o errore di rete: recuperabile
    throw new RexError(`Hermes network error: ${err.message}`, null);
  }

  if (!response.ok) {
    const body = await response.text().catch(() => '');
    throw new RexError(`Hermes ${response.status} [${action}] decoder=${decoder_id_original}: ${body}`, response.status);
  }

  return response.json().catch(() => null);
}

module.exports = { updateConsent, RexError };
