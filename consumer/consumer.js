'use strict';

const { updateConsent } = require('./lib/rex');

// ReportBatchItemFailures: solo i messaggi falliti tornano in coda (non l'intero batch).
// Richiede FunctionResponseTypes: [ReportBatchItemFailures] nel template SAM.
exports.handler = async (event) => {
  const batchItemFailures = [];

  for (const record of event.Records) {
    const { messageId } = record;

    let payload;
    try {
      payload = JSON.parse(record.body);
    } catch {
      console.error(`[CONSUMER] body non parsabile messageId=${messageId} – scartato`);
      continue;
    }

    try {
      await updateConsent(payload);
      console.log(`[CONSUMER] OK messageId=${messageId}`);
    } catch (err) {
      if (err.isRecoverable) {
        // 5xx / timeout: SQS riproverà fino a maxReceiveCount, poi DLQ
        console.warn(`[CONSUMER] errore recuperabile messageId=${messageId}: ${err.message} – ritorno in coda`);
        batchItemFailures.push({ itemIdentifier: messageId });
      } else {
        // 4xx non recuperabile: log e scarto (nessun retry)
        console.error(`[CONSUMER] errore non recuperabile messageId=${messageId} status=${err.statusCode}: ${err.message} – scartato`);
      }
    }
  }

  return { batchItemFailures };
};
