'use strict';

const { SQSClient, SendMessageBatchCommand } = require('@aws-sdk/client-sqs');

const client = new SQSClient({ region: process.env.AWS_REGION || 'eu-west-1' });

const SQS_BATCH_SIZE = 10; // limite AWS fisso per sendMessageBatch

/**
 * Pubblica un array di record su SQS in batch da 10.
 * Lancia un errore se anche un solo messaggio del batch fallisce.
 */
async function publishBatch(records, queueUrl) {
  const chunks = chunkArray(records, SQS_BATCH_SIZE);

  for (const chunk of chunks) {
    const entries = chunk.map((record, i) => ({
      Id: String(i),
      MessageBody: JSON.stringify(record),
    }));

    const response = await client.send(
      new SendMessageBatchCommand({ QueueUrl: queueUrl, Entries: entries })
    );

    if (response.Failed?.length > 0) {
      console.error('[SQS] messaggi falliti nel batch:', JSON.stringify(response.Failed));
      throw new Error(`sendMessageBatch: ${response.Failed.length} messaggi falliti`);
    }
  }
}

function chunkArray(arr, size) {
  const result = [];
  for (let i = 0; i < arr.length; i += size) {
    result.push(arr.slice(i, i + size));
  }
  return result;
}

module.exports = { publishBatch };
