import { validateEvent } from '../shared/schema.js';
import { normalizeEvent } from '../shared/normalize.js';

export class PoisonMessageError extends Error {
  constructor(reason) {
    super(reason);
    this.name = 'PoisonMessageError';
  }
}

/**
 * Convert one Kafka message into a ClickHouse row.
 * Throws PoisonMessageError for anything that can never succeed (bad JSON, bad
 * schema). Those go to the dead-letter topic instead of blocking the partition.
 *
 * @param {{ value: Buffer|string|null, offset: string }} message
 * @param {{ topic: string, partition: number }} source
 */
export function messageToRow(message, source) {
  if (!message.value) throw new PoisonMessageError('empty message');

  let envelope;
  try {
    envelope = JSON.parse(message.value.toString());
  } catch {
    throw new PoisonMessageError('message is not valid JSON');
  }

  const event = envelope?.e;
  // Re-validate: the topic can be written to by things other than our ingest API.
  const error = event ? validateEvent(event) : 'envelope has no event';
  if (error) throw new PoisonMessageError(`schema: ${error}`);

  // Deterministic fallback id: a redelivered message gets the same id, so
  // ReplacingMergeTree still deduplicates it.
  const id = event.id ?? `${source.topic}-${source.partition}-${message.offset}`;
  const receivedAtMs = Number.isFinite(envelope.r) ? envelope.r : Date.now();

  return normalizeEvent({ ...event, id }, { receivedAtMs, sourceIp: envelope.ip });
}
