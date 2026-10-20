import { Kafka, logLevel } from 'kafkajs';
import { config } from './config.js';

export function createKafka(clientIdSuffix) {
  return new Kafka({
    clientId: `${config.kafka.clientId}-${clientIdSuffix}`,
    brokers: config.kafka.brokers,
    logLevel: logLevel.WARN,
    retry: { initialRetryTime: 300, retries: 10 },
  });
}

/** Create the raw + DLQ topics if they don't exist yet. Safe to call on every boot. */
export async function ensureTopics(kafka, log) {
  const admin = kafka.admin();
  await admin.connect();
  try {
    const existing = new Set(await admin.listTopics());
    const wanted = [
      { topic: config.kafka.rawTopic, numPartitions: config.kafka.partitions },
      { topic: config.kafka.dlqTopic, numPartitions: 1 },
    ].filter((t) => !existing.has(t.topic));
    if (wanted.length) {
      await admin.createTopics({ topics: wanted, waitForLeaders: true });
      log.info({ topics: wanted.map((t) => t.topic) }, 'created kafka topics');
    }
  } finally {
    await admin.disconnect();
  }
}
