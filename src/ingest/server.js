import { CompressionTypes } from 'kafkajs';
import { config } from '../shared/config.js';
import { createKafka, ensureTopics } from '../shared/kafka.js';
import { createLogger } from '../shared/logger.js';
import { createRegistry } from '../shared/metrics.js';
import { buildIngestApp } from './app.js';
import { Batcher } from '../shared/batcher.js';

const log = createLogger('ingest');

async function main() {
  const kafka = createKafka('ingest');
  await ensureTopics(kafka, log);

  const kafkaProducer = kafka.producer({
    idempotent: true, // no duplicates from producer-side retries
    maxInFlightRequests: 5,
    allowAutoTopicCreation: false,
  });
  await kafkaProducer.connect();
  let ready = true;
  kafkaProducer.on(kafkaProducer.events.DISCONNECT, () => { ready = false; });
  kafkaProducer.on(kafkaProducer.events.CONNECT, () => { ready = true; });

  const { registry, client } = createRegistry('ingest');
  const metrics = {
    registry,
    events: new client.Counter({
      name: 'ingest_events_total',
      help: 'Events received by the ingest API, by outcome',
      labelNames: ['result'],
      registers: [registry],
    }),
    batchSize: new client.Histogram({
      name: 'ingest_kafka_batch_size',
      help: 'Events per Kafka batch',
      buckets: [10, 100, 500, 1000, 2000, 5000, 10000],
      registers: [registry],
    }),
    batchLatency: new client.Histogram({
      name: 'ingest_kafka_batch_seconds',
      help: 'Time for Kafka to acknowledge a batch',
      labelNames: ['ok'],
      buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5],
      registers: [registry],
    }),
  };

  const producer = new Batcher({
    batchSize: config.ingest.producerBatchSize,
    flushMs: config.ingest.producerFlushMs,
    maxPending: config.ingest.maxPendingEvents,
    send: (messages) =>
      kafkaProducer.send({
        topic: config.kafka.rawTopic,
        acks: -1, // wait for all in-sync replicas
        compression: CompressionTypes.GZIP,
        messages,
      }),
  });
  producer.onBatch = ({ size, ms, ok }) => {
    metrics.batchSize.observe(size);
    metrics.batchLatency.observe({ ok: String(ok) }, ms / 1000);
  };

  new client.Gauge({
    name: 'ingest_pending_events',
    help: 'Events waiting for Kafka acknowledgement',
    registers: [registry],
    collect() { this.set(producer.pending); },
  });

  const app = buildIngestApp({
    producer,
    apiKeys: config.ingest.apiKeys,
    maxBodyBytes: config.ingest.maxBodyBytes,
    maxEventsPerRequest: config.ingest.maxEventsPerRequest,
    metrics,
    isReady: () => ready,
    logger: { level: config.logLevel },
  });

  await app.listen({ host: '0.0.0.0', port: config.ingest.port });
  log.info({ port: config.ingest.port }, 'ingest api listening');

  let stopping = false;
  const shutdown = async (signal) => {
    if (stopping) return;
    stopping = true;
    log.info({ signal }, 'shutting down: draining requests and flushing to kafka');
    ready = false;
    await app.close(); // stop accepting, finish in-flight requests
    await producer.close(); // flush anything queued
    await kafkaProducer.disconnect();
    process.exit(0);
  };
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
}

main().catch((err) => {
  log.fatal({ err }, 'ingest failed to start');
  process.exit(1);
});
