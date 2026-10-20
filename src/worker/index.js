// Processing worker: Kafka consumer group -> transform -> batched ClickHouse inserts.
//
// Delivery guarantee: at-least-once. Offsets are committed only after the rows from
// those messages are safely in ClickHouse. If the worker crashes before committing,
// the messages are redelivered and the duplicate rows are collapsed by the
// ReplacingMergeTree engine (same event_id).
//
// Run more copies to scale out: Kafka spreads partitions across the consumer group.

import http from 'node:http';
import { config } from '../shared/config.js';
import { createKafka, ensureTopics } from '../shared/kafka.js';
import { createClickHouse } from '../shared/clickhouse.js';
import { createLogger } from '../shared/logger.js';
import { createRegistry } from '../shared/metrics.js';
import { Batcher } from '../shared/batcher.js';
import { withRetry } from '../shared/retry.js';
import { messageToRow, PoisonMessageError } from './transform.js';

const log = createLogger('worker');

async function main() {
  const kafka = createKafka('worker');
  await ensureTopics(kafka, log);
  const clickhouse = createClickHouse();

  // --- metrics -------------------------------------------------------------
  const { registry, client } = createRegistry('worker');
  const consumed = new client.Counter({
    name: 'worker_messages_total', help: 'Messages consumed, by outcome',
    labelNames: ['result'], registers: [registry],
  });
  const insertSeconds = new client.Histogram({
    name: 'worker_insert_seconds', help: 'ClickHouse insert latency per batch',
    buckets: [0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10], registers: [registry],
  });
  const insertRows = new client.Histogram({
    name: 'worker_insert_rows', help: 'Rows per ClickHouse insert',
    buckets: [100, 1000, 5000, 10000, 25000, 50000], registers: [registry],
  });
  const insertRetries = new client.Counter({
    name: 'worker_insert_retries_total', help: 'ClickHouse insert retries', registers: [registry],
  });
  const lagGauge = new client.Gauge({
    name: 'worker_consumer_lag', help: 'Messages behind the head of the partition',
    labelNames: ['partition'], registers: [registry],
  });

  // --- ClickHouse sink -----------------------------------------------------
  const insert = async (rows) => {
    const end = insertSeconds.startTimer();
    await withRetry(
      () => clickhouse.insert({ table: 'events', values: rows, format: 'JSONEachRow' }),
      {
        retries: config.worker.maxRetries,
        baseMs: config.worker.retryBaseMs,
        onRetry: (err, attempt, delay) => {
          insertRetries.inc();
          log.warn({ err: err.message, attempt, delay, rows: rows.length }, 'clickhouse insert failed, retrying');
        },
      },
    );
    end();
    insertRows.observe(rows.length);
  };

  // Rows from every partition this worker owns are merged into one insert.
  // ClickHouse strongly prefers few large inserts over many small ones.
  const sink = new Batcher({
    send: insert,
    batchSize: config.worker.batchSize,
    flushMs: config.worker.flushIntervalMs,
    maxPending: config.worker.batchSize * 20,
    maxInFlight: 2,
  });

  // --- DLQ -----------------------------------------------------------------
  const dlq = kafka.producer({ idempotent: true, maxInFlightRequests: 1 });
  await dlq.connect();

  // --- consumer ------------------------------------------------------------
  const consumer = kafka.consumer({
    groupId: config.kafka.consumerGroup,
    sessionTimeout: 30_000,
    heartbeatInterval: 3_000,
    maxBytesPerPartition: 4 * 1024 * 1024,
  });
  await consumer.connect();
  await consumer.subscribe({ topic: config.kafka.rawTopic, fromBeginning: true });

  await consumer.run({
    autoCommit: true,
    eachBatchAutoResolve: false, // we resolve offsets ourselves, after the insert
    partitionsConsumedConcurrently: 4,
    eachBatch: async ({ batch, resolveOffset, heartbeat, commitOffsetsIfNecessary, isRunning, isStale }) => {
      if (!isRunning() || isStale()) return;
      const source = { topic: batch.topic, partition: batch.partition };
      const rows = [];
      const dead = [];

      for (const message of batch.messages) {
        try {
          rows.push(messageToRow(message, source));
        } catch (err) {
          if (!(err instanceof PoisonMessageError)) throw err;
          dead.push({
            value: message.value,
            headers: {
              'x-error': err.message,
              'x-source-partition': String(batch.partition),
              'x-source-offset': message.offset,
            },
          });
        }
      }

      if (dead.length) {
        await dlq.send({ topic: config.kafka.dlqTopic, acks: -1, messages: dead });
        consumed.inc({ result: 'dead_lettered' }, dead.length);
        log.warn({ count: dead.length, partition: batch.partition }, 'sent poison messages to DLQ');
      }

      if (rows.length) {
        // Keep the group membership alive while waiting for a (possibly slow) insert.
        const beat = setInterval(() => heartbeat().catch(() => {}), 3_000);
        try {
          await sink.enqueue(rows);
        } finally {
          clearInterval(beat);
        }
        consumed.inc({ result: 'inserted' }, rows.length);
      }

      // Only now is it safe to move the offset forward.
      resolveOffset(batch.lastOffset());
      await commitOffsetsIfNecessary();
      const lag = Number(batch.offsetLag());
      if (Number.isFinite(lag)) lagGauge.set({ partition: String(batch.partition) }, lag);
    },
  });

  // --- metrics / health endpoint ------------------------------------------
  const server = http.createServer(async (req, res) => {
    if (req.url === '/metrics') {
      res.setHeader('Content-Type', registry.contentType);
      res.end(await registry.metrics());
    } else if (req.url === '/healthz') {
      res.end('ok');
    } else {
      res.statusCode = 404;
      res.end();
    }
  });
  server.listen(config.worker.metricsPort, () => log.info({ port: config.worker.metricsPort }, 'worker metrics listening'));

  log.info({ group: config.kafka.consumerGroup, topic: config.kafka.rawTopic }, 'worker consuming');

  let stopping = false;
  const shutdown = async (signal) => {
    if (stopping) return;
    stopping = true;
    log.info({ signal }, 'shutting down worker');
    await consumer.disconnect(); // waits for the current eachBatch (and its insert) to finish
    await sink.close();
    await dlq.disconnect();
    await clickhouse.close();
    server.close();
    process.exit(0);
  };
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
}

main().catch((err) => {
  log.fatal({ err }, 'worker failed to start');
  process.exit(1);
});
