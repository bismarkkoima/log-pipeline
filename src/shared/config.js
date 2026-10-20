// Central configuration. Every value can be overridden with an environment variable.

const int = (name, fallback) => {
  const raw = process.env[name];
  if (raw === undefined || raw === '') return fallback;
  const n = Number.parseInt(raw, 10);
  if (Number.isNaN(n)) throw new Error(`Env ${name} must be an integer, got "${raw}"`);
  return n;
};

const str = (name, fallback) => process.env[name] ?? fallback;

export const config = {
  serviceName: str('SERVICE_NAME', 'log-pipeline'),
  logLevel: str('LOG_LEVEL', 'info'),

  kafka: {
    brokers: str('KAFKA_BROKERS', 'localhost:19092').split(',').map((b) => b.trim()),
    clientId: str('KAFKA_CLIENT_ID', 'log-pipeline'),
    rawTopic: str('KAFKA_RAW_TOPIC', 'logs.raw'),
    dlqTopic: str('KAFKA_DLQ_TOPIC', 'logs.dlq'),
    partitions: int('KAFKA_PARTITIONS', 12),
    consumerGroup: str('KAFKA_CONSUMER_GROUP', 'log-workers'),
  },

  clickhouse: {
    url: str('CLICKHOUSE_URL', 'http://localhost:8123'),
    username: str('CLICKHOUSE_USER', 'pipeline'),
    password: str('CLICKHOUSE_PASSWORD', 'pipeline'),
    database: str('CLICKHOUSE_DB', 'logs'),
  },

  ingest: {
    port: int('INGEST_PORT', 8080),
    apiKeys: str('INGEST_API_KEYS', 'dev-key').split(',').map((k) => k.trim()).filter(Boolean),
    maxBodyBytes: int('INGEST_MAX_BODY_BYTES', 5 * 1024 * 1024),
    maxEventsPerRequest: int('INGEST_MAX_EVENTS', 5000),
    // Backpressure: reject new requests once this many events are waiting to be flushed to Kafka.
    maxPendingEvents: int('INGEST_MAX_PENDING', 200_000),
    producerFlushMs: int('INGEST_FLUSH_MS', 50),
    producerBatchSize: int('INGEST_PRODUCER_BATCH', 2000),
  },

  worker: {
    batchSize: int('WORKER_BATCH_SIZE', 10_000),
    flushIntervalMs: int('WORKER_FLUSH_MS', 1000),
    maxRetries: int('WORKER_MAX_RETRIES', 5),
    retryBaseMs: int('WORKER_RETRY_BASE_MS', 200),
    metricsPort: int('WORKER_METRICS_PORT', 9100),
  },

  query: {
    port: int('QUERY_PORT', 8081),
    maxRows: int('QUERY_MAX_ROWS', 1000),
  },
};
