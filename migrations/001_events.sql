-- Raw log events. One row per event.
--
-- * ReplacingMergeTree + event_id in the sort key: the pipeline is at-least-once,
--   so a worker crash can re-insert a batch. Duplicates share the same key and are
--   collapsed during background merges (use FINAL for exact counts on raw data).
-- * Partitioned by day so TTL drops whole partitions cheaply.
-- * Sort key starts with service/level because almost every query filters on them.
CREATE TABLE IF NOT EXISTS events
(
    event_id     String,
    timestamp    DateTime64(3, 'UTC'),
    received_at  DateTime64(3, 'UTC'),
    service      LowCardinality(String),
    level        LowCardinality(String),
    message      String CODEC(ZSTD(3)),
    host         LowCardinality(String),
    trace_id     String,
    http_method  LowCardinality(String),
    http_path    String CODEC(ZSTD(3)),
    http_route   LowCardinality(String),
    http_status  UInt16,
    duration_ms  Float32,
    source_ip    String,
    attributes   Map(LowCardinality(String), String),
    inserted_at  DateTime64(3, 'UTC') DEFAULT now64(3),

    INDEX idx_message  message  TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 4,
    INDEX idx_trace    trace_id TYPE bloom_filter(0.01)      GRANULARITY 4
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toDate(timestamp)
ORDER BY (service, level, timestamp, event_id)
TTL toDateTime(timestamp) + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;
