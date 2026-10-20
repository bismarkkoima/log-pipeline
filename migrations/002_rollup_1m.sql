-- Per-minute rollup maintained automatically on every insert into `events`.
-- Dashboards read from here instead of scanning raw rows, so they stay fast
-- no matter how much raw data there is. Kept 6x longer than raw logs.
CREATE TABLE IF NOT EXISTS events_1m
(
    minute        DateTime('UTC'),
    service       LowCardinality(String),
    level         LowCardinality(String),
    http_route    LowCardinality(String),
    events        SimpleAggregateFunction(sum, UInt64),
    http_requests SimpleAggregateFunction(sum, UInt64),
    http_5xx      SimpleAggregateFunction(sum, UInt64),
    duration_sum  SimpleAggregateFunction(sum, Float64),
    duration_q    AggregateFunction(quantilesTDigestIf(0.5, 0.95, 0.99), Float32, UInt8)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(minute)
ORDER BY (service, minute, level, http_route)
TTL minute + INTERVAL 180 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS events_1m_mv TO events_1m AS
SELECT
    toStartOfMinute(timestamp)                         AS minute,
    service,
    level,
    http_route,
    toUInt64(count())                                  AS events,
    toUInt64(countIf(http_status > 0))                 AS http_requests,
    toUInt64(countIf(http_status >= 500))              AS http_5xx,
    toFloat64(sum(duration_ms))                        AS duration_sum,
    quantilesTDigestIfState(0.5, 0.95, 0.99)(duration_ms, toUInt8(http_status > 0)) AS duration_q
FROM events
GROUP BY minute, service, level, http_route;
