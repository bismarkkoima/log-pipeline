// All SQL lives here. Every user-supplied value goes through ClickHouse's typed
// query parameters ({name:Type}), never string concatenation.
//
// Output aliases never reuse a column name: ClickHouse resolves aliases first, so
// `sum(events) AS events` would break any later `sumIf(events, ...)`.
//
// Aggregate endpoints read the per-minute rollup (events_1m), so they cost the same
// whether the raw table holds a million rows or ten billion.

const SERVICE_FILTER = `({service:String} = '' OR service = {service:String})`;

export const MAX_WINDOW_MINUTES = 7 * 24 * 60;

export function clampMinutes(raw, fallback = 60) {
  const n = Number.parseInt(raw, 10);
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.min(n, MAX_WINDOW_MINUTES);
}

/** Pick a bucket size that keeps charts around ~60-120 points. */
export function bucketMinutesFor(windowMinutes) {
  const steps = [1, 5, 15, 30, 60, 180, 360];
  return steps.find((s) => windowMinutes / s <= 120) ?? 360;
}

export const overviewSql = `
SELECT
    sum(events)                                         AS total_events,
    sumIf(events, level IN ('error', 'fatal'))          AS error_events,
    sum(http_requests)                                  AS total_requests,
    sum(http_5xx)                                       AS total_5xx,
    sum(duration_sum)                                   AS total_duration,
    quantilesTDigestIfMerge(0.5, 0.95, 0.99)(duration_q) AS latency,
    uniqExact(service)                                  AS services
FROM events_1m
WHERE minute >= toStartOfMinute(now() - INTERVAL {minutes:UInt32} MINUTE)
  AND ${SERVICE_FILTER}`;

export const timeseriesSql = `
SELECT
    toStartOfInterval(minute, INTERVAL {bucket:UInt32} MINUTE) AS t,
    sum(events)                                         AS total_events,
    sumIf(events, level IN ('error', 'fatal'))          AS error_events,
    sum(http_5xx)                                       AS total_5xx,
    quantilesTDigestIfMerge(0.5, 0.95, 0.99)(duration_q) AS latency
FROM events_1m
WHERE minute >= toStartOfMinute(now() - INTERVAL {minutes:UInt32} MINUTE)
  AND ${SERVICE_FILTER}
GROUP BY t
ORDER BY t
WITH FILL STEP toIntervalMinute({bucket:UInt32})`;

export const servicesSql = `
SELECT
    service,
    sum(events)                                AS total_events,
    sumIf(events, level IN ('error', 'fatal')) AS error_events,
    sum(http_requests)                         AS total_requests,
    quantilesTDigestIfMerge(0.95)(duration_q)[1] AS p95_ms
FROM events_1m
WHERE minute >= toStartOfMinute(now() - INTERVAL {minutes:UInt32} MINUTE)
GROUP BY service
ORDER BY total_events DESC
LIMIT 100`;

export const topRoutesSql = `
SELECT
    service,
    http_route                                 AS route,
    sum(http_requests)                         AS requests,
    sum(http_5xx)                              AS total_5xx,
    quantilesTDigestIfMerge(0.5, 0.95, 0.99)(duration_q) AS latency
FROM events_1m
WHERE minute >= toStartOfMinute(now() - INTERVAL {minutes:UInt32} MINUTE)
  AND ${SERVICE_FILTER}
  AND http_route != ''
GROUP BY service, route
ORDER BY requests DESC
LIMIT {limit:UInt32}`;

// Raw search over the events table. Filtering on service/level/time hits the sort
// key; message search uses the token bloom-filter index to skip granules.
export const searchSql = `
SELECT
    event_id, timestamp, service, level, message, host, trace_id,
    http_method, http_path, http_status, duration_ms, attributes
FROM events
WHERE timestamp >= now64(3) - INTERVAL {minutes:UInt32} MINUTE
  AND ${SERVICE_FILTER}
  AND ({level:String} = '' OR level = {level:String})
  AND ({trace_id:String} = '' OR trace_id = {trace_id:String})
  AND ({q:String} = '' OR positionCaseInsensitive(message, {q:String}) > 0)
  AND ({min_status:UInt16} = 0 OR http_status >= {min_status:UInt16})
ORDER BY timestamp DESC
LIMIT {limit:UInt32}`;

// End-to-end pipeline latency: time from the ingest API receiving an event to it
// being queryable in ClickHouse.
export const freshnessSql = `
SELECT
    count()                                                             AS events,
    quantiles(0.5, 0.99)(dateDiff('millisecond', received_at, inserted_at)) AS lag_ms,
    max(inserted_at)                                                    AS last_insert
FROM events
WHERE timestamp >= now64(3) - INTERVAL 15 MINUTE
  AND inserted_at >= now64(3) - INTERVAL 5 MINUTE`;
