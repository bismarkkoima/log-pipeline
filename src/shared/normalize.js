// Turns a validated producer event into the flat row stored in ClickHouse.
// Pure functions only, so they are easy to unit test.

const MAX_FUTURE_SKEW_MS = 24 * 60 * 60 * 1000; // reject clocks more than a day ahead
const MAX_PAST_AGE_MS = 30 * 24 * 60 * 60 * 1000; // older than retention is meaningless

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const NUMERIC_RE = /^\d+$/;
const HEX_ID_RE = /^[0-9a-f]{16,}$/i;

/** Format epoch ms as ClickHouse DateTime64(3) text in UTC: 2026-10-20 18:30:01.123 */
export function toClickHouseTime(ms) {
  return new Date(ms).toISOString().replace('T', ' ').replace('Z', '');
}

/**
 * Parse a producer timestamp. Accepts ISO strings, epoch seconds or epoch ms.
 * Falls back to the time the pipeline received it if the value is missing,
 * unparseable, or implausibly far in the future/past.
 */
export function parseTimestamp(value, receivedAtMs) {
  let ms;
  if (typeof value === 'number' && Number.isFinite(value)) {
    ms = value < 1e12 ? value * 1000 : value; // seconds vs milliseconds
  } else if (typeof value === 'string') {
    ms = Date.parse(value);
  }
  if (ms === undefined || Number.isNaN(ms)) return { ms: receivedAtMs, corrected: value !== undefined };
  if (ms - receivedAtMs > MAX_FUTURE_SKEW_MS) return { ms: receivedAtMs, corrected: true };
  if (receivedAtMs - ms > MAX_PAST_AGE_MS) return { ms: receivedAtMs, corrected: true };
  return { ms: Math.round(ms), corrected: false };
}

/**
 * Collapse high-cardinality path segments so analytics can group by route:
 *   /users/123/orders/9f2c...  ->  /users/:id/orders/:id
 * Query strings are dropped.
 */
export function normalizeRoute(path) {
  if (!path) return '';
  const clean = path.split('?')[0].split('#')[0];
  return clean
    .split('/')
    .map((seg) => (NUMERIC_RE.test(seg) || UUID_RE.test(seg) || HEX_ID_RE.test(seg) ? ':id' : seg))
    .join('/');
}

/** Derive a level from the HTTP status when the producer didn't send one. */
export function inferLevel(event) {
  if (event.level) return event.level;
  const status = event.http?.status;
  if (status >= 500) return 'error';
  if (status >= 400) return 'warn';
  return 'info';
}

function stringifyAttributes(attrs) {
  const out = {};
  if (!attrs) return out;
  for (const [k, v] of Object.entries(attrs)) out[k] = String(v);
  return out;
}

/**
 * @param {object} event   validated producer event (must already have `id`)
 * @param {object} meta    { receivedAtMs, sourceIp }
 */
export function normalizeEvent(event, meta) {
  const { ms, corrected } = parseTimestamp(event.timestamp, meta.receivedAtMs);
  const attributes = stringifyAttributes(event.attributes);
  if (corrected) attributes._timestamp_corrected = 'true';

  return {
    event_id: event.id,
    timestamp: toClickHouseTime(ms),
    received_at: toClickHouseTime(meta.receivedAtMs),
    service: event.service,
    level: inferLevel(event),
    message: event.message ?? '',
    host: event.host ?? '',
    trace_id: event.trace_id ?? '',
    http_method: (event.http?.method ?? '').toUpperCase(),
    http_path: event.http?.path ?? '',
    http_route: normalizeRoute(event.http?.path),
    http_status: event.http?.status ?? 0,
    duration_ms: event.http?.duration_ms ?? 0,
    source_ip: meta.sourceIp ?? '',
    attributes,
  };
}
