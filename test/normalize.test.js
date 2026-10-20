import { test } from 'node:test';
import assert from 'node:assert/strict';
import { inferLevel, normalizeEvent, normalizeRoute, parseTimestamp, toClickHouseTime } from '../src/shared/normalize.js';

const NOW = Date.parse('2026-10-20T18:00:00.000Z');

test('parseTimestamp accepts ISO strings, epoch seconds and epoch ms', () => {
  assert.equal(parseTimestamp('2026-10-20T17:59:00Z', NOW).ms, NOW - 60_000);
  assert.equal(parseTimestamp(NOW / 1000 - 5, NOW).ms, NOW - 5000);
  assert.equal(parseTimestamp(NOW - 5000, NOW).ms, NOW - 5000);
});

test('parseTimestamp falls back to receive time for missing, garbage or skewed values', () => {
  assert.deepEqual(parseTimestamp(undefined, NOW), { ms: NOW, corrected: false });
  assert.deepEqual(parseTimestamp('not a date', NOW), { ms: NOW, corrected: true });
  assert.deepEqual(parseTimestamp(NOW + 3 * 86_400_000, NOW), { ms: NOW, corrected: true });
  assert.deepEqual(parseTimestamp(NOW - 60 * 86_400_000, NOW), { ms: NOW, corrected: true });
});

test('toClickHouseTime formats UTC DateTime64(3)', () => {
  assert.equal(toClickHouseTime(NOW + 123), '2026-10-20 18:00:00.123');
});

test('normalizeRoute collapses ids and strips query strings', () => {
  assert.equal(normalizeRoute('/users/123/orders/987?x=1'), '/users/:id/orders/:id');
  assert.equal(normalizeRoute('/sessions/0b8f3a4e-9c1d-4f3e-8a2b-1234567890ab'), '/sessions/:id');
  assert.equal(normalizeRoute('/blobs/9f86d081884c7d659a2feaa0c55ad015'), '/blobs/:id');
  assert.equal(normalizeRoute('/health'), '/health');
  assert.equal(normalizeRoute(undefined), '');
});

test('inferLevel uses explicit level, else HTTP status', () => {
  assert.equal(inferLevel({ level: 'debug', http: { status: 500 } }), 'debug');
  assert.equal(inferLevel({ http: { status: 503 } }), 'error');
  assert.equal(inferLevel({ http: { status: 404 } }), 'warn');
  assert.equal(inferLevel({}), 'info');
});

test('normalizeEvent produces a complete row', () => {
  const row = normalizeEvent(
    {
      id: 'abc', service: 'orders', message: 'ok', timestamp: 'nope',
      http: { method: 'get', path: '/orders/42', status: 200, duration_ms: 12.5 },
      attributes: { retries: 2, cached: true },
    },
    { receivedAtMs: NOW, sourceIp: '10.0.0.1' },
  );
  assert.deepEqual(row, {
    event_id: 'abc',
    timestamp: '2026-10-20 18:00:00.000',
    received_at: '2026-10-20 18:00:00.000',
    service: 'orders',
    level: 'info',
    message: 'ok',
    host: '',
    trace_id: '',
    http_method: 'GET',
    http_path: '/orders/42',
    http_route: '/orders/:id',
    http_status: 200,
    duration_ms: 12.5,
    source_ip: '10.0.0.1',
    attributes: { retries: '2', cached: 'true', _timestamp_corrected: 'true' },
  });
});
