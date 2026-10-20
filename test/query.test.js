import { test } from 'node:test';
import assert from 'node:assert/strict';
import { buildQueryApp } from '../src/query/app.js';
import { bucketMinutesFor, clampMinutes } from '../src/query/queries.js';

function fakeClickHouse(responder) {
  const calls = [];
  return {
    calls,
    async query(opts) {
      calls.push(opts);
      return { json: async () => responder(opts) };
    },
  };
}

test('clampMinutes bounds the window', () => {
  assert.equal(clampMinutes('15'), 15);
  assert.equal(clampMinutes('-3'), 60);
  assert.equal(clampMinutes('abc'), 60);
  assert.equal(clampMinutes('999999'), 10080);
});

test('bucketMinutesFor keeps charts readable', () => {
  assert.equal(bucketMinutesFor(60), 1);
  assert.equal(bucketMinutesFor(360), 5);
  assert.equal(bucketMinutesFor(1440), 15);
  assert.equal(bucketMinutesFor(10080), 180);
});

test('overview converts ClickHouse strings to numbers and computes rates', async () => {
  const ch = fakeClickHouse(({ query }) =>
    query.includes('received_at')
      ? [{ events: '10', lag_ms: [120, 900], last_insert: '2026-10-20 18:00:00.000' }]
      : [{ total_events: '1000', error_events: '50', total_requests: '800', total_5xx: '8', total_duration: '16000', latency: [10, 40, 95.123], services: '3' }],
  );
  const app = buildQueryApp({ clickhouse: ch, maxRows: 1000 });
  const res = await app.inject('/api/overview?minutes=10&service=orders');
  const body = res.json();
  assert.equal(body.events, 1000);
  assert.equal(body.error_rate, 5);
  assert.equal(body.http_5xx_rate, 1);
  assert.equal(body.avg_latency_ms, 20);
  assert.deepEqual(body.latency_ms, { p50: 10, p95: 40, p99: 95.1 });
  assert.equal(body.pipeline.lag_p99_ms, 900);
  // user input goes through typed parameters, never into the SQL text
  const params = ch.calls.find((c) => !c.query.includes('received_at')).query_params;
  assert.deepEqual(params, { minutes: 10, service: 'orders' });
});

test('log search validates level and caps limit', async () => {
  const ch = fakeClickHouse(() => []);
  const app = buildQueryApp({ clickhouse: ch, maxRows: 50 });
  assert.equal((await app.inject('/api/logs?level=nope')).statusCode, 400);
  await app.inject("/api/logs?q=drop%20table'--&limit=5000");
  const params = ch.calls.at(-1).query_params;
  assert.equal(params.limit, 50);
  assert.equal(params.q, "drop table'--");
  assert.ok(!ch.calls.at(-1).query.includes('drop table'));
});

test('database errors become a 502, not a crash', async () => {
  const app = buildQueryApp({ clickhouse: { query: async () => { throw new Error('boom'); } }, maxRows: 10 });
  const res = await app.inject('/api/services');
  assert.equal(res.statusCode, 502);
});
