import { test } from 'node:test';
import assert from 'node:assert/strict';
import { gzipSync } from 'node:zlib';
import { buildIngestApp } from '../src/ingest/app.js';
import { BackpressureError } from '../src/shared/batcher.js';

function setup({ enqueue } = {}) {
  const enqueued = [];
  const producer = {
    pending: 0,
    enqueue: enqueue ?? (async (messages) => { enqueued.push(...messages.map((m) => JSON.parse(m.value))); }),
  };
  const app = buildIngestApp({ producer, apiKeys: ['secret'], maxBodyBytes: 1024 * 1024, maxEventsPerRequest: 100 });
  return { app, enqueued };
}

const headers = { 'x-api-key': 'secret', 'content-type': 'application/json' };
const ok = { service: 'orders', message: 'hello' };

test('rejects requests without a valid API key', async () => {
  const { app } = setup();
  const res = await app.inject({ method: 'POST', url: '/v1/logs', payload: [ok], headers: { 'x-api-key': 'wrong' } });
  assert.equal(res.statusCode, 401);
});

test('accepts a JSON array, assigns ids and stamps receive time', async () => {
  const { app, enqueued } = setup();
  const res = await app.inject({ method: 'POST', url: '/v1/logs', payload: [ok, { ...ok, id: 'mine' }], headers });
  assert.equal(res.statusCode, 202);
  assert.deepEqual(res.json(), { accepted: 2, rejected: 0, errors: [] });
  assert.equal(enqueued.length, 2);
  assert.match(enqueued[0].e.id, /^[0-9a-f-]{36}$/);
  assert.equal(enqueued[1].e.id, 'mine');
  assert.ok(Number.isFinite(enqueued[0].r));
});

test('accepts { events: [...] } and single objects', async () => {
  const { app } = setup();
  const a = await app.inject({ method: 'POST', url: '/v1/logs', payload: { events: [ok] }, headers });
  const b = await app.inject({ method: 'POST', url: '/v1/logs', payload: ok, headers });
  assert.equal(a.json().accepted, 1);
  assert.equal(b.json().accepted, 1);
});

test('partial acceptance: invalid events are reported, valid ones kept', async () => {
  const { app, enqueued } = setup();
  const res = await app.inject({
    method: 'POST', url: '/v1/logs', headers,
    payload: [ok, { message: 'no service' }, { ...ok, level: 'loud' }],
  });
  assert.equal(res.statusCode, 202);
  const body = res.json();
  assert.equal(body.accepted, 1);
  assert.equal(body.rejected, 2);
  assert.deepEqual(body.errors.map((e) => e.index), [1, 2]);
  assert.equal(enqueued.length, 1);
});

test('all-invalid batch returns 400', async () => {
  const { app } = setup();
  const res = await app.inject({ method: 'POST', url: '/v1/logs', payload: [{ nope: true }], headers });
  assert.equal(res.statusCode, 400);
});

test('gzipped NDJSON with a broken line', async () => {
  const { app, enqueued } = setup();
  const body = gzipSync(`${JSON.stringify(ok)}\n{broken\n${JSON.stringify(ok)}\n`);
  const res = await app.inject({
    method: 'POST', url: '/v1/logs', payload: body,
    headers: { 'x-api-key': 'secret', 'content-type': 'application/x-ndjson', 'content-encoding': 'gzip' },
  });
  assert.equal(res.statusCode, 202);
  assert.deepEqual(res.json(), { accepted: 2, rejected: 1, errors: [{ index: 1, error: 'invalid JSON' }] });
  assert.equal(enqueued.length, 2);
});

test('too many events per request returns 413', async () => {
  const { app } = setup();
  const res = await app.inject({ method: 'POST', url: '/v1/logs', payload: Array(101).fill(ok), headers });
  assert.equal(res.statusCode, 413);
});

test('backpressure becomes 503 with Retry-After', async () => {
  const { app } = setup({ enqueue: async () => { throw new BackpressureError(999); } });
  const res = await app.inject({ method: 'POST', url: '/v1/logs', payload: [ok], headers });
  assert.equal(res.statusCode, 503);
  assert.equal(res.headers['retry-after'], '1');
});

test('kafka failure becomes 503 so the client retries (no silent loss)', async () => {
  const { app } = setup({ enqueue: async () => { throw new Error('broker down'); } });
  const res = await app.inject({ method: 'POST', url: '/v1/logs', payload: [ok], headers });
  assert.equal(res.statusCode, 503);
  assert.equal(res.headers['retry-after'], '5');
});
