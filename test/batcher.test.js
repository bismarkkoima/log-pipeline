import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Batcher, BackpressureError } from '../src/shared/batcher.js';

const items = (n, tag = 'x') => Array.from({ length: n }, (_, i) => `${tag}${i}`);

test('coalesces concurrent callers into one batch', async () => {
  const sent = [];
  const b = new Batcher({ send: async (m) => { sent.push(m); }, batchSize: 100, flushMs: 10, maxPending: 1000 });
  await Promise.all([b.enqueue(items(3, 'a')), b.enqueue(items(4, 'b')), b.enqueue(items(5, 'c'))]);
  assert.equal(sent.length, 1);
  assert.equal(sent[0].length, 12);
});

test('flushes immediately when batchSize is reached', async () => {
  const sent = [];
  const b = new Batcher({ send: async (m) => { sent.push(m.length); }, batchSize: 10, flushMs: 60_000, maxPending: 1000 });
  await b.enqueue(items(10));
  assert.deepEqual(sent, [10]);
});

test('never splits a caller across batches', async () => {
  const sent = [];
  const b = new Batcher({ send: async (m) => { sent.push(m.length); }, batchSize: 10, flushMs: 5, maxPending: 1000 });
  await Promise.all([b.enqueue(items(6)), b.enqueue(items(6)), b.enqueue(items(6))]);
  // 6 + 6 exceeds batchSize (10), so every batch must carry whole callers only.
  for (const size of sent) assert.equal(size % 6, 0);
  assert.equal(sent.reduce((a, b2) => a + b2, 0), 18);
});

test('rejects every caller in a failed batch', async () => {
  const b = new Batcher({ send: async () => { throw new Error('kafka down'); }, batchSize: 100, flushMs: 5, maxPending: 1000 });
  const results = await Promise.allSettled([b.enqueue(items(2)), b.enqueue(items(2))]);
  assert.deepEqual(results.map((r) => r.status), ['rejected', 'rejected']);
  assert.equal(b.pending, 0);
});

test('applies backpressure when the backlog is full', async () => {
  let release;
  const blocked = new Promise((r) => { release = r; });
  const b = new Batcher({ send: () => blocked, batchSize: 5, flushMs: 5, maxPending: 10, maxInFlight: 1 });
  const first = b.enqueue(items(5)); // in flight
  const second = b.enqueue(items(5)); // queued
  await assert.rejects(b.enqueue(items(1)), BackpressureError);
  release();
  await Promise.all([first, second]);
  assert.equal(b.pending, 0);
});

test('close() flushes what is queued and refuses new work', async () => {
  const sent = [];
  const b = new Batcher({ send: async (m) => { sent.push(m.length); }, batchSize: 100, flushMs: 60_000, maxPending: 1000 });
  const p = b.enqueue(items(7));
  await b.close();
  await p;
  assert.deepEqual(sent, [7]);
  await assert.rejects(b.enqueue(items(1)), /shutting down/);
});
