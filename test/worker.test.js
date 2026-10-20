import { test } from 'node:test';
import assert from 'node:assert/strict';
import { messageToRow, PoisonMessageError } from '../src/worker/transform.js';
import { withRetry } from '../src/shared/retry.js';
import { splitStatements } from '../scripts/migrate.js';

const source = { topic: 'logs.raw', partition: 3 };
const msg = (value, offset = '42') => ({ value: Buffer.from(typeof value === 'string' ? value : JSON.stringify(value)), offset });

test('messageToRow normalizes a valid envelope', () => {
  const row = messageToRow(msg({ e: { id: 'x1', service: 'auth', message: 'login', http: { status: 500 } }, r: Date.now(), ip: '1.2.3.4' }), source);
  assert.equal(row.event_id, 'x1');
  assert.equal(row.level, 'error');
  assert.equal(row.source_ip, '1.2.3.4');
});

test('messageToRow derives a deterministic id when none was assigned', () => {
  const row = messageToRow(msg({ e: { service: 'auth', message: 'x' }, r: Date.now() }, '777'), source);
  assert.equal(row.event_id, 'logs.raw-3-777');
});

test('poison messages are classified for the DLQ', () => {
  assert.throws(() => messageToRow({ value: null, offset: '1' }, source), PoisonMessageError);
  assert.throws(() => messageToRow(msg('{not json'), source), PoisonMessageError);
  assert.throws(() => messageToRow(msg({ e: { message: 'no service' } }), source), PoisonMessageError);
  assert.throws(() => messageToRow(msg({ hello: 'world' }), source), PoisonMessageError);
});

test('withRetry retries then succeeds', async () => {
  let calls = 0;
  const result = await withRetry(async () => { calls += 1; if (calls < 3) throw new Error('flaky'); return 'ok'; }, { retries: 5, baseMs: 1, sleep: async () => {} });
  assert.equal(result, 'ok');
  assert.equal(calls, 3);
});

test('withRetry gives up after the retry budget', async () => {
  let calls = 0;
  await assert.rejects(withRetry(async () => { calls += 1; throw new Error('down'); }, { retries: 2, baseMs: 1, sleep: async () => {} }), /down/);
  assert.equal(calls, 3);
});

test('migration splitter handles comments and multiple statements', () => {
  const sql = `-- comment; with semicolon\nCREATE TABLE a (x UInt8) ENGINE = Memory;\n\nCREATE VIEW b AS SELECT 1;\n`;
  assert.deepEqual(splitStatements(sql), ['CREATE TABLE a (x UInt8) ENGINE = Memory', 'CREATE VIEW b AS SELECT 1']);
});
