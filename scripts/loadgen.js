#!/usr/bin/env node
// Synthetic traffic generator + benchmark for the ingest API.
//
//   node scripts/loadgen.js --rate 20000 --duration 60
//   node scripts/loadgen.js --rate 0 --duration 30 --concurrency 32   # flat out
//   node scripts/loadgen.js --rate 5000 --incident                      # payments outage
//
// Sends gzipped NDJSON batches and prints achieved throughput, request latency
// and how many requests the pipeline pushed back on (503 backpressure).

import { gzipSync } from 'node:zlib';
import { randomUUID } from 'node:crypto';
import { parseArgs } from 'node:util';

const { values: args } = parseArgs({
  options: {
    url: { type: 'string', default: process.env.INGEST_URL ?? 'http://localhost:8080/v1/logs' },
    key: { type: 'string', default: process.env.INGEST_API_KEY ?? 'dev-key' },
    rate: { type: 'string', default: '5000' }, // events/sec total, 0 = unlimited
    duration: { type: 'string', default: '30' }, // seconds
    batch: { type: 'string', default: '1000' }, // events per request
    concurrency: { type: 'string', default: '8' },
    incident: { type: 'boolean', default: false }, // simulate a failing payments service
  },
});

const RATE = Number(args.rate);
const DURATION_MS = Number(args.duration) * 1000;
const BATCH = Number(args.batch);
const CONCURRENCY = Number(args.concurrency);

// --- synthetic data --------------------------------------------------------

const SERVICES = {
  'api-gateway': ['/v1/products', '/v1/products/{id}', '/v1/cart', '/v1/checkout', '/v1/users/{id}'],
  orders: ['/orders', '/orders/{id}', '/orders/{id}/items', '/orders/{id}/cancel'],
  payments: ['/payments', '/payments/{id}', '/payments/{id}/refund', '/mpesa/callback'],
  auth: ['/login', '/logout', '/token/refresh', '/users/{id}/sessions'],
  catalog: ['/items', '/items/{id}', '/search', '/categories/{id}'],
};
const SERVICE_NAMES = Object.keys(SERVICES);
const HOSTS = ['node-a', 'node-b', 'node-c', 'node-d'];
const APP_MESSAGES = [
  ['info', 'cache warmed'], ['debug', 'db pool stats'], ['warn', 'slow query detected'],
  ['info', 'job completed'], ['error', 'upstream connection reset'], ['warn', 'retrying request'],
];

const pick = (arr) => arr[Math.floor(Math.random() * arr.length)];
// Rough log-normal: most requests fast, long tail of slow ones.
const latencyMs = (base) => Math.round(base * Math.exp(Math.random() * 1.6 + (Math.random() < 0.02 ? 2 : 0)) * 10) / 10;

function makeEvent(nowMs) {
  const service = pick(SERVICE_NAMES);
  const failing = args.incident && service === 'payments';
  const base = { id: randomUUID(), timestamp: nowMs, service, host: `${service}-${pick(HOSTS)}`, trace_id: randomUUID().slice(0, 16) };

  if (Math.random() < 0.15) {
    const [level, message] = pick(APP_MESSAGES);
    return { ...base, level, message: `${message} (worker=${Math.floor(Math.random() * 8)})` };
  }

  const route = pick(SERVICES[service]);
  const path = route.replace('{id}', () => String(Math.floor(Math.random() * 100000)));
  const r = Math.random();
  const status = failing && r < 0.4 ? 503 : r < 0.005 ? 500 : r < 0.04 ? 404 : r < 0.06 ? 401 : 200;
  const duration = latencyMs(failing ? 400 : 15);
  return {
    ...base,
    message: `${status >= 500 ? 'request failed' : 'request completed'}`,
    http: { method: route.includes('{id}') && Math.random() < 0.7 ? 'GET' : 'POST', path, status, duration_ms: duration },
    attributes: { region: pick(['nairobi', 'mombasa', 'kisumu', 'eldoret']), version: '1.4.2' },
  };
}

function makeBody(n) {
  const now = Date.now();
  let ndjson = '';
  for (let i = 0; i < n; i++) ndjson += JSON.stringify(makeEvent(now)) + '\n';
  return gzipSync(ndjson);
}

// --- runner ----------------------------------------------------------------

const stats = { sent: 0, accepted: 0, rejected: 0, throttled: 0, failed: 0, latencies: [] };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function worker(endAt) {
  const perWorkerRate = RATE > 0 ? RATE / CONCURRENCY : 0;
  while (Date.now() < endAt) {
    const started = Date.now();
    const body = makeBody(BATCH);
    try {
      const t0 = performance.now();
      const res = await fetch(args.url, {
        method: 'POST',
        headers: { 'content-type': 'application/x-ndjson', 'content-encoding': 'gzip', 'x-api-key': args.key },
        body,
      });
      const ms = performance.now() - t0;
      stats.sent += BATCH;
      if (res.status === 202) {
        const json = await res.json();
        stats.accepted += json.accepted;
        stats.rejected += json.rejected;
        stats.latencies.push(ms);
      } else if (res.status === 503) {
        stats.throttled += BATCH;
        await res.arrayBuffer();
        await sleep(Number(res.headers.get('retry-after') ?? 1) * 1000);
      } else {
        stats.failed += BATCH;
        console.error(`HTTP ${res.status}: ${(await res.text()).slice(0, 200)}`);
      }
    } catch (err) {
      stats.failed += BATCH;
      console.error(`request error: ${err.message}`);
      await sleep(500);
    }
    if (perWorkerRate) {
      const target = (BATCH / perWorkerRate) * 1000;
      const wait = target - (Date.now() - started);
      if (wait > 0) await sleep(wait);
    }
  }
}

const pct = (sorted, p) => (sorted.length ? sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * p))] : 0);

async function main() {
  console.log(`→ ${args.url}  rate=${RATE || 'unlimited'}/s  batch=${BATCH}  concurrency=${CONCURRENCY}  duration=${DURATION_MS / 1000}s${args.incident ? '  [incident: payments failing]' : ''}`);
  const startedAt = Date.now();
  const endAt = startedAt + DURATION_MS;

  let last = 0;
  const ticker = setInterval(() => {
    const delta = stats.accepted - last;
    last = stats.accepted;
    process.stdout.write(`  ${Math.round((Date.now() - startedAt) / 1000)}s  ${delta.toLocaleString()} ev/s accepted  (throttled ${stats.throttled.toLocaleString()})\n`);
  }, 1000);

  await Promise.all(Array.from({ length: CONCURRENCY }, () => worker(endAt)));
  clearInterval(ticker);

  const secs = (Date.now() - startedAt) / 1000;
  const lat = stats.latencies.sort((a, b) => a - b);
  console.log('\n=== results ===');
  console.log(`accepted     ${stats.accepted.toLocaleString()} events`);
  console.log(`throughput   ${Math.round(stats.accepted / secs).toLocaleString()} events/sec`);
  console.log(`rejected     ${stats.rejected.toLocaleString()} (schema)`);
  console.log(`throttled    ${stats.throttled.toLocaleString()} (503 backpressure)`);
  console.log(`failed       ${stats.failed.toLocaleString()}`);
  console.log(`request ms   p50 ${pct(lat, 0.5).toFixed(1)}  p95 ${pct(lat, 0.95).toFixed(1)}  p99 ${pct(lat, 0.99).toFixed(1)}`);
}

main();
