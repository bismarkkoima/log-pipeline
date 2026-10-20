import Fastify from 'fastify';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { LEVELS } from '../shared/schema.js';
import {
  bucketMinutesFor, clampMinutes, freshnessSql, overviewSql, searchSql,
  servicesSql, timeseriesSql, topRoutesSql,
} from './queries.js';

const here = path.dirname(fileURLToPath(import.meta.url));

// ClickHouse returns 64-bit ints as strings in JSON; convert for the frontend.
const num = (v) => (v === null || v === undefined ? 0 : Number(v));
const round = (v, d = 1) => Math.round(num(v) * 10 ** d) / 10 ** d;
const latency = (arr) => {
  const [p50 = 0, p95 = 0, p99 = 0] = (arr ?? []).map((v) => (Number.isFinite(Number(v)) ? round(v) : 0));
  return { p50, p95, p99 };
};

/**
 * @param {object} deps
 * @param {{ query(opts): Promise<{ json(): Promise<any[]> }> }} deps.clickhouse
 * @param {number} deps.maxRows
 */
export function buildQueryApp({ clickhouse, maxRows, logger = false }) {
  const app = Fastify({ logger });

  const run = async (query, query_params) => {
    const res = await clickhouse.query({ query, query_params, format: 'JSONEachRow' });
    return res.json();
  };

  const common = (q) => ({
    minutes: clampMinutes(q.minutes),
    service: typeof q.service === 'string' ? q.service : '',
  });

  app.get('/api/overview', async (req) => {
    const p = common(req.query);
    const [[row = {}], [fresh = {}]] = await Promise.all([run(overviewSql, p), run(freshnessSql, {})]);
    const events = num(row.total_events);
    const errors = num(row.error_events);
    const httpRequests = num(row.total_requests);
    return {
      window_minutes: p.minutes,
      events,
      events_per_sec: round(events / (p.minutes * 60), 2),
      errors,
      error_rate: events ? round((errors / events) * 100, 2) : 0,
      http_requests: httpRequests,
      http_5xx_rate: httpRequests ? round((num(row.total_5xx) / httpRequests) * 100, 2) : 0,
      avg_latency_ms: httpRequests ? round(num(row.total_duration) / httpRequests) : 0,
      latency_ms: latency(row.latency),
      services: num(row.services),
      pipeline: {
        recent_events: num(fresh.events),
        lag_p50_ms: round(fresh.lag_ms?.[0]),
        lag_p99_ms: round(fresh.lag_ms?.[1]),
        last_insert: fresh.last_insert ?? null,
      },
    };
  });

  app.get('/api/timeseries', async (req) => {
    const p = common(req.query);
    const bucket = bucketMinutesFor(p.minutes);
    const rows = await run(timeseriesSql, { ...p, bucket });
    return {
      bucket_minutes: bucket,
      points: rows.map((r) => ({
        t: r.t,
        events: num(r.total_events),
        errors: num(r.error_events),
        http_5xx: num(r.total_5xx),
        latency_ms: latency(r.latency),
      })),
    };
  });

  app.get('/api/services', async (req) => {
    const { minutes } = common(req.query);
    const rows = await run(servicesSql, { minutes });
    return rows.map((r) => ({
      service: r.service,
      events: num(r.total_events),
      errors: num(r.error_events),
      error_rate: num(r.total_events) ? round((num(r.error_events) / num(r.total_events)) * 100, 2) : 0,
      http_requests: num(r.total_requests),
      p95_ms: round(r.p95_ms),
    }));
  });

  app.get('/api/routes', async (req) => {
    const p = common(req.query);
    const limit = Math.min(clampMinutes(req.query.limit, 20), 200);
    const rows = await run(topRoutesSql, { ...p, limit });
    return rows.map((r) => ({
      service: r.service,
      route: r.route,
      requests: num(r.requests),
      error_rate: num(r.requests) ? round((num(r.total_5xx) / num(r.requests)) * 100, 2) : 0,
      latency_ms: latency(r.latency),
    }));
  });

  app.get('/api/logs', async (req, reply) => {
    const q = req.query;
    const level = typeof q.level === 'string' ? q.level : '';
    if (level && !LEVELS.includes(level)) {
      return reply.code(400).send({ error: `level must be one of ${LEVELS.join(', ')}` });
    }
    const params = {
      ...common(q),
      level,
      q: typeof q.q === 'string' ? q.q.slice(0, 200) : '',
      trace_id: typeof q.trace_id === 'string' ? q.trace_id : '',
      min_status: Math.max(0, Math.min(599, Number.parseInt(q.min_status, 10) || 0)),
      limit: Math.min(Number.parseInt(q.limit, 10) || 100, maxRows),
    };
    const rows = await run(searchSql, params);
    return rows.map((r) => ({ ...r, http_status: num(r.http_status), duration_ms: round(r.duration_ms) }));
  });

  app.get('/healthz', async () => ({ status: 'ok' }));

  app.get('/', async (_req, reply) => {
    const html = await readFile(path.join(here, 'public', 'index.html'), 'utf8');
    reply.type('text/html').send(html);
  });

  app.setErrorHandler((err, req, reply) => {
    req.log.error({ err }, 'query failed');
    reply.code(err.statusCode && err.statusCode < 500 ? err.statusCode : 502).send({ error: 'query failed' });
  });

  return app;
}
