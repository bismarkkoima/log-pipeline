import Fastify from 'fastify';
import { createHash, randomUUID, timingSafeEqual } from 'node:crypto';
import { createGunzip } from 'node:zlib';
import { validateEvent } from '../shared/schema.js';
import { BackpressureError } from '../shared/batcher.js';

const PARSE_ERROR = Symbol('parseError');
const MAX_ERRORS_RETURNED = 20;

const digest = (s) => createHash('sha256').update(s).digest();

/** Body can be a single event, an array of events, or { events: [...] }. */
function extractEvents(body) {
  if (Array.isArray(body)) return body;
  if (body && Array.isArray(body.events)) return body.events;
  if (body && typeof body === 'object') return [body];
  return null;
}

/**
 * Build the ingestion HTTP app. Dependencies are injected so tests can run
 * without Kafka.
 *
 * @param {object} deps
 * @param {{ enqueue(messages): Promise<void>, pending: number }} deps.producer
 * @param {string[]} deps.apiKeys
 * @param {number} deps.maxBodyBytes
 * @param {number} deps.maxEventsPerRequest
 * @param {object} [deps.metrics]   prom-client registry + instruments (optional)
 * @param {() => boolean} [deps.isReady]
 * @param {object|boolean} [deps.logger]
 */
export function buildIngestApp({ producer, apiKeys, maxBodyBytes, maxEventsPerRequest, metrics, isReady = () => true, logger = false }) {
  const app = Fastify({ logger, bodyLimit: maxBodyBytes, trustProxy: true });
  const keyDigests = apiKeys.map(digest);

  // --- body handling -------------------------------------------------------

  // Transparent gzip. bodyLimit then applies to the *decompressed* size,
  // which protects us from zip bombs.
  app.addHook('preParsing', async (req, _reply, payload) => {
    if ((req.headers['content-encoding'] || '').toLowerCase() !== 'gzip') return payload;
    const encodedLength = Number(req.headers['content-length'] || 0);
    delete req.headers['content-length'];
    const gunzip = payload.pipe(createGunzip());
    gunzip.receivedEncodedLength = encodedLength;
    payload.on('error', (err) => gunzip.destroy(err));
    return gunzip;
  });

  // Newline-delimited JSON: one event per line. A bad line rejects only that event.
  app.addContentTypeParser(['application/x-ndjson', 'application/ndjson'], { parseAs: 'string' }, (_req, body, done) => {
    const events = [];
    for (const line of body.split('\n')) {
      const trimmed = line.trim();
      if (!trimmed) continue;
      try {
        events.push(JSON.parse(trimmed));
      } catch {
        events.push({ [PARSE_ERROR]: true });
      }
    }
    done(null, events);
  });

  // --- auth ----------------------------------------------------------------

  const authenticate = async (req, reply) => {
    const key = req.headers['x-api-key'];
    if (typeof key !== 'string' || !keyDigests.some((d) => timingSafeEqual(d, digest(key)))) {
      return reply.code(401).send({ error: 'missing or invalid x-api-key' });
    }
  };

  // --- routes --------------------------------------------------------------

  app.post('/v1/logs', { preHandler: authenticate }, async (req, reply) => {
    const events = extractEvents(req.body);
    if (!events) return reply.code(400).send({ error: 'body must be an event, an array of events, or {"events": [...]}' });
    if (events.length === 0) return reply.code(400).send({ error: 'no events in request' });
    if (events.length > maxEventsPerRequest) {
      return reply.code(413).send({ error: `max ${maxEventsPerRequest} events per request, got ${events.length}` });
    }

    const receivedAtMs = Date.now();
    const messages = [];
    const errors = [];
    let rejected = 0;

    events.forEach((event, index) => {
      const error = event?.[PARSE_ERROR] ? 'invalid JSON' : validateEvent(event);
      if (error) {
        rejected += 1;
        if (errors.length < MAX_ERRORS_RETURNED) errors.push({ index, error });
        return;
      }
      // An id assigned here (or supplied by the producer) follows the event through
      // every retry, which is what lets ClickHouse collapse duplicates.
      const withId = event.id ? event : { ...event, id: randomUUID() };
      messages.push({ value: JSON.stringify({ e: withId, r: receivedAtMs, ip: req.ip }) });
    });

    metrics?.events.inc({ result: 'rejected' }, rejected);

    if (messages.length === 0) {
      return reply.code(400).send({ accepted: 0, rejected, errors });
    }

    try {
      await producer.enqueue(messages);
    } catch (err) {
      const backpressure = err instanceof BackpressureError;
      metrics?.events.inc({ result: backpressure ? 'throttled' : 'failed' }, messages.length);
      req.log.warn({ err: err.message, events: messages.length }, 'could not enqueue to kafka');
      return reply
        .code(503)
        .header('Retry-After', backpressure ? '1' : '5')
        .send({ error: backpressure ? 'pipeline is saturated, retry shortly' : 'log queue unavailable, retry later' });
    }

    metrics?.events.inc({ result: 'accepted' }, messages.length);
    return reply.code(202).send({ accepted: messages.length, rejected, errors });
  });

  app.get('/healthz', async () => ({ status: 'ok' }));

  app.get('/readyz', async (_req, reply) => {
    if (!isReady()) return reply.code(503).send({ status: 'not ready' });
    return { status: 'ready', pending: producer.pending };
  });

  if (metrics) {
    app.get('/metrics', async (_req, reply) => {
      reply.header('Content-Type', metrics.registry.contentType);
      return metrics.registry.metrics();
    });
  }

  return app;
}
