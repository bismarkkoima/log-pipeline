// Coalesces items from many concurrent callers into large batches. Used by the
// ingest API (HTTP requests -> Kafka batches) and the workers (Kafka partitions ->
// ClickHouse inserts).
//
// Each caller gets a promise that resolves only once *its* items have been shipped
// successfully. The ingest API awaits it before replying 202, and workers await it
// before committing Kafka offsets, so nothing is acknowledged before it's durable.
//
// Backpressure: if the downstream slows down, `pending` grows. Past `maxPending` new
// work is refused with BackpressureError, which the API turns into 503 + Retry-After
// instead of buffering until the process runs out of memory.

export class BackpressureError extends Error {
  constructor(pending) {
    super(`backlog full (${pending} items pending)`);
    this.name = 'BackpressureError';
    this.pending = pending;
  }
}

export class Batcher {
  /**
   * @param {object} opts
   * @param {(messages: Array<{value: string}>) => Promise<void>} opts.send  ships one batch
   * @param {number} opts.batchSize   flush as soon as this many messages are queued
   * @param {number} opts.flushMs     otherwise flush after this long
   * @param {number} opts.maxPending  refuse new work beyond this many queued + in-flight messages
   * @param {number} [opts.maxInFlight=4] concurrent batches allowed on the wire
   */
  constructor({ send, batchSize, flushMs, maxPending, maxInFlight = 4 }) {
    this.send = send;
    this.batchSize = batchSize;
    this.flushMs = flushMs;
    this.maxPending = maxPending;
    this.maxInFlight = maxInFlight;

    this.queue = []; // [{ messages, resolve, reject }]
    this.queuedCount = 0;
    this.inFlightCount = 0;
    this.inFlightBatches = 0;
    this.timer = null;
    this.closed = false;
    this.onBatch = null; // optional hook: ({ size, ms, ok }) => void, used for metrics
  }

  get pending() {
    return this.queuedCount + this.inFlightCount;
  }

  /** Queue messages; resolves when Kafka has acknowledged all of them. */
  enqueue(messages) {
    if (this.closed) return Promise.reject(new Error('producer is shutting down'));
    if (messages.length === 0) return Promise.resolve();
    if (this.pending + messages.length > this.maxPending) {
      return Promise.reject(new BackpressureError(this.pending));
    }
    return new Promise((resolve, reject) => {
      this.queue.push({ messages, resolve, reject });
      this.queuedCount += messages.length;
      if (this.queuedCount >= this.batchSize) this.#drain();
      else if (!this.timer) this.timer = setTimeout(() => this.#drain(), this.flushMs);
    });
  }

  /** Take whole requests off the queue until we reach roughly batchSize. */
  #takeBatch() {
    const entries = [];
    let size = 0;
    while (this.queue.length && (size === 0 || size + this.queue[0].messages.length <= this.batchSize)) {
      const entry = this.queue.shift();
      entries.push(entry);
      size += entry.messages.length;
    }
    this.queuedCount -= size;
    return { entries, size };
  }

  #drain() {
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    while (this.queue.length && this.inFlightBatches < this.maxInFlight) {
      const { entries, size } = this.#takeBatch();
      this.#ship(entries, size);
    }
    // Anything left waits for an in-flight batch to finish (see #ship), or the timer.
    if (this.queue.length && !this.timer && this.inFlightBatches < this.maxInFlight) {
      this.timer = setTimeout(() => this.#drain(), this.flushMs);
    }
  }

  async #ship(entries, size) {
    this.inFlightBatches += 1;
    this.inFlightCount += size;
    const messages = entries.flatMap((e) => e.messages);
    const started = Date.now();
    try {
      await this.send(messages);
      for (const e of entries) e.resolve();
      this.onBatch?.({ size, ms: Date.now() - started, ok: true });
    } catch (err) {
      for (const e of entries) e.reject(err);
      this.onBatch?.({ size, ms: Date.now() - started, ok: false });
    } finally {
      this.inFlightBatches -= 1;
      this.inFlightCount -= size;
      if (this.queue.length) {
        if (this.queuedCount >= this.batchSize || this.closed) this.#drain();
        else if (!this.timer) this.timer = setTimeout(() => this.#drain(), this.flushMs);
      }
    }
  }

  /** Stop accepting work and wait until everything queued has been sent. */
  async close() {
    this.closed = true;
    this.#drain();
    while (this.pending > 0) {
      await new Promise((r) => setTimeout(r, 10));
      if (this.queue.length && this.inFlightBatches < this.maxInFlight) this.#drain();
    }
  }
}
