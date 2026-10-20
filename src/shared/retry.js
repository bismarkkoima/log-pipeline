/**
 * Retry an async function with exponential backoff and full jitter.
 * Delay before attempt n (1-based) is random(0, base * 2^(n-1)), capped at maxDelayMs.
 */
export async function withRetry(fn, { retries, baseMs, maxDelayMs = 10_000, onRetry, sleep = defaultSleep }) {
  let attempt = 0;
  for (;;) {
    try {
      return await fn(attempt);
    } catch (err) {
      if (attempt >= retries) throw err;
      attempt += 1;
      const delay = Math.round(Math.random() * Math.min(maxDelayMs, baseMs * 2 ** (attempt - 1)));
      onRetry?.(err, attempt, delay);
      await sleep(delay);
    }
  }
}

const defaultSleep = (ms) => new Promise((r) => setTimeout(r, ms));
