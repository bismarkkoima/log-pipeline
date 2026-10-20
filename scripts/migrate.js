// Applies migrations/*.sql in order and records each one in schema_migrations,
// so it's safe to run on every deploy.
import { readdir, readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { config } from '../src/shared/config.js';
import { createClickHouse } from '../src/shared/clickhouse.js';
import { createLogger } from '../src/shared/logger.js';

const log = createLogger('migrate');
const dir = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'migrations');

/** Split a SQL file into statements. Our migrations never put ';' inside strings. */
export function splitStatements(sql) {
  return sql
    .split('\n')
    .filter((line) => !line.trim().startsWith('--'))
    .join('\n')
    .split(/;\s*(?:\n|$)/)
    .map((s) => s.trim())
    .filter(Boolean);
}

async function waitForClickHouse(client, attempts = 30) {
  for (let i = 1; i <= attempts; i++) {
    try {
      const { success } = await client.ping();
      if (success) return;
    } catch { /* not up yet */ }
    log.info({ attempt: i }, 'waiting for clickhouse');
    await new Promise((r) => setTimeout(r, 2000));
  }
  throw new Error('ClickHouse did not become ready');
}

export async function migrate() {
  const db = config.clickhouse.database;

  const root = createClickHouse({ database: 'default' });
  await waitForClickHouse(root);
  await root.command({ query: `CREATE DATABASE IF NOT EXISTS ${db}` });
  await root.close();

  const client = createClickHouse();
  await client.command({
    query: `CREATE TABLE IF NOT EXISTS schema_migrations
            (name String, applied_at DateTime DEFAULT now())
            ENGINE = MergeTree ORDER BY name`,
  });

  const appliedRes = await client.query({ query: 'SELECT name FROM schema_migrations', format: 'JSONEachRow' });
  const applied = new Set((await appliedRes.json()).map((r) => r.name));

  const files = (await readdir(dir)).filter((f) => f.endsWith('.sql')).sort();
  for (const file of files) {
    if (applied.has(file)) continue;
    const sql = await readFile(path.join(dir, file), 'utf8');
    for (const statement of splitStatements(sql)) {
      await client.command({ query: statement });
    }
    await client.insert({ table: 'schema_migrations', values: [{ name: file }], format: 'JSONEachRow' });
    log.info({ file }, 'applied migration');
  }
  await client.close();
  log.info('migrations up to date');
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  migrate().catch((err) => {
    log.error({ err }, 'migration failed');
    process.exit(1);
  });
}
