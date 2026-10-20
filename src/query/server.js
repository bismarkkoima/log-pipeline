import { config } from '../shared/config.js';
import { createClickHouse } from '../shared/clickhouse.js';
import { createLogger } from '../shared/logger.js';
import { buildQueryApp } from './app.js';

const log = createLogger('query');

async function main() {
  const clickhouse = createClickHouse();
  const app = buildQueryApp({
    clickhouse,
    maxRows: config.query.maxRows,
    logger: { level: config.logLevel },
  });
  await app.listen({ host: '0.0.0.0', port: config.query.port });
  log.info({ port: config.query.port }, 'query api + dashboard listening');

  const shutdown = async () => {
    await app.close();
    await clickhouse.close();
    process.exit(0);
  };
  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
}

main().catch((err) => {
  log.fatal({ err }, 'query api failed to start');
  process.exit(1);
});
