import { createClient } from '@clickhouse/client';
import { config } from './config.js';

export function createClickHouse({ database = config.clickhouse.database } = {}) {
  return createClient({
    url: config.clickhouse.url,
    username: config.clickhouse.username,
    password: config.clickhouse.password,
    database,
    request_timeout: 30_000,
    compression: { request: true, response: true },
    clickhouse_settings: {
      // Workers already batch thousands of rows per insert, so server-side
      // async inserts stay off: each insert is acknowledged only once it's durable.
      async_insert: 0,
      date_time_input_format: 'best_effort',
    },
  });
}
