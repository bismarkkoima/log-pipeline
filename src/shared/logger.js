import pino from 'pino';
import { config } from './config.js';

export function createLogger(component) {
  return pino({ level: config.logLevel, base: { component } });
}
