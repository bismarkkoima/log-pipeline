import Ajv from 'ajv';

export const LEVELS = ['debug', 'info', 'warn', 'error', 'fatal'];

// What producers are allowed to send. Kept permissive on optional fields,
// strict on the few that every query depends on.
export const logEventSchema = {
  type: 'object',
  required: ['service', 'message'],
  additionalProperties: false,
  properties: {
    id: { type: 'string', minLength: 1, maxLength: 64 },
    timestamp: {
      anyOf: [
        { type: 'string', minLength: 1, maxLength: 40 },
        { type: 'number' },
      ],
    },
    service: { type: 'string', minLength: 1, maxLength: 128, pattern: '^[A-Za-z0-9._-]+$' },
    level: { type: 'string', enum: LEVELS },
    message: { type: 'string', maxLength: 16384 },
    host: { type: 'string', maxLength: 255 },
    trace_id: { type: 'string', maxLength: 128 },
    http: {
      type: 'object',
      additionalProperties: false,
      properties: {
        method: { type: 'string', maxLength: 16 },
        path: { type: 'string', maxLength: 2048 },
        status: { type: 'integer', minimum: 100, maximum: 599 },
        duration_ms: { type: 'number', minimum: 0 },
      },
    },
    attributes: {
      type: 'object',
      maxProperties: 64,
      additionalProperties: { anyOf: [{ type: 'string' }, { type: 'number' }, { type: 'boolean' }] },
    },
  },
};

const ajv = new Ajv({ allErrors: false, coerceTypes: false });
const validateFn = ajv.compile(logEventSchema);

/** Returns null when valid, otherwise a short human-readable error string. */
export function validateEvent(event) {
  if (validateFn(event)) return null;
  const err = validateFn.errors?.[0];
  if (!err) return 'invalid event';
  const where = err.instancePath || '(root)';
  return `${where} ${err.message}`;
}
