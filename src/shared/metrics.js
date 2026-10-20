import client from 'prom-client';

export function createRegistry(component) {
  const registry = new client.Registry();
  registry.setDefaultLabels({ component });
  client.collectDefaultMetrics({ register: registry });
  return { registry, client };
}
