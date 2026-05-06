// Connector Registry
// Central map of sourceType → DataSourceConnector implementation.
// To add a new data source: implement DataSourceConnector, then call register().
// No other files need to change.

import { DataSourceConnector, ConnectorConfigSchema } from './connectors/base-connector';
import { PasConnector } from './connectors/pas-connector';

const REGISTRY = new Map<string, DataSourceConnector>();

function register(connector: DataSourceConnector): void {
  REGISTRY.set(connector.sourceType.toUpperCase(), connector);
}

// ── Register built-in connectors ──
register(new PasConnector());

// ── Public API ──

export function getConnector(sourceType: string): DataSourceConnector {
  const connector = REGISTRY.get(sourceType.toUpperCase());
  if (!connector) {
    throw new Error(
      `Unknown source type: "${sourceType}". Registered: ${Array.from(REGISTRY.keys()).join(', ')}`,
    );
  }
  return connector;
}

export function listConnectors(): ConnectorConfigSchema[] {
  return Array.from(REGISTRY.values()).map((c) => c.schema);
}

export function isValidSourceType(sourceType: string): boolean {
  return REGISTRY.has(sourceType.toUpperCase());
}
