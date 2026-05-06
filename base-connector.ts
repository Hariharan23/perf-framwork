// Base connector interface for all data pipeline sources.
// Each new source type implements DataSourceConnector and registers
// itself in connector-registry.ts — no changes needed elsewhere.

import { PceRecord } from '../pce-csv-parser';

export interface ConfigField {
  key: string;
  label: string;
  type: 'text' | 'number' | 'password' | 'select';
  required: boolean;
  default?: string | number;
  options?: string[];      // for type='select'
  placeholder?: string;
  sensitive?: boolean;     // stored encrypted in SSM SecureString
}

export interface ConnectorConfigSchema {
  sourceType: string;
  displayName: string;
  description: string;
  fields: ConfigField[];
}

export interface ConnectorConfig {
  envId: string;
  envName: string;
  [key: string]: any;
}

export interface FetchResult {
  records: PceRecord[];
  fetchedAt: string;
  durationMs: number;
  metadata?: Record<string, any>;
}

export interface DataSourceConnector {
  readonly sourceType: string;
  readonly schema: ConnectorConfigSchema;
  /** Pull records from the source — returns PceRecord[] for Neptune ingestion */
  fetch(config: ConnectorConfig): Promise<FetchResult>;
  /** Return array of human-readable validation error messages, empty = valid */
  validateConfig(config: ConnectorConfig): string[];
}
