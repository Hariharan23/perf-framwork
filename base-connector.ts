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

/** Default mapping rule scaffolded into the topology table when a connector is first registered. */
export interface EdgeTemplate {
  /** Neptune entity type this edge writes to. */
  toEntityType: 'Environment' | 'Integration';
  /**
   * upsert       — create or update the entity by matchBy key
   * enrich-only  — update configs on existing entity, never create
   * create-child — create entity as child of a resolved parent environment
   * resolve-scope — restrict parent env lookup to entities from a specific source:instance
   */
  mode: 'upsert' | 'enrich-only' | 'create-child' | 'resolve-scope';
  /** Which field from the incoming record is used to match / name the entity. */
  joinKey: 'hostname' | 'propertyKey' | 'envName' | 'fullUrl';
  /** Which field on the Neptune entity to match against. */
  matchBy: 'name' | 'id' | 'hostname';
  /** Lower number = evaluated first when multiple edges match the same target type. */
  priority: number;
  description: string;
}

export interface ConnectorConfigSchema {
  sourceType: string;
  displayName: string;
  description: string;
  /** Emoji or single character used in the topology canvas and UI. */
  icon?: string;
  fields: ConfigField[];
  /**
   * Default mapping rules automatically created in the topology table when this
   * connector is first registered.  Users can override or delete them per-instance.
   */
  defaultEdges?: EdgeTemplate[];
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
