// PAS Application Database Connector
// Supports PostgreSQL, MySQL, and Oracle — each environment may use a different engine.
// Data format: the same PCE key-value table (environment_name, property_key, property_value)
// so the downstream Neptune discovery pipeline requires no changes.

import { DataSourceConnector, ConnectorConfig, FetchResult, ConnectorConfigSchema } from './base-connector';
import { PceRecord } from '../pce-csv-parser';

const SUPPORTED_ENGINES = ['postgresql', 'mysql', 'oracle'] as const;
type DbEngine = typeof SUPPORTED_ENGINES[number];

export class PasConnector implements DataSourceConnector {
  readonly sourceType = 'PAS';

  readonly schema: ConnectorConfigSchema = {
    sourceType: 'PAS',
    displayName: 'PAS Application',
    description: 'Pulls environment configuration from PAS databases. Each environment connects to its own database instance.',
    fields: [
      {
        key: 'envName',
        label: 'Display Name',
        type: 'text',
        required: true,
        placeholder: 'e.g. Production, Staging',
      },
      {
        key: 'dbEngine',
        label: 'Database Engine',
        type: 'select',
        required: true,
        default: 'postgresql',
        options: ['postgresql', 'mysql', 'oracle'],
      },
      {
        key: 'host',
        label: 'Host',
        type: 'text',
        required: true,
        placeholder: 'db.example.com',
      },
      {
        key: 'port',
        label: 'Port',
        type: 'number',
        required: true,
        default: 5432,
        placeholder: '5432 (PostgreSQL) · 3306 (MySQL) · 1521 (Oracle)',
      },
      {
        key: 'database',
        label: 'Database Name',
        type: 'text',
        required: true,
        placeholder: 'my_db  (or Oracle Service Name / SID)',
      },
      {
        key: 'user',
        label: 'Username',
        type: 'text',
        required: true,
      },
      {
        key: 'password',
        label: 'Password',
        type: 'password',
        required: true,
        sensitive: true,
      },
      {
        key: 'table',
        label: 'Table Name',
        type: 'text',
        required: false,
        default: 'pce_properties',
        placeholder: 'pce_properties',
      },
      {
        key: 'ssl',
        label: 'Use SSL',
        type: 'select',
        required: false,
        default: 'true',
        options: ['true', 'false'],
      },
    ],
  };

  validateConfig(config: ConnectorConfig): string[] {
    const errors: string[] = [];
    if (!config.envName) errors.push('envName (Display Name) is required');
    if (!config.host)    errors.push('host is required');
    if (!config.port)    errors.push('port is required');
    if (!config.database) errors.push('database is required');
    if (!config.user)    errors.push('user is required');
    if (!config.password) errors.push('password is required');
    if (config.dbEngine && !SUPPORTED_ENGINES.includes(config.dbEngine as DbEngine)) {
      errors.push(`dbEngine must be one of: ${SUPPORTED_ENGINES.join(', ')}`);
    }
    return errors;
  }

  async fetch(config: ConnectorConfig): Promise<FetchResult> {
    const start = Date.now();
    const engine: DbEngine = (config.dbEngine as DbEngine) || 'postgresql';
    const table = this.sanitizeTableName(config.table || 'pce_properties');

    let records: PceRecord[];
    if (engine === 'postgresql') {
      records = await this.fetchPostgres(config, table);
    } else if (engine === 'mysql') {
      records = await this.fetchMysql(config, table);
    } else if (engine === 'oracle') {
      records = await this.fetchOracle(config, table);
    } else {
      throw new Error(`Unsupported database engine: ${engine}`);
    }

    return {
      records,
      fetchedAt: new Date().toISOString(),
      durationMs: Date.now() - start,
      metadata: {
        engine,
        table,
        host: config.host,
        database: config.database,
        recordCount: records.length,
      },
    };
  }

  private async fetchPostgres(config: ConnectorConfig, table: string): Promise<PceRecord[]> {
    const { Client } = await import('pg');
    const client = new Client({
      host: config.host,
      port: Number(config.port),
      database: config.database,
      user: config.user,
      password: config.password,
      ssl: config.ssl !== 'false' ? { rejectUnauthorized: false } : undefined,
      connectionTimeoutMillis: 10_000,
    });
    try {
      await client.connect();
      const result = await client.query(
        `SELECT environment_name, property_key, property_value
           FROM ${table}
          WHERE environment_name IS NOT NULL
            AND property_key IS NOT NULL
            AND property_value IS NOT NULL`,
      );
      return result.rows.map((row: any) => ({
        environmentName: String(row.environment_name).trim(),
        propertyKey:     String(row.property_key).trim(),
        propertyValue:   String(row.property_value).trim(),
      }));
    } finally {
      await client.end();
    }
  }

  private async fetchMysql(config: ConnectorConfig, table: string): Promise<PceRecord[]> {
    const mysql = await import('mysql2/promise');
    const connection = await mysql.createConnection({
      host: config.host,
      port: Number(config.port),
      database: config.database,
      user: config.user,
      password: config.password,
      ssl: config.ssl !== 'false' ? { rejectUnauthorized: false } : undefined,
      connectTimeout: 10_000,
    });
    try {
      const [rows] = await connection.execute(
        `SELECT environment_name, property_key, property_value
           FROM ?? 
          WHERE environment_name IS NOT NULL
            AND property_key IS NOT NULL
            AND property_value IS NOT NULL`,
        [table],
      );
      return (rows as any[]).map((row: any) => ({
        environmentName: String(row.environment_name).trim(),
        propertyKey:     String(row.property_key).trim(),
        propertyValue:   String(row.property_value).trim(),
      }));
    } finally {
      await connection.end();
    }
  }

  private async fetchOracle(config: ConnectorConfig, table: string): Promise<PceRecord[]> {
    // oracledb v6+ defaults to thin mode — no Oracle Client libraries required in Lambda
    const oracledb = await import('oracledb');
    const connection = await oracledb.getConnection({
      user: config.user,
      password: config.password,
      connectString: `${config.host}:${config.port}/${config.database}`,
    });
    try {
      const result = await connection.execute<{
        ENVIRONMENT_NAME: string;
        PROPERTY_KEY: string;
        PROPERTY_VALUE: string;
      }>(
        `SELECT environment_name, property_key, property_value
           FROM ${table}
          WHERE environment_name IS NOT NULL
            AND property_key IS NOT NULL
            AND property_value IS NOT NULL`,
        [],
        { outFormat: oracledb.OUT_FORMAT_OBJECT },
      );
      return (result.rows ?? []).map((row) => ({
        environmentName: String(row.ENVIRONMENT_NAME).trim(),
        propertyKey:     String(row.PROPERTY_KEY).trim(),
        propertyValue:   String(row.PROPERTY_VALUE).trim(),
      }));
    } finally {
      await connection.close();
    }
  }

  /** Only allow alphanumeric, underscore, dot — prevents SQL injection */
  private sanitizeTableName(name: string): string {
    if (!/^[a-zA-Z0-9_.]+$/.test(name)) {
      throw new Error(`Invalid table name: "${name}"`);
    }
    return name;
  }
}
