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
    icon: '🗄️',
    defaultEdges: [
      {
        toEntityType: 'Environment',
        mode: 'upsert',
        joinKey: 'envName',
        matchBy: 'name',
        priority: 10,
        description: 'Create or update an Environment node from the PAS environment name',
      },
      {
        toEntityType: 'Integration',
        mode: 'create-child',
        joinKey: 'hostname',
        matchBy: 'name',
        priority: 20,
        description: 'Create an Integration node as a child of the resolved parent Environment',
      },
    ],
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
        default: 'pasadm.propertyconfigurerentity',
        placeholder: 'pasadm.propertyconfigurerentity',
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
    const table = this.sanitizeTableName(config.table || 'pasadm.propertyconfigurerentity');
    const envId  = String(config.envId || config.envName || '').trim();

    let records: PceRecord[];
    if (engine === 'postgresql') {
      records = await this.fetchPostgres(config, table, envId);
    } else if (engine === 'mysql') {
      records = await this.fetchMysql(config, table, envId);
    } else if (engine === 'oracle') {
      records = await this.fetchOracle(config, table, envId);
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

  private async fetchPostgres(config: ConnectorConfig, table: string, envId: string): Promise<PceRecord[]> {
    const { Client } = await import('pg');
    let client: InstanceType<typeof Client> | undefined;
    try {
      client = new Client({
        host: config.host,
        port: Number(config.port),
        database: config.database,
        user: config.user,
        password: config.password,
        ssl: config.ssl !== 'false' ? { rejectUnauthorized: false } : undefined,
        connectionTimeoutMillis: 10_000,
      });
      await client.connect();
      const result = await client.query(
        `SELECT $1 AS environment_name, propertyname AS property_key, value AS property_value
           FROM ${table}
          WHERE propertyname IS NOT NULL
            AND value IS NOT NULL`,
        [envId],
      );
      return result.rows.map((row: any) => ({
        environmentName: String(row.environment_name).trim(),
        propertyKey:     String(row.property_key).trim(),
        propertyValue:   String(row.property_value).trim(),
      }));
    } finally {
      if (client) {
        try { await client.end(); } catch (e) {
          console.warn('[pas-connector] postgres close error', e);
        }
      }
    }
  }

  private async fetchMysql(config: ConnectorConfig, table: string, envId: string): Promise<PceRecord[]> {
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
        `SELECT ? AS environment_name, propertyname AS property_key, value AS property_value
           FROM ??
          WHERE propertyname IS NOT NULL
            AND value IS NOT NULL`,
        [envId, table],
      );
      return (rows as any[]).map((row: any) => ({
        environmentName: String(row.environment_name).trim(),
        propertyKey:     String(row.property_key).trim(),
        propertyValue:   String(row.property_value).trim(),
      }));
    } finally {
      try { await connection.end(); } catch (e) {
        console.warn('[pas-connector] mysql close error', e);
      }
    }
  }

  private async fetchOracle(config: ConnectorConfig, table: string, envId: string): Promise<PceRecord[]> {
    // oracledb v6+ defaults to thin mode — no Oracle Client libraries required in Lambda
    const oracledb = await import('oracledb');
    const connection = await oracledb.getConnection({
      user: config.user,
      password: config.password,
      connectString: `${config.host}:${config.port}/${config.database}`,
    });
    try {
      const result = await connection.execute(
        `SELECT :envId AS environment_name, propertyname AS property_key, value AS property_value
           FROM ${table}
          WHERE propertyname IS NOT NULL
            AND value IS NOT NULL`,
        { envId },
        { outFormat: oracledb.OUT_FORMAT_OBJECT },
      );
      return ((result.rows ?? []) as Array<Record<string, string>>).map((row) => ({
        environmentName: String(row['ENVIRONMENT_NAME']).trim(),
        propertyKey:     String(row['PROPERTY_KEY']).trim(),
        propertyValue:   String(row['PROPERTY_VALUE']).trim(),
      }));
    } finally {
      try { await connection.close(); } catch (e) {
        console.warn('[pas-connector] oracle close error', e);
      }
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
