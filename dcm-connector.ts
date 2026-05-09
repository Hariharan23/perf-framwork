// DCM (Deployment Configuration Manager) Connector
// Reads key=value .properties files stored in S3.
//
// S3 path layout:
//   {DCM_BUCKET_NAME}/{envId}/{appName}/{filename}.properties
//
// The envId is injected by pipeline-sync.ts as config._envId before
// calling fetch() — it is NOT a user-visible connector field.
// The bucket name comes from the DCM_BUCKET_NAME Lambda environment variable.
//
// Users configure two things per environment (Pipelines tab):
//   envName  — human-readable label written to Neptune
//   apps     — comma-separated list of app sub-folder names, e.g. "app1,app2"

import { S3Client, ListObjectsV2Command, GetObjectCommand } from '@aws-sdk/client-s3';
import { Readable } from 'stream';
import { DataSourceConnector, ConnectorConfig, FetchResult, ConnectorConfigSchema } from './base-connector';
import { PceRecord } from '../pce-csv-parser';

// ── helpers ───────────────────────────────────────────────────────────────────

async function streamToString(stream: Readable | ReadableStream | Blob | unknown): Promise<string> {
  if (stream instanceof Readable) {
    return new Promise((resolve, reject) => {
      const chunks: Buffer[] = [];
      (stream as Readable).on('data', (c: Buffer) => chunks.push(c));
      (stream as Readable).on('error', reject);
      (stream as Readable).on('end', () => resolve(Buffer.concat(chunks).toString('utf-8')));
    });
  }
  // SDK v3 may return a Web ReadableStream in some runtimes
  const reader = (stream as ReadableStream<Uint8Array>).getReader();
  const chunks: Uint8Array[] = [];
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    if (value) chunks.push(value);
  }
  return Buffer.concat(chunks.map((c) => Buffer.from(c))).toString('utf-8');
}

/**
 * Parse Java-style .properties format.
 * Strips blank lines and lines starting with # or !
 * Supports key=value and key: value (first = or : delimiter).
 */
function parseProperties(content: string): Record<string, string> {
  const result: Record<string, string> = {};
  for (const rawLine of content.split('\n')) {
    const line = rawLine.trim();
    if (!line || line.startsWith('#') || line.startsWith('!')) continue;
    // find first '=' or ':' as delimiter
    const delimIdx = Math.min(
      line.indexOf('=') < 0 ? Infinity : line.indexOf('='),
      line.indexOf(':') < 0 ? Infinity : line.indexOf(':'),
    );
    if (!isFinite(delimIdx)) continue;
    const key   = line.slice(0, delimIdx).trim();
    const value = line.slice(delimIdx + 1).trim();
    if (key) result[key] = value;
  }
  return result;
}

// ── Connector ─────────────────────────────────────────────────────────────────

export class DcmConnector implements DataSourceConnector {
  readonly sourceType = 'DCM';

  readonly schema: ConnectorConfigSchema = {
    sourceType: 'DCM',
    displayName: 'DCM Properties',
    description:
      'Reads .properties configuration files from S3. Each environment has its own S3 sub-folder; within it each app has its own sub-folder.',
    icon: '📄',
    defaultEdges: [
      {
        toEntityType: 'Environment',
        mode: 'upsert',
        joinKey: 'envName',
        matchBy: 'name',
        priority: 10,
        description: 'Create or update an Environment node using the DCM environment display name',
      },
      {
        toEntityType: 'Integration',
        mode: 'create-child',
        joinKey: 'propertyKey',
        matchBy: 'name',
        priority: 20,
        description: 'Create an Integration node for each DCM property entry (keyed as appName.propertyKey)',
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
        key: 'apps',
        label: 'App Folders',
        type: 'text',
        required: true,
        placeholder: 'e.g. auth-service,payment-service  (comma-separated)',
      },
    ],
  };

  validateConfig(config: ConnectorConfig): string[] {
    const errors: string[] = [];
    if (!config.envName) errors.push('envName (Display Name) is required');
    if (!config.apps)    errors.push('apps (App Folders) is required — provide at least one app folder name');
    const appList = String(config.apps || '').split(',').map((a) => a.trim()).filter(Boolean);
    if (appList.length === 0) errors.push('apps must contain at least one non-empty folder name');
    return errors;
  }

  async fetch(config: ConnectorConfig): Promise<FetchResult> {
    const startMs    = Date.now();
    const bucketName = process.env.DCM_BUCKET_NAME || '';
    // envId injected by pipeline-sync.ts; fall back to envName for backwards compat
    const envId      = String(config._envId || config.envName || '');
    const apps       = String(config.apps || '')
      .split(',')
      .map((a) => a.trim())
      .filter(Boolean);

    if (!bucketName) throw new Error('DCM_BUCKET_NAME environment variable is not set');
    if (!envId)      throw new Error('envId / envName not available in DCM config');
    if (!apps.length) throw new Error('No app folders configured for this DCM environment');

    const region = process.env.AWS_REGION || process.env.NEPTUNE_REGION || 'us-east-1';
    const s3     = new S3Client({ region });
    const records: PceRecord[] = [];

    for (const appName of apps) {
      const prefix = `${envId}/${appName}/`;
      let continuationToken: string | undefined;

      do {
        const listRes = await s3.send(
          new ListObjectsV2Command({
            Bucket:            bucketName,
            Prefix:            prefix,
            ContinuationToken: continuationToken,
          }),
        );

        for (const obj of listRes.Contents ?? []) {
          const objKey = obj.Key ?? '';
          if (!objKey.endsWith('.properties')) continue;

          const getRes = await s3.send(
            new GetObjectCommand({ Bucket: bucketName, Key: objKey }),
          );

          const text  = await streamToString(getRes.Body);
          const props = parseProperties(text);

          for (const [propKey, propValue] of Object.entries(props)) {
            records.push({
              environmentName: config.envName as string,
              propertyKey:     `${appName}.${propKey}`,
              propertyValue:   propValue,
            });
          }
        }

        continuationToken = listRes.IsTruncated
          ? listRes.NextContinuationToken
          : undefined;
      } while (continuationToken);
    }

    console.log(`[dcm-connector] fetched ${records.length} records from bucket=${bucketName} envId=${envId} apps=${apps.join(',')}`);
    return { records, fetchedAt: new Date().toISOString(), durationMs: Date.now() - startMs };
  }
}
