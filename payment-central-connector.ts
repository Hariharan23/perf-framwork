// Payment Central Connector
// Payment Central provides one config file per environment.
// Files are uploaded to S3 out-of-band (manually or via CI/CD) and this connector
// fetches the file from S3 when the pipeline runs for the respective environment.
//
// S3 path layout:
//   {PC_BUCKET_NAME}/{envId}/{fileName}
//
// The envId is injected by pipeline-sync.ts as config._envId before
// calling fetch() — it is NOT a user-visible connector field.
// The bucket name comes from the PC_BUCKET_NAME Lambda environment variable.
//
// Users configure per environment (Pipelines tab):
//   envName    — human-readable label written to Neptune
//   fileName   — the config file name in S3, e.g. "prod-config.json"
//   fileFormat — one of "auto" | "json" | "properties"
//                "auto" infers from the file extension; falls back to properties format

import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';
import { Readable } from 'stream';
import {
  DataSourceConnector,
  ConnectorConfig,
  FetchResult,
  ConnectorConfigSchema,
} from './base-connector';
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
 * Flatten a potentially nested JSON object into dot-separated key=value pairs.
 * Arrays are serialised to their JSON representation (stored as a single string value).
 */
function flattenJson(obj: Record<string, unknown>, prefix = ''): Record<string, string> {
  const result: Record<string, string> = {};
  for (const [key, val] of Object.entries(obj)) {
    const fullKey = prefix ? `${prefix}.${key}` : key;
    if (val !== null && typeof val === 'object' && !Array.isArray(val)) {
      Object.assign(result, flattenJson(val as Record<string, unknown>, fullKey));
    } else {
      result[fullKey] = Array.isArray(val) ? JSON.stringify(val) : String(val ?? '');
    }
  }
  return result;
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

/**
 * Infer the file format from the file extension when fileFormat='auto'.
 * Returns 'json' for .json, 'properties' for everything else.
 */
function inferFormat(fileName: string, declared: string): 'json' | 'properties' {
  if (declared === 'json') return 'json';
  if (declared === 'properties') return 'properties';
  return fileName.toLowerCase().endsWith('.json') ? 'json' : 'properties';
}

// ── Connector ─────────────────────────────────────────────────────────────────

export class PaymentCentralConnector implements DataSourceConnector {
  readonly sourceType = 'PAYMENT_CENTRAL';

  readonly schema: ConnectorConfigSchema = {
    sourceType: 'PAYMENT_CENTRAL',
    displayName: 'Payment Central',
    description:
      'Reads a single environment config file from S3. ' +
      'Upload the file to s3://{PC_BUCKET_NAME}/{envId}/{fileName} before running the pipeline.',
    icon: '',
    priority: 3,
    defaultEdges: [
      {
        toEntityType: 'Environment',
        mode: 'upsert',
        joinKey: 'envName',
        matchBy: 'name',
        priority: 10,
        description: 'Create or update an Environment node using the Payment Central environment display name',
      },
      {
        toEntityType: 'Integration',
        mode: 'create-child',
        joinKey: 'propertyKey',
        matchBy: 'name',
        priority: 20,
        description: 'Create an Integration node for each config entry in the Payment Central file',
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
        key: 'fileName',
        label: 'Config File Name',
        type: 'text',
        required: true,
        placeholder: 'e.g. prod-config.json  or  env-settings.properties',
      },
      {
        key: 'fileFormat',
        label: 'File Format',
        type: 'select',
        required: false,
        default: 'auto',
        options: ['auto', 'json', 'properties'],
      },
    ],
  };

  validateConfig(config: ConnectorConfig): string[] {
    const errors: string[] = [];
    if (!config.envName)  errors.push('envName (Display Name) is required');
    if (!config.fileName) errors.push('fileName (Config File Name) is required');
    if (
      config.fileFormat &&
      !['auto', 'json', 'properties'].includes(config.fileFormat as string)
    ) {
      errors.push('fileFormat must be one of: auto, json, properties');
    }
    return errors;
  }

  async fetch(config: ConnectorConfig): Promise<FetchResult> {
    const startMs = Date.now();

    const bucketName = process.env.PC_BUCKET_NAME || '';
    if (!bucketName) throw new Error('PC_BUCKET_NAME environment variable is not set');

    // envId injected by pipeline-sync.ts; fall back to envName for backwards compat
    const envId    = String(config._envId || config.envName || '').trim();
    const fileName = String(config.fileName   || '').trim();
    const fmt      = String(config.fileFormat || 'auto').trim();

    if (!envId)    throw new Error('envId / envName not available in Payment Central config');
    if (!fileName) throw new Error('fileName is required');

    const region = process.env.AWS_REGION || process.env.NEPTUNE_REGION || 'us-east-1';
    const s3     = new S3Client({ region });
    const s3Key  = `${envId}/${fileName}`;

    // ── Fetch file from S3 ────────────────────────────────────────────────────
    console.log(`[payment-central-connector] fetching s3://${bucketName}/${s3Key}`);

    let rawContent: string;
    try {
      const getRes = await s3.send(
        new GetObjectCommand({ Bucket: bucketName, Key: s3Key }),
      );
      rawContent = await streamToString(getRes.Body);
    } catch (err: any) {
      const code: string = err?.name || err?.Code || '';
      if (code === 'NoSuchKey' || code === 'NotFound') {
        throw new Error(
          `Payment Central config file not found in S3: s3://${bucketName}/${s3Key} — ` +
          `upload the file before running the pipeline`,
        );
      }
      throw new Error(`Payment Central S3 fetch failed: ${err?.message ?? err}`);
    }

    // ── Parse file into key-value pairs ───────────────────────────────────────
    const format = inferFormat(fileName, fmt);
    let kvMap: Record<string, string>;

    if (format === 'json') {
      let parsed: unknown;
      try {
        parsed = JSON.parse(rawContent);
      } catch {
        throw new Error(
          `Failed to parse Payment Central file as JSON (s3://${bucketName}/${s3Key}). ` +
          `Set fileFormat=properties if it is not a JSON file.`,
        );
      }
      if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
        throw new Error(
          'Payment Central JSON config must be a top-level object (not an array or primitive)',
        );
      }
      kvMap = flattenJson(parsed as Record<string, unknown>);
    } else {
      kvMap = parseProperties(rawContent);
    }

    const records: PceRecord[] = Object.entries(kvMap).map(([key, value]) => ({
      environmentName: config.envName as string,
      propertyKey:     key,
      propertyValue:   value,
    }));

    console.log(
      `[payment-central-connector] parsed ${records.length} records ` +
      `format=${format} envId=${envId} s3Key=${s3Key}`,
    );

    return {
      records,
      fetchedAt: new Date().toISOString(),
      durationMs: Date.now() - startMs,
      metadata: {
        bucketName,
        s3Key,
        format,
        recordCount: records.length,
      },
    };
  }
}
