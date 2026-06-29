// PCE Discovery Lambda Handler
// Entry point for the PCE Discovery Pipeline
// Supports 2 trigger types:
//   1. S3 PutObject event     → read CSV from S3
//   2. API Gateway POST       → CSV in request body

import { APIGatewayProxyEvent, S3Event, Context } from 'aws-lambda';
import { parsePceCsv, PceRecord } from '../shared/pce-csv-parser';
import { runDiscoveryPipeline, OrchestratorConfig } from '../shared/discovery-orchestrator';
import { NeptuneSparqlClient } from '../shared/neptune-sparql-client';

const CORS_HEADERS = {
  'Content-Type': 'application/json',
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization, Accept, Origin, X-Requested-With, x-api-key',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
};

function getOrchestratorConfig(): OrchestratorConfig {
  return {
    neptuneEndpoint: process.env.NEPTUNE_ENDPOINT || '',
    neptuneRegion: process.env.AWS_REGION || 'us-east-1',
    aliasTableName: process.env.ALIAS_TABLE_NAME,
    autoCreateEnvironments: process.env.AUTO_CREATE_ENVIRONMENTS !== 'false',
  };
}

/**
 * Detect trigger source and route accordingly
 */
export async function handler(event: any, context: Context): Promise<any> {
  console.log('PCE Discovery Lambda invoked:', JSON.stringify({
    source: detectSource(event),
    requestId: context.awsRequestId,
  }));

  try {
    // OPTIONS preflight
    if (event.httpMethod === 'OPTIONS') {
      return { statusCode: 200, headers: CORS_HEADERS, body: '' };
    }

    const source = detectSource(event);

    switch (source) {
      case 'api-gateway':
        return await handleApiGateway(event as APIGatewayProxyEvent);
      case 's3':
        return await handleS3Event(event as S3Event);
      default:
        console.error('Unknown trigger source:', JSON.stringify(event).substring(0, 500));
        return {
          statusCode: 400,
          headers: CORS_HEADERS,
          body: JSON.stringify({ error: 'Unknown trigger source' }),
        };
    }
  } catch (err) {
    console.error('PCE Discovery pipeline error:', err);
    return {
      statusCode: 500,
      headers: CORS_HEADERS,
      body: JSON.stringify({
        error: 'Pipeline execution failed',
        message: err instanceof Error ? err.message : String(err),
      }),
    };
  }
}

/**
 * Detect what triggered this Lambda
 */
function detectSource(event: any): 'api-gateway' | 's3' | 'unknown' {
  if (event.httpMethod || event.requestContext?.http) return 'api-gateway';
  if (event.Records?.[0]?.eventSource === 'aws:s3') return 's3';
  return 'unknown';
}

/**
 * Handle API Gateway requests.
 * GET is reserved for read-only retrieval operations.
 * POST is used for CSV upload and mutation operations.
 */
async function handleApiGateway(event: APIGatewayProxyEvent): Promise<any> {
  const body = event.isBase64Encoded
    ? Buffer.from(event.body || '', 'base64').toString('utf-8')
    : event.body || '';
  const method = (event.httpMethod || 'POST').toUpperCase();
  const queryOperation = event.queryStringParameters?.operation;

  if (method === 'GET') {
    if (queryOperation === 'list-orphan-nodes') {
      const neptune = new NeptuneSparqlClient(
        process.env.NEPTUNE_ENDPOINT || '',
        process.env.AWS_REGION || 'us-east-1',
      );
      const orphans = await neptune.listOrphanEnvironments();
      return {
        statusCode: 200,
        headers: CORS_HEADERS,
        body: JSON.stringify({ orphans, count: orphans.length }),
      };
    }

    return {
      statusCode: 405,
      headers: CORS_HEADERS,
      body: JSON.stringify({ error: 'GET is supported only for retrieval operations' }),
    };
  }

  if (!body.trim()) {
    return {
      statusCode: 400,
      headers: CORS_HEADERS,
      body: JSON.stringify({ error: 'Request body is empty. Send CSV content as the body.' }),
    };
  }

  // Check if it's a JSON operation request or a CSV upload
  let csvContent: string;
  let sourceFile = 'api-upload';

  try {
    const parsed = JSON.parse(body);

    // --- operation: list-orphan-nodes ---
    if (parsed.operation === 'list-orphan-nodes') {
      const neptune = new NeptuneSparqlClient(
        process.env.NEPTUNE_ENDPOINT || '',
        process.env.AWS_REGION || 'us-east-1',
      );
      const orphans = await neptune.listOrphanEnvironments();
      return {
        statusCode: 200,
        headers: CORS_HEADERS,
        body: JSON.stringify({ orphans, count: orphans.length }),
      };
    }

    // --- operation: delete-orphan-nodes ---
    if (parsed.operation === 'delete-orphan-nodes') {
      const neptune = new NeptuneSparqlClient(
        process.env.NEPTUNE_ENDPOINT || '',
        process.env.AWS_REGION || 'us-east-1',
      );
      // If caller provides explicit ids, delete only those; otherwise discover and delete all orphans.
      let ids: string[] = parsed.ids || [];
      if (ids.length === 0) {
        const orphans = await neptune.listOrphanEnvironments();
        ids = orphans.map(o => o.id);
      }
      const result = await neptune.deleteOrphanEnvironments(ids);
      return {
        statusCode: 200,
        headers: CORS_HEADERS,
        body: JSON.stringify(result),
      };
    }

    // --- operation: connect-orphan-node ---
    if (parsed.operation === 'connect-orphan-node') {
      const { nodeId, orphanName, sourceEnvName, rawHostname, createdByRun, createdAt,
              propertyKey, fullUrl, protocol, port, path: urlPath, endpointHint, sourceFile } = parsed;
      if (!nodeId || !sourceEnvName) {
        return {
          statusCode: 400,
          headers: CORS_HEADERS,
          body: JSON.stringify({ error: 'nodeId and sourceEnvName are required' }),
        };
      }
      const neptune = new NeptuneSparqlClient(
        process.env.NEPTUNE_ENDPOINT || '',
        process.env.AWS_REGION || 'us-east-1',
      );
      const extraConfig: Record<string, string> = {};
      if (rawHostname)    extraConfig.hostname          = rawHostname;
      if (propertyKey)    extraConfig.propertyKey       = propertyKey;
      if (fullUrl)        extraConfig.fullUrl            = fullUrl;
      if (protocol)       extraConfig.protocol           = protocol;
      if (port)           extraConfig.port               = String(port);
      if (urlPath)        extraConfig.path               = urlPath;
      if (endpointHint)   extraConfig.endpointHint       = endpointHint;
      if (sourceFile)     extraConfig.sourceFile         = sourceFile;
      if (createdByRun)   extraConfig.createdByRun       = createdByRun;
      if (createdAt)      extraConfig.discoveredAt       = createdAt;
      if (sourceEnvName)  extraConfig.sourceEnvironment  = sourceEnvName;
      await neptune.connectOrphanNode(nodeId, orphanName || nodeId, sourceEnvName, extraConfig);

      // Auto-write alias so Tier 2 (alias map) also catches this hostname on next run.
      // Only needed when the rawHostname differs from the orphanName (i.e. a real rename scenario).
      const aliasTable = process.env.ALIAS_TABLE_NAME;
      if (aliasTable && rawHostname && rawHostname !== (orphanName || nodeId)) {
        try {
          const { DynamoDBClient, PutItemCommand } = await import('@aws-sdk/client-dynamodb');
          const ddb = new DynamoDBClient({ region: process.env.AWS_REGION || 'us-east-1' });
          await ddb.send(new PutItemCommand({
            TableName: aliasTable,
            Item: {
              hostname:         { S: rawHostname },
              environment_name: { S: orphanName || nodeId },
              updatedAt:        { S: new Date().toISOString() },
              updatedBy:        { S: 'connect-orphan-node' },
            },
          }));
        } catch (aliasErr) {
          // Best-effort — do not fail the overall connect operation
          console.warn('[connect-orphan-node] failed to write alias entry:', aliasErr);
        }
      }

      return {
        statusCode: 200,
        headers: CORS_HEADERS,
        body: JSON.stringify({ success: true, message: `Connected "${orphanName || nodeId}" to "${sourceEnvName}"` }),
      };
    }

    // --- CSV upload via JSON envelope ---
    if (parsed.csv) {
      csvContent = parsed.csv;
      sourceFile = parsed.filename || 'api-upload';
    } else {
      // Not a recognised operation and no csv field — treat entire body as CSV
      csvContent = body;
    }
  } catch {
    // Not JSON — treat as raw CSV
    csvContent = body;
  }

  const records = parsePceCsv(csvContent);
  const config = getOrchestratorConfig();
  const result = await runDiscoveryPipeline(records, config, 'csv-upload', sourceFile);

  return {
    statusCode: 200,
    headers: CORS_HEADERS,
    body: JSON.stringify(result),
  };
}

/**
 * Handle S3 PutObject event — read CSV file from S3
 */
async function handleS3Event(event: S3Event): Promise<void> {
  const { S3Client, GetObjectCommand, CopyObjectCommand, DeleteObjectCommand } = await import('@aws-sdk/client-s3');
  const s3Client = new S3Client({});

  for (const record of event.Records) {
    const bucket = record.s3.bucket.name;
    const key = decodeURIComponent(record.s3.object.key.replace(/\+/g, ' '));

    // Skip non-CSV files and processed files
    if (!key.toLowerCase().endsWith('.csv') || key.startsWith('processed/')) {
      console.log(`Skipping non-CSV or already processed file: ${key}`);
      continue;
    }

    console.log(`Processing CSV from S3: s3://${bucket}/${key}`);

    // Read CSV content
    const response = await s3Client.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
    const csvContent = await response.Body?.transformToString('utf-8');

    if (!csvContent) {
      console.error(`Empty CSV file: s3://${bucket}/${key}`);
      continue;
    }

    const records: PceRecord[] = parsePceCsv(csvContent);
    const config = getOrchestratorConfig();
    const sourceFile = key.split('/').pop() || key;

    const result = await runDiscoveryPipeline(records, config, 'csv-upload', sourceFile);
    console.log(`S3 CSV pipeline result:`, JSON.stringify(result.summary));

    // Move processed file to processed/ prefix
    const processedKey = `processed/${sourceFile}`;
    await s3Client.send(new CopyObjectCommand({
      Bucket: bucket,
      CopySource: `${bucket}/${key}`,
      Key: processedKey,
    }));
    await s3Client.send(new DeleteObjectCommand({ Bucket: bucket, Key: key }));
    console.log(`Moved ${key} → ${processedKey}`);
  }
}
