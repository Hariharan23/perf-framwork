// PCE Discovery Lambda Handler
// Entry point for the PCE Discovery Pipeline
// Supports 2 trigger types:
//   1. S3 PutObject event     → read CSV from S3
//   2. API Gateway POST       → CSV in request body

import { APIGatewayProxyEvent, S3Event, Context } from 'aws-lambda';
import { parsePceCsv, PceRecord } from '../shared/pce-csv-parser';
import { runDiscoveryPipeline, OrchestratorConfig } from '../shared/discovery-orchestrator';
import { NeptuneSparqlClient } from '../shared/neptune-sparql-client';
import { toIntegrationName } from '../shared/key-parser';

const CORS_HEADERS = {
  'Content-Type': 'application/json',
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization, Accept, Origin, X-Requested-With, x-api-key',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
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

    // JSON operation dispatch (for non-CSV operations routed through API GW)
    if (source === 'api-gateway') {
      const contentType = (event.headers?.['content-type'] || event.headers?.['Content-Type'] || '').toLowerCase();
      const body = event.isBase64Encoded
        ? Buffer.from(event.body || '', 'base64').toString('utf-8')
        : event.body || '';
      if (contentType.includes('application/json') || body.trimStart().startsWith('{')) {
        try {
          const payload = JSON.parse(body);
          if (payload.operation) {
            return await handleJsonOperation(payload);
          }
        } catch { /* fall through to CSV handler */ }
      }
    }

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
 * Handle JSON operation requests (non-CSV API Gateway calls)
 */
async function handleJsonOperation(payload: any): Promise<any> {
  const { operation } = payload;
  const neptuneClient = new NeptuneSparqlClient(
    process.env.NEPTUNE_ENDPOINT || '',
    process.env.AWS_REGION || 'us-east-1',
  );

  switch (operation) {
    case 'list-pending-integrations': {
      const pending = await neptuneClient.listPendingIntegrations();
      return { statusCode: 200, headers: CORS_HEADERS, body: JSON.stringify(pending) };
    }

    case 'delete-pending-integration': {
      const { pendingId } = payload;
      if (!pendingId) return { statusCode: 400, headers: CORS_HEADERS, body: JSON.stringify({ error: 'pendingId required' }) };
      await neptuneClient.deletePendingIntegration(pendingId);
      return { statusCode: 200, headers: CORS_HEADERS, body: JSON.stringify({ success: true }) };
    }

    case 'connect-pending-integration': {
      const { pendingId, targetEnvName } = payload;
      if (!pendingId || !targetEnvName)
        return { statusCode: 400, headers: CORS_HEADERS, body: JSON.stringify({ error: 'pendingId and targetEnvName required' }) };

      // Fetch the stored pending integration record to get all original property details
      const allPending = await neptuneClient.listPendingIntegrations();
      const pending = allPending.find(p => p.id === pendingId);
      if (!pending)
        return { statusCode: 404, headers: CORS_HEADERS, body: JSON.stringify({ error: `PendingIntegration ${pendingId} not found` }) };

      // Look up (or create) source env
      const sourceEnvEntities = await neptuneClient.getEntitiesByTypeAndName('Environment', pending.sourceEnvironment);
      const sourceEnvId = sourceEnvEntities[0]?.id || pending.sourceEnvironmentId;
      if (!sourceEnvId)
        return { statusCode: 404, headers: CORS_HEADERS, body: JSON.stringify({ error: `Source environment "${pending.sourceEnvironment}" not found` }) };

      // Ensure target environment exists — create it with the stored hostname if new
      let targetEnvId: string | null = null;
      const existingTarget = await neptuneClient.getEntitiesByTypeAndName('Environment', targetEnvName);
      if (existingTarget.length > 0) {
        targetEnvId = existingTarget[0].id;
      } else {
        const created = await neptuneClient.createEnvironment({
          name: targetEnvName,
          type: 'Environment',
          description: `Manually linked via pending integration from ${pending.sourceEnvironment}`,
        });
        await neptuneClient.addConfigurationProperties(created.id, {
          rawHostname:     pending.rawHostname,
          createdBySource: pending.sourceEnvironment,
          createdByRun:    pending.createdByRun,
          createdAt:       new Date().toISOString(),
        });
        targetEnvId = created.id;
      }

      // Build integration name from the original property key — same algorithm as PCE discovery
      const integrationName = toIntegrationName(pending.propertyKey);
      const connectedAt = new Date().toISOString();

      // Create the Integration entity
      const integration = await neptuneClient.createIntegration({
        name: integrationName,
        type: 'Integration',
        description: `${pending.sourceEnvironment} → ${targetEnvName}`,
        status: 'active',
      });

      // Stamp ALL original property details — identical to what PCE auto-discovery writes
      const integrationConfig: Record<string, string> = {
        sourceEnvironment: pending.sourceEnvironment,
        targetEnvironment: targetEnvName,
        connectionType:    'url-property',
        hostname:          pending.rawHostname,
        rawHostname:       pending.rawHostname,
        propertyKey:       pending.propertyKey,
        fullUrl:           pending.fullUrl,
        protocol:          pending.protocol,
        port:              pending.port,
        path:              pending.path,
        endpointHint:      pending.endpointHint,
        resolutionMethod:  'manual-pending-connect',
        discoveredBy:      pending.createdByRun,
        discoveredAt:      pending.createdAt,
        connectedAt,
        createdBySource:   pending.sourceEnvironment,
        createdByRun:      pending.createdByRun,
      };
      if (pending.sourceFile) integrationConfig.sourceFile = pending.sourceFile;

      await neptuneClient.addConfigurationProperties(integration.id, integrationConfig);

      // Wire: sourceEnv → hasIntegration → Integration → integratesWith → targetEnv
      await neptuneClient.createRelationship(pending.sourceEnvironment, 'hasIntegration', integrationName, sourceEnvId, integration.id);
      await neptuneClient.createRelationship(integrationName, 'integratesWith', targetEnvName, integration.id, targetEnvId);

      // Clean up the pending record now that it's fully integrated
      await neptuneClient.deletePendingIntegration(pendingId);

      return { statusCode: 200, headers: CORS_HEADERS, body: JSON.stringify({
        success: true,
        integrationId:   integration.id,
        integrationName,
        targetEnvId,
        targetEnvCreated: existingTarget.length === 0,
      }) };
    }

    case 'list-orphan-nodes': {
      const orphans = await neptuneClient.listOrphanNodes();
      return { statusCode: 200, headers: CORS_HEADERS, body: JSON.stringify(orphans) };
    }

    case 'delete-orphan-node': {
      const { nodeId } = payload;
      if (!nodeId) return { statusCode: 400, headers: CORS_HEADERS, body: JSON.stringify({ error: 'nodeId required' }) };
      await neptuneClient.deleteEnvironmentNode(nodeId);
      return { statusCode: 200, headers: CORS_HEADERS, body: JSON.stringify({ success: true }) };
    }

    case 'connect-orphan-node': {
      const { nodeId, orphanName, sourceEnvName, rawHostname, createdByRun, createdAt,
              propertyKey, fullUrl, protocol, port, path, endpointHint, sourceFile } = payload;
      if (!nodeId || !orphanName || !sourceEnvName)
        return { statusCode: 400, headers: CORS_HEADERS, body: JSON.stringify({ error: 'nodeId, orphanName, sourceEnvName required' }) };

      // Look up sourceEnv id
      const sourceEnvEntities = await neptuneClient.getEntitiesByTypeAndName('Environment', sourceEnvName);
      const sourceEnvId = sourceEnvEntities[0]?.id;
      if (!sourceEnvId)
        return { statusCode: 404, headers: CORS_HEADERS, body: JSON.stringify({ error: `Source environment "${sourceEnvName}" not found in Neptune` }) };

      // Build integration name: same pattern as PCE discovery (from propertyKey if available)
      const integrationName = propertyKey
        ? propertyKey.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '')
        : `${sourceEnvName}_integratesWith_${orphanName}`;
      const connectedAt = new Date().toISOString();

      // Create the Integration entity
      const integration = await neptuneClient.createIntegration({
        name: integrationName,
        type: 'Integration',
        description: `${sourceEnvName} → ${orphanName} (manual connection)`,
        status: 'active',
      });

      // Stamp all configuration details — same keys as PCE discovery writes
      const integrationConfig: Record<string, string> = {
        sourceEnvironment: sourceEnvName,
        targetEnvironment: orphanName,
        connectionType:    'manual',
        hostname:          rawHostname || orphanName,
        rawHostname:       rawHostname || '',
        resolutionMethod:  'manual-connection',
        discoveredBy:      createdByRun || 'manual',
        discoveredAt:      createdAt    || connectedAt,
        connectedAt,
        createdBySource:   sourceEnvName,
        createdByRun:      createdByRun || '',
      };
      if (propertyKey)  integrationConfig.propertyKey  = propertyKey;
      if (fullUrl)      integrationConfig.fullUrl       = fullUrl;
      if (protocol)     integrationConfig.protocol      = protocol;
      if (port)         integrationConfig.port          = port;
      if (path)         integrationConfig.path          = path;
      if (endpointHint) integrationConfig.endpointHint  = endpointHint;
      if (sourceFile)   integrationConfig.sourceFile    = sourceFile;

      await neptuneClient.addConfigurationProperties(integration.id, integrationConfig);

      // Wire relationships exactly like PCE discovery:
      // 1. sourceEnv → hasIntegration → Integration
      await neptuneClient.createRelationship(sourceEnvName, 'hasIntegration', integrationName, sourceEnvId, integration.id);
      // 2. Integration → integratesWith → orphan node
      await neptuneClient.createRelationship(integrationName, 'integratesWith', orphanName, integration.id, nodeId);

      return { statusCode: 200, headers: CORS_HEADERS, body: JSON.stringify({ success: true, integrationId: integration.id, integrationName }) };
    }

    default:
      return { statusCode: 400, headers: CORS_HEADERS, body: JSON.stringify({ error: `Unknown operation: ${operation}` }) };
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
 * Handle API Gateway POST — CSV text in body
 */
async function handleApiGateway(event: APIGatewayProxyEvent): Promise<any> {
  const body = event.isBase64Encoded
    ? Buffer.from(event.body || '', 'base64').toString('utf-8')
    : event.body || '';

  if (!body.trim()) {
    return {
      statusCode: 400,
      headers: CORS_HEADERS,
      body: JSON.stringify({ error: 'Request body is empty. Send CSV content as the body.' }),
    };
  }

  // Check if it's JSON with a csv field, or raw CSV
  let csvContent: string;
  let sourceFile = 'api-upload';

  try {
    const parsed = JSON.parse(body);
    if (parsed.csv) {
      csvContent = parsed.csv;
      sourceFile = parsed.filename || 'api-upload';
    } else {
      // Not JSON with csv field — treat entire body as CSV
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
