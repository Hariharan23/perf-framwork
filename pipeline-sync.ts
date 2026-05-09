// Pipeline Sync Lambda
// Triggered by EventBridge Scheduler (per-environment schedule) or manually
// via the pipeline-config Lambda. Reads the environment config from SSM,
// fetches data through the appropriate connector, and feeds it into the
// existing Neptune discovery pipeline — identical to the PCE flow.

import { SSMClient, GetParameterCommand, PutParameterCommand } from '@aws-sdk/client-ssm';
import { DynamoDBClient, PutItemCommand } from '@aws-sdk/client-dynamodb';
import { getConnector } from '../shared/connector-registry';
import { runDiscoveryPipeline, OrchestratorConfig } from '../shared/discovery-orchestrator';

const REGION          = process.env.AWS_REGION || 'us-east-1';
const RUNS_TABLE      = process.env.PIPELINE_RUNS_TABLE || '';
const SSM_PREFIX      = '/ems/pipelines';
const TOPOLOGY_TABLE  = process.env.TOPOLOGY_TABLE_NAME || '';

const ssm    = new SSMClient({ region: REGION });
const dynamo = new DynamoDBClient({ region: REGION });

interface SyncEvent {
  sourceType: string;
  envId: string;
  triggeredBy?: string;
}

function orchestratorConfig(envId: string): OrchestratorConfig {
  return {
    neptuneEndpoint:        process.env.NEPTUNE_ENDPOINT || '',
    neptuneRegion:          REGION,
    aliasTableName:         process.env.ALIAS_TABLE_NAME,
    autoCreateEnvironments: true,
    topologyTableName:      TOPOLOGY_TABLE || undefined,
    sourceInstance:         envId,
  };
}

// ── Handler ───────────────────────────────────────────────────────────────────

export async function handler(event: SyncEvent): Promise<any> {
  const { sourceType, envId, triggeredBy = 'scheduler' } = event;
  const pipelineId = `${sourceType.toUpperCase()}#${envId}`;
  const runId      = new Date().toISOString();

  console.log(`[pipeline-sync] start pipelineId=${pipelineId} trigger=${triggeredBy}`);

  await writeRun(pipelineId, runId, 'running', 0, 0, '', triggeredBy);
  const start = Date.now();

  try {
    // 1. Load config from SSM (decrypted)
    const paramName = `${SSM_PREFIX}/${sourceType.toUpperCase()}/${envId}`;
    const paramRes  = await ssm.send(new GetParameterCommand({ Name: paramName, WithDecryption: true }));
    const config    = JSON.parse(paramRes.Parameter?.Value || '{}');
    // Inject envId so S3-based connectors (e.g. DCM) can build the correct S3 path
    config._envId = envId;

    // 2. Validate via connector
    const connector = getConnector(sourceType);
    const errors    = connector.validateConfig(config);
    if (errors.length > 0) throw new Error(`Config validation failed: ${errors.join('; ')}`);

    // 3. Fetch records
    console.log(`[pipeline-sync] fetching from ${sourceType}/${envId}`);
    const fetchResult = await connector.fetch(config);
    console.log(`[pipeline-sync] fetched ${fetchResult.records.length} records in ${fetchResult.durationMs}ms`);

    // 4. Ingest into Neptune (same pipeline as PCE)
    const pipelineResult = await runDiscoveryPipeline(fetchResult.records, orchestratorConfig(envId), 'pce-database');

    const durationMs = Date.now() - start;

    // 5. Update lastRun in SSM config (best-effort)
    await updateLastRun(paramName, config, runId, 'success', fetchResult.records.length);

    // 6. Record success
    await writeRun(pipelineId, runId, 'success', fetchResult.records.length, durationMs, '', triggeredBy, pipelineResult.summary);
    console.log(`[pipeline-sync] done pipelineId=${pipelineId} records=${fetchResult.records.length} ms=${durationMs}`);

    return { success: true, recordsProcessed: fetchResult.records.length, durationMs };
  } catch (err: any) {
    const durationMs = Date.now() - start;
    console.error(`[pipeline-sync] failed pipelineId=${pipelineId}`, err);
    await writeRun(pipelineId, runId, 'failed', 0, durationMs, err.message, triggeredBy);
    throw err;
  }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

async function updateLastRun(
  paramName: string,
  config: any,
  runId: string,
  status: string,
  recordsProcessed: number,
): Promise<void> {
  try {
    const updated = {
      ...config,
      lastRun: { runId, status, recordsProcessed, completedAt: new Date().toISOString() },
    };
    await ssm.send(new PutParameterCommand({
      Name: paramName,
      Value: JSON.stringify(updated),
      Type: 'SecureString',
      Overwrite: true,
    }));
  } catch (e) {
    console.warn('[pipeline-sync] could not update lastRun in SSM:', e);
  }
}

async function writeRun(
  pipelineId: string,
  runId: string,
  status: string,
  recordsProcessed: number,
  durationMs: number,
  error: string,
  triggeredBy: string,
  summary?: {
    totalEnvironmentsCreated: number;
    totalIntegrationsCreated: number;
    totalIntegrationsSkipped: number;
    totalIntegrationsStale: number;
    totalNonUrlConfigs: number;
    totalStubs: number;
    totalErrors: number;
  },
): Promise<void> {
  if (!RUNS_TABLE) return;
  try {
    await dynamo.send(new PutItemCommand({
      TableName: RUNS_TABLE,
      Item: {
        pipelineId:       { S: pipelineId },
        runId:            { S: runId },
        status:           { S: status },
        recordsProcessed: { N: String(recordsProcessed) },
        durationMs:       { N: String(durationMs) },
        error:            { S: error },
        triggeredBy:      { S: triggeredBy },
        ...(summary && {
          envsCreated:           { N: String(summary.totalEnvironmentsCreated) },
          integrationsCreated:   { N: String(summary.totalIntegrationsCreated) },
          integrationsSkipped:   { N: String(summary.totalIntegrationsSkipped) },
          integrationsStale:     { N: String(summary.totalIntegrationsStale) },
          nonUrlConfigs:         { N: String(summary.totalNonUrlConfigs) },
          stubs:                 { N: String(summary.totalStubs) },
          summaryErrors:         { N: String(summary.totalErrors) },
        }),
        // TTL: 30 days from now
        ttl: { N: String(Math.floor(Date.now() / 1000) + 30 * 24 * 60 * 60) },
      },
    }));
  } catch (e) {
    console.error('[pipeline-sync] failed to write run record:', e);
  }
}
