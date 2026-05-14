// Pipeline Config Lambda
// Handles all CRUD operations for pipeline environment configs (SSM) and
// EventBridge Scheduler management. Invoked by the frontend via API Gateway.
//
// Operations:
//   list-source-types     → registered connector schemas
//   list-environments     → all envs for a source type (password masked)
//   upsert-environment    → create or update env config in SSM SecureString
//   delete-environment    → remove SSM param + EventBridge schedule
//   upsert-schedule       → create or update EventBridge rate schedule
//   toggle-schedule       → enable / disable an existing schedule
//   delete-schedule       → remove EventBridge schedule
//   trigger-sync          → async-invoke pipeline-sync Lambda
//   get-run-history       → recent runs from DynamoDB
//
// DCM file operations (S3 bucket defined by DCM_BUCKET_NAME):
//   upload-dcm-properties → upload a .properties file to S3 for a DCM environment/app
//   list-dcm-files        → list all .properties files in S3 for a DCM environment
//
// Topology operations (pipeline-topology DynamoDB table):
//   get-topology          → all blocks + merged edges (global + per-instance overrides)
//   upsert-mapping-edge   → create or replace a mapping rule (global or per-instance)
//   delete-mapping-edge   → remove a mapping rule by pk
//   toggle-mapping-edge   → flip enabled flag on an existing edge
//   reset-instance-edges  → delete all per-instance edges for a source+instance pair
//   list-connectors       → list registered connectors from DynamoDB (DynamoDB-first)
//   register-connector    → register a new source type + scaffold default edges

import { APIGatewayProxyEvent, Context } from 'aws-lambda';
import {
  SSMClient,
  PutParameterCommand,
  GetParameterCommand,
  GetParametersByPathCommand,
  DeleteParameterCommand,
} from '@aws-sdk/client-ssm';
import {
  SchedulerClient,
  CreateScheduleCommand,
  UpdateScheduleCommand,
  DeleteScheduleCommand,
  GetScheduleCommand,
} from '@aws-sdk/client-scheduler';
import { LambdaClient, InvokeCommand } from '@aws-sdk/client-lambda';
import { S3Client, PutObjectCommand, ListObjectsV2Command, DeleteObjectCommand } from '@aws-sdk/client-s3';
import {
  DynamoDBClient,
  QueryCommand,
  ScanCommand,
  GetItemCommand,
  PutItemCommand,
  UpdateItemCommand,
  DeleteItemCommand,
} from '@aws-sdk/client-dynamodb';
import { marshall, unmarshall } from '@aws-sdk/util-dynamodb';
import { randomUUID } from 'crypto';
import { listConnectors, isValidSourceType, autoRegisterToDynamo } from '../shared/connector-registry';
import { EdgeTemplate } from '../shared/connectors/base-connector';

const REGION           = process.env.AWS_REGION || 'us-east-1';
const SSM_PREFIX       = '/ems/pipelines';
const RUNS_TABLE       = process.env.PIPELINE_RUNS_TABLE || '';
const TOPOLOGY_TABLE   = process.env.TOPOLOGY_TABLE_NAME || '';
const SYNC_LAMBDA_ARN  = process.env.PIPELINE_SYNC_LAMBDA_ARN || '';
const SCHEDULER_ROLE   = process.env.SCHEDULER_ROLE_ARN || '';
const DCM_BUCKET       = process.env.DCM_BUCKET_NAME || '';
const PC_BUCKET        = process.env.PC_BUCKET_NAME || '';

const ssm          = new SSMClient({ region: REGION });
const scheduler    = new SchedulerClient({ region: REGION });
const lambdaClient = new LambdaClient({ region: REGION });
const dynamo       = new DynamoDBClient({ region: REGION });

// Seed built-in connectors into topology table on cold-start (idempotent)
if (TOPOLOGY_TABLE) {
  autoRegisterToDynamo(TOPOLOGY_TABLE, REGION).catch((e) =>
    console.warn('autoRegisterToDynamo failed (non-fatal):', e?.message),
  );
}

const CORS = {
  'Content-Type': 'application/json',
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, x-api-key, Authorization',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
};

// ── Handler ──────────────────────────────────────────────────────────────────

export async function handler(event: APIGatewayProxyEvent, _ctx: Context): Promise<any> {
  if (event.httpMethod === 'OPTIONS') return { statusCode: 200, headers: CORS, body: '' };

  try {
    const body      = JSON.parse(event.body || '{}');
    const operation = body.operation as string;

    switch (operation) {
      case 'list-source-types':
        return ok(listConnectors());

      case 'list-environments':
        return ok(await listEnvironments(body.sourceType));

      case 'upsert-environment':
        return ok(await upsertEnvironment(body.sourceType, body.envId, body.config));

      case 'delete-environment':
        return ok(await deleteEnvironment(body.sourceType, body.envId));

      case 'upsert-schedule':
        return ok(await upsertSchedule(body.sourceType, body.envId, body.schedule));

      case 'toggle-schedule':
        return ok(await toggleSchedule(body.sourceType, body.envId, body.enabled));

      case 'delete-schedule':
        return ok(await deleteScheduleOp(body.sourceType, body.envId));

      case 'trigger-sync':
        return ok(await triggerSync(body.sourceType, body.envId));

      case 'get-run-history':
        return ok(await getRunHistory(body.sourceType, body.envId, body.limit));

      // ── Topology operations ──────────────────────────────────────────────

      case 'get-topology':
        return ok(await getTopology(body.envId));

      case 'upsert-mapping-edge':
        return ok(await upsertMappingEdge(body.edge));

      case 'delete-mapping-edge':
        return ok(await deleteMappingEdge(body.pk));

      case 'toggle-mapping-edge':
        return ok(await toggleMappingEdge(body.pk, body.enabled));

      case 'reset-instance-edges':
        return ok(await resetInstanceEdges(body.fromSource, body.fromInstance));

      case 'list-connectors':
        return ok(await listConnectorsFromDynamo());

      case 'register-connector':
        return ok(await registerConnector(body.connector));

      case 'update-connector-priority': {
        const { sourceType: srcType, priority: newPriority } = body;
        if (!srcType) return fail(400, 'sourceType is required');
        const parsed = parseInt(String(newPriority), 10);
        if (!Number.isInteger(parsed) || parsed < 1) return fail(400, 'priority must be a positive integer');
        return ok(await updateConnectorPriority(srcType, parsed));
      }

      // ── DCM file operations ───────────────────────────────────────────────

      case 'upload-dcm-properties':
        return ok(await uploadDcmProperties(body.envId, body.appName, body.filename, body.content));

      case 'list-dcm-files':
        return ok(await listDcmFiles(body.envId));

      case 'delete-dcm-file':
        return ok(await deleteDcmFile(body.s3Key));

      // ── Payment Central file operations ──────────────────────────────────

      case 'upload-pc-properties':
        return ok(await uploadPcFile(body.envId, body.filename, body.content));

      case 'list-pc-files':
        return ok(await listPcFiles(body.envId));

      case 'delete-pc-file':
        return ok(await deletePcFile(body.s3Key));

      default:
        return fail(400, `Unknown operation: ${operation}`);
    }
  } catch (err: any) {
    console.error('Pipeline config error:', err);
    return fail(500, err.message);
  }
}

// ── Helpers ──────────────────────────────────────────────────────────────────

function ok(data: any) {
  return { statusCode: 200, headers: CORS, body: JSON.stringify(data) };
}

function fail(status: number, message: string) {
  return { statusCode: status, headers: CORS, body: JSON.stringify({ error: message }) };
}

/** SSM parameter path for an environment config */
function ssmPath(sourceType: string, envId: string): string {
  return `${SSM_PREFIX}/${sourceType.toUpperCase()}/${sanitize(envId)}`;
}

/** EventBridge Scheduler name for an environment */
function schedulerName(sourceType: string, envId: string): string {
  return `ems-${sourceType.toUpperCase()}-${sanitize(envId)}`.slice(0, 64);
}

/** Allow only safe characters for SSM path segments and scheduler names */
function sanitize(s: string): string {
  return s.replace(/[^a-zA-Z0-9\-_.]/g, '-').slice(0, 64);
}

/** Mask sensitive fields before returning to the frontend */
function maskSensitive(config: any): any {
  const out = { ...config };
  if (out.password) out.password = '••••••••';
  return out;
}

// ── SSM helpers ──────────────────────────────────────────────────────────────

async function getFullConfig(sourceType: string, envId: string): Promise<any | null> {
  try {
    const res = await ssm.send(new GetParameterCommand({
      Name: ssmPath(sourceType, envId),
      WithDecryption: true,
    }));
    return JSON.parse(res.Parameter?.Value || '{}');
  } catch {
    return null;
  }
}

async function putConfig(sourceType: string, envId: string, config: any): Promise<void> {
  await ssm.send(new PutParameterCommand({
    Name: ssmPath(sourceType, envId),
    Value: JSON.stringify(config),
    Type: 'SecureString',
    Overwrite: true,
    Description: `EMS Pipeline config — ${sourceType} / ${envId}`,
  }));
}

// ── Operations ───────────────────────────────────────────────────────────────

async function listEnvironments(sourceType: string): Promise<any[]> {
  if (!sourceType || !isValidSourceType(sourceType)) {
    throw new Error(`Invalid source type: ${sourceType}`);
  }
  const path    = `${SSM_PREFIX}/${sourceType.toUpperCase()}`;
  const results: any[] = [];
  let nextToken: string | undefined;

  do {
    const res = await ssm.send(new GetParametersByPathCommand({
      Path: path,
      Recursive: false,
      WithDecryption: true,
      NextToken: nextToken,
    }));
    for (const param of res.Parameters || []) {
      try {
        results.push(maskSensitive(JSON.parse(param.Value || '{}')));
      } catch { /* skip malformed */ }
    }
    nextToken = res.NextToken;
  } while (nextToken);

  return results;
}

async function upsertEnvironment(sourceType: string, envId: string, config: any): Promise<any> {
  if (!isValidSourceType(sourceType)) throw new Error(`Invalid source type: ${sourceType}`);
  if (!envId || !/^[a-zA-Z0-9\-_.]+$/.test(envId)) {
    throw new Error('envId must contain only letters, numbers, hyphens, underscores, or dots');
  }

  let finalConfig = { ...config, sourceType: sourceType.toUpperCase(), envId };

  // If password is the masked placeholder, preserve the existing stored password
  if (finalConfig.password === '••••••••') {
    const existing = await getFullConfig(sourceType, envId);
    if (existing?.password) {
      finalConfig.password = existing.password;
      // Preserve existing schedule metadata
      if (existing.schedule && !finalConfig.schedule) {
        finalConfig.schedule = existing.schedule;
      }
    } else {
      throw new Error('Password is required for a new environment');
    }
  }

  await putConfig(sourceType, envId, finalConfig);
  return { success: true, envId, sourceType: sourceType.toUpperCase() };
}

async function deleteEnvironment(sourceType: string, envId: string): Promise<any> {
  // Remove EventBridge schedule first (ignore if not found)
  try {
    await scheduler.send(new DeleteScheduleCommand({ Name: schedulerName(sourceType, envId) }));
  } catch { /* schedule may not exist */ }

  await ssm.send(new DeleteParameterCommand({ Name: ssmPath(sourceType, envId) }));
  return { success: true };
}

async function upsertSchedule(
  sourceType: string,
  envId: string,
  schedule: { frequency: number; unit: 'minutes' | 'hours' | 'days'; enabled: boolean },
): Promise<any> {
  const name       = schedulerName(sourceType, envId);
  const expression = toRateExpression(schedule.frequency, schedule.unit);
  const payload    = JSON.stringify({ sourceType, envId, triggeredBy: 'scheduler' });

  const scheduleInput = {
    Name: name,
    ScheduleExpression: expression,
    ScheduleExpressionTimezone: 'UTC',
    State: schedule.enabled ? ('ENABLED' as const) : ('DISABLED' as const),
    Target: {
      Arn: SYNC_LAMBDA_ARN,
      RoleArn: SCHEDULER_ROLE,
      Input: payload,
    },
    FlexibleTimeWindow: { Mode: 'OFF' as const },
  };

  let schedulerArn: string;
  try {
    const res = await scheduler.send(new CreateScheduleCommand(scheduleInput));
    schedulerArn = res.ScheduleArn || '';
  } catch (createErr: any) {
    // Schedule already exists — name === 'ConflictException' is the SDK v3 norm,
    // but also check httpStatusCode 409 as a fallback for any SDK version quirks.
    const isConflict =
      createErr.name === 'ConflictException' ||
      createErr.__type === 'ConflictException' ||
      createErr.$metadata?.httpStatusCode === 409;
    if (isConflict) {
      const res = await scheduler.send(new UpdateScheduleCommand(scheduleInput));
      schedulerArn = res.ScheduleArn || '';
    } else {
      throw createErr;
    }
  }

  // Persist schedule metadata back into the SSM config
  const existing = await getFullConfig(sourceType, envId);
  if (existing) {
    existing.schedule = {
      ...schedule,
      schedulerName: name,
      schedulerArn,
      updatedAt: new Date().toISOString(),
    };
    await putConfig(sourceType, envId, existing);
  }

  return { success: true, schedulerArn };
}

async function toggleSchedule(sourceType: string, envId: string, enabled: boolean): Promise<any> {
  const name    = schedulerName(sourceType, envId);
  const current = await scheduler.send(new GetScheduleCommand({ Name: name }));

  await scheduler.send(new UpdateScheduleCommand({
    Name: name,
    ScheduleExpression: current.ScheduleExpression!,
    ScheduleExpressionTimezone: current.ScheduleExpressionTimezone,
    State: enabled ? 'ENABLED' : 'DISABLED',
    Target: current.Target!,
    FlexibleTimeWindow: current.FlexibleTimeWindow!,
  }));

  const existing = await getFullConfig(sourceType, envId);
  if (existing?.schedule) {
    existing.schedule.enabled  = enabled;
    existing.schedule.updatedAt = new Date().toISOString();
    await putConfig(sourceType, envId, existing);
  }

  return { success: true, enabled };
}

async function deleteScheduleOp(sourceType: string, envId: string): Promise<any> {
  try {
    await scheduler.send(new DeleteScheduleCommand({ Name: schedulerName(sourceType, envId) }));
  } catch { /* may not exist */ }

  const existing = await getFullConfig(sourceType, envId);
  if (existing) {
    delete existing.schedule;
    await putConfig(sourceType, envId, existing);
  }

  return { success: true };
}

async function triggerSync(sourceType: string, envId: string): Promise<any> {
  await lambdaClient.send(new InvokeCommand({
    FunctionName: SYNC_LAMBDA_ARN,
    InvocationType: 'Event',   // fire-and-forget
    Payload: Buffer.from(JSON.stringify({ sourceType, envId, triggeredBy: 'manual' })),
  }));
  return { success: true, message: 'Sync triggered asynchronously' };
}

async function getRunHistory(sourceType: string, envId: string, limit = 25): Promise<any[]> {
  if (!RUNS_TABLE) return [];
  const pipelineId = `${sourceType.toUpperCase()}#${envId}`;
  const res = await dynamo.send(new QueryCommand({
    TableName: RUNS_TABLE,
    KeyConditionExpression: 'pipelineId = :pid',
    ExpressionAttributeValues: { ':pid': { S: pipelineId } },
    ScanIndexForward: false,
    Limit: Math.min(limit, 100),
  }));

  return (res.Items || []).map((item) => ({
    pipelineId:          item.pipelineId?.S,
    runId:               item.runId?.S,
    status:              item.status?.S,
    recordsProcessed:    Number(item.recordsProcessed?.N ?? 0),
    durationMs:          Number(item.durationMs?.N ?? 0),
    error:               item.error?.S || '',
    triggeredBy:         item.triggeredBy?.S,
    envsCreated:         item.envsCreated         ? Number(item.envsCreated.N)         : null,
    integrationsCreated: item.integrationsCreated ? Number(item.integrationsCreated.N) : null,
    integrationsStale:   item.integrationsStale   ? Number(item.integrationsStale.N)   : null,
    autoCleanedNodes:         item.autoCleanedNodes         ? Number(item.autoCleanedNodes.N)         : null,
    resolvedByAliasMap:       item.resolvedByAliasMap       ? Number(item.resolvedByAliasMap.N)       : null,
    resolvedByNeptuneLookup:  item.resolvedByNeptuneLookup  ? Number(item.resolvedByNeptuneLookup.N)  : null,
    resolvedByAutoCreate:     item.resolvedByAutoCreate      ? Number(item.resolvedByAutoCreate.N)     : null,
    resolvedByInternalDomain: item.resolvedByInternalDomain ? Number(item.resolvedByInternalDomain.N) : null,
    summaryErrors:            item.summaryErrors             ? Number(item.summaryErrors.N)            : null,
  }));
}

// ── Utilities ─────────────────────────────────────────────────────────────────

function toRateExpression(frequency: number, unit: 'minutes' | 'hours' | 'days'): string {
  const singular = { minutes: 'minute', hours: 'hour', days: 'day' } as const;
  const plural   = { minutes: 'minutes', hours: 'hours', days: 'days' } as const;
  return `rate(${frequency} ${frequency === 1 ? singular[unit] : plural[unit]})`;
}

// ── Topology Operations ───────────────────────────────────────────────────────

/**
 * Returns the topology configuration for the canvas.
 *
 * If `envId` is supplied the response contains merged edges:
 *   - Global edges (fromInstance absent / null)
 *   - Per-instance overrides for `envId`
 * For each (fromSource, toEntityType, mode) key, an instance edge fully
 * replaces the corresponding global edge.
 *
 * Without `envId` all edges are returned unmerged (used by the full topology view).
 */
async function getTopology(envId?: string): Promise<any> {
  if (!TOPOLOGY_TABLE) throw new Error('TOPOLOGY_TABLE_NAME not configured');

  // Scan all records — table is small (O(100s) of rows)
  const res = await dynamo.send(new ScanCommand({ TableName: TOPOLOGY_TABLE }));
  const all  = (res.Items || []).map((i) => unmarshall(i));

  const blocks     = all.filter((r) => r.pk.startsWith('BLOCK#'));
  const connectors = all.filter((r) => r.pk.startsWith('CONNECTOR#'));
  const allEdges   = all.filter((r) => r.pk.startsWith('EDGE#'));

  if (!envId) {
    return { blocks, connectors, edges: allEdges };
  }

  // Merge: for same (fromSource, toEntityType, mode) key, instance edge wins over global
  const globalEdges   = allEdges.filter((e) => !e.fromInstance);
  const instanceEdges = allEdges.filter((e) => e.fromInstance === envId);

  const mergeKey = (e: any) => `${e.fromSource}|${e.toEntityType}|${e.mode}`;
  const overrideKeys = new Set(instanceEdges.map(mergeKey));

  const merged = [
    // Keep global edges that are NOT overridden by an instance edge
    ...globalEdges
      .filter((e) => !overrideKeys.has(mergeKey(e)))
      .map((e) => ({ ...e, _inherited: true })),
    // Instance edges (tagged so the UI can highlight them)
    ...instanceEdges.map((e) => ({ ...e, _instanceOverride: true })),
  ];

  return {
    blocks,
    connectors,
    edges: merged,
    envId,
    instanceOverrideCount: instanceEdges.length,
  };
}

/** Mapping edge fields accepted for upsert. */
interface MappingEdgeInput {
  pk?: string;                  // if supplied = update; absent = insert (new uuid)
  fromSource: string;
  fromInstance?: string | null; // null / absent = global
  toEntityType: 'Environment' | 'Integration';
  mode: 'upsert' | 'enrich-only' | 'create-child' | 'resolve-scope';
  joinKey: 'hostname' | 'propertyKey' | 'envName' | 'fullUrl';
  matchBy: 'name' | 'id' | 'hostname';
  resolveFrom?: string;         // only for resolve-scope mode
  resolveFromInstance?: string; // only for resolve-scope mode
  priority?: number;
  description?: string;
  enabled?: boolean;
}

async function upsertMappingEdge(edge: MappingEdgeInput): Promise<any> {
  if (!TOPOLOGY_TABLE) throw new Error('TOPOLOGY_TABLE_NAME not configured');
  if (!edge.fromSource) throw new Error('edge.fromSource is required');
  if (!edge.toEntityType) throw new Error('edge.toEntityType is required');
  if (!edge.mode) throw new Error('edge.mode is required');

  const pk  = edge.pk || `EDGE#${randomUUID()}`;
  const now = new Date().toISOString();

  const item: Record<string, any> = {
    pk,
    fromSource:    edge.fromSource.toUpperCase(),
    toEntityType:  edge.toEntityType,
    mode:          edge.mode,
    joinKey:       edge.joinKey || 'hostname',
    matchBy:       edge.matchBy || 'name',
    priority:      edge.priority ?? 50,
    enabled:       edge.enabled !== false,
    description:   edge.description || '',
    updatedAt:     now,
  };

  // fromInstance: store only if explicitly set (absent = global edge)
  if (edge.fromInstance != null && edge.fromInstance !== '') {
    item.fromInstance = edge.fromInstance;
  }

  // resolve-scope fields
  if (edge.mode === 'resolve-scope') {
    if (!edge.resolveFrom) throw new Error('resolveFrom is required for resolve-scope mode');
    item.resolveFrom = edge.resolveFrom.toUpperCase();
    if (edge.resolveFromInstance) item.resolveFromInstance = edge.resolveFromInstance;
  }

  await dynamo.send(new PutItemCommand({
    TableName: TOPOLOGY_TABLE,
    Item: marshall(item),
  }));

  return { success: true, pk };
}

async function deleteMappingEdge(pk: string): Promise<any> {
  if (!TOPOLOGY_TABLE) throw new Error('TOPOLOGY_TABLE_NAME not configured');
  if (!pk || !pk.startsWith('EDGE#')) throw new Error('Valid EDGE# pk is required');

  await dynamo.send(new DeleteItemCommand({
    TableName: TOPOLOGY_TABLE,
    Key: marshall({ pk }),
  }));
  return { success: true };
}

async function toggleMappingEdge(pk: string, enabled: boolean): Promise<any> {
  if (!TOPOLOGY_TABLE) throw new Error('TOPOLOGY_TABLE_NAME not configured');
  if (!pk || !pk.startsWith('EDGE#')) throw new Error('Valid EDGE# pk is required');

  await dynamo.send(new UpdateItemCommand({
    TableName: TOPOLOGY_TABLE,
    Key: marshall({ pk }),
    UpdateExpression: 'SET enabled = :e, updatedAt = :t',
    ExpressionAttributeValues: marshall({ ':e': enabled, ':t': new Date().toISOString() }),
  }));
  return { success: true, enabled };
}

/**
 * Delete all per-instance edges for a given source+instance pair.
 * Used when an environment config is deleted or when a user wants to reset to global rules.
 */
async function resetInstanceEdges(fromSource: string, fromInstance: string): Promise<any> {
  if (!TOPOLOGY_TABLE) throw new Error('TOPOLOGY_TABLE_NAME not configured');
  if (!fromSource)   throw new Error('fromSource is required');
  if (!fromInstance) throw new Error('fromInstance is required');

  // Scan for matching EDGE# records (table is small; no need for full GSI query)
  const res = await dynamo.send(new ScanCommand({
    TableName: TOPOLOGY_TABLE,
    FilterExpression: 'begins_with(pk, :prefix) AND fromSource = :src AND fromInstance = :inst',
    ExpressionAttributeValues: marshall({
      ':prefix': 'EDGE#',
      ':src':    fromSource.toUpperCase(),
      ':inst':   fromInstance,
    }),
  }));

  const pks = (res.Items || []).map((i) => unmarshall(i).pk as string);
  await Promise.all(
    pks.map((pk) =>
      dynamo.send(new DeleteItemCommand({
        TableName: TOPOLOGY_TABLE,
        Key: marshall({ pk }),
      })),
    ),
  );

  return { success: true, deletedCount: pks.length };
}

/**
 * List registered connectors — DynamoDB-first, falls back to code registry.
 * Returns the same shape as ConnectorConfigSchema so the frontend can render
 * the "Add Environment" form dynamically for any source type.
 */
async function listConnectorsFromDynamo(): Promise<any[]> {
  if (!TOPOLOGY_TABLE) return listConnectors();

  const res = await dynamo.send(new ScanCommand({
    TableName: TOPOLOGY_TABLE,
    FilterExpression: 'begins_with(pk, :prefix)',
    ExpressionAttributeValues: marshall({ ':prefix': 'CONNECTOR#' }),
  }));
  const records = (res.Items || []).map((i) => unmarshall(i));

  if (records.length === 0) {
    // Fall back to code registry (auto-seed hasn't fired yet on this cold-start)
    return listConnectors();
  }

  return records.map((r) => ({
    sourceType:   r.sourceType,
    displayName:  r.displayName,
    description:  r.description,
    icon:         r.icon || '',
    priority:     r.priority ?? 100,
    fields:       JSON.parse(r.configSchema || '[]'),
    defaultEdges: JSON.parse(r.defaultEdges || '[]'),
    enabled:      r.enabled !== false,
    builtIn:      r.builtIn === true,
  }));
}

/**
 * Register a new source connector at runtime (no code changes needed).
 * Creates CONNECTOR#, BLOCK#, and default EDGE# records.
 * Existing records are never overwritten.
 */
async function registerConnector(connector: {
  sourceType: string;
  displayName: string;
  description?: string;
  icon?: string;
  priority?: number;
  fields?: any[];
  defaultEdges?: EdgeTemplate[];
  canvasX?: number;
  canvasY?: number;
}): Promise<any> {
  if (!TOPOLOGY_TABLE) throw new Error('TOPOLOGY_TABLE_NAME not configured');
  if (!connector.sourceType) throw new Error('sourceType is required');

  const src = connector.sourceType.toUpperCase();
  const now = new Date().toISOString();

  // CONNECTOR# record (skip if already exists)
  await dynamo.send(new PutItemCommand({
    TableName: TOPOLOGY_TABLE,
    Item: marshall({
      pk:           `CONNECTOR#${src}`,
      sourceType:   src,
      displayName:  connector.displayName || src,
      description:  connector.description || '',
      icon:         connector.icon || '',
      priority:     connector.priority ?? 100,
      configSchema: JSON.stringify(connector.fields || []),
      defaultEdges: JSON.stringify(connector.defaultEdges || []),
      builtIn:      false,
      createdAt:    now,
    }),
    ConditionExpression: 'attribute_not_exists(pk)',
  })).catch(() => { /* already exists — safe to ignore */ });

  // BLOCK# canvas record (skip if already exists)
  await dynamo.send(new PutItemCommand({
    TableName: TOPOLOGY_TABLE,
    Item: marshall({
      pk:          `BLOCK#${src}`,
      sourceType:  src,
      displayName: connector.displayName || src,
      icon:        connector.icon || '',
      canvasX:     connector.canvasX ?? 60,
      canvasY:     connector.canvasY ?? 80,
      enabled:     true,
      createdAt:   now,
    }),
    ConditionExpression: 'attribute_not_exists(pk)',
  })).catch(() => {});

  // Default EDGE# records
  const edges: string[] = [];
  for (const template of (connector.defaultEdges || [])) {
    const pk = `EDGE#${randomUUID()}`;
    await dynamo.send(new PutItemCommand({
      TableName: TOPOLOGY_TABLE,
      Item: marshall({
        pk,
        fromSource:   src,
        toEntityType: template.toEntityType,
        mode:         template.mode,
        joinKey:      template.joinKey,
        matchBy:      template.matchBy,
        priority:     template.priority,
        description:  template.description,
        enabled:      true,
        builtIn:      false,
        createdAt:    now,
        updatedAt:    now,
      }),
    })).catch(() => {});
    edges.push(pk);
  }

  return { success: true, sourceType: src, edgesCreated: edges.length, edgePks: edges };
}

/**
 * Update the execution priority of an existing connector record.
 * Lower number = higher priority (runs first).
 */
async function updateConnectorPriority(sourceType: string, priority: number): Promise<any> {
  if (!TOPOLOGY_TABLE) throw new Error('TOPOLOGY_TABLE_NAME not configured');
  const src = sourceType.toUpperCase();
  const now = new Date().toISOString();
  await dynamo.send(new UpdateItemCommand({
    TableName: TOPOLOGY_TABLE,
    Key: marshall({ pk: `CONNECTOR#${src}` }),
    UpdateExpression: 'SET #priority = :p, updatedAt = :now',
    ExpressionAttributeNames: { '#priority': 'priority' },
    ExpressionAttributeValues: marshall({ ':p': priority, ':now': now }),
    ConditionExpression: 'attribute_exists(pk)',
  }));
  return { success: true, sourceType: src, priority };
}

// ── DCM S3 helpers ────────────────────────────────────────────────────────────

/**
 * Upload a .properties file to the DCM S3 bucket.
 * S3 path: {envId}/{appName}/{filename}
 * content must be a base64-encoded string of the file bytes.
 */
async function uploadDcmProperties(
  envId: string,
  appName: string,
  filename: string,
  content: string,
): Promise<{ success: boolean; s3Key: string }> {
  if (!DCM_BUCKET) throw new Error('DCM_BUCKET_NAME is not configured on this Lambda');
  if (!envId)     throw new Error('envId is required');
  if (!appName)   throw new Error('appName is required');
  if (!filename)  throw new Error('filename is required');

  // Strict filename validation — only allow safe .properties filenames
  if (!/^[a-zA-Z0-9\-_.]+\.properties$/.test(filename)) {
    throw new Error('filename must end in .properties and contain only letters, numbers, hyphens, underscores or dots');
  }

  const s3Key = `${sanitize(envId)}/${sanitize(appName)}/${filename}`;
  const body  = Buffer.from(content, 'base64');

  const s3 = new S3Client({ region: REGION });
  await s3.send(new PutObjectCommand({
    Bucket:      DCM_BUCKET,
    Key:         s3Key,
    Body:        body,
    ContentType: 'text/plain; charset=utf-8',
  }));

  return { success: true, s3Key };
}

/**
 * List all .properties files uploaded for a DCM environment.
 * Returns S3 objects under {envId}/ grouped by app sub-folder.
 */
async function listDcmFiles(envId: string): Promise<{ key: string; appName: string; filename: string; size: number; lastModified: string }[]> {
  if (!DCM_BUCKET) throw new Error('DCM_BUCKET_NAME is not configured on this Lambda');
  if (!envId)      throw new Error('envId is required');

  const s3 = new S3Client({ region: REGION });
  const prefix = `${sanitize(envId)}/`;
  const results: { key: string; appName: string; filename: string; size: number; lastModified: string }[] = [];
  let continuationToken: string | undefined;

  do {
    const res = await s3.send(new ListObjectsV2Command({
      Bucket:            DCM_BUCKET,
      Prefix:            prefix,
      ContinuationToken: continuationToken,
    }));

    for (const obj of res.Contents ?? []) {
      const key = obj.Key ?? '';
      if (!key.endsWith('.properties')) continue;
      // strip leading envId/ prefix then split on first /
      const rest      = key.slice(prefix.length);
      const slashIdx  = rest.indexOf('/');
      const appName   = slashIdx >= 0 ? rest.slice(0, slashIdx)     : '';
      const filename  = slashIdx >= 0 ? rest.slice(slashIdx + 1)    : rest;
      results.push({
        key,
        appName,
        filename,
        size:         obj.Size ?? 0,
        lastModified: obj.LastModified?.toISOString() ?? '',
      });
    }

    continuationToken = res.IsTruncated ? res.NextContinuationToken : undefined;
  } while (continuationToken);

  return results;
}

/**
 * Delete a specific DCM .properties file from S3 by its full S3 key.
 * The caller (frontend) must supply the exact key returned by list-dcm-files.
 */
async function deleteDcmFile(s3Key: string): Promise<{ success: boolean }> {
  if (!DCM_BUCKET) throw new Error('DCM_BUCKET_NAME is not configured on this Lambda');
  if (!s3Key)      throw new Error('s3Key is required');
  // Prevent path traversal — key must not start with / or contain ..
  if (s3Key.startsWith('/') || s3Key.includes('..')) {
    throw new Error('Invalid s3Key');
  }

  const s3 = new S3Client({ region: REGION });
  await s3.send(new DeleteObjectCommand({ Bucket: DCM_BUCKET, Key: s3Key }));
  return { success: true };
}

// ── Payment Central S3 operations ────────────────────────────────────────────

/**
 * Upload a file for a Payment Central environment.
 * S3 path: {envId}/{filename}
 */
async function uploadPcFile(
  envId: string,
  filename: string,
  content: string,
): Promise<{ success: boolean; s3Key: string }> {
  if (!PC_BUCKET) throw new Error('PC_BUCKET_NAME is not configured on this Lambda');
  if (!envId)     throw new Error('envId is required');
  if (!filename)  throw new Error('filename is required');

  // Allow any safe filename (json, properties, csv, txt, etc.)
  if (!/^[a-zA-Z0-9\-_.]+$/.test(filename)) {
    throw new Error('filename must contain only letters, numbers, hyphens, underscores or dots');
  }

  const s3Key = `${sanitize(envId)}/${filename}`;
  const body  = Buffer.from(content, 'base64');

  const s3 = new S3Client({ region: REGION });
  await s3.send(new PutObjectCommand({
    Bucket:      PC_BUCKET,
    Key:         s3Key,
    Body:        body,
    ContentType: 'text/plain; charset=utf-8',
  }));

  return { success: true, s3Key };
}

/**
 * List all files uploaded for a Payment Central environment.
 */
async function listPcFiles(envId: string): Promise<{ key: string; filename: string; size: number; lastModified: string }[]> {
  if (!PC_BUCKET) throw new Error('PC_BUCKET_NAME is not configured on this Lambda');
  if (!envId)     throw new Error('envId is required');

  const s3 = new S3Client({ region: REGION });
  const prefix = `${sanitize(envId)}/`;
  const results: { key: string; filename: string; size: number; lastModified: string }[] = [];
  let continuationToken: string | undefined;

  do {
    const res = await s3.send(new ListObjectsV2Command({
      Bucket:            PC_BUCKET,
      Prefix:            prefix,
      ContinuationToken: continuationToken,
    }));

    for (const obj of res.Contents ?? []) {
      const key = obj.Key ?? '';
      const filename = key.slice(prefix.length);
      if (!filename) continue;
      results.push({
        key,
        filename,
        size:         obj.Size ?? 0,
        lastModified: obj.LastModified?.toISOString() ?? '',
      });
    }

    continuationToken = res.IsTruncated ? res.NextContinuationToken : undefined;
  } while (continuationToken);

  return results;
}

/**
 * Delete a specific Payment Central file from S3.
 */
async function deletePcFile(s3Key: string): Promise<{ success: boolean }> {
  if (!PC_BUCKET) throw new Error('PC_BUCKET_NAME is not configured on this Lambda');
  if (!s3Key)     throw new Error('s3Key is required');
  if (s3Key.startsWith('/') || s3Key.includes('..')) {
    throw new Error('Invalid s3Key');
  }

  const s3 = new S3Client({ region: REGION });
  await s3.send(new DeleteObjectCommand({ Bucket: PC_BUCKET, Key: s3Key }));
  return { success: true };
}
