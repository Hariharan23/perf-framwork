// Pipeline Config Lambda
// Handles all CRUD operations for pipeline environment configs (SSM) and
// EventBridge Scheduler management. Invoked by the frontend via API Gateway.
//
// Operations:
//   list-source-types  → registered connector schemas
//   list-environments  → all envs for a source type (password masked)
//   upsert-environment → create or update env config in SSM SecureString
//   delete-environment → remove SSM param + EventBridge schedule
//   upsert-schedule    → create or update EventBridge rate schedule
//   toggle-schedule    → enable / disable an existing schedule
//   delete-schedule    → remove EventBridge schedule
//   trigger-sync       → async-invoke pipeline-sync Lambda
//   get-run-history    → recent runs from DynamoDB

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
import { DynamoDBClient, QueryCommand } from '@aws-sdk/client-dynamodb';
import { listConnectors, isValidSourceType } from '../shared/connector-registry';

const REGION           = process.env.AWS_REGION || 'us-east-1';
const SSM_PREFIX       = '/ems/pipelines';
const RUNS_TABLE       = process.env.PIPELINE_RUNS_TABLE || '';
const SYNC_LAMBDA_ARN  = process.env.PIPELINE_SYNC_LAMBDA_ARN || '';
const SCHEDULER_ROLE   = process.env.SCHEDULER_ROLE_ARN || '';

const ssm       = new SSMClient({ region: REGION });
const scheduler = new SchedulerClient({ region: REGION });
const lambdaClient = new LambdaClient({ region: REGION });
const dynamo    = new DynamoDBClient({ region: REGION });

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
  } catch (err: any) {
    if (err.name === 'ConflictException') {
      const res = await scheduler.send(new UpdateScheduleCommand(scheduleInput));
      schedulerArn = res.ScheduleArn || '';
    } else {
      throw err;
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
    pipelineId:       item.pipelineId?.S,
    runId:            item.runId?.S,
    status:           item.status?.S,
    recordsProcessed: Number(item.recordsProcessed?.N ?? 0),
    durationMs:       Number(item.durationMs?.N ?? 0),
    error:            item.error?.S || '',
    triggeredBy:      item.triggeredBy?.S,
  }));
}

// ── Utilities ─────────────────────────────────────────────────────────────────

function toRateExpression(frequency: number, unit: 'minutes' | 'hours' | 'days'): string {
  const singular = { minutes: 'minute', hours: 'hour', days: 'day' } as const;
  const plural   = { minutes: 'minutes', hours: 'hours', days: 'days' } as const;
  return `rate(${frequency} ${frequency === 1 ? singular[unit] : plural[unit]})`;
}
