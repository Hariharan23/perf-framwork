// Alias Manager Lambda
// Handles all alias CRUD and cleanser orchestration operations.
// Mounted on the PCE Discovery API Gateway at /alias-manager
//
// Operations:
//   list-aliases           → scan alias DynamoDB table
//   upsert-alias           → put/update alias, saving previousEnvironmentName
//   delete-alias           → delete alias by hostname
//   revert-alias           → swap environment_name ↔ previousEnvironmentName
//   preview-cleanser       → trigger cleanser-job in preview mode, return runId
//   trigger-cleanser       → trigger cleanser-job in live mode, return runId
//   get-cleanser-status    → query reconciler-runs table by runId
//   list-reconciler-runs   → scan reconciler-runs table, return recent 25 runs

import {
  DynamoDBClient,
  ScanCommand,
  GetItemCommand,
  PutItemCommand,
  DeleteItemCommand,
  QueryCommand,
} from '@aws-sdk/client-dynamodb';
import { LambdaClient, InvokeCommand } from '@aws-sdk/client-lambda';
import { v4 as uuidv4 } from 'uuid';

const REGION            = process.env.AWS_REGION || 'us-east-1';
const ALIAS_TABLE       = process.env.ALIAS_TABLE_NAME || '';
const RECONCILER_TABLE  = process.env.RECONCILER_RUNS_TABLE || '';
const CLEANSER_LAMBDA   = process.env.CLEANSER_LAMBDA_ARN || '';

const CORS = {
  'Content-Type': 'application/json',
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type,x-api-key',
  'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
};

const dynamo = new DynamoDBClient({ region: REGION });
const lambda = new LambdaClient({ region: REGION });

export async function handler(event: any): Promise<any> {
  if (event.httpMethod === 'OPTIONS') return ok({});

  const method = (event.httpMethod || 'POST').toUpperCase();
  const readOnlyOperations = new Set(['list-aliases', 'list-reconciler-runs']);
  let body: any;
  try {
    const raw = event.isBase64Encoded
      ? Buffer.from(event.body || '', 'base64').toString('utf-8')
      : event.body || '{}';
    body = JSON.parse(raw);
  } catch {
    body = {};
  }

  const { operation } = body.operation
    ? body
    : { operation: event.queryStringParameters?.operation };

  if (method === 'GET' && !operation) {
    return fail(400, 'Missing operation');
  }

  if (method === 'GET' && operation && !readOnlyOperations.has(operation)) {
    return fail(405, `GET is supported only for retrieval operations: ${Array.from(readOnlyOperations).join(', ')}`);
  }

  try {
    switch (operation) {
      case 'list-aliases':
        return ok(await listAliases());

      case 'upsert-alias': {
        const { hostname, environmentName, updatedBy = 'frontend-user' } = body;
        if (!hostname || !environmentName) return fail(400, 'hostname and environmentName required');
        validateSafeString(hostname, 'hostname');
        validateSafeString(environmentName, 'environmentName');
        return ok(await upsertAlias(hostname, environmentName, updatedBy));
      }

      case 'delete-alias': {
        const { hostname } = body;
        if (!hostname) return fail(400, 'hostname required');
        return ok(await deleteAlias(hostname));
      }

      case 'revert-alias': {
        const { hostname } = body;
        if (!hostname) return fail(400, 'hostname required');
        return ok(await revertAlias(hostname));
      }

      case 'preview-cleanser':
        return ok(await triggerCleanser(true, body.triggeredBy));

      case 'trigger-cleanser':
        return ok(await triggerCleanser(false, body.triggeredBy));

      case 'get-cleanser-status': {
        const { runId } = body;
        if (!runId) return fail(400, 'runId required');
        return ok(await getCleanserStatus(runId));
      }

      case 'list-reconciler-runs':
        return ok(await listReconcilerRuns(Number(body.limit ?? event.queryStringParameters?.limit ?? 25)));

      default:
        return fail(400, `Unknown operation: ${operation}`);
    }
  } catch (err: any) {
    console.error(`[alias-manager] operation=${operation} error:`, err);
    return fail(500, err.message || 'Internal error');
  }
}

// ── Operations ────────────────────────────────────────────────────────────────

async function listAliases(): Promise<any[]> {
  const items: any[] = [];
  let lastKey: Record<string, any> | undefined;
  do {
    const res = await dynamo.send(new ScanCommand({
      TableName: ALIAS_TABLE,
      ExclusiveStartKey: lastKey,
    }));
    for (const item of res.Items || []) {
      items.push({
        hostname:                item.hostname?.S || '',
        environmentName:         item.environment_name?.S || '',
        previousEnvironmentName: item.previousEnvironmentName?.S || null,
        updatedAt:               item.updatedAt?.S || null,
        updatedBy:               item.updatedBy?.S || null,
      });
    }
    lastKey = res.LastEvaluatedKey as Record<string, any> | undefined;
  } while (lastKey);
  return items;
}

async function upsertAlias(hostname: string, environmentName: string, updatedBy: string): Promise<any> {
  // Read current value to capture as previousEnvironmentName
  let previousEnvironmentName: string | undefined;
  try {
    const existing = await dynamo.send(new GetItemCommand({
      TableName: ALIAS_TABLE,
      Key: { hostname: { S: hostname } },
    }));
    const current = existing.Item?.environment_name?.S;
    if (current && current !== environmentName) {
      previousEnvironmentName = current;
    }
  } catch { /* first write — no previous */ }

  await dynamo.send(new PutItemCommand({
    TableName: ALIAS_TABLE,
    Item: {
      hostname:           { S: hostname },
      environment_name:   { S: environmentName },
      updatedAt:          { S: new Date().toISOString() },
      updatedBy:          { S: updatedBy },
      ...(previousEnvironmentName && {
        previousEnvironmentName: { S: previousEnvironmentName },
      }),
    },
  }));

  return { success: true, hostname, environmentName, previousEnvironmentName: previousEnvironmentName || null };
}

async function deleteAlias(hostname: string): Promise<any> {
  await dynamo.send(new DeleteItemCommand({
    TableName: ALIAS_TABLE,
    Key: { hostname: { S: hostname } },
  }));
  return { success: true };
}

async function revertAlias(hostname: string): Promise<any> {
  const res = await dynamo.send(new GetItemCommand({
    TableName: ALIAS_TABLE,
    Key: { hostname: { S: hostname } },
  }));
  const item = res.Item;
  if (!item) return { success: false, reason: 'alias not found' };

  const prev = item.previousEnvironmentName?.S;
  if (!prev) return { success: false, reason: 'no previous value to revert to' };

  const current = item.environment_name?.S || '';
  await dynamo.send(new PutItemCommand({
    TableName: ALIAS_TABLE,
    Item: {
      hostname:           { S: hostname },
      environment_name:   { S: prev },
      previousEnvironmentName: { S: current },
      updatedAt:          { S: new Date().toISOString() },
      updatedBy:          { S: 'revert' },
    },
  }));
  return { success: true, hostname, revertedTo: prev, previous: current };
}

async function triggerCleanser(isPreview: boolean, triggeredBy = 'frontend-user'): Promise<any> {
  const runId     = uuidv4();
  const now       = new Date().toISOString();
  const ttl       = Math.floor(Date.now() / 1000) + 90 * 24 * 60 * 60;

  // Write pending record
  await dynamo.send(new PutItemCommand({
    TableName: RECONCILER_TABLE,
    Item: {
      runId:       { S: runId },
      status:      { S: 'pending' },
      isPreview:   { BOOL: isPreview },
      triggeredBy: { S: triggeredBy },
      triggeredAt: { S: now },
      updatedAt:   { S: now },
      ttl:         { N: String(ttl) },
    },
  }));

  // Invoke cleanser-job async (fire-and-forget)
  await lambda.send(new InvokeCommand({
    FunctionName:   CLEANSER_LAMBDA,
    InvocationType: 'Event',
    Payload:        Buffer.from(JSON.stringify({ runId, isPreview, triggeredBy })),
  }));

  return { runId, status: 'pending', isPreview };
}

async function getCleanserStatus(runId: string): Promise<any> {
  const res = await dynamo.send(new GetItemCommand({
    TableName: RECONCILER_TABLE,
    Key: { runId: { S: runId } },
  }));
  const item = res.Item;
  if (!item) return { runId, status: 'not-found' };

  const stats = item.statsJson?.S ? JSON.parse(item.statsJson.S) : null;
  const plan  = item.planJson?.S  ? JSON.parse(item.planJson.S)  : null;

  return {
    runId,
    status:      item.status?.S,
    isPreview:   item.isPreview?.BOOL ?? false,
    triggeredBy: item.triggeredBy?.S,
    triggeredAt: item.triggeredAt?.S,
    updatedAt:   item.updatedAt?.S,
    stats,
    plan,
  };
}

async function listReconcilerRuns(limit: number): Promise<any[]> {
  const items: any[] = [];
  let lastKey: Record<string, any> | undefined;
  do {
    const res = await dynamo.send(new ScanCommand({
      TableName: RECONCILER_TABLE,
      ExclusiveStartKey: lastKey,
      ProjectionExpression: 'runId, #s, isPreview, triggeredBy, triggeredAt, updatedAt, statsJson, planJson',
      ExpressionAttributeNames: { '#s': 'status' },
    }));
    for (const item of res.Items || []) {
      items.push({
        runId:       item.runId?.S || '',
        status:      item.status?.S || 'unknown',
        isPreview:   item.isPreview?.BOOL ?? false,
        triggeredBy: item.triggeredBy?.S || null,
        triggeredAt: item.triggeredAt?.S || null,
        updatedAt:   item.updatedAt?.S   || null,
        stats:       item.statsJson?.S ? JSON.parse(item.statsJson.S) : null,
        plan:        item.planJson?.S  ? JSON.parse(item.planJson.S)  : null,
      });
    }
    lastKey = res.LastEvaluatedKey as Record<string, any> | undefined;
  } while (lastKey);

  // Sort newest-first and cap
  items.sort((a, b) => (b.triggeredAt || '').localeCompare(a.triggeredAt || ''));
  return items.slice(0, Math.min(limit, 50));
}

// ── Utilities ─────────────────────────────────────────────────────────────────

function ok(data: any) {
  return { statusCode: 200, headers: CORS, body: JSON.stringify(data) };
}
function fail(status: number, message: string) {
  return { statusCode: status, headers: CORS, body: JSON.stringify({ error: message }) };
}

/** Reject strings with SPARQL/DynamoDB injection characters */
function validateSafeString(value: string, field: string): void {
  if (/[<>"'\\]/.test(value)) {
    throw new Error(`${field} contains invalid characters`);
  }
}
