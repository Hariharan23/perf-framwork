/**
 * history-cleanup Lambda
 *
 * Handles history record retention management for the EMS Neptune graph.
 * All operations are exposed via POST /history-retention (API Gateway) and
 * the nightly EventBridge schedule invokes purge-history automatically.
 *
 * Operations:
 *   get-history-stats        → total count + age-bucket breakdown
 *   get-history-records      → paginated list filtered by age
 *   get-retention-config     → read retention days from SSM
 *   save-retention-config    → write retention days to SSM
 *   list-archives            → list .ndjson.gz files in S3 archive bucket
 *   purge-history            → archive to S3 then delete from Neptune
 *   restore-from-archive     → re-insert records from S3 archive into Neptune
 */

import { APIGatewayProxyEvent, APIGatewayProxyResult, Context } from 'aws-lambda';
import { NeptuneSparqlClient } from '../shared/neptune-sparql-client';
import {
  SSMClient,
  GetParameterCommand,
  PutParameterCommand,
} from '@aws-sdk/client-ssm';
import {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
  ListObjectsV2Command,
} from '@aws-sdk/client-s3';
import { createGzip, createGunzip } from 'zlib';
import { pipeline } from 'stream/promises';
import { PassThrough } from 'stream';

// ── Constants ─────────────────────────────────────────────────────────────────

const ONTOLOGY   = 'http://neptune.aws.com/envmgmt/ontology/';
const PAGE_SIZE  = 1000;
const BATCH_SIZE = 200;
const ARCHIVE_PREFIX = 'history-archives';
const SSM_RETENTION_KEY = process.env.SSM_RETENTION_KEY || '/ems/config/history-retention-days';
const ARCHIVE_BUCKET    = process.env.ARCHIVE_BUCKET    || '';
const REGION            = process.env.NEPTUNE_REGION    || process.env.AWS_REGION || 'us-east-1';
const DEFAULT_RETENTION_DAYS = 90;

// ── Clients ───────────────────────────────────────────────────────────────────

const neptuneClient = new NeptuneSparqlClient();
const ssm           = new SSMClient({ region: REGION });
const s3            = new S3Client({ region: REGION });

// ── CORS headers ──────────────────────────────────────────────────────────────

const headers = {
  'Content-Type': 'application/json',
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, x-api-key',
};

// ── Helpers ───────────────────────────────────────────────────────────────────

function ok(data: object, status = 200): APIGatewayProxyResult {
  return { statusCode: status, headers, body: JSON.stringify({ success: true, ...data }) };
}

function fail(status: number, error: string): APIGatewayProxyResult {
  return { statusCode: status, headers, body: JSON.stringify({ success: false, error }) };
}

function esc(s: string): string {
  return s.replace(/\\/g, '\\\\').replace(/"/g, '\\"');
}

function cutoffIso(days: number): string {
  return new Date(Date.now() - days * 86_400_000).toISOString();
}

function archiveS3Key(): string {
  const now = new Date();
  const ymd = now.toISOString().slice(0, 10).replace(/-/g, '/');
  return `${ARCHIVE_PREFIX}/${ymd}/archive-${now.toISOString().replace(/[:.]/g, '-')}.ndjson.gz`;
}

// ── SSM helpers ───────────────────────────────────────────────────────────────

async function getRetentionDays(): Promise<number> {
  try {
    const res = await ssm.send(new GetParameterCommand({ Name: SSM_RETENTION_KEY }));
    const val = parseInt(res.Parameter?.Value || '', 10);
    return isNaN(val) || val < 1 ? DEFAULT_RETENTION_DAYS : val;
  } catch {
    return DEFAULT_RETENTION_DAYS;
  }
}

async function saveRetentionDays(days: number): Promise<void> {
  await ssm.send(new PutParameterCommand({
    Name:      SSM_RETENTION_KEY,
    Value:     String(Math.max(1, days)),
    Type:      'String',
    Overwrite: true,
  }));
}

// ── S3 helpers ────────────────────────────────────────────────────────────────

async function uploadNdjsonGz(key: string, lines: string[], recordCount: number): Promise<void> {
  const ndjson = lines.join('\n') + '\n';
  const chunks: Buffer[] = [];
  await new Promise<void>((resolve, reject) => {
    const gz = createGzip();
    gz.on('data', (c: Buffer) => chunks.push(c));
    gz.on('end', resolve);
    gz.on('error', reject);
    gz.end(Buffer.from(ndjson, 'utf8'));
  });
  const body = Buffer.concat(chunks);

  await s3.send(new PutObjectCommand({
    Bucket:      ARCHIVE_BUCKET,
    Key:         key,
    Body:        body,
    ContentType: 'application/gzip',
    Metadata:    { recordCount: String(recordCount) },
  }));
}

async function downloadNdjsonGz(key: string): Promise<any[]> {
  const res = await s3.send(new GetObjectCommand({ Bucket: ARCHIVE_BUCKET, Key: key }));
  const stream = res.Body as Readable;
  const gunzip = createGunzip();
  const chunks: Buffer[] = [];
  const collector = new PassThrough();
  collector.on('data', (c: Buffer) => chunks.push(c));
  await pipeline(stream, gunzip, collector);
  const text = Buffer.concat(chunks).toString('utf8');
  return text.split('\n').filter(Boolean).map(l => JSON.parse(l));
}

// ── Neptune helpers ───────────────────────────────────────────────────────────

interface HistoryRecord {
  id: string;
  entityId: string;
  entityName: string;
  action: string;
  timestamp: string;
  user?: string;
  changes?: any;
}

async function countHistoryOlderThan(days: number): Promise<number> {
  const since = cutoffIso(days);
  const query = `
    PREFIX env: <${ONTOLOGY}>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT (COUNT(*) AS ?count) WHERE {
      ?h env:type "History" ;
         env:timestamp ?ts .
      FILTER (xsd:dateTime(?ts) < "${since}"^^xsd:dateTime)
    }
  `;
  const res = await (neptuneClient as any).executeSparqlQuery(query);
  const val = res.results?.bindings?.[0]?.count?.value;
  return val ? parseInt(val, 10) : 0;
}

async function countHistoryAll(): Promise<number> {
  const query = `
    PREFIX env: <${ONTOLOGY}>
    SELECT (COUNT(*) AS ?count) WHERE {
      ?h env:type "History" .
    }
  `;
  const res = await (neptuneClient as any).executeSparqlQuery(query);
  const val = res.results?.bindings?.[0]?.count?.value;
  return val ? parseInt(val, 10) : 0;
}

async function fetchHistoryPage(olderThanDays: number | null, offset: number, limit: number): Promise<HistoryRecord[]> {
  const filterClause = olderThanDays !== null
    ? `FILTER (xsd:dateTime(?ts) < "${cutoffIso(olderThanDays)}"^^xsd:dateTime)`
    : '';

  const query = `
    PREFIX env: <${ONTOLOGY}>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?id ?entityId ?entityName ?action ?ts ?user ?changes WHERE {
      ?h env:type "History" ;
         env:id ?id ;
         env:entityId ?entityId ;
         env:action ?action ;
         env:timestamp ?ts .
      OPTIONAL { ?h env:entityName ?entityName }
      OPTIONAL { ?h env:user ?user }
      OPTIONAL { ?h env:changes ?changes }
      ${filterClause}
    }
    ORDER BY DESC(?ts)
    LIMIT ${limit} OFFSET ${offset}
  `;
  const res = await (neptuneClient as any).executeSparqlQuery(query);
  return (res.results?.bindings || []).map((b: any): HistoryRecord => {
    const rec: HistoryRecord = {
      id:         b.id?.value         || '',
      entityId:   b.entityId?.value   || '',
      entityName: b.entityName?.value || '',
      action:     b.action?.value     || '',
      timestamp:  b.ts?.value         || '',
    };
    if (b.user?.value)    rec.user = b.user.value;
    if (b.changes?.value) {
      try { rec.changes = JSON.parse(b.changes.value); } catch { rec.changes = b.changes.value; }
    }
    return rec;
  });
}

async function fetchAllHistoryOlderThan(days: number): Promise<HistoryRecord[]> {
  const all: HistoryRecord[] = [];
  let offset = 0;
  while (true) {
    const page = await fetchHistoryPage(days, offset, PAGE_SIZE);
    all.push(...page);
    if (page.length < PAGE_SIZE) break;
    offset += PAGE_SIZE;
  }
  return all;
}

async function deleteHistoryNodes(records: HistoryRecord[]): Promise<number> {
  // Delete all triples where the subject has env:id matching one of our IDs
  const ids = records.map(r => r.id);
  let deleted = 0;

  for (let i = 0; i < ids.length; i += BATCH_SIZE) {
    const batch = ids.slice(i, i + BATCH_SIZE);
    const values = batch.map(id => `"${esc(id)}"`).join(' ');

    // Step 1: find the subject URIs for this batch
    const selectQuery = `
      PREFIX env: <${ONTOLOGY}>
      SELECT ?h WHERE {
        ?h env:type "History" ;
           env:id ?id .
        FILTER (?id IN (${values}))
      }
    `;
    const res = await (neptuneClient as any).executeSparqlQuery(selectQuery);
    const subjects: string[] = (res.results?.bindings || []).map((b: any) => b.h?.value).filter(Boolean);

    if (!subjects.length) {
      deleted += batch.length;
      continue;
    }

    // Step 2: delete ALL triples for each subject URI
    for (let j = 0; j < subjects.length; j += 50) {
      const subBatch = subjects.slice(j, j + 50);
      const uriValues = subBatch.map(u => `<${u}>`).join(' ');
      const deleteQuery = `
        PREFIX env: <${ONTOLOGY}>
        DELETE { ?h ?p ?o }
        WHERE {
          ?h ?p ?o .
          FILTER (?h IN (${uriValues}))
        }
      `;
      await (neptuneClient as any).executeSparqlUpdate(deleteQuery);
    }

    deleted += batch.length;
    console.log(`[history-cleanup] deleted batch of ${batch.length} history nodes (total=${deleted})`);
  }
  return deleted;
}

async function existsInNeptune(id: string): Promise<boolean> {
  const query = `
    PREFIX env: <${ONTOLOGY}>
    ASK { ?h env:id "${esc(id)}" ; env:type "History" }
  `;
  const res = await (neptuneClient as any).executeSparqlQuery(query);
  return res.boolean === true;
}

async function insertHistoryBatch(records: HistoryRecord[]): Promise<void> {
  for (let i = 0; i < records.length; i += BATCH_SIZE) {
    const batch = records.slice(i, i + BATCH_SIZE);
    let triples = '';
    for (const r of batch) {
      const uri = `${ONTOLOGY}entity/history_${r.id.replace(/[^a-zA-Z0-9_-]/g, '_')}`;
      triples += `  <${uri}> <${ONTOLOGY}type> "History" .\n`;
      triples += `  <${uri}> <${ONTOLOGY}id> "${esc(r.id)}" .\n`;
      triples += `  <${uri}> <${ONTOLOGY}entityId> "${esc(r.entityId)}" .\n`;
      triples += `  <${uri}> <${ONTOLOGY}entityName> "${esc(r.entityName || '')}" .\n`;
      triples += `  <${uri}> <${ONTOLOGY}action> "${esc(r.action)}" .\n`;
      triples += `  <${uri}> <${ONTOLOGY}timestamp> "${esc(r.timestamp)}"^^<http://www.w3.org/2001/XMLSchema#dateTime> .\n`;
      if (r.user)    triples += `  <${uri}> <${ONTOLOGY}user> "${esc(r.user)}" .\n`;
      if (r.changes) triples += `  <${uri}> <${ONTOLOGY}changes> "${esc(JSON.stringify(r.changes))}" .\n`;
    }
    const insertQuery = `INSERT DATA {\n${triples}}`;
    await (neptuneClient as any).executeSparqlUpdate(insertQuery);
  }
}

// ── Handler ───────────────────────────────────────────────────────────────────

export const handler = async (
  event: APIGatewayProxyEvent,
  _ctx: Context,
): Promise<APIGatewayProxyResult> => {
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers, body: '' };
  }

  let body: any = {};
  try {
    // EventBridge sends event directly (no httpMethod body), API Gateway sends JSON body
    body = event.body
      ? JSON.parse(event.body)
      : (event as any).operation ? event : {};
  } catch {
    return fail(400, 'Invalid JSON body');
  }

  const operation = body.operation || (event.queryStringParameters?.operation);
  console.log(`[history-cleanup] operation=${operation}`);

  // ── get-history-stats ────────────────────────────────────────────────────
  if (operation === 'get-history-stats') {
    const retentionDays = await getRetentionDays();
    const [total, older7, older30, older90, olderThanRetention] = await Promise.all([
      countHistoryAll(),
      countHistoryOlderThan(7),
      countHistoryOlderThan(30),
      countHistoryOlderThan(90),
      countHistoryOlderThan(retentionDays),
    ]);
    return ok({ total, last7d: total - older7, last30d: total - older30, last90d: total - older90, olderThanRetention, retentionDays });
  }

  // ── get-history-records ──────────────────────────────────────────────────
  if (operation === 'get-history-records') {
    const olderThanDays: number | null = body.olderThanDays != null ? Number(body.olderThanDays) : null;
    const pageSize = Math.min(Number(body.pageSize) || 100, 500);
    const page     = Math.max(Number(body.page) || 0, 0);
    const records  = await fetchHistoryPage(olderThanDays, page * pageSize, pageSize);
    return ok({ records, page, pageSize, count: records.length });
  }

  // ── get-retention-config ─────────────────────────────────────────────────
  if (operation === 'get-retention-config') {
    const days = await getRetentionDays();
    return ok({ days, ssmKey: SSM_RETENTION_KEY });
  }

  // ── save-retention-config ────────────────────────────────────────────────
  if (operation === 'save-retention-config') {
    const days = parseInt(String(body.days), 10);
    if (isNaN(days) || days < 1) return fail(400, '"days" must be a positive integer');
    await saveRetentionDays(days);
    return ok({ saved: true, days });
  }

  // ── list-archives ────────────────────────────────────────────────────────
  if (operation === 'list-archives') {
    if (!ARCHIVE_BUCKET) return fail(500, 'ARCHIVE_BUCKET not configured');
    const res = await s3.send(new ListObjectsV2Command({
      Bucket: ARCHIVE_BUCKET,
      Prefix: ARCHIVE_PREFIX + '/',
    }));
    const archives = (res.Contents || [])
      .filter(o => o.Key?.endsWith('.ndjson.gz'))
      .sort((a, b) => (b.LastModified?.getTime() || 0) - (a.LastModified?.getTime() || 0))
      .map(o => ({
        s3Key:        o.Key,
        sizeBytes:    o.Size,
        lastModified: o.LastModified?.toISOString(),
      }));
    return ok({ archives, count: archives.length });
  }

  // ── purge-history ────────────────────────────────────────────────────────
  if (operation === 'purge-history') {
    if (!ARCHIVE_BUCKET) return fail(500, 'ARCHIVE_BUCKET not configured');
    const dryRun: boolean = body.dryRun !== false && body.dryRun !== 'false';
    const retentionDays   = body.olderThanDays != null
      ? Math.max(1, Number(body.olderThanDays))
      : await getRetentionDays();

    console.log(`[history-cleanup] purge-history retentionDays=${retentionDays} dryRun=${dryRun}`);
    const records = await fetchAllHistoryOlderThan(retentionDays);
    console.log(`[history-cleanup] found ${records.length} history records older than ${retentionDays} days`);

    if (dryRun) {
      return ok({ dryRun: true, scanned: records.length, archived: 0, removed: 0, batches: 0, retentionDays, message: `DryRun: ${records.length} records would be archived and deleted. Set dryRun:false to execute.` });
    }

    if (records.length === 0) {
      return ok({ dryRun: false, scanned: 0, archived: 0, removed: 0, batches: 0, retentionDays, message: 'No records to purge.' });
    }

    // Archive to S3
    const s3Key = archiveS3Key();
    const lines = records.map(r => JSON.stringify(r));
    await uploadNdjsonGz(s3Key, lines, records.length);
    console.log(`[history-cleanup] archived ${records.length} records to s3://${ARCHIVE_BUCKET}/${s3Key}`);

    // Delete from Neptune
    const removed = await deleteHistoryNodes(records);
    const batches  = Math.ceil(records.length / BATCH_SIZE);

    return ok({ dryRun: false, scanned: records.length, archived: records.length, removed, batches, retentionDays, s3Key, message: `Archived ${records.length} records to S3 and deleted ${removed} from Neptune.` });
  }

  // ── restore-from-archive ─────────────────────────────────────────────────
  if (operation === 'restore-from-archive') {
    if (!ARCHIVE_BUCKET) return fail(500, 'ARCHIVE_BUCKET not configured');
    if (!body.s3Key)     return fail(400, 'Missing "s3Key" field');
    const dryRun: boolean = body.dryRun !== false && body.dryRun !== 'false';
    const entityIdFilter: string | undefined = body.entityId || undefined;

    console.log(`[history-cleanup] restore-from-archive s3Key=${body.s3Key} dryRun=${dryRun} entityId=${entityIdFilter || 'all'}`);

    let records: HistoryRecord[] = await downloadNdjsonGz(body.s3Key);

    if (entityIdFilter) {
      records = records.filter(r => r.entityId === entityIdFilter);
    }

    // Duplicate check
    const toRestore: HistoryRecord[] = [];
    let skipped = 0;
    for (const r of records) {
      const exists = await existsInNeptune(r.id);
      if (exists) { skipped++; } else { toRestore.push(r); }
    }

    if (dryRun) {
      return ok({ dryRun: true, s3Key: body.s3Key, total: records.length, restored: 0, skipped, wouldRestore: toRestore.length, message: `DryRun: ${toRestore.length} records would be restored, ${skipped} already exist. Set dryRun:false to execute.` });
    }

    if (toRestore.length > 0) {
      await insertHistoryBatch(toRestore);
    }

    return ok({ dryRun: false, s3Key: body.s3Key, total: records.length, restored: toRestore.length, skipped, message: `Restored ${toRestore.length} records from archive. ${skipped} already existed and were skipped.` });
  }

  return fail(400, `Unknown operation: ${operation}. Valid: get-history-stats, get-history-records, get-retention-config, save-retention-config, list-archives, purge-history, restore-from-archive`);
};
