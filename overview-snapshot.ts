/**
 * Overview Snapshot Lambda
 *
 * Triggered by an EventBridge rule every N minutes.
 * Collects all dashboard data in parallel, writes a JSON snapshot to S3,
 * generates a presigned URL, then fires an AppSync mutation so connected
 * browser clients receive a real-time push update.
 */
import { NeptuneSparqlClient } from '../shared/neptune-sparql-client';
import {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
  HeadObjectCommand,
  ListObjectsV2Command,
} from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import { DynamoDBClient, ScanCommand } from '@aws-sdk/client-dynamodb';
import { SignatureV4 } from '@smithy/signature-v4';
import { HttpRequest } from '@smithy/protocol-http';
import { defaultProvider } from '@aws-sdk/credential-provider-node';
import { Sha256 } from '@aws-crypto/sha256-js';

// ── Constants ─────────────────────────────────────────────────────────────────

const REGION           = process.env.AWS_REGION || 'us-east-1';
const SNAPSHOT_BUCKET  = process.env.SNAPSHOT_BUCKET || '';
const SNAPSHOT_KEY     = 'dashboard/overview-snapshot.json';
const HEALTH_TREND_KEY = 'dashboard/health-trend.json';
const APPSYNC_URL      = process.env.APPSYNC_URL || '';
const ALIAS_TABLE      = process.env.ALIAS_TABLE_NAME || '';
const RECONCILER_TABLE = process.env.RECONCILER_RUNS_TABLE || '';
const PIPELINE_RUNS_TABLE = process.env.PIPELINE_RUNS_TABLE || '';
const REPORT_BUCKET    = process.env.REPORT_BUCKET || '';

/** Hardcoded to match NeptuneSparqlClient.ontologyPrefix (private field) */
const ONTOLOGY_PREFIX = 'http://neptune.aws.com/envmgmt/ontology/';

const NOTIFY_MUTATION = `
  mutation NotifySnapshotUpdated($id: ID!, $generatedAt: AWSDateTime!, $presignedUrl: String!) {
    notifySnapshotUpdated(id: $id, generatedAt: $generatedAt, presignedUrl: $presignedUrl) {
      id
      generatedAt
      presignedUrl
    }
  }
`;

// ── Clients ───────────────────────────────────────────────────────────────────

const neptune = new NeptuneSparqlClient();
const s3      = new S3Client({ region: REGION });
const dynamo  = new DynamoDBClient({ region: REGION });

// ── Handler ───────────────────────────────────────────────────────────────────

export const handler = async (_event: any): Promise<void> => {
  console.log('Overview snapshot: collection started');
  const generatedAt = new Date().toISOString();

  // Collect all sources in parallel — allSettled so a single failure
  // does not block publishing the rest of the data.
  const [
    envsResult,
    healthResult,
    networkResult,
    integrationsResult,
    relationshipsResult,
    historyResult,
    degreesResult,
    orphansResult,
    aliasResult,
    reconcilerResult,
    reportsResult,
    pipelineRunsResult,
    scheduleResult,
  ] = await Promise.allSettled([
    neptune.getEntitiesByType('Environment'),
    collectHealthDashboard(),
    neptune.getNetworkOverview(),
    neptune.getEntitiesByType('Integration'),
    collectRelationships(),
    neptune.getAllRecentHistory({ days: 7, limit: 500 }),
    neptune.getNodeDegrees([]),
    neptune.listOrphanEnvironments(),
    collectAliases(),
    collectReconcilerRuns(),
    collectReports(),
    collectPipelineRuns(),
    collectScheduledUpdates(),
  ]);

  // Degrees Map → plain object for JSON serialisation
  const rawDegrees = settled(degreesResult);
  const degreeData = rawDegrees
    ? Object.fromEntries(rawDegrees as Map<string, number>)
    : null;

  // Persist today's avg health and retrieve the 7-day trend (sequential — needs healthResult)
  const healthEntities = (settled(healthResult) as any)?.entities || [];
  // Run health-trend persistence and the dedicated integration-history query in parallel.
  const [healthTrendData, integrationHistoryData] = await Promise.all([
    collectHealthTrend(healthEntities),
    neptune.getIntegrationHistory(7).catch(() => []),
  ]);

  // Compute all derived/display data server-side so the frontend can render
  // directly without any business logic or data manipulation.
  // The raw collector results are kept in a local variable only — they are NOT
  // written to S3 (removes frontend overload from large raw arrays).
  const rawSnapshot = {
    generatedAt,
    envData:                settled(envsResult),
    healthData:             settled(healthResult),
    networkData:            settled(networkResult),
    integrationsData:       settled(integrationsResult),
    relationshipsData:      settled(relationshipsResult),
    historyData:            settled(historyResult),
    degreeData,
    orphanData:             settled(orphansResult),
    aliasData:              settled(aliasResult),
    reconcilerData:         settled(reconcilerResult),
    reportData:             settled(reportsResult),
    pipelineData:           settled(pipelineRunsResult),
    scheduleData:           settled(scheduleResult),
    healthTrendData,
    integrationHistoryData,
  };

  const snapshot = {
    generatedAt,
    computed: computeDerivedData(rawSnapshot),
  };

  // ── Write to S3 ──
  await s3.send(new PutObjectCommand({
    Bucket: SNAPSHOT_BUCKET,
    Key:    SNAPSHOT_KEY,
    Body:   JSON.stringify(snapshot),
    ContentType: 'application/json',
  }));
  console.log(`Snapshot written → s3://${SNAPSHOT_BUCKET}/${SNAPSHOT_KEY}`);

  // ── Generate 7-day presigned URL ──
  const presignedUrl = await getSignedUrl(
    s3,
    new GetObjectCommand({ Bucket: SNAPSHOT_BUCKET, Key: SNAPSHOT_KEY }),
    { expiresIn: 7 * 24 * 60 * 60 },
  );

  // ── Notify AppSync (IAM-signed) ──
  if (APPSYNC_URL) {
    try {
      await callAppSyncMutation(generatedAt, presignedUrl);
      console.log('AppSync notification sent');
    } catch (err) {
      // Non-fatal — snapshot is already in S3; frontend can still poll
      console.error('AppSync notification failed (snapshot still published):', err);
    }
  }

  console.log('Overview snapshot: published successfully');
};

// ── Data collectors ───────────────────────────────────────────────────────────

async function collectHealthDashboard(): Promise<{
  entities: any[];
  totalWithStats: number;
  totalEntities: number;
}> {
  const healthNetwork = await neptune.getNetworkData();
  const entitiesWithStats = healthNetwork.nodes
    .filter((node: any) => node.stats && Object.keys(node.stats).length > 0)
    .map((node: any) => ({
      id:    node.id,
      name:  node.label,
      type:  node.type,
      stats: node.stats,
    }));
  return {
    entities:       entitiesWithStats,
    totalWithStats: entitiesWithStats.length,
    totalEntities:  healthNetwork.nodes.length,
  };
}

/**
 * Read the persisted health-trend JSON from S3, append today's avg health score
 * computed from live entity stats, prune entries older than 7 days, write back,
 * and return the updated map { "YYYY-MM-DD": avgHealthScore }.
 */
async function collectHealthTrend(entities: any[]): Promise<Record<string, number>> {
  const today = new Date().toISOString().slice(0, 10);

  // Compute today's average health from live entity stats
  const scores = entities
    .map((e: any) => parseFloat((e.stats || {}).healthScore || ''))
    .filter((v: number) => !isNaN(v) && v > 0);
  const avgToday = scores.length
    ? Math.round((scores.reduce((a: number, b: number) => a + b, 0) / scores.length) * 10) / 10
    : null;

  // Read existing trend from S3
  let hist: Record<string, number> = {};
  try {
    const res = await s3.send(new GetObjectCommand({
      Bucket: SNAPSHOT_BUCKET,
      Key:    HEALTH_TREND_KEY,
    }));
    const body = await (res.Body as any).transformToString();
    hist = JSON.parse(body);
  } catch (_) { /* first run or key missing — start fresh */ }

  // Update today's entry
  if (avgToday !== null) {
    hist[today] = avgToday;
  }

  // Prune anything older than 7 days
  const cutoff = new Date(Date.now() - 7 * 86400000).toISOString().slice(0, 10);
  for (const key of Object.keys(hist)) {
    if (key < cutoff) delete hist[key];
  }

  // Persist updated trend
  try {
    await s3.send(new PutObjectCommand({
      Bucket:      SNAPSHOT_BUCKET,
      Key:         HEALTH_TREND_KEY,
      Body:        JSON.stringify(hist),
      ContentType: 'application/json',
    }));
  } catch (err) {
    console.error('Failed to persist health trend:', err);
  }

  return hist;
}

async function collectRelationships(): Promise<any[]> {
  const relQuery = `
    PREFIX env: <${ONTOLOGY_PREFIX}>
    SELECT ?id ?relationshipType ?sourceEntity ?targetEntity ?sourceEntityId ?targetEntityId ?createdAt WHERE {
      ?rel env:type "Relationship" ;
           env:id ?id ;
           env:relationshipType ?relationshipType ;
           env:sourceEntity ?sourceEntity ;
           env:targetEntity ?targetEntity .
      OPTIONAL { ?rel env:sourceEntityId ?sourceEntityId }
      OPTIONAL { ?rel env:targetEntityId ?targetEntityId }
      OPTIONAL { ?rel env:createdAt ?createdAt }
    }
    ORDER BY ?sourceEntity ?relationshipType ?targetEntity
  `;
  const relResult = await neptune.executeSparqlQuery(relQuery);
  return (relResult.results?.bindings || []).map((b: any) => ({
    id:               b.id?.value || '',
    relationshipType: b.relationshipType?.value || '',
    sourceEntity:     b.sourceEntity?.value || '',
    targetEntity:     b.targetEntity?.value || '',
    sourceEntityId:   b.sourceEntityId?.value || '',
    targetEntityId:   b.targetEntityId?.value || '',
    createdAt:        b.createdAt?.value || '',
  }));
}

async function collectAliases(): Promise<any[]> {
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

async function collectReconcilerRuns(): Promise<any[]> {
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
      let stats: any = null;
      if (item.statsJson?.S) {
        try { stats = JSON.parse(item.statsJson.S); } catch { /* ignore */ }
      }
      let plan: any = null;
      if (item.planJson?.S) {
        try { plan = JSON.parse(item.planJson.S); } catch { /* ignore */ }
      }
      items.push({
        runId:       item.runId?.S || '',
        status:      item.status?.S || 'unknown',
        isPreview:   item.isPreview?.BOOL ?? false,
        triggeredBy: item.triggeredBy?.S || null,
        triggeredAt: item.triggeredAt?.S || null,
        updatedAt:   item.updatedAt?.S   || null,
        stats,
        plan,
      });
    }
    lastKey = res.LastEvaluatedKey as Record<string, any> | undefined;
  } while (lastKey);
  items.sort((a, b) => (b.triggeredAt || '').localeCompare(a.triggeredAt || ''));
  return items.slice(0, 25);
}

async function collectPipelineRuns(): Promise<any[]> {
  if (!PIPELINE_RUNS_TABLE) return [];
  const items: any[] = [];
  let lastKey: Record<string, any> | undefined;
  do {
    const res = await dynamo.send(new ScanCommand({
      TableName: PIPELINE_RUNS_TABLE,
      ExclusiveStartKey: lastKey,
      ProjectionExpression: 'pipelineId, runId, #s, triggeredBy, envsCreated, integrationsCreated, integrationsStale, summaryErrors, durationMs, #e',
      ExpressionAttributeNames: { '#s': 'status', '#e': 'error' },
      Limit: 1000,
    }));
    for (const item of res.Items || []) {
      const pipelineId  = item.pipelineId?.S || '';
      const hashIndex   = pipelineId.indexOf('#');
      const sourceType  = hashIndex > -1 ? pipelineId.substring(0, hashIndex) : pipelineId;
      items.push({
        pipelineId,
        sourceType,
        runId:               item.runId?.S               || '',
        status:              item.status?.S              || 'unknown',
        triggeredBy:         item.triggeredBy?.S         || null,
        envsCreated:         item.envsCreated?.N         ? Number(item.envsCreated.N) : 0,
        integrationsCreated: item.integrationsCreated?.N ? Number(item.integrationsCreated.N) : 0,
        integrationsStale:   item.integrationsStale?.N   ? Number(item.integrationsStale.N) : 0,
        summaryErrors:       item.summaryErrors?.N       ? Number(item.summaryErrors.N) : 0,
        durationMs:          item.durationMs?.N          ? Number(item.durationMs.N) : null,
        error:               item.error?.S               || null,
      });
    }
    lastKey = res.LastEvaluatedKey as Record<string, any> | undefined;
  } while (lastKey);

  // Group by sourceType, keep the latest run per source (runId is ISO timestamp)
  const bySource: Record<string, any[]> = {};
  for (const item of items) {
    if (!bySource[item.sourceType]) bySource[item.sourceType] = [];
    bySource[item.sourceType].push(item);
  }
  return Object.entries(bySource).map(([sourceType, runs]) => {
    runs.sort((a, b) => b.runId.localeCompare(a.runId));
    return {
      sourceType,
      totalRuns:  runs.length,
      latestRun:  runs[0],
      recentRuns: runs.slice(0, 5),
    };
  }).sort((a, b) => a.sourceType.localeCompare(b.sourceType));
}

async function collectReports(): Promise<any[]> {
  const response = await s3.send(new ListObjectsV2Command({
    Bucket: REPORT_BUCKET,
    Prefix: 'reports/',
    MaxKeys: 100,
  }));
  // Sort newest first, take top 10
  const items = (response.Contents || [])
    .filter(obj => obj.Key?.endsWith('.html'))
    .sort((a, b) => (b.LastModified?.getTime() ?? 0) - (a.LastModified?.getTime() ?? 0))
    .slice(0, 10);

  // Fetch presigned URLs + S3 metadata (envCount, avgHealthScore) in parallel
  const withUrls = await Promise.all(
    items.map(async (obj) => {
      let url: string | undefined;
      let metadata: Record<string, string> = {};
      try {
        url = await getSignedUrl(
          s3,
          new GetObjectCommand({ Bucket: REPORT_BUCKET, Key: obj.Key }),
          { expiresIn: 7 * 24 * 60 * 60 },
        );
      } catch (_) { /* non-fatal */ }
      try {
        const head = await s3.send(new HeadObjectCommand({ Bucket: REPORT_BUCKET, Key: obj.Key }));
        metadata = head.Metadata || {};
      } catch (_) { /* non-fatal */ }
      return {
        key:          obj.Key,
        lastModified: obj.LastModified?.toISOString(),
        size:         obj.Size,
        url,
        envCount:     metadata.environmentcount ? Number(metadata.environmentcount) : undefined,
        avgHealth:    metadata.avghealth        ? Number(metadata.avghealth)        : undefined,
      };
    }),
  );
  return withUrls;
}

// ── Parse schedule window string into ISO dates ───────────────────────────────
function parseScheduleWindow(schedule: string): { windowStart: string; windowEnd: string; description: string } {
  // Format: "Apr 12 02:00-04:00 UTC — description"
  const match = schedule.match(/^([A-Z][a-z]{2})\s+(\d{1,2})\s+(\d{2}:\d{2})-(\d{2}:\d{2})\s+UTC\s*[\u2014\u2013\-]+\s*(.*)/);
  if (!match) return { windowStart: '', windowEnd: '', description: schedule };

  const [, month, dayStr, startTime, endTime, description] = match;
  const monthMap: Record<string, number> = {
    Jan:1, Feb:2, Mar:3, Apr:4, May:5, Jun:6,
    Jul:7, Aug:8, Sep:9, Oct:10, Nov:11, Dec:12,
  };
  const monthNum = monthMap[month];
  if (!monthNum) return { windowStart: '', windowEnd: '', description: schedule };

  const day = parseInt(dayStr, 10);
  const now = new Date();
  let useYear = now.getUTCFullYear();

  // If the candidate date already passed (>1h ago), project to next year
  const candidate = new Date(Date.UTC(useYear, monthNum - 1, day,
    parseInt(startTime.split(':')[0], 10), parseInt(startTime.split(':')[1], 10)));
  // Keep original year — don't project to next year; let frontend show past vs future
  const startH = parseInt(startTime.split(':')[0], 10);
  const endH   = parseInt(endTime.split(':')[0], 10);
  const endDay = endH < startH ? day + 1 : day;

  return {
    windowStart: `${useYear}-${String(monthNum).padStart(2,'0')}-${String(day).padStart(2,'0')}T${startTime}:00Z`,
    windowEnd:   `${useYear}-${String(monthNum).padStart(2,'0')}-${String(endDay).padStart(2,'0')}T${endTime}:00Z`,
    description: description.trim(),
  };
}

// Parse JSON-array schedule format: [{"date":"2026-05-20","startTime":"20:47","endTime":"22:47","description":"..."},...]
function parseJsonScheduleWindows(schedule: string): Array<{ windowStart: string; windowEnd: string; description: string }> {
  try {
    const arr = JSON.parse(schedule);
    if (!Array.isArray(arr)) return [];
    const results: Array<{ windowStart: string; windowEnd: string; description: string }> = [];
    for (const w of arr) {
      const date: string        = w.date        || '';
      const startTime: string   = w.startTime   || '';
      const endTime: string     = w.endTime     || startTime;
      const description: string = w.description || '';
      if (!date || !startTime) continue;
      // Build end date — handle cross-midnight windows
      const [year, month, day] = date.split('-').map(Number);
      const startH = parseInt(startTime.split(':')[0], 10);
      const endH   = parseInt(endTime.split(':')[0],   10);
      const endDay = endH < startH ? day + 1 : day;
      results.push({
        windowStart: `${date}T${startTime}:00Z`,
        windowEnd:   `${year}-${String(month).padStart(2,'0')}-${String(endDay).padStart(2,'0')}T${endTime}:00Z`,
        description,
      });
    }
    return results;
  } catch {
    return [];
  }
}

// ── Collect scheduled maintenance windows from Neptune ────────────────────────
async function collectScheduledUpdates(): Promise<any[]> {
  // Single query: fetch every entity that has env:scheduledUpdates set,
  // together with its optional display metadata.  No in-memory join needed,
  // so there is no risk of UUID vs name mismatch between two result sets.
  const query = `
    PREFIX env: <${ONTOLOGY_PREFIX}>
    SELECT ?id ?name ?owner ?collaborators ?scheduledUpdates WHERE {
      ?entity env:id ?id ;
              env:scheduledUpdates ?scheduledUpdates .
      FILTER(BOUND(?scheduledUpdates) && ?scheduledUpdates != "")
      OPTIONAL { ?entity env:name ?name }
      OPTIONAL { ?entity env:owner ?owner }
      OPTIONAL { ?entity env:collaborators ?collaborators }
    }
  `;

  console.log('collectScheduledUpdates: querying Neptune for scheduled updates');
  const result = await neptune.executeSparqlQuery(query);
  const bindings = result.results?.bindings || [];
  console.log(`collectScheduledUpdates: ${bindings.length} schedule bindings returned`);

  type ScheduleItem = {
    envId: string; envName: string; owner: string; collaborators: string;
    schedule: string; windowStart: string; windowEnd: string;
    description: string; conflict: boolean; conflictWith: string;
  };

  const items = (bindings as any[])
    .map(b => {
      const envId    = b.id?.value              || '';
      const schedule = b.scheduledUpdates?.value || '';
      if (!envId || !schedule) return null;
      return {
        envId,
        schedule,
        envName:       b.name?.value          || envId,
        owner:         b.owner?.value         || '',
        collaborators: b.collaborators?.value || '',
      };
    })
    .filter(Boolean) as { envId: string; envName: string; owner: string; collaborators: string; schedule: string }[];

  console.log(`collectScheduledUpdates: ${items.length} items with valid id+schedule`);

  // Parse windows — one environment can have multiple windows in JSON array format
  const parsed: ScheduleItem[] = items.flatMap(item => {
    const trimmed = item.schedule.trim();
    // JSON array format: [{"date":"2026-05-20","startTime":"20:47","endTime":"22:47"},...]
    if (trimmed.startsWith('[')) {
      const windows = parseJsonScheduleWindows(trimmed);
      if (windows.length) {
        return windows.map(w => ({ ...item, ...w, conflict: false, conflictWith: '' }));
      }
      console.warn(`collectScheduledUpdates: failed to parse JSON schedule for ${item.envId}: "${item.schedule}"`);
      return [];
    }
    // Text format: "Apr 12 02:00-04:00 UTC — description"
    const { windowStart, windowEnd, description } = parseScheduleWindow(item.schedule);
    if (!windowStart) {
      console.warn(`collectScheduledUpdates: failed to parse schedule for ${item.envId}: "${item.schedule}"`);
      return [];
    }
    return [{ ...item, windowStart, windowEnd, description, conflict: false, conflictWith: '' }];
  });

  console.log(`collectScheduledUpdates: ${parsed.length} items after parse`);

  // Detect overlapping windows
  for (let i = 0; i < parsed.length; i++) {
    for (let j = i + 1; j < parsed.length; j++) {
      const a = parsed[i], b = parsed[j];
      const aS = new Date(a.windowStart).getTime(), aE = new Date(a.windowEnd).getTime();
      const bS = new Date(b.windowStart).getTime(), bE = new Date(b.windowEnd).getTime();
      if (aS < bE && bS < aE) {
        parsed[i].conflict = true;
        parsed[i].conflictWith = b.envName;
        parsed[j].conflict = true;
        parsed[j].conflictWith = a.envName;
      }
    }
  }

  return parsed.sort((a, b) =>
    new Date(a.windowStart).getTime() - new Date(b.windowStart).getTime()
  );
}

// ── AppSync IAM-signed mutation ───────────────────────────────────────────────

async function callAppSyncMutation(generatedAt: string, presignedUrl: string): Promise<void> {
  const url  = new URL(APPSYNC_URL);
  const body = JSON.stringify({
    query:     NOTIFY_MUTATION,
    variables: { id: 'OVERVIEW', generatedAt, presignedUrl },
  });

  const request = new HttpRequest({
    method:   'POST',
    hostname: url.hostname,
    path:     url.pathname,
    headers: {
      'Content-Type': 'application/json',
      host:           url.hostname,
    },
    body,
  });

  const signer = new SignatureV4({
    credentials: defaultProvider(),
    region:      REGION,
    service:     'appsync',
    sha256:      Sha256,
  });

  const signed   = await signer.sign(request);
  const response = await fetch(`https://${signed.hostname}${signed.path}`, {
    method:  signed.method,
    headers: signed.headers as Record<string, string>,
    body:    signed.body as string,
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`AppSync HTTP ${response.status}: ${text}`);
  }
  const json = await response.json() as any;
  if (json.errors?.length) {
    throw new Error(`AppSync errors: ${JSON.stringify(json.errors)}`);
  }
}

// ── Server-side derived data computation ─────────────────────────────────────
//
// All business logic that used to live in the frontend (alert derivation,
// chart buckets, topology filtering, governance score, activity feed assembly,
// etc.) is computed here so the React component can render directly from
// snapshot.computed.* without any manipulation.
// ─────────────────────────────────────────────────────────────────────────────

function computeDerivedData(snap: any): any {

  // ── Safe aliases ────────────────────────────────────────────────────────────
  const envs:         any[] = snap.envData                   ?? [];
  const healthEnts:   any[] = snap.healthData?.entities      ?? [];
  const netNodes:     any[] = snap.networkData?.nodes        ?? [];
  const netEdges:     any[] = snap.networkData?.edges        ?? [];
  const integrations: any[] = snap.integrationsData          ?? [];
  const history:      any[] = snap.historyData               ?? [];
  // prefer dedicated integration-history feed; fall back to full history
  const intHistory:   any[] = snap.integrationHistoryData ?? history;
  const orphans:      any[] = snap.orphanData                ?? [];
  const aliases:      any[] = snap.aliasData                 ?? [];
  const degrees:      Record<string, number> = snap.degreeData ?? {};
  const reconcilerRuns: any[] = snap.reconcilerData          ?? [];
  const pipelineRuns:   any[] = snap.pipelineData            ?? [];
  const schedules:      any[] = snap.scheduleData            ?? [];
  const reports:        any[] = snap.reportData              ?? [];
  const trendData:    Record<string, number> = snap.healthTrendData ?? {};
  const generatedAt:  string  = snap.generatedAt ?? new Date().toISOString();
  const snapshotDate  = new Date(generatedAt);

  // ── Shared name-lookup maps ─────────────────────────────────────────────────
  const envNameById = new Map<string, string>();
  for (const e of envs) if (e.id) envNameById.set(e.id, e.name || e.id);
  const entityNameById = new Map<string, string>([...envNameById]);
  for (const i of integrations) if (i.id) entityNameById.set(i.id, i.name || i.id);

  // ── 1 · KPI Strip ───────────────────────────────────────────────────────────
  const monitoredEnvs = healthEnts.filter((e: any) => e.type === 'Environment').length;
  const notTracked    = envs.length - monitoredEnvs;
  const kpi = {
    totalEnvironments:    envs.length,
    environmentsInGraph:  netNodes.filter((n: any) => n.type === 'Environment').length,
    monitoredEnvironments: monitoredEnvs,
    monitoredSubText:     notTracked > 0 ? `${notTracked} not tracked` : 'All envs tracked',
    totalIntegrations:    integrations.length,
    unresolvedOrphans:    orphans.length,
    configChanges7d:      history.length,
    totalAliases:         aliases.length,
  };

  // ── 2 · Active Alerts ───────────────────────────────────────────────────────
  // Thresholds: cpu >80%, memory >90%, latency >200 ms,
  //             availability <99.5%, healthScore <50
  const activeAlerts: any[] = [];
  for (const entity of healthEnts) {
    const s = entity.stats || {};
    const since = s.lastSyncedAt || s.collectionTime || null;

    const cpu = parseFloat(s.cpu);
    if (!isNaN(cpu) && cpu > 80) {
      activeAlerts.push({ severity: 'critical', entityId: entity.id, entityName: entity.name,
        stat: 'cpu', value: cpu, threshold: 80,
        issue: `CPU ${cpu}% (threshold 80%)`, sinceISO: since });
    }
    const memory = parseFloat(s.memory);
    if (!isNaN(memory) && memory > 90) {
      activeAlerts.push({ severity: 'critical', entityId: entity.id, entityName: entity.name,
        stat: 'memory', value: memory, threshold: 90,
        issue: `Memory ${memory}% (threshold 90%)`, sinceISO: since });
    }
    const latency = parseFloat(s.latency);
    if (!isNaN(latency) && latency > 200) {
      activeAlerts.push({ severity: 'warning', entityId: entity.id, entityName: entity.name,
        stat: 'latency', value: latency, threshold: 200,
        issue: `Latency ${latency} ms (threshold 200 ms)`, sinceISO: since });
    }
    const availability = parseFloat(s.availability);
    if (!isNaN(availability) && availability < 99.5) {
      activeAlerts.push({ severity: 'warning', entityId: entity.id, entityName: entity.name,
        stat: 'availability', value: availability, threshold: 99.5,
        issue: `Availability ${availability}% (threshold 99.5%)`, sinceISO: since });
    }
    const healthScore = parseFloat(s.healthScore);
    if (!isNaN(healthScore) && healthScore < 50) {
      activeAlerts.push({ severity: 'critical', entityId: entity.id, entityName: entity.name,
        stat: 'healthScore', value: healthScore, threshold: 50,
        issue: `Health score ${healthScore}/100 (threshold 50)`, sinceISO: since });
    }
  }

  // ── 3 · Status Buckets (doughnut chart) ─────────────────────────────────────
  const VALID_STATUS_BUCKETS = new Set(['Active', 'Degraded', 'Critical', 'Inactive']);
  const buckets: Record<string, number> = { Active: 0, Degraded: 0, Critical: 0, Inactive: 0, Other: 0 };
  for (const node of netNodes) {
    const raw = (node.properties?.status || node.status || '') as string;
    const key = raw.charAt(0).toUpperCase() + raw.slice(1).toLowerCase();
    buckets[VALID_STATUS_BUCKETS.has(key) ? key : 'Other']++;
  }
  const statusBuckets = { ...buckets, total: netNodes.length };

  // ── 4 · Topology Focus (D3 subgraph: alert nodes + one-hop neighbours) ──────
  // Classification sources (in priority order):
  //   A) activeAlerts — stats-based thresholds (cpu, memory, latency, healthScore)
  //   B) Neptune node state fields — currentState / status (top-level on EnvNode)
  const topoCriticalIds = new Set<string>();
  const topoWarningIds  = new Set<string>();

  // Source A: stats-based alerts already computed in section 2
  for (const alert of activeAlerts) {
    if (alert.severity === 'critical') {
      topoCriticalIds.add(alert.entityId);
      topoWarningIds.delete(alert.entityId);
    } else if (!topoCriticalIds.has(alert.entityId)) {
      topoWarningIds.add(alert.entityId);
    }
  }

  // Source B: Neptune `currentState` / `status` fields on topology nodes
  // (EnvNode has both as top-level fields — no .properties wrapper)
  for (const node of netNodes) {
    const id = String(node.id || '');
    if (!id) continue;
    const state = (
      (node.currentState || node.status || '') as string
    ).trim().toLowerCase();
    if (state === 'critical') {
      topoCriticalIds.add(id);
      topoWarningIds.delete(id);                  // escalate
    } else if ((state === 'warning' || state === 'degraded') && !topoCriticalIds.has(id)) {
      topoWarningIds.add(id);
    }
  }

  const hasNodes        = netNodes.length > 0;
  const allHealthy      = hasNodes && topoCriticalIds.size === 0 && topoWarningIds.size === 0;
  const noData          = !hasNodes;
  const alertNodeCount  = topoCriticalIds.size + topoWarningIds.size;

  // Count one-hop neighbours (for badge text only — we always send all nodes)
  let neighbourCount = 0;
  if (!allHealthy && !noData) {
    const alertIds = new Set<string>([...topoCriticalIds, ...topoWarningIds]);
    const neighbourIds = new Set<string>();
    for (const edge of netEdges) {
      const src = String(edge.source || '');
      const tgt = String(edge.target || '');
      if (alertIds.has(src) && !alertIds.has(tgt)) neighbourIds.add(tgt);
      if (alertIds.has(tgt) && !alertIds.has(src)) neighbourIds.add(src);
    }
    neighbourCount = neighbourIds.size;
  }

  // Build the subgraph to send: alert nodes + their immediate neighbours only.
  // Frontend renders whatever arrives — all filtering is done here.
  const alertNodeIds = new Set<string>([...topoCriticalIds, ...topoWarningIds]);
  let focusNodes: any[];
  let focusEdges: any[];
  if (allHealthy || noData) {
    // Nothing to highlight — send empty so frontend can show healthy/no-data message
    focusNodes = [];
    focusEdges = [];
  } else {
    // Expand visible set to include one-hop neighbours of alert nodes
    const visibleIds = new Set<string>([...alertNodeIds]);
    for (const edge of netEdges) {
      const src = String(edge.source || '');
      const tgt = String(edge.target || '');
      if (alertNodeIds.has(src)) visibleIds.add(tgt);
      if (alertNodeIds.has(tgt)) visibleIds.add(src);
    }
    focusNodes = netNodes
      .filter((n: any) => visibleIds.has(String(n.id || '')))
      .map((n: any) => ({
        ...n,
        _isCritical: topoCriticalIds.has(String(n.id || '')),
        _isWarning:  topoWarningIds.has(String(n.id || '')),
      }));
    focusEdges = netEdges.filter((e: any) =>
      visibleIds.has(String(e.source || '')) && visibleIds.has(String(e.target || '')));
  }

  const topologyFocus = {
    allHealthy,
    noData,
    criticalCount:  topoCriticalIds.size,
    warnCount:      topoWarningIds.size,
    neighbourCount,
    badgeText: noData
      ? '⚠ No topology data available'
      : allHealthy
        ? `✓ All ${netNodes.length} environments are healthy`
        : `${topoCriticalIds.size} critical · ${topoWarningIds.size} warning · ${neighbourCount} neighbours`,
    nodes: focusNodes,
    edges: focusEdges,
  };

  // ── 5 · Blast Radius (top 8 by degree) ──────────────────────────────────────
  const degreeEntries = Object.entries(degrees)
    .sort((a, b) => (b[1] as number) - (a[1] as number))
    .slice(0, 8);
  const maxDegree = (degreeEntries[0]?.[1] as number) ?? 1;
  const blastRadius = degreeEntries.map(([id, degree]) => ({
    entityId:    id,
    entityName:  (entityNameById.get(id) || id).slice(0, 20),
    degree,
    isSPOF:      degree >= 10,
    barWidthPct: Math.round((degree / maxDegree) * 100),
  }));

  // ── 6 · Governance & Hygiene Score ──────────────────────────────────────────
  const total         = envs.length;
  const govScore      = total === 0
    ? 100
    : Math.max(0, Math.round((total - orphans.length) / total * 100));
  const svgDashOffset = Math.round(201.06 * (1 - govScore / 100) * 10) / 10;
  const withOwner     = envs.filter((e: any) =>
    e.owner && String(e.owner).trim() !== '').length;
  const ownerStatus   = withOwner === total ? 'pass' : withOwner > total / 2 ? 'partial' : 'fail';
  const governanceScore = {
    score: govScore,
    svgDashOffset,
    ownerCheck:  { withOwner, total, status: ownerStatus, label: `${withOwner} / ${total}` },
    orphanCheck: { count: orphans.length, pass: orphans.length === 0,
                   label: orphans.length === 0 ? 'None' : `${orphans.length} envs` },
    aliasCheck:  { count: aliases.length, pass: aliases.length > 0,
                   label: `${aliases.length} aliases` },
    orphanItems: orphans.slice(0, 8).map((o: any) => ({
      name:   o.hostname || o.name || o.id || '–',
      source: o.source || '',
    })),
  };

  // ── 7 · Pipeline Rows (display-ready, up to 5) ──────────────────────────────
  const PIPELINE_STATUS_COLOR: Record<string, string> = {
    completed: '#059669', failed: '#dc2626', running: '#d97706',
  };
  const pipelineRows = pipelineRuns.slice(0, 5).map((p: any) => {
    const run     = p.latestRun || {};
    const hashIdx = (run.pipelineId || '').indexOf('#');
    const envId   = hashIdx > -1 ? run.pipelineId.slice(hashIdx + 1) : '';
    const ms: number | null = run.durationMs ?? null;
    let durationDisplay = '';
    if (ms !== null && ms >= 0) {
      const s = Math.floor(ms / 1000);
      durationDisplay = s < 60 ? (s > 0 ? `${s}s` : `${ms}ms`) : `${Math.floor(s / 60)}m ${s % 60}s`;
    }
    return {
      sourceType:          p.sourceType,
      totalRuns:           p.totalRuns           ?? 0,
      envName:             envNameById.get(envId) || envId || '—',
      status:              run.status             || 'unknown',
      statusColor:         PIPELINE_STATUS_COLOR[run.status] ?? '#64748b',
      triggeredBy:         run.triggeredBy        || null,
      runAt:               run.runId              || null,
      envsCreated:         run.envsCreated         ?? 0,
      integrationsCreated: run.integrationsCreated ?? 0,
      integrationsStale:   run.integrationsStale   ?? 0,
      summaryErrors:       run.summaryErrors       ?? 0,
      durationDisplay,
      error:               run.error || null,
    };
  });

  // ── 8 · Reconciler Rows (display-ready, up to 5) ────────────────────────────
  const RECONCILER_STATUS_COLOR: Record<string, string> = {
    completed: '#059669', 'completed-with-errors': '#d97706',
    failed: '#dc2626', running: '#d97706',
  };
  const reconcilerRows = reconcilerRuns.slice(0, 5).map((r: any) => {
    const st = r.stats;
    const parts: string[] = [];
    if (st && typeof st === 'object') {
      if (st.added        != null) parts.push(`${st.added} added`);
      if (st.updated      != null) parts.push(`${st.updated} updated`);
      if (st.deleted      != null) parts.push(`${st.deleted} deleted`);
      const rc = st.renamedCount || st.previewRenameCount || 0;
      const mc = st.mergedCount  || st.previewMergeCount  || 0;
      if (rc > 0) parts.push(`${rc} rename${rc !== 1 ? 's' : ''}`);
      if (mc > 0) parts.push(`${mc} merge${mc !== 1 ? 's' : ''}`);
      if (st.skippedCount > 0) parts.push(`${st.skippedCount} skipped`);
    }
    const planActions = (r.plan && Array.isArray(r.plan.plan)) ? r.plan.plan : [];
    const planLines: Array<{ action: string; from: string; to: string }> =
      planActions.slice(0, 4).map((p: any) => ({
        action: String(p.action || ''),
        from:   String(p.currentName || ''),
        to:     String(p.targetName  || ''),
      }));
    return {
      runId:       r.runId,
      triggeredAt: r.triggeredAt || r.runId || null,
      status:      r.status      || 'unknown',
      statusColor: RECONCILER_STATUS_COLOR[r.status] ?? '#64748b',
      isPreview:   r.isPreview   ?? false,
      triggeredBy: r.triggeredBy || null,
      statsLabel:  parts.length ? parts.join(' · ') : null,
      planLines,
    };
  });

  // ── 9 · Integration Changes (7-day chart + sorted log) ──────────────────────
  const ADD_ACTIONS    = new Set(['create-integration', 'add-integration']);
  const REMOVE_ACTIONS = new Set(['delete-integration', 'remove-integration']);
  const chartDates: string[] = [];
  for (let i = 6; i >= 0; i--) {
    const d = new Date(snapshotDate);
    d.setUTCDate(d.getUTCDate() - i);
    chartDates.push(d.toISOString().slice(0, 10));
  }
  const addedCounts   = new Array(7).fill(0) as number[];
  const removedCounts = new Array(7).fill(0) as number[];
  const intLog: any[] = [];
  for (const evt of intHistory) {
    const action   = evt.action || '';
    const isAdd    = ADD_ACTIONS.has(action);
    const isRemove = REMOVE_ACTIONS.has(action);
    if (!isAdd && !isRemove) continue;
    const dayStr = (evt.timestamp || '').slice(0, 10);
    const idx    = chartDates.indexOf(dayStr);
    if (idx >= 0) {
      if (isAdd) addedCounts[idx]++;
      else removedCounts[idx]++;
    }
    intLog.push({
      type:            isAdd ? 'added' : 'removed',
      action,
      sourceEnv:       evt.entityName                  || '',
      targetEnv:       evt.changes?.targetEnvName      || '',
      integrationName: evt.changes?.integrationName    || '',
      timestamp:       evt.timestamp                   || '',
      user:            evt.user                        || null,
    });
  }
  intLog.sort((a, b) => b.timestamp.localeCompare(a.timestamp));
  const totalAdded   = addedCounts.reduce((s, v) => s + v, 0);
  const totalRemoved = removedCounts.reduce((s, v) => s + v, 0);
  const integrationChanges = {
    chartDates, added: addedCounts, removed: removedCounts,
    totalAdded, totalRemoved, netChange: totalAdded - totalRemoved,
    log: intLog,
  };

  // ── 10 · Schedule Display (sorted + day-labels + badge classes) ─────────────
  // dayLabel / badgeClass are computed relative to snapshotDate (not browser
  // Date.now()).  Frontend SHOULD recompute dayLabel from windowStart when the
  // cached snapshot age exceeds ~1 hour.
  const snapMs    = snapshotDate.getTime();
  const future    = schedules.filter((s: any) => new Date(s.windowStart).getTime() >= snapMs);
  const past      = schedules.filter((s: any) => new Date(s.windowStart).getTime() <  snapMs);
  future.sort((a: any, b: any) => a.windowStart.localeCompare(b.windowStart));
  past.sort((a: any, b: any)   => b.windowStart.localeCompare(a.windowStart));
  const scheduleDisplay = {
    conflictCount:       future.filter((s: any) => s.conflict).length,
    snapshotGeneratedAt: generatedAt,
    items: [...future, ...past].map((s: any) => {
      const diffMs   = new Date(s.windowStart).getTime() - snapMs;
      const diffDays = diffMs / 86400000;
      const dayLabel = Math.abs(diffDays) < 1 ? 'Today'
        : diffDays > 0 ? `In ${Math.ceil(diffDays)}d`
        : `${Math.floor(-diffDays)}d ago`;
      const badgeClass = diffMs < 0 ? 'past'
        : diffDays <= 2 ? 'soon'
        : diffDays <= 7 ? 'medium'
        : 'far';
      return { ...s, dayLabel, badgeClass };
    }),
  };

  // ── 11 · Activity Feed (merged from 5 sources, newest-first, capped 100) ────
  const INT_ACTIONS = new Set([...ADD_ACTIONS, ...REMOVE_ACTIONS, 'update-integration']);
  const feedItems: any[] = [];

  // Source 1: health alerts — use the already-computed activeAlerts array
  for (const alert of activeAlerts) {
    feedItems.push({
      type:      'health-alert',
      color:     '#dc2626',
      message:   `${alert.issue} on ${alert.entityName}`,
      timestamp: alert.sinceISO || generatedAt,
    });
  }

  // Source 2: integration history events
  for (const evt of intHistory) {
    const action  = evt.action || '';
    const intName = evt.changes?.integrationName || '';
    const tgtEnv  = evt.changes?.targetEnvName   || '';
    if (ADD_ACTIONS.has(action)) {
      feedItems.push({ type: 'int-added', color: '#059669',
        message: `Integration added on ${evt.entityName}${intName ? ` — ${intName}` : ''}${tgtEnv ? ` → ${tgtEnv}` : ''}`,
        timestamp: evt.timestamp });
    } else if (REMOVE_ACTIONS.has(action)) {
      feedItems.push({ type: 'int-removed', color: '#dc2626',
        message: `Integration removed from ${evt.entityName}${intName ? ` — ${intName}` : ''}`,
        timestamp: evt.timestamp });
    } else if (action === 'update-integration') {
      feedItems.push({ type: 'int-updated', color: '#d97706',
        message: `Integration updated on ${evt.entityName}`,
        timestamp: evt.timestamp });
    }
  }

  // Source 3: general environment history (skip integration actions — handled above)
  for (const evt of history) {
    const action = evt.action || '';
    const user   = evt.user ? ` by ${evt.user}` : '';
    if (INT_ACTIONS.has(action)) continue;
    if (action === 'create') {
      feedItems.push({ type: 'env-create', color: '#059669',
        message: `Environment ${evt.entityName} created${user}`, timestamp: evt.timestamp });
    } else if (action === 'delete') {
      feedItems.push({ type: 'env-delete', color: '#dc2626',
        message: `Environment ${evt.entityName} deleted${user}`, timestamp: evt.timestamp });
    } else if (action === 'rename') {
      const from = evt.changes?.from || '';
      const to   = evt.changes?.to   || '';
      feedItems.push({ type: 'env-rename', color: '#7c3aed',
        message: `Environment ${evt.entityName} renamed${from && to ? ` ${from} → ${to}` : ''}${user}`,
        timestamp: evt.timestamp });
    } else if (action === 'update' || action === 'update-config') {
      feedItems.push({ type: 'env-update', color: '#d97706',
        message: `Config updated on ${evt.entityName}${user}`, timestamp: evt.timestamp });
    }
  }

  // Source 4: pipeline run events (one item per pipeline source, latest run)
  for (const p of pipelineRuns) {
    const run = p.latestRun || {};
    if (!run.runId) continue;
    const isSuccess = run.status === 'completed' && (run.summaryErrors ?? 0) === 0;
    const isFailed  = run.status === 'failed' || run.status === 'error';
    const isPartial = run.status === 'completed' && (run.summaryErrors ?? 0) > 0;
    if (isSuccess) {
      feedItems.push({ type: 'pipeline-success', color: '#059669',
        message: `${p.sourceType} discovery run completed — ${run.envsCreated ?? 0} envs synced, ${run.integrationsCreated ?? 0} new integrations found`,
        timestamp: run.runId });
    } else if (isFailed) {
      const errMsg = (run.error || '').slice(0, 60);
      feedItems.push({ type: 'pipeline-failed', color: '#dc2626',
        message: `${p.sourceType} pipeline failed${errMsg ? ` — ${errMsg}` : ''}`,
        timestamp: run.runId });
    } else if (isPartial) {
      feedItems.push({ type: 'pipeline-partial', color: '#d97706',
        message: `${p.sourceType} pipeline completed with warnings (${run.summaryErrors} errors)`,
        timestamp: run.runId });
    }
  }

  // Source 5a: schedule conflicts (deduplicated by pair)
  const seenConflictPairs = new Set<string>();
  for (const s of schedules) {
    if (!s.conflict || !s.conflictWith) continue;
    const key = [s.envName, s.conflictWith].sort().join('|');
    if (seenConflictPairs.has(key)) continue;
    seenConflictPairs.add(key);
    feedItems.push({ type: 'schedule-conflict', color: '#d97706',
      message: `Schedule conflict detected: ${s.envName} and ${s.conflictWith} overlap on ${(s.windowStart || '').slice(0, 10)}`,
      timestamp: s.windowStart || generatedAt });
  }

  // Source 5b: recent reports
  for (const r of reports) {
    if (!r.lastModified) continue;
    const envCount  = r.envCount  != null ? `${r.envCount} environments` : '';
    const avgHealth = r.avgHealth != null ? `, ${r.avgHealth} avg health` : '';
    feedItems.push({ type: 'report', color: '#059669',
      message: `Weekly report generated${envCount ? ` — ${envCount}${avgHealth}` : ''}`,
      timestamp: r.lastModified });
  }

  feedItems.sort((a, b) => (b.timestamp || '').localeCompare(a.timestamp || ''));
  const activityFeed = feedItems.slice(0, 100);

  // ── 12 · Health Trend Chart (7-day arrays for Chart.js line) ────────────────
  const trendDates: string[] = [];
  for (let i = 6; i >= 0; i--) {
    const d = new Date(snapshotDate);
    d.setUTCDate(d.getUTCDate() - i);
    trendDates.push(d.toISOString().slice(0, 10));
  }
  const trendValues  = trendDates.map(d => trendData[d] ?? null);
  const validValues  = trendValues.filter((v): v is number => v !== null);
  const hasData      = validValues.length > 0;
  const healthTrendChart = {
    dates:   trendDates,
    values:  trendValues,
    yMin:    hasData ? Math.max(0,   Math.min(...validValues) - 10) : 0,
    yMax:    hasData ? Math.min(100, Math.max(...validValues) + 10) : 100,
    hasData,
  };

  // ── 13 · Reports Display (top 4, display-ready) ─────────────────────────────
  const reportsDisplay = reports.slice(0, 4).map((r: any) => {
    const filename    = (r.key || '').split('/').pop() || '';
    const displayName = filename.replace(/\.html?$/, '').replace(/-/g, ' ');
    return {
      displayName,
      date:      r.lastModified || null,
      envCount:  r.envCount     ?? null,
      avgHealth: r.avgHealth    ?? null,
      url:       r.url          || null,
      size:      r.size         ?? null,
    };
  });

  // ── Assemble ─────────────────────────────────────────────────────────────────
  return {
    kpi,
    activeAlerts,
    statusBuckets,
    topologyFocus,
    blastRadius,
    governanceScore,
    pipelineRows,
    reconcilerRows,
    integrationChanges,
    scheduleDisplay,
    activityFeed,
    healthTrendChart,
    reportsDisplay,
  };
}

// ── Utility ───────────────────────────────────────────────────────────────────

function settled<T>(result: PromiseSettledResult<T>): T | null {
  if (result.status === 'fulfilled') return result.value;
  console.warn('Partial data collection failure:', (result as PromiseRejectedResult).reason);
  return null;
}
