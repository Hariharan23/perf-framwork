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
    neptune.getAllRecentHistory({ days: 7, limit: 100 }),
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

  const snapshot = {
    generatedAt,
    envData:           settled(envsResult),
    healthData:        settled(healthResult),
    networkData:       settled(networkResult),
    integrationsData:  settled(integrationsResult),
    relationshipsData: settled(relationshipsResult),
    historyData:       settled(historyResult),
    degreeData,
    orphanData:        settled(orphansResult),
    aliasData:         settled(aliasResult),
    reconcilerData:    settled(reconcilerResult),
    reportData:        settled(reportsResult),
    pipelineData:      settled(pipelineRunsResult),
    scheduleData:      settled(scheduleResult),
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

  // Parse windows
  const parsed: ScheduleItem[] = items.map(item => {
    const { windowStart, windowEnd, description } = parseScheduleWindow(item.schedule);
    if (!windowStart) console.warn(`collectScheduledUpdates: failed to parse schedule for ${item.envId}: "${item.schedule}"`);
    return { ...item, windowStart, windowEnd, description, conflict: false, conflictWith: '' };
  }).filter((item: ScheduleItem) => item.windowStart);

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

// ── Utility ───────────────────────────────────────────────────────────────────

function settled<T>(result: PromiseSettledResult<T>): T | null {
  if (result.status === 'fulfilled') return result.value;
  console.warn('Partial data collection failure:', (result as PromiseRejectedResult).reason);
  return null;
}
