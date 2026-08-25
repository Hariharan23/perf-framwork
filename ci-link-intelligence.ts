import { APIGatewayProxyEvent, APIGatewayProxyResult } from 'aws-lambda';
import { DynamoDBClient, GetItemCommand, PutItemCommand, ScanCommand, UpdateItemCommand } from '@aws-sdk/client-dynamodb';
import { marshall, unmarshall } from '@aws-sdk/util-dynamodb';
import { randomUUID } from 'crypto';
import { LambdaClient, InvokeCommand } from '@aws-sdk/client-lambda';
import { NeptuneSparqlClient } from '../shared/neptune-sparql-client';
import { loadCmdbConfig } from '../shared/cmdb/cmdb-config-loader';
import { ServiceNowAdapter } from '../shared/cmdb/servicenow-adapter';

const LINKS = process.env.CI_LINKS_TABLE!;
const EVENTS = process.env.CI_LINK_EVENTS_TABLE!;
const ONTOLOGY = 'http://neptune.aws.com/envmgmt/ontology/';
const ddb = new DynamoDBClient({});
const lambdaClient = new LambdaClient({});
const neptune = new NeptuneSparqlClient(process.env.NEPTUNE_ENDPOINT!, process.env.NEPTUNE_REGION || process.env.AWS_REGION || 'us-east-1');
const headers = { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' };

type EntityIdentity = { id: string; name: string; type: string; hostname: string; uniqueIdentifier: string; environmentType: string; ipAddress: string };
type AliasRecord = { hostname: string; environmentName: string };

function response(statusCode: number, data: unknown): APIGatewayProxyResult { return { statusCode, headers, body: JSON.stringify(data) }; }
function norm(v = ''): string { return v.toLowerCase().replace(/\bprd\b/g, 'production').replace(/\bprod\b/g, 'production').replace(/\bdev\b/g, 'development').replace(/\bqa\b/g, 'quality assurance').replace(/[^a-z0-9]+/g, ' ').trim(); }
function tokens(v = ''): Set<string> { return new Set(norm(v).split(' ').filter(x => x.length > 1)); }
function similarity(a: string, b: string): number {
  const aa = tokens(a), bb = tokens(b); if (!aa.size || !bb.size) return 0;
  const common = [...aa].filter(x => bb.has(x)).length;
  return common / new Set([...aa, ...bb]).size;
}
function lifecycleState(fields: Record<string, string>): string {
  const text = norm([fields.operational_status, fields.install_status, fields.life_cycle_stage, fields.life_cycle_stage_status].join(' '));
  if (text.includes('retired') || text.includes('end of life')) return 'RETIRED';
  if (text.includes('non operational') || text.includes('non-operational')) return 'NON_OPERATIONAL';
  return 'OPERATIONAL';
}

/** Use the established CMDB Connector unlink path so Environment-page and
 * Intelligence-page operations share one Neptune mutation implementation. */
async function unlinkThroughCmdbConnector(emsEntityId: string, unlinkedBy: string): Promise<void> {
  const functionArn = process.env.CMDB_CONNECTOR_FUNCTION_ARN;
  if (!functionArn) throw new Error('CMDB_CONNECTOR_FUNCTION_ARN is not configured');
  const payload = {
    httpMethod: 'POST', path: '/cmdb/cmdb-unlink',
    pathParameters: { operation: 'cmdb-unlink' }, queryStringParameters: null,
    headers: {}, body: JSON.stringify({ entityId: emsEntityId, unlinkedBy }),
    isBase64Encoded: false,
  };
  const invoked = await lambdaClient.send(new InvokeCommand({
    FunctionName: functionArn, InvocationType: 'RequestResponse', Payload: Buffer.from(JSON.stringify(payload)),
  }));
  if (invoked.FunctionError) throw new Error(`CMDB Connector invocation failed: ${invoked.FunctionError}`);
  const outer = JSON.parse(Buffer.from(invoked.Payload || []).toString('utf8') || '{}');
  if (outer.statusCode < 200 || outer.statusCode >= 300) {
    let detail = outer.body || 'unknown connector error';
    try { detail = JSON.parse(outer.body).error || detail; } catch { /* keep raw body */ }
    throw new Error(`CMDB Connector unlink failed: ${detail}`);
  }
}

async function listEntities(): Promise<EntityIdentity[]> {
  const q = `PREFIX env: <${ONTOLOGY}> SELECT ?id ?name ?type ?hostname ?uid ?environmentType ?ip WHERE {
    ?e env:id ?id ; env:name ?name ; env:type ?type .
    FILTER(?type IN ("Environment", "Application"))
    OPTIONAL { ?e env:rawHostname ?hostname } OPTIONAL { ?e env:uniqueIdentifier ?uid }
    OPTIONAL { ?e env:environmentType ?environmentType } OPTIONAL { ?e env:ipAddress ?ip }
  } ORDER BY ?name`;
  const r = await neptune.executeSparqlQuery(q);
  return (r.results?.bindings || []).map((b: any) => ({
    id: b.id?.value || '', name: b.name?.value || '', type: b.type?.value || '',
    hostname: b.hostname?.value || '', uniqueIdentifier: b.uid?.value || '',
    environmentType: b.environmentType?.value || '', ipAddress: b.ip?.value || '',
  }));
}

async function getLink(id: string): Promise<any | undefined> {
  const r = await ddb.send(new GetItemCommand({ TableName: LINKS, Key: marshall({ emsEntityId: id }) }));
  return r.Item ? unmarshall(r.Item) : undefined;
}

async function listAliases(): Promise<AliasRecord[]> {
  if (!process.env.ALIAS_TABLE_NAME) return [];
  const r = await ddb.send(new ScanCommand({ TableName: process.env.ALIAS_TABLE_NAME, ProjectionExpression: 'hostname, environmentName, environment_name' }));
  return (r.Items || []).map(item => {
    const a: any = unmarshall(item);
    return { hostname: a.hostname || '', environmentName: a.environmentName || a.environment_name || '' };
  }).filter(a => a.hostname && a.environmentName);
}

async function suggest(entity: EntityIdentity, adapter: ServiceNowAdapter, aliases: AliasRecord[]): Promise<any[]> {
  const entityAliases = aliases.filter(a => norm(a.environmentName) === norm(entity.name)).map(a => a.hostname);
  const signals = [...new Set([entity.hostname, entity.uniqueIdentifier, entity.name, ...entityAliases].filter(Boolean))].slice(0, 6);
  const found = new Map<string, any>();
  // Search independent identity signals concurrently. ServiceNow calls are the
  // dominant latency, so serial lookup here easily exceeds API Gateway's limit.
  const pages = await Promise.all(signals.map(signal => adapter.search(signal, 0, 5).catch(() => ({ results: [], hasMore: false, offset: 0, limit: 5 }))));
  for (const page of pages) for (const c of page.results) found.set(c.sysId, c);

  // Use cheap name similarity to bound expensive detail requests. Fetching
  // every result from every class was the primary cause of suggestion timeouts.
  const shortlist = [...found.values()]
    .sort((a, b) => similarity(entity.name, b.name) - similarity(entity.name, a.name))
    .slice(0, 5);
  const detailed = await Promise.all(shortlist.map(async c => ({
    candidate: c,
    detail: await adapter.fetchCiDetail(c.sysId, c.ciClass).catch(() => null),
  })));
  const ranked: any[] = [];
  for (const { candidate: c, detail } of detailed) {
    if (!detail) continue;
    let score = Math.round(similarity(entity.name, c.name) * 35);
    const evidence: string[] = [];
    if (norm(entity.name) === norm(c.name)) { score += 35; evidence.push('exact normalized name'); }
    if (entityAliases.some(a => norm(a) === norm(c.name) || norm(a) === norm(detail.fields.fqdn || detail.fields.host_name))) { score += 40; evidence.push('EMS alias match'); }
    if (entity.hostname && [detail.fields.fqdn, detail.fields.host_name, detail.fields.name].some(v => norm(v) === norm(entity.hostname))) { score += 45; evidence.push('hostname/FQDN match'); }
    if (entity.ipAddress && detail.fields.ip_address === entity.ipAddress) { score += 30; evidence.push('IP address match'); }
    if (entity.environmentType && norm(detail.fields.environment || detail.fields.environment_type).includes(norm(entity.environmentType))) { score += 15; evidence.push('environment match'); }
    const state = lifecycleState(detail.fields);
    if (state !== 'OPERATIONAL') { score -= 60; evidence.push(`CI is ${state.toLowerCase()}`); }
    ranked.push({ sysId: c.sysId, ciClass: c.ciClass, ciName: c.name, confidence: Math.max(0, Math.min(100, score)), lifecycleState: state, evidence });
  }
  return ranked.sort((a, b) => b.confidence - a.confidence).slice(0, 5);
}

export const handler = async (event: APIGatewayProxyEvent): Promise<APIGatewayProxyResult> => {
  if (event.httpMethod === 'OPTIONS') return response(200, {});
  const operation = event.pathParameters?.operation || event.queryStringParameters?.operation || '';
  let body: any = {}; try { body = event.body ? JSON.parse(event.body) : {}; } catch { return response(400, { error: 'Invalid JSON' }); }
  try {
    if (operation === 'suggestions') {
      const adapter = new ServiceNowAdapter(await loadCmdbConfig());
      const entities = await listEntities();
      const aliases = await listAliases();
      const output: any[] = [];
      const offset = Math.max(0, Number(event.queryStringParameters?.offset || 0));
      const limit = Math.min(Math.max(1, Number(event.queryStringParameters?.limit || 10)), 20);
      const page = entities.slice(offset, offset + limit);
      // Bound concurrency so the request is fast without overwhelming the
      // ServiceNow instance or consuming its entire REST API worker pool.
      for (let i = 0; i < page.length; i += 5) {
        const batch = page.slice(i, i + 5);
        const results = await Promise.all(batch.map(async entity => {
          const link = await getLink(entity.id); if (link?.linkStatus === 'LINKED') return null;
          const candidates = await suggest(entity, adapter, aliases);
          return { entity, recommendationState: candidates[0]?.confidence >= 95 ? 'AUTO_LINK_ELIGIBLE' : candidates[0]?.confidence >= 70 ? 'REVIEW' : 'UNMATCHED', candidates };
        }));
        output.push(...results.filter(Boolean));
      }
      const nextOffset = offset + page.length;
      return response(200, { count: output.length, suggestions: output, offset, limit, totalEntities: entities.length, nextOffset: nextOffset < entities.length ? nextOffset : null });
    }
    if (operation === 'approve') {
      if (!body.emsEntityId || !body.serviceNowSysId || !body.serviceNowClass) return response(400, { error: 'emsEntityId, serviceNowSysId and serviceNowClass are required' });
      const existing = await getLink(body.emsEntityId);
      if (existing?.linkStatus === 'LINKED') return response(409, { error: 'EMS entity already has an approved CI link' });

      // Match the Environment-page workflow: discover ServiceNow's recommended
      // fields, capture their current values, and persist the selection so the
      // existing refresh pipeline can keep the metadata current.
      const adapter = new ServiceNowAdapter(await loadCmdbConfig());
      const definitions = await adapter.getFieldDefinitions(body.serviceNowClass, body.serviceNowSysId);
      const recommended = definitions.filter(field => field.recommended);
      if (!recommended.length) return response(422, { error: `No recommended fields are available for CI class ${body.serviceNowClass}` });
      const selectedFields = recommended.map(field => field.key);
      const metadataFields: Record<string, string> = {};
      for (const field of recommended) if (field.currentValue !== '') metadataFields[field.key] = field.currentValue;

      const now = new Date().toISOString();
      const resolvedCiName = body.serviceNowName || metadataFields.name || body.serviceNowSysId;
      const item = { emsEntityId: body.emsEntityId, emsEntityName: body.emsEntityName || '', serviceNowSysId: body.serviceNowSysId, serviceNowClass: body.serviceNowClass,
        serviceNowName: resolvedCiName, confidence: Number(body.confidence || 0), evidence: body.evidence || [], selectedFields,
        metadataFieldCount: Object.keys(metadataFields).length,
        matchMethod: body.matchMethod || 'dashboard-approval', linkStatus: 'LINKED', healthStatus: 'UNKNOWN',
        approvedBy: body.approvedBy || 'unknown', approvedAt: now, updatedAt: now };
      // Keep the original Environment-page CMDB view and CI Intelligence in sync.
      await neptune.writeCmdbTriples(body.emsEntityId, {
        provider: 'servicenow', ci_sys_id: body.serviceNowSysId, ci_class: body.serviceNowClass,
        ci_name: resolvedCiName, linked_at: now, linked_by: item.approvedBy,
        last_synced_at: now, sync_status: 'ok', field_selection: JSON.stringify(selectedFields),
        ...metadataFields,
      });
      await ddb.send(new PutItemCommand({ TableName: LINKS, Item: marshall(item), ConditionExpression: 'attribute_not_exists(emsEntityId) OR linkStatus <> :linked', ExpressionAttributeValues: marshall({ ':linked': 'LINKED' }) }));
      await ddb.send(new PutItemCommand({ TableName: EVENTS, Item: marshall({ emsEntityId: body.emsEntityId, eventId: `${now}#${randomUUID()}`, eventType: 'LINK_APPROVED', createdAt: now, actor: item.approvedBy, details: JSON.stringify(item) }) }));
      return response(200, { ...item, capturedMetadata: metadataFields });
    }
    if (operation === 'unlink') {
      if (!body.emsEntityId) return response(400, { error: 'emsEntityId is required' });
      const existing = await getLink(body.emsEntityId);
      if (!existing || existing.linkStatus !== 'LINKED') return response(409, { error: 'Only an approved linked CI can be unlinked' });
      const now = new Date().toISOString();
      await unlinkThroughCmdbConnector(body.emsEntityId, body.unlinkedBy || 'unknown');
      await ddb.send(new UpdateItemCommand({
        TableName: LINKS, Key: marshall({ emsEntityId: body.emsEntityId }),
        UpdateExpression: 'SET linkStatus=:s, healthStatus=:h, unlinkedAt=:u, unlinkedBy=:b, updatedAt=:u',
        ExpressionAttributeValues: marshall({ ':s': 'UNLINKED', ':h': 'NOT_APPLICABLE', ':u': now, ':b': body.unlinkedBy || 'unknown' }),
      }));
      await ddb.send(new PutItemCommand({ TableName: EVENTS, Item: marshall({
        emsEntityId: body.emsEntityId, eventId: `${now}#${randomUUID()}`, eventType: 'LINK_UNLINKED',
        createdAt: now, actor: body.unlinkedBy || 'unknown', details: JSON.stringify(existing),
      }) }));
      return response(200, { emsEntityId: body.emsEntityId, unlinked: true, unlinkedAt: now });
    }
    if (operation === 'unlink-all') {
      if (body.confirmation !== 'UNLINK_ALL') return response(400, { error: 'confirmation must equal UNLINK_ALL' });
      const actor = body.unlinkedBy || 'unknown';
      const now = new Date().toISOString();
      // Neptune is authoritative for actual EMS metadata links. Include any
      // DynamoDB-only LINKED records as a defensive cleanup for partial runs.
      const [neptuneLinks, tableScan] = await Promise.all([
        neptune.listLinkedCmdbEntities(),
        ddb.send(new ScanCommand({ TableName: LINKS, FilterExpression: 'linkStatus = :linked', ExpressionAttributeValues: marshall({ ':linked': 'LINKED' }) })),
      ]);
      const targets = new Map<string, any>();
      for (const link of neptuneLinks) targets.set(link.entityId, {
        emsEntityId: link.entityId, emsEntityName: link.entityName, serviceNowSysId: link.sysId,
        serviceNowClass: link.ciClass, serviceNowName: link.ciName,
      });
      for (const raw of tableScan.Items || []) {
        const link = unmarshall(raw); targets.set(link.emsEntityId, { ...targets.get(link.emsEntityId), ...link });
      }

      const results: Array<{ emsEntityId: string; status: string; error?: string }> = [];
      const allTargets = [...targets.values()];
      for (let i = 0; i < allTargets.length; i += 5) {
        await Promise.all(allTargets.slice(i, i + 5).map(async target => {
          try {
            // This removes every meta_cmdb_* triple, including imported field
            // values, mappings, selection metadata, correlation IDs and status.
            await unlinkThroughCmdbConnector(target.emsEntityId, actor);
            const record = { ...target, linkStatus: 'UNLINKED', healthStatus: 'NOT_APPLICABLE',
              unlinkedAt: now, unlinkedBy: actor, updatedAt: now };
            await ddb.send(new PutItemCommand({ TableName: LINKS, Item: marshall(record, { removeUndefinedValues: true }) }));
            await ddb.send(new PutItemCommand({ TableName: EVENTS, Item: marshall({
              emsEntityId: target.emsEntityId, eventId: `${now}#${randomUUID()}`, eventType: 'LINK_BULK_UNLINKED',
              createdAt: now, actor, details: JSON.stringify(target),
            }) }));
            results.push({ emsEntityId: target.emsEntityId, status: 'UNLINKED' });
          } catch (e: any) {
            results.push({ emsEntityId: target.emsEntityId, status: 'FAILED', error: e.message || String(e) });
          }
        }));
      }
      const unlinked = results.filter(result => result.status === 'UNLINKED').length;
      return response(unlinked === results.length ? 200 : 207, {
        total: results.length, unlinked, failed: results.length - unlinked, results, unlinkedAt: now,
      });
    }
    if (operation === 'reject') {
      if (!body.emsEntityId) return response(400, { error: 'emsEntityId is required' });
      const now = new Date().toISOString();
      const item = { emsEntityId: body.emsEntityId, emsEntityName: body.emsEntityName || '', serviceNowSysId: body.serviceNowSysId || '', serviceNowClass: body.serviceNowClass || '', serviceNowName: body.serviceNowName || '', linkStatus: 'REJECTED', rejectionReason: body.reason || '', rejectedBy: body.rejectedBy || 'unknown', updatedAt: now };
      await ddb.send(new PutItemCommand({ TableName: LINKS, Item: marshall(item) }));
      await ddb.send(new PutItemCommand({ TableName: EVENTS, Item: marshall({ emsEntityId: body.emsEntityId, eventId: `${now}#${randomUUID()}`, eventType: 'SUGGESTION_REJECTED', createdAt: now, actor: item.rejectedBy, details: JSON.stringify(item) }) }));
      return response(200, item);
    }
    if (operation === 'health' || operation === 'links') {
      const r = await ddb.send(new ScanCommand({ TableName: LINKS, Limit: Math.min(Number(event.queryStringParameters?.limit || 100), 500) }));
      const entities = await listEntities();
      const entityNames = new Map(entities.map(entity => [entity.id, entity.name]));
      const links: any[] = (r.Items || []).map(item => {
        const link = unmarshall(item);
        return { ...link, emsEntityName: link.emsEntityName || entityNames.get(link.emsEntityId) || link.emsEntityId };
      });
      // Import links created by the existing Environment-page workflow. This
      // makes DynamoDB the shared workflow/read model without changing that UI.
      const knownById = new Map(links.map(link => [link.emsEntityId, link]));
      const legacyLinks = await neptune.listLinkedCmdbEntities();
      const importedAt = new Date().toISOString();
      for (const legacy of legacyLinks.filter(link => knownById.get(link.entityId)?.linkStatus !== 'LINKED')) {
        const imported = {
          emsEntityId: legacy.entityId, emsEntityName: legacy.entityName,
          serviceNowSysId: legacy.sysId, serviceNowClass: legacy.ciClass, serviceNowName: legacy.ciName,
          linkStatus: 'LINKED', healthStatus: legacy.syncStatus === 'error' ? 'CHECK_FAILED' : 'UNKNOWN',
          matchMethod: 'environment-page-import', approvedBy: 'legacy-environment-page',
          approvedAt: importedAt, updatedAt: importedAt, lastVerifiedAt: legacy.lastSynced || '',
        };
        await ddb.send(new PutItemCommand({ TableName: LINKS, Item: marshall(imported) }));
        const existingIndex = links.findIndex(link => link.emsEntityId === legacy.entityId);
        if (existingIndex >= 0) links[existingIndex] = imported; else links.push(imported);
        knownById.set(legacy.entityId, imported);
      }
      // Backfill names for older rejected records that predate serviceNowName.
      // Cap enrichment to ten parallel calls so health reads remain bounded.
      const missingNames = links.filter(link => !link.serviceNowName && link.serviceNowSysId && link.serviceNowClass).slice(0, 10);
      if (missingNames.length) {
        const adapter = new ServiceNowAdapter(await loadCmdbConfig());
        await Promise.all(missingNames.map(async link => {
          try {
            const ci = await adapter.fetchCiLifecycle(link.serviceNowSysId, link.serviceNowClass);
            link.serviceNowName = ci.name || '';
            if (link.serviceNowName) await ddb.send(new UpdateItemCommand({
              TableName: LINKS, Key: marshall({ emsEntityId: link.emsEntityId }),
              UpdateExpression: 'SET serviceNowName=:n', ExpressionAttributeValues: marshall({ ':n': link.serviceNowName }),
            }));
          } catch { /* deleted/inaccessible CI remains identified by sys_id */ }
        }));
      }
      return response(200, { count: links.length, links });
    }
    return response(400, { error: `Unknown operation ${operation}`, available: ['suggestions','approve','reject','unlink','unlink-all','links','health'] });
  } catch (e: any) { console.error(e); return response(500, { error: e.message || String(e) }); }
};
