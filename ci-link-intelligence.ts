import { APIGatewayProxyEvent, APIGatewayProxyResult } from 'aws-lambda';
import { DynamoDBClient, GetItemCommand, PutItemCommand, ScanCommand } from '@aws-sdk/client-dynamodb';
import { marshall, unmarshall } from '@aws-sdk/util-dynamodb';
import { randomUUID } from 'crypto';
import { NeptuneSparqlClient } from '../shared/neptune-sparql-client';
import { loadCmdbConfig } from '../shared/cmdb/cmdb-config-loader';
import { ServiceNowAdapter } from '../shared/cmdb/servicenow-adapter';

const LINKS = process.env.CI_LINKS_TABLE!;
const EVENTS = process.env.CI_LINK_EVENTS_TABLE!;
const ONTOLOGY = 'http://neptune.aws.com/envmgmt/ontology/';
const ddb = new DynamoDBClient({});
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
  for (const signal of signals) {
    const page = await adapter.search(signal, 0, 10);
    for (const c of page.results) found.set(c.sysId, c);
  }
  const ranked: any[] = [];
  for (const c of found.values()) {
    const detail = await adapter.fetchCiDetail(c.sysId, c.ciClass);
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
      for (const entity of entities.slice(0, Math.min(Number(event.queryStringParameters?.limit || 50), 100))) {
        const link = await getLink(entity.id); if (link?.linkStatus === 'LINKED') continue;
        const candidates = await suggest(entity, adapter, aliases);
        output.push({ entity, recommendationState: candidates[0]?.confidence >= 95 ? 'AUTO_LINK_ELIGIBLE' : candidates[0]?.confidence >= 70 ? 'REVIEW' : 'UNMATCHED', candidates });
      }
      return response(200, { count: output.length, suggestions: output });
    }
    if (operation === 'approve') {
      if (!body.emsEntityId || !body.serviceNowSysId || !body.serviceNowClass) return response(400, { error: 'emsEntityId, serviceNowSysId and serviceNowClass are required' });
      const now = new Date().toISOString();
      const item = { emsEntityId: body.emsEntityId, serviceNowSysId: body.serviceNowSysId, serviceNowClass: body.serviceNowClass,
        serviceNowName: body.serviceNowName || '', confidence: Number(body.confidence || 0), evidence: body.evidence || [],
        matchMethod: body.matchMethod || 'dashboard-approval', linkStatus: 'LINKED', healthStatus: 'UNKNOWN',
        approvedBy: body.approvedBy || 'unknown', approvedAt: now, updatedAt: now };
      await ddb.send(new PutItemCommand({ TableName: LINKS, Item: marshall(item), ConditionExpression: 'attribute_not_exists(emsEntityId) OR linkStatus <> :linked', ExpressionAttributeValues: marshall({ ':linked': 'LINKED' }) }));
      await ddb.send(new PutItemCommand({ TableName: EVENTS, Item: marshall({ emsEntityId: body.emsEntityId, eventId: `${now}#${randomUUID()}`, eventType: 'LINK_APPROVED', createdAt: now, actor: item.approvedBy, details: JSON.stringify(item) }) }));
      return response(200, item);
    }
    if (operation === 'reject') {
      if (!body.emsEntityId) return response(400, { error: 'emsEntityId is required' });
      const now = new Date().toISOString();
      const item = { emsEntityId: body.emsEntityId, serviceNowSysId: body.serviceNowSysId || '', serviceNowClass: body.serviceNowClass || '', linkStatus: 'REJECTED', rejectionReason: body.reason || '', rejectedBy: body.rejectedBy || 'unknown', updatedAt: now };
      await ddb.send(new PutItemCommand({ TableName: LINKS, Item: marshall(item) }));
      await ddb.send(new PutItemCommand({ TableName: EVENTS, Item: marshall({ emsEntityId: body.emsEntityId, eventId: `${now}#${randomUUID()}`, eventType: 'SUGGESTION_REJECTED', createdAt: now, actor: item.rejectedBy, details: JSON.stringify(item) }) }));
      return response(200, item);
    }
    if (operation === 'health' || operation === 'links') {
      const r = await ddb.send(new ScanCommand({ TableName: LINKS, Limit: Math.min(Number(event.queryStringParameters?.limit || 100), 500) }));
      const links = (r.Items || []).map(item => unmarshall(item));
      return response(200, { count: links.length, links });
    }
    return response(400, { error: `Unknown operation ${operation}`, available: ['suggestions','approve','reject','links','health'] });
  } catch (e: any) { console.error(e); return response(500, { error: e.message || String(e) }); }
};
