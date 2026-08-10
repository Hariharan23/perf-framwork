/**
 * ServiceNow API Lambda
 *
 * Exposes EMS environment integration topology for ServiceNow Service Mapping
 * via an approval-gated workflow. Only environments with a linked CMDB CI
 * (meta_cmdb_ci_sys_id in Neptune) are included.
 *
 * Operations (all under /servicenow/):
 *   GET  service-map     — read-only topology feed
 *   GET  sm-rel-types    — list cmdb_rel_type records from ServiceNow for the UI picker
 *   POST sm-diff         — compare EMS topology with ServiceNow cmdb_rel_ci;
 *                          returns toCreate / toDelete / inSync for approver review
 *   POST sm-push         — execute a user-approved list of create/delete actions
 */

import { APIGatewayProxyEvent, APIGatewayProxyResult } from 'aws-lambda';
import { NeptuneSparqlClient } from '../shared/neptune-sparql-client';
import { ServiceNowAdapter } from '../shared/cmdb/servicenow-adapter';
import { loadCmdbConfig } from '../shared/cmdb/cmdb-config-loader';

const neptune  = new NeptuneSparqlClient();
const ONTOLOGY = 'http://neptune.aws.com/envmgmt/ontology/';

const HEADERS = {
  'Content-Type': 'application/json',
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, x-api-key',
};

function ok(body: unknown): APIGatewayProxyResult {
  return { statusCode: 200, headers: HEADERS, body: JSON.stringify(body) };
}
function fail(code: number, msg: string): APIGatewayProxyResult {
  return { statusCode: code, headers: HEADERS, body: JSON.stringify({ error: msg }) };
}
async function getAdapter(): Promise<ServiceNowAdapter> {
  return new ServiceNowAdapter(await loadCmdbConfig());
}

// ── Shared types & Neptune helper ─────────────────────────────────────────────

interface EmsConnection {
  key: string; parentSysId: string; parentName: string;
  childSysId: string; childName: string;
  integrationName: string; endpointUrl: string; businessPurpose: string;
}

function extractUrlAndPurpose(
  integName: string,
  integConfigs: Map<string, Record<string, string>>,
): { endpointUrl: string; businessPurpose: string } {
  const cfg = integConfigs.get(integName) || {};
  for (const [k, v] of Object.entries(cfg)) {
    if (!k.startsWith('config_') || k.startsWith('config_bp_')) continue;
    if (/^https?:\/\//i.test(v)) {
      const field = k.slice('config_'.length);
      return { endpointUrl: v, businessPurpose: cfg[`config_bp_${field}`] || '' };
    }
  }
  return { endpointUrl: '', businessPurpose: '' };
}

async function buildEmsConnections() {
  const linked    = await neptune.listLinkedCmdbEntities();
  const linkedMap = new Map(linked.map(e => [e.entityId, e]));
  if (linkedMap.size === 0) return { linked, connections: [] as EmsConnection[] };

  const relResult = await neptune.executeSparqlQuery(`
    PREFIX env: <${ONTOLOGY}>
    SELECT ?relationshipType ?sourceEntity ?targetEntity ?sourceEntityId ?targetEntityId WHERE {
      ?rel env:type "Relationship" ;
           env:relationshipType ?relationshipType ;
           env:sourceEntity ?sourceEntity ;
           env:targetEntity ?targetEntity .
      OPTIONAL { ?rel env:sourceEntityId ?sourceEntityId }
      OPTIONAL { ?rel env:targetEntityId ?targetEntityId }
      FILTER(?relationshipType IN ("hasIntegration", "integratesWith", "stubs"))
    }`);
  const rels = (relResult.results?.bindings || []).map((b: any) => ({
    type: b.relationshipType?.value || '', source: b.sourceEntity?.value || '',
    target: b.targetEntity?.value || '', sourceId: b.sourceEntityId?.value || '',
    targetId: b.targetEntityId?.value || '',
  }));

  const cfgResult = await neptune.executeSparqlQuery(`
    PREFIX env: <${ONTOLOGY}>
    SELECT ?name ?prop ?val WHERE {
      ?e env:type "Integration" ; env:name ?name ; ?rawProp ?val .
      BIND(STRAFTER(STR(?rawProp), "${ONTOLOGY}") AS ?prop)
      FILTER(STRSTARTS(?prop, "config_"))
      FILTER(!STRSTARTS(?prop, "config_bpBy_"))
      FILTER(!STRSTARTS(?prop, "config_bpOn_"))
      FILTER(!STRSTARTS(?prop, "config_tag_"))
      FILTER(!STRSTARTS(?prop, "config_orphan"))
    }`);
  const integConfigs = new Map<string, Record<string, string>>();
  for (const b of (cfgResult.results?.bindings || [])) {
    const name = b.name?.value || '', prop = b.prop?.value || '', val = b.val?.value || '';
    if (!name || !prop) continue;
    const m = integConfigs.get(name) || {}; m[prop] = val; integConfigs.set(name, m);
  }

  const envToIntegNames = new Map<string, string[]>();
  const integToTarget   = new Map<string, { id: string; name: string }>();
  for (const rel of rels) {
    if (rel.type === 'hasIntegration') {
      const arr = envToIntegNames.get(rel.sourceId) || []; arr.push(rel.target); envToIntegNames.set(rel.sourceId, arr);
    } else if (!integToTarget.has(rel.source)) {
      integToTarget.set(rel.source, { id: rel.targetId, name: rel.target });
    }
  }

  const connections: EmsConnection[] = [];
  for (const linkedEnv of linked) {
    for (const integName of (envToIntegNames.get(linkedEnv.entityId) || [])) {
      const target = integToTarget.get(integName);
      if (!target?.id) continue;
      const tgt = linkedMap.get(target.id);
      if (!tgt) continue;
      const { endpointUrl, businessPurpose } = extractUrlAndPurpose(integName, integConfigs);
      connections.push({
        key: `${linkedEnv.sysId}::${tgt.sysId}`,
        parentSysId: linkedEnv.sysId, parentName: linkedEnv.entityName,
        childSysId: tgt.sysId, childName: tgt.entityName,
        integrationName: integName, endpointUrl, businessPurpose,
      });
    }
  }
  return { linked, connections };
}

// ── Handler ───────────────────────────────────────────────────────────────────

export const handler = async (event: APIGatewayProxyEvent): Promise<APIGatewayProxyResult> => {
  console.log('servicenow-api:', JSON.stringify({ method: event.httpMethod, path: event.path }));
  if (event.httpMethod === 'OPTIONS') return { statusCode: 200, headers: HEADERS, body: '' };

  const operation = event.pathParameters?.operation || event.queryStringParameters?.operation || '';
  let body: any = {};
  if (event.body) { try { body = JSON.parse(event.body); } catch { return fail(400, 'Invalid JSON body'); } }

  try {
    switch (operation) {

      // ── service-map: read-only topology feed ───────────────────────────────
      case 'service-map': {
        const { linked, connections } = await buildEmsConnections();
        let totalEnvCount = linked.length;
        try {
          const r = await neptune.executeSparqlQuery(
            `PREFIX env: <${ONTOLOGY}> SELECT (COUNT(?e) AS ?cnt) WHERE { ?e env:type "Environment" }`);
          totalEnvCount = parseInt(r.results?.bindings?.[0]?.cnt?.value || String(linked.length), 10);
        } catch { /* non-fatal */ }

        const sysIdToLinked = new Map(linked.map(e => [e.sysId, e]));
        const connsByEnvId  = new Map<string, EmsConnection[]>();
        for (const c of connections) {
          const env = sysIdToLinked.get(c.parentSysId);
          if (!env) continue;
          const arr = connsByEnvId.get(env.entityId) || []; arr.push(c); connsByEnvId.set(env.entityId, arr);
        }

        return ok({
          generatedAt: new Date().toISOString(),
          totalLinkedEnvironments: linked.length,
          totalConnections: connections.length,
          environments: linked.map(e => ({
            emsEntityId: e.entityId, emsEntityName: e.entityName,
            cmdb: { sysId: e.sysId, ciClass: e.ciClass, ciName: e.ciName, lastSynced: e.lastSynced, syncStatus: e.syncStatus },
            integrations: (connsByEnvId.get(e.entityId) || []).map(c => ({
              integrationName: c.integrationName, direction: 'outbound',
              connectedTo: { emsEntityName: c.childName, cmdbSysId: c.childSysId,
                cmdbCiName: sysIdToLinked.get(c.childSysId)?.ciName || '',
                cmdbCiClass: sysIdToLinked.get(c.childSysId)?.ciClass || '' },
              endpointUrl: c.endpointUrl, businessPurpose: c.businessPurpose,
            })),
          })),
          skipped: { reason: 'Environments without a CMDB CI link are excluded.', count: Math.max(0, totalEnvCount - linked.length) },
        });
      }

      // ── sm-rel-types: list ServiceNow relationship types ───────────────────
      case 'sm-rel-types': {
        const adapter = await getAdapter();
        return ok({ relTypes: await adapter.getRelTypes() });
      }

      // ── sm-diff: compare EMS topology vs ServiceNow cmdb_rel_ci ───────────
      case 'sm-diff': {
        const relTypeSysId: string = body.relTypeSysId || '';
        const { linked, connections } = await buildEmsConnections();

        if (linked.length === 0) {
          return ok({ toCreate: [], toDelete: [], inSync: [], relTypeSysId, generatedAt: new Date().toISOString() });
        }

        const adapter = await getAdapter();
        const snRels  = await adapter.getCmdbRelCiForSysIds(
          linked.map(e => e.sysId).filter(Boolean),
          relTypeSysId || undefined,
        );

        const snKeyMap  = new Map(snRels.map(r => [`${r.parentSysId}::${r.childSysId}`, r]));
        const emsKeySet = new Set(connections.map(c => c.key));

        const toCreate = connections.filter(c => !snKeyMap.has(c.key)).map(c => ({ ...c, relTypeSysId }));
        const inSync   = connections.filter(c =>  snKeyMap.has(c.key)).map(c => ({
          ...c, relTypeSysId, snRelSysId: snKeyMap.get(c.key)!.snRelSysId,
        }));
        const toDelete = snRels.filter(r => !emsKeySet.has(`${r.parentSysId}::${r.childSysId}`)).map(r => ({
          key: `${r.parentSysId}::${r.childSysId}`,
          parentSysId: r.parentSysId, parentName: linked.find(e => e.sysId === r.parentSysId)?.entityName || r.parentSysId,
          childSysId:  r.childSysId,  childName:  linked.find(e => e.sysId === r.childSysId)?.entityName  || r.childSysId,
          integrationName: '', endpointUrl: '', businessPurpose: '',
          snRelSysId: r.snRelSysId, relTypeSysId: r.relTypeSysId, relTypeName: r.relTypeName,
        }));

        return ok({ toCreate, toDelete, inSync, relTypeSysId, generatedAt: new Date().toISOString() });
      }

      // ── sm-push: execute approved create/delete actions ────────────────────
      case 'sm-push': {
        const items: Array<{
          action: 'create' | 'delete';
          parentSysId: string; childSysId: string; relTypeSysId: string;
          snRelSysId?: string; label: string;
        }> = body.items || [];

        if (!Array.isArray(items) || items.length === 0) return fail(400, '"items" must be a non-empty array');

        const adapter = await getAdapter();
        const results: Array<{ label: string; action: string; status: string; detail: string }> = [];

        for (const item of items) {
          try {
            if (item.action === 'create') {
              if (!item.relTypeSysId) throw new Error('"relTypeSysId" is required for create');
              const id = await adapter.createCmdbRelCi(item.parentSysId, item.childSysId, item.relTypeSysId);
              results.push({ label: item.label, action: 'create', status: 'ok', detail: `Created cmdb_rel_ci sys_id=${id}` });
            } else if (item.action === 'delete') {
              if (!item.snRelSysId) throw new Error('"snRelSysId" is required for delete');
              await adapter.deleteCmdbRelCi(item.snRelSysId);
              results.push({ label: item.label, action: 'delete', status: 'ok', detail: `Deleted cmdb_rel_ci sys_id=${item.snRelSysId}` });
            } else {
              results.push({ label: item.label, action: String(item.action), status: 'skipped', detail: 'Unknown action' });
            }
          } catch (e) {
            results.push({ label: item.label, action: item.action, status: 'error', detail: e instanceof Error ? e.message : String(e) });
          }
        }

        const okCount = results.filter(r => r.status === 'ok').length;
        return ok({ results, summary: { total: items.length, ok: okCount, errors: results.length - okCount }, executedAt: new Date().toISOString() });
      }

      default:
        return fail(400, `Unknown operation "${operation}". Available: service-map, sm-rel-types, sm-diff, sm-push`);
    }
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    console.error('[servicenow-api] error:', msg);
    return fail(500, `Internal error: ${msg}`);
  }
};
