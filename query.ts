import { APIGatewayProxyEvent, APIGatewayProxyResult } from 'aws-lambda';
import { NeptuneSparqlClient, NetworkData, Entity, NetworkOverview, EnvironmentDetail, NeighborResult } from '../shared/neptune-sparql-client';

const neptuneClient = new NeptuneSparqlClient();

// ── History presentation layer ────────────────────────────────────────────────

type DisplayChange =
  | { field: string; from: string; to: string }
  | { field: string; key: string; value: string }
  | { field: string; value: string };

const ACTION_BADGE: Record<string, string> = {
  created:                      'created',
  updated:                      'updated',
  deleted:                      'deleted',
  'create-integration':         'integration added',
  'delete-integration':         'integration removed',
  'update-integration-target':  'integration retargeted',
  'config-updated':             'config updated',
};

function formatTimestamp(iso: string): string {
  try {
    const d = new Date(iso);
    return d.toLocaleString('en-US', {
      month: 'short', day: 'numeric', year: 'numeric',
      hour: 'numeric', minute: '2-digit', timeZone: 'UTC', timeZoneName: 'short',
    });
  } catch {
    return iso;
  }
}

function buildDisplayChanges(action: string, changes: Record<string, any>): DisplayChange[] {
  const out: DisplayChange[] = [];

  if (!changes || typeof changes !== 'object') return out;

  // Integration events
  if (action === 'create-integration' || action === 'delete-integration') {
    if (changes.integrationName) out.push({ field: 'Integration', value: changes.integrationName });
    if (changes.targetEnvName)   out.push({ field: 'Target',      value: changes.targetEnvName });
    if (changes.connectionType)  out.push({ field: 'Type',        value: changes.connectionType });
    if (changes.relationshipType) out.push({ field: 'Relationship', value: changes.relationshipType });
    return out;
  }

  if (action === 'update-integration-target') {
    if (changes.integrationName) out.push({ field: 'Integration', value: changes.integrationName });
    if (changes.oldTarget && changes.newTarget) {
      (out as any[]).push({ field: 'Target', from: changes.oldTarget, to: changes.newTarget });
    }
    if (changes.connectionType)  out.push({ field: 'Connection type', value: changes.connectionType });
    return out;
  }

  // created — show owner / description / type; configs are set in bulk and not worth listing
  if (action === 'created') {
    if (changes.type)        out.push({ field: 'Type',        value: changes.type });
    if (changes.owner)       out.push({ field: 'Owner',       value: changes.owner });
    if (changes.description) out.push({ field: 'Description', value: changes.description });
    if (changes.primaryUsage) out.push({ field: 'Primary usage', value: changes.primaryUsage });
    const cfgCount = changes.configurations ? Object.keys(changes.configurations).length : 0;
    if (cfgCount > 0) out.push({ field: 'Configurations', value: `${cfgCount} key${cfgCount > 1 ? 's' : ''} set (via pipeline)` });
    return out;
  }

  // config-updated (pipeline-driven) — show only the keys that actually changed
  if (action === 'config-updated') {
    const cfg = changes.configurations || changes;
    for (const [k, v] of Object.entries(cfg as Record<string, any>)) {
      if (typeof v === 'object' && v !== null && 'old' in v && 'new' in v) {
        if ((v as any).old === null) {
          out.push({ field: 'Added', key: k, value: String((v as any).new) });
        } else {
          (out as any[]).push({ field: 'Changed', key: k, from: String((v as any).old), to: String((v as any).new) });
        }
      } else {
        out.push({ field: 'Config', key: k, value: String(v) });
      }
    }
    return out;
  }

  // updated (user/admin-driven) — show metadata diffs + config diffs; skip stats/system fields
  for (const [field, val] of Object.entries(changes)) {
    if (SKIP_FIELDS.has(field) || STATS_FIELDS.has(field)) continue;

    if (field === 'configurations' && typeof val === 'object' && val !== null) {
      // Pipeline or user-triggered config diff: show only changed keys
      for (const [k, v] of Object.entries(val as Record<string, any>)) {
        if (typeof v === 'object' && v !== null && 'old' in v && 'new' in v) {
          if ((v as any).old === null) {
            out.push({ field: 'Config added', key: k, value: String((v as any).new) });
          } else {
            (out as any[]).push({ field: 'Config changed', key: k, from: String((v as any).old), to: String((v as any).new) });
          }
        } else {
          out.push({ field: 'Config', key: k, value: String(v) });
        }
      }
      continue;
    }

    const label = META_LABELS[field] || fieldLabel(field);
    if (typeof val === 'object' && val !== null && 'old' in val && 'new' in val) {
      const from = (val as any).old != null ? String((val as any).old) : '(none)';
      (out as any[]).push({ field: label, from, to: String((val as any).new) });
    } else if (val !== null && val !== undefined) {
      out.push({ field: label, value: String(val) });
    }
  }
  return out;
}

/** Returns a human-readable actor label — strips pipeline suffixes, labels system actors. */
function actorLabel(by: string): string {
  if (!by || by === 'system') return 'an automated process';
  // strip common pipeline/service suffixes for display
  const cleaned = by
    .replace(/[-_](pipeline|job|sync|process|service|lambda|bot)$/i, '')
    .replace(/[-_]/g, ' ')
    .trim();
  return cleaned || by;
}

const STATS_FIELDS = new Set(['cpu', 'memory', 'swap', 'storage', 'availability', 'latency', 'healthScore', 'currentState']);
// Fields that are system/pipeline bookkeeping and should never appear in user-facing summaries
const SKIP_FIELDS  = new Set(['type', 'sourceType', 'envId', 'triggeredBy', 'addedBy', 'removedBy']);
// Human-readable labels for known metadata fields
const META_LABELS: Record<string, string> = {
  name:             'Name',
  owner:            'Owner',
  description:      'Description',
  status:           'Status',
  primaryUsage:     'Primary usage',
  currentUsage:     'Current usage',
  collaborators:    'Collaborators',
  currentBuild:     'Current build',
  scheduledUpdates: 'Scheduled maintenance',
  team:             'Team',
};

function buildSummary(action: string, changes: Record<string, any>, by: string, entityName?: string): string {
  const actor  = actorLabel(by);
  const label  = entityName ? `'${entityName}'` : 'this entity';
  const c      = changes || {};

  // ── Integration events ──────────────────────────────────────────────────
  if (action === 'create-integration') {
    const int  = c.integrationName || 'an integration';
    const tgt  = c.targetEnvName   || 'another environment';
    const conn = c.connectionType  ? ` (${c.connectionType})` : '';
    return `Integration '${int}' connected to '${tgt}'${conn}`;
  }
  if (action === 'delete-integration') {
    const int = c.integrationName || 'an integration';
    const tgt = c.targetEnvName   || 'another environment';
    return `Integration '${int}' disconnected from '${tgt}'`;
  }
  if (action === 'update-integration-target') {
    const int  = c.integrationName || 'an integration';
    const from = c.oldTarget        || 'previous target';
    const to   = c.newTarget        || 'new target';
    const conn = c.connectionType   ? ` (${c.connectionType})` : '';
    return `Integration '${int}' retargeted from '${from}' to '${to}'${conn}`;
  }

  // ── Lifecycle events ────────────────────────────────────────────────────
  if (action === 'created') {
    const type    = c.type || 'Entity';
    const owner   = c.owner ? ` (owned by '${c.owner}')` : '';
    const cfgKeys = c.configurations ? Object.keys(c.configurations) : [];
    const cfgPart = cfgKeys.length > 0
      ? ` — ${cfgKeys.length} config key${cfgKeys.length > 1 ? 's' : ''} loaded via pipeline`
      : '';
    return `New ${type} ${label} registered${owner}${cfgPart}`;
  }

  if (action === 'deleted') {
    return `${label} was removed`;
  }

  // ── Updated ─────────────────────────────────────────────────────────────
  // ── config-updated (pipeline-driven) ───────────────────────────────────
  if (action === 'config-updated') {
    const cfg     = c.configurations || c;
    const entries = Object.entries(cfg as Record<string, any>);
    const added   = entries.filter(([, v]) => typeof v === 'object' && v !== null && (v as any).old === null);
    const changed = entries.filter(([, v]) => typeof v === 'object' && v !== null && (v as any).old !== null);
    const plain   = entries.filter(([, v]) => !(typeof v === 'object' && v !== null && 'old' in (v as any)));
    const total   = entries.length;
    const parts: string[] = [];
    if (changed.length > 0) {
      const preview = changed.slice(0, 2).map(([k]) => k).join(', ') + (changed.length > 2 ? `, +${changed.length - 2} more` : '');
      parts.push(`${changed.length} config value${changed.length > 1 ? 's' : ''} changed (${preview})`);
    }
    if (added.length > 0) {
      parts.push(`${added.length} new config key${added.length > 1 ? 's' : ''} added`);
    }
    if (plain.length > 0 && changed.length === 0 && added.length === 0) {
      parts.push(`${total} configuration${total > 1 ? 's' : ''} pushed by pipeline`);
    }
    return parts.length > 0 ? parts.join('; ') : 'Pipeline config updated';
  }

  // ── updated (user/admin change — metadata and/or configs) ───────────────
  const keys = Object.keys(c);
  const parts: string[] = [];

  // 1. Name rename
  if (c.name && typeof c.name === 'object' && 'old' in c.name) {
    parts.push(`Renamed from '${c.name.old}' to '${c.name.new}'`);
  }

  // 2. Status
  if (c.status) {
    const sv = typeof c.status === 'object' && 'new' in c.status ? c.status.new : c.status;
    const so = typeof c.status === 'object' && 'old' in c.status && c.status.old ? ` (was '${c.status.old}')` : '';
    parts.push(`Status changed to '${sv}'${so}`);
  }

  // 3. Owner
  if (c.owner) {
    const nv = typeof c.owner === 'object' && 'new' in c.owner ? c.owner.new : c.owner;
    const ov = typeof c.owner === 'object' && 'old' in c.owner && c.owner.old ? ` from '${c.owner.old}'` : '';
    parts.push(`Owner changed${ov} to '${nv}'`);
  }

  // 4. Config diff (user edited configs via UI, or pipeline logged as 'updated')
  if (c.configurations && typeof c.configurations === 'object') {
    const entries  = Object.entries(c.configurations as Record<string, any>);
    const added    = entries.filter(([, v]) => typeof v === 'object' && v !== null && (v as any).old === null);
    const modified = entries.filter(([, v]) => typeof v === 'object' && v !== null && (v as any).old !== null);
    const plain    = entries.filter(([, v]) => !(typeof v === 'object' && v !== null && 'old' in (v as any)));
    const cfgParts: string[] = [];
    if (modified.length > 0) {
      const preview = modified.slice(0, 2).map(([k]) => k).join(', ') + (modified.length > 2 ? `, +${modified.length - 2} more` : '');
      cfgParts.push(`${modified.length} config value${modified.length > 1 ? 's' : ''} changed (${preview})`);
    }
    if (added.length > 0) {
      cfgParts.push(`${added.length} new config key${added.length > 1 ? 's' : ''} added`);
    }
    if (plain.length > 0 && modified.length === 0 && added.length === 0) {
      cfgParts.push(`${plain.length} config value${plain.length > 1 ? 's' : ''} updated`);
    }
    if (cfgParts.length > 0) parts.push(cfgParts.join(', '));
  }

  // 5. Stats / health
  if (keys.some((k) => STATS_FIELDS.has(k))) {
    if (c.currentState)  parts.push(`Operational state changed to '${typeof c.currentState === 'object' ? c.currentState.new ?? c.currentState : c.currentState}'`);
    else if (c.healthScore) parts.push(`Health score updated to ${typeof c.healthScore === 'object' ? c.healthScore.new ?? c.healthScore : c.healthScore}`);
    else parts.push('Health metrics refreshed');
  }

  // 6. Other metadata fields
  const HANDLED = new Set(['name', 'status', 'owner', 'configurations', ...STATS_FIELDS]);
  for (const field of keys) {
    if (HANDLED.has(field) || SKIP_FIELDS.has(field)) continue;
    const val = c[field];
    const nv  = typeof val === 'object' && val !== null && 'new' in val ? (val as any).new : val;
    const ov  = typeof val === 'object' && val !== null && 'old' in val && (val as any).old ? ` from '${(val as any).old}'` : '';
    if (field === 'description')      { parts.push('Description updated'); continue; }
    if (field === 'collaborators')    { parts.push('Collaborators updated'); continue; }
    if (field === 'scheduledUpdates') { parts.push('Scheduled maintenance updated'); continue; }
    if (field === 'primaryUsage' || field === 'currentUsage') { parts.push(`Usage set to '${nv}'`); continue; }
    if (field === 'currentBuild')     { parts.push(`Build updated to '${nv}'`); continue; }
    if (field === 'team')             { parts.push(`Team set to '${nv}'`); continue; }
    parts.push(`${META_LABELS[field] || fieldLabel(field)} updated${ov ? ov + ` to '${nv}'` : ''}`);
  }

  return parts.length > 0 ? parts.join('; ') : 'Details updated';
}

function capitalize(s: string): string {
  return s.charAt(0).toUpperCase() + s.slice(1);
}

/** Convert camelCase field names to human-readable labels */
function fieldLabel(field: string): string {
  return field
    .replace(/([A-Z])/g, ' $1')
    .replace(/^./, (c) => c.toUpperCase())
    .trim();
}

function enrichEntry(entry: any): any {
  const action     = entry.action     || '';
  const changes    = entry.changes    || {};
  const by         = entry.user       || 'system';
  const entityName = entry.entityName || '';
  return {
    ...entry,
    badge:              ACTION_BADGE[action] || action,
    by,
    formattedTimestamp: formatTimestamp(entry.timestamp),
    summary:            buildSummary(action, changes, by, entityName),
    displayChanges:     buildDisplayChanges(action, changes),
  };
}

function enrichHistory(entries: any[]): any[] {
  return entries.map(enrichEntry);
}

export const handler = async (event: APIGatewayProxyEvent): Promise<APIGatewayProxyResult> => {
  console.log('Event:', JSON.stringify(event, null, 2));
  
  const headers = {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization, Accept, Origin, X-Requested-With, x-api-key',
    'Access-Control-Max-Age': '300'
  };

  try {
    // Handle CORS preflight requests
    if (event.httpMethod === 'OPTIONS') {
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({ message: 'CORS preflight successful' }),
      };
    }

    // Parse request body if present
    let requestBody: any = {};
    if (event.body) {
      try {
        requestBody = JSON.parse(event.body);
      } catch (e) {
        console.warn('Failed to parse request body:', e);
      }
    }

    // Extract operation from path, query parameters, or request body
    const operation = event.pathParameters?.operation || 
                     event.queryStringParameters?.operation || 
                     requestBody.operation;
    
    if (!operation) {
      return {
        statusCode: 400,
        headers,
        body: JSON.stringify({ 
          error: 'Missing operation parameter',
          availableOperations: [
            'get-network', 'get-environments-list', 'get-network-for-envs',
            'get-entities', 'get-entity', 'get-entity-history', 'get-configurations', 
            'get-config-entries', 'get-deployments', 'get-environments', 
            'get-applications', 'get-integrations', 'get-relationships', 'health',
            'get-network-overview', 'get-environment-detail', 'get-node-neighbors', 'get-node-degrees',
            'get-recent-history'
          ]
        }),
      };
    }

    let result: any;

    switch (operation.toLowerCase()) {
      case 'health':
        const isHealthy = await neptuneClient.healthCheck();
        result = { 
          status: isHealthy ? 'healthy' : 'unhealthy',
          timestamp: new Date().toISOString(),
          service: 'neptune-query'
        };
        break;

      case 'get-network':
        const networkData: NetworkData = await neptuneClient.getNetworkData();
        result = {
          networkData,
          nodeCount: networkData.nodes.length,
          edgeCount: networkData.edges.length,
        };
        break;

      case 'get-environments-list':
        const envList = await neptuneClient.getEnvironmentsList();
        result = {
          environments: envList,
          count: envList.length,
        };
        break;

      case 'get-network-for-envs': {
        // Support GET (comma-separated query string) and POST (JSON body array)
        const rawEnvIds = event.queryStringParameters?.envIds;
        const envIds: string[] = rawEnvIds
          ? rawEnvIds.split(',').map((s: string) => s.trim()).filter(Boolean)
          : (requestBody.envIds || []);
        const rawMaxHops = event.queryStringParameters?.maxHops || requestBody.maxHops;
        const maxHops: number = Math.min(Math.max(parseInt(rawMaxHops) || 3, 1), 20);
        const envNetworkData: NetworkData = await neptuneClient.getNetworkDataForEnvs(
          envIds.length > 0 ? envIds : undefined,
          maxHops
        );
        result = {
          networkData: envNetworkData,
          nodeCount: envNetworkData.nodes.length,
          edgeCount: envNetworkData.edges.length,
          envIds,
        };
        break;
      }

      case 'get-entities': {
        const entityType = event.queryStringParameters?.type || requestBody?.type;

        if (!entityType) {
          // Get all entity types
          const [environments, applications, integrations] = await Promise.all([
            neptuneClient.getEntitiesByType('Environment'),
            neptuneClient.getEntitiesByType('Application'),
            neptuneClient.getEntitiesByType('Integration'),
          ]);
          
          result = {
            entities: {
              Environment: environments,
              Application: applications,
              Integration: integrations,
            },
            totalCount: environments.length + applications.length + integrations.length,
          };
        } else {
          // Get entities of specific type
          const entities = await neptuneClient.getEntitiesByType(entityType);
          result = {
            entities,
            type: entityType,
            count: entities.length,
          };
        }
        break;
      }

      case 'get-entity': {
        const entityId = event.queryStringParameters?.id || event.pathParameters?.id || requestBody?.id;
        
        if (!entityId) {
          return {
            statusCode: 400,
            headers,
            body: JSON.stringify({ 
              error: 'Missing entity ID parameter',
              usage: 'Use ?id=<entity-id> or path parameter'
            }),
          };
        }

        const entity = await neptuneClient.getEntityById(entityId);
        
        if (!entity) {
          return {
            statusCode: 404,
            headers,
            body: JSON.stringify({ 
              error: 'Entity not found',
              entityId 
            }),
          };
        }

        // Reshape: extract config_* keys into a structured configurations[] array.
        // BP triples stored as config_bp_<key>, config_bpBy_<key>, config_bpOn_<key>.
        // Orphan triples (config_orphan*) excluded from the configurations list.
        const BP_SKIP_KEYS = new Set([
          'config_orphanReason', 'config_orphanedAt', 'config_orphanedBy',
          'config_orphanDetail', 'config_previouslyConnected',
        ]);
        const rawEntity = entity as Record<string, any>;
        const _bpVal: Record<string, string> = {};
        const _bpBy:  Record<string, string> = {};
        const _bpOn:  Record<string, string> = {};
        const _cfgKeys: string[] = [];

        for (const key of Object.keys(rawEntity)) {
          if (!key.startsWith('config_')) continue;
          if (BP_SKIP_KEYS.has(key)) continue;
          if (key.startsWith('config_bp_'))   { _bpVal[key.slice('config_bp_'.length)]  = rawEntity[key]; continue; }
          if (key.startsWith('config_bpBy_')) { _bpBy[key.slice('config_bpBy_'.length)] = rawEntity[key]; continue; }
          if (key.startsWith('config_bpOn_')) { _bpOn[key.slice('config_bpOn_'.length)] = rawEntity[key]; continue; }
          _cfgKeys.push(key);
        }

        const configurations = _cfgKeys.map((key) => {
          const cfgName  = key.slice('config_'.length);
          const cfgValue = String(rawEntity[key] ?? '');
          return {
            configName:               cfgName,
            configValue:              cfgValue,
            configType:               /^https?:\/\/|^ftp:\/\//i.test(cfgValue) ? 'url' : 'text',
            businessPurpose:          _bpVal[cfgName] || '',
            businessPurposeUpdatedBy: _bpBy[cfgName]  || '',
            businessPurposeUpdatedOn: _bpOn[cfgName]  || '',
          };
        });

        // Strip all config_* keys from the entity object sent to the client
        const cleanEntity: Record<string, any> = {};
        for (const [k, v] of Object.entries(rawEntity)) {
          if (!k.startsWith('config_')) cleanEntity[k] = v;
        }

        result = { entity: cleanEntity, configurations };
        break;
      }

      case 'get-entity-history':
        const historyEntityId = event.queryStringParameters?.entityId
          || event.queryStringParameters?.id
          || event.pathParameters?.id
          || requestBody?.entityId
          || requestBody?.id;
        
        if (!historyEntityId) {
          return {
            statusCode: 400,
            headers,
            body: JSON.stringify({ 
              error: 'Missing entity ID parameter',
              usage: 'GET: ?entityId=<id> or ?id=<id>  |  POST: { "entityId": "<id>" } or { "id": "<id>" }'
            }),
          };
        }

        const history = enrichHistory(await neptuneClient.getEntityHistory(historyEntityId));
        
        result = { 
          entityId: historyEntityId,
          history,
          count: history.length
        };
        break;

      case 'get-environments':
        const environments = await neptuneClient.getEntitiesByType('Environment');
        result = {
          environments,
          count: environments.length,
        };
        break;

      case 'get-applications':
        const applications = await neptuneClient.getEntitiesByType('Application');
        result = {
          applications,
          count: applications.length,
        };
        break;

      case 'get-integrations':
        const integrations = await neptuneClient.getEntitiesByType('Integration');
        result = {
          integrations,
          count: integrations.length,
        };
        break;

      case 'get-relationships':
        const relQuery = `
          PREFIX env: <${neptuneClient['ontologyPrefix']}>
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
        const relResult = await neptuneClient.executeSparqlQuery(relQuery);
        const relationships = (relResult.results?.bindings || []).map((b: any) => ({
          id: b.id?.value || '',
          relationshipType: b.relationshipType?.value || '',
          sourceEntity: b.sourceEntity?.value || '',
          targetEntity: b.targetEntity?.value || '',
          sourceEntityId: b.sourceEntityId?.value || '',
          targetEntityId: b.targetEntityId?.value || '',
          createdAt: b.createdAt?.value || '',
        }));
        result = {
          relationships,
          count: relationships.length,
        };
        break;

      case 'get-configurations':
        const configEntityId = event.queryStringParameters?.entityId;
        
        if (!configEntityId) {
          return {
            statusCode: 400,
            headers,
            body: JSON.stringify({ 
              error: 'Missing entityId parameter',
              usage: 'Use ?entityId=<entity-id>'
            }),
          };
        }

        const configurations = await neptuneClient.getEntityConfigurations(configEntityId);
        result = {
          configurations,
          entityId: configEntityId,
          count: configurations.length,
        };
        break;

      case 'get-config-entries':
        const configId = event.queryStringParameters?.configId;
        
        if (!configId) {
          return {
            statusCode: 400,
            headers,
            body: JSON.stringify({ 
              error: 'Missing configId parameter',
              usage: 'Use ?configId=<config-id>'
            }),
          };
        }

        const configEntries = await neptuneClient.getConfigurationEntries(configId);
        result = {
          configurationEntries: configEntries,
          configurationId: configId,
          count: configEntries.length,
        };
        break;

      case 'get-deployments':
        const envId = event.queryStringParameters?.environmentId;
        
        if (!envId) {
          return {
            statusCode: 400,
            headers,
            body: JSON.stringify({ 
              error: 'Missing environmentId parameter',
              usage: 'Use ?environmentId=<env-id>'
            }),
          };
        }

        const deployments = await neptuneClient.getEnvironmentDeployments(envId);
        result = {
          deployments,
          environmentId: envId,
          count: deployments.length,
        };
        break;

      case 'sparql-query': {
        const sparql = (requestBody?.query || '').trim();
        if (!sparql) {
          return { statusCode: 400, headers, body: JSON.stringify({ error: 'Missing query parameter' }) };
        }
        // Only allow SELECT queries (read-only)
        const normalised = sparql.replace(/\s+/g, ' ').toUpperCase();
        const forbidden = ['INSERT', 'DELETE', 'DROP', 'CLEAR', 'CREATE', 'LOAD', 'COPY', 'MOVE', 'ADD'];
        for (const keyword of forbidden) {
          const regex = new RegExp(`\\b${keyword}\\b`);
          if (regex.test(normalised)) {
            return { statusCode: 403, headers, body: JSON.stringify({ error: `Write operations (${keyword}) are not allowed. Only SELECT queries are permitted.` }) };
          }
        }
        if (!normalised.includes('SELECT') && !normalised.includes('ASK') && !normalised.includes('DESCRIBE') && !normalised.includes('CONSTRUCT')) {
          return { statusCode: 403, headers, body: JSON.stringify({ error: 'Only SELECT, ASK, DESCRIBE, and CONSTRUCT queries are permitted.' }) };
        }
        const sparqlResult = await neptuneClient.executeSparqlQuery(sparql);
        // Only process SELECT queries for key/value array output
        if (sparqlResult && sparqlResult.results && Array.isArray(sparqlResult.results.bindings)) {
          const bindings = sparqlResult.results.bindings;
          const rows = bindings.map((row: any) => {
            const obj: Record<string, string> = {};
            for (const key of Object.keys(row)) {
              obj[key] = row[key]?.value ?? '';
            }
            return obj;
          });
          result = { results: rows };
        } else {
          // For ASK, DESCRIBE, CONSTRUCT, or error cases, return as-is
          result = sparqlResult;
        }
        break;
      }
      case 'sparql-update':
        return {
          statusCode: 403,
          headers,
          body: JSON.stringify({ error: 'Raw SPARQL UPDATE is disabled for security. Use the structured operations instead.' }),
        };

      case 'get-health-dashboard':
        // Returns all entities with their stats for a health overview dashboard
        const healthNetwork = await neptuneClient.getNetworkData();
        const entitiesWithStats = healthNetwork.nodes
          .filter((node: any) => node.stats && Object.keys(node.stats).length > 0)
          .map((node: any) => ({
            id: node.id,
            name: node.label,
            type: node.type,
            stats: node.stats,
          }));
        result = {
          entities: entitiesWithStats,
          totalWithStats: entitiesWithStats.length,
          totalEntities: healthNetwork.nodes.length,
        };
        break;

      case 'get-network-overview': {
        const overview: NetworkOverview = await neptuneClient.getNetworkOverview();
        result = {
          overview,
          nodeCount: overview.nodes.length,
          edgeCount: overview.edges.length,
        };
        break;
      }

      case 'get-environment-detail': {
        const envId = event.queryStringParameters?.envId || requestBody?.envId;
        if (!envId) {
          return {
            statusCode: 400,
            headers,
            body: JSON.stringify({ error: 'Missing envId parameter' }),
          };
        }
        const detail: EnvironmentDetail = await neptuneClient.getEnvironmentDetail(envId);
        result = { detail };
        break;
      }

      case 'get-node-neighbors': {
        const nodeId = event.queryStringParameters?.nodeId || requestBody?.nodeId;
        if (!nodeId) {
          return {
            statusCode: 400,
            headers,
            body: JSON.stringify({ error: 'Missing nodeId parameter' }),
          };
        }
        const neighbors: NeighborResult = await neptuneClient.getNodeNeighbors(nodeId);
        result = { neighbors };
        break;
      }

      case 'get-node-degrees': {
        const nodeIds: string[] = Array.isArray(requestBody?.nodeIds) ? requestBody.nodeIds : [];
        const degreeMap = await neptuneClient.getNodeDegrees(nodeIds);
        const degrees: Record<string, number> = {};
        degreeMap.forEach((count, id) => { degrees[id] = count; });
        result = { degrees };
        break;
      }

      case 'get-recent-history': {
        const days = Math.min(parseInt(requestBody?.days || '7', 10), 90);
        const limit = Math.min(parseInt(requestBody?.limit || '100', 10), 500);
        const history = enrichHistory(await neptuneClient.getAllRecentHistory({ days, limit }));
        result = { history, count: history.length, days };
        break;
      }

      default:
        return {
          statusCode: 400,
          headers,
          body: JSON.stringify({ 
            error: `Unknown operation: ${operation}`,
            availableOperations: [
              'get-network', 'get-environments-list', 'get-network-for-envs',
              'get-entities', 'get-entity', 'get-configurations', 
              'get-config-entries', 'get-deployments', 'get-environments', 
              'get-applications', 'get-integrations', 'get-relationships', 'health',
              'get-health-dashboard',
              'get-network-overview', 'get-environment-detail', 'get-node-neighbors', 'get-node-degrees',
            'get-recent-history'
            ]
          }),
        };
    }

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        operation,
        data: result,
        timestamp: new Date().toISOString(),
      }),
    };

  } catch (error) {
    console.error('Error processing query request:', error);
    
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({
        error: 'Internal server error',
        message: error instanceof Error ? error.message : 'Unknown error occurred',
        timestamp: new Date().toISOString(),
      }),
    };
  }
};