import { APIGatewayProxyEvent, APIGatewayProxyResult } from 'aws-lambda';
import { NeptuneSparqlClient, NetworkData, Entity, NetworkOverview, EnvironmentDetail, NeighborResult } from '../shared/neptune-sparql-client';

const neptuneClient = new NeptuneSparqlClient();

// ── History presentation layer ────────────────────────────────────────────────

type DisplayChange =
  | { field: string; from: string; to: string }
  | { field: string; key: string; value: string }
  | { field: string; value: string };

const ACTION_BADGE: Record<string, string> = {
  created:              'created',
  updated:              'updated',
  deleted:              'deleted',
  'create-integration': 'integration added',
  'delete-integration': 'integration removed',
  'config-updated':     'config updated',
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
    if (changes.relationshipType) out.push({ field: 'Type',       value: changes.relationshipType });
    return out;
  }

  // created — show owner + config keys
  if (action === 'created') {
    if (changes.owner)       out.push({ field: 'Owner', value: changes.owner });
    if (changes.description) out.push({ field: 'Description', value: changes.description });
    if (changes.configurations && typeof changes.configurations === 'object') {
      for (const [k, v] of Object.entries(changes.configurations)) {
        out.push({ field: 'Configuration', key: k, value: String(v) });
      }
    }
    return out;
  }

  // updated — show field-level diffs
  for (const [field, val] of Object.entries(changes)) {
    if (field === 'configurations' && typeof val === 'object' && val !== null) {
      // val is a per-key diff map: { key: { old, new } } or { key: rawValue }
      for (const [k, v] of Object.entries(val as Record<string, any>)) {
        if (typeof v === 'object' && v !== null && 'old' in v && 'new' in v) {
          if (v.old === null) {
            out.push({ field: 'Config added', key: k, value: String(v.new) });
          } else {
            (out as any[]).push({ field: 'Config changed', key: k, from: String(v.old), to: String(v.new) });
          }
        } else {
          out.push({ field: 'Configuration', key: k, value: String(v) });
        }
      }
      continue;
    }
    if (typeof val === 'object' && val !== null && 'old' in val && 'new' in val) {
      out.push({ field: capitalize(field), from: String(val.old), to: String(val.new) });
    } else {
      out.push({ field: capitalize(field), value: String(val) });
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
const SKIP_FIELDS  = new Set(['type', 'sourceType', 'envId', 'triggeredBy', 'addedBy', 'removedBy', 'owner', 'description']);

function buildSummary(action: string, changes: Record<string, any>, by: string, entityName?: string): string {
  const actor  = actorLabel(by);
  const label  = entityName ? `'${entityName}'` : 'this entity';
  const c      = changes || {};

  // ── Integration events ──────────────────────────────────────────────────
  if (action === 'create-integration') {
    const int = c.integrationName || 'an integration';
    const tgt = c.targetEnvName   || 'another environment';
    return `Integration '${int}' connected to '${tgt}'`;
  }
  if (action === 'delete-integration') {
    const int = c.integrationName || 'an integration';
    const tgt = c.targetEnvName   || 'another environment';
    return `Integration '${int}' disconnected from '${tgt}'`;
  }

  // ── Lifecycle events ────────────────────────────────────────────────────
  if (action === 'created') {
    const type    = c.type || 'Entity';
    const cfgKeys = c.configurations ? Object.keys(c.configurations) : [];
    const owner   = c.owner ? ` (owned by '${c.owner}')` : '';
    const cfgPart = cfgKeys.length > 0
      ? ` — ${cfgKeys.length} configuration${cfgKeys.length > 1 ? 's' : ''} set`
      : '';
    return `New ${type} ${label} registered${owner}${cfgPart}`;
  }

  if (action === 'deleted') {
    return `${label} was removed`;
  }

  // ── Updated ─────────────────────────────────────────────────────────────
  // Identify what categories of things changed
  const keys = Object.keys(c);

  const nameChange   = c.name && typeof c.name === 'object' && 'old' in c.name;
  const statusChange = typeof c.status === 'string';
  const cfgChange    = c.configurations && typeof c.configurations === 'object';
  const statsChange  = keys.some((k) => STATS_FIELDS.has(k));
  const otherFields  = keys.filter((k) => !SKIP_FIELDS.has(k) && !STATS_FIELDS.has(k) && k !== 'name' && k !== 'status' && k !== 'configurations');

  const parts: string[] = [];

  // 1. Name rename — always most prominent
  if (nameChange) {
    parts.push(`Renamed from '${c.name.old}' to '${c.name.new}'`);
  }

  // 2. Status — clear lifecycle signal
  if (statusChange) {
    parts.push(`Status changed to '${c.status}'`);
  }

  // 3. Config diff — summarise concisely, don't enumerate all keys
  if (cfgChange) {
    const entries  = Object.entries(c.configurations as Record<string, any>);
    const added    = entries.filter(([, v]) => typeof v === 'object' && v !== null && (v as any).old === null);
    const modified = entries.filter(([, v]) => typeof v === 'object' && v !== null && (v as any).old !== null);
    const plain    = entries.filter(([, v]) => !(typeof v === 'object' && v !== null && 'old' in (v as any)));
    const total    = entries.length;

    if (modified.length > 0 && added.length > 0) {
      parts.push(`${modified.length} config value${modified.length > 1 ? 's' : ''} changed, ${added.length} new key${added.length > 1 ? 's' : ''} added`);
    } else if (modified.length > 0) {
      const preview = modified.slice(0, 2).map(([k]) => k).join(', ') + (modified.length > 2 ? ', …' : '');
      parts.push(`${modified.length} config value${modified.length > 1 ? 's' : ''} changed (${preview})`);
    } else if (added.length > 0) {
      parts.push(`${added.length} new config key${added.length > 1 ? 's' : ''} added`);
    } else if (plain.length > 0) {
      parts.push(`${total} configuration${total > 1 ? 's' : ''} updated`);
    }
  }

  // 4. Health/stats — group them as one phrase, don't list individual metrics
  if (statsChange) {
    const statsKeys = keys.filter((k) => STATS_FIELDS.has(k));
    if (statsKeys.includes('currentState')) {
      parts.push(`Operational state changed to '${c.currentState}'`);
    } else if (statsKeys.includes('healthScore')) {
      parts.push(`Health metrics refreshed (score: ${c.healthScore})`);
    } else {
      parts.push(`Health metrics refreshed`);
    }
  }

  // 5. Other meaningful fields (e.g. currentBuild, collaborators)
  for (const field of otherFields) {
    const val = c[field];
    if (field === 'currentBuild') { parts.push(`Build updated to '${val}'`); continue; }
    if (field === 'collaborators') { parts.push(`Collaborators updated`); continue; }
    if (field === 'scheduledUpdates') { parts.push(`Scheduled maintenance updated`); continue; }
    if (field === 'primaryUsage' || field === 'currentUsage') { parts.push(`Usage details updated`); continue; }
  }

  return parts.length > 0 ? parts.join('; ') : `Details updated`;
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

        result = { entity };
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