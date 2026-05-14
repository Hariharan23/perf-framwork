import { APIGatewayProxyEvent, APIGatewayProxyResult } from 'aws-lambda';
import { SSMClient, GetParametersByPathCommand } from '@aws-sdk/client-ssm';
import { NeptuneSparqlClient, NetworkData, Entity, NetworkOverview, EnvironmentDetail, NeighborResult } from '../shared/neptune-sparql-client';

const neptuneClient = new NeptuneSparqlClient();

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
            'get-network-overview', 'get-environment-detail', 'get-node-neighbors', 'get-node-degrees'
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

      case 'get-network-for-envs':
        const envIds: string[] = requestBody.envIds || [];
        const maxHops: number = Math.min(Math.max(parseInt(requestBody.maxHops) || 3, 1), 20);
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

      case 'get-entities':
        const entityType = event.queryStringParameters?.type;
        
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

      case 'get-entity':
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

      case 'get-entity-history':
        const historyEntityId = event.queryStringParameters?.id || requestBody?.entityId;
        
        if (!historyEntityId) {
          return {
            statusCode: 400,
            headers,
            body: JSON.stringify({ 
              error: 'Missing entity ID parameter',
              usage: 'Use ?id=<entity-id> query parameter or entityId in request body'
            }),
          };
        }

        const history = await neptuneClient.getEntityHistory(historyEntityId);
        
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
        result = sparqlResult;
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

        // Cross-reference SSM pipeline configs to identify primary pipeline environments.
        // Each config JSON has an `envName` field (the human-readable name written to Neptune).
        // Match against node.name (case-insensitive) to mark those nodes as hasPipeline.
        const pipelineEnvNames = new Set<string>();
        try {
          const ssm = new SSMClient({ region: process.env.AWS_REGION || 'us-east-1' });
          let nextToken: string | undefined;
          do {
            const res = await ssm.send(new GetParametersByPathCommand({
              Path: '/ems/pipelines',
              Recursive: true,
              WithDecryption: true,
              NextToken: nextToken,
            }));
            for (const param of res.Parameters || []) {
              try {
                const cfg = JSON.parse(param.Value || '{}');
                if (cfg.envName) {
                  pipelineEnvNames.add((cfg.envName as string).toLowerCase());
                }
              } catch { /* skip malformed param */ }
            }
            nextToken = res.NextToken;
          } while (nextToken);
        } catch (ssmErr) {
          console.warn('get-network-overview: SSM pipeline lookup failed (non-fatal):', ssmErr);
        }

        // Stamp hasPipeline on nodes whose name matches a pipeline envName
        if (pipelineEnvNames.size > 0) {
          for (const node of overview.nodes) {
            if (node.name && pipelineEnvNames.has(node.name.toLowerCase())) {
              (node as any).hasPipeline = true;
            }
          }
        }

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
              'get-network-overview', 'get-environment-detail', 'get-node-neighbors', 'get-node-degrees'
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