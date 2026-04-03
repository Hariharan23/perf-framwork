import { APIGatewayProxyEvent, APIGatewayProxyResult } from 'aws-lambda';
import { NeptuneSparqlClient, NetworkData, Entity } from '../shared/neptune-sparql-client';
import { gzipSync } from 'zlib';

const neptuneClient = new NeptuneSparqlClient();

/**
 * Compress large responses to stay under API Gateway's 6MB Lambda response limit.
 * Returns compressed data as base64 inside a regular JSON envelope.
 * JSON gzip compresses ~85-90%, base64 adds ~33% — net ~15-20% of original.
 * This means we can handle raw payloads up to ~30MB.
 */
function buildResponse(
  statusCode: number,
  headers: Record<string, string>,
  body: any,
): APIGatewayProxyResult {
  const jsonBody = JSON.stringify(body);

  // If response > 1MB, compress it inside a JSON envelope
  if (jsonBody.length > 1_000_000) {
    const compressed = gzipSync(Buffer.from(jsonBody, 'utf-8'));
    const envelope = JSON.stringify({
      _compressed: true,
      _originalSize: jsonBody.length,
      _compressedSize: compressed.length,
      payload: compressed.toString('base64'),
    });
    console.log(`Compressed response: ${jsonBody.length} → ${compressed.length} bytes (base64 envelope: ${envelope.length})`);
    return { statusCode, headers, body: envelope };
  }

  return { statusCode, headers, body: jsonBody };
}

/**
 * Strip heavy properties from network nodes for visualization-only (slim) mode.
 * Keeps: id, label, type, properties.status, properties.owner
 * Removes: stats, configurations, config_* fields, all extra properties
 */
function slimNetworkData(data: NetworkData): NetworkData {
  return {
    nodes: data.nodes.map((node: any) => ({
      id: node.id,
      label: node.label,
      type: node.type,
      properties: {
        status: node.properties?.status,
        owner: node.properties?.owner,
      },
    })),
    edges: data.edges.map((edge: any) => ({
      id: edge.id,
      source: edge.source,
      target: edge.target,
      type: edge.type,
      label: edge.label,
      relationshipId: edge.relationshipId,
      properties: {},
    })),
  };
}

export const handler = async (event: APIGatewayProxyEvent): Promise<APIGatewayProxyResult> => {
  console.log('Event:', JSON.stringify(event, null, 2));
  
  const headers = {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization, Accept, Origin, X-Requested-With',
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
            'get-applications', 'get-integrations', 'health', 'sparql-query', 'sparql-update'
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

      case 'get-network': {
        const slim = requestBody.slim === true;
        const networkData: NetworkData = await neptuneClient.getNetworkData();
        result = {
          networkData: slim ? slimNetworkData(networkData) : networkData,
          nodeCount: networkData.nodes.length,
          edgeCount: networkData.edges.length,
        };
        break;
      }

      case 'get-environments-list':
        const envList = await neptuneClient.getEnvironmentsList();
        result = {
          environments: envList,
          count: envList.length,
        };
        break;

      case 'get-network-for-envs': {
        const slimEnv = requestBody.slim === true;
        const envIds: string[] = requestBody.envIds || [];
        const envNetworkData: NetworkData = await neptuneClient.getNetworkDataForEnvs(
          envIds.length > 0 ? envIds : undefined
        );
        result = {
          networkData: slimEnv ? slimNetworkData(envNetworkData) : envNetworkData,
          nodeCount: envNetworkData.nodes.length,
          edgeCount: envNetworkData.edges.length,
          envIds,
        };
        break;
      }

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
        const entityId = event.queryStringParameters?.id || event.pathParameters?.id;
        
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

      case 'sparql-query':
        if (!requestBody.query) {
          throw new Error('SPARQL query is required in request body');
        }
        result = await neptuneClient.executeSparqlQuery(requestBody.query);
        break;

      case 'sparql-update':
        if (!requestBody.query) {
          throw new Error('SPARQL update query is required in request body');
        }
        await neptuneClient.executeSparqlUpdate(requestBody.query);
        result = {
          message: 'SPARQL update executed successfully',
          query: requestBody.query,
        };
        break;

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
              'get-applications', 'get-integrations', 'health', 'sparql-query', 'sparql-update',
              'get-health-dashboard'
            ]
          }),
        };
    }

    return buildResponse(200, headers, {
      success: true,
      operation,
      data: result,
      timestamp: new Date().toISOString(),
    });

  } catch (error) {
    console.error('Error processing query request:', error);
    
    return buildResponse(500, headers, {
      error: 'Internal server error',
      message: error instanceof Error ? error.message : 'Unknown error occurred',
      timestamp: new Date().toISOString(),
    });
  }
};