import { APIGatewayProxyEvent, APIGatewayProxyResult } from 'aws-lambda';
import { NeptuneSparqlClient, Environment, Application, Integration } from '../shared/neptune-sparql-client';

const neptuneClient = new NeptuneSparqlClient();

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
    let body: any = {};
    if (event.body) {
      try {
        body = JSON.parse(event.body);
      } catch (parseError) {
        return {
          statusCode: 400,
          headers,
          body: JSON.stringify({ error: 'Invalid JSON in request body' }),
        };
      }
    }

    // Extract operation from path, query parameters, or request body
    const operation = event.pathParameters?.operation || 
                     event.queryStringParameters?.operation || 
                     body.operation;
    
    if (!operation) {
      return {
        statusCode: 400,
        headers,
        body: JSON.stringify({ 
          error: 'Missing operation parameter',
          availableOperations: [
            'create-environment', 'create-application', 'create-integration', 
            'create-environment-config', 'create-application-config', 'create-integration-config',
            'create-config-entry', 'create-deployment', 'initialize-ontology', 'health', 'create',
            'update-entity', 'bulk-ingest'
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
          service: 'neptune-ingestion'
        };
        break;

      case 'create':
        // Generic create operation that determines entity type from body
        if (!body.type || !body.name) {
          return {
            statusCode: 400,
            headers,
            body: JSON.stringify({ 
              error: 'Missing required fields: type, name',
              received: body,
              supportedTypes: ['Environment', 'Application', 'Integration']
            }),
          };
        }

        // ========== VALIDATION: Prevent duplicate entities ==========
        
        const entityType = body.type.toLowerCase();
        
        // 1. Environment names must be unique (cannot have duplicates)
        if (entityType === 'environment') {
          const existingEnv = await neptuneClient.checkEntityExists(body.name, 'Environment');
          if (existingEnv) {
            return {
              statusCode: 409,
              headers,
              body: JSON.stringify({ 
                error: 'Environment with this name already exists',
                name: body.name,
                message: 'Environment names must be unique. Please use a different name.'
              }),
            };
          }
        }
        
        // 2. Application/Integration can have duplicate names if configs differ
        if (entityType === 'application' || entityType === 'integration') {
          const typeName = entityType.charAt(0).toUpperCase() + entityType.slice(1);
          const existingEntities = await neptuneClient.getEntitiesByTypeAndName(typeName, body.name);
          
          if (existingEntities.length > 0) {
            // Prepare new entity's configurations
            const newConfigs: Record<string, any> = {};
            
            if (entityType === 'application') {
              if (body.version) newConfigs.version = body.version;
              if (body.endpoint) newConfigs.endpoint = body.endpoint;
            } else if (entityType === 'integration') {
              if (body.sourceService) newConfigs.sourceService = body.sourceService;
              if (body.targetService) newConfigs.targetService = body.targetService;
              if (body.protocol) newConfigs.protocol = body.protocol;
            }
            
            // Add custom configurations
            if (body.configurations && typeof body.configurations === 'object') {
              Object.assign(newConfigs, body.configurations);
            }
            
            // Check if any existing entity has identical configurations
            for (const existing of existingEntities) {
              const existingConfigs = await neptuneClient.getEntityConfigurationsAsObject(existing.id);
              
              // Compare configurations - check if they're identical
              const newKeys = Object.keys(newConfigs).sort();
              const existingKeys = Object.keys(existingConfigs).sort();
              
              const keysMatch = JSON.stringify(newKeys) === JSON.stringify(existingKeys);
              let valuesMatch = true;
              
              if (keysMatch) {
                for (const key of newKeys) {
                  if (newConfigs[key] !== existingConfigs[key]) {
                    valuesMatch = false;
                    break;
                  }
                }
              } else {
                valuesMatch = false;
              }
              
              // If configurations are identical, reject the duplicate
              if (keysMatch && valuesMatch) {
                return {
                  statusCode: 409,
                  headers,
                  body: JSON.stringify({ 
                    error: `${typeName} with identical name and configurations already exists`,
                    name: body.name,
                    existingId: existing.id,
                    message: `A ${typeName} with this name and identical configurations already exists. Please either use a different name or provide different configurations.`,
                    existingConfigs,
                    newConfigs
                  }),
                };
              }
            }
            
            // If we reach here, configurations differ - allow creation
            console.log(`Allowing duplicate ${typeName} name "${body.name}" with different configurations`);
          }
        }
        
        // ========== END VALIDATION ==========

        // Route to specific create operation based on type
        switch (body.type.toLowerCase()) {
          case 'environment':
            const env: Omit<Environment, 'id'> = {
              name: body.name,
              type: 'Environment',
              uniqueIdentifier: body.uniqueIdentifier,
              owner: body.owner,
              description: body.description
            };
            result = await neptuneClient.createEnvironment(env);
            
            // Store entity-specific properties as config_ properties
            const entityConfig: Record<string, any> = {};
            if (body.region) entityConfig.region = body.region;
            if (body.endpoint) entityConfig.endpoint = body.endpoint;
            
            // Add custom configurations if provided
            if (body.configurations && typeof body.configurations === 'object') {
              Object.assign(entityConfig, body.configurations);
            }
            
            // Store all configurations with config_ prefix
            if (Object.keys(entityConfig).length > 0) {
              try {
                console.log(`Attempting to add ${Object.keys(entityConfig).length} configuration properties to environment ${result.id}`);
                console.log('Configuration data:', JSON.stringify(entityConfig));
                await neptuneClient.addConfigurationProperties(result.id, entityConfig);
                console.log(`Successfully added configuration properties to environment ${result.id}`);
              } catch (configError) {
                console.error(`Failed to add configuration properties to environment ${result.id}:`, configError);
                // Include this in the response so the user knows
                result.configurationWarning = `Warning: Entity created but configurations could not be saved: ${configError instanceof Error ? configError.message : 'Unknown error'}`;
              }
            }
            
            // Create relationships if provided
            if (body.relationships && Array.isArray(body.relationships)) {
              for (const rel of body.relationships) {
                try {
                  await neptuneClient.createRelationship(body.name, rel.type, rel.target, result.id);
                } catch (relError) {
                  console.warn(`Failed to create relationship ${rel.type} to ${rel.target}:`, relError);
                }
              }
            }
            
            // Log history event
            await neptuneClient.logHistory(result.id, body.name, 'created', {
              type: 'Environment',
              configurations: entityConfig,
              owner: body.owner,
              description: body.description
            }, body.owner);
            
            break;

          case 'application':
            const app: Omit<Application, 'id'> = {
              name: body.name,
              type: 'Application',
              owner: body.owner,
              description: body.description
            };
            result = await neptuneClient.createApplication(app);
            
            // Store entity-specific properties as config_ properties
            const appConfig: Record<string, any> = {};
            if (body.version) appConfig.version = body.version;
            if (body.endpoint) appConfig.endpoint = body.endpoint;
            
            // Add custom configurations if provided
            if (body.configurations && typeof body.configurations === 'object') {
              Object.assign(appConfig, body.configurations);
            }
            
            // Store all configurations with config_ prefix
            if (Object.keys(appConfig).length > 0) {
              try {
                console.log(`Attempting to add ${Object.keys(appConfig).length} configuration properties to application ${result.id}`);
                console.log('Configuration data:', JSON.stringify(appConfig));
                await neptuneClient.addConfigurationProperties(result.id, appConfig);
                console.log(`Successfully added configuration properties to application ${result.id}`);
              } catch (configError) {
                console.error(`Failed to add configuration properties to application ${result.id}:`, configError);
                result.configurationWarning = `Warning: Entity created but configurations could not be saved: ${configError instanceof Error ? configError.message : 'Unknown error'}`;
              }
            }
            
            // Create relationships if provided
            if (body.relationships && Array.isArray(body.relationships)) {
              for (const rel of body.relationships) {
                try {
                  await neptuneClient.createRelationship(body.name, rel.type, rel.target, result.id);
                } catch (relError) {
                  console.warn(`Failed to create relationship ${rel.type} to ${rel.target}:`, relError);
                }
              }
            }
            
            // Log history event
            await neptuneClient.logHistory(result.id, body.name, 'created', {
              type: 'Application',
              configurations: appConfig,
              owner: body.owner,
              description: body.description
            }, body.owner);
            
            break;

          case 'integration':
            const integration: Omit<Integration, 'id'> = {
              name: body.name,
              type: 'Integration',
              owner: body.owner,
              description: body.description
            };
            result = await neptuneClient.createIntegration(integration);
            
            // Store entity-specific properties as config_ properties
            const intConfig: Record<string, any> = {};
            if (body.sourceService) intConfig.sourceService = body.sourceService;
            if (body.targetService) intConfig.targetService = body.targetService;
            if (body.protocol) intConfig.protocol = body.protocol;
            
            // Add custom configurations if provided
            if (body.configurations && typeof body.configurations === 'object') {
              Object.assign(intConfig, body.configurations);
            }
            
            // Store all configurations with config_ prefix
            if (Object.keys(intConfig).length > 0) {
              try {
                console.log(`Attempting to add ${Object.keys(intConfig).length} configuration properties to integration ${result.id}`);
                console.log('Configuration data:', JSON.stringify(intConfig));
                await neptuneClient.addConfigurationProperties(result.id, intConfig);
                console.log(`Successfully added configuration properties to integration ${result.id}`);
              } catch (configError) {
                console.error(`Failed to add configuration properties to integration ${result.id}:`, configError);
                result.configurationWarning = `Warning: Entity created but configurations could not be saved: ${configError instanceof Error ? configError.message : 'Unknown error'}`;
              }
            }
            
            // Create relationships if provided
            if (body.relationships && Array.isArray(body.relationships)) {
              for (const rel of body.relationships) {
                try {
                  await neptuneClient.createRelationship(body.name, rel.type, rel.target, result.id);
                } catch (relError) {
                  console.warn(`Failed to create relationship ${rel.type} to ${rel.target}:`, relError);
                }
              }
            }
            
            // Log history event
            await neptuneClient.logHistory(result.id, body.name, 'created', {
              type: 'Integration',
              configurations: intConfig,
              owner: body.owner,
              description: body.description
            }, body.owner);
            
            break;

          default:
            return {
              statusCode: 400,
              headers,
              body: JSON.stringify({ 
                error: 'Unsupported entity type',
                received: body.type,
                supported: ['Environment', 'Application', 'Integration']
              }),
            };
        }
        break;

      case 'create-environment':
        if (!body.name) {
          return {
            statusCode: 400,
            headers,
            body: JSON.stringify({ 
              error: 'Missing required fields: name',
              received: body 
            }),
          };
        }

        const environment: Omit<Environment, 'id'> = {
          name: body.name,
          type: 'Environment',
          uniqueIdentifier: body.uniqueIdentifier,
          owner: body.owner,
          description: body.description || `Environment ${body.name}`,
          status: body.status || 'active',
        };

        result = await neptuneClient.createEnvironment(environment);
        break;

      case 'create-application':
        if (!body.name) {
          return {
            statusCode: 400,
            headers,
            body: JSON.stringify({ 
              error: 'Missing required field: name',
              received: body 
            }),
          };
        }

        const application: Omit<Application, 'id'> = {
          name: body.name,
          type: 'Application',
          uniqueIdentifier: body.uniqueIdentifier,
          owner: body.owner,
          description: body.description || `Application ${body.name}`,
          status: body.status || 'active',
        };

        result = await neptuneClient.createApplication(application);
        break;

      case 'create-integration':
        if (!body.name) {
          return {
            statusCode: 400,
            headers,
            body: JSON.stringify({ 
              error: 'Missing required field: name',
              received: body 
            }),
          };
        }

        const integration: Omit<Integration, 'id'> = {
          name: body.name,
          type: 'Integration',
          uniqueIdentifier: body.uniqueIdentifier,
          owner: body.owner,
          description: body.description || `Integration: ${body.name}`,
          status: body.status || 'active',
        };

        result = await neptuneClient.createIntegration(integration);
        break;

      case 'create-environment-config':
        if (!body.entityId) {
          return {
            statusCode: 400,
            headers,
            body: JSON.stringify({ error: 'Missing entityId for environment configuration' }),
          };
        }

        const envConfig = await neptuneClient.createEnvironmentConfiguration({
          type: 'EnvironmentConfiguration',
          configurationMap: body.configurationMap,
        }, body.entityId);
        result = envConfig;
        break;

      case 'create-application-config':
        if (!body.entityId) {
          return {
            statusCode: 400,
            headers,
            body: JSON.stringify({ error: 'Missing entityId for application configuration' }),
          };
        }

        const appConfig = await neptuneClient.createApplicationConfiguration({
          type: 'ApplicationConfiguration',
          configurationMap: body.configurationMap,
        }, body.entityId);
        result = appConfig;
        break;

      case 'create-integration-config':
        if (!body.entityId) {
          return {
            statusCode: 400,
            headers,
            body: JSON.stringify({ error: 'Missing entityId for integration configuration' }),
          };
        }

        const intConfig = await neptuneClient.createIntegrationConfiguration({
          type: 'IntegrationConfiguration',
          configurationMap: body.configurationMap,
          sourceService: body.sourceService,
          targetService: body.targetService,
        }, body.entityId);
        result = intConfig;
        break;

      case 'create-config-entry':
        if (!body.configurationId || !body.key || !body.value) {
          return {
            statusCode: 400,
            headers,
            body: JSON.stringify({ 
              error: 'Missing required fields: configurationId, key, value' 
            }),
          };
        }

        const configEntry = await neptuneClient.createConfigurationEntry({
          configurationKey: body.key,
          configurationValue: body.value,
          configurationType: body.type,
        }, body.configurationId);
        result = configEntry;
        break;

      case 'create-deployment':
        if (!body.applicationId || !body.environmentId) {
          return {
            statusCode: 400,
            headers,
            body: JSON.stringify({ 
              error: 'Missing required fields: applicationId, environmentId' 
            }),
          };
        }

        await neptuneClient.createDeployment(body.applicationId, body.environmentId);
        result = { 
          message: 'Deployment relationship created successfully',
          applicationId: body.applicationId,
          environmentId: body.environmentId 
        };
        break;

      case 'initialize-ontology':
        await neptuneClient.initializeOntology();
        result = { 
          message: 'Ontology initialized successfully',
          timestamp: new Date().toISOString()
        };
        break;

      case 'create-relationship':
        const { sourceEntityName, relationshipType, targetEntityName } = body;
        
        if (!sourceEntityName || !relationshipType || !targetEntityName) {
          return {
            statusCode: 400,
            headers,
            body: JSON.stringify({ 
              error: 'Missing required fields: sourceEntityName, relationshipType, targetEntityName',
              received: body 
            }),
          };
        }

        try {
          await neptuneClient.createRelationship(sourceEntityName, relationshipType, targetEntityName);
          result = {
            message: 'Relationship created successfully',
            sourceEntityName,
            relationshipType,
            targetEntityName,
            timestamp: new Date().toISOString()
          };
        } catch (relationshipError) {
          const errorMessage = relationshipError instanceof Error ? relationshipError.message : 'Unknown error';
          return {
            statusCode: 400,
            headers,
            body: JSON.stringify({ 
              error: `Failed to create relationship: ${errorMessage}`,
              sourceEntityName,
              relationshipType,
              targetEntityName
            }),
          };
        }
        break;

      case 'reverse-relationship':
        if (!body.relationshipId) {
          return {
            statusCode: 400,
            headers,
            body: JSON.stringify({ 
              error: 'Missing required field: relationshipId',
              received: body 
            }),
          };
        }

        try {
          const reverseResult = await neptuneClient.reverseRelationship(body.relationshipId);
          
          if (!reverseResult.success) {
            return {
              statusCode: 404,
              headers,
              body: JSON.stringify({ 
                error: reverseResult.message
              }),
            };
          }
          
          result = {
            success: true,
            message: reverseResult.message,
            relationshipId: body.relationshipId,
            timestamp: new Date().toISOString()
          };
        } catch (reverseError) {
          const errorMessage = reverseError instanceof Error ? reverseError.message : 'Unknown error';
          return {
            statusCode: 400,
            headers,
            body: JSON.stringify({ 
              error: `Failed to reverse relationship: ${errorMessage}`,
              relationshipId: body.relationshipId
            }),
          };
        }
        break;

      case 'delete-relationship':
        if (!body.relationshipId) {
          return {
            statusCode: 400,
            headers,
            body: JSON.stringify({ 
              error: 'Missing required field: relationshipId',
              received: body 
            }),
          };
        }

        try {
          const deleteResult = await neptuneClient.deleteRelationship(body.relationshipId);
          
          result = {
            success: true,
            message: deleteResult.message,
            relationshipId: body.relationshipId,
            timestamp: new Date().toISOString()
          };
        } catch (deleteError) {
          const errorMessage = deleteError instanceof Error ? deleteError.message : 'Unknown error';
          return {
            statusCode: 400,
            headers,
            body: JSON.stringify({ 
              error: `Failed to delete relationship: ${errorMessage}`,
              relationshipId: body.relationshipId
            }),
          };
        }
        break;

      case 'update-entity':
        if (!body.id || !body.type || !body.name) {
          return {
            statusCode: 400,
            headers,
            body: JSON.stringify({ 
              error: 'Missing required fields: id, type, name',
              received: body 
            }),
          };
        }

        try {
          // Strip URI prefix if present to get just the UUID
          let entityId = body.id;
          const ontologyPrefix = 'http://neptune.aws.com/envmgmt/ontology/';
          if (entityId.startsWith(ontologyPrefix)) {
            entityId = entityId.replace(ontologyPrefix, '');
          }
          
          console.log(`Update request for entity ID: ${entityId} (original: ${body.id})`);
          console.log(`Full update body:`, JSON.stringify(body, null, 2));
          
          // Escape special characters in strings for SPARQL
          const escapeSparql = (str: string) => {
            if (!str) return '';
            return str.replace(/\\/g, '\\\\').replace(/"/g, '\\"').replace(/\n/g, '\\n').replace(/\r/g, '\\r').replace(/\t/g, '\\t');
          };
          
          // First, find the entity URI
          const findEntityQuery = `
            PREFIX env: <http://neptune.aws.com/envmgmt/ontology/>
            
            SELECT ?entity WHERE {
              ?entity env:id "${escapeSparql(entityId)}" .
            }
          `;
          
          console.log(`Executing find entity query:`, findEntityQuery);
          const findResult = await neptuneClient.executeSparqlQuery(findEntityQuery);
          console.log(`Find entity result:`, JSON.stringify(findResult, null, 2));
          
          if (!findResult.results?.bindings?.length) {
            // Try to list all entities to debug
            const debugQuery = `
              PREFIX env: <http://neptune.aws.com/envmgmt/ontology/>
              SELECT ?entity ?id WHERE {
                ?entity env:id ?id .
              }
              LIMIT 10
            `;
            const debugResult = await neptuneClient.executeSparqlQuery(debugQuery);
            console.log(`Debug - All entities in database:`, JSON.stringify(debugResult, null, 2));
            
            throw new Error(`Entity with ID ${entityId} not found in Neptune. Searched for entity with env:id property matching "${entityId}".`);
          }
          
          const entityUri = findResult.results.bindings[0].entity.value;
          console.log(`Found entity URI: ${entityUri} for ID: ${entityId}`);
          
          // Get the old entity name before updating (for relationship migration)
          const getOldNameQuery = `
            PREFIX env: <http://neptune.aws.com/envmgmt/ontology/>
            
            SELECT ?oldName WHERE {
              <${entityUri}> env:name ?oldName .
            }
          `;
          
          const oldNameResult = await neptuneClient.executeSparqlQuery(getOldNameQuery);
          const oldName = oldNameResult.results?.bindings?.[0]?.oldName?.value || '';
          console.log(`Old entity name: ${oldName}, New entity name: ${body.name}`);
          
          // Update basic entity properties
          const updateQuery = `
            PREFIX env: <http://neptune.aws.com/envmgmt/ontology/>
            
            DELETE {
              <${entityUri}> env:name ?oldName .
              <${entityUri}> env:description ?oldDesc .
              <${entityUri}> env:owner ?oldOwner .
              <${entityUri}> env:status ?oldStatus .
            }
            INSERT {
              <${entityUri}> env:name "${escapeSparql(body.name)}" .
              <${entityUri}> env:description "${escapeSparql(body.description || '')}" .
              <${entityUri}> env:owner "${escapeSparql(body.owner || '')}" .
              <${entityUri}> env:status "${escapeSparql(body.status || 'active')}" .
            }
            WHERE {
              OPTIONAL { <${entityUri}> env:name ?oldName }
              OPTIONAL { <${entityUri}> env:description ?oldDesc }
              OPTIONAL { <${entityUri}> env:owner ?oldOwner }
              OPTIONAL { <${entityUri}> env:status ?oldStatus }
            }
          `;
          
          console.log('Executing update query:', updateQuery);
          await neptuneClient.executeSparqlUpdate(updateQuery);
          
          // ========== PRESERVE RELATIONSHIPS ON NAME CHANGE ==========
          // If the entity name changed, migrate all relationships to use the new name
          if (oldName && body.name && oldName !== body.name) {
            console.log(`Entity name changed from "${oldName}" to "${body.name}" - migrating relationships`);
            const migrationResult = await neptuneClient.migrateEntityRelationships(oldName, body.name);
            console.log(`Successfully migrated ${migrationResult.updated} relationships`);
          }
          // ========== END RELATIONSHIP PRESERVATION ==========
          
          // Delete old config properties
          const deleteConfigQuery = `
            PREFIX env: <http://neptune.aws.com/envmgmt/ontology/>
            
            DELETE {
              <${entityUri}> ?configProp ?configValue .
            }
            WHERE {
              <${entityUri}> ?configProp ?configValue .
              FILTER(CONTAINS(STR(?configProp), "config_"))
            }
          `;
          
          console.log('Deleting old config properties');
          await neptuneClient.executeSparqlUpdate(deleteConfigQuery);
          
          // Add new configuration properties if provided
          if (body.configurations && typeof body.configurations === 'object') {
            console.log(`Updating configurations for entity ${entityId}:`, body.configurations);
            await neptuneClient.addConfigurationProperties(entityId, body.configurations);
          }
          
          // Log history event
          const updateChanges: Record<string, any> = {};
          if (oldName && body.name && oldName !== body.name) {
            updateChanges.name = { old: oldName, new: body.name };
          }
          if (body.configurations) {
            updateChanges.configurations = body.configurations;
          }
          
          await neptuneClient.logHistory(entityId, body.name, 'updated', updateChanges, body.owner);
          
          result = {
            id: entityId,
            name: body.name,
            type: body.type,
            message: 'Entity updated successfully',
            timestamp: new Date().toISOString()
          };
        } catch (updateError) {
          console.error('Error updating entity:', updateError);
          const errorMessage = updateError instanceof Error ? updateError.message : 'Unknown error';
          return {
            statusCode: 400,
            headers,
            body: JSON.stringify({ 
              error: `Failed to update entity: ${errorMessage}`
            }),
          };
        }
        break;

      case 'bulk-ingest':
        if (!body.entities || !Array.isArray(body.entities)) {
          return {
            statusCode: 400,
            headers,
            body: JSON.stringify({ 
              error: 'Missing or invalid "entities" array in request body',
              hint: 'Send a JSON object with an "entities" array containing entity definitions'
            }),
          };
        }

        console.log(`Processing bulk ingestion for ${body.entities.length} entities`);
        
        const results = {
          total: body.entities.length,
          successful: 0,
          failed: 0,
          results: [] as any[]
        };

        for (let i = 0; i < body.entities.length; i++) {
          const entityData = body.entities[i];
          
          try {
            console.log(`Processing entity ${i + 1}/${body.entities.length}: ${entityData.name}`);
            
            // Validate required fields
            if (!entityData.type || !entityData.name) {
              throw new Error('Missing required fields: type, name');
            }

            let entityResult: any;

            // Route based on entity type (similar to 'create' operation)
            switch (entityData.type.toLowerCase()) {
              case 'environment':
                const env: Omit<Environment, 'id'> = {
                  name: entityData.name,
                  type: 'Environment',
                  uniqueIdentifier: entityData.uniqueIdentifier,
                  owner: entityData.owner,
                  description: entityData.description
                };
                entityResult = await neptuneClient.createEnvironment(env);
                
                // Store configurations
                const envConfig: Record<string, any> = {};
                if (entityData.region) envConfig.region = entityData.region;
                if (entityData.endpoint) envConfig.endpoint = entityData.endpoint;
                if (entityData.configurations) Object.assign(envConfig, entityData.configurations);
                
                if (Object.keys(envConfig).length > 0) {
                  await neptuneClient.addConfigurationProperties(entityResult.id, envConfig);
                }
                break;

              case 'application':
                const app: Omit<Application, 'id'> = {
                  name: entityData.name,
                  type: 'Application',
                  owner: entityData.owner,
                  description: entityData.description
                };
                entityResult = await neptuneClient.createApplication(app);
                
                // Store configurations
                const appConfig: Record<string, any> = {};
                if (entityData.version) appConfig.version = entityData.version;
                if (entityData.endpoint) appConfig.endpoint = entityData.endpoint;
                if (entityData.configurations) Object.assign(appConfig, entityData.configurations);
                
                if (Object.keys(appConfig).length > 0) {
                  await neptuneClient.addConfigurationProperties(entityResult.id, appConfig);
                }
                break;

              case 'integration':
                const integration: Omit<Integration, 'id'> = {
                  name: entityData.name,
                  type: 'Integration',
                  owner: entityData.owner,
                  description: entityData.description
                };
                entityResult = await neptuneClient.createIntegration(integration);
                
                // Store configurations
                const intConfig: Record<string, any> = {};
                if (entityData.sourceService) intConfig.sourceService = entityData.sourceService;
                if (entityData.targetService) intConfig.targetService = entityData.targetService;
                if (entityData.protocol) intConfig.protocol = entityData.protocol;
                if (entityData.configurations) Object.assign(intConfig, entityData.configurations);
                
                if (Object.keys(intConfig).length > 0) {
                  await neptuneClient.addConfigurationProperties(entityResult.id, intConfig);
                }
                break;

              default:
                throw new Error(`Unsupported entity type: ${entityData.type}`);
            }

            // Create relationships if provided
            if (entityData.relationships && Array.isArray(entityData.relationships)) {
              for (const rel of entityData.relationships) {
                try {
                  await neptuneClient.createRelationship(entityData.name, rel.type, rel.target, entityResult.id);
                } catch (relError) {
                  console.warn(`Failed to create relationship for ${entityData.name}:`, relError);
                }
              }
            }

            // Create incoming relationships (from other entities to this one)
            if (entityData.incomingRelationships && Array.isArray(entityData.incomingRelationships)) {
              for (const incomingRel of entityData.incomingRelationships) {
                try {
                  // Create relationship FROM incomingRel.from TO current entity
                  await neptuneClient.createRelationship(incomingRel.from, incomingRel.type, entityData.name, undefined, entityResult.id);
                  console.log(`Created incoming relationship: ${incomingRel.from} -> ${entityData.name}`);
                } catch (relError) {
                  console.warn(`Failed to create incoming relationship from ${incomingRel.from}:`, relError);
                }
              }
            }

            results.successful++;
            results.results.push({
              index: i,
              name: entityData.name,
              type: entityData.type,
              status: 'success',
              id: entityResult.id
            });

          } catch (entityError) {
            console.error(`Failed to process entity ${i + 1}:`, entityError);
            results.failed++;
            results.results.push({
              index: i,
              name: entityData.name || 'Unknown',
              type: entityData.type || 'Unknown',
              status: 'failed',
              error: entityError instanceof Error ? entityError.message : 'Unknown error'
            });
          }
        }

        result = results;
        console.log(`Bulk ingestion completed: ${results.successful} successful, ${results.failed} failed`);
        break;

      default:
        return {
          statusCode: 400,
          headers,
          body: JSON.stringify({ 
            error: `Unknown operation: ${operation}`,
            availableOperations: [
          'create-environment', 'create-application', 'create-integration', 
          'create-environment-config', 'create-application-config', 'create-integration-config',
          'create-config-entry', 'create-deployment', 'create-relationship', 'initialize-ontology', 'health',
          'create', 'update-entity', 'bulk-ingest'
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
    console.error('Error processing request:', error);
    
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