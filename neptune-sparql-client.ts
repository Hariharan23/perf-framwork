// Neptune SPARQL Client for Environment Management System
// Pure SPARQL operations following the complete ontology (excluding change tracking)

import { NeptunedataClient } from '@aws-sdk/client-neptunedata';
import { defaultProvider } from '@aws-sdk/credential-provider-node';
import { AwsCredentialIdentityProvider } from '@aws-sdk/types';
import { HttpRequest } from '@smithy/protocol-http';
import { Sha256 } from '@aws-crypto/sha256-js';
import { SignatureV4 } from '@smithy/signature-v4';
import { v4 as uuidv4 } from 'uuid';
import axios from 'axios';

// Core Entity interfaces following ontology
export interface Entity {
  id: string;
  name: string;
  type: string;
  uniqueIdentifier?: string;
  owner?: string;
  description?: string;
  createdAt?: string;
  status?: string;
}

export interface Environment extends Entity {
  type: 'Environment';
}

export interface Application extends Entity {
  type: 'Application';
}

export interface Integration extends Entity {
  type: 'Integration';
}

// Configuration interfaces following ontology
export interface Configuration {
  id: string;
  type: string;
  configurationMap?: string; // JSON string
  createdAt?: string;
}

export interface EnvironmentConfiguration extends Configuration {
  type: 'EnvironmentConfiguration';
}

export interface ApplicationConfiguration extends Configuration {
  type: 'ApplicationConfiguration';
}

export interface IntegrationConfiguration extends Configuration {
  type: 'IntegrationConfiguration';
  sourceService?: string;
  targetService?: string;
}

export interface ConfigurationEntry {
  id: string;
  configurationKey: string;
  configurationValue: string;
  configurationType?: string;
}

export interface NetworkNode {
  id: string;
  label: string;
  type: string;
  properties: Record<string, any>;
  configurations?: Record<string, string>;
}

export interface NetworkEdge {
  id: string;
  source: string;
  target: string;
  type: string;
  label?: string;
  relationshipId?: string | null;  // ID for relationship entities (for reversing/deleting)
  properties: Record<string, any>;
}

export interface NetworkData {
  nodes: NetworkNode[];
  edges: NetworkEdge[];
}

export class NeptuneSparqlClient {
  private neptuneEndpoint: string;
  private region: string;
  private credentials: AwsCredentialIdentityProvider;
  private signer: SignatureV4;
  
  // Ontology namespace
  private readonly ontologyPrefix = 'http://neptune.aws.com/envmgmt/ontology/';

  /**
   * Extract UUID from full URI or return as-is if already a UUID
   */
  private extractEntityId(idOrUri: string): string {
    if (idOrUri.startsWith(this.ontologyPrefix)) {
      return idOrUri.replace(this.ontologyPrefix, '');
    }
    return idOrUri;
  }

  /**
   * Escape special characters for SPARQL string literals
   * Follows SPARQL 1.1 specification for string escaping
   */
  private escapeSparql(str: string): string {
    if (!str) return '';
    return str
      .replace(/\\/g, '\\\\')  // Escape backslashes first (must be first!)
      .replace(/"/g, '\\"')      // Escape double quotes
      .replace(/\n/g, '\\n')      // Escape newlines
      .replace(/\r/g, '\\r')      // Escape carriage returns
      .replace(/\t/g, '\\t');     // Escape tabs
  }

  constructor(neptuneEndpoint?: string, region: string = 'us-east-1') {
    this.neptuneEndpoint = neptuneEndpoint || process.env.NEPTUNE_ENDPOINT!;
    this.region = region || process.env.AWS_REGION || 'us-east-1';
    
    // Use the AWS SDK's default credential provider chain
    this.credentials = defaultProvider();
    
    this.signer = new SignatureV4({
      credentials: this.credentials,
      region: this.region,
      service: 'neptune-db',
      sha256: Sha256,
    });

    if (!this.neptuneEndpoint) {
      throw new Error('Neptune endpoint is required. Set NEPTUNE_ENDPOINT environment variable or pass as constructor parameter.');
    }

    // Log configuration for debugging (without sensitive info)
    console.log(`Neptune SPARQL Client initialized:`);
    console.log(`- Endpoint: ${this.neptuneEndpoint}`);
    console.log(`- Region: ${this.region}`);
    console.log(`- Service: neptune-db`);
    console.log(`- IAM Authentication: enabled`);
  }

  /**
   * Execute SPARQL query against Neptune with IAM authentication
   */
  async executeSparqlQuery(query: string): Promise<any> {
    const body = `query=${encodeURIComponent(query)}`;
    
    // Create the request for signing
    const request = new HttpRequest({
      method: 'POST',
      protocol: 'https:',
      hostname: this.neptuneEndpoint,
      port: 8182,
      path: '/sparql',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'application/sparql-results+json',
        'Host': `${this.neptuneEndpoint}:8182`,
      },
      body,
    });

    try {
      // Sign the request with IAM credentials
      const signedRequest = await this.signer.sign(request);
      
      // Construct the full URL
      const url = `https://${this.neptuneEndpoint}:8182/sparql`;
      
      console.log(`Executing SPARQL query to: ${url}`);
      console.log(`Query: ${query.substring(0, 200)}...`);
      
      const response = await axios({
        method: 'POST',
        url,
        headers: {
          ...signedRequest.headers,
          'Content-Length': body.length.toString(),
        },
        data: body,
        timeout: 30000, // 30 second timeout
        validateStatus: function (status) {
          return status >= 200 && status < 500; // Accept 4xx errors for better error handling
        }
      });

      if (response.status >= 400) {
        throw new Error(`Neptune query failed with status ${response.status}: ${JSON.stringify(response.data)}`);
      }

      return response.data;
    } catch (error) {
      console.error('SPARQL query error:', error);
      throw error;
    }
  }

  /**
   * Execute SPARQL update against Neptune with IAM authentication
   */
  async executeSparqlUpdate(update: string): Promise<any> {
    const body = `update=${encodeURIComponent(update)}`;
    
    // Create the request for signing
    const request = new HttpRequest({
      method: 'POST',
      protocol: 'https:',
      hostname: this.neptuneEndpoint,
      port: 8182,
      path: '/sparql',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'application/sparql-results+json',
        'Host': `${this.neptuneEndpoint}:8182`,
      },
      body,
    });

    try {
      // Sign the request with IAM credentials
      const signedRequest = await this.signer.sign(request);
      
      // Construct the full URL
      const url = `https://${this.neptuneEndpoint}:8182/sparql`;
      
      console.log(`Executing SPARQL update to: ${url}`);
      console.log(`Update: ${update.substring(0, 200)}...`);
      
      const response = await axios({
        method: 'POST',
        url,
        headers: {
          ...signedRequest.headers,
          'Content-Length': body.length.toString(),
        },
        data: body,
        timeout: 30000, // 30 second timeout
        validateStatus: function (status) {
          return status >= 200 && status < 500; // Accept 4xx errors for better error handling
        }
      });

      if (response.status >= 400) {
        throw new Error(`Neptune update failed with status ${response.status}: ${JSON.stringify(response.data)}`);
      }

      return response.data;
    } catch (error) {
      console.error('SPARQL update error:', error);
      throw error;
    }
  }

  /**
   * Health check for Neptune cluster
   */
  async healthCheck(): Promise<boolean> {
    try {
      const query = `
        ASK { 
          ?s ?p ?o 
        }
      `;
      
      await this.executeSparqlQuery(query);
      return true;
    } catch (error) {
      console.error('Health check failed:', error);
      return false;
    }
  }

  /**
   * Create Environment using SPARQL following ontology
   */
  async createEnvironment(environment: Omit<Environment, 'id'>): Promise<Environment> {
    const id = uuidv4();
    const timestamp = new Date().toISOString();
    const entity: Environment = { 
      ...environment, 
      id, 
      type: 'Environment',
      createdAt: timestamp
    };
    
    const update = `
      PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
      PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
      PREFIX env: <${this.ontologyPrefix}>
      PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
      
      INSERT DATA {
        env:${id} rdf:type env:Environment ;
                  env:id "${this.escapeSparql(entity.id)}" ;
                  env:name "${this.escapeSparql(entity.name)}" ;
                  env:type "${this.escapeSparql(entity.type)}" ;
                  env:createdAt "${timestamp}"^^xsd:dateTime ;
                  ${entity.uniqueIdentifier ? `env:uniqueIdentifier "${this.escapeSparql(entity.uniqueIdentifier)}" ;` : ''}
                  ${entity.owner ? `env:owner "${this.escapeSparql(entity.owner)}" ;` : ''}
                  ${entity.status ? `env:status "${this.escapeSparql(entity.status)}" ;` : ''}
                  env:description "${this.escapeSparql(entity.description || '')}" .
      }
    `;
    
    await this.executeSparqlUpdate(update);
    return entity;
  }

  /**
   * Create Application using SPARQL following ontology
   */
  async createApplication(application: Omit<Application, 'id'>): Promise<Application> {
    const id = uuidv4();
    const timestamp = new Date().toISOString();
    const entity: Application = { 
      ...application, 
      id, 
      type: 'Application',
      createdAt: timestamp
    };
    
    const update = `
      PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
      PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
      PREFIX env: <${this.ontologyPrefix}>
      PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
      
      INSERT DATA {
        env:${id} rdf:type env:Application ;
                  env:id "${this.escapeSparql(entity.id)}" ;
                  env:name "${this.escapeSparql(entity.name)}" ;
                  env:type "${this.escapeSparql(entity.type)}" ;
                  env:createdAt "${timestamp}"^^xsd:dateTime ;
                  ${entity.uniqueIdentifier ? `env:uniqueIdentifier "${this.escapeSparql(entity.uniqueIdentifier)}" ;` : ''}
                  ${entity.owner ? `env:owner "${this.escapeSparql(entity.owner)}" ;` : ''}
                  ${entity.status ? `env:status "${this.escapeSparql(entity.status)}" ;` : ''}
                  env:description "${this.escapeSparql(entity.description || '')}" .
      }
    `;
    
    await this.executeSparqlUpdate(update);
    return entity;
  }

  /**
   * Create Integration using SPARQL following ontology
   */
  async createIntegration(integration: Omit<Integration, 'id'>): Promise<Integration> {
    const id = uuidv4();
    const timestamp = new Date().toISOString();
    const entity: Integration = { 
      ...integration, 
      id, 
      type: 'Integration',
      createdAt: timestamp
    };
    
    const update = `
      PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
      PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
      PREFIX env: <${this.ontologyPrefix}>
      PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
      
      INSERT DATA {
        env:${id} rdf:type env:Integration ;
                  env:id "${this.escapeSparql(entity.id)}" ;
                  env:name "${this.escapeSparql(entity.name)}" ;
                  env:type "${this.escapeSparql(entity.type)}" ;
                  env:createdAt "${timestamp}"^^xsd:dateTime ;
                  ${entity.uniqueIdentifier ? `env:uniqueIdentifier "${this.escapeSparql(entity.uniqueIdentifier)}" ;` : ''}
                  ${entity.owner ? `env:owner "${this.escapeSparql(entity.owner)}" ;` : ''}
                  ${entity.status ? `env:status "${this.escapeSparql(entity.status)}" ;` : ''}
                  env:description "${this.escapeSparql(entity.description || '')}" .
      }
    `;
    
    await this.executeSparqlUpdate(update);
    return entity;
  }

  /**
   * Create Environment Configuration using SPARQL
   */
  async createEnvironmentConfiguration(config: Omit<EnvironmentConfiguration, 'id'>, environmentId: string): Promise<EnvironmentConfiguration> {
    const id = uuidv4();
    const timestamp = new Date().toISOString();
    const entity: EnvironmentConfiguration = { 
      ...config, 
      id, 
      type: 'EnvironmentConfiguration',
      createdAt: timestamp
    };
    
    const update = `
      PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
      PREFIX env: <${this.ontologyPrefix}>
      PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
      
      INSERT DATA {
        env:${id} rdf:type env:EnvironmentConfiguration ;
                  env:id "${this.escapeSparql(entity.id)}" ;
                  env:type "${this.escapeSparql(entity.type)}" ;
                  env:createdAt "${timestamp}"^^xsd:dateTime ;
                  ${entity.configurationMap ? `env:configurationMap "${this.escapeSparql(entity.configurationMap)}" ;` : ''}
                  rdf:type env:Configuration .
                  
        env:${environmentId} env:hasEnvironmentConfig env:${id} .
      }
    `;
    
    await this.executeSparqlUpdate(update);
    return entity;
  }

  /**
   * Create Application Configuration using SPARQL
   */
  async createApplicationConfiguration(config: Omit<ApplicationConfiguration, 'id'>, applicationId: string): Promise<ApplicationConfiguration> {
    const id = uuidv4();
    const timestamp = new Date().toISOString();
    const entity: ApplicationConfiguration = { 
      ...config, 
      id, 
      type: 'ApplicationConfiguration',
      createdAt: timestamp
    };
    
    const update = `
      PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
      PREFIX env: <${this.ontologyPrefix}>
      PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
      
      INSERT DATA {
        env:${id} rdf:type env:ApplicationConfiguration ;
                  env:id "${this.escapeSparql(entity.id)}" ;
                  env:type "${this.escapeSparql(entity.type)}" ;
                  env:createdAt "${timestamp}"^^xsd:dateTime ;
                  ${entity.configurationMap ? `env:configurationMap "${this.escapeSparql(entity.configurationMap)}" ;` : ''}
                  rdf:type env:Configuration .
                  
        env:${applicationId} env:hasApplicationConfig env:${id} .
      }
    `;
    
    await this.executeSparqlUpdate(update);
    return entity;
  }

  /**
   * Create Integration Configuration using SPARQL
   */
  async createIntegrationConfiguration(config: Omit<IntegrationConfiguration, 'id'>, integrationId: string): Promise<IntegrationConfiguration> {
    const id = uuidv4();
    const timestamp = new Date().toISOString();
    const entity: IntegrationConfiguration = { 
      ...config, 
      id, 
      type: 'IntegrationConfiguration',
      createdAt: timestamp
    };
    
    const update = `
      PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
      PREFIX env: <${this.ontologyPrefix}>
      PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
      
      INSERT DATA {
        env:${id} rdf:type env:IntegrationConfiguration ;
                  env:id "${this.escapeSparql(entity.id)}" ;
                  env:type "${this.escapeSparql(entity.type)}" ;
                  env:createdAt "${timestamp}"^^xsd:dateTime ;
                  ${entity.configurationMap ? `env:configurationMap "${this.escapeSparql(entity.configurationMap)}" ;` : ''}
                  ${entity.sourceService ? `env:sourceService "${this.escapeSparql(entity.sourceService)}" ;` : ''}
                  ${entity.targetService ? `env:targetService "${this.escapeSparql(entity.targetService)}" ;` : ''}
                  rdf:type env:Configuration .
                  
        env:${integrationId} env:hasIntegrationConfig env:${id} .
      }
    `;
    
    await this.executeSparqlUpdate(update);
    return entity;
  }

  /**
   * Create Configuration Entry using SPARQL
   */
  async createConfigurationEntry(entry: Omit<ConfigurationEntry, 'id'>, configurationId: string): Promise<ConfigurationEntry> {
    const id = uuidv4();
    const entity: ConfigurationEntry = { ...entry, id };
    
    const update = `
      PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
      PREFIX env: <${this.ontologyPrefix}>
      
      INSERT DATA {
        env:${id} rdf:type env:ConfigurationEntry ;
                  env:id "${this.escapeSparql(entity.id)}" ;
                  env:configurationKey "${this.escapeSparql(entity.configurationKey)}" ;
                  env:configurationValue "${this.escapeSparql(entity.configurationValue)}" ;
                  ${entity.configurationType ? `env:configurationType "${this.escapeSparql(entity.configurationType)}" ;` : ''}
                  rdf:type env:ConfigurationEntry .
                  
        env:${configurationId} env:hasConfigurationEntry env:${id} .
      }
    `;
    
    await this.executeSparqlUpdate(update);
    return entity;
  }

  /**
   * Get all entities by type following ontology
   */
  async getEntitiesByType(entityType: string): Promise<Entity[]> {
    const query = `
      PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
      PREFIX env: <${this.ontologyPrefix}>
      
      SELECT ?id ?name ?uniqueIdentifier ?owner ?description ?createdAt ?status WHERE {
        ?entity rdf:type env:${entityType} ;
                env:id ?id ;
                env:name ?name ;
                env:type ?type ;
                env:description ?description .
        OPTIONAL { ?entity env:uniqueIdentifier ?uniqueIdentifier }
        OPTIONAL { ?entity env:owner ?owner }
        OPTIONAL { ?entity env:createdAt ?createdAt }
        OPTIONAL { ?entity env:status ?status }
      }
    `;
    
    const result = await this.executeSparqlQuery(query);
    
    return result.results?.bindings?.map((binding: any) => ({
      id: binding.id?.value || '',
      name: binding.name?.value || '',
      type: entityType,
      uniqueIdentifier: binding.uniqueIdentifier?.value,
      owner: binding.owner?.value,
      description: binding.description?.value || '',
      createdAt: binding.createdAt?.value,
      status: binding.status?.value,
    })) || [];
  }

  /**
   * Get network data for visualization following ontology
   */
  async getNetworkData(): Promise<NetworkData> {
    // Get all nodes with ALL their properties (including config_* properties)
    const nodesQuery = `
      PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
      PREFIX env: <${this.ontologyPrefix}>
      
      SELECT ?id ?name ?type ?status ?owner ?property ?value WHERE {
        ?entity env:id ?id ;
                env:name ?name ;
                env:type ?type .
        OPTIONAL { ?entity env:status ?status }
        OPTIONAL { ?entity env:owner ?owner }
        OPTIONAL { 
          ?entity ?property ?value .
          FILTER(STRSTARTS(STR(?property), "${this.ontologyPrefix}"))
          FILTER(?property NOT IN (env:id, env:name, env:type))
        }
        FILTER(?type IN ("Environment", "Application", "Integration"))
      }
    `;

    // Get all edges - both direct predicates and relationship entities
    const edgesQuery = `
      PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
      PREFIX env: <${this.ontologyPrefix}>
      
      SELECT ?sourceId ?targetId ?relationType ?relationLabel ?relationshipId WHERE {
        {
          # Direct predicate relationships (like your CAS_QA integrates PASQA304)
          ?sourceEntity ?predicate ?targetEntity .
          ?sourceEntity env:name ?sourceName .
          ?targetEntity env:name ?targetName .
          ?sourceEntity env:id ?sourceId .
          ?targetEntity env:id ?targetId .
          FILTER(STRSTARTS(STR(?predicate), "${this.ontologyPrefix}"))
          FILTER(?predicate NOT IN (env:id, env:name, env:type, env:description, env:owner, env:status, env:createdAt, env:uniqueIdentifier, env:version, env:endpoint, env:region, env:sourceService, env:targetService))
          BIND(STRAFTER(STR(?predicate), "${this.ontologyPrefix}") AS ?relationType)
          BIND(?relationType AS ?relationLabel)
          BIND("" AS ?relationshipId)
        }
        UNION
        {
          # Relationship entities
          ?relationship env:type "Relationship" .
          ?relationship env:relationshipType ?relationType .
          ?relationship env:id ?relationshipId .
          
          # Get entity IDs directly from relationship (preferred method)
          OPTIONAL { ?relationship env:sourceEntityId ?sourceId . }
          OPTIONAL { ?relationship env:targetEntityId ?targetId . }
          
          # Fallback: if no IDs stored, use names (legacy support)
          OPTIONAL {
            ?relationship env:sourceEntity ?sourceName .
            ?relationship env:targetEntity ?targetName .
            FILTER(!BOUND(?sourceId) || !BOUND(?targetId))
            
            # Find first entity with matching names
            ?sourceEntity env:name ?sourceName .
            ?targetEntity env:name ?targetName .
            BIND(IF(!BOUND(?sourceId), STR(?sourceEntity), ?sourceId) AS ?fallbackSourceId)
            BIND(IF(!BOUND(?targetId), STR(?targetEntity), ?targetId) AS ?fallbackTargetId)
          }
          
          # Use direct IDs if available, otherwise use fallback
          BIND(COALESCE(?sourceId, STRAFTER(STR(?fallbackSourceId), "${this.ontologyPrefix}")) AS ?finalSourceId)
          BIND(COALESCE(?targetId, STRAFTER(STR(?fallbackTargetId), "${this.ontologyPrefix}")) AS ?finalTargetId)
          
          # Only include if we have valid IDs
          FILTER(BOUND(?finalSourceId) && BOUND(?finalTargetId))
          
          BIND(?finalSourceId AS ?sourceId)
          BIND(?finalTargetId AS ?targetId)
          BIND(?relationType AS ?relationLabel)
        }
        UNION
        {
          # Integration source relationships
          ?integration env:type "Integration" .
          ?integration env:id ?integrationId .
          ?integration env:sourceService ?sourceUri .
          
          # Extract entity name from URI or use direct name
          BIND(IF(CONTAINS(STR(?sourceUri), "/"), STRAFTER(STR(?sourceUri), "/"), STR(?sourceUri)) AS ?sourceName)
          
          ?sourceEntity env:name ?sourceName .
          ?sourceEntity env:id ?sourceId .
          
          BIND(?integrationId AS ?targetId)
          BIND("sourceOf" AS ?relationType)
          BIND("source of" AS ?relationLabel)
          BIND("" AS ?relationshipId)
        }
        UNION
        {
          # Integration target relationships  
          ?integration env:type "Integration" .
          ?integration env:id ?integrationId .
          ?integration env:targetService ?targetUri .
          
          # Extract entity name from URI or use direct name
          BIND(IF(CONTAINS(STR(?targetUri), "/"), STRAFTER(STR(?targetUri), "/"), STR(?targetUri)) AS ?targetName)
          
          ?targetEntity env:name ?targetName .
          ?targetEntity env:id ?targetId .
          
          BIND(?integrationId AS ?sourceId)
          BIND("targetOf" AS ?relationType)
          BIND("target of" AS ?relationLabel)
          BIND("" AS ?relationshipId)
        }
      }
    `;

    const [nodesResult, edgesResult] = await Promise.all([
      this.executeSparqlQuery(nodesQuery),
      this.executeSparqlQuery(edgesQuery)
    ]);

    console.log('Nodes query result:', JSON.stringify(nodesResult, null, 2));
    
    // Test query to specifically look for config properties
    const configTestQuery = `
      PREFIX env: <${this.ontologyPrefix}>
      
      SELECT ?entity ?property ?value WHERE {
        ?entity ?property ?value .
        FILTER(CONTAINS(STR(?property), "config_"))
      }
      LIMIT 10
    `;
    
    const configTestResult = await this.executeSparqlQuery(configTestQuery);
    console.log('Config test query result:', JSON.stringify(configTestResult, null, 2));

    // Build nodes with all their properties
    const nodeMap: Record<string, any> = {};
    nodesResult.results?.bindings?.forEach((binding: any) => {
      const entityId = binding.id?.value || '';
      
      if (!nodeMap[entityId]) {
        nodeMap[entityId] = {
          id: entityId,
          label: binding.name?.value || '',
          type: binding.type?.value || '',
          properties: {
            status: binding.status?.value,
            owner: binding.owner?.value,
          },
          configurations: {} // Initialize configurations object
        };
      }
      
      // Add any additional properties, including config_ properties
      const property = binding.property?.value;
      const value = binding.value?.value;
      
      if (property && value) {
        const propName = property.replace(this.ontologyPrefix, '');
        console.log(`Entity ${entityId}: Found property ${property} -> ${propName} = ${value}`);
        
        // If it's a config property, add it both directly AND to configurations object
        if (propName.startsWith('config_')) {
          nodeMap[entityId][propName] = value; // Direct property for backward compatibility
          const configKey = propName.replace('config_', ''); // Remove config_ prefix for display
          nodeMap[entityId].configurations[configKey] = value; // Add to configurations object
          console.log(`  -> Added to configurations as '${configKey}'`);
        } else {
          // Regular property, just add directly
          nodeMap[entityId][propName] = value;
        }
      }
    });
    
    const nodes: NetworkNode[] = Object.values(nodeMap);
    
    const edges: NetworkEdge[] = edgesResult.results?.bindings?.map((binding: any, index: number) => ({
      id: `edge-${index}`,
      source: binding.sourceId?.value || '',
      target: binding.targetId?.value || '',
      type: binding.relationType?.value || 'related',
      label: binding.relationLabel?.value || binding.relationType?.value || 'related',
      relationshipId: binding.relationshipId?.value || null, // Include relationship ID for reversing/deleting
      properties: {}
    })) || [];
    
    return { nodes, edges };
  }

  /**
   * Get entity by ID following ontology
   */
  async getEntityById(id: string): Promise<Entity | null> {
    const query = `
      PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
      PREFIX env: <${this.ontologyPrefix}>
      
      SELECT ?name ?type ?uniqueIdentifier ?owner ?description ?createdAt ?status WHERE {
        ?entity env:id "${this.escapeSparql(id)}" ;
                env:name ?name ;
                env:type ?type ;
                env:description ?description .
        OPTIONAL { ?entity env:uniqueIdentifier ?uniqueIdentifier }
        OPTIONAL { ?entity env:owner ?owner }
        OPTIONAL { ?entity env:createdAt ?createdAt }
        OPTIONAL { ?entity env:status ?status }
      }
    `;
    
    const result = await this.executeSparqlQuery(query);
    
    if (!result.results?.bindings?.length) {
      return null;
    }
    
    const binding = result.results.bindings[0];
    return {
      id,
      name: binding.name?.value || '',
      type: binding.type?.value || '',
      uniqueIdentifier: binding.uniqueIdentifier?.value,
      owner: binding.owner?.value,
      description: binding.description?.value || '',
      createdAt: binding.createdAt?.value,
      status: binding.status?.value,
    };
  }

  /**
   * Get entity by name and type
   */
  async getEntityByName(name: string, type?: string): Promise<Entity | null> {
    const typeFilter = type ? `FILTER(?type = "${this.escapeSparql(type)}")` : '';
    
    const query = `
      PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
      PREFIX env: <${this.ontologyPrefix}>
      
      SELECT ?id ?name ?type ?uniqueIdentifier ?owner ?description ?createdAt ?status WHERE {
        ?entity env:name "${this.escapeSparql(name)}" ;
                env:id ?id ;
                env:name ?name ;
                env:type ?type ;
                env:description ?description .
        OPTIONAL { ?entity env:uniqueIdentifier ?uniqueIdentifier }
        OPTIONAL { ?entity env:owner ?owner }
        OPTIONAL { ?entity env:createdAt ?createdAt }
        OPTIONAL { ?entity env:status ?status }
        ${typeFilter}
      }
      LIMIT 1
    `;
    
    const result = await this.executeSparqlQuery(query);
    
    if (!result.results?.bindings?.length) {
      return null;
    }
    
    const binding = result.results.bindings[0];
    return {
      id: binding.id?.value || '',
      name: binding.name?.value || '',
      type: binding.type?.value || '',
      uniqueIdentifier: binding.uniqueIdentifier?.value,
      owner: binding.owner?.value,
      description: binding.description?.value || '',
      createdAt: binding.createdAt?.value,
      status: binding.status?.value,
    };
  }

  /**
   * Check if entity with name exists (optionally filter by type)
   */
  async checkEntityExists(name: string, type?: string): Promise<boolean> {
    const entity = await this.getEntityByName(name, type);
    return entity !== null;
  }

  /**
   * Get all entities by type and name
   */
  async getEntitiesByTypeAndName(type: string, name: string): Promise<Entity[]> {
    const query = `
      PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
      PREFIX env: <${this.ontologyPrefix}>
      
      SELECT ?id ?name ?type ?uniqueIdentifier ?owner ?description ?createdAt ?status WHERE {
        ?entity env:name "${this.escapeSparql(name)}" ;
                env:type "${this.escapeSparql(type)}" ;
                env:id ?id ;
                env:name ?name ;
                env:type ?type ;
                env:description ?description .
        OPTIONAL { ?entity env:uniqueIdentifier ?uniqueIdentifier }
        OPTIONAL { ?entity env:owner ?owner }
        OPTIONAL { ?entity env:createdAt ?createdAt }
        OPTIONAL { ?entity env:status ?status }
      }
    `;
    
    const result = await this.executeSparqlQuery(query);
    
    return result.results?.bindings?.map((binding: any) => ({
      id: binding.id?.value || '',
      name: binding.name?.value || '',
      type: binding.type?.value || '',
      uniqueIdentifier: binding.uniqueIdentifier?.value,
      owner: binding.owner?.value,
      description: binding.description?.value || '',
      createdAt: binding.createdAt?.value,
      status: binding.status?.value,
    })) || [];
  }

  /**
   * Get all configuration properties for an entity as a flat object
   */
  async getEntityConfigurationsAsObject(entityId: string): Promise<Record<string, string>> {
    const query = `
      PREFIX env: <${this.ontologyPrefix}>
      
      SELECT ?key ?value WHERE {
        <${this.ontologyPrefix}${entityId}> ?property ?value .
        FILTER(STRSTARTS(STR(?property), STR(env:)))
        BIND(REPLACE(STR(?property), STR(env:), "") AS ?key)
        FILTER(?key NOT IN ("id", "name", "type", "description", "owner", "uniqueIdentifier", "createdAt", "status"))
      }
    `;
    
    const result = await this.executeSparqlQuery(query);
    const configs: Record<string, string> = {};
    
    if (result.results?.bindings) {
      for (const binding of result.results.bindings) {
        const key = binding.key?.value;
        const value = binding.value?.value;
        if (key && value) {
          configs[key] = value;
        }
      }
    }
    
    return configs;
  }

  /**
   * Get configurations for an entity
   */
  async getEntityConfigurations(entityId: string): Promise<Configuration[]> {
    const query = `
      PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
      PREFIX env: <${this.ontologyPrefix}>
      
      SELECT ?configId ?configType ?configMap ?createdAt WHERE {
        ?entity env:id "${this.escapeSparql(entityId)}" ;
                env:hasConfiguration ?config .
        ?config env:id ?configId ;
                env:type ?configType .
        OPTIONAL { ?config env:configurationMap ?configMap }
        OPTIONAL { ?config env:createdAt ?createdAt }
      }
    `;
    
    const result = await this.executeSparqlQuery(query);
    
    return result.results?.bindings?.map((binding: any) => ({
      id: binding.configId?.value || '',
      type: binding.configType?.value || '',
      configurationMap: binding.configMap?.value,
      createdAt: binding.createdAt?.value,
    })) || [];
  }

  /**
   * Get configuration entries for a configuration
   */
  async getConfigurationEntries(configurationId: string): Promise<ConfigurationEntry[]> {
    const query = `
      PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
      PREFIX env: <${this.ontologyPrefix}>
      
      SELECT ?entryId ?key ?value ?type WHERE {
        ?config env:id "${this.escapeSparql(configurationId)}" ;
                env:hasConfigurationEntry ?entry .
        ?entry env:id ?entryId ;
               env:configurationKey ?key ;
               env:configurationValue ?value .
        OPTIONAL { ?entry env:configurationType ?type }
      }
    `;
    
    const result = await this.executeSparqlQuery(query);
    
    return result.results?.bindings?.map((binding: any) => ({
      id: binding.entryId?.value || '',
      configurationKey: binding.key?.value || '',
      configurationValue: binding.value?.value || '',
      configurationType: binding.type?.value,
    })) || [];
  }

  /**
   * Delete entity by ID
   */
  async deleteEntity(id: string): Promise<boolean> {
    const update = `
      PREFIX ems: <http://example.org/ems#>
      
      DELETE WHERE {
        ems:${id} ?p ?o .
      }
    `;
    
    try {
      await this.executeSparqlUpdate(update);
      return true;
    } catch (error) {
      console.error(`Failed to delete entity ${id}:`, error);
      return false;
    }
  }

  /**
   * Initialize ontology following the complete schema
   */
  async initializeOntology(): Promise<void> {
    const update = `
      PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
      PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
      PREFIX owl: <http://www.w3.org/2002/07/owl#>
      PREFIX env: <${this.ontologyPrefix}>
      PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
      PREFIX dc: <http://purl.org/dc/elements/1.1/>
      PREFIX dcterms: <http://purl.org/dc/terms/>
      
      INSERT DATA {
        # Ontology metadata
        <${this.ontologyPrefix}> rdf:type owl:Ontology ;
            dc:title "Environment Management System Ontology" ;
            dc:description "RDF ontology for managing environments, applications, integrations and their configurations" ;
            dc:creator "Environment Management Team" ;
            dcterms:created "2025-12-15"^^xsd:date ;
            owl:versionInfo "1.0" .
        
        # Core entity classes
        env:Environment rdf:type owl:Class ;
            rdfs:label "Environment" ;
            rdfs:comment "A deployment or runtime environment" .
        
        env:Application rdf:type owl:Class ;
            rdfs:label "Application" ;
            rdfs:comment "A software application or service" .
        
        env:Integration rdf:type owl:Class ;
            rdfs:label "Integration" ;
            rdfs:comment "An integration between applications or services" .
        
        # Configuration classes
        env:Configuration rdf:type owl:Class ;
            rdfs:label "Configuration" ;
            rdfs:comment "Base class for all configuration types" .
        
        env:EnvironmentConfiguration rdf:type owl:Class ;
            rdfs:subClassOf env:Configuration ;
            rdfs:label "Environment Configuration" ;
            rdfs:comment "Configuration for environment infrastructure and deployment settings" .
        
        env:ApplicationConfiguration rdf:type owl:Class ;
            rdfs:subClassOf env:Configuration ;
            rdfs:label "Application Configuration" ;
            rdfs:comment "Configuration for application runtime and deployment parameters" .
        
        env:IntegrationConfiguration rdf:type owl:Class ;
            rdfs:subClassOf env:Configuration ;
            rdfs:label "Integration Configuration" ;
            rdfs:comment "Configuration for integration connections and protocols" .
        
        env:ConfigurationEntry rdf:type owl:Class ;
            rdfs:label "Configuration Entry" ;
            rdfs:comment "Individual key-value configuration parameter" .
        
        # Core properties
        env:id rdf:type owl:DatatypeProperty ;
            rdfs:label "identifier" ;
            rdfs:comment "Unique identifier for entity" ;
            rdfs:range xsd:string .
        
        env:name rdf:type owl:DatatypeProperty ;
            rdfs:label "name" ;
            rdfs:comment "Human-readable name" ;
            rdfs:range xsd:string .
        
        env:uniqueIdentifier rdf:type owl:DatatypeProperty ;
            rdfs:label "unique identifier" ;
            rdfs:comment "Business unique identifier" ;
            rdfs:range xsd:string .
        
        env:owner rdf:type owl:DatatypeProperty ;
            rdfs:label "owner" ;
            rdfs:comment "Owner or responsible party" ;
            rdfs:range xsd:string .
        
        env:description rdf:type owl:DatatypeProperty ;
            rdfs:label "description" ;
            rdfs:comment "Detailed description" ;
            rdfs:range xsd:string .
        
        env:type rdf:type owl:DatatypeProperty ;
            rdfs:label "type" ;
            rdfs:comment "Entity type classification" ;
            rdfs:range xsd:string .
        
        env:createdAt rdf:type owl:DatatypeProperty ;
            rdfs:label "created at" ;
            rdfs:comment "Creation timestamp" ;
            rdfs:range xsd:dateTime .
        
        env:endpoint rdf:type owl:DatatypeProperty ;
            rdfs:label "endpoint" ;
            rdfs:comment "Service endpoint URL" ;
            rdfs:range xsd:string .
        
        env:region rdf:type owl:DatatypeProperty ;
            rdfs:label "region" ;
            rdfs:comment "Geographic or cloud region" ;
            rdfs:range xsd:string .
        
        env:status rdf:type owl:DatatypeProperty ;
            rdfs:label "status" ;
            rdfs:comment "Current operational status" ;
            rdfs:range xsd:string .
        
        env:version rdf:type owl:DatatypeProperty ;
            rdfs:label "version" ;
            rdfs:comment "Version information" ;
            rdfs:range xsd:string .
        
        # Configuration properties
        env:configurationMap rdf:type owl:DatatypeProperty ;
            rdfs:label "configuration map" ;
            rdfs:comment "Complete configuration as JSON object" ;
            rdfs:domain env:Configuration ;
            rdfs:range xsd:string .
        
        env:configurationKey rdf:type owl:DatatypeProperty ;
            rdfs:label "configuration key" ;
            rdfs:comment "Configuration property name" ;
            rdfs:domain env:ConfigurationEntry ;
            rdfs:range xsd:string .
        
        env:configurationValue rdf:type owl:DatatypeProperty ;
            rdfs:label "configuration value" ;
            rdfs:comment "Configuration property value" ;
            rdfs:domain env:ConfigurationEntry ;
            rdfs:range xsd:string .
        
        env:configurationType rdf:type owl:DatatypeProperty ;
            rdfs:label "configuration type" ;
            rdfs:comment "Data type of configuration value" ;
            rdfs:domain env:ConfigurationEntry ;
            rdfs:range xsd:string .
        
        # Integration specific properties
        env:sourceService rdf:type owl:DatatypeProperty ;
            rdfs:label "source service" ;
            rdfs:comment "Source service identifier in integration" ;
            rdfs:range xsd:string .
        
        env:targetService rdf:type owl:DatatypeProperty ;
            rdfs:label "target service" ;
            rdfs:comment "Target service identifier in integration" ;
            rdfs:range xsd:string .
        
        # Object properties (relationships)
        env:hasConfiguration rdf:type owl:ObjectProperty ;
            rdfs:label "has configuration" ;
            rdfs:comment "Links an entity to its configuration" ;
            rdfs:domain owl:Thing ;
            rdfs:range env:Configuration .
        
        env:hasEnvironmentConfig rdf:type owl:ObjectProperty ;
            rdfs:subPropertyOf env:hasConfiguration ;
            rdfs:label "has environment configuration" ;
            rdfs:domain env:Environment ;
            rdfs:range env:EnvironmentConfiguration .
        
        env:hasApplicationConfig rdf:type owl:ObjectProperty ;
            rdfs:subPropertyOf env:hasConfiguration ;
            rdfs:label "has application configuration" ;
            rdfs:domain env:Application ;
            rdfs:range env:ApplicationConfiguration .
        
        env:hasIntegrationConfig rdf:type owl:ObjectProperty ;
            rdfs:subPropertyOf env:hasConfiguration ;
            rdfs:label "has integration configuration" ;
            rdfs:domain env:Integration ;
            rdfs:range env:IntegrationConfiguration .
        
        env:hasConfigurationEntry rdf:type owl:ObjectProperty ;
            rdfs:label "has configuration entry" ;
            rdfs:domain env:Configuration ;
            rdfs:range env:ConfigurationEntry .
        
        env:integratesWith rdf:type owl:ObjectProperty ;
            rdfs:label "integrates with" ;
            rdfs:comment "Direct integration relationship between services" .
        
        env:deployedIn rdf:type owl:ObjectProperty ;
            rdfs:label "deployed in" ;
            rdfs:comment "Application deployed in environment" ;
            rdfs:domain env:Application ;
            rdfs:range env:Environment .
      }
    `;
    
    await this.executeSparqlUpdate(update);
  }

  /**
   * Create deployment relationship
   */
  async createDeployment(applicationId: string, environmentId: string): Promise<void> {
    const update = `
      PREFIX env: <${this.ontologyPrefix}>
      
      INSERT DATA {
        env:${applicationId} env:deployedIn env:${environmentId} .
      }
    `;
    
    await this.executeSparqlUpdate(update);
  }

  /**
   * Get deployments for an environment
   */
  async getEnvironmentDeployments(environmentId: string): Promise<Entity[]> {
    const query = `
      PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
      PREFIX env: <${this.ontologyPrefix}>
      
      SELECT ?appId ?appName ?appType ?version ?status WHERE {
        ?app env:deployedIn ?env .
        ?env env:id "${this.escapeSparql(environmentId)}" .
        ?app env:id ?appId ;
             env:name ?appName ;
             env:type ?appType .
        OPTIONAL { ?app env:version ?version }
        OPTIONAL { ?app env:status ?status }
      }
    `;
    
    const result = await this.executeSparqlQuery(query);
    
    return result.results?.bindings?.map((binding: any) => ({
      id: binding.appId?.value || '',
      name: binding.appName?.value || '',
      type: binding.appType?.value || '',
      version: binding.version?.value,
      status: binding.status?.value,
    })) || [];
  }

  /**
   * Create relationships between entities
   */
  async createRelationship(sourceEntityName: string, relationshipType: string, targetEntityName: string, sourceEntityId?: string, targetEntityId?: string): Promise<void> {
    // If IDs not provided, look them up by name (get the first match)
    if (!sourceEntityId) {
      const sourceEntity = await this.getEntityByName(sourceEntityName);
      if (!sourceEntity) {
        throw new Error(`Source entity "${sourceEntityName}" not found`);
      }
      sourceEntityId = sourceEntity.id;
    }
    
    if (!targetEntityId) {
      const targetEntity = await this.getEntityByName(targetEntityName);
      if (!targetEntity) {
        throw new Error(`Target entity "${targetEntityName}" not found`);
      }
      targetEntityId = targetEntity.id;
    }
    
    // Generate a unique ID for this relationship entity
    const relationshipId = `rel_${uuidv4()}`;
    const relationshipUri = `${this.ontologyPrefix}entity/${relationshipId}`;
    
    // Create relationship as an entity with source, target, IDs and type properties
    const insertQuery = `
      PREFIX env: <${this.ontologyPrefix}>
      
      INSERT DATA {
        <${relationshipUri}> env:id "${this.escapeSparql(relationshipId)}" .
        <${relationshipUri}> env:name "${this.escapeSparql(sourceEntityName)}_${this.escapeSparql(relationshipType)}_${this.escapeSparql(targetEntityName)}" .
        <${relationshipUri}> env:type "Relationship" .
        <${relationshipUri}> env:relationshipType "${this.escapeSparql(relationshipType)}" .
        <${relationshipUri}> env:sourceEntity "${this.escapeSparql(sourceEntityName)}" .
        <${relationshipUri}> env:targetEntity "${this.escapeSparql(targetEntityName)}" .
        <${relationshipUri}> env:sourceEntityId "${this.escapeSparql(sourceEntityId)}" .
        <${relationshipUri}> env:targetEntityId "${this.escapeSparql(targetEntityId)}" .
        <${relationshipUri}> env:description "Relationship: ${this.escapeSparql(sourceEntityName)} ${this.escapeSparql(relationshipType)} ${this.escapeSparql(targetEntityName)}" .
        <${relationshipUri}> env:createdAt "${new Date().toISOString()}" .
      }
    `;
    
    await this.executeSparqlUpdate(insertQuery);
  }

  /**
   * Update relationships when an entity name changes
   * Preserves all relationships by updating references from old name to new name
   */
  async migrateEntityRelationships(oldName: string, newName: string): Promise<{ updated: number }> {
    console.log(`Migrating relationships from "${oldName}" to "${newName}"`);
    
    // Update relationships where entity is the source
    const updateSourceQuery = `
      PREFIX env: <${this.ontologyPrefix}>
      
      DELETE {
        ?rel env:sourceEntity "${this.escapeSparql(oldName)}" .
        ?rel env:name ?oldRelName .
      }
      INSERT {
        ?rel env:sourceEntity "${this.escapeSparql(newName)}" .
        ?rel env:name ?newRelName .
      }
      WHERE {
        ?rel env:type "Relationship" ;
             env:sourceEntity "${this.escapeSparql(oldName)}" ;
             env:targetEntity ?target ;
             env:relationshipType ?relType ;
             env:name ?oldRelName .
        BIND(CONCAT("${this.escapeSparql(newName)}", "_", ?relType, "_", ?target) AS ?newRelName)
      }
    `;
    
    await this.executeSparqlUpdate(updateSourceQuery);
    
    // Update relationships where entity is the target
    const updateTargetQuery = `
      PREFIX env: <${this.ontologyPrefix}>
      
      DELETE {
        ?rel env:targetEntity "${this.escapeSparql(oldName)}" .
        ?rel env:name ?oldRelName .
      }
      INSERT {
        ?rel env:targetEntity "${this.escapeSparql(newName)}" .
        ?rel env:name ?newRelName .
      }
      WHERE {
        ?rel env:type "Relationship" ;
             env:targetEntity "${this.escapeSparql(oldName)}" ;
             env:sourceEntity ?source ;
             env:relationshipType ?relType ;
             env:name ?oldRelName .
        BIND(CONCAT(?source, "_", ?relType, "_", "${this.escapeSparql(newName)}") AS ?newRelName)
      }
    `;
    
    await this.executeSparqlUpdate(updateTargetQuery);
    
    // Count updated relationships
    const countQuery = `
      PREFIX env: <${this.ontologyPrefix}>
      
      SELECT (COUNT(?rel) AS ?count) WHERE {
        ?rel env:type "Relationship" .
        {
          ?rel env:sourceEntity "${this.escapeSparql(newName)}" .
        } UNION {
          ?rel env:targetEntity "${this.escapeSparql(newName)}" .
        }
      }
    `;
    
    const countResult = await this.executeSparqlQuery(countQuery);
    const updated = parseInt(countResult.results?.bindings?.[0]?.count?.value || '0', 10);
    
    console.log(`Migrated ${updated} relationships for entity "${newName}"`);
    return { updated };
  }

  /**
   * Reverse the direction of a relationship (swap source and target)
   */
  async reverseRelationship(relationshipId: string): Promise<{ success: boolean; message: string }> {
    console.log(`Reversing relationship ${relationshipId}`);
    
    // Get the current relationship details
    const getRelQuery = `
      PREFIX env: <${this.ontologyPrefix}>
      
      SELECT ?source ?target ?sourceId ?targetId ?relType WHERE {
        ?rel env:id "${this.escapeSparql(relationshipId)}" ;
             env:sourceEntity ?source ;
             env:targetEntity ?target ;
             env:relationshipType ?relType .
        OPTIONAL { ?rel env:sourceEntityId ?sourceId . }
        OPTIONAL { ?rel env:targetEntityId ?targetId . }
      }
    `;
    
    const result = await this.executeSparqlQuery(getRelQuery);
    
    if (!result.results?.bindings?.length) {
      return { success: false, message: `Relationship ${relationshipId} not found` };
    }
    
    const binding = result.results.bindings[0];
    const oldSource = binding.source?.value;
    const oldTarget = binding.target?.value;
    const oldSourceId = binding.sourceId?.value;
    const oldTargetId = binding.targetId?.value;
    const relType = binding.relType?.value;
    
    console.log(`Swapping: ${oldSource} (${oldSourceId}) -> ${oldTarget} (${oldTargetId}) to ${oldTarget} (${oldTargetId}) -> ${oldSource} (${oldSourceId})`);
    
    // Update the relationship by swapping source and target (both names and IDs)
    const updateQuery = `
      PREFIX env: <${this.ontologyPrefix}>
      
      DELETE {
        ?rel env:sourceEntity "${this.escapeSparql(oldSource)}" .
        ?rel env:targetEntity "${this.escapeSparql(oldTarget)}" .
        ${oldSourceId ? `?rel env:sourceEntityId "${this.escapeSparql(oldSourceId)}" .` : ''}
        ${oldTargetId ? `?rel env:targetEntityId "${this.escapeSparql(oldTargetId)}" .` : ''}
        ?rel env:name ?oldName .
        ?rel env:description ?oldDesc .
      }
      INSERT {
        ?rel env:sourceEntity "${this.escapeSparql(oldTarget)}" .
        ?rel env:targetEntity "${this.escapeSparql(oldSource)}" .
        ${oldTargetId ? `?rel env:sourceEntityId "${this.escapeSparql(oldTargetId)}" .` : ''}
        ${oldSourceId ? `?rel env:targetEntityId "${this.escapeSparql(oldSourceId)}" .` : ''}
        ?rel env:name "${this.escapeSparql(oldTarget)}_${this.escapeSparql(relType)}_${this.escapeSparql(oldSource)}" .
        ?rel env:description "Relationship: ${this.escapeSparql(oldTarget)} ${this.escapeSparql(relType)} ${this.escapeSparql(oldSource)}" .
      }
      WHERE {
        ?rel env:id "${this.escapeSparql(relationshipId)}" ;
             env:name ?oldName ;
             env:description ?oldDesc .
      }
    `;
    
    await this.executeSparqlUpdate(updateQuery);
    
    return { 
      success: true, 
      message: `Relationship reversed: ${oldTarget} -> ${oldSource}` 
    };
  }

  /**
   * Delete a relationship by ID
   */
  async deleteRelationship(relationshipId: string): Promise<{ success: boolean; message: string }> {
    console.log(`Deleting relationship ${relationshipId}`);
    
    const deleteQuery = `
      PREFIX env: <${this.ontologyPrefix}>
      
      DELETE {
        ?rel ?p ?o .
      }
      WHERE {
        ?rel env:id "${this.escapeSparql(relationshipId)}" ;
             ?p ?o .
      }
    `;
    
    await this.executeSparqlUpdate(deleteQuery);
    
    return { 
      success: true, 
      message: `Relationship ${relationshipId} deleted` 
    };
  }

  /**
   * Log a history event for an entity
   */
  async logHistory(entityId: string, entityName: string, action: string, changes?: Record<string, any>, user?: string): Promise<void> {
    const historyId = `history_${uuidv4()}`;
    const historyUri = `${this.ontologyPrefix}entity/${historyId}`;
    const timestamp = new Date().toISOString();
    
    // Build the insert query for the history event
    let insertData = `
      <${historyUri}> env:id "${this.escapeSparql(historyId)}" .
      <${historyUri}> env:type "History" .
      <${historyUri}> env:entityId "${this.escapeSparql(entityId)}" .
      <${historyUri}> env:entityName "${this.escapeSparql(entityName)}" .
      <${historyUri}> env:action "${this.escapeSparql(action)}" .
      <${historyUri}> env:timestamp "${timestamp}" .
    `;
    
    // Add user if provided
    if (user) {
      insertData += `<${historyUri}> env:user "${this.escapeSparql(user)}" .\n`;
    }
    
    // Add changes as JSON string if provided
    if (changes && Object.keys(changes).length > 0) {
      const changesJson = this.escapeSparql(JSON.stringify(changes));
      insertData += `<${historyUri}> env:changes "${changesJson}" .\n`;
    }
    
    const insertQuery = `
      PREFIX env: <${this.ontologyPrefix}>
      
      INSERT DATA {
        ${insertData}
      }
    `;
    
    await this.executeSparqlUpdate(insertQuery);
  }

  /**
   * Get history for an entity
   */
  async getEntityHistory(entityId: string): Promise<Array<{
    id: string;
    action: string;
    timestamp: string;
    user?: string;
    changes?: any;
  }>> {
    const query = `
      PREFIX env: <${this.ontologyPrefix}>
      
      SELECT ?id ?action ?timestamp ?user ?changes WHERE {
        ?history env:type "History" ;
                 env:id ?id ;
                 env:entityId "${entityId}" ;
                 env:action ?action ;
                 env:timestamp ?timestamp .
        OPTIONAL { ?history env:user ?user }
        OPTIONAL { ?history env:changes ?changes }
      }
      ORDER BY DESC(?timestamp)
    `;
    
    const result = await this.executeSparqlQuery(query);
    
    return result.results?.bindings?.map((binding: any) => {
      const historyEntry: any = {
        id: binding.id?.value || '',
        action: binding.action?.value || '',
        timestamp: binding.timestamp?.value || '',
      };
      
      if (binding.user?.value) {
        historyEntry.user = binding.user.value;
      }
      
      if (binding.changes?.value) {
        try {
          historyEntry.changes = JSON.parse(binding.changes.value);
        } catch (e) {
          historyEntry.changes = binding.changes.value;
        }
      }
      
      return historyEntry;
    }) || [];
  }

  /**
   * Add configuration properties directly to an entity
   */
  async addConfigurationProperties(entityId: string, configurations: Record<string, any>): Promise<void> {
    if (!configurations || Object.keys(configurations).length === 0) {
      return;
    }

    // Find the entity URI by ID
    const entityQuery = `
      PREFIX env: <${this.ontologyPrefix}>
      
      SELECT ?entity WHERE {
        ?entity env:id "${this.escapeSparql(entityId)}"
      }
    `;

    const entityResult = await this.executeSparqlQuery(entityQuery);
    if (!entityResult.results?.bindings?.length) {
      throw new Error(`Entity with ID ${entityId} not found`);
    }

    const entityUri = entityResult.results.bindings[0].entity.value;
    
    // Build INSERT DATA query to add config properties
    let insertTriples = '';
    for (const [key, value] of Object.entries(configurations)) {
      if (value !== null && value !== undefined && value !== '') {
        const configProperty = `env:config_${key}`;
        // Properly escape all special characters in the value for SPARQL
        const escapedValue = this.escapeSparql(String(value));
        insertTriples += `    <${entityUri}> ${configProperty} "${escapedValue}" .\n`;
      }
    }

    if (insertTriples) {
      const updateQuery = `
        PREFIX env: <${this.ontologyPrefix}>
        
        INSERT DATA {
${insertTriples}        }
      `;

      await this.executeSparqlUpdate(updateQuery);
      console.log(`Added ${Object.keys(configurations).length} configuration properties to entity ${entityId}`);
    }
  }
}