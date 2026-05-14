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

// Entity Stats interface (ScienceLogic monitoring/health + meta data)
export interface EntityStats {
  // --- ScienceLogic Meta Properties ---
  deviceName?: string;
  deviceDescription?: string;
  ipAddress?: string;
  deviceClass?: string;
  organization?: string;
  collectionMode?: string;
  deviceHostname?: string;
  managedType?: string;
  category?: string;
  subClass?: string;
  uptime?: string;
  collectionTime?: string;
  collectorGroup?: string;
  sourceId?: string;
  sourceSystem?: string;
  lastSyncedAt?: string;
  // --- ScienceLogic Stats Properties ---
  availability?: string;
  latency?: string;
  cpu?: string;
  memory?: string;
  swap?: string;
  currentState?: string;
  // --- Derived/Internal Properties ---
  storage?: string;
  healthScore?: string;
}

// List of stat/meta property names for iteration (must match EntityStats keys)
export const STAT_PROPERTIES: (keyof EntityStats)[] = [
  // Meta
  'deviceName', 'deviceDescription',
  'ipAddress', 'deviceClass', 'organization', 'collectionMode',
  'deviceHostname', 'managedType', 'category', 'subClass',
  'uptime', 'collectionTime', 'collectorGroup',
  'sourceId', 'sourceSystem', 'lastSyncedAt',
  // Stats
  'availability', 'latency', 'cpu', 'memory', 'swap', 'currentState',
  // Derived/Internal
  'storage', 'healthScore',
];

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
  stats?: EntityStats;
}

export interface OrphanEnvironment extends Entity {
  rawHostname?: string;
  createdBySource?: string;
  createdByRun?: string;
  propertyKey?: string;
  fullUrl?: string;
  protocol?: string;
  port?: string;
  path?: string;
  endpointHint?: string;
  sourceFile?: string;
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
  stats?: EntityStats;
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

// --- Progressive exploration interfaces ---

/** Minimal environment skeleton returned by getNetworkOverview */
export interface EnvNode {
  id: string;
  name: string;
  status?: string;
  sourceService?: string;   // set when created/discovered by a pipeline
  tag?: string;             // user-assigned label tag (e.g. "Production", "DR")
  tagColor?: string;        // hex color for this node in the graph (e.g. "#e11d48")
}

/** Aggregated env-to-env edge with integration count weight */
export interface WeightedEdge {
  id: string;       // stable: sorted(envA, envB).join('::')
  source: string;
  target: string;
  weight: number;
}

export interface NetworkOverview {
  nodes: EnvNode[];
  edges: WeightedEdge[];
}

/** Full node with property bag, used as the focus in getEnvironmentDetail */
export interface FullNode extends NetworkNode {
  // inherits id, label, type, properties, configurations, stats
}

export interface EnvironmentDetail {
  focus: FullNode;
  neighbors: NetworkNode[];
  edges: NetworkEdge[];
}

export interface NeighborResult {
  neighbors: NetworkNode[];
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
   * Lightweight: get just environment names and IDs for dropdown population
   */
  async getEnvironmentsList(): Promise<{ id: string; name: string }[]> {
    const query = `
      PREFIX env: <${this.ontologyPrefix}>
      SELECT ?id ?name WHERE {
        ?entity env:id ?id ;
                env:name ?name ;
                env:type "Environment" .
      }
      ORDER BY ?name
    `;
    const result = await this.executeSparqlQuery(query);
    return (result.results?.bindings || []).map((b: any) => ({
      id: b.id?.value || '',
      name: b.name?.value || '',
    }));
  }

  /**
   * Get network subgraph for specific environment(s) — only nodes/edges reachable
   * from the given env IDs through Relationship entities (N-hop).
   * If envIds is empty/null, returns the full graph.
   */
  async getNetworkDataForEnvs(envIds?: string[], maxHops: number = 3): Promise<NetworkData> {
    if (!envIds || envIds.length === 0) {
      return this.getNetworkData();
    }

    // BFS expansion: iteratively discover reachable entity IDs hop-by-hop
    const allReachable = new Set<string>(envIds);
    let frontier = new Set<string>(envIds);

    for (let hop = 0; hop < maxHops && frontier.size > 0; hop++) {
      const frontierValues = Array.from(frontier).map(id => `"${this.escapeSparql(id)}"`).join(' ');

      const hopQuery = `
        PREFIX env: <${this.ontologyPrefix}>
        SELECT DISTINCT ?entityId WHERE {
          VALUES ?startId { ${frontierValues} }
          {
            ?rel env:type "Relationship" ;
                 env:sourceEntityId ?startId ;
                 env:targetEntityId ?entityId .
          }
          UNION
          {
            ?rel env:type "Relationship" ;
                 env:targetEntityId ?startId ;
                 env:sourceEntityId ?entityId .
          }
        }
      `;

      const hopResult = await this.executeSparqlQuery(hopQuery);
      const newIds = new Set<string>();
      (hopResult.results?.bindings || []).forEach((b: any) => {
        const id = b.entityId?.value;
        if (id && !allReachable.has(id)) {
          newIds.add(id);
          allReachable.add(id);
        }
      });
      frontier = newIds;
      if (newIds.size === 0) break; // No new entities found, stop early
    }

    if (allReachable.size === 0) {
      return { nodes: [], edges: [] };
    }

    // Fetch node details only for reachable entities
    const idValues = Array.from(allReachable).map(id => `"${this.escapeSparql(id)}"`).join(' ');

    const nodesQuery = `
      PREFIX env: <${this.ontologyPrefix}>
      SELECT ?id ?name ?type ?status ?owner ?property ?value WHERE {
        VALUES ?id { ${idValues} }
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

    // Step 3: Fetch edges only for reachable entities
    const edgesQuery = `
      PREFIX env: <${this.ontologyPrefix}>
      SELECT ?sourceId ?targetId ?relationType ?relationLabel ?relationshipId WHERE {
        VALUES ?sourceId { ${idValues} }
        VALUES ?targetId { ${idValues} }
        ?relationship env:type "Relationship" ;
                      env:relationshipType ?relationType ;
                      env:id ?relationshipId ;
                      env:sourceEntityId ?sourceId ;
                      env:targetEntityId ?targetId .
        BIND(?relationType AS ?relationLabel)
      }
    `;

    const [nodesResult, edgesResult] = await Promise.all([
      this.executeSparqlQuery(nodesQuery),
      this.executeSparqlQuery(edgesQuery),
    ]);

    const nodes = this.buildNodesFromBindings(nodesResult.results?.bindings || []);
    const allEdges = this.buildEdgesFromBindings(edgesResult.results?.bindings || []);
    const nodeIds = new Set(nodes.map(n => n.id));
    const edges = allEdges.filter(e => nodeIds.has(e.source) && nodeIds.has(e.target));

    return { nodes, edges };
  }

  /**
   * Get network data for visualization following ontology
   * Optimized: removed expensive full-graph scan, uses only Relationship entities
   */
  async getNetworkData(): Promise<NetworkData> {
    // Get all nodes — lean query, properties fetched via OPTIONAL on same entity
    const nodesQuery = `
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

    // Get all edges — only from Relationship entities with stored IDs (no full-graph scan)
    const edgesQuery = `
      PREFIX env: <${this.ontologyPrefix}>
      SELECT ?sourceId ?targetId ?relationType ?relationLabel ?relationshipId WHERE {
        ?relationship env:type "Relationship" ;
                      env:relationshipType ?relationType ;
                      env:id ?relationshipId ;
                      env:sourceEntityId ?sourceId ;
                      env:targetEntityId ?targetId .
        BIND(?relationType AS ?relationLabel)
      }
    `;

    const [nodesResult, edgesResult] = await Promise.all([
      this.executeSparqlQuery(nodesQuery),
      this.executeSparqlQuery(edgesQuery),
    ]);

    const nodes = this.buildNodesFromBindings(nodesResult.results?.bindings || []);
    const allEdges = this.buildEdgesFromBindings(edgesResult.results?.bindings || []);
    const nodeIds = new Set(nodes.map(n => n.id));
    const edges = allEdges.filter(e => nodeIds.has(e.source) && nodeIds.has(e.target));

    return { nodes, edges };
  }

  /**
   * Build NetworkNode array from SPARQL bindings
   */
  private buildNodesFromBindings(bindings: any[]): NetworkNode[] {
    const nodeMap: Record<string, any> = {};
    bindings.forEach((binding: any) => {
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
          configurations: {},
        };
      }
      const property = binding.property?.value;
      const value = binding.value?.value;
      if (property && value) {
        const propName = property.replace(this.ontologyPrefix, '');
        if (STAT_PROPERTIES.includes(propName as any)) {
          if (!nodeMap[entityId].stats) {
            nodeMap[entityId].stats = {};
          }
          nodeMap[entityId].stats[propName] = value;
        } else if (propName.startsWith('config_')) {
          nodeMap[entityId][propName] = value;
          const configKey = propName.replace('config_', '');
          nodeMap[entityId].configurations[configKey] = value;
        } else {
          nodeMap[entityId][propName] = value;
        }
      }
    });
    return Object.values(nodeMap);
  }

  /**
   * Build NetworkEdge array from SPARQL bindings
   */
  private buildEdgesFromBindings(bindings: any[]): NetworkEdge[] {
    return bindings.map((binding: any, index: number) => ({
      id: `edge-${index}`,
      source: binding.sourceId?.value || '',
      target: binding.targetId?.value || '',
      type: binding.relationType?.value || 'related',
      label: binding.relationLabel?.value || binding.relationType?.value || 'related',
      relationshipId: binding.relationshipId?.value || null,
      properties: {},
    }));
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
                env:type ?type .
        OPTIONAL { ?entity env:description ?description }
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
                env:type ?type .
        OPTIONAL { ?entity env:description ?description }
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
   * Find Environment nodes that have no Relationship referencing them
   * (neither as source nor target). These are disconnected/orphan nodes.
   */
  async listOrphanEnvironments(): Promise<OrphanEnvironment[]> {
    // Look up Integration entities that target each orphan by name — they store
    // the source environment and the original property details we need for "Connect".
    const query = `
      PREFIX env: <${this.ontologyPrefix}>
      SELECT ?id ?name ?description ?discoveredBy ?discoveredAt ?rawHostname
             (SAMPLE(?linkedSrc) AS ?sourceEnv)
             (SAMPLE(?linkedPropKey) AS ?propKey)
             (SAMPLE(?linkedFullUrl) AS ?fullUrl)
             (SAMPLE(?linkedProtocol) AS ?protocol)
             (SAMPLE(?linkedPort) AS ?port)
             (SAMPLE(?linkedPath) AS ?pathVal)
             (SAMPLE(?linkedHint) AS ?hint)
             (SAMPLE(?linkedSrcFile) AS ?srcFile)
             (SAMPLE(?linkedDiscoveredAt) AS ?intDiscoveredAt)
      WHERE {
        ?entity env:type "Environment" ;
                env:id ?id ;
                env:name ?name .
        OPTIONAL { ?entity env:description ?description }
        OPTIONAL { ?entity env:config_discoveredBy ?discoveredBy }
        OPTIONAL { ?entity env:config_discoveredAt ?discoveredAt }
        OPTIONAL { ?entity env:config_hostname ?rawHostname }
        OPTIONAL {
          ?intEntity env:type "Integration" ;
                     env:config_targetEnvironment ?name ;
                     env:config_sourceEnvironment ?linkedSrc .
          OPTIONAL { ?intEntity env:config_propertyKey ?linkedPropKey }
          OPTIONAL { ?intEntity env:config_fullUrl ?linkedFullUrl }
          OPTIONAL { ?intEntity env:config_protocol ?linkedProtocol }
          OPTIONAL { ?intEntity env:config_port ?linkedPort }
          OPTIONAL { ?intEntity env:config_path ?linkedPath }
          OPTIONAL { ?intEntity env:config_endpointHint ?linkedHint }
          OPTIONAL { ?intEntity env:config_sourceFile ?linkedSrcFile }
          OPTIONAL { ?intEntity env:config_discoveredAt ?linkedDiscoveredAt }
        }
        FILTER NOT EXISTS {
          ?rel env:type "Relationship" ;
               env:sourceEntityId ?id .
        }
        FILTER NOT EXISTS {
          ?rel env:type "Relationship" ;
               env:targetEntityId ?id .
        }
      }
      GROUP BY ?id ?name ?description ?discoveredBy ?discoveredAt ?rawHostname
    `;
    const result = await this.executeSparqlQuery(query);
    return (result.results?.bindings || []).map((b: any) => ({
      id: b.id?.value || '',
      name: b.name?.value || '',
      type: 'Environment',
      description: b.description?.value || '',
      createdAt: b.discoveredAt?.value || b.intDiscoveredAt?.value || '',
      createdBySource: b.discoveredBy?.value || b.sourceEnv?.value || '',
      rawHostname: b.rawHostname?.value || '',
      propertyKey: b.propKey?.value || '',
      fullUrl: b.fullUrl?.value || '',
      protocol: b.protocol?.value || '',
      port: b.port?.value || '',
      path: b.pathVal?.value || '',
      endpointHint: b.hint?.value || '',
      sourceFile: b.srcFile?.value || '',
    }));
  }

  /**
   * Delete a list of orphan Environment nodes by id.
   * Also removes any config/stat triples attached to each node.
   * Returns the count of successfully deleted nodes.
   */
  async deleteOrphanEnvironments(ids: string[]): Promise<{ deleted: number; errors: string[] }> {
    let deleted = 0;
    const errors: string[] = [];
    for (const id of ids) {
      try {
        const update = `
          PREFIX env: <${this.ontologyPrefix}>
          DELETE WHERE {
            ?entity env:id "${this.escapeSparql(id)}" ;
                    ?p ?o .
          }
        `;
        await this.executeSparqlUpdate(update);
        deleted++;
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        errors.push(`${id}: ${msg}`);
        console.error(`Failed to delete orphan environment ${id}:`, err);
      }
    }
    return { deleted, errors };
  }

  /**
   * Convert an orphan Environment node into an Integration node and wire it
   * to a source environment via a hasIntegration Relationship.
   * Any extra config properties (hostname, propertyKey, etc.) are stored as config_* triples.
   */
  async connectOrphanNode(
    orphanId: string,
    orphanName: string,
    sourceEnvName: string,
    extraConfig: Record<string, string> = {},
  ): Promise<void> {
    // 1. Re-type the orphan node: Environment → Integration
    const changeType = `
      PREFIX env: <${this.ontologyPrefix}>
      DELETE { ?e env:type "Environment" }
      INSERT { ?e env:type "Integration" }
      WHERE  { ?e env:id "${this.escapeSparql(orphanId)}" ; env:type "Environment" }
    `;
    await this.executeSparqlUpdate(changeType);

    // 2. Persist any extra config properties on the (now) Integration node
    if (Object.keys(extraConfig).length > 0) {
      await this.addConfigurationProperties(orphanId, extraConfig);
    }

    // 3. Create hasIntegration Relationship: sourceEnv → Integration
    await this.createRelationship(sourceEnvName, 'hasIntegration', orphanName);
  }

  /**
   * Find an Environment node by its permanent `config_discoveredHostname` triple.
   * This survives admin renames because the hostname triple is stamped at creation
   * and never updated.  Returns the current env:name so the resolver can map
   * the raw hostname to the renamed node.
   */
  async searchByDiscoveredHostname(hostname: string): Promise<{ name: string } | null> {
    const query = `
      PREFIX env: <${this.ontologyPrefix}>
      SELECT ?name WHERE {
        ?entity env:type "Environment" ;
                env:config_discoveredHostname ?h ;
                env:name ?name .
        FILTER(LCASE(?h) = LCASE("${this.escapeSparql(hostname)}"))
      } LIMIT 1
    `;
    try {
      const result = await this.executeSparqlQuery(query);
      const b = result.results?.bindings?.[0];
      return b ? { name: b.name?.value || '' } : null;
    } catch (err) {
      console.warn(`Neptune discoveredHostname search failed for "${hostname}":`, err);
      return null;
    }
  }

  /**
   * Find an Environment node by its permanent `config_sourceEnvName` triple.
   * This triple is stamped at creation time by the pipeline AND by the cleanser
   * before any alias-driven rename, so it is never lost across reconciliations.
   * Returns the entity UUID so callers can update the existing node instead of
   * creating a duplicate.
   */
  async searchBySourceEnvName(envName: string): Promise<string | null> {
    const query = `
      PREFIX env: <${this.ontologyPrefix}>
      SELECT ?id WHERE {
        ?entity env:type "Environment" ;
                env:config_sourceEnvName ?sn ;
                env:id ?id .
        FILTER(LCASE(?sn) = LCASE("${this.escapeSparql(envName)}"))
      } LIMIT 1
    `;
    try {
      const result = await this.executeSparqlQuery(query);
      const b = result.results?.bindings?.[0];
      return b ? (b.id?.value || null) : null;
    } catch (err) {
      console.warn(`Neptune sourceEnvName search failed for "${envName}":`, err);
      return null;
    }
  }

  /**
   * Rename an environment node — replaces the env:name triple in place.
   */
  async renameEnvironment(nodeId: string, newName: string): Promise<void> {
    const update = `
      PREFIX env: <${this.ontologyPrefix}>
      DELETE {
        ?entity env:name ?oldName .
      }
      INSERT {
        ?entity env:name "${this.escapeSparql(newName)}" .
      }
      WHERE {
        ?entity env:id "${this.escapeSparql(nodeId)}" ;
                env:name ?oldName .
      }
    `;
    await this.executeSparqlUpdate(update);
  }

  /**
   * Merge the source environment node into the target:
   * 1. Re-point all Relationship edges that reference sourceId to targetId.
   * 2. Delete all triples of the source node.
   */
  async mergeEnvironments(sourceId: string, targetId: string): Promise<void> {
    // Re-point relationships where source env is the sourceEntityId
    const rerouteSource = `
      PREFIX env: <${this.ontologyPrefix}>
      DELETE {
        ?rel env:sourceEntityId "${this.escapeSparql(sourceId)}" .
      }
      INSERT {
        ?rel env:sourceEntityId "${this.escapeSparql(targetId)}" .
      }
      WHERE {
        ?rel env:type "Relationship" ;
             env:sourceEntityId "${this.escapeSparql(sourceId)}" .
      }
    `;
    await this.executeSparqlUpdate(rerouteSource);

    // Re-point relationships where source env is the targetEntityId
    const rerouteTarget = `
      PREFIX env: <${this.ontologyPrefix}>
      DELETE {
        ?rel env:targetEntityId "${this.escapeSparql(sourceId)}" .
      }
      INSERT {
        ?rel env:targetEntityId "${this.escapeSparql(targetId)}" .
      }
      WHERE {
        ?rel env:type "Relationship" ;
             env:targetEntityId "${this.escapeSparql(sourceId)}" .
      }
    `;
    await this.executeSparqlUpdate(rerouteTarget);

    // Delete the source entity node itself
    const deleteSource = `
      PREFIX env: <${this.ontologyPrefix}>
      DELETE WHERE {
        ?entity env:id "${this.escapeSparql(sourceId)}" ;
                ?p ?o .
      }
    `;
    await this.executeSparqlUpdate(deleteSource);
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
    
    // Build a single DELETE/INSERT WHERE query to upsert config properties
    let deleteClause = '';
    let insertClause = '';
    let wherePatterns = '';
    for (const [key, value] of Object.entries(configurations)) {
      if (value !== null && value !== undefined && value !== '') {
        const configProperty = `env:config_${key}`;
        const escapedValue = this.escapeSparql(String(value));
        const varName = `old_${key.replace(/[^a-zA-Z0-9]/g, '_')}`;
        deleteClause += `    <${entityUri}> ${configProperty} ?${varName} .\n`;
        insertClause += `    <${entityUri}> ${configProperty} "${escapedValue}" .\n`;
        wherePatterns += `    OPTIONAL { <${entityUri}> ${configProperty} ?${varName} } .\n`;
      }
    }

    if (insertClause) {
      const upsertQuery = `
        PREFIX env: <${this.ontologyPrefix}>
        
        DELETE {
${deleteClause}        }
        INSERT {
${insertClause}        }
        WHERE {
${wherePatterns}        }
      `;

      await this.executeSparqlUpdate(upsertQuery);
      console.log(`Upserted ${Object.keys(configurations).length} configuration properties on entity ${entityId}`);
    }
  }

  /**
   * Add stats properties directly to an entity (env:cpu, env:memory, etc.)
   * Stats are stored as first-class ontology properties, NOT as config_ properties.
   */
  async addStatsProperties(entityId: string, stats: EntityStats): Promise<void> {
    if (!stats || Object.keys(stats).length === 0) {
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
    
    // Build INSERT DATA query to add stat properties
    let insertTriples = '';
    for (const prop of STAT_PROPERTIES) {
      const value = stats[prop];
      if (value !== null && value !== undefined && value !== '') {
        const escapedValue = this.escapeSparql(String(value));
        insertTriples += `    <${entityUri}> env:${prop} "${escapedValue}" .\n`;
      }
    }

    if (insertTriples) {
      const updateQuery = `
        PREFIX env: <${this.ontologyPrefix}>
        
        INSERT DATA {
${insertTriples}        }
      `;

      await this.executeSparqlUpdate(updateQuery);
      console.log(`Added stats properties to entity ${entityId}: ${Object.keys(stats).filter(k => stats[k as keyof EntityStats]).join(', ')}`);
    }
  }

  /**
   * Update stats properties for an entity (delete old values, insert new ones)
   */
  async updateStatsProperties(entityId: string, stats: EntityStats): Promise<void> {
    if (!stats || Object.keys(stats).length === 0) {
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

    // Delete existing stat properties
    let deletePatterns = '';
    let deleteWherePatterns = '';
    for (const prop of STAT_PROPERTIES) {
      deletePatterns += `      <${entityUri}> env:${prop} ?old_${prop} .\n`;
      deleteWherePatterns += `      OPTIONAL { <${entityUri}> env:${prop} ?old_${prop} }\n`;
    }

    const deleteQuery = `
      PREFIX env: <${this.ontologyPrefix}>
      
      DELETE {
${deletePatterns}      }
      WHERE {
${deleteWherePatterns}      }
    `;

    await this.executeSparqlUpdate(deleteQuery);

    // Insert new stat values
    await this.addStatsProperties(entityId, stats);
  }

  // ---------------------------------------------------------------------------
  // Progressive exploration methods
  // ---------------------------------------------------------------------------

  /**
   * Return all environments (skeleton) plus aggregated env-to-env edges.
   * No apps or integrations are included — this is the overview landing view.
   */
  async getNetworkOverview(): Promise<NetworkOverview> {
    const envsQuery = `
      PREFIX env: <${this.ontologyPrefix}>
      SELECT ?id ?name ?status ?sourceService ?tag ?tagColor WHERE {
        ?entity env:id ?id ;
                env:name ?name ;
                env:type "Environment" .
        OPTIONAL { ?entity env:status ?status }
        OPTIONAL { ?entity env:sourceService ?sourceService }
        OPTIONAL { ?entity env:tag ?tag }
        OPTIONAL { ?entity env:tagColor ?tagColor }
      }
    `;

    const edgesQuery = `
      PREFIX env: <${this.ontologyPrefix}>
      SELECT ?envA ?envB (COUNT(DISTINCT ?integration) AS ?weight) WHERE {
        ?integration env:type "Integration" ;
                     env:id ?integrationId .
        ?relA env:type "Relationship" ;
              env:relationshipType "hasIntegration" ;
              env:sourceEntityId ?envA ;
              env:targetEntityId ?integrationId .
        ?relB env:type "Relationship" ;
              env:relationshipType "integratesWith" ;
              env:sourceEntityId ?integrationId ;
              env:targetEntityId ?envB .
        FILTER(?envA != ?envB)
      }
      GROUP BY ?envA ?envB
    `;

    let envsResult: any;
    let edgesResult: any;

    try {
      [envsResult, edgesResult] = await Promise.all([
        this.executeSparqlQuery(envsQuery),
        this.executeSparqlQuery(edgesQuery),
      ]);
    } catch (error) {
      console.error('getNetworkOverview: query error', error);
      throw error;
    }

    const nodes: EnvNode[] = (envsResult.results?.bindings || []).map((b: any) => ({
      id: b.id?.value || '',
      name: b.name?.value || '',
      status: b.status?.value,
      sourceService: b.sourceService?.value || undefined,
      tag: b.tag?.value || undefined,
      tagColor: b.tagColor?.value || undefined,
    }));

    const edges: WeightedEdge[] = (edgesResult.results?.bindings || []).map((b: any) => {
      const envA = b.envA?.value || '';
      const envB = b.envB?.value || '';
      const sorted = [envA, envB].sort();
      return {
        id: `${sorted[0]}::${sorted[1]}`,
        source: envA,
        target: envB,
        weight: parseInt(b.weight?.value || '1', 10),
      };
    });

    return { nodes, edges };
  }

  /**
   * Return focus node (full property bag) plus its 1-hop neighbors and connecting edges.
   */
  async getEnvironmentDetail(envId: string): Promise<EnvironmentDetail> {
    const escapedId = this.escapeSparql(envId);

    const neighborQuery = `
      PREFIX env: <${this.ontologyPrefix}>
      SELECT ?role ?nodeId ?nodeName ?nodeType ?nodeStatus ?prop ?val WHERE {
        {
          BIND("focus" AS ?role)
          ?entity env:id "${escapedId}" ;
                  env:id ?nodeId ;
                  env:name ?nodeName ;
                  env:type ?nodeType .
          OPTIONAL { ?entity env:status ?nodeStatus }
          OPTIONAL {
            ?entity ?prop ?val .
            FILTER(STRSTARTS(STR(?prop), "${this.ontologyPrefix}"))
            FILTER(?prop NOT IN (env:id, env:name, env:type))
          }
        } UNION {
          BIND("neighbor" AS ?role)
          {
            ?rel env:type "Relationship" ;
                 env:sourceEntityId "${escapedId}" ;
                 env:targetEntityId ?nodeId .
          } UNION {
            ?rel env:type "Relationship" ;
                 env:targetEntityId "${escapedId}" ;
                 env:sourceEntityId ?nodeId .
          }
          ?neighbor env:id ?nodeId ;
                    env:name ?nodeName ;
                    env:type ?nodeType .
          OPTIONAL { ?neighbor env:status ?nodeStatus }
        }
      }
    `;

    const edgesQuery = `
      PREFIX env: <${this.ontologyPrefix}>
      SELECT ?sourceId ?targetId ?relationType ?relationshipId WHERE {
        ?rel env:type "Relationship" ;
             env:id ?relationshipId ;
             env:relationshipType ?relationType ;
             env:sourceEntityId ?sourceId ;
             env:targetEntityId ?targetId .
        FILTER(?sourceId = "${escapedId}" || ?targetId = "${escapedId}")
      }
    `;

    let neighborResult: any;
    let edgesResult: any;
    try {
      [neighborResult, edgesResult] = await Promise.all([
        this.executeSparqlQuery(neighborQuery),
        this.executeSparqlQuery(edgesQuery),
      ]);
    } catch (error) {
      console.error('getEnvironmentDetail: query error', error);
      throw error;
    }

    const bindings: any[] = neighborResult.results?.bindings || [];

    // Split focus vs neighbor bindings
    const focusBindings = bindings.filter((b: any) => b.role?.value === 'focus');
    const neighborBindings = bindings.filter((b: any) => b.role?.value === 'neighbor');

    // Build focus node using same row-per-property pattern as buildNodesFromBindings
    const focusMap: Record<string, any> = {};
    for (const b of focusBindings) {
      const id = b.nodeId?.value || '';
      if (!focusMap[id]) {
        focusMap[id] = {
          id,
          label: b.nodeName?.value || '',
          type: b.nodeType?.value || '',
          properties: { status: b.nodeStatus?.value },
          configurations: {},
        };
      }
      const prop = b.prop?.value;
      const val = b.val?.value;
      if (prop && val) {
        const propName = prop.replace(this.ontologyPrefix, '');
        if (STAT_PROPERTIES.includes(propName as any)) {
          if (!focusMap[id].stats) focusMap[id].stats = {};
          focusMap[id].stats[propName] = val;
        } else if (propName.startsWith('config_')) {
          focusMap[id][propName] = val;
          focusMap[id].configurations[propName.replace('config_', '')] = val;
        } else {
          focusMap[id].properties[propName] = val;
        }
      }
    }

    const focusNodes = Object.values(focusMap) as FullNode[];
    if (focusNodes.length === 0) {
      throw new Error(`Environment with id "${envId}" not found`);
    }
    const focus = focusNodes[0];

    // Build skeleton neighbor nodes (deduplicated)
    const neighborMap: Record<string, NetworkNode> = {};
    for (const b of neighborBindings) {
      const id = b.nodeId?.value || '';
      if (id && id !== envId && !neighborMap[id]) {
        neighborMap[id] = {
          id,
          label: b.nodeName?.value || '',
          type: b.nodeType?.value || '',
          properties: { status: b.nodeStatus?.value },
          configurations: {},
        };
      }
    }
    const neighbors = Object.values(neighborMap);

    // Build edges
    const edgeBindings: any[] = edgesResult.results?.bindings || [];
    const neighborIds = new Set([envId, ...neighbors.map((n) => n.id)]);
    const edges: NetworkEdge[] = edgeBindings
      .filter((b: any) => {
        const src = b.sourceId?.value || '';
        const tgt = b.targetId?.value || '';
        return neighborIds.has(src) && neighborIds.has(tgt);
      })
      .map((b: any, i: number) => ({
        id: b.relationshipId?.value || `detail-edge-${i}`,
        source: b.sourceId?.value || '',
        target: b.targetId?.value || '',
        type: b.relationType?.value || 'related',
        label: b.relationType?.value || 'related',
        relationshipId: b.relationshipId?.value || null,
        properties: {},
      }));

    return { focus, neighbors, edges };
  }

  /**
   * Return 1-hop neighbors and connecting edges for any node.
   */
  async getNodeNeighbors(nodeId: string): Promise<NeighborResult> {
    const escapedId = this.escapeSparql(nodeId);

    const query = `
      PREFIX env: <${this.ontologyPrefix}>
      SELECT ?neighborId ?neighborName ?neighborType ?neighborStatus
             ?relType ?relationshipId ?direction WHERE {
        {
          BIND("outgoing" AS ?direction)
          ?rel env:type "Relationship" ;
               env:id ?relationshipId ;
               env:sourceEntityId "${escapedId}" ;
               env:targetEntityId ?neighborId ;
               env:relationshipType ?relType .
        } UNION {
          BIND("incoming" AS ?direction)
          ?rel env:type "Relationship" ;
               env:id ?relationshipId ;
               env:targetEntityId "${escapedId}" ;
               env:sourceEntityId ?neighborId ;
               env:relationshipType ?relType .
        }
        ?neighbor env:id ?neighborId ;
                  env:name ?neighborName ;
                  env:type ?neighborType .
        OPTIONAL { ?neighbor env:status ?neighborStatus }
      }
    `;

    let result: any;
    try {
      result = await this.executeSparqlQuery(query);
    } catch (error) {
      console.error('getNodeNeighbors: query error', error);
      throw error;
    }

    const bindings: any[] = result.results?.bindings || [];

    const neighborMap: Record<string, NetworkNode> = {};
    const edges: NetworkEdge[] = [];

    for (const b of bindings) {
      const neighborId = b.neighborId?.value || '';
      const direction = b.direction?.value || 'outgoing';
      const relType = b.relType?.value || 'related';
      const relId = b.relationshipId?.value || '';

      if (neighborId && !neighborMap[neighborId]) {
        neighborMap[neighborId] = {
          id: neighborId,
          label: b.neighborName?.value || '',
          type: b.neighborType?.value || '',
          properties: { status: b.neighborStatus?.value },
          configurations: {},
        };
      }

      // source is always the originator of the relationship
      const edgeSource = direction === 'outgoing' ? nodeId : neighborId;
      const edgeTarget = direction === 'outgoing' ? neighborId : nodeId;

      edges.push({
        id: relId || `neighbor-edge-${edges.length}`,
        source: edgeSource,
        target: edgeTarget,
        type: relType,
        label: relType,
        relationshipId: relId || null,
        properties: {},
      });
    }

    return {
      neighbors: Object.values(neighborMap),
      edges,
    };
  }

  /**
   * Return total edge count per node id.
   * Used by the frontend to compute hidden-edge badge values.
   */
  async getNodeDegrees(nodeIds: string[]): Promise<Map<string, number>> {
    if (nodeIds.length === 0) return new Map();

    const CHUNK_SIZE = 500;
    const result = new Map<string, number>();

    for (let i = 0; i < nodeIds.length; i += CHUNK_SIZE) {
      const chunk = nodeIds.slice(i, i + CHUNK_SIZE);
      const valuesClause = chunk.map((id) => `"${this.escapeSparql(id)}"`).join(' ');

      const query = `
        PREFIX env: <${this.ontologyPrefix}>
        SELECT ?nodeId (COUNT(DISTINCT ?rel) AS ?totalEdges) WHERE {
          VALUES ?nodeId { ${valuesClause} }
          {
            ?rel env:type "Relationship" ;
                 env:sourceEntityId ?nodeId .
          } UNION {
            ?rel env:type "Relationship" ;
                 env:targetEntityId ?nodeId .
          }
        }
        GROUP BY ?nodeId
      `;

      let chunkResult: any;
      try {
        chunkResult = await this.executeSparqlQuery(query);
      } catch (error) {
        console.error('getNodeDegrees: query error for chunk', error);
        continue; // Skip failed chunk; caller can still use partial results
      }

      for (const b of (chunkResult.results?.bindings || [])) {
        const id = b.nodeId?.value || '';
        const count = parseInt(b.totalEdges?.value || '0', 10);
        if (id) result.set(id, count);
      }
    }

    return result;
  }
}