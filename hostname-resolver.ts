// Hostname Resolver for PCE Discovery Pipeline
// 3-tier resolution: DynamoDB alias map → Neptune substring search → auto-create
// Also classifies connections as STUB vs REAL

import { NeptuneSparqlClient } from './neptune-sparql-client';
import { ParsedUrl } from './url-parser';

// Hostnames that indicate a stub/local service
const LOCALHOST_PATTERNS = ['localhost', '127.0.0.1', '0.0.0.0', '::1'];

export type ConnectionType = 'real' | 'stub';

export interface ResolvedTarget {
  hostname: string;
  resolvedEnvironmentName: string;
  connectionType: ConnectionType;
  resolutionMethod: 'internal-domain' | 'alias-map' | 'neptune-lookup' | 'auto-create';
  isNewEnvironment: boolean;
}

export interface HostnameResolverConfig {
  /** DynamoDB table name for hostname-to-environment alias map */
  aliasTableName?: string;
  /** Whether to auto-create Environment entities that don't exist in Neptune */
  autoCreateEnvironments: boolean;
  /** Internal domain suffix for hostname → environment resolution (e.g. '.mycompany.internal') */
  internalDomainSuffix?: string;
}

/**
 * Hostname Resolver
 * Resolves a URL hostname to an environment name using a 3-tier strategy
 */
export class HostnameResolver {
  private neptuneClient: NeptuneSparqlClient;
  private config: HostnameResolverConfig;
  private aliasCache: Map<string, string> = new Map();
  private internalDomainSuffix: string | null;

  constructor(neptuneClient: NeptuneSparqlClient, config: HostnameResolverConfig) {
    this.neptuneClient = neptuneClient;
    this.config = config;
    this.internalDomainSuffix = config.internalDomainSuffix?.toLowerCase() || null;
  }

  /**
   * Load alias map from DynamoDB into local cache
   */
  async loadAliasMap(): Promise<void> {
    if (!this.config.aliasTableName) return;

    try {
      const { DynamoDBClient, ScanCommand } = await import('@aws-sdk/client-dynamodb');
      const ddbClient = new DynamoDBClient({});
      const result = await ddbClient.send(new ScanCommand({
        TableName: this.config.aliasTableName,
      }));

      this.aliasCache.clear();
      for (const item of result.Items || []) {
        const hostname = item.hostname?.S;
        const envName = item.environment_name?.S;
        if (hostname && envName) {
          this.aliasCache.set(hostname.toLowerCase(), envName);
        }
      }
      console.log(`Loaded ${this.aliasCache.size} hostname aliases from DynamoDB`);
    } catch (err) {
      console.warn('Failed to load alias map from DynamoDB, proceeding without aliases:', err);
    }
  }

  /**
   * Determine if a hostname is a stub/localhost reference
   */
  isStubHostname(hostname: string, sourceEnvironmentName: string): boolean {
    const lower = hostname.toLowerCase();
    const srcLower = sourceEnvironmentName.toLowerCase();

    // localhost / loopback
    if (LOCALHOST_PATTERNS.includes(lower)) return true;

    // Hostname itself matches source environment name
    if (lower === srcLower) return true;
    const firstSegment = lower.split('.')[0];
    if (firstSegment === srcLower) return true;

    // Points to itself — extract env name from internal domain and compare
    if (this.internalDomainSuffix && lower.endsWith(this.internalDomainSuffix)) {
      const envFromHostname = lower.replace(this.internalDomainSuffix, '');
      if (envFromHostname === srcLower) return true;
    }

    return false;
  }

  /**
   * Resolve a hostname to a target environment name
   * Returns the resolved environment and whether it's a stub or real connection
   */
  async resolve(parsedUrl: ParsedUrl, sourceEnvironmentName: string): Promise<ResolvedTarget> {
    const hostname = parsedUrl.hostname.toLowerCase();

    // --- Check for STUB first ---
    if (this.isStubHostname(hostname, sourceEnvironmentName)) {
      return {
        hostname,
        resolvedEnvironmentName: sourceEnvironmentName,
        connectionType: 'stub',
        resolutionMethod: 'internal-domain',
        isNewEnvironment: false,
      };
    }

    // --- Tier 1: Internal domain pattern ---
    if (this.internalDomainSuffix && hostname.endsWith(this.internalDomainSuffix)) {
      const envName = hostname;
      const exists = await this.neptuneClient.checkEntityExists(envName, 'Environment');
      if (exists) {
        return {
          hostname,
          resolvedEnvironmentName: envName,
          connectionType: 'real',
          resolutionMethod: 'internal-domain',
          isNewEnvironment: false,
        };
      }

      // Internal domain pattern matched but environment doesn't exist yet
      if (this.config.autoCreateEnvironments) {
        return {
          hostname,
          resolvedEnvironmentName: envName,
          connectionType: 'real',
          resolutionMethod: 'auto-create',
          isNewEnvironment: true,
        };
      }
    }

    // --- Tier 2: DynamoDB alias map ---
    const aliasMatch = this.aliasCache.get(hostname);
    if (aliasMatch) {
      return {
        hostname,
        resolvedEnvironmentName: aliasMatch,
        connectionType: 'real',
        resolutionMethod: 'alias-map',
        isNewEnvironment: false,
      };
    }

    // --- Tier 3: Neptune substring search ---
    // Try to find an environment whose name appears in the hostname
    const envMatch = await this.searchNeptuneForHostname(hostname);
    if (envMatch) {
      return {
        hostname,
        resolvedEnvironmentName: envMatch,
        connectionType: 'real',
        resolutionMethod: 'neptune-lookup',
        isNewEnvironment: false,
      };
    }

    // --- Tier 4: Auto-create from hostname ---
    if (this.config.autoCreateEnvironments) {
      return {
        hostname,
        resolvedEnvironmentName: hostname,
        connectionType: 'real',
        resolutionMethod: 'auto-create',
        isNewEnvironment: true,
      };
    }

    // Cannot resolve — treat as unknown external, use hostname as-is
    return {
      hostname,
      resolvedEnvironmentName: hostname,
      connectionType: 'real',
      resolutionMethod: 'auto-create',
      isNewEnvironment: true,
    };
  }

  /**
   * Search Neptune for an Environment whose name is a substring of the hostname
   */
  private async searchNeptuneForHostname(hostname: string): Promise<string | null> {
    try {
      const query = `
        PREFIX env: <http://neptune.aws.com/envmgmt/ontology/>
        SELECT ?name WHERE {
          ?entity env:type "Environment" ;
                  env:name ?name .
          FILTER(CONTAINS(LCASE("${hostname}"), LCASE(?name)))
        }
        ORDER BY DESC(STRLEN(?name))
        LIMIT 1
      `;
      const result = await this.neptuneClient.executeSparqlQuery(query);
      const bindings = result.results?.bindings;
      if (bindings && bindings.length > 0) {
        return bindings[0].name?.value || null;
      }
    } catch (err) {
      console.warn(`Neptune hostname search failed for "${hostname}":`, err);
    }
    return null;
  }
}
