// Integration Builder for PCE Discovery Pipeline
// Creates Environment, Integration entities and Relationships in Neptune
// Handles idempotency, stale detection, and stub vs real classification

import { NeptuneSparqlClient, Environment, Integration } from './neptune-sparql-client';
import { ClassifiedProperty } from './url-parser';
import { toFullIntegrationName, parsePropertyKey } from './key-parser';
import { HostnameResolver, ResolvedTarget, ConnectionType } from './hostname-resolver';

export interface DiscoveryResult {
  sourceEnvironment: string;
  environmentsCreated: string[];
  integrationsCreated: string[];
  integrationsUpdated: Array<{ name: string; oldTarget: string; newTarget: string }>;  // Target environment changed
  integrationsSkipped: string[];  // Already existed with same config
  integrationsStale: string[];    // Marked stale (no longer in source data)
  nonUrlConfigsStored: number;
  stubsDetected: number;
  errors: Array<{ key: string; error: string }>;
}

export interface IntegrationBuildInput {
  sourceEnvironmentName: string;
  urlProperties: ClassifiedProperty[];
  nonUrlProperties: ClassifiedProperty[];
  discoveredBy: string;  // "pce-discovery-pipeline" or "pce-csv-upload"
  sourceFile?: string;   // CSV filename if uploaded
  /**
   * When set, the Neptune hostname lookup tier is scoped to environments
   * whose `config_discoveredBy` triple matches this value.
   * Format: "<SOURCE_TYPE>:<instanceId>" e.g. "PAS:pasDatasource"
   * Derived from the `resolve-scope` edge in the topology table.
   */
  resolveScope?: string;
}

/**
 * Integration Builder
 * Orchestrates Neptune entity creation from classified PCE properties
 */
export class IntegrationBuilder {
  private neptuneClient: NeptuneSparqlClient;
  private hostnameResolver: HostnameResolver;

  constructor(neptuneClient: NeptuneSparqlClient, hostnameResolver: HostnameResolver) {
    this.neptuneClient = neptuneClient;
    this.hostnameResolver = hostnameResolver;
  }

  /**
   * Build all entities for a single source environment
   */
  async buildForEnvironment(input: IntegrationBuildInput): Promise<DiscoveryResult> {
    const result: DiscoveryResult = {
      sourceEnvironment: input.sourceEnvironmentName,
      environmentsCreated: [],
      integrationsCreated: [],
      integrationsUpdated: [],
      integrationsSkipped: [],
      integrationsStale: [],
      nonUrlConfigsStored: 0,
      stubsDetected: 0,
      errors: [],
    };

    // Step 1: Ensure source environment exists
    const sourceEnvId = await this.ensureEnvironmentExists(input.sourceEnvironmentName, result);
    if (!sourceEnvId) {
      result.errors.push({ key: '_source', error: `Failed to create/find source environment: ${input.sourceEnvironmentName}` });
      return result;
    }

    // Step 2: Store non-URL properties as config on the source environment
    if (input.nonUrlProperties.length > 0) {
      await this.storeNonUrlConfigs(sourceEnvId, input.nonUrlProperties, result);
    }

    // Step 3: Process URL properties → create integrations
    const activeIntegrationKeys = new Set<string>();

    for (const prop of input.urlProperties) {
      try {
        const integrationKey = await this.processUrlProperty(
          prop,
          input.sourceEnvironmentName,
          sourceEnvId,
          input.discoveredBy,
          input.sourceFile,
          result
        );
        if (integrationKey) {
          activeIntegrationKeys.add(integrationKey);
        }
      } catch (err) {
        const errMsg = err instanceof Error ? err.message : String(err);
        result.errors.push({ key: prop.propertyKey, error: errMsg });
        console.error(`Error processing ${prop.propertyKey}:`, err);
      }
    }

    // Step 4: Mark stale integrations (ones discovered previously but no longer in source data)
    await this.markStaleIntegrations(
      input.sourceEnvironmentName,
      input.discoveredBy,
      activeIntegrationKeys,
      result
    );

    return result;
  }

  /**
   * Ensure an Environment entity exists in Neptune, create if not
   */
  private async ensureEnvironmentExists(name: string, result: DiscoveryResult): Promise<string | null> {
    try {
      // 1. Fast path — node still has its original connector envName.
      const entities = await this.neptuneClient.getEntitiesByTypeAndName('Environment', name);
      if (entities.length > 0) return entities[0].id;

      // 2. Alias-rename resilient — node was renamed by the cleanser reconciler
      //    but still carries config_sourceEnvName stamped at creation and before
      //    each rename. Prevents duplicate nodes across all connectors.
      const renamedId = await this.neptuneClient.searchBySourceEnvName(name);
      if (renamedId) {
        console.log(`[integration-builder] environment "${name}" was alias-renamed — resolved to existing node ${renamedId}`);
        return renamedId;
      }

      // 3. Hostname breadcrumb fallback for dot/colon style names (backward compat).
      if (name.includes('.') || name.includes(':')) {
        const byHostname = await this.neptuneClient.searchByDiscoveredHostname(name);
        if (byHostname) {
          const found = await this.neptuneClient.getEntitiesByTypeAndName('Environment', byHostname.name);
          if (found.length > 0) return found[0].id;
        }
      }

      // 4. Truly new — create it.
      const env: Omit<Environment, 'id'> = {
        name,
        type: 'Environment',
        description: `Auto-discovered by PCE pipeline`,
      };
      const created = await this.neptuneClient.createEnvironment(env);
      result.environmentsCreated.push(name);
      console.log(`Created environment: ${name} (${created.id})`);

      // Stamp permanent breadcrumbs that survive future alias-driven renames.
      const stamps: Record<string, string> = { sourceEnvName: name };
      if (name.includes('.') || name.includes(':')) stamps.discoveredHostname = name;
      await this.neptuneClient.addConfigurationProperties(created.id, stamps);

      return created.id;
    } catch (err) {
      console.error(`Failed to ensure environment "${name}":`, err);
      return null;
    }
  }

  /**
   * Store non-URL properties as configuration on the source environment
   */
  private async storeNonUrlConfigs(
    envId: string,
    nonUrls: ClassifiedProperty[],
    result: DiscoveryResult
  ): Promise<void> {
    const configs: Record<string, string> = {};
    for (const prop of nonUrls) {
      configs[prop.propertyKey] = prop.propertyValue;
    }

    try {
      await this.neptuneClient.addConfigurationProperties(envId, configs);
      result.nonUrlConfigsStored = nonUrls.length;
      console.log(`Stored ${nonUrls.length} non-URL configs on environment ${envId}`);
    } catch (err) {
      console.error(`Failed to store non-URL configs:`, err);
      result.errors.push({ key: '_non_url_configs', error: String(err) });
    }
  }

  /**
   * Process a single URL property: resolve target, create integration, wire relationship
   * Returns the idempotency key for active integration tracking
   */
  private async processUrlProperty(
    prop: ClassifiedProperty,
    sourceEnvName: string,
    sourceEnvId: string,
    discoveredBy: string,
    sourceFile: string | undefined,
    result: DiscoveryResult
  ): Promise<string | null> {
    if (!prop.parsedUrl) return null;

    // Resolve hostname → target environment
    const resolved: ResolvedTarget = await this.hostnameResolver.resolve(prop.parsedUrl, sourceEnvName);

    // Create target environment if new
    if (resolved.isNewEnvironment) {
      const targetEnvId = await this.ensureEnvironmentExists(resolved.resolvedEnvironmentName, result);
      if (!targetEnvId) {
        result.errors.push({ key: prop.propertyKey, error: `Failed to create target environment: ${resolved.resolvedEnvironmentName}` });
        return null;
      }
    }

    // Build integration name from property key using the full property name
    // (service + endpoint hint) so each endpoint is tracked as a distinct integration.
    const integrationName = toFullIntegrationName(prop.propertyKey);
    const parsedKey = parsePropertyKey(prop.propertyKey);

    // Idempotency key: integration name + source environment
    const idempotencyKey = `${integrationName}::${sourceEnvName}`;

    // Check if this exact integration already exists
    const existingIntegrations = await this.neptuneClient.getEntitiesByTypeAndName('Integration', integrationName);
    let alreadyExists = false;

    for (const existing of existingIntegrations) {
      const existingConfigs = await this.neptuneClient.getEntityConfigurationsAsObject(existing.id);
      if (existingConfigs.config_sourceEnvironment === sourceEnvName) {
        // Same integration from same source — check if target changed
        const oldTarget = existingConfigs.config_targetEnvironment || '';

        if (oldTarget && oldTarget !== resolved.resolvedEnvironmentName) {
          // TARGET CHANGED — re-wire the integration to the new target
          console.log(`Integration "${integrationName}" target changed: "${oldTarget}" → "${resolved.resolvedEnvironmentName}"`);

          // Ensure new target environment exists
          if (resolved.isNewEnvironment) {
            const newTargetEnvId = await this.ensureEnvironmentExists(resolved.resolvedEnvironmentName, result);
            if (!newTargetEnvId) {
              result.errors.push({ key: prop.propertyKey, error: `Failed to create new target environment: ${resolved.resolvedEnvironmentName}` });
              return idempotencyKey;
            }
          }

          // Update config properties on the integration entity
          const newRelType = resolved.connectionType === 'stub' ? 'stubs' : 'integratesWith';
          const updatedConfigs: Record<string, string> = {
            targetEnvironment: resolved.resolvedEnvironmentName,
            connectionType: resolved.connectionType,
            protocol: prop.parsedUrl!.protocol,
            hostname: prop.parsedUrl!.hostname,
            port: String(prop.parsedUrl!.port || ''),
            path: prop.parsedUrl!.path,
            fullUrl: prop.parsedUrl!.original,
            resolutionMethod: resolved.resolutionMethod,
            lastUpdatedAt: new Date().toISOString(),
          };
          await this.neptuneClient.addConfigurationProperties(existing.id, updatedConfigs);

          // Delete old integratesWith/stubs relationship (integration → old target)
          try {
            const oldRelQuery = `
              PREFIX env: <http://neptune.aws.com/envmgmt/ontology/>
              SELECT ?relId WHERE {
                ?rel env:type "Relationship" ;
                     env:id ?relId ;
                     env:sourceEntityId "${existing.id}" ;
                     env:relationshipType ?rt .
                FILTER(?rt IN ("integratesWith", "stubs"))
              }
            `;
            const oldRelResult = await this.neptuneClient.executeSparqlQuery(oldRelQuery);
            const oldRelBindings = oldRelResult.results?.bindings || [];
            for (const rb of oldRelBindings) {
              const relId = rb.relId?.value;
              if (relId) {
                await this.neptuneClient.deleteRelationship(relId);
                console.log(`Deleted old relationship ${relId} (${integrationName} → ${oldTarget})`);
              }
            }
          } catch (relErr) {
            console.warn(`Failed to delete old target relationship for "${integrationName}":`, relErr);
          }

          // Create new relationship to the new target
          await this.neptuneClient.createRelationship(
            integrationName,
            newRelType,
            resolved.resolvedEnvironmentName,
            existing.id
          );

          // Log history: target change on the source environment
          try {
            await this.neptuneClient.logHistory(
              sourceEnvId,
              sourceEnvName,
              'update-integration-target',
              {
                integrationName,
                oldTarget,
                newTarget: resolved.resolvedEnvironmentName,
                connectionType: resolved.connectionType,
                updatedBy: discoveredBy,
              },
              discoveredBy
            );
          } catch (histErr) {
            console.warn(`Failed to log target change history for "${integrationName}":`, histErr);
          }

          // Orphan detection: check if old target is now disconnected
          try {
            const oldTargetEntity = await this.neptuneClient.getEntityByName(oldTarget, 'Environment');
            if (oldTargetEntity) {
              const relCount = await this.neptuneClient.countEntityRelationships(oldTargetEntity.id);
              if (relCount === 0) {
                const orphanDetail = {
                  integrationName,
                  previousSourceEnvironment: sourceEnvName,
                  oldTarget,
                  newTarget: resolved.resolvedEnvironmentName,
                  retargetedAt: new Date().toISOString(),
                };
                await this.neptuneClient.stampOrphanReason(
                  oldTargetEntity.id,
                  'retarget',
                  orphanDetail,
                  discoveredBy,
                );
                await this.neptuneClient.logHistory(
                  oldTargetEntity.id,
                  oldTarget,
                  'orphaned-by-retarget',
                  {
                    integrationName,
                    previousSourceEnvironment: sourceEnvName,
                    newTarget: resolved.resolvedEnvironmentName,
                    message: `Integration '${integrationName}' was retargeted away from this node to '${resolved.resolvedEnvironmentName}'`,
                  },
                  discoveredBy,
                );
                console.log(`Orphan reason stamped on "${oldTarget}" — retargeted to "${resolved.resolvedEnvironmentName}"`);
              }
            }
          } catch (orphanErr) {
            console.warn(`Orphan detection failed for old target "${oldTarget}" (non-fatal):`, orphanErr);
          }

          result.integrationsUpdated.push({
            name: integrationName,
            oldTarget,
            newTarget: resolved.resolvedEnvironmentName,
          });
        } else {
          // Same target — skip (idempotent)
          result.integrationsSkipped.push(integrationName);
          console.log(`Integration "${integrationName}" from ${sourceEnvName} already exists, skipping`);
        }

        alreadyExists = true;
        break;
      }
    }

    if (alreadyExists) return idempotencyKey;

    // Track stub
    if (resolved.connectionType === 'stub') {
      result.stubsDetected++;
    }

    // Create Integration entity
    const relationshipType = resolved.connectionType === 'stub' ? 'stubs' : 'integratesWith';

    const integration: Omit<Integration, 'id'> = {
      name: integrationName,
      type: 'Integration',
      description: `${sourceEnvName} → ${resolved.resolvedEnvironmentName} (${resolved.connectionType})`,
      status: resolved.connectionType === 'stub' ? 'stubbed' : 'active',
    };

    const created = await this.neptuneClient.createIntegration(integration);

    // Add configuration properties
    const configs: Record<string, string> = {
      sourceEnvironment: sourceEnvName,
      targetEnvironment: resolved.resolvedEnvironmentName,
      connectionType: resolved.connectionType,
      protocol: prop.parsedUrl.protocol,
      hostname: prop.parsedUrl.hostname,
      port: String(prop.parsedUrl.port || ''),
      path: prop.parsedUrl.path,
      fullUrl: prop.parsedUrl.original,
      propertyKey: prop.propertyKey,
      endpointHint: parsedKey.endpointHint,
      discoveredBy,
      discoveredAt: new Date().toISOString(),
      resolutionMethod: resolved.resolutionMethod,
    };
    if (sourceFile) {
      configs.sourceFile = sourceFile;
    }

    await this.neptuneClient.addConfigurationProperties(created.id, configs);

    // Create relationships: sourceEnv → hasIntegration → Integration → integratesWith/stubs → targetEnv
    // 1. Source environment → hasIntegration → Integration entity
    await this.neptuneClient.createRelationship(
      sourceEnvName,
      'hasIntegration',
      integrationName,
      sourceEnvId,
      created.id
    );

    // 2. Integration entity → integratesWith/stubs → Target environment
    await this.neptuneClient.createRelationship(
      integrationName,
      relationshipType,
      resolved.resolvedEnvironmentName,
      created.id
    );

    result.integrationsCreated.push(integrationName);
    console.log(`Created integration: "${integrationName}" (${sourceEnvName} → ${resolved.resolvedEnvironmentName}, ${resolved.connectionType})`);

    // Log create-integration history on the source environment
    try {
      await this.neptuneClient.logHistory(
        sourceEnvId,
        sourceEnvName,
        'create-integration',
        {
          integrationName,
          targetEnvName: resolved.resolvedEnvironmentName,
          connectionType: resolved.connectionType,
          createdBy: discoveredBy,
        },
        discoveredBy
      );
    } catch (histErr) {
      console.warn(`Failed to log creation history for "${integrationName}":`, histErr);
    }

    return idempotencyKey;
  }

  /**
   * Find integrations that were previously discovered by this pipeline for this environment
   * but are no longer present in the source data → log removal history, delete entity + relationships
   */
  private async markStaleIntegrations(
    sourceEnvName: string,
    discoveredBy: string,
    activeKeys: Set<string>,
    result: DiscoveryResult
  ): Promise<void> {
    try {
      // Query Neptune for all integrations discovered by this pipeline for this source env
      // Also fetch the target environment and source env ID for history logging
      const query = `
        PREFIX env: <http://neptune.aws.com/envmgmt/ontology/>
        SELECT ?id ?name ?targetEnv ?srcEnvId WHERE {
          ?entity env:type "Integration" ;
                  env:id ?id ;
                  env:name ?name ;
                  env:config_discoveredBy "${discoveredBy}" ;
                  env:config_sourceEnvironment "${sourceEnvName}" .
          OPTIONAL { ?entity env:config_targetEnvironment ?targetEnv }
          OPTIONAL {
            ?srcNode env:type "Environment" ;
                     env:name "${sourceEnvName}" ;
                     env:id ?srcEnvId .
          }
        }
      `;
      const queryResult = await this.neptuneClient.executeSparqlQuery(query);
      const bindings = queryResult.results?.bindings || [];

      for (const binding of bindings) {
        const name = binding.name?.value;
        const id = binding.id?.value;
        if (!name || !id) continue;

        const key = `${name}::${sourceEnvName}`;
        if (!activeKeys.has(key)) {
          const targetEnvName = binding.targetEnv?.value || 'unknown';
          const srcEnvId = binding.srcEnvId?.value || '';

          // Log delete-integration history on the source environment
          if (srcEnvId) {
            try {
              await this.neptuneClient.logHistory(
                srcEnvId,
                sourceEnvName,
                'delete-integration',
                {
                  integrationName: name,
                  targetEnvName,
                  targetEnvId: '',
                  removedBy: discoveredBy,
                },
                discoveredBy
              );
            } catch (histErr) {
              console.warn(`Failed to log removal history for "${name}":`, histErr);
            }
          }

          // Delete the two relationship entities (hasIntegration + integratesWith/stubs)
          try {
            const relQuery = `
              PREFIX env: <http://neptune.aws.com/envmgmt/ontology/>
              SELECT ?relId WHERE {
                ?rel env:type "Relationship" ;
                     env:id ?relId .
                { ?rel env:targetEntityId "${id}" ; env:relationshipType "hasIntegration" }
                UNION
                { ?rel env:sourceEntityId "${id}" ; env:relationshipType ?rt .
                  FILTER(?rt IN ("integratesWith", "stubs")) }
              }
            `;
            const relResult = await this.neptuneClient.executeSparqlQuery(relQuery);
            const relBindings = relResult.results?.bindings || [];
            for (const rb of relBindings) {
              const relId = rb.relId?.value;
              if (relId) {
                await this.neptuneClient.deleteRelationship(relId);
              }
            }
          } catch (relErr) {
            console.warn(`Failed to delete relationships for stale integration "${name}":`, relErr);
          }

          // Delete the integration entity itself
          try {
            const deleteQuery = `
              PREFIX env: <http://neptune.aws.com/envmgmt/ontology/>
              DELETE { ?entity ?p ?o }
              WHERE { ?entity env:id "${id}" ; ?p ?o }
            `;
            await this.neptuneClient.executeSparqlUpdate(deleteQuery);
          } catch (delErr) {
            console.warn(`Failed to delete stale integration entity "${name}":`, delErr);
          }

          result.integrationsStale.push(name);
          console.log(`Removed stale integration "${name}" (no longer in source data for ${sourceEnvName})`);
        }
      }
    } catch (err) {
      console.warn(`Stale detection failed for ${sourceEnvName}:`, err);
    }
  }
}
