// Integration Builder for PCE Discovery Pipeline
// Creates Environment, Integration entities and Relationships in Neptune
// Handles idempotency, stale detection, and stub vs real classification

import { NeptuneSparqlClient, Environment, Integration } from './neptune-sparql-client';
import { ClassifiedProperty } from './url-parser';
import { toIntegrationName, parsePropertyKey } from './key-parser';
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
  autoCleanedNodes: string[];      // Edgeless auto-created env nodes deleted after stale cleanup
  resolvedByAliasMap: number;      // Integrations resolved via DynamoDB alias map
  resolvedByNeptuneLookup: number; // Integrations resolved by matching existing Neptune environment
  resolvedByAutoCreate: number;    // Integrations that triggered auto-create of a new environment node
  resolvedByInternalDomain: number;// Integrations resolved via internal domain suffix pattern
  errors: Array<{ key: string; error: string }>;
}

export interface IntegrationBuildInput {
  sourceEnvironmentName: string;
  urlProperties: ClassifiedProperty[];
  nonUrlProperties: ClassifiedProperty[];
  discoveredBy: string;  // "pce-discovery-pipeline" or "pce-csv-upload"
  sourceFile?: string;   // CSV filename if uploaded
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
      autoCleanedNodes: [],
      resolvedByAliasMap: 0,
      resolvedByNeptuneLookup: 0,
      resolvedByAutoCreate: 0,
      resolvedByInternalDomain: 0,
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
   * When autoCreateMeta is provided, extra config properties are stamped on
   * the newly-created node so orphan-tracking queries can attribute it.
   */
  private async ensureEnvironmentExists(
    name: string,
    result: DiscoveryResult,
    autoCreateMeta?: {
      rawHostname:   string;
      createdByRun:  string;
      createdBySource: string;
      propertyKey?:  string;
      fullUrl?:      string;
      protocol?:     string;
      port?:         string;
      path?:         string;
      endpointHint?: string;
      sourceFile?:   string;
    },
  ): Promise<string | null> {
    try {
      const exists = await this.neptuneClient.checkEntityExists(name, 'Environment');
      if (exists) {
        const entities = await this.neptuneClient.getEntitiesByTypeAndName('Environment', name);
        return entities[0]?.id || null;
      }

      const env: Omit<Environment, 'id'> = {
        name,
        type: 'Environment',
        description: `Auto-discovered by PCE pipeline`,
      };
      const created = await this.neptuneClient.createEnvironment(env);
      result.environmentsCreated.push(name);
      console.log(`Created environment: ${name} (${created.id})`);

      // Stamp auto-create metadata so the orphan-tracker can tell
      // which pipeline run created this node and from which hostname.
      if (autoCreateMeta) {
        const meta: Record<string, string> = {
          rawHostname:     autoCreateMeta.rawHostname,
          createdByRun:    autoCreateMeta.createdByRun,
          createdBySource: autoCreateMeta.createdBySource,
          createdAt:       new Date().toISOString(),
        };
        if (autoCreateMeta.propertyKey)  meta.propertyKey  = autoCreateMeta.propertyKey;
        if (autoCreateMeta.fullUrl)      meta.fullUrl      = autoCreateMeta.fullUrl;
        if (autoCreateMeta.protocol)     meta.protocol     = autoCreateMeta.protocol;
        if (autoCreateMeta.port)         meta.port         = autoCreateMeta.port;
        if (autoCreateMeta.path)         meta.path         = autoCreateMeta.path;
        if (autoCreateMeta.endpointHint) meta.endpointHint = autoCreateMeta.endpointHint;
        if (autoCreateMeta.sourceFile)   meta.sourceFile   = autoCreateMeta.sourceFile;
        await this.neptuneClient.addConfigurationProperties(created.id, meta);
      }

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

    // Create target environment if new — track its ID so we can roll it back
    // if any subsequent write (createIntegration, createRelationship) fails.
    let freshlyCreatedTargetId: string | null = null;
    if (resolved.isNewEnvironment) {
      const parsedKeyMeta = parsePropertyKey(prop.propertyKey);
      const targetEnvId = await this.ensureEnvironmentExists(
        resolved.resolvedEnvironmentName,
        result,
        {
          rawHostname:     prop.parsedUrl.hostname,
          createdByRun:    discoveredBy,
          createdBySource: sourceEnvName,
          propertyKey:     prop.propertyKey,
          fullUrl:         prop.parsedUrl.original,
          protocol:        prop.parsedUrl.protocol,
          port:            String(prop.parsedUrl.port || ''),
          path:            prop.parsedUrl.path,
          endpointHint:    parsedKeyMeta.endpointHint,
          sourceFile:      sourceFile,
        },
      );
      if (!targetEnvId) {
        result.errors.push({ key: prop.propertyKey, error: `Failed to create target environment: ${resolved.resolvedEnvironmentName}` });
        return null;
      }
      freshlyCreatedTargetId = targetEnvId;
    }

    // Build integration name from property key
    const integrationName = toIntegrationName(prop.propertyKey);
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

    let created: Integration;
    try {
      created = await this.neptuneClient.createIntegration(integration);

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
    } catch (writeErr) {
      // Mid-pipeline failure after the target env was already written to Neptune.
      // Roll back that freshly-created node so it doesn't sit as an edgeless orphan.
      if (freshlyCreatedTargetId) {
        try {
          await this.neptuneClient.deleteEnvironmentNode(freshlyCreatedTargetId);
          const idx = result.environmentsCreated.indexOf(resolved.resolvedEnvironmentName);
          if (idx !== -1) result.environmentsCreated.splice(idx, 1);
          console.warn(`[integration-builder] rolled back auto-created target "${resolved.resolvedEnvironmentName}" after write failure`);
        } catch (rollbackErr) {
          console.error(`[integration-builder] rollback of "${resolved.resolvedEnvironmentName}" failed:`, rollbackErr);
        }
      }
      const errMsg = writeErr instanceof Error ? writeErr.message : String(writeErr);
      result.errors.push({ key: prop.propertyKey, error: `Integration write failed (rolled back): ${errMsg}` });
      return null;
    }

    result.integrationsCreated.push(integrationName);
    // Track resolution method for this newly created integration
    if (resolved.resolutionMethod === 'alias-map') result.resolvedByAliasMap++;
    else if (resolved.resolutionMethod === 'neptune-lookup') result.resolvedByNeptuneLookup++;
    else if (resolved.resolutionMethod === 'auto-create') result.resolvedByAutoCreate++;
    else if (resolved.resolutionMethod === 'internal-domain') result.resolvedByInternalDomain++;
    console.log(`Created integration: "${integrationName}" (${sourceEnvName} → ${resolved.resolvedEnvironmentName}, ${resolved.connectionType}, via ${resolved.resolutionMethod})`);

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
        SELECT ?id ?name ?targetEnv ?targetResolutionMethod ?srcEnvId WHERE {
          ?entity env:type "Integration" ;
                  env:id ?id ;
                  env:name ?name ;
                  env:config_discoveredBy "${discoveredBy}" ;
                  env:config_sourceEnvironment "${sourceEnvName}" .
          OPTIONAL { ?entity env:config_targetEnvironment ?targetEnv }
          OPTIONAL { ?entity env:config_resolutionMethod ?targetResolutionMethod }
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

          // If the target was auto-created from a raw hostname, check whether it is now
          // edgeless (no other integration still points to it). If so, delete it — it has
          // no business meaning without any integration referencing it.
          const targetResolutionMethod = binding.targetResolutionMethod?.value || '';
          if (targetResolutionMethod === 'auto-create' && targetEnvName && targetEnvName !== 'unknown') {
            try {
              const targetIdQuery = `
                PREFIX env: <http://neptune.aws.com/envmgmt/ontology/>
                SELECT ?nodeId WHERE {
                  ?e env:type "Environment" ; env:name "${targetEnvName}" ; env:id ?nodeId .
                } LIMIT 1
              `;
              const targetIdResult = await this.neptuneClient.executeSparqlQuery(targetIdQuery);
              const targetNodeId = targetIdResult.results?.bindings?.[0]?.nodeId?.value;
              if (targetNodeId) {
                const stillConnected = await this.neptuneClient.hasAnyEdges(targetNodeId);
                if (!stillConnected) {
                  await this.neptuneClient.deleteEnvironmentNode(targetNodeId);
                  result.autoCleanedNodes.push(targetEnvName);
                  console.log(`Deleted edgeless auto-created environment "${targetEnvName}" (last integration pointing to it was removed)`);
                }
              }
            } catch (orphanErr) {
              console.warn(`Failed to check/delete auto-created target "${targetEnvName}" after stale cleanup:`, orphanErr);
            }
          }
        }
      }
    } catch (err) {
      console.warn(`Stale detection failed for ${sourceEnvName}:`, err);
    }
  }
}
