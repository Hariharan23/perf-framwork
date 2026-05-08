// Discovery Orchestrator for PCE Pipeline
// Coordinates the full pipeline: records → classify → resolve → build
// Accepts PceRecord[] from either PostgreSQL or CSV — source-agnostic

import { NeptuneSparqlClient } from './neptune-sparql-client';
import { PceRecord, groupByEnvironment } from './pce-csv-parser';
import { classifyProperties } from './url-parser';
import { HostnameResolver, HostnameResolverConfig } from './hostname-resolver';
import { IntegrationBuilder, DiscoveryResult, IntegrationBuildInput } from './integration-builder';

// ── Topology edge type (subset used at runtime) ───────────────────────────────

interface TopologyEdge {
  pk: string;
  fromSource: string;
  fromInstance?: string;
  toEntityType: string;
  mode: string;
  joinKey: string;
  matchBy: string;
  resolveFrom?: string;
  resolveFromInstance?: string;
  priority: number;
  enabled: boolean;
}

// ── Config ────────────────────────────────────────────────────────────────────

export interface OrchestratorConfig {
  neptuneEndpoint: string;
  neptuneRegion: string;
  aliasTableName?: string;
  autoCreateEnvironments: boolean;
  /**
   * Name of the DynamoDB topology table (`pipeline-topology`).
   * When set, the orchestrator fetches topology edges at run time to apply
   * per-source / per-instance routing rules (e.g. `resolve-scope`).
   */
  topologyTableName?: string;
  /**
   * The pipeline instance identifier (envId) for the current run.
   * Used to look up instance-specific topology edge overrides.
   */
  sourceInstance?: string;
}

export interface PipelineResult {
  source: 'pce-database' | 'csv-upload';
  sourceFile?: string;
  startedAt: string;
  completedAt: string;
  totalRecords: number;
  environmentsProcessed: number;
  results: DiscoveryResult[];
  summary: {
    totalEnvironmentsCreated: number;
    totalIntegrationsCreated: number;
    totalIntegrationsSkipped: number;
    totalIntegrationsStale: number;
    totalNonUrlConfigs: number;
    totalStubs: number;
    totalErrors: number;
  };
}

/**
 * Run the full discovery pipeline on a set of PCE records
 */
export async function runDiscoveryPipeline(
  records: PceRecord[],
  config: OrchestratorConfig,
  source: 'pce-database' | 'csv-upload',
  sourceFile?: string
): Promise<PipelineResult> {
  const startedAt = new Date().toISOString();
  console.log(`Starting PCE discovery pipeline (source: ${source}, records: ${records.length})`);

  // ── Topology-aware routing ──────────────────────────────────────────────────
  // Derive the `resolveScope` string from the topology table if configured.
  // `resolveScope` restricts Neptune hostname lookup to environments whose
  // `config_discoveredBy` triple matches "<resolveFrom>:<resolveFromInstance>".
  let resolveScope: string | undefined;

  if (config.topologyTableName && config.sourceInstance) {
    try {
      resolveScope = await deriveResolveScope(
        config.topologyTableName,
        source,            // used as fromSource (e.g. "pce-database" → mapped to source type)
        config.sourceInstance,
        config.neptuneRegion,
      );
    } catch (e) {
      console.warn('Failed to derive resolveScope from topology table (non-fatal):', e);
    }
  }

  // ── Initialize clients ──────────────────────────────────────────────────────
  const neptuneClient = new NeptuneSparqlClient(config.neptuneEndpoint, config.neptuneRegion);
  const resolverConfig: HostnameResolverConfig = {
    aliasTableName:    config.aliasTableName,
    autoCreateEnvironments: config.autoCreateEnvironments,
    discoveredByScope: resolveScope,
  };
  const hostnameResolver   = new HostnameResolver(neptuneClient, resolverConfig);
  const integrationBuilder = new IntegrationBuilder(neptuneClient, hostnameResolver);

  // Load alias map from DynamoDB (if configured)
  await hostnameResolver.loadAliasMap();

  // Group records by environment
  const grouped = groupByEnvironment(records);
  console.log(`Processing ${grouped.size} environment(s)${resolveScope ? ` (resolveScope: ${resolveScope})` : ''}`);

  const discoveredBy = source === 'csv-upload' ? 'pce-csv-upload' : 'pce-discovery-pipeline';
  const results: DiscoveryResult[] = [];

  // Process each environment
  for (const [envName, envRecords] of grouped.entries()) {
    console.log(`\n--- Processing environment: ${envName} (${envRecords.length} properties) ---`);

    // Classify properties as URL or NON_URL
    const { urlProperties, nonUrlProperties } = classifyProperties(envRecords);
    console.log(`  URL properties: ${urlProperties.length}, NON_URL: ${nonUrlProperties.length}`);

    // Build integrations
    const buildInput: IntegrationBuildInput = {
      sourceEnvironmentName: envName,
      urlProperties,
      nonUrlProperties,
      discoveredBy,
      sourceFile,
      resolveScope,
    };

    const envResult = await integrationBuilder.buildForEnvironment(buildInput);
    results.push(envResult);

    console.log(`  Created: ${envResult.integrationsCreated.length}, Updated: ${envResult.integrationsUpdated.length}, Skipped: ${envResult.integrationsSkipped.length}, Stale: ${envResult.integrationsStale.length}, Stubs: ${envResult.stubsDetected}, Errors: ${envResult.errors.length}`);
  }

  // Build summary
  const summary = {
    totalEnvironmentsCreated: results.reduce((sum, r) => sum + r.environmentsCreated.length, 0),
    totalIntegrationsCreated: results.reduce((sum, r) => sum + r.integrationsCreated.length, 0),
    totalIntegrationsUpdated: results.reduce((sum, r) => sum + r.integrationsUpdated.length, 0),
    totalIntegrationsSkipped: results.reduce((sum, r) => sum + r.integrationsSkipped.length, 0),
    totalIntegrationsStale: results.reduce((sum, r) => sum + r.integrationsStale.length, 0),
    totalNonUrlConfigs: results.reduce((sum, r) => sum + r.nonUrlConfigsStored, 0),
    totalStubs: results.reduce((sum, r) => sum + r.stubsDetected, 0),
    totalErrors: results.reduce((sum, r) => sum + r.errors.length, 0),
  };

  const completedAt = new Date().toISOString();
  console.log(`\nPipeline completed: ${summary.totalIntegrationsCreated} created, ${summary.totalIntegrationsUpdated} updated, ${summary.totalIntegrationsSkipped} skipped, ${summary.totalIntegrationsStale} stale, ${summary.totalErrors} errors`);

  return {
    source,
    sourceFile,
    startedAt,
    completedAt,
    totalRecords: records.length,
    environmentsProcessed: grouped.size,
    results,
    summary,
  };
}

// ── Topology helper ────────────────────────────────────────────────────────────

/**
 * Query the topology DynamoDB table for `resolve-scope` edges applicable to
 * the current pipeline run and derive the `resolveScope` string.
 *
 * Resolution order (instance override beats global):
 *   1. Instance-specific edge: fromSource=src AND fromInstance=sourceInstance AND mode=resolve-scope
 *   2. Global edge:            fromSource=src AND fromInstance absent    AND mode=resolve-scope
 *
 * Returns `"<resolveFrom>:<resolveFromInstance>"` or `undefined` if no such edge exists.
 */
async function deriveResolveScope(
  tableName: string,
  source: string,
  sourceInstance: string,
  region: string,
): Promise<string | undefined> {
  const { DynamoDBClient, QueryCommand } = await import('@aws-sdk/client-dynamodb');
  const { unmarshall } = await import('@aws-sdk/util-dynamodb');

  const ddb = new DynamoDBClient({ region });

  // Query fromSource-index for all resolve-scope edges from this source type
  const fromSource = source.toUpperCase()
    .replace('PCE-DATABASE',  'PAS')
    .replace('PCE-CSV-UPLOAD', 'CSV');  // normalise source → connector sourceType

  const res = await ddb.send(new QueryCommand({
    TableName: tableName,
    IndexName: 'fromSource-index',
    KeyConditionExpression: 'fromSource = :src',
    FilterExpression: '#m = :mode AND enabled = :t',
    ExpressionAttributeNames: { '#m': 'mode' },
    ExpressionAttributeValues: {
      ':src':  { S: fromSource },
      ':mode': { S: 'resolve-scope' },
      ':t':    { BOOL: true },
    },
  }));

  const edges: TopologyEdge[] = (res.Items || []).map((i) => unmarshall(i) as TopologyEdge);
  if (edges.length === 0) return undefined;

  // Prefer instance-specific edge over global
  const instanceEdge = edges.find((e) => e.fromInstance === sourceInstance);
  const edge = instanceEdge ?? edges.find((e) => !e.fromInstance);
  if (!edge || !edge.resolveFrom) return undefined;

  const scope = edge.resolveFromInstance
    ? `${edge.resolveFrom}:${edge.resolveFromInstance}`
    : edge.resolveFrom;

  console.log(`Topology resolve-scope derived: "${scope}" (from ${instanceEdge ? 'instance' : 'global'} edge ${edge.pk})`);
  return scope;
}
