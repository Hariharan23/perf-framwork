// Discovery Orchestrator for PCE Pipeline
// Coordinates the full pipeline: records → classify → resolve → build
// Accepts PceRecord[] from either PostgreSQL or CSV — source-agnostic

import { NeptuneSparqlClient } from './neptune-sparql-client';
import { PceRecord, groupByEnvironment } from './pce-csv-parser';
import { classifyProperties } from './url-parser';
import { HostnameResolver, HostnameResolverConfig } from './hostname-resolver';
import { IntegrationBuilder, DiscoveryResult, IntegrationBuildInput } from './integration-builder';

export interface OrchestratorConfig {
  neptuneEndpoint: string;
  neptuneRegion: string;
  aliasTableName?: string;
  autoCreateEnvironments: boolean;
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

  // Initialize clients
  const neptuneClient = new NeptuneSparqlClient(config.neptuneEndpoint, config.neptuneRegion);
  const resolverConfig: HostnameResolverConfig = {
    aliasTableName: config.aliasTableName,
    autoCreateEnvironments: config.autoCreateEnvironments,
  };
  const hostnameResolver = new HostnameResolver(neptuneClient, resolverConfig);
  const integrationBuilder = new IntegrationBuilder(neptuneClient, hostnameResolver);

  // Load alias map from DynamoDB (if configured)
  await hostnameResolver.loadAliasMap();

  // Group records by environment
  const grouped = groupByEnvironment(records);
  console.log(`Processing ${grouped.size} environment(s)`);

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
