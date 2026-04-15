// Report Data Collector — queries Neptune for all data needed to generate a report
// Fetches environments, connections, history, stats for a given owner

import { NeptuneSparqlClient, NetworkData, NetworkNode, NetworkEdge, Entity, EntityStats } from './neptune-sparql-client';

export interface RemovedConnection {
  type: 'Environment' | 'Integration';
  name: string;
  targetEnvName: string;
  owner: string;
  removedAt: string;
  removedBy: string;
}

export interface EnvironmentReportData {
  id: string;
  name: string;
  owner: string;
  description: string;
  status: string;
  createdAt: string;
  stats: EntityStats;
  properties: Record<string, any>;
  configurations: Record<string, string>;
  connectedEnvironments: ConnectedEnvironment[];
  integrations: IntegrationSummary[];
  removedConnections: RemovedConnection[];
  applications: ApplicationInfo[];
  history: HistoryEntry[];
  scheduledUpdates: string;
  collaborators: string;
  updatedAt: string;
  primaryUsage: string;
  currentUsage: string;
}

export interface ConnectedEnvironment {
  id: string;
  name: string;
  owner: string;
  status: string;
  stats: EntityStats;
  integrationCount: number;
  newIntegrationCount: number;
  isNewConnection: boolean;
}

export interface IntegrationSummary {
  id: string;
  name: string;
  sourceEnvName: string;
  targetEnvName: string;
  status: string;
  createdAt: string;
  isNew: boolean;
  properties: Record<string, any>;
}

export interface ApplicationInfo {
  id: string;
  name: string;
  version: string;
  healthScore: number;
  status: string;
}

export interface HistoryEntry {
  id: string;
  action: string;
  timestamp: string;
  user: string;
  details: string;
}

export interface OwnerReportData {
  owner: string;
  reportMode: 'owner' | 'environment';
  reportDate: string;
  periodStart: string;
  periodEnd: string;
  reportType: 'weekly' | 'daily';
  environments: EnvironmentReportData[];
  allConnectedEnvNames: string[];
  totalIntegrations: number;
  totalChanges: number;
  totalAlerts: number;
}

export class ReportDataCollector {
  private client: NeptuneSparqlClient;

  constructor(client: NeptuneSparqlClient) {
    this.client = client;
  }

  /**
   * Collect all data for an owner's report
   */
  async collectForOwner(owner: string, reportType: 'weekly' | 'daily' = 'weekly', customRange?: { startDate: string; endDate: string }): Promise<OwnerReportData> {
    const now = new Date();
    let periodStart: Date;
    let periodEnd: Date;
    if (customRange?.startDate && customRange?.endDate) {
      periodStart = new Date(customRange.startDate);
      periodEnd = new Date(customRange.endDate);
    } else {
      const periodDays = reportType === 'weekly' ? 7 : 1;
      periodStart = new Date(now);
      periodStart.setDate(periodStart.getDate() - periodDays);
      periodEnd = now;
    }

    // Step 1: Get all environments owned by this person
    const ownerEnvs = await this.getEnvironmentsByOwner(owner);
    if (ownerEnvs.length === 0) {
      return {
        owner,
        reportMode: 'owner' as const,
        reportDate: now.toISOString(),
        periodStart: periodStart.toISOString(),
        periodEnd: periodEnd.toISOString(),
        reportType,
        environments: [],
        allConnectedEnvNames: [],
        totalIntegrations: 0,
        totalChanges: 0,
        totalAlerts: 0,
      };
    }

    // Step 2: Get network data for these environments
    const envIds = ownerEnvs.map(e => e.id);
    const networkData = await this.client.getNetworkData();

    // Step 3: Build per-environment report data in parallel
    const envReportPromises = ownerEnvs.map(env =>
      this.buildEnvironmentReport(env, networkData, periodStart, periodEnd)
    );
    const environments = await Promise.all(envReportPromises);

    // Step 4: Aggregate
    const allConnectedNames = new Set<string>();
    let totalIntegrations = 0;
    let totalChanges = 0;

    for (const env of environments) {
      for (const conn of env.connectedEnvironments) {
        allConnectedNames.add(conn.name);
      }
      totalIntegrations += env.integrations.length;
      totalChanges += env.history.length;
    }

    return {
      owner,
      reportMode: 'owner' as const,
      reportDate: now.toISOString(),
      periodStart: periodStart.toISOString(),
      periodEnd: now.toISOString(),
      reportType,
      environments,
      allConnectedEnvNames: Array.from(allConnectedNames),
      totalIntegrations,
      totalChanges,
      totalAlerts: this.countAlerts(environments),
    };
  }

  /**
   * Collect report data for specific environment IDs (no owner filter)
   */
  async collectForEnvironmentIds(environmentIds: string[], reportType: 'weekly' | 'daily' = 'weekly', customRange?: { startDate: string; endDate: string }): Promise<OwnerReportData> {
    const now = new Date();
    let periodStart: Date;
    let periodEnd: Date;
    if (customRange?.startDate && customRange?.endDate) {
      periodStart = new Date(customRange.startDate);
      periodEnd = new Date(customRange.endDate);
    } else {
      const periodDays = reportType === 'weekly' ? 7 : 1;
      periodStart = new Date(now);
      periodStart.setDate(periodStart.getDate() - periodDays);
      periodEnd = now;
    }

    const networkData = await this.client.getNetworkData();
    const idSet = new Set(environmentIds);
    const matchedEnvs = networkData.nodes.filter(n =>
      n.type === 'Environment' && idSet.has(n.id)
    );

    if (matchedEnvs.length === 0) {
      return {
        owner: 'Selected Environments',
        reportMode: 'environment' as const,
        reportDate: now.toISOString(),
        periodStart: periodStart.toISOString(),
        periodEnd: periodEnd.toISOString(),
        reportType,
        environments: [],
        allConnectedEnvNames: [],
        totalIntegrations: 0,
        totalChanges: 0,
        totalAlerts: 0,
      };
    }

    const envReportPromises = matchedEnvs.map(env =>
      this.buildEnvironmentReport(env, networkData, periodStart, periodEnd)
    );
    const environments = await Promise.all(envReportPromises);

    const allConnectedNames = new Set<string>();
    let totalIntegrations = 0;
    let totalChanges = 0;
    for (const env of environments) {
      for (const conn of env.connectedEnvironments) {
        allConnectedNames.add(conn.name);
      }
      totalIntegrations += env.integrations.length;
      totalChanges += env.history.length;
    }

    // Derive owner label from matched environments
    const owners = [...new Set(matchedEnvs.map(e => e.properties?.owner).filter(Boolean))];
    const ownerLabel = owners.length === 1 ? owners[0] : owners.length > 0 ? owners.join(', ') : 'Selected Environments';

    return {
      owner: ownerLabel,
      reportMode: 'environment',
      reportDate: now.toISOString(),
      periodStart: periodStart.toISOString(),
      periodEnd: periodEnd.toISOString(),
      reportType,
      environments,
      allConnectedEnvNames: Array.from(allConnectedNames),
      totalIntegrations,
      totalChanges,
      totalAlerts: this.countAlerts(environments),
    };
  }

  /**
   * Get all environments owned by a specific person
   */
  private async getEnvironmentsByOwner(owner: string): Promise<NetworkNode[]> {
    const allEnvs = await this.client.getEntitiesByType('Environment');
    const networkData = await this.client.getNetworkData();

    // Filter environments by owner (case-insensitive)
    const ownerLower = owner.toLowerCase();
    return networkData.nodes.filter(n =>
      n.type === 'Environment' &&
      n.properties?.owner?.toLowerCase() === ownerLower
    );
  }

  /**
   * Build full report data for a single environment
   */
  private async buildEnvironmentReport(
    envNode: NetworkNode,
    networkData: NetworkData,
    periodStart: Date,
    periodEnd: Date
  ): Promise<EnvironmentReportData> {
    // Get history for this environment
    const history = await this.client.getEntityHistory(envNode.id);
    const periodHistory = history.filter(h => {
      const ts = new Date(h.timestamp);
      return ts >= periodStart && ts <= periodEnd;
    });

    // Find connected environments via relationships (1-hop: env → integration → targetEnv)
    const connectedEnvs = this.findConnectedEnvironments(envNode.id, networkData, periodStart);

    // Find integrations for this environment
    const integrations = this.findIntegrations(envNode.id, networkData, periodStart);

    // Find applications (runsOn relationship)
    const applications = this.findApplications(envNode.id, networkData);

    // Extract removed connections from history (delete-integration entries)
    // Deduplicate by integrationName: when both hasIntegration and integratesWith
    // are deleted, two entries are logged — keep the one with best target info.
    const rawRemoved = periodHistory
      .filter(h => h.action === 'delete-integration')
      .map(h => {
        let changes: any = {};
        if (h.changes) {
          try { changes = typeof h.changes === 'string' ? JSON.parse(h.changes) : h.changes; } catch (_) {}
        }
        return {
          type: 'Integration' as const,
          name: changes.integrationName || 'Unknown integration',
          targetEnvName: changes.targetEnvName || 'unknown',
          owner: '',
          removedAt: h.timestamp,
          removedBy: changes.removedBy || h.user || 'system',
        };
      });

    // Deduplicate: group by integrationName, prefer the entry with a known targetEnvName
    const removedMap = new Map<string, RemovedConnection>();
    for (const r of rawRemoved) {
      const existing = removedMap.get(r.name);
      if (!existing) {
        removedMap.set(r.name, r);
      } else if (existing.targetEnvName === 'unknown' && r.targetEnvName !== 'unknown') {
        removedMap.set(r.name, r);
      }
    }
    const removedConnections: RemovedConnection[] = Array.from(removedMap.values());

    // buildNodesFromBindings stores extra props at top level of envNode, not inside properties
    const n = envNode as any;
    return {
      id: envNode.id,
      name: envNode.label,
      owner: n.owner || envNode.properties?.owner || '',
      description: n.description || envNode.properties?.description || '',
      status: envNode.properties?.status || envNode.stats?.currentState || 'unknown',
      createdAt: n.createdAt || envNode.properties?.createdAt || '',
      updatedAt: n.updatedAt || envNode.properties?.updatedAt || '',
      primaryUsage: n.primaryUsage || envNode.properties?.primaryUsage || '',
      currentUsage: n.currentUsage || envNode.properties?.currentUsage || '',
      stats: envNode.stats || {},
      properties: envNode.properties || {},
      configurations: envNode.configurations || {},
      connectedEnvironments: connectedEnvs,
      integrations,
      removedConnections,
      applications,
      scheduledUpdates: n.scheduledUpdates || '',
      collaborators: n.collaborators || '',
      history: this.deduplicateHistory(periodHistory).map(h => ({
        id: h.id,
        action: h.action,
        timestamp: h.timestamp,
        user: h.user || 'system',
        details: h.changes
          ? (typeof h.changes === 'string' ? h.changes : JSON.stringify(h.changes))
          : '',
      })),
    };
  }

  /**
   * Deduplicate integration history entries: when both hasIntegration and
   * integratesWith are deleted/created, two entries are logged for the same
   * integration. Keep the entry with the most complete information.
   */
  private deduplicateHistory(history: Array<{ id: string; action: string; timestamp: string; user?: string; changes?: any }>): typeof history {
    const integrationActions = new Set(['delete-integration', 'create-integration']);
    const nonIntegration: typeof history = [];
    const integrationMap = new Map<string, typeof history[0]>();

    for (const h of history) {
      if (!integrationActions.has(h.action)) {
        nonIntegration.push(h);
        continue;
      }

      let changes: any = {};
      if (h.changes) {
        try { changes = typeof h.changes === 'string' ? JSON.parse(h.changes) : h.changes; } catch (_) {}
      }
      const key = `${h.action}::${changes.integrationName || h.id}`;
      const existing = integrationMap.get(key);

      if (!existing) {
        integrationMap.set(key, h);
      } else {
        // Prefer the entry with a known targetEnvName
        let existChanges: any = {};
        if (existing.changes) {
          try { existChanges = typeof existing.changes === 'string' ? JSON.parse(existing.changes) : existing.changes; } catch (_) {}
        }
        if ((!existChanges.targetEnvName || existChanges.targetEnvName === 'unknown') &&
            changes.targetEnvName && changes.targetEnvName !== 'unknown') {
          integrationMap.set(key, h);
        }
      }
    }

    return [...nonIntegration, ...integrationMap.values()].sort(
      (a, b) => new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime()
    );
  }

  /**
   * Find all environments connected to the given environment via integrations.
   * Aggregates integration counts per connected environment.
   */
  private findConnectedEnvironments(
    envId: string,
    networkData: NetworkData,
    periodStart: Date
  ): ConnectedEnvironment[] {
    const envNode = networkData.nodes.find(n => n.id === envId);
    if (!envNode) return [];

    // Find integration nodes connected to this env
    const integrationIds = new Set<string>();
    const targetEnvMap = new Map<string, { count: number; newCount: number; intIds: string[] }>();

    // Edges: env → hasIntegration → integration
    const outEdges = networkData.edges.filter(e => e.source === envId && e.type === 'hasIntegration');
    for (const edge of outEdges) {
      integrationIds.add(edge.target);
    }

    // Edges: integration → integratesWith → targetEnv
    for (const intId of integrationIds) {
      const intEdges = networkData.edges.filter(e => e.source === intId && e.type === 'integratesWith');
      const intNode = networkData.nodes.find(n => n.id === intId);
      const intCreatedAt = intNode?.properties?.createdAt || (intNode as any)?.createdAt || '';
      const isNew = intCreatedAt
        ? new Date(intCreatedAt) >= periodStart
        : false;

      for (const intEdge of intEdges) {
        const targetId = intEdge.target;
        if (targetId === envId) continue; // skip self

        const existing = targetEnvMap.get(targetId) || { count: 0, newCount: 0, intIds: [] };
        existing.count++;
        if (isNew) existing.newCount++;
        existing.intIds.push(intId);
        targetEnvMap.set(targetId, existing);
      }
    }

    // Also check reverse: other envs that have integrations pointing to this env
    const inboundIntEdges = networkData.edges.filter(e => e.target === envId && e.type === 'integratesWith');
    for (const intEdge of inboundIntEdges) {
      const intId = intEdge.source;
      const intNode = networkData.nodes.find(n => n.id === intId);
      const intCreatedAt = intNode?.properties?.createdAt || (intNode as any)?.createdAt || '';
      const isNew = intCreatedAt
        ? new Date(intCreatedAt) >= periodStart
        : false;

      // Find which env owns this integration
      const ownerEdge = networkData.edges.find(e => e.target === intId && e.type === 'hasIntegration');
      if (ownerEdge && ownerEdge.source !== envId) {
        const sourceId = ownerEdge.source;
        const existing = targetEnvMap.get(sourceId) || { count: 0, newCount: 0, intIds: [] };
        existing.count++;
        if (isNew) existing.newCount++;
        existing.intIds.push(intId);
        targetEnvMap.set(sourceId, existing);
      }
    }

    // Build connected environment list
    const result: ConnectedEnvironment[] = [];
    for (const [targetId, data] of targetEnvMap) {
      const targetNode = networkData.nodes.find(n => n.id === targetId);
      if (!targetNode || targetNode.type !== 'Environment') continue;

      const targetCreatedAt = targetNode.properties?.createdAt || (targetNode as any)?.createdAt || '';
      const isNewEnv = targetCreatedAt ? new Date(targetCreatedAt) >= periodStart : false;

      result.push({
        id: targetId,
        name: targetNode.label,
        owner: targetNode.properties?.owner || (targetNode as any).owner || '',
        status: targetNode.properties?.status || targetNode.stats?.currentState || 'unknown',
        stats: targetNode.stats || {},
        integrationCount: data.count,
        newIntegrationCount: data.newCount,
        isNewConnection: isNewEnv || (data.newCount > 0 && data.count === data.newCount),
      });
    }

    return result.sort((a, b) => b.integrationCount - a.integrationCount);
  }

  /**
   * Find all integrations for an environment
   */
  private findIntegrations(
    envId: string,
    networkData: NetworkData,
    periodStart: Date
  ): IntegrationSummary[] {
    const outEdges = networkData.edges.filter(e => e.source === envId && e.type === 'hasIntegration');
    const results: IntegrationSummary[] = [];

    for (const edge of outEdges) {
      const intNode = networkData.nodes.find(n => n.id === edge.target);
      if (!intNode) continue;

      // Find target environment
      const targetEdge = networkData.edges.find(e => e.source === intNode.id && e.type === 'integratesWith');
      const targetNode = targetEdge ? networkData.nodes.find(n => n.id === targetEdge.target) : null;

      const createdAt = intNode.properties?.createdAt || (intNode as any)?.createdAt || '';
      const isNew = createdAt ? new Date(createdAt) >= periodStart : false;

      results.push({
        id: intNode.id,
        name: intNode.label,
        sourceEnvName: networkData.nodes.find(n => n.id === envId)?.label || '',
        targetEnvName: targetNode?.label || 'unknown',
        status: intNode.properties?.status || 'active',
        createdAt,
        isNew,
        properties: intNode.properties || {},
      });
    }

    return results;
  }

  /**
   * Find applications deployed on an environment
   */
  private findApplications(envId: string, networkData: NetworkData): ApplicationInfo[] {
    const appEdges = networkData.edges.filter(
      e => (e.source === envId && e.type === 'runsOn') ||
           (e.target === envId && e.type === 'runsOn')
    );

    const apps: ApplicationInfo[] = [];
    for (const edge of appEdges) {
      const appId = edge.source === envId ? edge.target : edge.source;
      const appNode = networkData.nodes.find(n => n.id === appId && n.type === 'Application');
      if (!appNode) continue;

      apps.push({
        id: appNode.id,
        name: appNode.label,
        version: appNode.properties?.version || '',
        healthScore: parseInt(appNode.stats?.healthScore || '0', 10),
        status: appNode.properties?.status || 'active',
      });
    }

    return apps;
  }

  /**
   * Count alerts based on thresholds
   */
  private countAlerts(environments: EnvironmentReportData[]): number {
    let count = 0;
    for (const env of environments) {
      const cpu = parseFloat(env.stats.cpu || '0');
      const health = parseFloat(env.stats.healthScore || '100');
      const memory = parseFloat(env.stats.memory || '0');
      const storage = parseFloat(env.stats.storage || '0');

      if (cpu > 80) count++;
      if (health < 70) count++;
      if (memory > 85) count++;
      if (storage > 90) count++;
      if (env.status === 'Warning' || env.status === 'Critical') count++;

      // Check ScienceLogic freshness
      if (env.stats.lastSyncedAt) {
        const syncAge = Date.now() - new Date(env.stats.lastSyncedAt).getTime();
        if (syncAge > 24 * 60 * 60 * 1000) count++; // stale > 24h
      }
    }
    return count;
  }
}
