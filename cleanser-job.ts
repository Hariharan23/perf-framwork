// Cleanser Job Lambda
// Invoked asynchronously by alias-manager (InvocationType: Event).
// Reads all aliases from DynamoDB, resolves each against Neptune,
// renames or merges the node, and writes progress to reconciler-runs table.

import { DynamoDBClient, ScanCommand, PutItemCommand, UpdateItemCommand } from '@aws-sdk/client-dynamodb';
import { NeptuneSparqlClient } from '../shared/neptune-sparql-client';

const REGION           = process.env.AWS_REGION || 'us-east-1';
const ALIAS_TABLE      = process.env.ALIAS_TABLE_NAME || '';
const RECONCILER_TABLE = process.env.RECONCILER_RUNS_TABLE || '';
const NEPTUNE_ENDPOINT = process.env.NEPTUNE_ENDPOINT || '';

const dynamo = new DynamoDBClient({ region: REGION });

interface CleanserEvent {
  runId: string;
  isPreview: boolean;
  triggeredBy?: string;
}

interface AliasItem {
  hostname: string;
  environmentName: string;
}

interface PlannedAction {
  hostname: string;
  currentName: string;
  targetName: string;
  action: 'rename' | 'merge' | 'skip' | 'not-found';
  nodeId?: string;
  mergeIntoId?: string;
}

export async function handler(event: CleanserEvent): Promise<void> {
  const { runId, isPreview, triggeredBy = 'unknown' } = event;
  console.log(`[cleanser-job] start runId=${runId} preview=${isPreview}`);

  await updateRun(runId, 'running', null, null);

  const neptune = new NeptuneSparqlClient(NEPTUNE_ENDPOINT, REGION);

  try {
    // 1. Scan all aliases
    const aliases = await scanAliases();
    console.log(`[cleanser-job] loaded ${aliases.length} aliases`);

    // 2. Plan actions (resolve each alias against Neptune)
    const plan: PlannedAction[] = [];
    for (const alias of aliases) {
      const action = await planAction(neptune, alias);
      plan.push(action);
    }

    const renamed  = plan.filter(p => p.action === 'rename');
    const merged   = plan.filter(p => p.action === 'merge');
    const skipped  = plan.filter(p => p.action === 'skip' || p.action === 'not-found');

    if (isPreview) {
      // Preview: store the plan and exit — no Neptune writes
      await updateRun(runId, 'completed', { plan, isPreview: true }, {
        renamedCount: 0,
        mergedCount:  0,
        skippedCount: skipped.length,
        previewRenameCount: renamed.length,
        previewMergeCount:  merged.length,
      });
      console.log(`[cleanser-job] preview complete — ${renamed.length} renames, ${merged.length} merges planned`);
      return;
    }

    // 3. Execute: apply renames then merges
    let renamedCount = 0;
    let mergedCount  = 0;
    const errors: string[] = [];

    for (const action of renamed) {
      try {
        // Stamp the original name before renaming so pipelines can still find
        // this node by its connector envName after the alias reconciliation.
        await neptune.addConfigurationProperties(action.nodeId!, { sourceEnvName: action.currentName });
        await neptune.renameEnvironment(action.nodeId!, action.targetName);
        renamedCount++;
        console.log(`[cleanser-job] renamed "${action.currentName}" → "${action.targetName}"`);
      } catch (err) {
        errors.push(`rename ${action.hostname}: ${err instanceof Error ? err.message : String(err)}`);
      }
    }

    for (const action of merged) {
      try {
        // Stamp the original name on the surviving target node so pipelines
        // that reference the old envName can still locate it.
        await neptune.addConfigurationProperties(action.mergeIntoId!, { sourceEnvName: action.currentName });
        await neptune.mergeEnvironments(action.nodeId!, action.mergeIntoId!);
        mergedCount++;
        console.log(`[cleanser-job] merged "${action.currentName}" into "${action.targetName}"`);
      } catch (err) {
        errors.push(`merge ${action.hostname}: ${err instanceof Error ? err.message : String(err)}`);
      }
    }

    await updateRun(
      runId,
      errors.length > 0 ? 'completed-with-errors' : 'completed',
      { plan, errors },
      { renamedCount, mergedCount, skippedCount: skipped.length, errors },
    );
    console.log(`[cleanser-job] done renamed=${renamedCount} merged=${mergedCount} errors=${errors.length}`);

  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    console.error('[cleanser-job] fatal error:', err);
    await updateRun(runId, 'failed', { error: msg }, { renamedCount: 0, mergedCount: 0, skippedCount: 0 });
  }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

async function scanAliases(): Promise<AliasItem[]> {
  const items: AliasItem[] = [];
  let lastKey: Record<string, any> | undefined;
  do {
    const res = await dynamo.send(new ScanCommand({
      TableName: ALIAS_TABLE,
      ExclusiveStartKey: lastKey,
    }));
    for (const item of res.Items || []) {
      const hostname = item.hostname?.S;
      const envName  = item.environment_name?.S;
      if (hostname && envName) items.push({ hostname, environmentName: envName });
    }
    lastKey = res.LastEvaluatedKey as Record<string, any> | undefined;
  } while (lastKey);
  return items;
}

async function planAction(neptune: NeptuneSparqlClient, alias: AliasItem): Promise<PlannedAction> {
  const { hostname, environmentName: targetName } = alias;

  // Find current Neptune node whose name matches the hostname
  const fromNodes = await neptune.getEntitiesByTypeAndName('Environment', hostname);
  if (!fromNodes.length) {
    return { hostname, currentName: hostname, targetName, action: 'not-found' };
  }
  const fromNode = fromNodes[0];

  // Skip if already renamed
  if (fromNode.name === targetName) {
    return { hostname, currentName: fromNode.name, targetName, action: 'skip', nodeId: fromNode.id };
  }

  // Check whether targetName already exists in Neptune
  const toNodes = await neptune.getEntitiesByTypeAndName('Environment', targetName);
  if (toNodes.length > 0) {
    // Merge: re-point all edges from fromNode into toNode, delete fromNode
    return {
      hostname,
      currentName:  fromNode.name,
      targetName,
      action:       'merge',
      nodeId:       fromNode.id,
      mergeIntoId:  toNodes[0].id,
    };
  }

  // Rename: target doesn't exist yet — update the name in-place
  return { hostname, currentName: fromNode.name, targetName, action: 'rename', nodeId: fromNode.id };
}

async function updateRun(runId: string, status: string, plan: any, stats: any): Promise<void> {
  if (!RECONCILER_TABLE) return;
  try {
    await dynamo.send(new UpdateItemCommand({
      TableName: RECONCILER_TABLE,
      Key: { runId: { S: runId } },
      UpdateExpression:
        'SET #s = :s, updatedAt = :ua' +
        (plan  !== null ? ', planJson = :p'  : '') +
        (stats !== null ? ', statsJson = :st' : ''),
      ExpressionAttributeNames: { '#s': 'status' },
      ExpressionAttributeValues: {
        ':s':  { S: status },
        ':ua': { S: new Date().toISOString() },
        ...(plan  !== null && { ':p':  { S: JSON.stringify(plan) } }),
        ...(stats !== null && { ':st': { S: JSON.stringify(stats) } }),
      },
    }));
  } catch (e) {
    console.error('[cleanser-job] failed to update run record:', e);
  }
}
