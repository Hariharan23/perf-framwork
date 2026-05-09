// Connector Registry
// Central map of sourceType → DataSourceConnector implementation.
// To add a new data source: implement DataSourceConnector, then call register().
// No other files need to change.
//
// autoRegisterToDynamo(): call once on cold-start (from pipeline-config/pipeline-sync)
// to seed CONNECTOR#, BLOCK#, and default EDGE# records in the topology DynamoDB table.
// The operation is idempotent — existing records are never overwritten.

import { DynamoDBClient, GetItemCommand, PutItemCommand } from '@aws-sdk/client-dynamodb';
import { marshall } from '@aws-sdk/util-dynamodb';
import { randomUUID } from 'crypto';
import { DataSourceConnector, ConnectorConfigSchema, EdgeTemplate } from './connectors/base-connector';
import { PasConnector } from './connectors/pas-connector';
import { DcmConnector } from './connectors/dcm-connector';

const REGISTRY = new Map<string, DataSourceConnector>();

function register(connector: DataSourceConnector): void {
  REGISTRY.set(connector.sourceType.toUpperCase(), connector);
}

// ── Register built-in connectors ──
register(new PasConnector());
register(new DcmConnector());

// ── Public API ──

export function getConnector(sourceType: string): DataSourceConnector {
  const connector = REGISTRY.get(sourceType.toUpperCase());
  if (!connector) {
    throw new Error(
      `Unknown source type: "${sourceType}". Registered: ${Array.from(REGISTRY.keys()).join(', ')}`,
    );
  }
  return connector;
}

export function listConnectors(): ConnectorConfigSchema[] {
  return Array.from(REGISTRY.values()).map((c) => c.schema);
}

export function isValidSourceType(sourceType: string): boolean {
  return REGISTRY.has(sourceType.toUpperCase());
}

/**
 * Seed all built-in connectors into the pipeline-topology DynamoDB table.
 * Creates CONNECTOR#, BLOCK#, and global EDGE# records for any source type
 * that does not already have a CONNECTOR# record.  Safe to call on every
 * cold start — existing records are never modified.
 */
export async function autoRegisterToDynamo(topologyTableName: string, region: string): Promise<void> {
  const ddb = new DynamoDBClient({ region });
  const now = new Date().toISOString();

  for (const connector of REGISTRY.values()) {
    const connectorPk = `CONNECTOR#${connector.sourceType.toUpperCase()}`;

    // Check if already seeded (idempotency guard)
    const existing = await ddb.send(new GetItemCommand({
      TableName: topologyTableName,
      Key: marshall({ pk: connectorPk }),
    }));
    if (existing.Item) continue; // already registered

    // 1. Write CONNECTOR# record — metadata + serialised schema
    await ddb.send(new PutItemCommand({
      TableName: topologyTableName,
      Item: marshall({
        pk: connectorPk,
        sourceType: connector.sourceType.toUpperCase(),
        displayName: connector.schema.displayName,
        description: connector.schema.description,
        icon: connector.schema.icon ?? '',
        configSchema: JSON.stringify(connector.schema.fields),
        defaultEdges: JSON.stringify(connector.schema.defaultEdges ?? []),
        builtIn: true,
        createdAt: now,
      }),
      ConditionExpression: 'attribute_not_exists(pk)',
    })).catch(() => { /* another Lambda already seeded it — safe to ignore */ });

    // 2. Write BLOCK# canvas record (default position, will be updated by UI)
    await ddb.send(new PutItemCommand({
      TableName: topologyTableName,
      Item: marshall({
        pk: `BLOCK#${connector.sourceType.toUpperCase()}`,
        sourceType: connector.sourceType.toUpperCase(),
        displayName: connector.schema.displayName,
        icon: connector.schema.icon ?? '',
        canvasX: 60,
        canvasY: 80,
        enabled: true,
        createdAt: now,
      }),
      ConditionExpression: 'attribute_not_exists(pk)',
    })).catch(() => {});

    // 3. Write global EDGE# records for each default edge template
    for (const template of (connector.schema.defaultEdges ?? [])) {
      await seedEdge(ddb, topologyTableName, connector.sourceType.toUpperCase(), template, now);
    }
  }
}

async function seedEdge(
  ddb: DynamoDBClient,
  tableName: string,
  sourceType: string,
  template: EdgeTemplate,
  now: string,
): Promise<void> {
  const pk = `EDGE#${randomUUID()}`;
  await ddb.send(new PutItemCommand({
    TableName: tableName,
    Item: marshall({
      pk,
      fromSource: sourceType,
      // fromInstance is intentionally absent (null = global — applies to every instance)
      toEntityType: template.toEntityType,
      mode: template.mode,
      joinKey: template.joinKey,
      matchBy: template.matchBy,
      priority: template.priority,
      description: template.description,
      enabled: true,
      builtIn: true,
      createdAt: now,
      updatedAt: now,
    }),
  })).catch(() => {});
}
