import { APIGatewayProxyEvent, APIGatewayProxyResult } from 'aws-lambda';
import { NeptuneSparqlClient } from '../shared/neptune-sparql-client';

const neptuneClient = new NeptuneSparqlClient();

// ── Constants ─────────────────────────────────────────────────────────────────

const ORPHAN_REASON_TRIPLES = [
  'config_orphanReason',
  'config_orphanedAt',
  'config_orphanedBy',
  'config_orphanDetail',
  'config_previouslyConnected',
] as const;

// ── SPARQL helpers ────────────────────────────────────────────────────────────

function escSparql(value: string): string {
  return value.replace(/\\/g, '\\\\').replace(/"/g, '\\"');
}

const ONTOLOGY = 'http://neptune.aws.com/envmgmt/ontology/';

/** Write the 5 orphan-reason triples onto an entity node. Idempotent — deletes first. */
async function stampOrphanReason(
  entityId: string,
  reason: string,
  detail: Record<string, unknown>,
  triggeredBy: string,
): Promise<void> {
  const now = new Date().toISOString();
  const detailJson = JSON.stringify(detail).replace(/\\/g, '\\\\').replace(/"/g, '\\"');

  const update = `
    PREFIX env: <${ONTOLOGY}>
    DELETE {
      ?e env:config_orphanReason ?or ;
         env:config_orphanedAt ?oa ;
         env:config_orphanedBy ?ob ;
         env:config_orphanDetail ?od ;
         env:config_previouslyConnected ?pc .
    }
    INSERT {
      ?e env:config_orphanReason "${escSparql(reason)}" ;
         env:config_orphanedAt "${escSparql(now)}" ;
         env:config_orphanedBy "${escSparql(triggeredBy)}" ;
         env:config_orphanDetail "${detailJson}" ;
         env:config_previouslyConnected "true" .
    }
    WHERE {
      ?e env:id "${escSparql(entityId)}" .
      OPTIONAL { ?e env:config_orphanReason ?or }
      OPTIONAL { ?e env:config_orphanedAt ?oa }
      OPTIONAL { ?e env:config_orphanedBy ?ob }
      OPTIONAL { ?e env:config_orphanDetail ?od }
      OPTIONAL { ?e env:config_previouslyConnected ?pc }
    }
  `;
  await neptuneClient.executeSparqlUpdate(update);
}

/** Remove the 5 orphan-reason triples from an entity node. */
async function clearOrphanReason(entityId: string): Promise<void> {
  const update = `
    PREFIX env: <${ONTOLOGY}>
    DELETE {
      ?e env:config_orphanReason ?or ;
         env:config_orphanedAt ?oa ;
         env:config_orphanedBy ?ob ;
         env:config_orphanDetail ?od ;
         env:config_previouslyConnected ?pc .
    }
    WHERE {
      ?e env:id "${escSparql(entityId)}" .
      OPTIONAL { ?e env:config_orphanReason ?or }
      OPTIONAL { ?e env:config_orphanedAt ?oa }
      OPTIONAL { ?e env:config_orphanedBy ?ob }
      OPTIONAL { ?e env:config_orphanDetail ?od }
      OPTIONAL { ?e env:config_previouslyConnected ?pc }
    }
  `;
  await neptuneClient.executeSparqlUpdate(update);
}

/** Read the orphan-reason triples for a single entity. Returns null if none set. */
async function getOrphanReason(entityId: string): Promise<{
  orphanReason: string;
  orphanedAt: string;
  orphanedBy: string;
  orphanDetail: string;
  previouslyConnected: boolean;
} | null> {
  const query = `
    PREFIX env: <${ONTOLOGY}>
    SELECT ?orphanReason ?orphanedAt ?orphanedBy ?orphanDetail ?previouslyConnected
    WHERE {
      ?e env:id "${escSparql(entityId)}" .
      OPTIONAL { ?e env:config_orphanReason ?orphanReason }
      OPTIONAL { ?e env:config_orphanedAt ?orphanedAt }
      OPTIONAL { ?e env:config_orphanedBy ?orphanedBy }
      OPTIONAL { ?e env:config_orphanDetail ?orphanDetail }
      OPTIONAL { ?e env:config_previouslyConnected ?previouslyConnected }
    } LIMIT 1
  `;
  const result = await neptuneClient.executeSparqlQuery(query);
  const b = result.results?.bindings?.[0];
  if (!b || !b.orphanReason?.value) return null;
  return {
    orphanReason: b.orphanReason?.value || '',
    orphanedAt: b.orphanedAt?.value || '',
    orphanedBy: b.orphanedBy?.value || '',
    orphanDetail: b.orphanDetail?.value || '',
    previouslyConnected: b.previouslyConnected?.value === 'true',
  };
}

/** Count active Relationship edges touching a node (source or target). */
async function countRelationships(entityId: string): Promise<number> {
  const query = `
    PREFIX env: <${ONTOLOGY}>
    SELECT (COUNT(*) AS ?count) WHERE {
      {
        ?rel env:type "Relationship" ; env:sourceEntityId "${escSparql(entityId)}" .
      } UNION {
        ?rel env:type "Relationship" ; env:targetEntityId "${escSparql(entityId)}" .
      }
    }
  `;
  const result = await neptuneClient.executeSparqlQuery(query);
  const val = result.results?.bindings?.[0]?.count?.value;
  return val ? parseInt(val, 10) : 0;
}

// ── CORS headers ──────────────────────────────────────────────────────────────

const HEADERS = {
  'Content-Type': 'application/json',
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, DELETE, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization, Accept, Origin, X-Requested-With, x-api-key',
  'Access-Control-Max-Age': '300',
};

function ok(data: unknown, extra?: Record<string, unknown>): APIGatewayProxyResult {
  return {
    statusCode: 200,
    headers: HEADERS,
    body: JSON.stringify({ success: true, ...extra, data, timestamp: new Date().toISOString() }),
  };
}

function err(statusCode: number, message: string): APIGatewayProxyResult {
  return {
    statusCode,
    headers: HEADERS,
    body: JSON.stringify({ success: false, error: message, timestamp: new Date().toISOString() }),
  };
}

// ── Handler ───────────────────────────────────────────────────────────────────

export const handler = async (event: APIGatewayProxyEvent): Promise<APIGatewayProxyResult> => {
  console.log('OrphanMaintenance event:', JSON.stringify(event, null, 2));

  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers: HEADERS, body: JSON.stringify({ message: 'CORS preflight successful' }) };
  }

  let body: Record<string, any> = {};
  if (event.body) {
    try { body = JSON.parse(event.body); } catch { /* ignore */ }
  }

  const operation =
    event.pathParameters?.operation ||
    event.queryStringParameters?.operation ||
    body.operation;

  if (!operation) {
    return err(400, 'Missing operation parameter. Available: list-orphans, stamp-orphan-reason, clear-orphan-reason, connect-orphan, delete-orphan, health');
  }

  try {
    switch (operation.toLowerCase()) {

      // ── health ──────────────────────────────────────────────────────────────
      case 'health': {
        const healthy = await neptuneClient.healthCheck();
        return ok({ status: healthy ? 'healthy' : 'unhealthy', service: 'orphan-maintenance' });
      }

      // ── list-orphans ────────────────────────────────────────────────────────
      case 'list-orphans': {
        const rawOrphans = await neptuneClient.listOrphanEnvironments();

        // Fetch orphan-reason triples for each node in parallel (best-effort)
        const withReason = await Promise.all(
          rawOrphans.map(async (n) => {
            try {
              const reason = await getOrphanReason(n.id);
              return reason
                ? {
                    ...n,
                    orphanReason: reason.orphanReason,
                    orphanedAt: reason.orphanedAt,
                    orphanedBy: reason.orphanedBy,
                    orphanDetail: (() => {
                      try { return JSON.parse(reason.orphanDetail); } catch { return reason.orphanDetail; }
                    })(),
                    previouslyConnected: reason.previouslyConnected,
                  }
                : n;
            } catch {
              return n;
            }
          }),
        );

        // Optional filter by reason
        const filterReason = event.queryStringParameters?.reason || body.reason;
        const filtered = filterReason
          ? withReason.filter((n: any) => n.orphanReason === filterReason)
          : withReason;

        return ok(filtered, { count: filtered.length });
      }

      // ── stamp-orphan-reason ─────────────────────────────────────────────────
      case 'stamp-orphan-reason': {
        const { entityId, reason, detail = {}, triggeredBy = 'admin' } = body;
        if (!entityId) return err(400, 'Missing entityId');
        if (!reason)   return err(400, 'Missing reason');

        await stampOrphanReason(entityId, reason, detail, triggeredBy);
        return ok({ entityId, reason, stamped: true });
      }

      // ── clear-orphan-reason ─────────────────────────────────────────────────
      case 'clear-orphan-reason': {
        const entityId = body.entityId || event.queryStringParameters?.entityId;
        if (!entityId) return err(400, 'Missing entityId');

        await clearOrphanReason(entityId);
        return ok({ entityId, cleared: true });
      }

      // ── connect-orphan ──────────────────────────────────────────────────────
      // Thin wrapper: clears orphan-reason stamp, then delegates to the existing
      // connectOrphanNode logic in neptune-sparql-client.
      case 'connect-orphan': {
        const { entityId, entityName, sourceEnvName, extraConfig = {} } = body;
        if (!entityId)      return err(400, 'Missing entityId');
        if (!entityName)    return err(400, 'Missing entityName');
        if (!sourceEnvName) return err(400, 'Missing sourceEnvName');

        // Clear any retarget/orphan stamps before reconnecting
        await clearOrphanReason(entityId);

        await neptuneClient.connectOrphanNode(entityId, entityName, sourceEnvName, extraConfig);
        return ok({ entityId, entityName, sourceEnvName, connected: true });
      }

      // ── delete-orphan ───────────────────────────────────────────────────────
      // Guard: refuses retarget-reason orphans unless force=true is passed.
      case 'delete-orphan': {
        const { entityId, force = false } = body;
        if (!entityId) return err(400, 'Missing entityId');

        // Check orphan reason
        const reasonData = await getOrphanReason(entityId);
        if (reasonData?.orphanReason === 'retarget' && !force) {
          return err(409, [
            `Cannot delete node '${entityId}' — it was orphaned by a pipeline retarget`,
            'and may still be referenced. Pass force:true to override, or use connect-orphan',
            'to reconnect it first.',
          ].join(' '));
        }

        // Check it's still actually an orphan (no live relationships)
        const relCount = await countRelationships(entityId);
        if (relCount > 0 && !force) {
          return err(409, `Cannot delete node '${entityId}' — it has ${relCount} active relationship(s). Pass force:true to override.`);
        }

        const result = await neptuneClient.deleteOrphanEnvironments([entityId]);
        if (result.errors.length > 0) {
          return err(500, `Delete failed: ${result.errors.join('; ')}`);
        }
        return ok({ entityId, deleted: result.deleted === 1 });
      }

      // ── bulk-delete-orphans ─────────────────────────────────────────────────
      // Bulk delete — skips retarget-reason nodes unless force=true.
      case 'bulk-delete-orphans': {
        const { entityIds, force = false }: { entityIds: string[]; force: boolean } = body;
        if (!Array.isArray(entityIds) || entityIds.length === 0) {
          return err(400, 'Missing or empty entityIds array');
        }

        const skipped: string[] = [];
        const toDelete: string[] = [];

        await Promise.all(
          entityIds.map(async (id) => {
            const reasonData = await getOrphanReason(id).catch(() => null);
            if (reasonData?.orphanReason === 'retarget' && !force) {
              skipped.push(id);
            } else {
              toDelete.push(id);
            }
          }),
        );

        let deleted = 0;
        const errors: string[] = [];
        if (toDelete.length > 0) {
          const result = await neptuneClient.deleteOrphanEnvironments(toDelete);
          deleted = result.deleted;
          errors.push(...result.errors);
        }

        return ok({ deleted, skipped, errors, skippedCount: skipped.length });
      }

      // ── get-orphan-reason ───────────────────────────────────────────────────
      case 'get-orphan-reason': {
        const entityId = body.entityId || event.queryStringParameters?.entityId;
        if (!entityId) return err(400, 'Missing entityId');

        const reasonData = await getOrphanReason(entityId);
        return ok(
          reasonData
            ? {
                ...reasonData,
                orphanDetail: (() => {
                  try { return JSON.parse(reasonData.orphanDetail); } catch { return reasonData.orphanDetail; }
                })(),
              }
            : null,
          { entityId },
        );
      }

      default:
        return err(400, `Unknown operation: ${operation}. Available: list-orphans, stamp-orphan-reason, clear-orphan-reason, connect-orphan, delete-orphan, bulk-delete-orphans, get-orphan-reason, health`);
    }
  } catch (error) {
    console.error('OrphanMaintenance error:', error);
    return {
      statusCode: 500,
      headers: HEADERS,
      body: JSON.stringify({
        success: false,
        error: 'Internal server error',
        message: error instanceof Error ? error.message : 'Unknown error',
        timestamp: new Date().toISOString(),
      }),
    };
  }
};
