/**
 * remove-nonurl-configs Lambda
 *
 * One-shot maintenance handler that removes all non-URL config_ triples from
 * Neptune (including history / any node type).  Invoke via API Gateway or
 * directly from the Lambda console.
 *
 * POST /maintenance   body: { "operation": "remove-nonurl-configs", "dryRun": true|false }
 *
 * Response:
 *   { scanned, removed, skipped, batches, dryRun, details[] }
 */

import { APIGatewayProxyEvent, APIGatewayProxyResult, Context } from 'aws-lambda';
import { NeptuneSparqlClient } from '../shared/neptune-sparql-client';

const ONTOLOGY_PREFIX = 'http://neptune.aws.com/envmgmt/ontology/';
const URL_PATTERN     = /^(https?|ftp):\/\//i;
const PAGE_SIZE       = 1000;
const BATCH_SIZE      = 200;   // triples deleted per SPARQL UPDATE

// config_ prefixes that must NEVER be deleted
const KEEP_PREFIXES = [
  `${ONTOLOGY_PREFIX}config_bp_`,
  `${ONTOLOGY_PREFIX}config_bpBy_`,
  `${ONTOLOGY_PREFIX}config_bpOn_`,
  `${ONTOLOGY_PREFIX}config_orphan`,
  `${ONTOLOGY_PREFIX}config_previouslyConnected`,
  `${ONTOLOGY_PREFIX}config_sourceEnvName`,
  `${ONTOLOGY_PREFIX}config_discoveredHostname`,
  `${ONTOLOGY_PREFIX}config_discoveredBy`,
];

function shouldKeep(predicate: string): boolean {
  return KEEP_PREFIXES.some(p => predicate.startsWith(p));
}

const headers = {
  'Content-Type': 'application/json',
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, x-api-key',
};

export const handler = async (
  event: APIGatewayProxyEvent,
  _ctx: Context,
): Promise<APIGatewayProxyResult> => {
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers, body: '' };
  }

  const body = event.body ? JSON.parse(event.body) : {};
  const operation = body.operation || event.queryStringParameters?.operation;

  if (operation !== 'remove-nonurl-configs') {
    return {
      statusCode: 400,
      headers,
      body: JSON.stringify({
        error: 'Missing or unknown operation',
        usage: 'POST { "operation": "remove-nonurl-configs", "dryRun": true }',
      }),
    };
  }

  const dryRun: boolean = body.dryRun !== false && body.dryRun !== 'false'; // default true (safe)

  const neptuneEndpoint = process.env.NEPTUNE_ENDPOINT!;
  const region          = process.env.AWS_REGION || 'us-east-1';

  // We need raw SPARQL access — reuse NeptuneSparqlClient's public executeSparqlQuery/Update
  // by calling through the shared client.  For scanning we issue direct queries via the
  // client's public interface; for bulk deletes we build DELETE DATA queries.
  const client = new NeptuneSparqlClient(neptuneEndpoint, region);

  // ── Step 1: Scan all config_ triples, page by page ──────────────────────
  const toDelete: Array<{ subject: string; predicate: string; value: string; valueType: 'uri' | 'literal' }> = [];
  let offset = 0;
  let scanned = 0;

  console.log(`[remove-nonurl-configs] starting scan dryRun=${dryRun}`);

  while (true) {
    const scanQuery = `
      SELECT ?s ?p ?val WHERE {
        ?s ?p ?val .
        FILTER(STRSTARTS(STR(?p), "${ONTOLOGY_PREFIX}config_"))
      }
      ORDER BY ?s ?p
      LIMIT ${PAGE_SIZE} OFFSET ${offset}
    `;

    const result = await (client as any).executeSparqlQuery(scanQuery);
    const bindings: any[] = result.results?.bindings || [];
    scanned += bindings.length;

    for (const b of bindings) {
      const pred  = b.p?.value  || '';
      const value = b.val?.value || '';
      const vtype: 'uri' | 'literal' = b.val?.type === 'uri' ? 'uri' : 'literal';

      if (shouldKeep(pred)) continue;
      if (URL_PATTERN.test(value)) continue;

      toDelete.push({ subject: b.s?.value || '', predicate: pred, value, valueType: vtype });
    }

    console.log(`[remove-nonurl-configs] page offset=${offset} fetched=${bindings.length} toDelete=${toDelete.length}`);

    if (bindings.length < PAGE_SIZE) break;
    offset += PAGE_SIZE;
  }

  console.log(`[remove-nonurl-configs] scan complete scanned=${scanned} toDelete=${toDelete.length}`);

  // ── Step 2: Delete in batches ────────────────────────────────────────────
  let removed = 0;
  let batches  = 0;

  if (!dryRun && toDelete.length > 0) {
    for (let i = 0; i < toDelete.length; i += BATCH_SIZE) {
      const batch = toDelete.slice(i, i + BATCH_SIZE);
      let deleteData = '';
      for (const t of batch) {
        const esc  = (s: string) => s.replace(/\\/g, '\\\\').replace(/"/g, '\\"');
        const obj  = t.valueType === 'uri' ? `<${t.value}>` : `"${esc(t.value)}"`;
        deleteData += `  <${t.subject}> <${t.predicate}> ${obj} .\n`;
      }
      const updateQuery = `DELETE DATA {\n${deleteData}}`;
      await (client as any).executeSparqlUpdate(updateQuery);
      removed += batch.length;
      batches++;
      console.log(`[remove-nonurl-configs] batch ${batches} deleted ${batch.length} triples (total=${removed})`);
    }
  }

  // ── Response ─────────────────────────────────────────────────────────────
  const details = toDelete.slice(0, 100).map(t => ({
    predicate: t.predicate.replace(ONTOLOGY_PREFIX, ''),
    value:     t.value.length > 80 ? t.value.slice(0, 80) + '…' : t.value,
  }));

  return {
    statusCode: 200,
    headers,
    body: JSON.stringify({
      dryRun,
      scanned,
      toRemove:  toDelete.length,
      removed:   dryRun ? 0 : removed,
      batches:   dryRun ? 0 : batches,
      message:   dryRun
        ? `DryRun: ${toDelete.length} non-URL config triples would be removed. Set dryRun:false to execute.`
        : `Removed ${removed} non-URL config triples in ${batches} batches.`,
      sampleDetails: details,
    }),
  };
};
