#!/usr/bin/env npx ts-node
/**
 * remove-nonurl-configs.ts
 *
 * Removes all non-URL config_ triples from Neptune, including history nodes.
 * A config_ triple is considered "non-URL" if its value does not match
 * http(s):// or ftp:// patterns.
 *
 * Usage:
 *   NEPTUNE_ENDPOINT=<host> AWS_REGION=us-east-1 npx ts-node scripts/remove-nonurl-configs.ts
 *
 * Add --dry-run to preview without deleting.
 * Add --batch-size=N to control triples deleted per SPARQL update (default 200).
 */

import { HttpRequest } from '@aws-sdk/protocol-http';
import { SignatureV4 } from '@aws-sdk/signature-v4';
import { defaultProvider } from '@aws-sdk/credential-provider-node';
import { Sha256 } from '@aws-crypto/sha256-js';
import axios from 'axios';

// ── Config ─────────────────────────────────────────────────────────────────

const NEPTUNE_ENDPOINT = process.env.NEPTUNE_ENDPOINT || '';
const REGION           = process.env.AWS_REGION || 'us-east-1';
const ONTOLOGY_PREFIX  = 'http://neptune.aws.com/envmgmt/ontology/';
const URL_PATTERN      = /^(https?|ftp):\/\//i;
const DRY_RUN          = process.argv.includes('--dry-run');
const BATCH_SIZE       = parseInt(
  (process.argv.find(a => a.startsWith('--batch-size=')) || '--batch-size=200').split('=')[1],
  10,
);

if (!NEPTUNE_ENDPOINT) {
  console.error('ERROR: NEPTUNE_ENDPOINT environment variable is required.');
  process.exit(1);
}

// ── SPARQL client ──────────────────────────────────────────────────────────

const signer = new SignatureV4({
  credentials: defaultProvider(),
  region: REGION,
  service: 'neptune-db',
  sha256: Sha256,
});

async function sparqlQuery(query: string): Promise<any> {
  const body = `query=${encodeURIComponent(query)}`;
  const req = new HttpRequest({
    method: 'POST',
    protocol: 'https:',
    hostname: NEPTUNE_ENDPOINT,
    port: 8182,
    path: '/sparql',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      'Accept': 'application/sparql-results+json',
      'Host': `${NEPTUNE_ENDPOINT}:8182`,
    },
    body,
  });
  const signed = await signer.sign(req);
  const res = await axios.post(
    `https://${NEPTUNE_ENDPOINT}:8182/sparql`,
    body,
    { headers: { ...signed.headers, 'Content-Length': String(body.length) }, timeout: 60000 },
  );
  if (res.status >= 400) throw new Error(`Query failed ${res.status}: ${JSON.stringify(res.data)}`);
  return res.data;
}

async function sparqlUpdate(update: string): Promise<void> {
  const body = `update=${encodeURIComponent(update)}`;
  const req = new HttpRequest({
    method: 'POST',
    protocol: 'https:',
    hostname: NEPTUNE_ENDPOINT,
    port: 8182,
    path: '/sparql',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      'Accept': 'application/sparql-results+json',
      'Host': `${NEPTUNE_ENDPOINT}:8182`,
    },
    body,
  });
  const signed = await signer.sign(req);
  const res = await axios.post(
    `https://${NEPTUNE_ENDPOINT}:8182/sparql`,
    body,
    { headers: { ...signed.headers, 'Content-Length': String(body.length) }, timeout: 60000 },
  );
  if (res.status >= 400) throw new Error(`Update failed ${res.status}: ${JSON.stringify(res.data)}`);
}

// ── Main ───────────────────────────────────────────────────────────────────

interface Triple { subject: string; predicate: string; value: string; }

async function fetchAllNonUrlConfigTriples(): Promise<Triple[]> {
  // Fetch in pages of 1000 using OFFSET/LIMIT since Neptune caps at 1000 rows
  const triples: Triple[] = [];
  let offset = 0;
  const PAGE = 1000;

  console.log('Scanning Neptune for non-URL config_ triples (this may take a moment)...');

  while (true) {
    const query = `
      SELECT ?s ?p ?val WHERE {
        ?s ?p ?val .
        FILTER(STRSTARTS(STR(?p), "${ONTOLOGY_PREFIX}config_"))
        FILTER(!STRSTARTS(STR(?p), "${ONTOLOGY_PREFIX}config_bp_"))
        FILTER(!STRSTARTS(STR(?p), "${ONTOLOGY_PREFIX}config_bpBy_"))
        FILTER(!STRSTARTS(STR(?p), "${ONTOLOGY_PREFIX}config_bpOn_"))
        FILTER(!STRSTARTS(STR(?p), "${ONTOLOGY_PREFIX}config_orphan"))
        FILTER(!STRSTARTS(STR(?p), "${ONTOLOGY_PREFIX}config_previouslyConnected"))
        FILTER(!STRSTARTS(STR(?p), "${ONTOLOGY_PREFIX}config_sourceEnvName"))
        FILTER(!STRSTARTS(STR(?p), "${ONTOLOGY_PREFIX}config_discoveredHostname"))
      }
      ORDER BY ?s ?p
      LIMIT ${PAGE} OFFSET ${offset}
    `;

    const result = await sparqlQuery(query);
    const bindings: any[] = result.results?.bindings || [];

    for (const b of bindings) {
      const value = b.val?.value || '';
      if (!URL_PATTERN.test(value)) {
        triples.push({
          subject:   b.s?.value  || '',
          predicate: b.p?.value  || '',
          value,
        });
      }
    }

    console.log(`  Page offset=${offset}: ${bindings.length} rows fetched, ${triples.length} non-URL triples collected so far`);

    if (bindings.length < PAGE) break;
    offset += PAGE;
  }

  return triples;
}

async function deleteInBatches(triples: Triple[]): Promise<void> {
  let deleted = 0;
  for (let i = 0; i < triples.length; i += BATCH_SIZE) {
    const batch = triples.slice(i, i + BATCH_SIZE);
    let deleteData = '';
    for (const t of batch) {
      // Value may be a URI binding (type=uri) or literal — handle both
      const obj = t.value.startsWith('http') ? `<${t.value}>` : `"${t.value.replace(/\\/g, '\\\\').replace(/"/g, '\\"')}"`;
      deleteData += `  <${t.subject}> <${t.predicate}> ${obj} .\n`;
    }
    const updateQuery = `DELETE DATA {\n${deleteData}}`;

    if (DRY_RUN) {
      console.log(`  [DRY-RUN] Would delete batch ${i / BATCH_SIZE + 1} (${batch.length} triples)`);
    } else {
      await sparqlUpdate(updateQuery);
      deleted += batch.length;
      console.log(`  Deleted batch ${Math.floor(i / BATCH_SIZE) + 1} (${batch.length} triples) — total deleted: ${deleted}`);
    }
  }
}

async function main() {
  console.log(`Neptune endpoint : ${NEPTUNE_ENDPOINT}`);
  console.log(`Region           : ${REGION}`);
  console.log(`Dry run          : ${DRY_RUN}`);
  console.log(`Batch size       : ${BATCH_SIZE}`);
  console.log('');

  const triples = await fetchAllNonUrlConfigTriples();

  if (triples.length === 0) {
    console.log('\nNo non-URL config triples found. Nothing to do.');
    return;
  }

  console.log(`\nFound ${triples.length} non-URL config triples to remove.`);

  if (DRY_RUN) {
    console.log('\nSample (first 20):');
    triples.slice(0, 20).forEach(t => {
      const pred = t.predicate.replace(ONTOLOGY_PREFIX, '');
      console.log(`  ${t.subject.split('/').pop()} | ${pred} = "${t.value}"`);
    });
    console.log('\nRe-run without --dry-run to delete.');
    return;
  }

  console.log('\nDeleting...');
  await deleteInBatches(triples);
  console.log(`\nDone. ${triples.length} non-URL config triples removed from Neptune.`);
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
