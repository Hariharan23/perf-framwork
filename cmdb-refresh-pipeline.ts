/**
 * CMDB Refresh Pipeline Lambda
 *
 * Triggered by EventBridge Scheduler (or manually via cmdb-trigger-refresh-now).
 * For every EMS entity that has a linked CI it:
 *   1. Re-fetches latest CI field values from ServiceNow
 *   2. Fetches scheduled maintenance windows + open change requests
 *   3. Writes refreshed data back to Neptune as meta_cmdb_* triples
 *   4. Applies stored field mappings to first-class EMS properties
 *   5. Writes run summary to DynamoDB (SRE-POC-cmdb-sync-runs table)
 *   6. Updates /ems/cmdb/refresh_last_run SSM parameter
 */

import { NeptuneSparqlClient } from '../shared/neptune-sparql-client';
import { ServiceNowAdapter, sanitiseCmdbKey } from '../shared/cmdb/servicenow-adapter';
import { loadCmdbConfig } from '../shared/cmdb/cmdb-config-loader';
import { DynamoDBClient, PutItemCommand } from '@aws-sdk/client-dynamodb';
import { SSMClient, PutParameterCommand } from '@aws-sdk/client-ssm';
import { randomUUID } from 'crypto';

const REGION         = process.env.AWS_REGION || 'us-east-1';
const RUNS_TABLE     = process.env.CMDB_SYNC_RUNS_TABLE || 'SRE-POC-cmdb-sync-runs';
/** Gap in ms between ServiceNow calls — avoids rate-limiting */
const INTER_CALL_MS  = parseInt(process.env.CMDB_INTER_CALL_MS || '200', 10);
/** TTL: 30 days */
const TTL_SECONDS    = 30 * 24 * 60 * 60;

const dynamoClient = new DynamoDBClient({ region: REGION });
const ssmClient    = new SSMClient({ region: REGION });
const neptune      = new NeptuneSparqlClient();

function sleep(ms: number) { return new Promise(r => setTimeout(r, ms)); }

export const handler = async (event: unknown): Promise<void> => {
  const runId    = randomUUID();
  const startTs  = new Date().toISOString();
  console.log(`CMDB refresh pipeline start runId=${runId}`, event);

  let succeeded = 0;
  let failed    = 0;
  const errors: string[] = [];

  try {
    const config  = await loadCmdbConfig();
    const adapter = new ServiceNowAdapter(config);

    // ── 1. Get all linked entities from Neptune ───────────────────────────
    const entities = await neptune.listLinkedCmdbEntities();
    console.log(`Found ${entities.length} linked entities`);

    for (const entity of entities) {
      try {
        // ── 2a. Fetch fresh CI field values ─────────────────────────────
        const ciDetail = await adapter.fetchCiDetail(entity.sysId, entity.ciClass);

        // ── 2b. Fetch maintenance schedule + open changes ────────────────
        await sleep(INTER_CALL_MS);
        const schedule = await adapter.getCiSchedule(entity.sysId);

        // Build field map from selected stored fields
        const existingProps = await neptune.getCmdbProperties(entity.entityId);
        const selectionRaw  = existingProps['field_selection'] || '';
        const selectedKeys: string[] = selectionRaw
          ? selectionRaw.split(',').map((s: string) => s.trim()).filter(Boolean)
          : Object.keys(ciDetail.fields).slice(0, 30); // fallback: first 30 fields

        const fieldsToWrite: Record<string, string> = {};
        for (const k of selectedKeys) {
          if (ciDetail.fields[k] !== undefined) fieldsToWrite[k] = ciDetail.fields[k];
        }

        // ── 2c. Append schedule info as JSON ─────────────────────────────
        fieldsToWrite['schedule_info'] = JSON.stringify({
          fetchedAt: new Date().toISOString(),
          changes:   schedule.changes,
          incidents: schedule.incidents,
        });
        fieldsToWrite['last_synced_at'] = new Date().toISOString();
        fieldsToWrite['sync_status']    = 'ok';

        // ── 3. Write to Neptune ──────────────────────────────────────────
        await neptune.writeCmdbTriples(entity.entityId, fieldsToWrite);

        // ── 4. Apply stored field mappings ───────────────────────────────
        const mapping: Record<string, string> = {};
        for (const [k, v] of Object.entries(existingProps)) {
          if (k.startsWith('field_map_')) mapping[k.slice('field_map_'.length)] = v;
        }
        if (Object.keys(mapping).length > 0) {
          await neptune.applyFieldMappings(entity.entityId, mapping, fieldsToWrite);
        }

        succeeded++;
        console.log(`Refreshed entity=${entity.entityId} ci=${entity.sysId} changes=${schedule.changes.length} incidents=${schedule.incidents.length}`);
      } catch (e: any) {
        failed++;
        const msg = `entity=${entity.entityId}: ${e?.message || e}`;
        errors.push(msg);
        console.error('CMDB refresh error:', msg);

        // Write error status to Neptune so UI can show it
        try {
          await neptune.writeCmdbTriples(entity.entityId, {
            sync_status:    'error',
            last_synced_at: new Date().toISOString(),
          });
        } catch { /* best-effort */ }
      }

      await sleep(INTER_CALL_MS);
    }
  } catch (fatal: any) {
    console.error('CMDB refresh pipeline fatal error:', fatal);
    failed++;
    errors.push(`fatal: ${fatal?.message || fatal}`);
  }

  const endTs = new Date().toISOString();
  const ttl   = Math.floor(Date.now() / 1000) + TTL_SECONDS;

  // ── 5. Write run summary to DynamoDB ─────────────────────────────────────
  try {
    await dynamoClient.send(new PutItemCommand({
      TableName: RUNS_TABLE,
      Item: {
        runId:     { S: runId },
        startedAt: { S: startTs },
        endedAt:   { S: endTs },
        succeeded: { N: String(succeeded) },
        failed:    { N: String(failed) },
        errors:    { S: JSON.stringify(errors.slice(0, 20)) },
        ttl:       { N: String(ttl) },
      },
    }));
  } catch (e) {
    console.error('Failed to write run record to DynamoDB:', e);
  }

  // ── 6. Update last-run SSM param ─────────────────────────────────────────
  try {
    await ssmClient.send(new PutParameterCommand({
      Name:      '/ems/cmdb/refresh_last_run',
      Value:     JSON.stringify({ runId, startedAt: startTs, endedAt: endTs, succeeded, failed }),
      Type:      'String',
      Overwrite: true,
    }));
  } catch (e) {
    console.error('Failed to update refresh_last_run SSM param:', e);
  }

  console.log(`CMDB refresh pipeline complete runId=${runId} succeeded=${succeeded} failed=${failed}`);
};
