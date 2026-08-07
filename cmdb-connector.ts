/**
 * CMDB Connector Lambda
 *
 * Handles all CMDB ↔ EMS operations for ServiceNow integration.
 * Routes on the `operation` query/path parameter.
 *
 * Operations:
 *   GET  cmdb-test-connection          — verify ServiceNow connectivity
 *   GET  cmdb-search?q=<term>          — search CIs by keyword
 *   GET  cmdb-get-fields?sysId=&ciClass= — list available fields + live values
 *   POST cmdb-link                     — link a CI to an EMS entity + store selected fields
 *   POST cmdb-refresh                  — re-fetch selected fields from ServiceNow
 *   POST cmdb-update-fields            — add or remove selected fields without relinking
 *   POST cmdb-unlink                   — remove all CMDB data from an entity
 *   GET  cmdb-get-status?entityId=     — read current CMDB link state for an entity
 */

import { APIGatewayProxyEvent, APIGatewayProxyResult } from 'aws-lambda';
import { NeptuneSparqlClient } from '../shared/neptune-sparql-client';
import { ServiceNowAdapter, sanitiseCmdbKey } from '../shared/cmdb/servicenow-adapter';
import { loadCmdbConfig } from '../shared/cmdb/cmdb-config-loader';

// ── Module-level singletons (survive warm invocations) ──────────────────────

const neptuneClient = new NeptuneSparqlClient();

async function getAdapter(): Promise<ServiceNowAdapter> {
  const config = await loadCmdbConfig();
  return new ServiceNowAdapter(config);
}

// ── Response helpers ─────────────────────────────────────────────────────────

const CORS_HEADERS = {
  'Content-Type': 'application/json',
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization, x-api-key',
};

function ok(body: unknown): APIGatewayProxyResult {
  return { statusCode: 200, headers: CORS_HEADERS, body: JSON.stringify(body) };
}

function err(statusCode: number, message: string): APIGatewayProxyResult {
  return { statusCode, headers: CORS_HEADERS, body: JSON.stringify({ error: message }) };
}

function body<T>(event: APIGatewayProxyEvent): T {
  try {
    return JSON.parse(event.body || '{}') as T;
  } catch {
    throw new Error('Invalid JSON body');
  }
}

// ── Handler ──────────────────────────────────────────────────────────────────

export const handler = async (event: APIGatewayProxyEvent): Promise<APIGatewayProxyResult> => {
  console.log('CMDB event:', JSON.stringify({ method: event.httpMethod, path: event.path, qs: event.queryStringParameters }));

  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers: CORS_HEADERS, body: '' };
  }

  const operation = event.pathParameters?.operation
    || event.queryStringParameters?.operation
    || '';

  try {
    switch (operation) {

      // ── GET cmdb-test-connection ─────────────────────────────────────────
      case 'cmdb-test-connection': {
        const adapter = await getAdapter();
        const result  = await adapter.testConnection();
        return ok({ provider: 'servicenow', ...result });
      }

      // ── GET cmdb-search?q=<term> ─────────────────────────────────────────
      case 'cmdb-search': {
        const q = (event.queryStringParameters?.q || '').trim();
        if (!q) return err(400, 'Query parameter "q" is required');

        const adapter = await getAdapter();
        const results = await adapter.search(q);
        return ok({ results });
      }

      // ── GET cmdb-get-fields?sysId=&ciClass= ─────────────────────────────
      case 'cmdb-get-fields': {
        const { sysId, ciClass } = event.queryStringParameters || {};
        if (!sysId)   return err(400, 'Query parameter "sysId" is required');
        if (!ciClass) return err(400, 'Query parameter "ciClass" is required');

        const adapter = await getAdapter();
        const fields  = await adapter.getFieldDefinitions(ciClass, sysId);
        return ok({ sysId, ciClass, fields });
      }

      // ── POST cmdb-link ───────────────────────────────────────────────────
      case 'cmdb-link': {
        const {
          entityId, sysId, ciClass, ciName,
          selectedFields,  // string[] of field keys to pull
          linkedBy,
        } = body<{
          entityId: string; sysId: string; ciClass: string; ciName: string;
          selectedFields: string[]; linkedBy?: string;
        }>(event);

        if (!entityId)                               return err(400, '"entityId" is required');
        if (!sysId)                                  return err(400, '"sysId" is required');
        if (!ciClass)                                return err(400, '"ciClass" is required');
        if (!Array.isArray(selectedFields) || selectedFields.length === 0)
                                                     return err(400, '"selectedFields" must be a non-empty array');

        const adapter   = await getAdapter();
        const ciDetail  = await adapter.fetchCiDetail(sysId, ciClass);

        // Only store values for the user-selected fields
        const fieldsToStore: Record<string, string> = {};
        for (const key of selectedFields) {
          const sanitised = sanitiseCmdbKey(key);
          if (ciDetail.fields[sanitised] !== undefined) {
            fieldsToStore[sanitised] = ciDetail.fields[sanitised];
          }
        }

        const now = new Date().toISOString();

        // System / correlation fields always stored
        const systemFields: Record<string, string> = {
          provider:           'servicenow',
          ci_sys_id:          sysId,
          ci_class:           ciClass,
          ci_name:            ciName || ciDetail.name,
          linked_at:          now,
          linked_by:          linkedBy || 'unknown',
          last_synced_at:     now,
          field_selection:    JSON.stringify(selectedFields.map(sanitiseCmdbKey)),
        };

        await neptuneClient.writeCmdbTriples(entityId, { ...systemFields, ...fieldsToStore });

        return ok({
          entityId,
          sysId,
          ciClass,
          ciName: ciName || ciDetail.name,
          storedFieldCount: Object.keys(fieldsToStore).length,
          lastSyncedAt: now,
        });
      }

      // ── POST cmdb-refresh ────────────────────────────────────────────────
      case 'cmdb-refresh': {
        const { entityId } = body<{ entityId: string }>(event);
        if (!entityId) return err(400, '"entityId" is required');

        const existing = await neptuneClient.getCmdbProperties(entityId);
        const sysId    = existing['ci_sys_id'];
        const ciClass  = existing['ci_class'];

        if (!sysId || !ciClass) {
          return err(404, 'Entity has no linked CMDB CI. Use cmdb-link first.');
        }

        let selectedFields: string[] = [];
        try {
          selectedFields = JSON.parse(existing['field_selection'] || '[]');
        } catch {
          return err(500, 'Stored field_selection is not valid JSON');
        }

        if (selectedFields.length === 0) {
          return err(400, 'No fields selected. Use cmdb-update-fields to select fields first.');
        }

        const adapter   = await getAdapter();
        const ciDetail  = await adapter.fetchCiDetail(sysId, ciClass);

        const refreshed: Record<string, string> = {};
        for (const key of selectedFields) {
          if (ciDetail.fields[key] !== undefined) {
            refreshed[key] = ciDetail.fields[key];
          }
        }
        refreshed['last_synced_at'] = new Date().toISOString();

        await neptuneClient.writeCmdbTriples(entityId, refreshed);

        return ok({
          entityId,
          sysId,
          refreshedFieldCount: Object.keys(refreshed).length - 1,   // exclude last_synced_at
          lastSyncedAt: refreshed['last_synced_at'],
        });
      }

      // ── POST cmdb-update-fields ──────────────────────────────────────────
      case 'cmdb-update-fields': {
        const { entityId, selectedFields } = body<{ entityId: string; selectedFields: string[] }>(event);
        if (!entityId)                                           return err(400, '"entityId" is required');
        if (!Array.isArray(selectedFields) || selectedFields.length === 0)
                                                                 return err(400, '"selectedFields" must be a non-empty array');

        const existing = await neptuneClient.getCmdbProperties(entityId);
        const sysId    = existing['ci_sys_id'];
        const ciClass  = existing['ci_class'];
        if (!sysId || !ciClass) return err(404, 'Entity has no linked CMDB CI');

        let currentSelection: string[] = [];
        try {
          currentSelection = JSON.parse(existing['field_selection'] || '[]');
        } catch { /* empty */ }

        const newSelection  = selectedFields.map(sanitiseCmdbKey);
        const toAdd         = newSelection.filter(k => !currentSelection.includes(k));
        const toRemove      = currentSelection.filter(k => !newSelection.includes(k));

        // Remove deselected field triples
        if (toRemove.length > 0) {
          await neptuneClient.deleteCmdbFields(entityId, toRemove);
        }

        // Fetch + store newly added fields
        if (toAdd.length > 0) {
          const adapter  = await getAdapter();
          const ciDetail = await adapter.fetchCiDetail(sysId, ciClass);
          const toWrite: Record<string, string> = {};
          for (const key of toAdd) {
            if (ciDetail.fields[key] !== undefined) toWrite[key] = ciDetail.fields[key];
          }
          if (Object.keys(toWrite).length > 0) {
            await neptuneClient.writeCmdbTriples(entityId, toWrite);
          }
        }

        // Update field_selection metadata
        await neptuneClient.writeCmdbTriples(entityId, {
          field_selection:  JSON.stringify(newSelection),
          last_synced_at:   new Date().toISOString(),
        });

        return ok({
          entityId,
          added:   toAdd,
          removed: toRemove,
          currentSelection: newSelection,
        });
      }

      // ── POST cmdb-unlink ─────────────────────────────────────────────────
      case 'cmdb-unlink': {
        const { entityId, unlinkedBy } = body<{ entityId: string; unlinkedBy?: string }>(event);
        if (!entityId) return err(400, '"entityId" is required');

        await neptuneClient.deleteAllCmdbTriples(entityId);

        console.log(`cmdb-unlink: ${entityId} unlinked by ${unlinkedBy || 'unknown'}`);
        return ok({ entityId, unlinked: true });
      }

      // ── GET cmdb-get-status?entityId= ────────────────────────────────────
      case 'cmdb-get-status': {
        const entityId = event.queryStringParameters?.entityId;
        if (!entityId) return err(400, 'Query parameter "entityId" is required');

        const props = await neptuneClient.getCmdbProperties(entityId);

        if (!props['ci_sys_id']) {
          return ok({ entityId, linked: false });
        }

        // Separate system/correlation keys from user-imported field values
        const systemKeys = new Set([
          'provider', 'ci_sys_id', 'ci_class', 'ci_name',
          'linked_at', 'linked_by', 'last_synced_at', 'field_selection',
        ]);

        let selectedFields: string[] = [];
        try {
          selectedFields = JSON.parse(props['field_selection'] || '[]');
        } catch { /* ignore */ }

        const fieldValues: Record<string, string> = {};
        for (const [k, v] of Object.entries(props)) {
          if (!systemKeys.has(k)) fieldValues[k] = v;
        }

        return ok({
          entityId,
          linked:          true,
          provider:        props['provider']       || 'servicenow',
          sysId:           props['ci_sys_id'],
          ciClass:         props['ci_class'],
          ciName:          props['ci_name'],
          linkedAt:        props['linked_at'],
          linkedBy:        props['linked_by'],
          lastSyncedAt:    props['last_synced_at'],
          selectedFields,
          fieldCount:      selectedFields.length,
          fieldValues,
        });
      }

      default:
        return err(400, `Unknown operation: "${operation}". Valid operations: cmdb-test-connection, cmdb-search, cmdb-get-fields, cmdb-link, cmdb-refresh, cmdb-update-fields, cmdb-unlink, cmdb-get-status`);
    }

  } catch (e: any) {
    console.error('CMDB connector error:', e);
    const msg = e?.response?.data?.error?.message || e?.message || 'Internal error';
    const status = e?.response?.status === 401 ? 502 : 500;
    return err(status, msg);
  }
};
