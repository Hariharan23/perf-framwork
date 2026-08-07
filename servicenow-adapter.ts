/**
 * ServiceNow CMDB Adapter
 *
 * Connects to ServiceNow Table REST API to search CIs, fetch field definitions,
 * and retrieve CI field values. All configuration is passed in via CmdbConfig
 * loaded from SSM / Secrets Manager at Lambda cold-start.
 */

import axios, { AxiosInstance } from 'axios';

// ── Configuration ─────────────────────────────────────────────────────────────

export interface CmdbConfig {
  instanceUrl: string;          // e.g. https://company.service-now.com
  username: string;
  password: string;
  ciClasses: string[];          // e.g. ['cmdb_ci_appl', 'cmdb_ci_server']
  timeoutMs: number;
  searchLimit: number;
}

// ── Shape types ───────────────────────────────────────────────────────────────

export interface CmdbSearchResult {
  sysId:   string;
  name:    string;
  ciClass: string;
  summary: string;  // short_description
}

export interface CmdbFieldDefinition {
  key:          string;
  label:        string;
  type:         string;   // string | date | boolean | integer | reference
  category:     string;   // Ownership | Support | Lifecycle | Technical | Other
  recommended:  boolean;
  currentValue: string;
}

export interface CmdbCiDetail {
  sysId:   string;
  name:    string;
  ciClass: string;
  fields:  Record<string, string>;  // fieldKey → display value
}

export interface ConnectionTestResult {
  ok:      boolean;
  message: string;
}

// ── Field category hints ──────────────────────────────────────────────────────
// Fields matching these name prefixes/patterns are placed into specific categories.

const OWNERSHIP_FIELDS = new Set([
  'owned_by', 'managed_by', 'assigned_to', 'support_group',
  'change_control', 'business_unit', 'department', 'company',
  'it_owner', 'it_owner_manager', 'vendor',
]);

const SUPPORT_FIELDS = new Set([
  'support_group', 'support_tier', 'maintenance_schedule',
  'contract', 'service_level_agreement', 'sla',
]);

const LIFECYCLE_FIELDS = new Set([
  'operational_status', 'lifecycle_status', 'install_date',
  'due_date', 'decommission_date', 'end_of_life', 'purchase_date',
  'warranty_expiration', 'last_reviewed',
]);

const TECHNICAL_FIELDS = new Set([
  'version', 'os', 'os_version', 'ip_address', 'mac_address',
  'serial_number', 'asset_tag', 'hardware_type', 'cpu_type',
  'disk_space', 'ram', 'environment_type', 'cluster_id',
]);

// Fields recommended by default in the UI field-selection step
const RECOMMENDED_FIELDS = new Set([
  'name', 'short_description', 'owned_by', 'support_group',
  'operational_status', 'it_owner', 'department', 'support_tier',
  'business_unit', 'version', 'lifecycle_status',
]);

// Fields to always skip — internal ServiceNow metadata not useful in EMS
const SKIP_FIELDS = new Set([
  'sys_id', 'sys_created_by', 'sys_created_on', 'sys_updated_by',
  'sys_updated_on', 'sys_class_name', 'sys_mod_count', 'sys_domain',
  'sys_domain_path', 'sys_tags',
]);

// ── Helpers ───────────────────────────────────────────────────────────────────

/**
 * Sanitise a CMDB field key for use as a Neptune property name suffix.
 * Result is lowercase, spaces/hyphens → underscore, non-alphanumeric stripped.
 */
export function sanitiseCmdbKey(key: string): string {
  return key
    .toLowerCase()
    .replace(/[\s\-]/g, '_')
    .replace(/[^a-z0-9_]/g, '')
    .slice(0, 60);
}

function categoryForField(key: string): string {
  if (OWNERSHIP_FIELDS.has(key))  return 'Ownership';
  if (SUPPORT_FIELDS.has(key))    return 'Support';
  if (LIFECYCLE_FIELDS.has(key))  return 'Lifecycle';
  if (TECHNICAL_FIELDS.has(key))  return 'Technical';
  return 'Other';
}

/**
 * Extract a display-safe string value from a ServiceNow field binding.
 * Reference fields come as { value: sysId, display_value: "Name" }.
 */
function extractDisplayValue(raw: any): string {
  if (raw === null || raw === undefined) return '';
  if (typeof raw === 'object') {
    if (raw.display_value !== undefined && raw.display_value !== null) {
      return String(raw.display_value);
    }
    if (raw.value !== undefined && raw.value !== null) {
      return String(raw.value);
    }
    return '';
  }
  return String(raw);
}

// ── ServiceNow Adapter ────────────────────────────────────────────────────────

export class ServiceNowAdapter {
  private readonly http: AxiosInstance;
  private readonly config: CmdbConfig;

  constructor(config: CmdbConfig) {
    this.config = config;
    this.http = axios.create({
      baseURL:  config.instanceUrl,
      timeout:  config.timeoutMs,
      auth: { username: config.username, password: config.password },
      headers: {
        'Accept':       'application/json',
        'Content-Type': 'application/json',
        // Request display_value for reference fields alongside raw value
        'X-WantDisplay': 'true',
      },
    });
  }

  /**
   * Verify connectivity to ServiceNow by fetching one record from cmdb_ci.
   */
  async testConnection(): Promise<ConnectionTestResult> {
    try {
      const url = `/api/now/table/cmdb_ci?sysparm_limit=1&sysparm_fields=sys_id`;
      await this.http.get(url);
      return { ok: true, message: `Connected to ${this.config.instanceUrl}` };
    } catch (err: any) {
      const msg = err?.response?.data?.error?.message || err?.message || 'Unknown error';
      return { ok: false, message: msg };
    }
  }

  /**
   * Search CIs by keyword across all configured CI classes.
   * Returns up to config.searchLimit results.
   */
  async search(query: string): Promise<CmdbSearchResult[]> {
    const results: CmdbSearchResult[] = [];
    const escaped = encodeURIComponent(query.replace(/'/g, "\\'"));
    const limit    = Math.min(this.config.searchLimit, 50);

    for (const ciClass of this.config.ciClasses) {
      // ServiceNow encoded query: name CONTAINS <term> OR short_description CONTAINS <term>
      const snQuery = `nameCONTAINS${query}^ORshort_descriptionCONTAINS${query}`;
      const url = `/api/now/table/${ciClass}`
        + `?sysparm_query=${encodeURIComponent(snQuery)}`
        + `&sysparm_limit=${limit}`
        + `&sysparm_fields=sys_id,name,short_description`
        + `&sysparm_display_value=true`;

      let resp: any;
      try {
        resp = await this.http.get(url);
      } catch (err: any) {
        // Skip unreachable CI class rather than failing entire search
        console.warn(`ServiceNow search: skipping class ${ciClass}:`, err?.response?.status || err?.message);
        continue;
      }

      for (const record of (resp.data?.result || [])) {
        results.push({
          sysId:   extractDisplayValue(record.sys_id),
          name:    extractDisplayValue(record.name),
          ciClass,
          summary: extractDisplayValue(record.short_description),
        });
      }

      if (results.length >= limit) break;
    }

    return results.slice(0, limit);
  }

  /**
   * Fetch full field values for a specific CI.
   * Uses sysparm_display_value=all so both raw values and display labels are returned.
   */
  async fetchCiDetail(sysId: string, ciClass: string): Promise<CmdbCiDetail> {
    const url = `/api/now/table/${ciClass}/${sysId}`
      + `?sysparm_display_value=all`
      + `&sysparm_exclude_reference_link=true`;

    const resp = await this.http.get(url);
    const record = resp.data?.result;

    if (!record) {
      throw new Error(`CI not found: sysId=${sysId}, class=${ciClass}`);
    }

    const fields: Record<string, string> = {};
    for (const [key, val] of Object.entries(record)) {
      if (SKIP_FIELDS.has(key)) continue;
      const display = extractDisplayValue(val);
      if (display !== '') {
        fields[sanitiseCmdbKey(key)] = display;
      }
    }

    return {
      sysId,
      name:    fields['name'] || sysId,
      ciClass,
      fields,
    };
  }

  /**
   * Return field definitions for a CI class.
   * Fetches from sys_dictionary to get labels and types.
   * Merges with live values fetched for the specific sysId.
   */
  async getFieldDefinitions(ciClass: string, sysId: string): Promise<CmdbFieldDefinition[]> {
    // Fetch schema from sys_dictionary for this table
    const dictUrl = `/api/now/table/sys_dictionary`
      + `?sysparm_query=name=${ciClass}^active=true^internal_type!=collection`
      + `&sysparm_fields=element,column_label,internal_type`
      + `&sysparm_limit=200`;

    // Fetch live field values for the specific CI in parallel
    const [dictResp, ciDetail] = await Promise.all([
      this.http.get(dictUrl).catch(() => ({ data: { result: [] } })),
      this.fetchCiDetail(sysId, ciClass).catch(() => ({ fields: {} as Record<string, string> })),
    ]);

    const liveValues = (ciDetail as CmdbCiDetail).fields;
    const definitions: CmdbFieldDefinition[] = [];
    const seen = new Set<string>();

    for (const col of (dictResp.data?.result || [])) {
      const rawKey = (col.element || '').toLowerCase();
      if (!rawKey || SKIP_FIELDS.has(rawKey)) continue;

      const key = sanitiseCmdbKey(rawKey);
      if (seen.has(key)) continue;
      seen.add(key);

      definitions.push({
        key,
        label:        extractDisplayValue(col.column_label) || key,
        type:         extractDisplayValue(col.internal_type) || 'string',
        category:     categoryForField(rawKey),
        recommended:  RECOMMENDED_FIELDS.has(rawKey),
        currentValue: liveValues[key] || '',
      });
    }

    // Sort: recommended first, then alphabetically within category
    definitions.sort((a, b) => {
      if (a.recommended !== b.recommended) return a.recommended ? -1 : 1;
      if (a.category !== b.category) return a.category.localeCompare(b.category);
      return a.label.localeCompare(b.label);
    });

    return definitions;
  }
}
