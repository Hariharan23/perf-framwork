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

export interface CmdbSearchPage {
  results: CmdbSearchResult[];
  hasMore: boolean;
  offset:  number;
  limit:   number;
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

export interface CmdbCiLifecycle {
  sysId: string;
  ciClass: string;
  name: string;
  operationalStatus: string;
  installStatus: string;
  lifecycleStage: string;
  lifecycleStatus: string;
  updatedAt: string;
}

export interface CiChangeRecord {
  number:      string;  // CHG0012345
  description: string;
  state:       string;  // Scheduled | Open | Work in Progress
  type:        string;  // Normal | Standard | Emergency
  startDate:   string;
  endDate:     string;
  assignedTo:  string;
}

export interface CiIncidentRecord {
  number:      string;  // INC0012345
  description: string;
  state:       string;
  priority:    string;  // 1-Critical … 4-Low
  openedAt:    string;
}

export interface CiScheduleResult {
  sysId:     string;
  changes:   CiChangeRecord[];
  incidents: CiIncidentRecord[];
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
   * Search CIs by keyword across all configured CI classes with pagination.
   * Distributes limit across classes proportionally; uses sysparm_offset for
   * server-side paging within each class when offset > 0.
   */
  async search(query: string, offset = 0, limit = 20): Promise<CmdbSearchPage> {
    const safeLimit  = Math.min(Math.max(limit, 1), 50);
    const classes    = this.config.ciClasses;
    const perClass   = Math.ceil(safeLimit / classes.length);
    // For offset, distribute proportionally: each class owns a slice of perClass rows
    const classOffset = Math.floor(offset / classes.length);

    const results: CmdbSearchResult[] = [];
    const snQuery = `nameCONTAINS${query}^ORshort_descriptionCONTAINS${query}`;

    for (const ciClass of classes) {
      if (results.length >= safeLimit) break;
      const remaining = safeLimit - results.length;
      const fetchLimit = Math.min(perClass + 1, remaining + 1); // fetch +1 to detect hasMore
      const url = `/api/now/table/${ciClass}`
        + `?sysparm_query=${encodeURIComponent(snQuery)}`
        + `&sysparm_limit=${fetchLimit}`
        + `&sysparm_offset=${classOffset}`
        + `&sysparm_fields=sys_id,name,short_description`
        + `&sysparm_display_value=true`;

      let resp: any;
      try {
        resp = await this.http.get(url);
      } catch (err: any) {
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
    }

    const hasMore = results.length > safeLimit;
    return {
      results: results.slice(0, safeLimit),
      hasMore,
      offset,
      limit: safeLimit,
    };
  }

  /**
   * Fetch scheduled maintenance windows (change requests) and active incidents for a CI.
   * Fires two parallel ServiceNow Table API requests to minimise latency.
   */
  async getCiSchedule(sysId: string): Promise<CiScheduleResult> {
    const chgQuery   = `cmdb_ci=${sysId}^stateIN-1,1,2^ORDERBYstart_date`;
    const incQuery   = `cmdb_ci=${sysId}^active=true^ORDERBYpriority`;
    const chgFields  = 'number,short_description,state,type,start_date,end_date,assigned_to';
    const incFields  = 'number,short_description,state,priority,opened_at';

    const [chgResp, incResp] = await Promise.allSettled([
      this.http.get(`/api/now/table/change_request`
        + `?sysparm_query=${encodeURIComponent(chgQuery)}`
        + `&sysparm_limit=10&sysparm_fields=${chgFields}&sysparm_display_value=true`),
      this.http.get(`/api/now/table/incident`
        + `?sysparm_query=${encodeURIComponent(incQuery)}`
        + `&sysparm_limit=10&sysparm_fields=${incFields}&sysparm_display_value=true`),
    ]);

    const changes: CiChangeRecord[] = [];
    if (chgResp.status === 'fulfilled') {
      for (const r of (chgResp.value.data?.result || [])) {
        changes.push({
          number:      extractDisplayValue(r.number),
          description: extractDisplayValue(r.short_description),
          state:       extractDisplayValue(r.state),
          type:        extractDisplayValue(r.type),
          startDate:   extractDisplayValue(r.start_date),
          endDate:     extractDisplayValue(r.end_date),
          assignedTo:  extractDisplayValue(r.assigned_to),
        });
      }
    }

    const incidents: CiIncidentRecord[] = [];
    if (incResp.status === 'fulfilled') {
      for (const r of (incResp.value.data?.result || [])) {
        incidents.push({
          number:      extractDisplayValue(r.number),
          description: extractDisplayValue(r.short_description),
          state:       extractDisplayValue(r.state),
          priority:    extractDisplayValue(r.priority),
          openedAt:    extractDisplayValue(r.opened_at),
        });
      }
    }

    return { sysId, changes, incidents };
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

  /** Read only the fields needed to validate a durable EMS↔CI link. */
  async fetchCiLifecycle(sysId: string, ciClass: string): Promise<CmdbCiLifecycle> {
    const fields = 'sys_id,name,operational_status,install_status,life_cycle_stage,life_cycle_stage_status,sys_updated_on';
    const resp = await this.http.get(`/api/now/table/${ciClass}/${sysId}?sysparm_fields=${fields}&sysparm_display_value=true`);
    const r = resp.data?.result;
    if (!r) throw new Error(`CI not found: sysId=${sysId}, class=${ciClass}`);
    return {
      sysId, ciClass,
      name: extractDisplayValue(r.name),
      operationalStatus: extractDisplayValue(r.operational_status),
      installStatus: extractDisplayValue(r.install_status),
      lifecycleStage: extractDisplayValue(r.life_cycle_stage),
      lifecycleStatus: extractDisplayValue(r.life_cycle_stage_status),
      updatedAt: extractDisplayValue(r.sys_updated_on),
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

  // ── Service Map helpers ───────────────────────────────────────────────────

  /** List all cmdb_rel_type records so the approver UI can pick one. */
  async getRelTypes(): Promise<Array<{ sysId: string; name: string }>> {
    const resp = await this.http.get(
      `/api/now/table/cmdb_rel_type?sysparm_fields=sys_id,name&sysparm_limit=200&sysparm_query=ORDERBYname`,
    );
    return (resp.data?.result || []).map((r: any) => ({
      sysId: extractDisplayValue(r.sys_id),
      name:  extractDisplayValue(r.name),
    }));
  }

  /**
   * Fetch all cmdb_rel_ci rows that involve any of the given CI sys_ids
   * (either as parent or child). Optionally filter by rel type sys_id.
   */
  async getCmdbRelCiForSysIds(
    sysIds: string[],
    relTypeSysId?: string,
  ): Promise<Array<{
    snRelSysId:   string;
    parentSysId:  string;
    childSysId:   string;
    relTypeSysId: string;
    relTypeName:  string;
  }>> {
    if (sysIds.length === 0) return [];
    let query = `parentIN${sysIds.join(',')}^ORchildIN${sysIds.join(',')}`;
    if (relTypeSysId) query += `^type=${relTypeSysId}`;
    const resp = await this.http.get(
      `/api/now/table/cmdb_rel_ci`
        + `?sysparm_query=${encodeURIComponent(query)}`
        + `&sysparm_fields=sys_id,parent,child,type`
        + `&sysparm_display_value=true`
        + `&sysparm_limit=1000`,
    );
    return (resp.data?.result || []).map((r: any) => ({
      snRelSysId:   extractDisplayValue(r.sys_id),
      parentSysId:  typeof r.parent === 'object' ? (r.parent?.value || '') : String(r.parent || ''),
      childSysId:   typeof r.child  === 'object' ? (r.child?.value  || '') : String(r.child  || ''),
      relTypeSysId: typeof r.type   === 'object' ? (r.type?.value   || '') : String(r.type   || ''),
      relTypeName:  typeof r.type   === 'object' ? (r.type?.display_value || '') : '',
    }));
  }

  /** Create a single cmdb_rel_ci record and return its new sys_id. */
  async createCmdbRelCi(parentSysId: string, childSysId: string, relTypeSysId: string): Promise<string> {
    const resp = await this.http.post('/api/now/table/cmdb_rel_ci', {
      parent: parentSysId,
      child:  childSysId,
      type:   relTypeSysId,
    });
    return extractDisplayValue(resp.data?.result?.sys_id);
  }

  /** Delete a cmdb_rel_ci record by its sys_id. */
  async deleteCmdbRelCi(snRelSysId: string): Promise<void> {
    await this.http.delete(`/api/now/table/cmdb_rel_ci/${snRelSysId}`);
  }
}
