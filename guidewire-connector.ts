// Guidewire Runtime Property Connector
// Connects to a Guidewire application (PolicyCenter, ClaimCenter, BillingCenter)
// and reads runtime properties via the Guidewire property API.
//
// Authentication flow:
//   1. POST tokenUrl with client_credentials grant → access_token
//   2. GET propertyApiUrl with Authorization: Bearer {token} → XML response
//   3. Parse XML: each <Property><Name/><Value/></Property> → PceRecord
//
// URL vs non-URL routing is handled downstream by classifyProperties() in url-parser.ts:
//   • URL values  (https?://...) → Integration child nodes in Neptune
//   • Non-URL values              → config attributes on the Environment node
//
// This connector is zero-dependency: HTTP calls use the built-in https/http module.

import * as https from 'https';
import * as http from 'http';
import { URL } from 'url';
import {
  DataSourceConnector,
  ConnectorConfig,
  FetchResult,
  ConnectorConfigSchema,
} from './base-connector';
import { PceRecord } from '../pce-csv-parser';

// ── HTTP helper ───────────────────────────────────────────────────────────────

interface HttpResponse {
  statusCode: number;
  body: string;
}

function httpRequest(
  urlStr: string,
  options: { method: 'GET' | 'POST'; headers?: Record<string, string>; body?: string },
): Promise<HttpResponse> {
  return new Promise((resolve, reject) => {
    const parsed  = new URL(urlStr);
    const isHttps = parsed.protocol === 'https:';
    const lib     = isHttps ? https : http;

    const reqOpts: https.RequestOptions = {
      hostname: parsed.hostname,
      port:     parsed.port || (isHttps ? 443 : 80),
      path:     parsed.pathname + (parsed.search || ''),
      method:   options.method,
      headers:  options.headers,
    };

    const req = lib.request(reqOpts, (res) => {
      const chunks: Buffer[] = [];
      res.on('data', (c: Buffer) => chunks.push(c));
      res.on('end', () =>
        resolve({
          statusCode: res.statusCode ?? 0,
          body: Buffer.concat(chunks).toString('utf-8'),
        }),
      );
      res.on('error', reject);
    });

    req.on('error', reject);
    if (options.body) req.write(options.body);
    req.end();
  });
}

// ── XML parser ────────────────────────────────────────────────────────────────

/**
 * Zero-dependency Guidewire property XML parser.
 *
 * Handles the format emitted by the Guidewire Runtime Property API:
 *   <Properties xmlns="http://guidewire.com/psc/properties">
 *     <Property>
 *       <Group>integration</Group>
 *       <Name>CCCAPDPortalURL_Ext</Name>
 *       <Description>CCC APD portal url for SSO</Description>
 *       <Value>https://test/sso/saml</Value>
 *     </Property>
 *     ...
 *   </Properties>
 *
 * Only <Name> and <Value> are extracted per record.
 * <Group> and <Description> are intentionally ignored — PceRecord has no extra fields
 * and the downstream classifyProperties() does not use them.
 *
 * Blocks missing <Name> or <Value> are skipped with a console warning.
 */
function parseGuidewireXml(xml: string, envName: string): PceRecord[] {
  const records: PceRecord[] = [];

  // Match each <Property>...</Property> block (DOTALL via [\s\S])
  const propBlockRe = /<Property\b[^>]*>([\s\S]*?)<\/Property>/gi;

  // Build a regex for a specific element tag within a block
  const tagContent = (tag: string, src: string): string | null => {
    const m = new RegExp(`<${tag}\\b[^>]*>([\\s\\S]*?)<\\/${tag}>`, 'i').exec(src);
    return m ? m[1].trim() : null;
  };

  let match: RegExpExecArray | null;
  while ((match = propBlockRe.exec(xml)) !== null) {
    const block = match[1];
    const name  = tagContent('Name',  block);
    const value = tagContent('Value', block);

    if (!name) {
      console.warn('[guidewire-connector] skipping <Property> block with missing or empty <Name>');
      continue;
    }
    if (value === null) {
      console.warn(`[guidewire-connector] skipping property "${name}" — missing <Value>`);
      continue;
    }

    records.push({ environmentName: envName, propertyKey: name, propertyValue: value });
  }

  return records;
}

// ── Connector ─────────────────────────────────────────────────────────────────

export class GuidewireConnector implements DataSourceConnector {
  readonly sourceType = 'GUIDEWIRE';

  readonly schema: ConnectorConfigSchema = {
    sourceType:  'GUIDEWIRE',
    displayName: 'Guidewire',
    description:
      'Reads runtime properties from a Guidewire application (PolicyCenter, ClaimCenter, or ' +
      'BillingCenter) via OAuth 2.0 client credentials and the Guidewire Runtime Property API. ' +
      'URL-valued properties are ingested as Integration nodes; ' +
      'non-URL values are stored as configuration attributes on the Environment node.',
    icon:     '',
    priority: 4,
    defaultEdges: [
      {
        toEntityType: 'Environment',
        mode:         'upsert',
        joinKey:      'envName',
        matchBy:      'name',
        priority:     10,
        description:  'Create or update an Environment node using the Guidewire environment display name',
      },
      {
        toEntityType: 'Integration',
        mode:         'create-child',
        joinKey:      'propertyKey',
        matchBy:      'name',
        priority:     20,
        description:
          'Create an Integration node for each URL-valued property. ' +
          'Non-URL values are stored as config attributes on the Environment node.',
      },
    ],
    fields: [
      {
        key:         'envName',
        label:       'Display Name',
        type:        'text',
        required:    true,
        placeholder: 'e.g. GW Production CC',
      },
      {
        key:         'appType',
        label:       'Application',
        type:        'select',
        required:    true,
        options:     ['PC', 'CC', 'BC'],
        placeholder: 'Select Guidewire application',
      },
      {
        key:         'tokenUrl',
        label:       'Token Endpoint URL',
        type:        'text',
        required:    true,
        placeholder: 'https://your-gw-host/pc/rest/oauth2/token',
      },
      {
        key:         'clientId',
        label:       'Client ID',
        type:        'text',
        required:    true,
        placeholder: 'OAuth client ID',
      },
      {
        key:         'clientSecret',
        label:       'Client Secret',
        type:        'password',
        required:    true,
        sensitive:   true,
        placeholder: 'OAuth client secret',
      },
      {
        key:         'propertyApiUrl',
        label:       'Runtime Property API URL',
        type:        'text',
        required:    true,
        placeholder: 'https://your-gw-host/pc/rest/admin/v1/systemProperties',
      },
    ],
  };

  // ── Validation ──────────────────────────────────────────────────────────────

  validateConfig(config: ConnectorConfig): string[] {
    const errors: string[] = [];
    if (!config.envName)        errors.push('envName (Display Name) is required');
    if (!config.appType)        errors.push('appType (Application) is required');
    if (!config.tokenUrl)       errors.push('tokenUrl (Token Endpoint URL) is required');
    if (!config.clientId)       errors.push('clientId (Client ID) is required');
    if (!config.clientSecret)   errors.push('clientSecret (Client Secret) is required');
    if (!config.propertyApiUrl) errors.push('propertyApiUrl (Runtime Property API URL) is required');
    return errors;
  }

  // ── Private helpers ─────────────────────────────────────────────────────────

  /**
   * Obtain an OAuth 2.0 Bearer token via client_credentials grant.
   * Sends a form-encoded POST and extracts access_token from the JSON response.
   */
  private async fetchToken(
    tokenUrl: string,
    clientId: string,
    clientSecret: string,
  ): Promise<string> {
    const body = new URLSearchParams({
      grant_type:    'client_credentials',
      client_id:     clientId,
      client_secret: clientSecret,
    }).toString();

    const res = await httpRequest(tokenUrl, {
      method:  'POST',
      headers: {
        'Content-Type':   'application/x-www-form-urlencoded',
        'Content-Length': String(Buffer.byteLength(body)),
        'Accept':         'application/json',
      },
      body,
    });

    if (res.statusCode < 200 || res.statusCode >= 300) {
      throw new Error(
        `Guidewire token request failed: HTTP ${res.statusCode} — ${res.body.slice(0, 300)}`,
      );
    }

    let parsed: Record<string, unknown>;
    try {
      parsed = JSON.parse(res.body);
    } catch {
      throw new Error(
        `Guidewire token response is not valid JSON: ${res.body.slice(0, 300)}`,
      );
    }

    const token = parsed['access_token'];
    if (typeof token !== 'string' || !token) {
      throw new Error(
        `Guidewire token response missing access_token field: ${res.body.slice(0, 300)}`,
      );
    }

    return token;
  }

  /**
   * Fetch the raw XML property document from the Guidewire Runtime Property API.
   */
  private async fetchPropertiesXml(propertyApiUrl: string, token: string): Promise<string> {
    const res = await httpRequest(propertyApiUrl, {
      method:  'GET',
      headers: {
        'Authorization': `Bearer ${token}`,
        'Accept':        'application/xml',
      },
    });

    if (res.statusCode < 200 || res.statusCode >= 300) {
      throw new Error(
        `Guidewire property API request failed: HTTP ${res.statusCode} — ${res.body.slice(0, 300)}`,
      );
    }

    return res.body;
  }

  // ── fetch ────────────────────────────────────────────────────────────────────

  async fetch(config: ConnectorConfig): Promise<FetchResult> {
    const startMs = Date.now();
    const envName        = String(config['envName']        || '');
    const appType        = String(config['appType']        || '');
    const tokenUrl       = String(config['tokenUrl']       || '');
    const clientId       = String(config['clientId']       || '');
    const clientSecret   = String(config['clientSecret']   || '');
    const propertyApiUrl = String(config['propertyApiUrl'] || '');

    console.log(
      `[guidewire-connector] requesting token from ${tokenUrl} (app: ${appType}, env: "${envName}")`,
    );
    const token = await this.fetchToken(tokenUrl, clientId, clientSecret);

    console.log(`[guidewire-connector] fetching runtime properties from ${propertyApiUrl}`);
    const xml = await this.fetchPropertiesXml(propertyApiUrl, token);

    const records = parseGuidewireXml(xml, envName);

    const urlCount    = records.filter(r => /^https?:\/\//i.test(r.propertyValue)).length;
    const configCount = records.length - urlCount;
    console.log(
      `[guidewire-connector] parsed ${records.length} properties for "${envName}" ` +
      `(${urlCount} URL → Integration, ${configCount} non-URL → config attributes)`,
    );

    return {
      records,
      fetchedAt: new Date().toISOString(),
      durationMs: Date.now() - startMs,
      metadata: {
        appType,
        propertyApiUrl,
        totalProperties: records.length,
        urlProperties:   urlCount,
        configProperties: configCount,
      },
    };
  }
}
