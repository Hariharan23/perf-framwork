/**
 * CMDB Config Loader
 *
 * Reads all /ems/cmdb/* SSM parameters and credentials from Secrets Manager
 * at Lambda cold-start. The result is cached at module level so subsequent
 * invocations within the same Lambda instance skip the SSM round-trip.
 */

import { SSMClient, GetParameterCommand } from '@aws-sdk/client-ssm';
import { CmdbConfig } from './servicenow-adapter';

const ssmClient = new SSMClient({});

let cachedConfig: CmdbConfig | null = null;

async function getParam(name: string, defaultValue?: string): Promise<string> {
  try {
    const resp = await ssmClient.send(new GetParameterCommand({ Name: name, WithDecryption: true }));
    return resp.Parameter?.Value || defaultValue || '';
  } catch {
    if (defaultValue !== undefined) return defaultValue;
    throw new Error(`Required SSM parameter not found: ${name}`);
  }
}

/**
 * Load CMDB configuration from SSM (including SecureString parameters).
 * Credentials are stored as SecureString at /ems/cmdb/username and /ems/cmdb/password.
 * Cached after first successful load for the lifetime of the Lambda instance.
 */
export async function loadCmdbConfig(): Promise<CmdbConfig> {
  if (cachedConfig) return cachedConfig;

  const [
    instanceUrl,
    ciClassesRaw,
    timeoutRaw,
    searchLimitRaw,
    username,
    password,
  ] = await Promise.all([
    getParam('/ems/cmdb/instance_url'),
    getParam('/ems/cmdb/ci_classes', 'cmdb_ci_appl,cmdb_ci_server'),
    getParam('/ems/cmdb/timeout_ms', '10000'),
    getParam('/ems/cmdb/search_limit', '20'),
    getParam('/ems/cmdb/username'),
    getParam('/ems/cmdb/password'),
  ]);

  cachedConfig = {
    instanceUrl: instanceUrl.replace(/\/$/, ''),   // strip trailing slash
    username,
    password,
    ciClasses:   ciClassesRaw.split(',').map(s => s.trim()).filter(Boolean),
    timeoutMs:   parseInt(timeoutRaw, 10)  || 10000,
    searchLimit: parseInt(searchLimitRaw, 10) || 20,
  };

  console.log(`CMDB config loaded: instanceUrl=${cachedConfig.instanceUrl}, ciClasses=${cachedConfig.ciClasses.join(',')}`);
  return cachedConfig;
}

/** Force reload on next call (useful for config updates without Lambda restart) */
export function clearCmdbConfigCache(): void {
  cachedConfig = null;
}
