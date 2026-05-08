// Key Parser for PCE Discovery Pipeline
// Converts Java bean property keys to human-readable service names
// e.g. "recordPaymentReceiptServiceImpl.endpointRecordPaymentUri" → "Record Payment Receipt Service"

// Suffixes to strip from the service portion of the key
const STRIP_SUFFIXES = ['Impl', 'Bean', 'Proxy', 'Service', 'Client', 'Adapter', 'Delegate'];

// Known abbreviations to preserve in uppercase
const UPPERCASE_WORDS = new Set(['api', 'url', 'uri', 'http', 'https', 'ssl', 'tls', 'jms', 'jndi', 'jdbc', 'soap', 'rest', 'xml', 'json', 'sql', 'id']);

export interface ParsedKey {
  original: string;
  serviceName: string;           // Human-readable: "Record Payment Receipt"
  endpointHint: string;          // The method/endpoint portion: "endpointRecordPaymentUri"
  serviceNameRaw: string;        // Raw first segment: "recordPaymentReceiptServiceImpl"
}

/**
 * Split a camelCase or PascalCase string into individual words
 */
function splitCamelCase(str: string): string[] {
  return str
    .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
    .replace(/([A-Z]+)([A-Z][a-z])/g, '$1 $2')
    .split(/[\s_.-]+/)
    .filter(Boolean);
}

/**
 * Convert a word to title case, respecting known abbreviations
 */
function titleCaseWord(word: string): string {
  const lower = word.toLowerCase();
  if (UPPERCASE_WORDS.has(lower)) return word.toUpperCase();
  return word.charAt(0).toUpperCase() + word.slice(1).toLowerCase();
}

/**
 * Strip known suffixes from a raw service name segment
 */
function stripSuffixes(raw: string): string {
  let result = raw;
  for (const suffix of STRIP_SUFFIXES) {
    if (result.endsWith(suffix) && result.length > suffix.length) {
      result = result.slice(0, -suffix.length);
    }
  }
  return result;
}

/**
 * Parse a Java bean property key into structured parts
 *
 * Input:  "recordPaymentReceiptServiceImpl.endpointRecordPaymentUri"
 * Output: { serviceName: "Record Payment Receipt", endpointHint: "endpointRecordPaymentUri", ... }
 *
 * If name has no dot separator, treats the whole key as the service name.
 */
export function parsePropertyKey(key: string): ParsedKey {
  const dotIndex = key.indexOf('.');
  let serviceNameRaw: string;
  let endpointHint: string;

  if (dotIndex > 0) {
    serviceNameRaw = key.substring(0, dotIndex);
    endpointHint = key.substring(dotIndex + 1);
  } else {
    serviceNameRaw = key;
    endpointHint = '';
  }

  const stripped = stripSuffixes(serviceNameRaw);
  const words = splitCamelCase(stripped);
  const serviceName = words.map(titleCaseWord).join(' ');

  return {
    original: key,
    serviceName,
    endpointHint,
    serviceNameRaw,
  };
}

/**
 * Get a concise integration name from a property key
 * Used as the Integration entity name in Neptune
 */
export function toIntegrationName(key: string): string {
  return parsePropertyKey(key).serviceName;
}

/**
 * Get a full integration name from a property key, incorporating both the service
 * segment and the endpoint hint segment, as PascalCase without spaces.
 * e.g. "recordPaymentReceiptServiceImpl.endpointRecordPaymentUri"
 *      → "RecordPaymentReceiptEndpointRecordPaymentUri"
 * Used for PAS PCE discovery where the full property name identifies the integration.
 */
export function toFullIntegrationName(key: string): string {
  const parsed = parsePropertyKey(key);
  if (!parsed.endpointHint) {
    return splitCamelCase(parsed.serviceNameRaw)
      .map(w => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase())
      .join('');
  }
  const serviceWords = splitCamelCase(stripSuffixes(parsed.serviceNameRaw));
  const endpointWords = splitCamelCase(parsed.endpointHint);
  return [...serviceWords, ...endpointWords]
    .map(w => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase())
    .join('');
}
