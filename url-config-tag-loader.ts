/**
 * url-config-tag-loader.ts
 *
 * Loads and parses the URL config tagging property file from S3.
 *
 * File format (one app per line, blank lines and # comments ignored):
 *   ApplicationName1='urlconfig,keyname1', 'urlconfig,keyname2', 'urlconfig,keyname3'
 *   ApplicationName2='urlconfig,keyname6', 'urlconfig,keyname4', 'urlconfig,keyname5'
 *
 * Each quoted token is '<type>,<configKey>'.  The type prefix (e.g. "urlconfig")
 * is ignored — only the part after the last comma inside the quotes is used as
 * the config key to match against incoming URL config property keys.
 *
 * Result: Map<configKey (lowercase), appName>
 * If a key appears under multiple apps the last definition wins (warn logged).
 */

import { S3Client, GetObjectCommand, PutObjectCommand, NoSuchKey } from '@aws-sdk/client-s3';

export const TAG_FILE_KEY = 'config/url-config-tags.properties';

/** configKey (normalised to lowercase) → application tag name */
export type TagMapping = Map<string, string>;

// ── Parser ────────────────────────────────────────────────────────────────────

export function parseTagProperties(content: string): TagMapping {
  const mapping: TagMapping = new Map();

  for (const rawLine of content.split('\n')) {
    const line = rawLine.trim();
    if (!line || line.startsWith('#')) continue;

    const eqIdx = line.indexOf('=');
    if (eqIdx === -1) continue;

    const appName   = line.slice(0, eqIdx).trim();
    const rightSide = line.slice(eqIdx + 1);

    // Extract all single-quoted tokens: 'urlconfig,keyname1'
    const tokenRegex = /'([^']+)'/g;
    let match: RegExpExecArray | null;
    while ((match = tokenRegex.exec(rightSide)) !== null) {
      const token = match[1].trim();                          // e.g. "urlconfig,keyname1"
      const lastComma = token.lastIndexOf(',');
      const configKey = (lastComma === -1 ? token : token.slice(lastComma + 1)).trim().toLowerCase();
      if (!configKey) continue;

      if (mapping.has(configKey)) {
        console.warn(`[url-config-tag-loader] configKey "${configKey}" already mapped to "${mapping.get(configKey)}", overwriting with "${appName}"`);
      }
      mapping.set(configKey, appName);
    }
  }

  return mapping;
}

// ── S3 I/O ────────────────────────────────────────────────────────────────────

/**
 * Load the tag mapping from S3.
 * Returns an empty map — not an error — when the file does not exist yet.
 */
export async function loadTagMapping(
  bucketName: string,
  region = process.env.AWS_REGION || 'us-east-1',
): Promise<TagMapping> {
  if (!bucketName) {
    console.warn('[url-config-tag-loader] S3_BUCKET not configured — tagging disabled');
    return new Map();
  }

  const s3 = new S3Client({ region });
  try {
    const res = await s3.send(new GetObjectCommand({ Bucket: bucketName, Key: TAG_FILE_KEY }));
    const content = await res.Body!.transformToString('utf-8');
    const mapping = parseTagProperties(content);
    console.log(`[url-config-tag-loader] loaded ${mapping.size} tag mappings from s3://${bucketName}/${TAG_FILE_KEY}`);
    return mapping;
  } catch (err: any) {
    if (err.name === 'NoSuchKey' || err instanceof NoSuchKey) {
      console.log(`[url-config-tag-loader] tag file not found — tagging skipped`);
      return new Map();
    }
    // Non-fatal: log and continue pipeline without tags
    console.error('[url-config-tag-loader] failed to load tag file:', err?.message);
    return new Map();
  }
}

/**
 * Upload (create or overwrite) the tag property file in S3.
 */
export async function saveTagMapping(
  content: string,
  bucketName: string,
  region = process.env.AWS_REGION || 'us-east-1',
): Promise<void> {
  const s3 = new S3Client({ region });
  await s3.send(new PutObjectCommand({
    Bucket:      bucketName,
    Key:         TAG_FILE_KEY,
    Body:        content,
    ContentType: 'text/plain',
  }));
  console.log(`[url-config-tag-loader] saved tag file to s3://${bucketName}/${TAG_FILE_KEY}`);
}
