const ENV_ALIASES: Record<string, string> = {
  dev: 'development', development: 'development', qa: 'qa', test: 'test',
  tst: 'test', uat: 'uat', stage: 'staging', stg: 'staging', staging: 'staging',
  perf: 'performance', performance: 'performance',
  cert: 'certification', certification: 'certification',
  prod: 'production', prd: 'production', production: 'production',
};

export interface CanonicalIdentity {
  original: string;
  compact: string;
  canonicalBase: string;
  tokens: string[];
  environments: string[];
  numericAnchors: string[];
  searchVariants: string[];
}

function splitToken(token: string): string[] {
  const embeddedWhole = token.match(/^(.+?)(dev|qa|test|tst|uat|stage|stg|perf|performance|cert|certification|prod|prd)(\d+)$/);
  if (embeddedWhole) return [embeddedWhole[1], embeddedWhole[2], embeddedWhole[3]].filter(Boolean);
  const pieces: string[] = [];
  for (const part of token.match(/[a-z]+|\d+/g) || []) {
    pieces.push(part);
  }
  return pieces.filter(Boolean);
}

export function canonicalizeIdentity(value = ''): CanonicalIdentity {
  const normalized = value.toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
  const tokens = normalized.split(/\s+/).filter(Boolean).flatMap(splitToken);
  const environments = [...new Set(tokens.filter(t => ENV_ALIASES[t]).map(t => ENV_ALIASES[t]))];
  const numericAnchors = [...new Set(tokens.filter(t => /^\d+$/.test(t)).map(t => String(Number(t))))];
  const baseTokens = [...tokens];
  // Ignore only extra trailing environment labels. Embedded environment tokens
  // such as QA in appqa101 remain part of the identity.
  while (baseTokens.length > 1 && ENV_ALIASES[baseTokens[baseTokens.length - 1]]) baseTokens.pop();
  const canonicalBase = baseTokens.join('');
  const compact = tokens.join('');
  const core = tokens.filter(t => !ENV_ALIASES[t] && !/^\d+$/.test(t)).join('');
  const primaryEnv = tokens.find(t => ENV_ALIASES[t]) || '';
  const number = numericAnchors[0] || '';
  const searchVariants = [...new Set([
    canonicalBase, compact, value.toLowerCase(),
    [core, primaryEnv, number].filter(Boolean).join('_'),
    [core, primaryEnv, number].filter(Boolean).join('-'),
    [primaryEnv, number].filter(Boolean).join(''), number,
  ].map(v => v.trim()).filter(v => v.length >= 3))].slice(0, 6);
  return { original: value, compact, canonicalBase, tokens, environments, numericAnchors, searchVariants };
}

function jaccard(a: string[], b: string[]): number {
  const aa = new Set(a), bb = new Set(b); if (!aa.size || !bb.size) return 0;
  const common = [...aa].filter(token => bb.has(token)).length;
  return common / new Set([...aa, ...bb]).size;
}

export function compareCanonicalIdentities(ems: CanonicalIdentity, ci: CanonicalIdentity): {
  score: number; evidence: string[]; numericConflict: boolean; environmentConflict: boolean;
} {
  let score = 0; const evidence: string[] = [];
  if (ems.canonicalBase && ems.canonicalBase === ci.canonicalBase) { score += 60; evidence.push(`canonical base matched: ${ems.canonicalBase}`); }
  else if (ems.compact === ci.compact) { score += 55; evidence.push('compact name matched'); }
  else { score += Math.round(jaccard(ems.tokens, ci.tokens) * 35); }

  const numericConflict = ems.numericAnchors.length > 0 && ci.numericAnchors.length > 0
    && !ems.numericAnchors.some(n => ci.numericAnchors.includes(n));
  if (numericConflict) { score -= 80; evidence.push(`numeric identifier conflict: ${ems.numericAnchors.join('/')} vs ${ci.numericAnchors.join('/')}`); }
  else if (ems.numericAnchors.some(n => ci.numericAnchors.includes(n))) { score += 15; evidence.push(`numeric identifier matched: ${ems.numericAnchors.find(n => ci.numericAnchors.includes(n))}`); }

  const environmentConflict = ems.environments.length > 0 && ci.environments.length > 0
    && !ems.environments.some(env => ci.environments.includes(env));
  if (environmentConflict) { score -= 35; evidence.push(`environment conflict: ${ems.environments.join('/')} vs ${ci.environments.join('/')}`); }
  else if (ems.environments.some(env => ci.environments.includes(env))) { score += 10; evidence.push(`environment matched: ${ems.environments.find(env => ci.environments.includes(env))}`); }
  let lastNonEnvironment = -1;
  ci.tokens.forEach((token, index) => { if (!ENV_ALIASES[token]) lastNonEnvironment = index; });
  const ignored = ci.tokens.slice(lastNonEnvironment + 1);
  if (ignored.length) evidence.push(`ignored trailing environment suffix: ${ignored.join(', ')}`);
  return { score, evidence, numericConflict, environmentConflict };
}

export function ciClassPreference(emsType: string, ciClass: string): { score: number; label: string } {
  const preferences: Record<string, string[][]> = {
    Application: [['cmdb_ci_appl'], ['cmdb_ci_business_app', 'cmdb_ci_service_auto']],
    Environment: [['cmdb_ci_appl', 'cmdb_ci_service_auto'], ['cmdb_ci_business_app', 'cmdb_ci_server']],
  };
  const groups = preferences[emsType] || [];
  const exactOrSubclass = (expected: string) => ciClass === expected || ciClass.startsWith(`${expected}_`);
  if ((groups[0] || []).some(exactOrSubclass)) return { score: 15, label: 'preferred CI class' };
  if ((groups[1] || []).some(exactOrSubclass)) return { score: 8, label: 'compatible CI class' };
  return { score: 0, label: 'unclassified CI class' };
}
