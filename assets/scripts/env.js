<<<<<<< HEAD
export const SUPABASE_URL = 'https://ewgtclzscwbokxmzxbcu.supabase.co';
export const SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImV3Z3RjbHpzY3dib2t4bXp4YmN1Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjUwODA3MzIsImV4cCI6MjA4MDY1NjczMn0.QkM72rVeBpm6uGgBVdG4ulIzEg3V_7T8usqvIf6vBto';
=======
const DEFAULT_SUPABASE_PROJECT_REF = 'lnfmogsjvdkqgwprlmtn';
const DEFAULT_SUPABASE_URL = 'https://lnfmogsjvdkqgwprlmtn.supabase.co';
// Publishable keys are safe to ship in the browser; never place a service-role key here.
const DEFAULT_SUPABASE_PUBLISHABLE_KEY = 'sb_publishable_HhPw8JLinAfDtUNWXnQg8Q_KhXvprNM';

const getRuntimeConfig = () => {
  const browserConfig = (
    typeof window !== 'undefined'
    && window.__TECHLOC_RUNTIME_CONFIG__
    && typeof window.__TECHLOC_RUNTIME_CONFIG__ === 'object'
  )
    ? window.__TECHLOC_RUNTIME_CONFIG__
    : null;

  const nodeConfig = (typeof process !== 'undefined' && process?.env)
    ? {
      supabaseUrl: process.env.SUPABASE_URL,
      supabaseAnonKey: process.env.SUPABASE_ANON_KEY || process.env.SUPABASE_PUBLISHABLE_KEY,
      supabaseProjectRef: process.env.SUPABASE_PROJECT_REF
    }
    : null;

  return {
    supabaseUrl: browserConfig?.supabaseUrl || nodeConfig?.supabaseUrl || DEFAULT_SUPABASE_URL,
    supabaseAnonKey: browserConfig?.supabaseAnonKey || nodeConfig?.supabaseAnonKey || DEFAULT_SUPABASE_PUBLISHABLE_KEY,
    supabaseProjectRef: browserConfig?.supabaseProjectRef || nodeConfig?.supabaseProjectRef || DEFAULT_SUPABASE_PROJECT_REF
  };
};

const runtimeConfig = getRuntimeConfig();

const toSafeString = (value = '') => `${value || ''}`.trim();
const PLACEHOLDER_KEY_PATTERNS = [
  'your-anon-or-publishable-key',
  'your-service-role-key',
  'replace-me',
  'changeme',
];

const decodeBase64 = (value = '') => {
  if (!value) return '';
  try {
    if (typeof atob === 'function') return atob(value);
    if (typeof Buffer !== 'undefined') return Buffer.from(value, 'base64').toString('utf8');
  } catch (_error) {
    return '';
  }
  return '';
};

const decodeJwtPayload = (token) => {
  try {
    const parts = String(token || '').split('.');
    if (parts.length < 2) return null;
    const payload = parts[1].replace(/-/g, '+').replace(/_/g, '/');
    const padding = '='.repeat((4 - (payload.length % 4)) % 4);
    const decoded = decodeBase64(`${payload}${padding}`);
    return decoded ? JSON.parse(decoded) : null;
  } catch (_error) {
    return null;
  }
};

const deriveProjectRefFromUrl = (url = '') => {
  try {
    const host = new URL(url).hostname || '';
    const match = host.match(/^([a-z0-9-]+)\.supabase\.co$/i);
    return match?.[1] ? String(match[1]).trim() : '';
  } catch (_error) {
    return '';
  }
};

export const SUPABASE_URL = toSafeString(runtimeConfig.supabaseUrl);
export const SUPABASE_KEY = toSafeString(runtimeConfig.supabaseAnonKey);
const keyProjectRef = toSafeString(decodeJwtPayload(SUPABASE_KEY)?.ref);
export const SUPABASE_PROJECT_REF = (
  toSafeString(runtimeConfig.supabaseProjectRef)
  || deriveProjectRefFromUrl(SUPABASE_URL)
  || keyProjectRef
);
export const SUPABASE_DB_HOST = SUPABASE_PROJECT_REF ? `db.${SUPABASE_PROJECT_REF}.supabase.co` : '';
export const SUPABASE_DB_PORT = 5432;
export const SUPABASE_DB_NAME = 'postgres';
export const SUPABASE_DB_USER = 'postgres';

const reportGlobalIssue = (title = '', message = '', details = '') => {
  if (typeof window !== 'undefined' && typeof window.reportGlobalIssue === 'function') {
    window.reportGlobalIssue(title, message, details);
  }
};

export const assertSupabaseTarget = (url = SUPABASE_URL, key = SUPABASE_KEY) => {
  const normalizedUrl = toSafeString(url);
  const normalizedKey = toSafeString(key);

  if (!normalizedUrl) {
    console.error('Missing Supabase URL.');
    reportGlobalIssue(
      'Missing Supabase URL',
      'SUPABASE_URL is empty; connection cannot be established.',
      'Set SUPABASE_URL in server environment variables.'
    );
    return false;
  }

  let parsedUrl = null;
  try {
    parsedUrl = new URL(normalizedUrl);
  } catch (_error) {
    console.error('Invalid Supabase URL format.');
    return false;
  }

  if (SUPABASE_PROJECT_REF) {
    const allowedHost = `${SUPABASE_PROJECT_REF}.supabase.co`;
    if (parsedUrl.hostname !== allowedHost) {
      console.error(`Blocked Supabase host: ${parsedUrl.hostname}. Allowed host: ${allowedHost}.`);
      reportGlobalIssue(
        'Blocked Supabase Host',
        `Detected ${parsedUrl.hostname}, expected ${allowedHost}.`,
        `URL: ${normalizedUrl}`
      );
      return false;
    }
  }

  if (!normalizedKey) {
    console.error('Missing Supabase anon/publishable key.');
    reportGlobalIssue(
      'Missing Supabase Key',
      'SUPABASE_KEY is empty; connection cannot be established.',
      'Set SUPABASE_ANON_KEY (or SUPABASE_PUBLISHABLE_KEY) in server environment variables.'
    );
    return false;
  }

  const lowerKey = normalizedKey.toLowerCase();
  if (PLACEHOLDER_KEY_PATTERNS.some((pattern) => lowerKey.includes(pattern))) {
    console.error('Supabase key is still a placeholder value.');
    reportGlobalIssue(
      'Invalid Supabase Key',
      'The configured Supabase key is still a placeholder and cannot authenticate.',
      'Set SUPABASE_ANON_KEY or SUPABASE_PUBLISHABLE_KEY in /Users/jreynoso/Downloads/Techloc2/.env.'
    );
    return false;
  }

  const payload = decodeJwtPayload(normalizedKey);
  const keyRef = payload?.ref ? String(payload.ref).trim() : '';
  if (SUPABASE_PROJECT_REF && keyRef && keyRef !== SUPABASE_PROJECT_REF) {
    console.error(`Blocked Supabase key ref: ${keyRef}. Expected: ${SUPABASE_PROJECT_REF}.`);
    reportGlobalIssue(
      'Blocked Supabase Key',
      `JWT ref ${keyRef} does not match allowed project ${SUPABASE_PROJECT_REF}.`,
      'Use the anon/publishable key from the allowed Supabase project.'
    );
    return false;
  }

  return true;
};
>>>>>>> impte
