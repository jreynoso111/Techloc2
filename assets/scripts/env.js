export const SUPABASE_PROJECT_REF = 'lnfmogsjvdkqgwprlmtn';
export const SUPABASE_DB_HOST = 'db.lnfmogsjvdkqgwprlmtn.supabase.co';
export const SUPABASE_DB_PORT = 5432;
export const SUPABASE_DB_NAME = 'postgres';
export const SUPABASE_DB_USER = 'postgres';
export const SUPABASE_URL = `https://${SUPABASE_PROJECT_REF}.supabase.co`;
// Use the anon/publishable key that belongs to SUPABASE_PROJECT_REF only.
export const SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImxuZm1vZ3NqdmRrcWd3cHJsbXRuIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzEyNTQxMzMsImV4cCI6MjA4NjgzMDEzM30.HmK-Ndhy0LUZfQnXAUA65QiGX9Lfw-ba9cm5NPVPi0k';

const decodeJwtPayload = (token) => {
  try {
    const parts = String(token || '').split('.');
    if (parts.length < 2) return null;
    const payload = parts[1].replace(/-/g, '+').replace(/_/g, '/');
    const json = typeof atob === 'function' ? atob(payload) : null;
    return json ? JSON.parse(json) : null;
  } catch (_error) {
    return null;
  }
};

export const assertSupabaseTarget = (url = SUPABASE_URL, key = SUPABASE_KEY) => {
  let parsedUrl = null;
  try {
    parsedUrl = new URL(url);
  } catch (_error) {
    console.error('Invalid Supabase URL format.');
    return false;
  }

  const allowedHost = `${SUPABASE_PROJECT_REF}.supabase.co`;
  if (parsedUrl.hostname !== allowedHost) {
    console.error(`Blocked Supabase host: ${parsedUrl.hostname}. Allowed host: ${allowedHost}.`);
    if (typeof window !== 'undefined' && typeof window.reportGlobalIssue === 'function') {
      window.reportGlobalIssue(
        'Blocked Supabase Host',
        `Detected ${parsedUrl.hostname}, expected ${allowedHost}.`,
        `URL: ${url}`
      );
    }
    return false;
  }

  if (!key) {
    console.error('Missing Supabase key for the allowed project.');
    if (typeof window !== 'undefined' && typeof window.reportGlobalIssue === 'function') {
      window.reportGlobalIssue(
        'Missing Supabase Key',
        'SUPABASE_KEY is empty; connection cannot be established.',
        'Set the anon/publishable key in assets/scripts/env.js.'
      );
    }
    return false;
  }

  const payload = decodeJwtPayload(key);
  const keyRef = payload?.ref ? String(payload.ref).trim() : '';
  if (keyRef && keyRef !== SUPABASE_PROJECT_REF) {
    console.error(`Blocked Supabase key ref: ${keyRef}. Expected: ${SUPABASE_PROJECT_REF}.`);
    if (typeof window !== 'undefined' && typeof window.reportGlobalIssue === 'function') {
      window.reportGlobalIssue(
        'Blocked Supabase Key',
        `JWT ref ${keyRef} does not match allowed project ${SUPABASE_PROJECT_REF}.`,
        'Use the anon/publishable key from the allowed Supabase project.'
      );
    }
    return false;
  }

  return true;
};
