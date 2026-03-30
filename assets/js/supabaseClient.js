import { SUPABASE_KEY, SUPABASE_URL, assertSupabaseTarget } from '../scripts/env.js';
import { notifyGlobalAlert } from '../scripts/globalAlerts.js';

// Single source of truth for the data client; load this file from pages/scripts.

const existingClient = typeof window !== 'undefined' ? window.supabaseClient : null;
const supabaseLibReady =
  typeof window !== 'undefined' &&
  window.supabase &&
  typeof window.supabase.createClient === 'function';

if (!supabaseLibReady) {
  console.error(
    'Data library not found. Please include the client script before supabaseClient.js.'
  );
  notifyGlobalAlert({
    title: 'Data Library Missing',
    message: 'The client library is not loaded before supabaseClient.js.',
    details: 'Expected window.supabase.createClient to be available.',
  });
}

const DEFAULT_FETCH_TIMEOUT_MS = 300_000; // 5 minutes to accommodate large uploads

const createFetchWithTimeout = (timeoutMs = DEFAULT_FETCH_TIMEOUT_MS) => {
  if (typeof fetch !== 'function') return null;
  return (resource, options = {}) => {
    const controller = new AbortController();
    const { signal, ...rest } = options || {};
    const timeoutId = setTimeout(() => controller.abort('timeout'), timeoutMs);

    if (signal) {
      if (signal.aborted) controller.abort(signal.reason);
      signal.addEventListener(
        'abort',
        () => controller.abort(signal.reason),
        { once: true }
      );
    }

    return fetch(resource, { ...rest, signal: controller.signal }).finally(() =>
      clearTimeout(timeoutId)
    );
  };
};

const supabaseInstance =
  existingClient ||
  (supabaseLibReady && SUPABASE_URL && SUPABASE_KEY && assertSupabaseTarget(SUPABASE_URL, SUPABASE_KEY)
    ? window.supabase.createClient(SUPABASE_URL, SUPABASE_KEY, {
        auth: {
          persistSession: true,
          autoRefreshToken: true,
        },
        global: {
          fetch: createFetchWithTimeout(),
        },
      })
    : null);

const supabase = existingClient || supabaseInstance || null;

if (!supabase && supabaseLibReady) {
  notifyGlobalAlert({
    title: 'Database Connection Blocked',
    message: 'The data client was not created due to validation or missing credentials.',
    details: 'The configured data endpoint did not pass validation.',
  });
}

if (typeof window !== 'undefined' && supabase) {
  window.supabaseClient = supabase;
}

export { supabase };
export default supabase;
