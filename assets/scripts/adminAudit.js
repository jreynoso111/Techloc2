import { supabase as sharedSupabase } from '../js/supabaseClient.js';
import { getWebAdminSession } from './web-admin-session.js';

const CHANGE_LOG_TABLE = 'admin_change_log';
const CACHE_TTL_MS = 5 * 60 * 1000;

let cachedActor = { value: null, expiresAt: 0 };

const now = () => Date.now();
const normalizeValue = (value) => {
  if (value === undefined) return null;
  if (value === null) return null;
  if (typeof value === 'object') return JSON.stringify(value);
  return value;
};
const normalizeDetails = (value) => {
  if (!value) return {};
  if (typeof value === 'object') return value;
  return { value: String(value) };
};

const resolveActor = async (client, explicitActor) => {
  if (explicitActor) return explicitActor;

  if (cachedActor.value && cachedActor.expiresAt > now()) {
    return cachedActor.value;
  }

  try {
    if (client?.auth && typeof client.auth.getUser === 'function') {
      const { data } = await client.auth.getUser();
      const actor = data?.user?.email || data?.user?.id || null;
      if (actor) {
        cachedActor = { value: actor, expiresAt: now() + CACHE_TTL_MS };
        return actor;
      }
    }

    const localAdminSession = getWebAdminSession();
    if (localAdminSession?.email) {
      cachedActor = { value: localAdminSession.email, expiresAt: now() + CACHE_TTL_MS };
      return localAdminSession.email;
    }

    if (client?.auth && typeof client.auth.getSession === 'function') {
      const { data } = await client.auth.getSession();
      const actor = data?.session?.user?.email || data?.session?.user?.id || 'anon';
      cachedActor = { value: actor, expiresAt: now() + CACHE_TTL_MS };
      return actor;
    }
  } catch (error) {
    console.warn('Admin audit: unable to resolve actor', error?.message || error);
  }
  return 'anon';
};

const resolveProfile = () => {
  const localAdminSession = getWebAdminSession();
  const role = document.body?.dataset.userRole || window.currentUserRole || localAdminSession?.role || 'guest';
  const status = document.body?.dataset.userStatus || window.currentUserStatus || localAdminSession?.status || 'unknown';
  const email = localAdminSession?.email || null;

  return {
    email,
    role: String(role || 'guest').toLowerCase(),
    status: String(status || 'unknown').toLowerCase(),
    pagePath: typeof window !== 'undefined' ? window.location.pathname : null,
  };
};

export const logAdminEvent = async ({
  client,
  action = 'edit',
  tableName = 'admin',
  summary = '',
  recordId = null,
  columnName = null,
  previousValue = null,
  newValue = null,
  actor = null,
  profileEmail = null,
  profileRole = null,
  profileStatus = null,
  pagePath = null,
  source = 'web',
  details = {},
} = {}) => {
  const supabaseClient =
    client ||
    sharedSupabase ||
    (typeof window !== 'undefined' ? window.supabaseClient : null);

  if (!supabaseClient) return false;

  try {
    const profile = resolveProfile();
    const actorName = await resolveActor(supabaseClient, actor);
    const normalizedAction = String(action || 'edit').toLowerCase();
    const fallbackSummary = `Admin ${normalizedAction} on ${tableName}`;

    await supabaseClient.from(CHANGE_LOG_TABLE).insert([
      {
        table_name: tableName,
        action: normalizedAction,
        summary: summary || fallbackSummary,
        actor: actorName || 'anon',
        record_id: recordId ?? null,
        column_name: columnName ?? null,
        previous_value: normalizeValue(previousValue),
        new_value: normalizeValue(newValue),
        profile_email: profileEmail || profile.email || actorName || null,
        profile_role: profileRole || profile.role || 'guest',
        profile_status: profileStatus || profile.status || 'unknown',
        page_path: pagePath || profile.pagePath || null,
        source: String(source || 'web').toLowerCase(),
        details: normalizeDetails(details),
        created_at: new Date().toISOString(),
      },
    ]);

    return true;
  } catch (error) {
    console.warn('Admin audit: unable to write change log entry', error?.message || error);
    return false;
  }
};

export default logAdminEvent;
