import { supabase as sharedSupabase } from '../js/supabaseClient.js';

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

const resolveActor = async (client, explicitActor) => {
  if (explicitActor) return explicitActor;

  if (cachedActor.value && cachedActor.expiresAt > now()) {
    return cachedActor.value;
  }

  try {
    const { data } = await client.auth.getUser();
    const actor = data?.user?.email || data?.user?.id || 'anon';
    cachedActor = { value: actor, expiresAt: now() + CACHE_TTL_MS };
    return actor;
  } catch (error) {
    console.warn('Admin audit: unable to resolve actor', error?.message || error);
    return 'anon';
  }
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
} = {}) => {
  const supabaseClient =
    client ||
    sharedSupabase ||
    (typeof window !== 'undefined' ? window.supabaseClient : null);

  if (!supabaseClient) return false;

  try {
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
