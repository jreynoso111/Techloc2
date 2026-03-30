export const SERVICE_TABLE = 'Services';

export const SERVICE_CATEGORY_HINTS = {
  tech: ['GPS Technician'],
  reseller: ['dealer/auction'],
  repair: ['Repair Shop']
};

export const TABLES = {
  services: SERVICE_TABLE,
  vehicles: 'vehicles',
  deals: 'DealsJP1',
  invoices: 'NS-Invoices&pays',
  hotspots: 'Hotspots',
  blacklist: 'Services_Blacklist',
  gpsDeviceBlacklist: 'gps_blacklist',
  repairHistory: 'repair_history',
  gpsHistory: 'PT-LastPing'
};

export const SUPABASE_TIMEOUT_MS = 10000;

export const ensureSupabaseSession = async (supabaseClient) => {
  if (!supabaseClient?.auth?.getSession) return false;

  const isMissingAuthSessionError = (error) => {
    const message = String(error?.message || '').toLowerCase();
    return (
      message.includes('auth session missing') ||
      message.includes('session missing') ||
      message.includes('invalid refresh token')
    );
  };

  let sessionData = null;
  try {
    const { data, error } = await supabaseClient.auth.getSession();
    if (error && !isMissingAuthSessionError(error)) throw error;
    sessionData = data;
  } catch (error) {
    if (!isMissingAuthSessionError(error)) throw error;
  }

  if (sessionData?.session) return true;

  if (typeof supabaseClient.auth.refreshSession !== 'function') return false;

  try {
    const { data: refreshed, error: refreshError } = await supabaseClient.auth.refreshSession();
    if (refreshError && !isMissingAuthSessionError(refreshError)) throw refreshError;
    return Boolean(refreshed?.session);
  } catch (error) {
    if (!isMissingAuthSessionError(error)) throw error;
    return false;
  }
};
