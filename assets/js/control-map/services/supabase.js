export const SERVICE_TABLE = 'Services';

export const SERVICE_CATEGORY_HINTS = {
  tech: ['GPS Technician'],
  reseller: ['dealer/auction'],
  repair: ['Repair Shop']
};

export const TABLES = {
  services: SERVICE_TABLE,
  vehicles: 'vehicles',
  vehiclesUpdates: 'vehicles_updates',
  deals: 'DealsJP1',
  hotspots: 'Hotspots',
  blacklist: 'Services_Blacklist',
  repairHistory: 'services_request',
  gpsHistory: '"PT-LastPing"'
};

export const SUPABASE_TIMEOUT_MS = 10000;

export const ensureSupabaseSession = async (supabaseClient) => {
  const { data, error } = await supabaseClient.auth.getSession();
  if (error || !data?.session) {
    const { data: refreshed, error: refreshError } = await supabaseClient.auth.refreshSession();
    if (refreshError || !refreshed?.session) {
      throw refreshError || new Error('Supabase session unavailable');
    }
  }
};
