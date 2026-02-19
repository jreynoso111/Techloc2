import { DashboardState } from '../core/state.js';
import { assertSupabaseTarget } from '../env.js';

const resolveSupabaseModule = async () => {
  const moduleUrls = [
    new URL('../../js/supabaseClient.js', import.meta.url),
    new URL('../js/supabaseClient.js', import.meta.url),
    new URL('/assets/js/supabaseClient.js', window.location.origin),
  ];

  for (const moduleUrl of moduleUrls) {
    try {
      return await import(moduleUrl.href);
    } catch (error) {
      if (moduleUrl === moduleUrls[moduleUrls.length - 1]) {
        throw error;
      }
    }
  }

  return null;
};

export const getSupabaseClient = async ({ supabaseUrl, supabaseAnonKey, showDebug }) => {
  try {
    const existingClient = typeof window !== 'undefined' ? window.supabaseClient : null;
    if (existingClient?.from) return existingClient;

    const mod = await resolveSupabaseModule();
    const supabaseClient = mod?.supabase || mod?.default || null;
    if (supabaseClient?.from) return supabaseClient;
    throw new Error('supabaseClient.js loaded but did not export a Supabase client (expected export const supabase = createClient(...)).');
  } catch (error) {
    if (supabaseUrl && supabaseAnonKey) {
      if (!assertSupabaseTarget(supabaseUrl, supabaseAnonKey)) {
        if (showDebug) {
          showDebug(
            'Blocked Supabase target',
            'The provided Supabase URL/key do not match the allowed project.',
            { supabaseUrl }
          );
        }
        return null;
      }
      return window.supabase.createClient(supabaseUrl, supabaseAnonKey);
    }
    if (showDebug) {
      showDebug(
        'Supabase client not available',
        'Tu import falló y el fallback no tiene URL/KEY. Revisa supabaseClient.js export o pega credenciales aquí.',
        { error: String(error) }
      );
    }
    return null;
  }
};

export const initializeSupabaseRealtime = ({ supabaseClient, setConnectionStatus, handleVehicleChange }) => {
  if (!supabaseClient?.channel) return;
  if (DashboardState.realtime.channel) return;

  const channel = supabaseClient
    .channel('inventory-control-changes')
    .on('postgres_changes', { event: '*', schema: 'public', table: 'DealsJP1' }, handleVehicleChange);

  DashboardState.realtime.channel = channel;

  channel.subscribe((status) => {
    if (status === 'SUBSCRIBED') setConnectionStatus('Live');
    else if (status === 'CHANNEL_ERROR' || status === 'TIMED_OUT') setConnectionStatus('Reconnecting…');
    else if (status === 'CLOSED') setConnectionStatus('Offline');
  });

  window.addEventListener('beforeunload', () => {
    if (DashboardState.realtime.channel) {
      supabaseClient.removeChannel(DashboardState.realtime.channel);
      DashboardState.realtime.channel = null;
    }
  });
};

export const hydrateVehiclesFromSupabase = async ({
  supabaseClient,
  setConnectionStatus,
  renderDashboard,
  showDebug,
  buildSchemaFromData,
  setVehiclesFromArray,
  initializeTablePreferences,
  setupFilters,
  getField,
}) => {
  if (supabaseClient === null) {
    console.warn('Supabase client not available, skipping vehicle hydration.');
    DashboardState.ui.isLoading = false;
    setConnectionStatus('Offline');
    renderDashboard();
    return;
  }
  if (!supabaseClient?.from) {
    DashboardState.ui.isLoading = false;
    setConnectionStatus('Offline');
    renderDashboard();
    return;
  }

  setConnectionStatus('Reconnecting…');

  const { data, error } = await supabaseClient
    .from('DealsJP1')
    .select('*')
    .limit(5000);

  if (error) {
    setConnectionStatus('Offline');
    DashboardState.ui.isLoading = false;
    renderDashboard();

    if (showDebug) {
      showDebug(
        'Supabase SELECT failed',
        'Esto suele ser RLS (no tienes SELECT permitido), credenciales, o tabla/columnas distintas.',
        { code: error.code, message: error.message, details: error.details, hint: error.hint }
      );
    }
    return;
  }

  DashboardState.schema = buildSchemaFromData(data || []);

  setVehiclesFromArray((data || []).map((vehicle) => {
    const updatedAt = getField(vehicle, 'Updated At', 'Updated', 'Last Updated');
    return {
      ...vehicle,
      lastEventAt: updatedAt ? new Date(updatedAt).getTime() : null,
    };
  }));

  DashboardState.ui.isLoading = false;
  initializeTablePreferences();
  setupFilters({ preserveSelections: true });
  renderDashboard();
  setConnectionStatus('Live');
};
