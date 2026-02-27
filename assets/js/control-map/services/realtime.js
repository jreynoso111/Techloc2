const CONTROL_MAP_CHANNEL = 'control-map-events';

export const initializeControlMapRealtime = ({
  supabaseClient,
  tables,
  onVehiclesChange,
  onDealsChange,
  onInvoicesChange,
  onHotspotsChange,
  onBlacklistChange,
  onServicesChange
}) => {
  if (!supabaseClient || typeof supabaseClient.channel !== 'function') {
    return { stop: () => {} };
  }

  let channel = supabaseClient.channel(CONTROL_MAP_CHANNEL);

  const bindTable = (table, handler) => {
    if (!table || typeof handler !== 'function') return;
    channel = channel.on(
      'postgres_changes',
      { event: '*', schema: 'public', table },
      (payload) => {
        handler(payload);
      }
    );
  };

  bindTable(tables?.vehicles, onVehiclesChange);
  bindTable(tables?.deals, onDealsChange);
  bindTable(tables?.invoices, onInvoicesChange);
  bindTable(tables?.hotspots, onHotspotsChange);
  bindTable(tables?.blacklist, onBlacklistChange);
  bindTable(tables?.services, onServicesChange);

  channel.subscribe();

  const stop = () => {
    supabaseClient.removeChannel(channel);
  };

  return { stop };
};

export const startSupabaseKeepAlive = ({ supabaseClient, table, intervalMs = 4 * 60 * 1000 }) => {
  if (!supabaseClient) return () => {};

  const timer = setInterval(async () => {
    try {
      await supabaseClient.from(table).select('id').limit(1);
    } catch (error) {
      // Keep-alive failures are non-blocking.
    }
  }, intervalMs);

  return () => clearInterval(timer);
};
