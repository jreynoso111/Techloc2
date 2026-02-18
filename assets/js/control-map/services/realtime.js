const CONTROL_MAP_CHANNEL = 'control-map-events';

export const initializeControlMapRealtime = ({
  supabaseClient,
  tables,
  onVehiclesChange,
  onHotspotsChange,
  onBlacklistChange,
  onServicesChange
}) => {
  if (!supabaseClient || typeof supabaseClient.channel !== 'function') {
    return { stop: () => {} };
  }

  const channel = supabaseClient
    .channel(CONTROL_MAP_CHANNEL)
    .on('postgres_changes', { event: '*', schema: 'public', table: tables.vehicles }, (payload) => {
      onVehiclesChange?.(payload);
    })
    .on('postgres_changes', { event: '*', schema: 'public', table: tables.hotspots }, (payload) => {
      onHotspotsChange?.(payload);
    })
    .on('postgres_changes', { event: '*', schema: 'public', table: tables.blacklist }, (payload) => {
      onBlacklistChange?.(payload);
    })
    .on('postgres_changes', { event: '*', schema: 'public', table: tables.services }, (payload) => {
      onServicesChange?.(payload);
    });

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
