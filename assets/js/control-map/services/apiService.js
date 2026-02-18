import { initializeControlMapRealtime } from './realtime.js';

const createEventBus = () => {
  const listeners = new Map();

  const on = (event, callback) => {
    if (!event || typeof callback !== 'function') return () => {};
    const handlers = listeners.get(event) || new Set();
    handlers.add(callback);
    listeners.set(event, handlers);
    return () => off(event, callback);
  };

  const off = (event, callback) => {
    const handlers = listeners.get(event);
    if (!handlers) return;
    handlers.delete(callback);
    if (!handlers.size) listeners.delete(event);
  };

  const emit = (event, payload) => {
    const handlers = listeners.get(event);
    if (!handlers) return;
    handlers.forEach((handler) => handler(payload));
  };

  return { on, off, emit };
};

export const createControlMapApiService = ({
  supabaseClient,
  tables,
  handlers = {}
}) => {
  const bus = createEventBus();
  const handlerMap = new Map([
    ['vehicles', handlers.vehicles],
    ['hotspots', handlers.hotspots],
    ['blacklist', handlers.blacklist],
    ['services', handlers.services]
  ]);

  const handleEvent = (eventKey) => (payload) => {
    bus.emit(eventKey, payload);
    const handler = handlerMap.get(eventKey);
    handler?.(payload);
  };

  const realtime = initializeControlMapRealtime({
    supabaseClient,
    tables,
    onVehiclesChange: handleEvent('vehicles'),
    onHotspotsChange: handleEvent('hotspots'),
    onBlacklistChange: handleEvent('blacklist'),
    onServicesChange: handleEvent('services')
  });

  return {
    on: bus.on,
    off: bus.off,
    stop: realtime.stop
  };
};
