const STORAGE_KEY = 'techloc:navigation-state';
const listeners = new Set();

const DEFAULT_SERVICE_FILTERS = {
  tech: '',
  reseller: '',
  repair: '',
  custom: '',
};
const DEFAULT_SERVICE_FILTER_IDS = {
  tech: null,
  reseller: null,
  repair: null,
  custom: null,
};

let currentState = null;

const safeParse = (value) => {
  if (!value) return null;
  try {
    return JSON.parse(value);
  } catch (error) {
    console.warn('Unable to parse navigation state from storage.', error);
    return null;
  }
};

const normalizeSelection = (selection) => {
  if (!selection) return null;
  const key = selection.key ? String(selection.key).trim() : '';
  const vin = selection.vin ? String(selection.vin).trim() : '';
  const customerId = selection.customerId ? String(selection.customerId).trim() : '';
  return {
    id: selection.id ?? null,
    key,
    vin,
    customerId,
    updatedAt: selection.updatedAt || Date.now(),
  };
};

const normalizeServiceFilters = (filters) => {
  const next = { ...DEFAULT_SERVICE_FILTERS };
  if (filters && typeof filters === 'object') {
    Object.keys(DEFAULT_SERVICE_FILTERS).forEach((key) => {
      if (filters[key] !== undefined && filters[key] !== null) {
        next[key] = String(filters[key]);
      }
    });
  }
  return next;
};

const normalizeServiceFilterIds = (filters) => {
  const next = { ...DEFAULT_SERVICE_FILTER_IDS };
  if (filters && typeof filters === 'object') {
    Object.keys(DEFAULT_SERVICE_FILTER_IDS).forEach((key) => {
      const value = filters[key];
      if (Array.isArray(value)) {
        next[key] = value.map((id) => `${id}`);
      } else {
        next[key] = value ?? null;
      }
    });
  }
  return next;
};

const normalizeState = (state) => {
  const raw = state && typeof state === 'object' ? state : {};
  return {
    selectedVehicle: normalizeSelection(raw.selectedVehicle),
    serviceFilters: normalizeServiceFilters(raw.serviceFilters),
    serviceFilterIds: normalizeServiceFilterIds(raw.serviceFilterIds),
  };
};

const getNavigationState = () => {
  if (currentState) return currentState;
  const stored = safeParse(localStorage.getItem(STORAGE_KEY));
  currentState = normalizeState(stored);
  return currentState;
};

const notify = (next) => {
  listeners.forEach((listener) => listener(next));
  window.dispatchEvent(new CustomEvent('navigation-state-change', { detail: next }));
};

const setNavigationState = (nextState) => {
  const prev = getNavigationState();
  const merged = {
    ...prev,
    ...nextState,
    serviceFilters: {
      ...prev.serviceFilters,
      ...(nextState?.serviceFilters || {}),
    },
    serviceFilterIds: {
      ...prev.serviceFilterIds,
      ...(nextState?.serviceFilterIds || {}),
    },
  };
  const normalized = normalizeState(merged);
  if (JSON.stringify(prev) === JSON.stringify(normalized)) return;
  currentState = normalized;
  localStorage.setItem(STORAGE_KEY, JSON.stringify(normalized));
  notify(normalized);
};

const subscribeNavigationState = (listener) => {
  if (typeof listener !== 'function') return () => {};
  listeners.add(listener);
  return () => listeners.delete(listener);
};

const bindNavigationStorageListener = () => {
  window.addEventListener('storage', (event) => {
    if (event.key !== STORAGE_KEY) return;
    currentState = normalizeState(safeParse(event.newValue));
    notify(currentState);
  });
};

const getSelectedVehicle = () => getNavigationState().selectedVehicle;

const setSelectedVehicle = (selection) => {
  setNavigationState({ selectedVehicle: selection });
};

const subscribeSelectedVehicle = (listener) => subscribeNavigationState((state) => listener(state.selectedVehicle));

const getServiceFilters = () => getNavigationState().serviceFilters;

const setServiceFilter = (type, value) => {
  if (!Object.hasOwn(DEFAULT_SERVICE_FILTERS, type)) return;
  setNavigationState({ serviceFilters: { [type]: value ?? '' } });
};

const setServiceFilters = (filters) => {
  setNavigationState({ serviceFilters: filters });
};

const getServiceFilterIds = () => getNavigationState().serviceFilterIds;

const setServiceFilterIds = (type, ids) => {
  if (!Object.hasOwn(DEFAULT_SERVICE_FILTER_IDS, type)) return;
  setNavigationState({ serviceFilterIds: { [type]: ids ?? null } });
};

const setServiceFilterIdsBatch = (filters) => {
  setNavigationState({ serviceFilterIds: filters });
};

const subscribeServiceFilters = (listener) => subscribeNavigationState((state) => listener(state.serviceFilters));
const subscribeServiceFilterIds = (listener) => subscribeNavigationState((state) => listener(state.serviceFilterIds));

export {
  bindNavigationStorageListener,
  getNavigationState,
  subscribeNavigationState,
  getSelectedVehicle,
  setSelectedVehicle,
  subscribeSelectedVehicle,
  getServiceFilters,
  setServiceFilter,
  setServiceFilters,
  getServiceFilterIds,
  setServiceFilterIds,
  setServiceFilterIdsBatch,
  subscribeServiceFilters,
  subscribeServiceFilterIds,
};
