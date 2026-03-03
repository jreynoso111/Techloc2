export const APP_SETTINGS_STORAGE_KEY = 'techlocAppSettings:v1';

export const DEFAULT_APP_SETTINGS = Object.freeze({
  vehicleMarkerStalePingDays: 3,
  vehicleMarkerStaleOpacity: 0.55
});

const clampNumber = (value, { min = Number.NEGATIVE_INFINITY, max = Number.POSITIVE_INFINITY, fallback = 0 } = {}) => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(min, Math.min(max, parsed));
};

export const normalizeAppSettings = (input = {}) => {
  const source = (input && typeof input === 'object') ? input : {};
  return {
    vehicleMarkerStalePingDays: Math.round(
      clampNumber(source.vehicleMarkerStalePingDays, {
        min: 0,
        max: 365,
        fallback: DEFAULT_APP_SETTINGS.vehicleMarkerStalePingDays
      })
    ),
    vehicleMarkerStaleOpacity: clampNumber(source.vehicleMarkerStaleOpacity, {
      min: 0.2,
      max: 1,
      fallback: DEFAULT_APP_SETTINGS.vehicleMarkerStaleOpacity
    })
  };
};

const safeLocalStorage = () => {
  if (typeof window === 'undefined' || !window.localStorage) return null;
  return window.localStorage;
};

export const getAppSettings = () => {
  const storage = safeLocalStorage();
  if (!storage) return { ...DEFAULT_APP_SETTINGS };
  try {
    const raw = storage.getItem(APP_SETTINGS_STORAGE_KEY);
    if (!raw) return { ...DEFAULT_APP_SETTINGS };
    const parsed = JSON.parse(raw);
    return normalizeAppSettings({ ...DEFAULT_APP_SETTINGS, ...parsed });
  } catch (_error) {
    return { ...DEFAULT_APP_SETTINGS };
  }
};

export const saveAppSettings = (settings = {}) => {
  const normalized = normalizeAppSettings(settings);
  const storage = safeLocalStorage();
  if (storage) {
    storage.setItem(APP_SETTINGS_STORAGE_KEY, JSON.stringify(normalized));
  }
  return normalized;
};

export const updateAppSettings = (partial = {}) => {
  const merged = { ...getAppSettings(), ...(partial || {}) };
  return saveAppSettings(merged);
};

