import {
  APP_SETTINGS_STORAGE_KEY,
  DEFAULT_APP_SETTINGS,
  getAppSettings,
  normalizeAppSettings,
  saveAppSettings
} from './appSettings.js';
import { supabase as supabaseClient } from '../js/supabaseClient.js';
import { requireSession } from './admin-auth.js';

const thresholdInput = document.getElementById('setting-stale-days');
const opacityInput = document.getElementById('setting-stale-opacity');
const opacityValue = document.getElementById('setting-stale-opacity-value');
const settingsForm = document.getElementById('admin-settings-form');
const resetButton = document.getElementById('settings-reset');
const statusLabel = document.getElementById('settings-status');
const SETTINGS_TABLE = 'app_settings';
const SETTINGS_KEY = 'control_map';
let activeSession = null;
let remoteSettingsAvailable = true;

const isMissingSettingsTableError = (error) => {
  const code = String(error?.code || '').trim().toUpperCase();
  const message = String(error?.message || '').toLowerCase();
  return code === 'PGRST205' || message.includes("could not find the table 'public.app_settings'");
};

const isMissingSettingsTableDisplayError = (message = '') =>
  String(message || '').toLowerCase().includes('`app_settings` table is missing');

const setStatus = (message, tone = 'neutral') => {
  if (!statusLabel) return;
  statusLabel.textContent = message || '';
  statusLabel.classList.remove('text-slate-400', 'text-emerald-300', 'text-amber-300', 'text-red-300');
  if (tone === 'success') {
    statusLabel.classList.add('text-emerald-300');
    return;
  }
  if (tone === 'warning') {
    statusLabel.classList.add('text-amber-300');
    return;
  }
  if (tone === 'error') {
    statusLabel.classList.add('text-red-300');
    return;
  }
  statusLabel.classList.add('text-slate-400');
};

const renderOpacityValue = (value) => {
  if (!opacityValue) return;
  const numeric = Number(value);
  const pct = Number.isFinite(numeric) ? Math.round(numeric * 100) : Math.round(DEFAULT_APP_SETTINGS.vehicleMarkerStaleOpacity * 100);
  opacityValue.textContent = `${pct}%`;
};

const renderForm = (settings = {}) => {
  const normalized = normalizeAppSettings(settings);
  if (thresholdInput) thresholdInput.value = String(normalized.vehicleMarkerStalePingDays);
  if (opacityInput) opacityInput.value = String(normalized.vehicleMarkerStaleOpacity);
  renderOpacityValue(normalized.vehicleMarkerStaleOpacity);
};

const readForm = () => {
  const raw = {
    vehicleMarkerStalePingDays: thresholdInput?.value,
    vehicleMarkerStaleOpacity: opacityInput?.value
  };
  return normalizeAppSettings({ ...getAppSettings(), ...raw });
};

const isAdministrator = (session = null) => {
  const candidates = [
    window.currentUserRole,
    session?.user?.app_metadata?.role,
    session?.user?.user_metadata?.role,
  ];
  return candidates.some((value) => String(value || '').toLowerCase() === 'administrator');
};

const fetchRemoteSettings = async () => {
  if (!supabaseClient?.from || !remoteSettingsAvailable) return null;
  const { data, error } = await supabaseClient
    .from(SETTINGS_TABLE)
    .select('settings')
    .eq('key', SETTINGS_KEY)
    .maybeSingle();
  if (error) {
    if (isMissingSettingsTableError(error)) {
      remoteSettingsAvailable = false;
      return null;
    }
    throw error;
  }
  if (!data?.settings || typeof data.settings !== 'object') return null;
  return normalizeAppSettings({ ...DEFAULT_APP_SETTINGS, ...data.settings });
};

const saveRemoteSettings = async (settings = {}) => {
  if (!supabaseClient?.from || !remoteSettingsAvailable) return;
  const payload = normalizeAppSettings(settings);
  const row = {
    key: SETTINGS_KEY,
    settings: payload,
    updated_by: activeSession?.user?.id || null
  };
  const { error } = await supabaseClient
    .from(SETTINGS_TABLE)
    .upsert(row, { onConflict: 'key' });
  if (error) {
    if (isMissingSettingsTableError(error)) {
      remoteSettingsAvailable = false;
      throw new Error('`app_settings` table is missing in the live database. Apply the migration to enable shared settings.');
    }
    throw error;
  }
};

if (opacityInput) {
  opacityInput.addEventListener('input', () => {
    renderOpacityValue(opacityInput.value);
  });
}

if (settingsForm) {
  settingsForm.addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      const nextSettings = readForm();
      const saved = saveAppSettings(nextSettings);
      await saveRemoteSettings(saved);
      renderForm(saved);
      setStatus('Saved to the cloud and applied locally.', 'success');
    } catch (error) {
      const localSaved = saveAppSettings(readForm());
      renderForm(localSaved);
      const rawMessage = String(error?.message || '').trim();
      if (isMissingSettingsTableDisplayError(rawMessage)) {
        setStatus('Saved locally. Shared settings are not available yet, so this value is not synced.', 'warning');
      } else {
        setStatus(rawMessage || 'Saved locally, but could not persist to the cloud.', 'warning');
      }
    }
  });
}

if (resetButton) {
  resetButton.addEventListener('click', async () => {
    try {
      const saved = saveAppSettings(DEFAULT_APP_SETTINGS);
      await saveRemoteSettings(saved);
      renderForm(saved);
      setStatus('Defaults restored and synced to the cloud.', 'warning');
    } catch (error) {
      const saved = saveAppSettings(DEFAULT_APP_SETTINGS);
      renderForm(saved);
      const rawMessage = String(error?.message || '').trim();
      if (isMissingSettingsTableDisplayError(rawMessage)) {
        setStatus('Defaults restored locally. Shared settings are not available yet, so defaults are not synced.', 'warning');
      } else {
        setStatus(rawMessage || 'Defaults restored locally; cloud sync failed.', 'warning');
      }
    }
  });
}

if (typeof window !== 'undefined') {
  window.addEventListener('storage', (event) => {
    if (event?.key !== APP_SETTINGS_STORAGE_KEY) return;
    renderForm(getAppSettings());
    setStatus('Settings updated from another tab.', 'neutral');
  });
}

const initializeSettingsPage = async () => {
  try {
    activeSession = await requireSession();
    if (!isAdministrator(activeSession)) {
      setStatus('Administrator access is required for this page.', 'error');
      return;
    }

    renderForm(getAppSettings());
    setStatus('Loading settings from the cloud…', 'neutral');

    try {
      const remoteSettings = await fetchRemoteSettings();
      if (remoteSettings) {
        const saved = saveAppSettings(remoteSettings);
        renderForm(saved);
        setStatus('Loaded from the cloud.', 'neutral');
      } else if (!remoteSettingsAvailable) {
        setStatus('Shared settings are not deployed yet. Using local settings only.', 'warning');
      } else {
        setStatus('No remote settings yet. Using defaults/local values.', 'warning');
      }
    } catch (error) {
      setStatus(error?.message || 'Could not read shared settings. Using local values.', 'warning');
    }
  } catch (error) {
    setStatus(error?.message || 'Unable to initialize settings page.', 'error');
  }
};

void initializeSettingsPage();

if (window.lucide?.createIcons) {
  window.lucide.createIcons();
}
