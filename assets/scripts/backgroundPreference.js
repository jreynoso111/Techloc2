import { supabase as supabaseClient } from '../js/supabaseClient.js';
import {
  BACKGROUND_STORAGE_KEY,
  normalizeBackgroundMode,
} from './backgroundManager.js';

const getLocalMode = () => normalizeBackgroundMode(localStorage.getItem(BACKGROUND_STORAGE_KEY));
const setLocalMode = (mode) =>
  localStorage.setItem(BACKGROUND_STORAGE_KEY, normalizeBackgroundMode(mode));

const getCurrentUserId = async () => {
  if (!supabaseClient?.auth?.getUser) return null;
  try {
    const {
      data: { user },
    } = await supabaseClient.auth.getUser();
    return user?.id || null;
  } catch (error) {
    console.warn('Unable to resolve current user for background preference', error);
    return null;
  }
};

const loadProfileMode = async () => {
  if (!supabaseClient) return null;

  const userId = await getCurrentUserId();
  if (!userId) return null;

  const { data, error } = await supabaseClient
    .from('profiles')
    .select('background_mode')
    .eq('id', userId)
    .maybeSingle();

  if (error) {
    console.warn('Unable to fetch background preference from profile', error);
    return null;
  }

  return data?.background_mode ? normalizeBackgroundMode(data.background_mode) : null;
};

const saveProfileMode = async (mode) => {
  if (!supabaseClient) return;
  const userId = await getCurrentUserId();
  if (!userId) return;

  const normalized = normalizeBackgroundMode(mode);
  const { error } = await supabaseClient
    .from('profiles')
    .update({ background_mode: normalized })
    .eq('id', userId);

  if (error) {
    throw error;
  }
};

export const loadPreferredBackgroundMode = async () => {
  const remoteMode = await loadProfileMode();
  if (remoteMode) {
    setLocalMode(remoteMode);
    return remoteMode;
  }

  return getLocalMode();
};

export const persistBackgroundMode = async (mode) => {
  const normalized = normalizeBackgroundMode(mode);
  setLocalMode(normalized);

  try {
    await saveProfileMode(normalized);
  } catch (error) {
    console.warn('Unable to save background preference to profile', error);
  }
};

export const clearBackgroundPreference = () => {
  localStorage.removeItem(BACKGROUND_STORAGE_KEY);
};
