const STORAGE_KEY = 'techloc:web-admin-session:v1';

const canUseStorage = () => typeof window !== 'undefined' && typeof window.localStorage !== 'undefined';

const clearWebAdminSession = () => {
  if (!canUseStorage()) return;
  window.localStorage.removeItem(STORAGE_KEY);
};

const getWebAdminSession = () => {
  clearWebAdminSession();
  return null;
};

const setWebAdminSession = () => {
  clearWebAdminSession();
  return null;
};

const isWebAdminCredentials = () => false;
const buildWebAdminSession = () => null;
const isWebAdminSession = () => false;
const getWebAdminAccess = () => null;

clearWebAdminSession();

export {
  getWebAdminSession,
  setWebAdminSession,
  clearWebAdminSession,
  isWebAdminCredentials,
  buildWebAdminSession,
  isWebAdminSession,
  getWebAdminAccess,
};
