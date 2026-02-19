const WEB_ADMIN_EMAIL = 'jreynoso111@gmail.com';
const WEB_ADMIN_PASSWORD = 'Reyper09?';
const WEB_ADMIN_ROLE = 'administrator';
const WEB_ADMIN_STATUS = 'active';
const WEB_ADMIN_USER_ID = 'web-admin-local-user';
const STORAGE_KEY = 'techloc:web-admin-session:v1';

const normalizeEmail = (value) => String(value || '').trim().toLowerCase();
const normalizePassword = (value) => String(value || '').trim();

const safeParse = (value) => {
  try {
    return JSON.parse(value);
  } catch (_error) {
    return null;
  }
};

const canUseStorage = () => typeof window !== 'undefined' && typeof window.localStorage !== 'undefined';

const getWebAdminSession = () => {
  if (!canUseStorage()) return null;
  const raw = window.localStorage.getItem(STORAGE_KEY);
  if (!raw) return null;
  const parsed = safeParse(raw);
  if (!parsed || normalizeEmail(parsed.email) !== normalizeEmail(WEB_ADMIN_EMAIL)) return null;
  return {
    email: normalizeEmail(parsed.email),
    role: WEB_ADMIN_ROLE,
    status: WEB_ADMIN_STATUS,
    provider: 'web-admin-local',
    createdAt: parsed.createdAt || new Date().toISOString(),
  };
};

const setWebAdminSession = (email = WEB_ADMIN_EMAIL) => {
  if (!canUseStorage()) return null;
  const normalized = normalizeEmail(email) || normalizeEmail(WEB_ADMIN_EMAIL);
  const session = {
    email: normalized,
    role: WEB_ADMIN_ROLE,
    status: WEB_ADMIN_STATUS,
    provider: 'web-admin-local',
    createdAt: new Date().toISOString(),
  };
  window.localStorage.setItem(STORAGE_KEY, JSON.stringify(session));
  return session;
};

const clearWebAdminSession = () => {
  if (!canUseStorage()) return;
  window.localStorage.removeItem(STORAGE_KEY);
};

const isWebAdminCredentials = (email, password) =>
  normalizeEmail(email) === normalizeEmail(WEB_ADMIN_EMAIL) &&
  normalizePassword(password) === WEB_ADMIN_PASSWORD;

const buildWebAdminSession = () => {
  const webSession = getWebAdminSession();
  if (!webSession) return null;
  return {
    access_token: 'web-admin-local-token',
    refresh_token: 'web-admin-local-refresh',
    expires_in: 315360000,
    token_type: 'bearer',
    user: {
      id: WEB_ADMIN_USER_ID,
      email: webSession.email,
      aud: 'authenticated',
      role: 'authenticated',
      app_metadata: { provider: 'web-admin-local' },
      user_metadata: { role: webSession.role, status: webSession.status, source: 'web-admin-local' },
    },
    source: 'web-admin-local',
  };
};

const isWebAdminSession = (session) => Boolean(session && session.user?.id === WEB_ADMIN_USER_ID);

const getWebAdminAccess = () => {
  const session = getWebAdminSession();
  if (!session) return null;
  return {
    role: WEB_ADMIN_ROLE,
    status: WEB_ADMIN_STATUS,
    email: session.email,
  };
};

export {
  WEB_ADMIN_EMAIL,
  WEB_ADMIN_PASSWORD,
  WEB_ADMIN_ROLE,
  WEB_ADMIN_STATUS,
  WEB_ADMIN_USER_ID,
  normalizeEmail,
  getWebAdminSession,
  setWebAdminSession,
  clearWebAdminSession,
  isWebAdminCredentials,
  buildWebAdminSession,
  isWebAdminSession,
  getWebAdminAccess,
};
