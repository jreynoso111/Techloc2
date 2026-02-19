import { SUPABASE_KEY, SUPABASE_URL, assertSupabaseTarget } from './env.js';
import { supabase as sharedSupabaseClient } from '../js/supabaseClient.js';
import {
  buildWebAdminSession,
  clearWebAdminSession,
  getWebAdminAccess,
  isWebAdminSession,
} from './web-admin-session.js';

const LOGIN_PAGE = new URL('../../pages/login.html', import.meta.url).toString();
const ADMIN_HOME = new URL('../../pages/admin/index.html', import.meta.url).toString();
const CONTROL_VIEW = new URL('../../pages/control-map.html', import.meta.url).toString();


const supabaseClient =
  sharedSupabaseClient ||
  window.supabaseClient ||
  (window.supabase?.createClient && SUPABASE_URL && SUPABASE_KEY && assertSupabaseTarget(SUPABASE_URL, SUPABASE_KEY)
    ? window.supabase.createClient(SUPABASE_URL, SUPABASE_KEY, {
        auth: {
          persistSession: true,
          autoRefreshToken: true,
        },
      })
    : null);

const hasSupabaseAuth =
  Boolean(supabaseClient?.auth) && typeof supabaseClient.auth.getSession === 'function';

if (!hasSupabaseAuth) {
  console.warn('Supabase auth unavailable in admin guard. Using local web-admin fallback when available.');
}

if (supabaseClient) {
  window.supabaseClient = supabaseClient;
}

let currentSession = null;
let initialSessionResolved = false;
let initializationPromise = null;
let cachedUserRole = null;
let cachedUserStatus = null;
let cachedUserProfile = null;
const sessionListeners = new Set();
const broadcastRoleStatus = (role, status) =>
  window.dispatchEvent(
    new CustomEvent('auth:role-updated', {
      detail: { role: role ?? null, status: status ?? null },
    }),
  );

const HOME_PAGE = new URL('../../index.html', import.meta.url).toString();

const redirectToLogin = () => {
  window.location.href = LOGIN_PAGE;
};

const redirectToAdminHome = () => {
  window.location.href = ADMIN_HOME;
};

const redirectToControlView = () => {
  window.location.href = CONTROL_VIEW;
};

const redirectToHome = () => {
  window.location.href = HOME_PAGE;
};

const notifySessionListeners = (session) => {
  sessionListeners.forEach((listener) => listener(session));
};

const roleAllowsDashboard = (role) => ['administrator', 'moderator'].includes(String(role || '').toLowerCase());

const setSession = (session) => {
  currentSession = session;
  notifySessionListeners(session);
};

const getEffectiveSession = (session) => session || buildWebAdminSession() || null;

const getUserAccess = async (session) => {
  if (window.currentUserRole && window.currentUserStatus) {
    return { role: window.currentUserRole, status: window.currentUserStatus };
  }

  if (cachedUserRole && cachedUserStatus) {
    window.currentUserRole = cachedUserRole;
    window.currentUserStatus = cachedUserStatus;
    broadcastRoleStatus(cachedUserRole, cachedUserStatus);
    return { role: cachedUserRole, status: cachedUserStatus };
  }

  const userId = session?.user?.id;
  if (!userId) {
    window.currentUserRole = 'user';
    window.currentUserStatus = 'active';
    broadcastRoleStatus('user', 'active');
    return { role: 'user', status: 'active' };
  }

  if (isWebAdminSession(session)) {
    const localAccess = getWebAdminAccess();
    const role = (localAccess?.role || 'administrator').toLowerCase();
    const status = (localAccess?.status || 'active').toLowerCase();
    cachedUserRole = role;
    cachedUserStatus = status;
    window.currentUserRole = role;
    window.currentUserStatus = status;
    broadcastRoleStatus(role, status);
    return { role, status };
  }

  if (!supabaseClient?.from) {
    return { role: 'user', status: 'active' };
  }

  const { data, error } = await supabaseClient
    .from('profiles')
    .select('role, status')
    .eq('id', userId)
    .single();

  if (error) {
    console.warn('Unable to fetch user role', error);
    return { role: 'user', status: 'active' };
  }

  const normalizedRole = (data?.role || 'user').toLowerCase();
  const normalizedStatus = (data?.status || 'active').toLowerCase();
  cachedUserRole = normalizedRole;
  cachedUserStatus = normalizedStatus;
  window.currentUserRole = normalizedRole;
  window.currentUserStatus = normalizedStatus;
  broadcastRoleStatus(normalizedRole, normalizedStatus);
  return { role: normalizedRole, status: normalizedStatus };
};

const getUserProfile = async (session) => {
  if (window.currentUserProfile) return window.currentUserProfile;
  if (cachedUserProfile) {
    window.currentUserProfile = cachedUserProfile;
    return cachedUserProfile;
  }

  const userId = session?.user?.id;
  if (!userId) {
    cachedUserProfile = null;
    return null;
  }

  if (isWebAdminSession(session)) {
    const localAccess = getWebAdminAccess();
    const profile = {
      name: 'Web Admin',
      email: localAccess?.email || session?.user?.email || null,
    };
    cachedUserProfile = profile;
    window.currentUserProfile = profile;
    return profile;
  }

  if (!supabaseClient?.from) {
    return null;
  }

  const { data, error } = await supabaseClient.from('profiles').select('name, email').eq('id', userId).single();

  if (error) {
    console.warn('Unable to fetch user profile', error);
    return null;
  }

  cachedUserProfile = data || null;
  window.currentUserProfile = cachedUserProfile;
  return cachedUserProfile;
};

const recordLastConnection = async (session) => {
  const userId = session?.user?.id;
  if (!userId) return;
  if (isWebAdminSession(session)) return;
  if (!supabaseClient?.from) return;
  if (typeof localStorage === 'undefined') return;

  const storageKey = `techloc:last-connection:${userId}`;
  const now = Date.now();
  const lastRecorded = Number(localStorage.getItem(storageKey) || 0);
  if (Number.isFinite(lastRecorded) && lastRecorded && now - lastRecorded < 5 * 60 * 1000) return;

  localStorage.setItem(storageKey, String(now));
  const { error } = await supabaseClient
    .from('profiles')
    .update({ last_connection: new Date(now).toISOString() })
    .eq('id', userId);

  if (error) {
    console.warn('Unable to record last connection', error);
  }
};

const updateHeaderAccount = async (session) => {
  const accountName = document.querySelector('[data-account-name]');
  if (!accountName) return;

  if (!session?.user) {
    accountName.textContent = 'Account';
    return;
  }

  const profile = await getUserProfile(session);
  const fallbackEmail = session.user.email || profile?.email || 'Account';
  const label = session.user.email || profile?.email || fallbackEmail;
  accountName.textContent = label;
};

const applyRoleVisibility = (role) => {
  const adminOnly = document.querySelectorAll('[data-admin-only]');
  adminOnly.forEach((item) => {
    if (role === 'administrator') {
      item.classList.remove('hidden');
      item.removeAttribute('aria-hidden');
    } else {
      item.classList.add('hidden');
      item.setAttribute('aria-hidden', 'true');
    }
  });
};

const isAuthorizedUser = (session) => Boolean(session);

const routeInfo = (() => {
  const path = window.location.pathname.toLowerCase();
  return {
    isAdminRoute: path.includes('/admin/'),
    isAdminDashboard:
      path.endsWith('/admin/index.html') || path.endsWith('/admin/') || path.endsWith('admin/index.html'),
    isControlView: path.endsWith('/pages/control-map.html') || path.endsWith('pages/control-map.html'),
    isLoginPage: path.endsWith('/login.html') || path.endsWith('login.html'),
    isProfilesPage: path.includes('/admin/profiles.html'),
  };
})();

const getCurrentSession = async () => {
  await initializeAuthState();
  return currentSession;
};

const initializeAuthState = () => {
  if (initializationPromise) return initializationPromise;

  initializationPromise = (async () => {
    try {
      if (hasSupabaseAuth) {
        const { data } = await supabaseClient.auth.getSession();
        setSession(getEffectiveSession(data?.session ?? null));
      } else {
        setSession(getEffectiveSession(null));
      }
    } catch (error) {
      console.error('Session prefetch error', error);
      setSession(getEffectiveSession(null));
    } finally {
      initialSessionResolved = true;
    }

    if (hasSupabaseAuth && typeof supabaseClient.auth.onAuthStateChange === 'function') {
      supabaseClient.auth.onAuthStateChange((event, session) => {
        if (event === 'SIGNED_OUT') {
          clearWebAdminSession();
        }

        const effectiveSession = getEffectiveSession(session);
        setSession(effectiveSession);

        if (!effectiveSession) {
          cachedUserRole = null;
          cachedUserStatus = null;
          cachedUserProfile = null;
          window.currentUserRole = null;
          window.currentUserStatus = null;
          window.currentUserProfile = null;
          broadcastRoleStatus(null, null);
        }

        if (event === 'SIGNED_OUT') {
          const isProtectedRoute = routeInfo.isAdminRoute || routeInfo.isControlView;
          if (isProtectedRoute && !routeInfo.isLoginPage) {
            redirectToLogin();
          }
        }
      });
    }
  })();

  return initializationPromise;
};

const waitForAuthorizedSession = () =>
  new Promise((resolve, reject) => {
    let cleanedUp = false;

    const cleanup = () => {
      cleanedUp = true;
      sessionListeners.delete(checkSession);
    };

    const handleAuthorized = (session) => {
      cleanup();
      resolve(session);
    };

    const handleUnauthorized = async (reason) => {
      cleanup();
      clearWebAdminSession();
      if (hasSupabaseAuth && typeof supabaseClient.auth.signOut === 'function') {
        await supabaseClient.auth.signOut();
      }
      redirectToLogin();
      reject(new Error(reason));
    };

    const checkSession = (session) => {
      const authorized = isAuthorizedUser(session);
      if (authorized) {
        handleAuthorized(session);
        return;
      }

      if (session === null && initialSessionResolved && !cleanedUp) {
        handleUnauthorized('No active Supabase session');
      }
    };

    initializeAuthState()
      .then(() => {
        if (isAuthorizedUser(currentSession)) {
          handleAuthorized(currentSession);
          return;
        }
        sessionListeners.add(checkSession);
        checkSession(currentSession);
      })
      .catch((error) => {
        console.error('Authentication initialization failed', error);
        handleUnauthorized('Initialization failed');
      });
  });

const requireSession = async () => {
  const session = await waitForAuthorizedSession();
  return session;
};

const ensureLogoutButton = () => {
  let logoutButton = document.querySelector('[data-admin-logout]');

  if (!logoutButton) {
    const headerActions =
      document.querySelector('[data-site-header] [data-admin-actions]') ||
      document.querySelector('[data-site-header] .md\\:flex') ||
      document.querySelector('[data-site-header] .flex.items-center.justify-between');
    if (!headerActions) return null;

    logoutButton = document.createElement('button');
    logoutButton.type = 'button';
    logoutButton.dataset.adminLogout = 'true';
    logoutButton.className =
      'hidden items-center gap-2 rounded-full border border-red-700 px-3 py-1 text-xs font-semibold uppercase tracking-wide text-red-200 transition hover:border-red-400 hover:text-white';
    logoutButton.innerHTML = '<span>Logout</span>';
    headerActions.appendChild(logoutButton);
  }

  return logoutButton;
};

const setupLogoutButton = () => {
  const logoutButton = ensureLogoutButton();
  if (!logoutButton) return;

  logoutButton.classList.remove('hidden');
  if (logoutButton.dataset.bound === 'true') return;

  logoutButton.dataset.bound = 'true';
  logoutButton.addEventListener('click', async () => {
    clearWebAdminSession();
    if (hasSupabaseAuth && typeof supabaseClient.auth.signOut === 'function') {
      const { error } = await supabaseClient.auth.signOut();
      if (error) {
        console.error('Supabase sign out error', error);
        return;
      }
    }
    redirectToLogin();
  });
};

const waitForDom = () =>
  new Promise((resolve) => {
    if (document.readyState !== 'loading') {
      resolve();
      return;
    }
    document.addEventListener('DOMContentLoaded', () => resolve(), { once: true });
  });

const waitForPageLoad = () =>
  new Promise((resolve) => {
    if (document.readyState === 'complete') {
      resolve();
      return;
    }
    window.addEventListener('load', () => resolve(), { once: true });
  });

const applyLoadingState = () => {
  const protectedBlocks = document.querySelectorAll('[data-auth-protected]');
  protectedBlocks.forEach((block) => {
    block.classList.add('hidden');
    block.setAttribute('aria-hidden', 'true');
  });

  const loading = document.querySelector('[data-auth-loading]');
  if (loading) {
    loading.classList.remove('hidden');
  }
};

const revealAuthorizedUi = () => {
  const loading = document.querySelector('[data-auth-loading]');
  if (loading) {
    loading.remove();
  }

  const protectedBlocks = document.querySelectorAll('[data-auth-protected]');
  protectedBlocks.forEach((block) => {
    block.classList.remove('hidden');
    block.removeAttribute('aria-hidden');
  });

  const gatedItems = document.querySelectorAll('[data-auth-visible]');
  gatedItems.forEach((item) => item.classList.remove('hidden'));
};

const syncNavigationVisibility = async (sessionFromEvent = null) => {
  await waitForDom();
  await initializeAuthState();

  const navItems = document.querySelectorAll('[data-auth-visible]');
  const guestItems = document.querySelectorAll('[data-auth-guest]');
  if (!navItems.length && !guestItems.length) return;

  const session = sessionFromEvent ?? currentSession;
  const authorized = isAuthorizedUser(session);
  const { role, status } = authorized ? await getUserAccess(session) : { role: 'user', status: 'active' };

  if (status === 'suspended' && (routeInfo.isAdminRoute || routeInfo.isControlView)) {
    clearWebAdminSession();
    if (hasSupabaseAuth && typeof supabaseClient.auth.signOut === 'function') {
      await supabaseClient.auth.signOut();
    }
    redirectToLogin();
    return;
  }

  applyRoleVisibility(role);

  if (authorized) {
    navItems.forEach((item) => item.classList.remove('hidden'));
    guestItems.forEach((item) => item.classList.add('hidden'));
    setupLogoutButton();
    await updateHeaderAccount(session);
    await recordLastConnection(session);
  } else {
    navItems.forEach((item) => item.classList.add('hidden'));
    guestItems.forEach((item) => item.classList.remove('hidden'));
    const logoutButton = ensureLogoutButton();
    if (logoutButton) {
      logoutButton.classList.add('hidden');
    }
    await updateHeaderAccount(null);
  }
};

const enforceAdminGuard = async () => {
  await waitForDom();
  applyLoadingState();
  const session = await requireSession();
  const { role, status } = await getUserAccess(session);
  setupLogoutButton();
  applyRoleVisibility(role);

  if (status === 'suspended') {
    clearWebAdminSession();
    if (hasSupabaseAuth && typeof supabaseClient.auth.signOut === 'function') {
      await supabaseClient.auth.signOut();
    }
    redirectToLogin();
    return session;
  }

  if (routeInfo.isAdminRoute && !roleAllowsDashboard(role)) {
    redirectToHome();
    return session;
  }

  await waitForPageLoad();
  if (routeInfo.isAdminDashboard) {
    window.adminAuthReady = true;
    window.dispatchEvent(new Event('admin:auth-ready'));
    return session;
  }

  revealAuthorizedUi();
  return session;
};

const startNavigationSync = () => {
  const handleNavigationSync = (session) =>
    syncNavigationVisibility(session).catch((error) => console.error('Navigation auth sync failed', error));

  sessionListeners.add(handleNavigationSync);
  initializeAuthState()
    .then(() => handleNavigationSync(currentSession))
    .catch((error) => console.error('Navigation initialization failed', error));
};

const autoStart = () => {
  initializeAuthState();

  if ((routeInfo.isAdminRoute || routeInfo.isControlView) && !routeInfo.isLoginPage) {
    enforceAdminGuard().catch((error) => console.error('Authentication guard failed', error));
  }

  startNavigationSync();
};

autoStart();

export {
  supabaseClient,
  enforceAdminGuard,
  requireSession,
  redirectToLogin,
  redirectToAdminHome,
  redirectToHome,
  redirectToControlView,
  setupLogoutButton,
  LOGIN_PAGE,
  ADMIN_HOME,
  HOME_PAGE,
};
