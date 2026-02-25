import { supabase as supabaseClient } from '../js/supabaseClient.js';
import {
  clearWebAdminSession,
} from './web-admin-session.js';

(function () {
  const hasSupabaseAuth =
    Boolean(supabaseClient?.auth) && typeof supabaseClient.auth.getSession === 'function';
  if (!hasSupabaseAuth) {
    console.warn('Supabase session APIs unavailable.');
  }
  const PROFILE_LOOKUP_TIMEOUT_MS = 2200;

  const whenDomReady = new Promise((resolve) => {
    if (document.readyState !== 'loading') {
      resolve();
      return;
    }
    document.addEventListener('DOMContentLoaded', () => resolve(), { once: true });
  });

  const navIds = {
    home: 'nav-home',
    control: 'nav-control-view',
    dashboard: 'nav-dashboard',
    services: 'nav-services',
    login: 'nav-login',
    logout: 'nav-logout',
  };

  const getNavElement = (key) => document.getElementById(navIds[key]);

  const roleAllowsDashboard = (role) => ['administrator', 'moderator'].includes(String(role || '').toLowerCase());
  const roleAllowsServiceRequests = (role) => String(role || '').toLowerCase() === 'administrator';

  // Rutas protegidas (control map served from /pages/control-map.html; root redirect removed)
  const protectedRoutes = [
    (path) => path.endsWith('/pages/control-map.html') || path.endsWith('pages/control-map.html'),
    (path) => path.endsWith('/services-request.html') || path.endsWith('services-request.html'),
    (path) => path.includes('/admin/'),
  ];

  const isServiceRequestPath = () => {
    const path = window.location.pathname.toLowerCase();
    return path.endsWith('/services-request.html') || path.endsWith('services-request.html');
  };

  const mapsTo = (page) => {
    const normalizedPage = page.startsWith('/') ? page.slice(1) : page;
    const path = window.location.pathname;
    const normalizedPath = path.toLowerCase();
    const pagesIndex = normalizedPath.indexOf('/pages/');
    const basePath = pagesIndex !== -1 ? path.slice(0, pagesIndex + 1) : path.slice(0, path.lastIndexOf('/') + 1);
    return `${basePath}${normalizedPage}`;
  };

  const isAdminRoute = window.location.pathname.toLowerCase().includes('/admin/');
  if (isAdminRoute) {
    whenDomReady.then(() => {
      const homeLink = getNavElement('home');
      const controlLink = getNavElement('control');
      const dashboardLink = getNavElement('dashboard');
      const servicesLink = getNavElement('services');
      const loginLink = getNavElement('login');
      if (homeLink) homeLink.href = mapsTo('index.html');
      if (controlLink) controlLink.href = mapsTo('pages/control-map.html');
      if (servicesLink) servicesLink.href = mapsTo('pages/admin/services.html');
      if (dashboardLink) dashboardLink.href = mapsTo('pages/admin/index.html');
      if (loginLink) loginLink.href = mapsTo('pages/login.html');
    });
    return;
  }

  const withTimeout = (promise, timeoutMs = PROFILE_LOOKUP_TIMEOUT_MS, label = 'operation') =>
    new Promise((resolve, reject) => {
      const timeoutId = setTimeout(() => {
        const timeoutError = new Error(`${label} timed out after ${timeoutMs}ms`);
        timeoutError.name = 'TimeoutError';
        reject(timeoutError);
      }, timeoutMs);

      Promise.resolve(promise)
        .then((value) => {
          clearTimeout(timeoutId);
          resolve(value);
        })
        .catch((error) => {
          clearTimeout(timeoutId);
          reject(error);
        });
    });

  const getEffectiveSession = async () => {
    if (!hasSupabaseAuth) return null;

    try {
      const { data, error } = await supabaseClient.auth.getSession();
      if (error) {
        console.warn('Supabase getSession warning:', error);
      }

      if (data?.session) return data.session;

      if (typeof supabaseClient.auth.refreshSession === 'function') {
        const { data: refreshed, error: refreshError } = await supabaseClient.auth.refreshSession();
        if (refreshError) {
          console.warn('Supabase refreshSession warning:', refreshError);
          return null;
        }
        return refreshed?.session || null;
      }
    } catch (error) {
      console.warn('Supabase session resolution warning:', error);
    }

    return null;
  };

  const normalizeAccessValue = (value, fallback) => {
    const normalized = String(value || '').trim().toLowerCase();
    return normalized || fallback;
  };

  const resolveFallbackProfileForSession = (session) => {
    const sessionRole = session?.user?.app_metadata?.role || session?.user?.user_metadata?.role || null;
    const sessionStatus = session?.user?.app_metadata?.status || session?.user?.user_metadata?.status || null;
    const fallbackRole = normalizeAccessValue(window.currentUserRole || sessionRole, 'user');
    const fallbackStatus = normalizeAccessValue(window.currentUserStatus || sessionStatus, 'active');
    return {
      role: fallbackRole,
      status: fallbackStatus,
      email: session?.user?.email || null,
    };
  };

  // --- NUEVO: Función para obtener el rol y estado desde la tabla profiles ---
  const fetchUserProfile = async (userId, fallbackProfile = { role: 'user', status: 'active', email: null }) => {
    try {
      const { data, error } = await withTimeout(
        supabaseClient
          .from('profiles')
          .select('role, status, email')
          .eq('id', userId)
          .maybeSingle(),
        PROFILE_LOOKUP_TIMEOUT_MS,
        'Profile lookup',
      );

      if (error || !data)
        return fallbackProfile; // Valores por defecto si falla

      return {
        role: normalizeAccessValue(data.role, fallbackProfile.role || 'user'),
        status: normalizeAccessValue(data.status, fallbackProfile.status || 'active'),
        email: data.email || fallbackProfile.email || null,
      };
    } catch (err) {
      const isTimeout = String(err?.name || '') === 'TimeoutError';
      console.warn(isTimeout ? 'Profile lookup timed out; using fallback role.' : 'Error fetching role:', err);
      return fallbackProfile;
    }
  };

  const resolveProfileForSession = async (session) => {
    if (!session?.user) {
      return {
        role: 'user',
        status: 'active',
        email: null,
      };
    }

    const fallbackProfile = resolveFallbackProfileForSession(session);
    return fetchUserProfile(session.user.id, fallbackProfile);
  };

  const waitForAccountLabel = (timeoutMs = 3000) =>
    new Promise((resolve) => {
      const start = Date.now();
      const check = () => {
        const label = document.querySelector('[data-account-name]');
        if (label) {
          resolve(label);
          return;
        }
        if (Date.now() - start >= timeoutMs) {
          resolve(null);
          return;
        }
        setTimeout(check, 100);
      };
      check();
    });

  const updateHeaderAccount = async (session, profileEmail) => {
    const accountName = await waitForAccountLabel();
    if (!accountName) return;

    if (!session?.user) {
      accountName.textContent = 'Account';
      return;
    }

    const label = session.user.email || profileEmail || 'Account';
    accountName.textContent = label;
  };

  const applyAccessState = (role, status) => {
    const normalizedRole = normalizeAccessValue(role, 'user');
    const normalizedStatus = normalizeAccessValue(status, 'active');
    window.currentUserRole = normalizedRole;
    window.currentUserStatus = normalizedStatus;
    document.body.setAttribute('data-user-role', normalizedRole);
    document.body.setAttribute('data-user-status', normalizedStatus);
    window.dispatchEvent(
      new CustomEvent('auth:role-ready', { detail: { role: normalizedRole, status: normalizedStatus } })
    );
  };

  const clearAccessState = () => {
    window.currentUserRole = null;
    window.currentUserStatus = null;
    document.body.removeAttribute('data-user-role');
    document.body.removeAttribute('data-user-status');
  };

  const toggleDashboardLinks = (hasSession, role, status) =>
    whenDomReady.then(() => {
      const isSuspended = status === 'suspended';
      const canShowDashboard = hasSession && !isSuspended && roleAllowsDashboard(role);
      const dashboardLinks = document.querySelectorAll('[data-dashboard-link]');

      dashboardLinks.forEach((link) => {
        if (!link) return;

        if (canShowDashboard) {
          link.classList.remove('hidden');
          link.removeAttribute('aria-hidden');
          link.removeAttribute('tabindex');
          link.style.pointerEvents = '';
        } else {
          link.classList.add('hidden');
          link.setAttribute('aria-hidden', 'true');
          link.setAttribute('tabindex', '-1');
          link.style.pointerEvents = 'none';
        }
      });
    });

  const updateNav = (hasSession, role, status) => // <--- Modificado para aceptar 'role' y 'status'
    whenDomReady.then(() => {
      const homeLink = getNavElement('home');
      const controlLink = getNavElement('control');
      const dashboardLink = getNavElement('dashboard');
      const servicesLink = getNavElement('services');
      const loginLink = getNavElement('login');
      const logoutButton = getNavElement('logout');

      if (homeLink) homeLink.href = mapsTo('index.html');
      if (controlLink) controlLink.href = mapsTo('pages/control-map.html');
      if (servicesLink) servicesLink.href = mapsTo('pages/admin/services.html');
      if (dashboardLink) dashboardLink.href = mapsTo('pages/admin/index.html');
      if (loginLink) loginLink.href = mapsTo('pages/login.html');

      const isSuspended = status === 'suspended';
      const canShowDashboard = hasSession && !isSuspended && roleAllowsDashboard(role);
      const canShowServices = hasSession && !isSuspended && roleAllowsServiceRequests(role);

      if (hasSession && !isSuspended) {
        // Lógica de visualización basada en sesión
        controlLink?.classList.remove('hidden');
        controlLink?.classList.add('md:inline-flex');

        if (canShowServices) {
          servicesLink?.classList.remove('hidden');
        } else {
          servicesLink?.classList.add('hidden');
        }

        if (canShowDashboard) {
          dashboardLink?.classList.remove('hidden');
          dashboardLink?.classList.add('md:inline-flex');
        } else {
          dashboardLink?.classList.add('hidden');
          dashboardLink?.classList.remove('md:inline-flex');
        }

        logoutButton?.classList.remove('hidden');
        loginLink?.classList.add('hidden');
      } else {
        controlLink?.classList.add('hidden');
        controlLink?.classList.remove('md:inline-flex');
        servicesLink?.classList.add('hidden');
        dashboardLink?.classList.add('hidden');
        dashboardLink?.classList.remove('md:inline-flex');
        logoutButton?.classList.add('hidden');
        loginLink?.classList.remove('hidden');
      }

      toggleDashboardLinks(hasSession, role, status);
    });

  const toggleProtectedBlocks = (hasSession) =>
    whenDomReady.then(() => {
      const loading = document.querySelector('[data-auth-loading]');
      const protectedBlocks = document.querySelectorAll('[data-auth-protected]');
      const loadingMode = loading?.dataset?.authLoadingMode || '';
      const deferLoadingRemoval = loadingMode === 'defer';

      if (hasSession) {
        if (!deferLoadingRemoval) loading?.remove();
        protectedBlocks.forEach((block) => {
          block.classList.remove('hidden');
          block.removeAttribute('aria-hidden');
        });
        return;
      }

      loading?.classList.remove('hidden');
      protectedBlocks.forEach((block) => {
        block.classList.add('hidden');
        block.setAttribute('aria-hidden', 'true');
      });
    });

  const isProtectedRoute = () => {
    const path = window.location.pathname.toLowerCase();
    return protectedRoutes.some((matcher) => matcher(path));
  };

  const enforceRouteProtection = (hasSession, role) => {
    if (!hasSession && isProtectedRoute()) {
      window.location.replace(mapsTo('pages/login.html'));
      return;
    }

    if (hasSession && isServiceRequestPath() && !roleAllowsServiceRequests(role)) {
      window.location.replace(mapsTo('index.html'));
      return;
    }

    if (window.currentUserStatus === 'suspended' && isProtectedRoute()) {
      clearWebAdminSession();
      if (hasSupabaseAuth && typeof supabaseClient.auth.signOut === 'function') {
        supabaseClient.auth.signOut();
      }
      window.location.replace(mapsTo('pages/login.html'));
    }
  };

  const bindLogout = () => {
    whenDomReady.then(() => {
      const logoutButton = getNavElement('logout');
      if (!logoutButton || logoutButton.dataset.bound === 'true') return;

        logoutButton.dataset.bound = 'true';
        logoutButton.addEventListener('click', async (event) => {
          event.preventDefault();
          try {
            clearWebAdminSession();
            if (hasSupabaseAuth && typeof supabaseClient.auth.signOut === 'function') {
              await supabaseClient.auth.signOut();
            }
            // Limpiar rol global al salir
            window.currentUserRole = null;
            window.currentUserStatus = null;
            document.body.removeAttribute('data-user-role');
            document.body.removeAttribute('data-user-status');
          } catch (error) {
            console.error('Error during Supabase sign out', error);
          }
        });
      });
  };

  const startAuthFlow = async () => {
    try {
      const session = await getEffectiveSession();
      if (!session) clearWebAdminSession();
      const hasSession = Boolean(session);
      let userRole = 'user'; // Rol por defecto
      let userStatus = 'active';

      if (hasSession && session.user) {
        const fallbackProfile = resolveFallbackProfileForSession(session);
        userRole = normalizeAccessValue(fallbackProfile.role, 'user');
        userStatus = normalizeAccessValue(fallbackProfile.status, 'active');
        applyAccessState(userRole, userStatus);
        updateHeaderAccount(session, fallbackProfile.email);
      } else {
        clearAccessState();
        updateHeaderAccount(null);
      }

      const isLoginPage = window.location.pathname.toLowerCase().includes('/login.html');
      if (hasSession && isLoginPage) {
        window.location.replace(mapsTo('pages/control-map.html'));
        return;
      }

      updateNav(hasSession, userRole, userStatus); // Pasamos el rol y estado
      toggleProtectedBlocks(hasSession);
      enforceRouteProtection(hasSession, userRole);
      bindLogout();

      if (hasSession && session.user) {
        resolveProfileForSession(session)
          .then(async (profile) => {
            const resolvedRole = normalizeAccessValue(profile.role, userRole);
            const resolvedStatus = normalizeAccessValue(profile.status, userStatus);
            applyAccessState(resolvedRole, resolvedStatus);
            updateHeaderAccount(session, profile.email);
            updateNav(true, resolvedRole, resolvedStatus);
            enforceRouteProtection(true, resolvedRole);
          })
          .catch((error) => {
            console.warn('Deferred profile resolution warning', error);
          });
      }

      if (hasSupabaseAuth && typeof supabaseClient.auth.onAuthStateChange === 'function') {
        supabaseClient.auth.onAuthStateChange(async (event, session) => {
          if (event === 'SIGNED_OUT') {
            clearWebAdminSession();
          }

          const effectiveSession = session || null;
          const sessionExists = Boolean(effectiveSession);
          let updatedRole = 'user';
          let updatedStatus = 'active';

          if (sessionExists && effectiveSession.user) {
             const fallbackProfile = resolveFallbackProfileForSession(effectiveSession);
             updatedRole = normalizeAccessValue(fallbackProfile.role, 'user');
             updatedStatus = normalizeAccessValue(fallbackProfile.status, 'active');
             applyAccessState(updatedRole, updatedStatus);
             updateHeaderAccount(effectiveSession, fallbackProfile.email);

             resolveProfileForSession(effectiveSession)
               .then(async (profile) => {
                 const strictRole = normalizeAccessValue(profile.role, updatedRole);
                 const strictStatus = normalizeAccessValue(profile.status, updatedStatus);
                 applyAccessState(strictRole, strictStatus);
                 updateHeaderAccount(effectiveSession, profile.email);
                 updateNav(true, strictRole, strictStatus);
                 enforceRouteProtection(true, strictRole);
               })
               .catch((error) => {
                 console.warn('Deferred profile refresh warning', error);
               });
          } else {
             clearAccessState();
             updateHeaderAccount(null);
          }

          const onLoginPage = window.location.pathname.toLowerCase().includes('/login.html');

          updateNav(sessionExists, updatedRole, updatedStatus);
          toggleProtectedBlocks(sessionExists);
          enforceRouteProtection(sessionExists, updatedRole);

          if (event === 'SIGNED_IN' && onLoginPage) {
            window.location.replace(mapsTo('pages/control-map.html'));
            return;
          }

          if (event === 'SIGNED_OUT' && isProtectedRoute()) {
            window.location.replace(mapsTo('pages/login.html'));
          }
        });
      }
    } catch (error) {
      console.error('Failed to verify Supabase session', error);
      enforceRouteProtection(false, null);
      updateNav(false, null, null);
      toggleProtectedBlocks(false);
    }
  };

  startAuthFlow();
})();
