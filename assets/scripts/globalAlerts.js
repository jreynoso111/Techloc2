const ROOT_ID = 'techloc-global-alert-root';
const MAX_ALERTS = 8;
const DEFAULT_IGNORED_REQUEST_PATTERNS = [
  'https://ipapi.co/json/',
  'https://ipwho.is/',
  '/rest/v1/admin_change_log',
];

const toText = (value) => {
  if (value === undefined || value === null) return '';
  if (typeof value === 'string') return value;
  try {
    return JSON.stringify(value, null, 2);
  } catch (_error) {
    return String(value);
  }
};

const ensureRoot = () => {
  let root = document.getElementById(ROOT_ID);
  if (root) return root;
  root = document.createElement('div');
  root.id = ROOT_ID;
  root.style.position = 'fixed';
  root.style.top = '16px';
  root.style.right = '16px';
  root.style.zIndex = '999999';
  root.style.width = 'min(420px, calc(100vw - 24px))';
  root.style.display = 'grid';
  root.style.gap = '8px';
  root.style.maxHeight = '80vh';
  root.style.overflow = 'auto';
  document.body.appendChild(root);
  return root;
};

const buildCard = ({ title, message, details = '', level = 'error' }) => {
  const card = document.createElement('article');
  const accent = level === 'warn' ? '#f59e0b' : '#ef4444';
  card.style.border = `1px solid ${accent}`;
  card.style.borderRadius = '12px';
  card.style.background = 'rgba(2, 6, 23, 0.96)';
  card.style.padding = '10px 12px';
  card.style.boxShadow = `0 8px 30px rgba(15, 23, 42, 0.65)`;
  card.style.color = '#e2e8f0';
  card.style.fontFamily = 'ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif';
  card.style.fontSize = '12px';
  card.style.lineHeight = '1.35';

  const now = new Date();
  const stamp = `${String(now.getHours()).padStart(2, '0')}:${String(now.getMinutes()).padStart(2, '0')}:${String(now.getSeconds()).padStart(2, '0')}`;
  const safeTitle = toText(title || 'Error');
  const safeMessage = toText(message || 'Unknown issue');
  const safeDetails = toText(details);

  card.innerHTML = `
    <div style="display:flex;align-items:flex-start;justify-content:space-between;gap:8px;">
      <div style="min-width:0;">
        <div style="font-weight:800;letter-spacing:0.02em;color:${accent};">${safeTitle}</div>
        <div style="margin-top:4px;color:#f8fafc;word-break:break-word;">${safeMessage}</div>
        ${safeDetails ? `<details style="margin-top:6px;"><summary style="cursor:pointer;color:#93c5fd;">Details</summary><pre style="white-space:pre-wrap;margin:6px 0 0;color:#cbd5e1;max-height:180px;overflow:auto;">${safeDetails}</pre></details>` : ''}
        <div style="margin-top:6px;color:#94a3b8;font-size:11px;">${stamp}</div>
      </div>
      <button type="button" aria-label="Dismiss alert" style="border:1px solid #334155;background:#0f172a;color:#e2e8f0;border-radius:999px;padding:2px 8px;cursor:pointer;">x</button>
    </div>
  `;

  card.querySelector('button')?.addEventListener('click', () => card.remove());
  return card;
};

const pushAlert = (payload) => {
  if (typeof document === 'undefined') return;
  const root = ensureRoot();
  root.prepend(buildCard(payload));
  while (root.children.length > MAX_ALERTS) {
    root.removeChild(root.lastElementChild);
  }
};

export const notifyGlobalAlert = ({ title, message, details = '', level = 'error', trackActivity = true } = {}) => {
  pushAlert({ title, message, details, level });
  if (trackActivity && typeof window !== 'undefined' && typeof window.techlocLogActivity === 'function') {
    window.techlocLogActivity({
      action: level === 'warn' ? 'warning' : 'error',
      summary: `${title || 'Alert'}: ${message || 'Unknown issue'}`,
      source: 'global-alerts',
      details: {
        title: title || 'Alert',
        message: message || 'Unknown issue',
        level,
        details: String(details || '').slice(0, 400),
      },
    });
  }
};

const parsePromiseReason = (reason) => {
  if (!reason) return { message: 'Promise rejected without reason', details: '' };
  if (reason instanceof Error) return { message: reason.message, details: reason.stack || '' };
  return { message: toText(reason), details: '' };
};

const wrapFetch = () => {
  if (typeof window.fetch !== 'function') return;
  if (window.__techlocFetchWrapped) return;

  const baseFetch = window.fetch.bind(window);
  window.fetch = async (...args) => {
    const target = args?.[0];
    const requestUrl = typeof target === 'string' ? target : (target?.url || 'unknown');
    const ignoredPatterns = Array.isArray(window.__techlocIgnoredAlertRequestPatterns)
      ? window.__techlocIgnoredAlertRequestPatterns
      : DEFAULT_IGNORED_REQUEST_PATTERNS;
    const isIgnored = ignoredPatterns.some((pattern) => requestUrl.includes(pattern));
    try {
      const response = await baseFetch(...args);
      if (!response.ok && !isIgnored) {
        let bodyText = '';
        try {
          bodyText = await response.clone().text();
        } catch (_error) {
          bodyText = '';
        }
        notifyGlobalAlert({
          title: 'HTTP Error',
          message: `${response.status} ${response.statusText} while requesting ${requestUrl}`,
          details: bodyText ? bodyText.slice(0, 1000) : '',
          level: 'warn',
        });
      }
      return response;
    } catch (error) {
      if (!isIgnored) {
        notifyGlobalAlert({
          title: 'Network Error',
          message: `Request failed: ${requestUrl}`,
          details: error?.stack || error?.message || String(error),
        });
      }
      throw error;
    }
  };

  window.__techlocFetchWrapped = true;
};

const wireGlobalHandlers = () => {
  window.addEventListener('error', (event) => {
    notifyGlobalAlert({
      title: 'Runtime Error',
      message: event.message || 'Unhandled runtime exception',
      details: event.error?.stack || `${event.filename || ''}:${event.lineno || ''}:${event.colno || ''}`,
    });
  });

  window.addEventListener('unhandledrejection', (event) => {
    const parsed = parsePromiseReason(event.reason);
    notifyGlobalAlert({
      title: 'Unhandled Promise Rejection',
      message: parsed.message,
      details: parsed.details,
    });
  });

  window.addEventListener('offline', () => {
    notifyGlobalAlert({
      title: 'Connection',
      message: 'You are offline.',
      level: 'warn',
    });
  });

  window.addEventListener('online', () => {
    notifyGlobalAlert({
      title: 'Connection',
      message: 'Connection restored.',
      level: 'warn',
    });
  });
};

export const initGlobalAlerts = () => {
  if (typeof window === 'undefined') return;
  if (window.__techlocGlobalAlertsInitialized) return;
  window.__techlocGlobalAlertsInitialized = true;

  const boot = () => {
    ensureRoot();
    wrapFetch();
    wireGlobalHandlers();
    window.reportGlobalIssue = (title, message, details = '', level = 'error') =>
      notifyGlobalAlert({ title, message, details, level });
  };

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', boot, { once: true });
  } else {
    boot();
  }
};

export default initGlobalAlerts;
