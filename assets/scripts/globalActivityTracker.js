import { logAdminEvent } from './adminAudit.js';
import { getWebAdminSession } from './web-admin-session.js';

const ACTIVITY_TABLE = 'web_activity';
const MAX_LABEL_LENGTH = 90;
const DEDUPE_WINDOW_MS = 8000;
const seenEvents = new Map();

const sanitizeLabel = (value) => String(value || '').replace(/\s+/g, ' ').trim().slice(0, MAX_LABEL_LENGTH);

const shouldSkipEvent = (key) => {
  const now = Date.now();
  const last = seenEvents.get(key);
  if (last && now - last < DEDUPE_WINDOW_MS) return true;
  seenEvents.set(key, now);
  return false;
};

const resolveProfile = () => {
  const localAdminSession = getWebAdminSession();
  return {
    email: localAdminSession?.email || null,
    role: String(document.body?.dataset.userRole || window.currentUserRole || localAdminSession?.role || 'guest').toLowerCase(),
    status: String(document.body?.dataset.userStatus || window.currentUserStatus || localAdminSession?.status || 'unknown').toLowerCase(),
    pagePath: window.location.pathname || '/',
  };
};

const track = async ({
  action = 'activity',
  summary = 'Web activity',
  source = 'global-tracker',
  details = {},
  dedupeKey = null,
} = {}) => {
  const profile = resolveProfile();
  const key = dedupeKey || `${action}:${summary}:${profile.pagePath}`;
  if (shouldSkipEvent(key)) return false;

  return logAdminEvent({
    action,
    tableName: ACTIVITY_TABLE,
    summary,
    source,
    details,
    profileEmail: profile.email,
    profileRole: profile.role,
    profileStatus: profile.status,
    pagePath: profile.pagePath,
    actor: profile.email || null,
  });
};

const describeTarget = (target) => {
  if (!target) return 'unknown';
  const id = sanitizeLabel(target.id);
  const name = sanitizeLabel(target.getAttribute?.('name'));
  const label = sanitizeLabel(target.dataset?.activityLabel || target.getAttribute?.('aria-label') || target.textContent);
  const tag = String(target.tagName || 'element').toLowerCase();

  if (id) return `${tag}#${id}`;
  if (name) return `${tag}[name=${name}]`;
  if (label) return `${tag}:${label}`;
  return tag;
};

const bindPageTracking = () => {
  const page = window.location.pathname || '/';
  track({
    action: 'page_view',
    summary: `Viewed ${page}`,
    details: { href: window.location.href },
    dedupeKey: `page_view:${page}`,
  });
};

const bindClickTracking = () => {
  document.addEventListener(
    'click',
    (event) => {
      const clickable = event.target?.closest?.('a,button,[data-track-activity]');
      if (!clickable) return;
      if (clickable.hasAttribute('data-no-activity-track')) return;

      const targetLabel = describeTarget(clickable);
      const href = clickable.getAttribute?.('href');
      track({
        action: 'click',
        summary: `Clicked ${targetLabel}`,
        details: {
          target: targetLabel,
          href: href || null,
        },
        dedupeKey: `click:${targetLabel}:${window.location.pathname}`,
      });
    },
    true
  );
};

const bindFormTracking = () => {
  document.addEventListener(
    'submit',
    (event) => {
      const form = event.target;
      if (!(form instanceof HTMLFormElement)) return;
      if (form.hasAttribute('data-no-activity-track')) return;

      const formName = sanitizeLabel(form.id || form.getAttribute('name') || form.getAttribute('action') || 'form');
      track({
        action: 'submit',
        summary: `Submitted form ${formName}`,
        details: {
          form: formName,
          method: sanitizeLabel(form.method || 'GET'),
          actionUrl: sanitizeLabel(form.getAttribute('action') || ''),
        },
        dedupeKey: `submit:${formName}:${window.location.pathname}`,
      });
    },
    true
  );
};

export const initGlobalActivityTracker = () => {
  if (typeof window === 'undefined') return;
  if (window.__techlocActivityTrackerInitialized) return;
  window.__techlocActivityTrackerInitialized = true;

  window.techlocLogActivity = (payload = {}) => track(payload);

  const boot = () => {
    bindPageTracking();
    bindClickTracking();
    bindFormTracking();
  };

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', boot, { once: true });
  } else {
    boot();
  }
};

export default initGlobalActivityTracker;
