#!/usr/bin/env node

import { createServer } from 'node:http';
import { readFile, stat } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT_DIR = path.resolve(__dirname, '..');

const PORT = Number.parseInt(process.env.PORT || '8080', 10);
const SUPABASE_URL = String(process.env.SUPABASE_URL || '').trim().replace(/\/+$/, '');
const SUPABASE_SERVICE_ROLE_KEY = String(process.env.SUPABASE_SERVICE_ROLE_KEY || '').trim();
const SUPABASE_ANON_KEY = String(
  process.env.SUPABASE_ANON_KEY || process.env.SUPABASE_PUBLISHABLE_KEY || ''
).trim();
const SUPABASE_PROJECT_REF = 'lnfmogsjvdkqgwprlmtn';

const REPAIR_HISTORY_TABLE = 'repair_history';
const ALLOWED_ROLES_RAW = String(process.env.REPAIR_HISTORY_ALLOWED_ROLES || '').trim();
const ALLOWED_ROLES = new Set(
  ALLOWED_ROLES_RAW
    .split(',')
    .map((value) => value.trim().toLowerCase())
    .filter(Boolean)
);

const MAX_BODY_BYTES = 512_000;

const JSON_HEADERS = {
  'content-type': 'application/json; charset=utf-8',
  'cache-control': 'no-store',
};

const MIME_TYPES = {
  '.css': 'text/css; charset=utf-8',
  '.html': 'text/html; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.map': 'application/json; charset=utf-8',
  '.png': 'image/png',
  '.svg': 'image/svg+xml',
  '.txt': 'text/plain; charset=utf-8',
  '.woff': 'font/woff',
  '.woff2': 'font/woff2',
};

const ALLOWED_REPAIR_FIELDS = new Set([
  'vehicle_id',
  'deal_status',
  'customer_id',
  'unit_type',
  'model_year',
  'model',
  'inv_prep_stat',
  'deal_completion',
  'pt_status',
  'pt_serial',
  'encore_serial',
  'phys_loc',
  'VIN',
  'vehicle_status',
  'open_balance',
  'days_stationary',
  'short_location',
  'current_stock_no',
  'cs_contact_date',
  'status',
  'doc',
  'shipping_date',
  'poc_name',
  'poc_phone',
  'customer_availability',
  'installer_request_date',
  'installation_company',
  'technician_availability_date',
  'installation_place',
  'repair_price',
  'repair_notes',
  'shortvin',
]);

const json = (res, statusCode, payload) => {
  res.writeHead(statusCode, JSON_HEADERS);
  res.end(JSON.stringify(payload));
};

const CLIENT_RUNTIME_CONFIG_GLOBAL = '__TECHLOC_RUNTIME_CONFIG__';

const buildClientRuntimeConfig = () => ({
  supabaseUrl: SUPABASE_URL,
  supabaseAnonKey: SUPABASE_ANON_KEY,
  supabaseProjectRef: SUPABASE_PROJECT_REF,
});

const renderClientRuntimeConfigScript = () => {
  const payload = JSON.stringify(buildClientRuntimeConfig()).replace(/</g, '\\u003c');
  return `<script>window.${CLIENT_RUNTIME_CONFIG_GLOBAL}=${payload};</script>`;
};

const injectRuntimeConfigIntoHtml = (html = '') => {
  if (!html) return html;
  const scriptTag = renderClientRuntimeConfigScript();
  if (/<\/head>/i.test(html)) {
    return html.replace(/<\/head>/i, `${scriptTag}\n</head>`);
  }
  if (/<\/body>/i.test(html)) {
    return html.replace(/<\/body>/i, `${scriptTag}\n</body>`);
  }
  return `${scriptTag}\n${html}`;
};

const normalizeVin = (value) =>
  String(value || '')
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, '')
    .slice(-17);

const sanitizeRepairPayload = (input) => {
  if (!input || typeof input !== 'object') return {};
  const payload = {};
  Object.entries(input).forEach(([key, value]) => {
    if (!ALLOWED_REPAIR_FIELDS.has(key)) return;
    if (value === undefined) return;
    payload[key] = value;
  });
  if (payload.VIN) {
    payload.VIN = normalizeVin(payload.VIN);
  }
  if (payload.shortvin) {
    payload.shortvin = normalizeVin(payload.shortvin).slice(-6);
  } else if (payload.VIN) {
    payload.shortvin = payload.VIN.slice(-6);
  }
  return payload;
};

const parseJsonBody = async (req) =>
  new Promise((resolve, reject) => {
    let total = 0;
    const chunks = [];
    req.on('data', (chunk) => {
      total += chunk.length;
      if (total > MAX_BODY_BYTES) {
        reject(new Error('Request body is too large.'));
        req.destroy();
        return;
      }
      chunks.push(chunk);
    });
    req.on('end', () => {
      if (!chunks.length) {
        resolve({});
        return;
      }
      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString('utf8')));
      } catch (_error) {
        reject(new Error('Invalid JSON payload.'));
      }
    });
    req.on('error', reject);
  });

const getBearerToken = (req) => {
  const raw = String(req.headers.authorization || '');
  if (!raw.toLowerCase().startsWith('bearer ')) return '';
  return raw.slice(7).trim();
};

const supabaseRequest = async (
  endpoint,
  { method = 'GET', body = null, headers = {}, authToken = SUPABASE_SERVICE_ROLE_KEY } = {}
) => {
  const requestHeaders = {
    apikey: SUPABASE_SERVICE_ROLE_KEY,
    authorization: `Bearer ${authToken}`,
    ...headers,
  };
  const requestOptions = {
    method,
    headers: requestHeaders,
  };
  if (body !== null) {
    requestOptions.body = JSON.stringify(body);
    requestHeaders['content-type'] = 'application/json';
  }
  return fetch(`${SUPABASE_URL}${endpoint}`, requestOptions);
};

const parseSupabaseError = async (response) => {
  try {
    const payload = await response.json();
    const message = payload?.message || payload?.msg || `Supabase request failed (${response.status}).`;
    return { message, details: payload };
  } catch (_error) {
    const text = await response.text();
    return {
      message: text || `Supabase request failed (${response.status}).`,
      details: null,
    };
  }
};

const decodeJwtPayload = (token) => {
  const parts = String(token || '').split('.');
  if (parts.length < 2) return null;
  try {
    const normalized = parts[1].replace(/-/g, '+').replace(/_/g, '/');
    const padding = '='.repeat((4 - (normalized.length % 4)) % 4);
    const decoded = Buffer.from(normalized + padding, 'base64').toString('utf8');
    return JSON.parse(decoded);
  } catch (_error) {
    return null;
  }
};

const validateConfig = () => {
  const missing = [];
  if (!SUPABASE_URL) missing.push('SUPABASE_URL');
  if (!SUPABASE_ANON_KEY) missing.push('SUPABASE_ANON_KEY (or SUPABASE_PUBLISHABLE_KEY)');
  if (missing.length) {
    throw new Error(`Missing required environment variables: ${missing.join(', ')}`);
  }

  const expectedHost = `${SUPABASE_PROJECT_REF}.supabase.co`;
  let host = '';
  try {
    host = new URL(SUPABASE_URL).hostname;
  } catch (_error) {
    throw new Error(`Invalid SUPABASE_URL: ${SUPABASE_URL}`);
  }
  if (host !== expectedHost) {
    throw new Error(`Blocked SUPABASE_URL host: ${host}. Expected ${expectedHost}.`);
  }

  if (SUPABASE_SERVICE_ROLE_KEY) {
    const tokenPayload = decodeJwtPayload(SUPABASE_SERVICE_ROLE_KEY);
    const tokenRef = String(tokenPayload?.ref || '').trim();
    if (tokenRef && tokenRef !== SUPABASE_PROJECT_REF) {
      throw new Error(
        `Blocked SUPABASE_SERVICE_ROLE_KEY ref: ${tokenRef}. Expected ${SUPABASE_PROJECT_REF}.`
      );
    }
  }

  const anonPayload = decodeJwtPayload(SUPABASE_ANON_KEY);
  const anonRef = String(anonPayload?.ref || '').trim();
  if (anonRef && anonRef !== SUPABASE_PROJECT_REF) {
    throw new Error(
      `Blocked SUPABASE_ANON_KEY ref: ${anonRef}. Expected ${SUPABASE_PROJECT_REF}.`
    );
  }
};

const getUserFromAccessToken = async (token) => {
  if (!token) return null;
  const keyForValidation = SUPABASE_ANON_KEY || SUPABASE_SERVICE_ROLE_KEY;
  const response = await fetch(`${SUPABASE_URL}/auth/v1/user`, {
    method: 'GET',
    headers: {
      apikey: keyForValidation,
      authorization: `Bearer ${token}`,
    },
  });
  if (!response.ok) return null;
  return response.json();
};

const getUserProfile = async (userId) => {
  if (!userId) return null;
  const response = await supabaseRequest(
    `/rest/v1/profiles?id=eq.${encodeURIComponent(userId)}&select=role,status,email&limit=1`
  );
  if (!response.ok) return null;
  const rows = await response.json();
  return Array.isArray(rows) && rows.length ? rows[0] : null;
};

const requireAuthorizedRole = async (req, res) => {
  if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
    json(res, 500, {
      error: {
        message: 'Secure Supabase proxy is not configured.',
      },
    });
    return null;
  }

  const accessToken = getBearerToken(req);
  if (!accessToken) {
    json(res, 401, {
      error: {
        message: 'Missing bearer token.',
      },
    });
    return null;
  }

  const user = await getUserFromAccessToken(accessToken);
  if (!user?.id) {
    json(res, 401, {
      error: {
        message: 'Invalid or expired access token.',
      },
    });
    return null;
  }

  const profile = await getUserProfile(user.id);
  const role = String(profile?.role || 'user').toLowerCase();
  const status = String(profile?.status || 'active').toLowerCase();
  const blockedByAllowlist = ALLOWED_ROLES.size > 0 && !ALLOWED_ROLES.has(role);
  if (blockedByAllowlist || status === 'suspended') {
    json(res, 403, {
      error: {
        message: 'You do not have access to Repair History.',
        details: { role, status },
      },
    });
    return null;
  }

  return { user, profile };
};

const requireActiveAdministrator = async (req, res) => {
  if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
    json(res, 500, {
      error: {
        message: 'Secure Supabase proxy is not configured.',
      },
    });
    return null;
  }

  const accessToken = getBearerToken(req);
  if (!accessToken) {
    json(res, 401, {
      error: {
        message: 'Missing bearer token.',
      },
    });
    return null;
  }

  const user = await getUserFromAccessToken(accessToken);
  if (!user?.id) {
    json(res, 401, {
      error: {
        message: 'Invalid or expired access token.',
      },
    });
    return null;
  }

  const profile = await getUserProfile(user.id);
  const role = String(profile?.role || 'user').toLowerCase();
  const status = String(profile?.status || 'active').toLowerCase();
  if (role !== 'administrator' || status === 'suspended') {
    json(res, 403, {
      error: {
        message: 'Administrator role is required.',
        details: { role, status },
      },
    });
    return null;
  }

  return { user, profile };
};

const resolveUserEmailForReset = async ({ userId, email }) => {
  if (email) {
    return String(email).trim().toLowerCase();
  }

  if (userId) {
    const response = await supabaseRequest(
      `/rest/v1/profiles?id=eq.${encodeURIComponent(userId)}&select=email&limit=1`
    );
    if (!response.ok) {
      const parsed = await parseSupabaseError(response);
      throw new Error(parsed.message || 'Could not resolve target profile email.');
    }
    const rows = await response.json();
    const resolvedEmail = Array.isArray(rows) && rows.length ? rows[0]?.email : '';
    return String(resolvedEmail || '').trim().toLowerCase();
  }

  return '';
};

const handleAdminApi = async (req, res, pathname) => {
  if (req.method !== 'POST' || pathname !== '/api/admin/password-reset') {
    json(res, 404, { error: { message: 'Not found.' } });
    return;
  }

  const auth = await requireActiveAdministrator(req, res);
  if (!auth) return;

  let body;
  try {
    body = await parseJsonBody(req);
  } catch (error) {
    json(res, 400, { error: { message: error?.message || 'Invalid JSON payload.' } });
    return;
  }

  const targetUserId = String(body?.userId || '').trim();
  const targetEmailRaw = String(body?.email || '').trim();
  let targetEmail = '';

  try {
    targetEmail = await resolveUserEmailForReset({
      userId: targetUserId,
      email: targetEmailRaw,
    });
  } catch (error) {
    json(res, 400, { error: { message: error?.message || 'Invalid target account.' } });
    return;
  }

  if (!targetEmail) {
    json(res, 400, {
      error: {
        message: 'Target profile email is required.',
      },
    });
    return;
  }

  const redirectTo = new URL('/pages/reset-password.html', `http://${req.headers.host || '127.0.0.1'}`).toString();

  const response = await supabaseRequest('/auth/v1/admin/generate_link', {
    method: 'POST',
    body: {
      type: 'recovery',
      email: targetEmail,
      options: { redirectTo },
    },
  });

  if (!response.ok) {
    const parsed = await parseSupabaseError(response);
    json(res, response.status, { error: parsed });
    return;
  }

  const payload = await response.json();
  json(res, 200, {
    data: {
      ok: true,
      email: targetEmail,
      userId: targetUserId || null,
      generated: Boolean(payload?.properties?.action_link || payload?.action_link),
    },
  });
};

const handleRepairHistoryApi = async (req, res, pathname, searchParams) => {
  const auth = await requireAuthorizedRole(req, res);
  if (!auth) return;

  if (req.method === 'GET' && pathname === '/api/repair-history') {
    const normalizedVin = normalizeVin(searchParams.get('vin') || '');
    if (!normalizedVin) {
      json(res, 400, {
        error: {
          message: 'VIN is required.',
        },
      });
      return;
    }

    const shortVin = normalizedVin.slice(-6);
    const params = new URLSearchParams({
      select: '*',
      or: `(VIN.ilike.%${normalizedVin}%,shortvin.ilike.%${shortVin}%)`,
      order: 'created_at.desc',
    });

    const response = await supabaseRequest(`/rest/v1/${REPAIR_HISTORY_TABLE}?${params.toString()}`);
    if (!response.ok) {
      const parsed = await parseSupabaseError(response);
      json(res, response.status, { error: parsed });
      return;
    }
    const rows = await response.json();
    json(res, 200, { data: rows || [] });
    return;
  }

  if (req.method === 'POST' && pathname === '/api/repair-history') {
    const body = await parseJsonBody(req);
    const payload = sanitizeRepairPayload(body);
    if (!payload.VIN) {
      json(res, 400, {
        error: {
          message: 'VIN is required in payload.',
        },
      });
      return;
    }

    const response = await supabaseRequest(`/rest/v1/${REPAIR_HISTORY_TABLE}?select=*`, {
      method: 'POST',
      headers: { Prefer: 'return=representation' },
      body: payload,
    });
    if (!response.ok) {
      const parsed = await parseSupabaseError(response);
      json(res, response.status, { error: parsed });
      return;
    }
    const rows = await response.json();
    json(res, 200, { data: rows || [] });
    return;
  }

  const idMatch = pathname.match(/^\/api\/repair-history\/([^/]+)$/);
  if (!idMatch) {
    json(res, 404, { error: { message: 'Not found.' } });
    return;
  }
  const repairId = decodeURIComponent(idMatch[1]);
  if (!repairId) {
    json(res, 400, { error: { message: 'Repair ID is required.' } });
    return;
  }

  if (req.method === 'PATCH') {
    const body = await parseJsonBody(req);
    const payload = sanitizeRepairPayload(body);
    if (!Object.keys(payload).length) {
      json(res, 400, { error: { message: 'No editable fields in payload.' } });
      return;
    }
    const response = await supabaseRequest(
      `/rest/v1/${REPAIR_HISTORY_TABLE}?id=eq.${encodeURIComponent(repairId)}&select=*`,
      {
        method: 'PATCH',
        headers: { Prefer: 'return=representation' },
        body: payload,
      }
    );
    if (!response.ok) {
      const parsed = await parseSupabaseError(response);
      json(res, response.status, { error: parsed });
      return;
    }
    const rows = await response.json();
    json(res, 200, { data: rows || [] });
    return;
  }

  if (req.method === 'DELETE') {
    const response = await supabaseRequest(
      `/rest/v1/${REPAIR_HISTORY_TABLE}?id=eq.${encodeURIComponent(repairId)}&select=*`,
      {
        method: 'DELETE',
        headers: { Prefer: 'return=representation' },
      }
    );
    if (!response.ok) {
      const parsed = await parseSupabaseError(response);
      json(res, response.status, { error: parsed });
      return;
    }
    const rows = await response.json();
    json(res, 200, { data: rows || [] });
    return;
  }

  json(res, 405, { error: { message: 'Method not allowed.' } });
};

const serveStatic = async (req, res, pathname) => {
  if (req.method !== 'GET' && req.method !== 'HEAD') {
    json(res, 405, { error: { message: 'Method not allowed.' } });
    return;
  }

  let requestPath = pathname || '/';
  if (requestPath === '/') requestPath = '/index.html';

  const decodedPath = decodeURIComponent(requestPath);
  const filePath = path.resolve(ROOT_DIR, `.${decodedPath}`);
  if (!filePath.startsWith(ROOT_DIR)) {
    json(res, 403, { error: { message: 'Forbidden.' } });
    return;
  }

  let targetPath = filePath;
  try {
    const fileStat = await stat(targetPath);
    if (fileStat.isDirectory()) {
      targetPath = path.join(targetPath, 'index.html');
    }
  } catch (_error) {
    json(res, 404, { error: { message: 'File not found.' } });
    return;
  }

  try {
    const ext = path.extname(targetPath).toLowerCase();
    const mimeType = MIME_TYPES[ext] || 'application/octet-stream';
    const content = await readFile(targetPath);
    const responseBody = ext === '.html'
      ? Buffer.from(injectRuntimeConfigIntoHtml(content.toString('utf8')), 'utf8')
      : content;
    res.writeHead(200, {
      'content-type': mimeType,
      'cache-control': 'no-store',
    });
    if (req.method === 'HEAD') {
      res.end();
      return;
    }
    res.end(responseBody);
  } catch (_error) {
    json(res, 404, { error: { message: 'File not found.' } });
  }
};

const start = () => {
  validateConfig();

  const server = createServer(async (req, res) => {
    try {
      const url = new URL(req.url || '/', `http://${req.headers.host || '127.0.0.1'}`);
      const { pathname, searchParams } = url;

      if (pathname === '/api/health') {
        json(res, 200, {
          ok: true,
          service: 'secure-supabase-proxy',
          table: REPAIR_HISTORY_TABLE,
        });
        return;
      }

      if (pathname === '/api/repair-history' || pathname.startsWith('/api/repair-history/')) {
        await handleRepairHistoryApi(req, res, pathname, searchParams);
        return;
      }

      if (pathname === '/api/admin/password-reset') {
        await handleAdminApi(req, res, pathname);
        return;
      }

      await serveStatic(req, res, pathname);
    } catch (error) {
      json(res, 500, {
        error: {
          message: error?.message || 'Unhandled server error.',
        },
      });
    }
  });

  server.listen(PORT, () => {
    console.log(`[secure-proxy] running at http://127.0.0.1:${PORT}`);
  });
};

start();
