import { requireSession, redirectToAdminHome, redirectToLogin, supabaseClient } from './admin-auth.js';
import { clearWebAdminSession, getWebAdminAccess, isWebAdminSession } from './web-admin-session.js';

const TABLE_NAME = 'gps_blacklist';
const KEY_CANDIDATES = ['id', 'uuid', 'serial', 'device_id', 'gps_id'];

const els = {
  statusPill: document.getElementById('status-pill'),
  totalCount: document.getElementById('total-count'),
  pkLabel: document.getElementById('pk-label'),
  lastSync: document.getElementById('last-sync'),
  diagnosticsPanel: document.getElementById('diagnostics-panel'),
  diagnosticsSummary: document.getElementById('diagnostics-summary'),
  diagnosticsLog: document.getElementById('diagnostics-log'),
  clearErrorsBtn: document.getElementById('clear-errors-btn'),
  headRow: document.getElementById('table-head-row'),
  body: document.getElementById('table-body'),
  feedback: document.getElementById('feedback'),
  searchInput: document.getElementById('search-input'),
  sortColumn: document.getElementById('sort-column'),
  sortDirection: document.getElementById('sort-direction'),
  filterColumn: document.getElementById('filter-column'),
  filterValue: document.getElementById('filter-value'),
  clearFiltersBtn: document.getElementById('clear-filters-btn'),
  refreshBtn: document.getElementById('refresh-btn'),
  addNewBtn: document.getElementById('add-new-btn'),
  modal: document.getElementById('row-modal'),
  modalTitle: document.getElementById('modal-title'),
  modalClose: document.getElementById('modal-close'),
  modalCancel: document.getElementById('modal-cancel'),
  modalSave: document.getElementById('modal-save'),
  rowForm: document.getElementById('row-form'),
};

const state = {
  rows: [],
  columns: [],
  visibleColumns: [],
  diagnostics: [],
  searchQuery: '',
  sortColumn: '',
  sortDirection: 'asc',
  filterColumn: '',
  filterValue: '',
  primaryKey: null,
  editingKey: null,
  currentUserLabel: null,
};

const ADDED_AT_COL = 'added_at';
const AUTO_FIELDS = new Set(['is_active', 'added_by', 'uuid']);
const HIDDEN_COLUMNS = new Set(['uuid', 'is_active']);
const ADMIN_ROLE = 'administrator';
const DIAGNOSTIC_LIMIT = 40;
let diagnosticsBound = false;

const setStatus = (message, tone = 'neutral') => {
  if (!els.statusPill) return;
  const toneClasses = {
    neutral: 'border-slate-700 bg-slate-900 text-slate-300',
    success: 'border-emerald-700/60 bg-emerald-900/50 text-emerald-200',
    error: 'border-red-700/60 bg-red-900/50 text-red-100',
  };

  els.statusPill.textContent = message;
  els.statusPill.className = `rounded-full px-3 py-1 text-[11px] font-semibold uppercase tracking-wide ${toneClasses[tone] || toneClasses.neutral}`;
};

const setFeedback = (message, tone = 'neutral') => {
  if (!els.feedback) return;
  const toneClasses = {
    neutral: 'text-slate-300',
    success: 'text-emerald-300',
    error: 'text-red-300',
  };
  els.feedback.textContent = message;
  els.feedback.className = `min-h-5 text-xs ${toneClasses[tone] || toneClasses.neutral}`;
};


const normalizeColumnName = (value) => String(value || '').trim().toLowerCase();
const normalizeRole = (value) => String(value || '').trim().toLowerCase();
const toText = (value) => {
  if (value === null || value === undefined) return '';
  if (typeof value === 'string') return value;
  try {
    return JSON.stringify(value);
  } catch (_error) {
    return String(value);
  }
};

const normalizeErrorPayload = (error) => {
  if (!error) {
    return {
      message: 'Unknown error',
      code: null,
      details: null,
      hint: null,
    };
  }

  if (typeof error === 'string') {
    return {
      message: error,
      code: null,
      details: null,
      hint: null,
    };
  }

  return {
    message: toText(error.message || error.error || 'Unknown error'),
    code: toText(error.code || ''),
    details: toText(error.details || ''),
    hint: toText(error.hint || ''),
  };
};

const renderDiagnostics = () => {
  if (!els.diagnosticsPanel || !els.diagnosticsSummary || !els.diagnosticsLog) return;

  if (!state.diagnostics.length) {
    els.diagnosticsPanel.classList.add('hidden');
    els.diagnosticsSummary.textContent = 'No runtime errors detected.';
    els.diagnosticsLog.textContent = '';
    return;
  }

  els.diagnosticsPanel.classList.remove('hidden');
  els.diagnosticsSummary.textContent = `${state.diagnostics.length} error(s) detected during this session.`;
  els.diagnosticsLog.textContent = state.diagnostics
    .map((entry, index) => {
      const lines = [
        `#${index + 1} ${entry.at}`,
        `context: ${entry.context}`,
        `message: ${entry.message}`,
      ];
      if (entry.code) lines.push(`code: ${entry.code}`);
      if (entry.details) lines.push(`details: ${entry.details}`);
      if (entry.hint) lines.push(`hint: ${entry.hint}`);
      if (entry.metadata && Object.keys(entry.metadata).length) lines.push(`meta: ${toText(entry.metadata)}`);
      return lines.join('\n');
    })
    .join('\n\n');
};

const reportError = (context, error, metadata = {}) => {
  const payload = normalizeErrorPayload(error);
  const entry = {
    at: new Date().toISOString(),
    context,
    message: payload.message || 'Unknown error',
    code: payload.code || null,
    details: payload.details || null,
    hint: payload.hint || null,
    metadata,
  };

  state.diagnostics.unshift(entry);
  if (state.diagnostics.length > DIAGNOSTIC_LIMIT) {
    state.diagnostics.length = DIAGNOSTIC_LIMIT;
  }

  renderDiagnostics();

  const suffix = entry.code ? ` [${entry.code}]` : '';
  setStatus('Error detected', 'error');
  setFeedback(`${entry.context}: ${entry.message}${suffix}`, 'error');

  console.error(`[gps_blacklist_admin] ${context}`, error, metadata);
};

const bindDiagnostics = () => {
  if (diagnosticsBound) return;
  diagnosticsBound = true;

  els.clearErrorsBtn?.addEventListener('click', () => {
    state.diagnostics = [];
    renderDiagnostics();
    setFeedback('Diagnostics cleared.', 'neutral');
  });

  window.addEventListener('error', (event) => {
    reportError('window.error', event.error || event.message || 'Unhandled window error', {
      filename: event.filename,
      lineno: event.lineno,
      colno: event.colno,
    });
  });

  window.addEventListener('unhandledrejection', (event) => {
    reportError('window.unhandledrejection', event.reason || 'Unhandled promise rejection');
  });
};

const detectPrimaryKey = (columns) => KEY_CANDIDATES.find((key) => columns.includes(key)) || null;

const getDisplayColumns = (rows) => {
  const merged = new Set();
  rows.forEach((row) => Object.keys(row || {}).forEach((key) => merged.add(key)));
  return [...merged].filter((key) => !HIDDEN_COLUMNS.has(normalizeColumnName(key)));
};

const findColumnByNormalizedName = (normalizedName) =>
  state.columns.find((col) => normalizeColumnName(col) === normalizeColumnName(normalizedName)) || null;

const formatAddedAt = (value) => {
  if (!value) return '—';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);

  const mm = String(date.getMonth() + 1).padStart(2, '0');
  const dd = String(date.getDate()).padStart(2, '0');
  const yy = String(date.getFullYear()).slice(-2);
  const hh = String(date.getHours()).padStart(2, '0');
  const min = String(date.getMinutes()).padStart(2, '0');
  return `${mm}/${dd}/${yy} [${hh}:${min}]`;
};

const isAddedAtColumn = (columnName) => normalizeColumnName(columnName) === ADDED_AT_COL;



const compareValues = (a, b) => {
  const aText = String(a ?? '').toLowerCase();
  const bText = String(b ?? '').toLowerCase();
  const aNumber = Number(aText);
  const bNumber = Number(bText);
  const bothNumeric = !Number.isNaN(aNumber) && !Number.isNaN(bNumber) && aText !== '' && bText !== '';

  if (bothNumeric) return aNumber - bNumber;
  return aText.localeCompare(bText, 'es', { numeric: true, sensitivity: 'base' });
};

const getFilteredRows = () => {
  const query = state.searchQuery.trim().toLowerCase();
  const filterValue = state.filterValue.trim().toLowerCase();

  let rows = [...state.rows];

  if (query) {
    rows = rows.filter((row) =>
      state.visibleColumns.some((col) => String(row[col] ?? '').toLowerCase().includes(query)),
    );
  }

  if (state.filterColumn && filterValue) {
    rows = rows.filter((row) => String(row[state.filterColumn] ?? '').toLowerCase().includes(filterValue));
  }

  if (state.sortColumn) {
    const direction = state.sortDirection === 'desc' ? -1 : 1;
    rows.sort((a, b) => compareValues(a[state.sortColumn], b[state.sortColumn]) * direction);
  }

  return rows;
};

const renderColumnSelectors = () => {
  const options = state.visibleColumns
    .map((col) => `<option value="${col}">${col}</option>`)
    .join('');

  if (els.sortColumn) {
    els.sortColumn.innerHTML = `<option value="">No sort</option>${options}`;
    els.sortColumn.value = state.sortColumn && state.visibleColumns.includes(state.sortColumn) ? state.sortColumn : '';
  }

  if (els.filterColumn) {
    els.filterColumn.innerHTML = `<option value="">No filter</option>${options}`;
    els.filterColumn.value = state.filterColumn && state.visibleColumns.includes(state.filterColumn) ? state.filterColumn : '';
  }
};

const formatCell = (columnName, value) => {
  if (value === null || value === undefined || value === '') return '—';
  if (isAddedAtColumn(columnName)) return formatAddedAt(value);
  if (typeof value === 'object') return JSON.stringify(value);
  const valueText = String(value);
  return valueText.length > 80 ? `${valueText.slice(0, 77)}...` : valueText;
};

const renderHeader = () => {
  if (!els.headRow) return;
  const headers = state.visibleColumns
    .map(
      (col) =>
        `<th class="px-3 py-3 text-left text-[11px] font-semibold uppercase tracking-wide text-slate-300">${col.toUpperCase()}</th>`,
    )
    .join('');
  els.headRow.innerHTML = `${headers}<th class="px-3 py-3 text-right text-[11px] font-semibold uppercase tracking-wide text-slate-300">ACCIONES</th>`;
};

const renderTable = () => {
  renderHeader();
  if (!els.body) return;

  const rowsToRender = getFilteredRows();

  if (!rowsToRender.length) {
    els.body.innerHTML = `<tr><td colspan="${Math.max(state.visibleColumns.length + 1, 2)}" class="px-4 py-8 text-center text-slate-500 italic">No records match the applied filters.</td></tr>`;
    return;
  }

  els.body.innerHTML = rowsToRender
    .map((row) => {
      const rowKey = state.primaryKey ? row[state.primaryKey] : '';
      const cells = state.visibleColumns
        .map((col) => `<td class="max-w-[260px] truncate px-3 py-2 text-slate-200" title="${String(row[col] ?? '')}">${formatCell(col, row[col])}</td>`)
        .join('');

      return `
        <tr class="hover:bg-slate-800/40">
          ${cells}
          <td class="px-3 py-2">
            <div class="flex justify-end gap-2">
              <button type="button" title="Edit" data-edit="${String(rowKey ?? '')}" class="h-7 w-7 rounded-md border border-blue-700/60 text-sm text-blue-200 hover:bg-blue-600/20">✎</button>
              <button type="button" title="Delete" data-delete="${String(rowKey ?? '')}" class="h-7 w-7 rounded-md border border-red-700/60 text-sm text-red-200 hover:bg-red-600/20">🗑</button>
            </div>
          </td>
        </tr>
      `;
    })
    .join('');
};

const updateCounters = () => {
  if (els.totalCount) els.totalCount.textContent = String(getFilteredRows().length);
  if (els.pkLabel) els.pkLabel.textContent = state.primaryKey || 'Not found';
  if (els.lastSync) els.lastSync.textContent = new Date().toLocaleString();
};

const openModal = (title, row = null) => {
  state.editingKey = row && state.primaryKey ? row[state.primaryKey] : null;
  els.modalTitle.textContent = title;
  els.rowForm.innerHTML = '';

  const editableColumns = state.visibleColumns.filter((col) => {
    const normalizedCol = normalizeColumnName(col);
    return col !== state.primaryKey && !isAddedAtColumn(col) && !AUTO_FIELDS.has(normalizedCol);
  });

  editableColumns.forEach((col) => {
    const value = row?.[col] ?? '';
    const fieldId = `field-${col}`;
    const inputType = typeof value === 'number' ? 'number' : 'text';
    els.rowForm.insertAdjacentHTML(
      'beforeend',
      `
      <label class="grid gap-1 text-xs font-semibold uppercase tracking-wide text-slate-300" for="${fieldId}">
        <span>${col}</span>
        <input id="${fieldId}" data-field="${col}" type="${inputType}" value="${String(value).replaceAll('"', '&quot;')}" class="w-full rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 text-xs text-slate-100 focus:border-blue-500 focus:outline-none" />
      </label>
      `,
    );
  });

  els.modal.classList.remove('hidden');
  els.modal.classList.add('flex');
};

const closeModal = () => {
  state.editingKey = null;
  els.modal.classList.add('hidden');
  els.modal.classList.remove('flex');
};

const buildPayloadFromForm = () => {
  const payload = {};
  els.rowForm.querySelectorAll('[data-field]').forEach((input) => {
    const field = input.dataset.field;
    const raw = input.value;
    payload[field] = raw === '' ? null : raw;
  });
  return payload;
};

const applyInsertDefaults = (payload) => {
  const sanitizedPayload = { ...payload };

  Object.keys(sanitizedPayload).forEach((column) => {
    if (normalizeColumnName(column) === 'uuid') {
      delete sanitizedPayload[column];
    }
  });

  const isActiveCol = findColumnByNormalizedName('is_active') || 'is_active';
  const addedByCol = findColumnByNormalizedName('added_by') || 'added_by';

  sanitizedPayload[isActiveCol] = true;
  sanitizedPayload[addedByCol] = state.currentUserLabel || 'unknown-user';

  return sanitizedPayload;
};

const saveModalRecord = async () => {
  const payload = buildPayloadFromForm();
  setStatus('Saving…');

  if (state.editingKey !== null && state.primaryKey) {
    const { error } = await supabaseClient.from(TABLE_NAME).update(payload).eq(state.primaryKey, state.editingKey);
    if (error) {
      reportError('saveModalRecord.update', error, {
        table: TABLE_NAME,
        primaryKey: state.primaryKey,
        rowKey: state.editingKey,
      });
      return;
    }
    setFeedback('Record updated successfully.', 'success');
  } else {
    const insertPayload = applyInsertDefaults(payload);
    const { error } = await supabaseClient.from(TABLE_NAME).insert([insertPayload]);
    if (error) {
      reportError('saveModalRecord.insert', error, {
        table: TABLE_NAME,
      });
      return;
    }
    setFeedback('Record inserted successfully.', 'success');
  }

  closeModal();
  await fetchRows();
};

const fetchRows = async () => {
  setStatus('Loading…');
  const { data, error } = await supabaseClient.from(TABLE_NAME).select('*').limit(500);

  if (error) {
    const isPermissionError = String(error?.code || '') === '42501';
    if (isPermissionError) {
      reportError('fetchRows.select', error, {
        table: TABLE_NAME,
        authHint: 'The request is not authenticated as an admin Supabase user.',
      });
      setFeedback(
        'Permission denied. Re-login with Supabase credentials from /pages/login.html (not local limited mode).',
        'error',
      );
      return;
    }

    reportError('fetchRows.select', error, { table: TABLE_NAME });
    return;
  }

  state.rows = data || [];
  state.columns = getDisplayColumns(state.rows);
  state.visibleColumns = [...state.columns];
  state.primaryKey = detectPrimaryKey(Object.keys(state.rows[0] || {}));

  if (state.sortColumn && !state.visibleColumns.includes(state.sortColumn)) state.sortColumn = '';
  if (state.filterColumn && !state.visibleColumns.includes(state.filterColumn)) state.filterColumn = '';

  renderColumnSelectors();
  renderTable();
  updateCounters();
  setStatus(`${TABLE_NAME} table ready`, 'success');
  setFeedback(`Loaded ${state.rows.length} records.`, 'success');
};

const validateAdminRole = async (session) => {
  const roleCandidates = new Set(
    [
      window.currentUserRole,
      document.body?.dataset.userRole,
      session?.user?.user_metadata?.role,
      session?.user?.app_metadata?.role,
    ]
      .map((role) => normalizeRole(role))
      .filter(Boolean),
  );

  if (isWebAdminSession(session)) {
    const localRole = normalizeRole(getWebAdminAccess()?.role);
    if (localRole) roleCandidates.add(localRole);
  }

  // Resolve source of truth from profiles to avoid auth metadata races.
  const userId = session?.user?.id;
  if (userId && supabaseClient?.from) {
    const { data, error } = await supabaseClient
      .from('profiles')
      .select('role, status')
      .eq('id', userId)
      .maybeSingle();

    if (error) {
      reportError('validateAdminRole.profile_lookup', error, { userId });
    } else if (data) {
      const profileRole = normalizeRole(data.role);
      const profileStatus = normalizeRole(data.status) || 'active';
      if (profileRole) {
        roleCandidates.add(profileRole);
        window.currentUserRole = profileRole;
        if (document.body) document.body.dataset.userRole = profileRole;
      }
      if (profileStatus) {
        window.currentUserStatus = profileStatus;
        if (document.body) document.body.dataset.userStatus = profileStatus;
      }
      if (profileStatus === 'suspended') return false;
    }
  }

  if (roleCandidates.has(ADMIN_ROLE)) return true;

  reportError(
    'validateAdminRole',
    { message: 'Administrator role was not found for the current user.' },
    { roles: [...roleCandidates] },
  );
  return false;
};

const handleRowActions = async (event) => {
  const editValue = event.target.closest('[data-edit]')?.dataset.edit;
  const deleteValue = event.target.closest('[data-delete]')?.dataset.delete;

  if (editValue !== undefined) {
    const row = state.rows.find((entry) => String(entry[state.primaryKey]) === String(editValue));
    if (!row) return;
    openModal('Edit record', row);
    return;
  }

  if (deleteValue !== undefined) {
    if (!window.confirm(`Delete ${state.primaryKey}=${deleteValue}?`)) return;

    setStatus('Deleting…');
    const { error } = await supabaseClient.from(TABLE_NAME).delete().eq(state.primaryKey, deleteValue);
    if (error) {
      reportError('handleRowActions.delete', error, {
        table: TABLE_NAME,
        primaryKey: state.primaryKey,
        rowKey: deleteValue,
      });
      return;
    }

    setFeedback('Record deleted successfully.', 'success');
    await fetchRows();
  }
};

const initialize = async () => {
  try {
    bindDiagnostics();
    renderDiagnostics();

    const session = await requireSession();
    if (isWebAdminSession(session)) {
      clearWebAdminSession();
      reportError('initialize.auth_mode', {
        message: 'GPS Blacklist requires a real Supabase authenticated session. Local admin mode is not enough.',
      });
      setStatus('Supabase auth required', 'error');
      setFeedback('Go to /pages/login.html and sign in with Supabase credentials.', 'error');
      setTimeout(() => redirectToLogin(), 500);
      return;
    }

    state.currentUserLabel = session?.user?.email || session?.user?.id || 'unknown-user';

    const isAdmin = await validateAdminRole(session);

    if (!isAdmin) {
      reportError('initialize.authorization', { message: 'Administrator role is required for this page.' });
      redirectToAdminHome();
      return;
    }

    els.refreshBtn?.addEventListener('click', fetchRows);
    els.searchInput?.addEventListener('input', (event) => {
      state.searchQuery = event.target.value || '';
      renderTable();
      updateCounters();
    });
    els.sortColumn?.addEventListener('change', (event) => {
      state.sortColumn = event.target.value || '';
      renderTable();
      updateCounters();
    });
    els.sortDirection?.addEventListener('change', (event) => {
      state.sortDirection = event.target.value === 'desc' ? 'desc' : 'asc';
      renderTable();
      updateCounters();
    });
    els.filterColumn?.addEventListener('change', (event) => {
      state.filterColumn = event.target.value || '';
      renderTable();
      updateCounters();
    });
    els.filterValue?.addEventListener('input', (event) => {
      state.filterValue = event.target.value || '';
      renderTable();
      updateCounters();
    });
    els.clearFiltersBtn?.addEventListener('click', () => {
      state.searchQuery = '';
      state.sortColumn = '';
      state.sortDirection = 'asc';
      state.filterColumn = '';
      state.filterValue = '';

      if (els.searchInput) els.searchInput.value = '';
      if (els.sortColumn) els.sortColumn.value = '';
      if (els.sortDirection) els.sortDirection.value = 'asc';
      if (els.filterColumn) els.filterColumn.value = '';
      if (els.filterValue) els.filterValue.value = '';

      renderTable();
      updateCounters();
    });
    els.addNewBtn?.addEventListener('click', () => openModal('New record'));
    els.modalClose?.addEventListener('click', closeModal);
    els.modalCancel?.addEventListener('click', closeModal);
    els.modalSave?.addEventListener('click', () => {
      saveModalRecord().catch((error) => {
        reportError('saveModalRecord.catch', error);
      });
    });

    els.body?.addEventListener('click', (event) => {
      handleRowActions(event).catch((error) => {
        reportError('handleRowActions.catch', error);
      });
    });

    await fetchRows();
    window.lucide?.createIcons();
  } catch (error) {
    reportError('initialize.catch', error);
    setStatus('Session error', 'error');
  }
};

initialize();
