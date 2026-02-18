import '../authManager.js';
import { requireSession, supabaseClient } from '../admin-auth.js';
import '../sharedHeader.js';
import '../adminNav.js';
import { setupBackgroundManager } from '../backgroundManager.js';
import {
  loadPreferredBackgroundMode,
  persistBackgroundMode,
} from '../backgroundPreference.js';
import { logAdminEvent } from '../adminAudit.js';
import { setServiceFilterIds } from '../../js/shared/navigationStore.js';
import { deleteRow as deleteServiceRow, duplicateRow as duplicateServiceRow, refresh as refreshServices } from './services-api.js';
import { renderCharts as renderChartsUI, resizeCharts } from './services-charts.js';
import { commitInlineEdit as commitInlineEditUI, startInlineEdit as startInlineEditUI } from './services-editor.js';
import { attachResizeHandles, attachScrollSync } from './services-events.js';
import { ALL_COLUMNS, DB_FIELD_BY_COL_ID, loadPrefs, savePrefs, state } from './services-state.js';
import { renderBody as renderBodyUI, renderHeader as renderHeaderUI, renderTable as renderTableUI } from './services-ui.js';

const READ_TABLE = 'Services';
const WRITE_TABLE = 'Services';
const CHANGE_LOG_TABLE = 'admin_change_log';
const PAGE_CATEGORY = 'GPS Technician';

const toBool = (v) => {
  if (v === true || v === false) return v;
  const s = String(v ?? '').trim().toLowerCase();
  if (['true', '1', 'yes', 'y', 'verified'].includes(s)) return true;
  if (['false', '0', 'no', 'n', 'unverified', 'not verified'].includes(s)) return false;
  return null;
};
const boolToLabel = (v) => {
  if (v === true) return 'Verified';
  if (v === false) return 'Not verified';
  if (v === null || v === undefined || String(v).trim() === '') return '—';
  return String(v);
};

const getField = (row, ...keys) => {
  for (const key of keys) {
    const value = row[key];
    if (value !== undefined && value !== null && String(value).trim() !== '') return value;
  }
  return '';
};

const normalizeVerified = (value) => {
  const boolVal = toBool(value);
  if (boolVal === true || boolVal === false) return boolVal;
  const raw = value === null || value === undefined ? '' : String(value).trim();
  return raw === '' ? null : raw;
};

const normalizeRow = (row) => ({
  id: row.id,
  company: getField(row, 'company_name', 'Installation Company', 'Company', 'company', 'name'),
  authorization: getField(row, 'Authorization', 'authorization'),
  category: getField(row, 'category', 'Category'),
  phone: getField(row, 'Phone', 'phone'),
  contact: getField(row, 'contact', 'Contact'),
  address: getField(row, 'address', 'Address'),
  city: getField(row, 'City', 'city'),
  state: getField(row, 'State', 'state', 'state_code'),
  zip: getField(row, 'Zip', 'zip', 'zipcode', 'postal_code'),
  email: getField(row, 'Email', 'email'),
  notes: getField(row, 'Notes', 'notes'),
  website: getField(row, 'website', 'Website'),
  availability: getField(row, 'availability', 'Availability'),
  verified: normalizeVerified(getField(row, 'Verified', 'verified', 'is_verified', 'Is Verified')),
  lat: getField(row, 'lat', 'latitude', 'Lat'),
  long: getField(row, 'long', 'lng', 'longitude', 'Lon'),
});

const els = {
  status: document.getElementById('dataset-status'),
  total: document.getElementById('tech-total'),
  states: document.getElementById('tech-states'),
  companies: document.getElementById('tech-companies'),
  paginationSummary: document.getElementById('pagination-summary'),
  prevPage: document.getElementById('prev-page'),
  nextPage: document.getElementById('next-page'),
  pageIndicator: document.getElementById('page-indicator'),
  refresh: document.getElementById('refresh-button'),
  addRecord: document.getElementById('add-record'),
  colgroup: document.getElementById('colgroup'),
  thead: document.getElementById('table-head'),
  tbody: document.getElementById('table-body'),
  tableScroll: document.getElementById('table-scroll'),
  topScrollbar: document.getElementById('top-scrollbar'),
  topScrollbarInner: document.getElementById('top-scrollbar-inner'),
  columnsBtn: document.getElementById('columns-btn'),
  columnsPopover: document.getElementById('columns-popover'),
  columnsList: document.getElementById('columns-list'),
  colsAll: document.getElementById('cols-all'),
  colsNone: document.getElementById('cols-none'),
  analyticsWrap: document.getElementById('analytics-wrap'),
  eraseFilters: document.getElementById('erase-filters'),
};

// ---------------- AUTH ----------------
const applySessionState = async (session) => {
  if (session?.user?.id) {
    state.currentUserId = session.user.id;
    state.currentUserEmail = session.user.email || '';
    if (window.currentUserRole) {
      state.currentUserRole = window.currentUserRole;
    } else {
      const { data: profile } = await supabaseClient
        .from('profiles')
        .select('role')
        .eq('id', session.user.id)
        .single();
      state.currentUserRole = profile?.role || 'user';
    }
  } else {
    state.currentUserRole = 'anon';
    state.currentUserId = 'anon';
    state.currentUserEmail = '';
  }

  loadPrefs();

  const isAdmin = state.currentUserRole === 'administrator';
  if (els.addRecord) {
    els.addRecord.classList.toggle('hidden', !isAdmin);
    els.addRecord.disabled = !isAdmin;
  }
};

supabaseClient.auth.onAuthStateChange((_event, session) => {
  const nextUserId = session?.user?.id || 'anon';
  if (nextUserId === state.currentUserId) return;
  applySessionState(session);
  renderTable();
});

// ---------------- HELPERS ----------------
const ensureToastContainer = () => {
  let container = document.getElementById('toast-container');
  if (!container) {
    container = document.createElement('div');
    container.id = 'toast-container';
    container.className = 'fixed bottom-6 right-6 z-50 flex flex-col gap-3';
    document.body.appendChild(container);
  }
  return container;
};

const showToast = (message, type = 'info') => {
  const container = ensureToastContainer();
  const toast = document.createElement('div');

  const variantClasses =
    type === 'success'
      ? 'border-emerald-500/60 bg-emerald-500/10 text-emerald-50 shadow-emerald-500/30'
      : type === 'warning'
        ? 'border-amber-500/60 bg-amber-500/10 text-amber-50 shadow-amber-500/30'
        : type === 'info'
          ? 'border-blue-500/60 bg-blue-500/10 text-blue-50 shadow-blue-500/30'
          : 'border-red-500/60 bg-red-500/10 text-red-50 shadow-red-500/30';

  toast.className = `flex min-w-[260px] max-w-sm items-start gap-3 rounded-xl border px-4 py-3 text-sm shadow-xl backdrop-blur ${variantClasses}`;

  const icon = document.createElement('span');
  icon.className = `mt-0.5 h-2.5 w-2.5 rounded-full ${
    type === 'success' ? 'bg-emerald-400' : type === 'warning' ? 'bg-amber-400' : type === 'info' ? 'bg-blue-400' : 'bg-red-400'
  }`;

  const text = document.createElement('p');
  text.className = 'whitespace-pre-line font-semibold leading-relaxed';
  text.textContent = message;

  toast.appendChild(icon);
  toast.appendChild(text);

  container.appendChild(toast);

  setTimeout(() => {
    toast.classList.add('opacity-0', 'translate-y-2', 'transition', 'duration-300');
    setTimeout(() => toast.remove(), 320);
  }, 4200);
};

const logChange = async ({
  action = 'edit',
  summary = '',
  tableName = WRITE_TABLE,
  recordId = null,
  columnName = null,
  previousValue = null,
  newValue = null,
} = {}) => {
  await logAdminEvent({
    client: supabaseClient,
    action,
    tableName,
    summary,
    recordId,
    columnName,
    previousValue,
    newValue,
    actor: state.currentUserEmail || state.currentUserId || null,
  });
};

const showConfirm = (message, { confirmText = 'Confirm', cancelText = 'Cancel' } = {}) =>
  new Promise((resolve) => {
    const overlay = document.createElement('div');
    overlay.className = 'fixed inset-0 z-50 flex items-center justify-center bg-slate-950/70 backdrop-blur';

    const dialog = document.createElement('div');
    dialog.className = 'w-[min(420px,92vw)] rounded-2xl border border-slate-800 bg-slate-900/95 p-5 shadow-2xl shadow-black';

    dialog.innerHTML = `
      <div class="flex items-start gap-3">
        <div class="mt-1 h-3 w-3 rounded-full bg-amber-400"></div>
        <div class="space-y-3">
          <p class="text-sm font-semibold text-white">${message}</p>
          <div class="flex flex-wrap gap-2">
            <button type="button" class="rounded-xl bg-emerald-600 px-4 py-2 text-sm font-semibold text-white shadow shadow-emerald-600/40 hover:-translate-y-0.5 transition" data-confirm> ${confirmText} </button>
            <button type="button" class="rounded-xl border border-slate-700 px-4 py-2 text-sm font-semibold text-slate-200 hover:-translate-y-0.5 transition" data-cancel> ${cancelText} </button>
          </div>
        </div>
      </div>
    `;

    const cleanup = (result) => {
      document.removeEventListener('keydown', onKey);
      overlay.remove();
      resolve(result);
    };

    dialog.querySelector('[data-confirm]')?.addEventListener('click', () => cleanup(true));
    dialog.querySelector('[data-cancel]')?.addEventListener('click', () => cleanup(false));
    overlay.addEventListener('click', (e) => { if (e.target === overlay) cleanup(false); });
    function onKey(e) {
      if (e.key === 'Escape') cleanup(false);
    }
    document.addEventListener('keydown', onKey);

    overlay.appendChild(dialog);
    document.body.appendChild(overlay);
  });

const getText = (value) => String(value ?? '').toLowerCase().trim();

const skeleton = {
  show: () => {
    els.status.textContent = 'Loading data…';
    els.total.innerHTML = '<div class="h-7 w-16 rounded bg-slate-800/70 animate-pulse"></div>';
    els.states.innerHTML = '<div class="h-7 w-12 rounded bg-slate-800/70 animate-pulse"></div>';
    els.companies.innerHTML = '<div class="h-7 w-12 rounded bg-slate-800/70 animate-pulse"></div>';

    els.paginationSummary.textContent = 'Loading…';
    els.pageIndicator.textContent = '—';
    els.prevPage.disabled = true;
    els.nextPage.disabled = true;
    if (els.refresh) els.refresh.disabled = true;

    const cols = getVisibleOrderedColumns();
    els.tbody.innerHTML = '';
    for (let i = 0; i < 6; i += 1) {
      const tr = document.createElement('tr');
      cols.forEach(() => {
        const td = document.createElement('td');
        td.className = 'compact-td';
        td.innerHTML = '<div class="h-4 w-full rounded bg-slate-800/70 animate-pulse"></div>';
        tr.appendChild(td);
      });
      els.tbody.appendChild(tr);
    }
  },
  hide: () => {
    els.prevPage.disabled = false;
    els.nextPage.disabled = false;
    if (els.refresh) els.refresh.disabled = false;
  }
};

const updateStats = () => {
  const total = state.rows.length;
  const statesCount = new Set(state.rows.map(r => (r.state || '').trim()).filter(Boolean)).size;
  const companiesCount = new Set(state.rows.map(r => (r.company || '').trim()).filter(Boolean)).size;
  els.total.textContent = total;
  els.states.textContent = statesCount;
  els.companies.textContent = companiesCount;
  els.status.textContent = total ? `Connected • ${total} records` : 'Awaiting data';
};

const getColumnDisplayValue = (colId, row) => {
  if (colId === 'verified') return boolToLabel(row.verified);
  const col = ALL_COLUMNS.find(c => c.id === colId);
  const raw = col ? row[col.key] : '';
  if (raw === null || raw === undefined || String(raw).trim() === '') return '—';
  return String(raw);
};

const getFilterOptionsForColumn = (colId) => {
  const values = new Set();
  state.rows.forEach((row) => {
    values.add(getColumnDisplayValue(colId, row));
  });
  return Array.from(values).sort((a, b) => getText(a).localeCompare(getText(b), undefined, { numeric: true }));
};

const getColumnFilterState = (colId) => {
  const stored = state.filters.columnFilters[colId] || {};
  return {
    values: Array.isArray(stored.values) ? stored.values : [],
    query: typeof stored.query === 'string' ? stored.query : '',
  };
};

const setColumnFilter = (colId, next) => {
  const values = Array.isArray(next.values) ? next.values : [];
  const query = typeof next.query === 'string' ? next.query : '';
  if (!values.length && !query) {
    delete state.filters.columnFilters[colId];
  } else {
    state.filters.columnFilters[colId] = { values, query };
  }
  state.pagination.page = 1;
  savePrefs();
  renderTable();
};

const applyFilters = () => {
  let result = [...state.rows];
  const filters = state.filters.columnFilters || {};
  Object.entries(filters).forEach(([colId, filter]) => {
    const values = Array.isArray(filter.values) ? filter.values : [];
    const query = typeof filter.query === 'string' ? filter.query.trim() : '';
    if (!values.length && !query) return;
    const selected = new Set(values.map(getText));
    result = result.filter((row) => {
      const display = getColumnDisplayValue(colId, row);
      const text = getText(display);
      const matchesQuery = query ? text.includes(getText(query)) : true;
      const matchesSelection = selected.size ? selected.has(text) : true;
      return matchesQuery && matchesSelection;
    });
  });
  return result;
};

const hasActiveFilters = () => {
  const filters = state.filters.columnFilters || {};
  return Object.values(filters).some((filter) => {
    const values = Array.isArray(filter.values) ? filter.values : [];
    const query = typeof filter.query === 'string' ? filter.query.trim() : '';
    return values.length > 0 || query.length > 0;
  });
};

const clearAllFilters = () => {
  state.filters.columnFilters = {};
  state.pagination.page = 1;
  savePrefs();
  renderTable();
};

const updateEraseFiltersButton = () => {
  if (!els.eraseFilters) return;
  els.eraseFilters.disabled = !hasActiveFilters();
};

const applySort = (rows) => {
  const { key, dir } = state.sort;
  if (!key) return rows;
  const col = ALL_COLUMNS.find(c => c.id === key);
  if (!col) return rows;

  const sorted = [...rows].sort((a, b) => {
    if (col.id === 'verified') {
      const normalize = (v) => (v === true ? '2' : v === false ? '1' : getText(v));
      const as = normalize(a.verified);
      const bs = normalize(b.verified);
      return as.localeCompare(bs, undefined, { numeric: true, sensitivity: 'base' });
    }
    return getText(a[col.key]).localeCompare(getText(b[col.key]), undefined, { numeric: true, sensitivity: 'base' });
  });

  return dir === 'asc' ? sorted : sorted.reverse();
};

const getVisibleOrderedColumns = () => state.columnOrder.filter(colId => state.columnVisibility[colId]);

// ---------------- TABLE RENDER ----------------
const renderColgroup = () => {
  const cols = getVisibleOrderedColumns();
  els.colgroup.innerHTML = '';
  cols.forEach((colId) => {
    const colEl = document.createElement('col');
    colEl.setAttribute('data-col', colId);
    colEl.style.width = `${state.columnWidths[colId] || 140}px`;
    els.colgroup.appendChild(colEl);
  });
};

const syncTopScrollbar = () => {
  els.topScrollbarInner.style.width = els.tableScroll.scrollWidth + 'px';
  els.topScrollbar.scrollLeft = els.tableScroll.scrollLeft;
};

const setColWidth = (colId, px) => {
  const min = 72;
  const max = 800;
  state.columnWidths[colId] = Math.max(min, Math.min(max, px));
  const colEl = els.colgroup.querySelector(`col[data-col="${colId}"]`);
  if (colEl) colEl.style.width = `${state.columnWidths[colId]}px`;
  const th = els.thead.querySelector(`th[data-col="${colId}"]`);
  if (th) {
    th.style.width = `${state.columnWidths[colId]}px`;
    th.style.minWidth = `${state.columnWidths[colId]}px`;
  }
  syncTopScrollbar();
  savePrefs();
  requestAnimationFrame(() => {
    resizeCharts();
  });
};

const sortIcon = (colId) => {
  if (state.sort.key !== colId) return 'arrow-up-down';
  return state.sort.dir === 'asc' ? 'arrow-up' : 'arrow-down';
};

const renameColumn = (colId) => {
  if (!colId || colId === 'actions') return;
  const baseLabel = state.columnLabels[colId] || (ALL_COLUMNS.find(c => c.id === colId)?.label) || colId;
  const next = prompt('Nuevo nombre de columna', baseLabel);
  if (next === null) return;
  const trimmed = next.trim();
  if (!trimmed) return;
  state.columnLabels[colId] = trimmed;
  savePrefs();
  renderTable();
};

const toggleSort = (colId) => {
  if (state.sort.key === colId) state.sort.dir = state.sort.dir === 'asc' ? 'desc' : 'asc';
  else { state.sort.key = colId; state.sort.dir = 'asc'; }
  state.pagination.page = 1;
  renderTable();
};

const positionFilterPopover = (popover) => {
  if (!popover || !els.tableScroll) return;
  popover.style.transform = '';
  popover.style.left = 'auto';
  popover.style.right = '0';

  const containerRect = els.tableScroll.getBoundingClientRect();
  const padding = 8;
  const viewportLeft = padding;
  const viewportRight = window.innerWidth - padding;
  const visibleLeft = Math.max(containerRect.left + padding, viewportLeft);
  const visibleRight = Math.min(containerRect.right - padding, viewportRight);
  const visibleWidth = Math.max(240, visibleRight - visibleLeft);
  popover.style.maxWidth = `${visibleWidth}px`;

  const popoverRect = popover.getBoundingClientRect();
  let shiftX = 0;

  if (popoverRect.left < visibleLeft) {
    shiftX = visibleLeft - popoverRect.left;
  } else if (popoverRect.right > visibleRight) {
    shiftX = visibleRight - popoverRect.right;
  }

  if (shiftX) {
    popover.style.transform = `translateX(${shiftX}px)`;
  }
};

const buildHeaderCell = (colId) => {
  const th = document.createElement('th');
  th.className = 'text-left font-semibold text-slate-200 resizable bg-slate-950/60 compact-th';
  th.setAttribute('data-col', colId);

  const startingWidth = state.columnWidths[colId] || 140;
  th.style.width = `${startingWidth}px`;
  th.style.minWidth = `${startingWidth}px`;

  if (colId === 'actions') {
    th.className = 'text-left font-semibold text-blue-300 resizable bg-slate-950/60 compact-th';
    th.style.width = `${state.columnWidths[colId] || 110}px`;
    th.style.minWidth = `${state.columnWidths[colId] || 110}px`;
    th.innerHTML = `
      <div class="th-inner">
        <span class="text-blue-300">Actions</span>
        <span class="opacity-60 text-[11px] uppercase tracking-wide">—</span>
      </div>
      <div class="col-resizer" data-resize="${colId}" title="Drag to resize"></div>
    `;
    return th;
  }

  const col = ALL_COLUMNS.find(c => c.id === colId);
  const label = state.columnLabels[colId] || col?.label || colId;

  th.setAttribute('draggable', 'true');

  const thInner = document.createElement('div');
  thInner.className = 'th-inner';

  const labelWrap = document.createElement('div');
  labelWrap.className = 'flex items-center gap-2 min-w-0';
  const labelSpan = document.createElement('span');
  labelSpan.className = 'truncate th-label';
  labelSpan.dataset.renameCol = colId;
  labelSpan.title = 'Doble click para renombrar';
  labelSpan.textContent = label;
  labelWrap.appendChild(labelSpan);

  const actionsWrap = document.createElement('div');
  actionsWrap.className = 'header-actions';

  const filterWrapper = document.createElement('div');
  filterWrapper.className = 'relative';

  const filterBtn = document.createElement('button');
  filterBtn.type = 'button';
  filterBtn.title = 'Filter column';
  filterBtn.className = 'filter-btn';
  const filterState = getColumnFilterState(colId);
  const hasFilter = filterState.values.length || filterState.query;
  filterBtn.innerHTML = `<i data-lucide="list-filter" class="h-3.5 w-3.5 ${hasFilter ? 'text-blue-400' : 'text-slate-400'}"></i>`;

  const filterPopover = document.createElement('div');
  filterPopover.className = 'filter-popover hidden';

  const filterTitle = document.createElement('label');
  filterTitle.textContent = 'Filter this column';
  filterPopover.appendChild(filterTitle);

  const searchWrap = document.createElement('div');
  searchWrap.className = 'filter-search';
  const searchIcon = document.createElement('i');
  searchIcon.setAttribute('data-lucide', 'search');
  searchIcon.className = 'h-3.5 w-3.5 text-slate-500';
  const searchInput = document.createElement('input');
  searchInput.type = 'search';
  searchInput.placeholder = `Search ${label}`;
  searchInput.value = filterState.query;
  searchWrap.appendChild(searchIcon);
  searchWrap.appendChild(searchInput);
  filterPopover.appendChild(searchWrap);

  const optionsWrapper = document.createElement('div');
  optionsWrapper.className = 'filter-options';
  filterPopover.appendChild(optionsWrapper);

  const selected = new Set(filterState.values);
  let searchTimer;
  const applyFilterUpdate = (closeAfter = false) => {
    setColumnFilter(colId, { values: Array.from(selected), query: searchInput.value.trim() });
    if (closeAfter) closePopover();
  };
  const renderOptions = (term = '') => {
    const normalizedTerm = getText(term);
    optionsWrapper.innerHTML = '';
    const options = getFilterOptionsForColumn(colId);
    const filteredOptions = normalizedTerm
      ? options.filter(value => getText(value).includes(normalizedTerm))
      : options;

    if (!filteredOptions.length) {
      const empty = document.createElement('p');
      empty.className = 'text-xs text-slate-500';
      empty.textContent = 'No options';
      optionsWrapper.appendChild(empty);
      return;
    }

    filteredOptions.forEach((value) => {
      const optionLabel = document.createElement('label');
      optionLabel.className = 'filter-option';
      const checkbox = document.createElement('input');
      checkbox.type = 'checkbox';
      checkbox.className = 'h-4 w-4 accent-blue-600';
      checkbox.value = value;
      checkbox.checked = selected.has(value);
      checkbox.addEventListener('change', (event) => {
        if (event.target.checked) {
          selected.add(value);
        } else {
          selected.delete(value);
        }
        applyFilterUpdate();
      });
      const text = document.createElement('span');
      text.className = 'truncate';
      text.textContent = value;
      optionLabel.appendChild(checkbox);
      optionLabel.appendChild(text);
      optionsWrapper.appendChild(optionLabel);
    });
  };

  renderOptions(searchInput.value);

  const actionsRow = document.createElement('div');
  actionsRow.className = 'filter-actions';
  const applyBtn = document.createElement('button');
  applyBtn.type = 'button';
  applyBtn.className = 'flex-1 rounded-md bg-blue-600 px-3 py-2 text-xs font-semibold text-white hover:bg-blue-500';
  applyBtn.textContent = 'Apply';
  const clearBtn = document.createElement('button');
  clearBtn.type = 'button';
  clearBtn.className = 'flex-1 rounded-md border border-slate-700 bg-slate-900 px-3 py-2 text-xs font-semibold text-slate-200 hover:border-blue-500';
  clearBtn.textContent = 'Clear';
  actionsRow.appendChild(applyBtn);
  actionsRow.appendChild(clearBtn);
  filterPopover.appendChild(actionsRow);

  const closePopover = (event) => {
    if (!event || !filterWrapper.contains(event.target)) {
      filterPopover.classList.add('hidden');
      document.removeEventListener('click', closePopover);
    }
  };

  filterBtn.addEventListener('click', (event) => {
    event.stopPropagation();
    const willOpen = filterPopover.classList.contains('hidden');
    filterPopover.classList.toggle('hidden');
    if (willOpen) {
      renderOptions(searchInput.value);
      requestAnimationFrame(() => positionFilterPopover(filterPopover));
      document.addEventListener('click', closePopover);
    } else {
      document.removeEventListener('click', closePopover);
    }
  });

  searchInput.addEventListener('input', () => {
    renderOptions(searchInput.value);
    clearTimeout(searchTimer);
    searchTimer = setTimeout(() => applyFilterUpdate(), 150);
  });

  applyBtn.addEventListener('click', () => {
    applyFilterUpdate(true);
  });

  clearBtn.addEventListener('click', () => {
    selected.clear();
    searchInput.value = '';
    renderOptions('');
    applyFilterUpdate(true);
  });

  filterWrapper.appendChild(filterBtn);
  filterWrapper.appendChild(filterPopover);

  const sortBtn = document.createElement('button');
  sortBtn.type = 'button';
  sortBtn.className = 'sort-btn text-[11px] font-semibold text-slate-200';
  sortBtn.dataset.sortCol = colId;
  sortBtn.title = 'Sort';
  sortBtn.innerHTML = `<i data-lucide="${sortIcon(colId)}" class="h-3.5 w-3.5"></i>`;

  actionsWrap.appendChild(filterWrapper);
  actionsWrap.appendChild(sortBtn);

  thInner.appendChild(labelWrap);
  thInner.appendChild(actionsWrap);
  th.appendChild(thInner);

  const resizer = document.createElement('div');
  resizer.className = 'col-resizer';
  resizer.dataset.resize = colId;
  resizer.title = 'Drag to resize';
  th.appendChild(resizer);

  return th;
};


const renderPagination = (rows) => {
  const totalPages = Math.max(1, Math.ceil(rows.length / state.pagination.pageSize));
  const prevPage = state.pagination.page;
  if (state.pagination.page > totalPages) state.pagination.page = totalPages;

  const start = (state.pagination.page - 1) * state.pagination.pageSize;
  const end = Math.min(rows.length, start + state.pagination.pageSize);

  els.paginationSummary.textContent = rows.length
    ? `Showing ${start + 1}-${end} of ${rows.length}`
    : 'No entries to display';

  els.pageIndicator.textContent = `${state.pagination.page} / ${totalPages}`;
  els.prevPage.disabled = state.pagination.page === 1 || rows.length === 0;
  els.nextPage.disabled = state.pagination.page >= totalPages || rows.length === 0;

  if (prevPage !== state.pagination.page) savePrefs();

  return { start, end };
};

const renderHeader = () => renderHeaderUI({
  els,
  getVisibleOrderedColumns,
  buildHeaderCell,
  state,
  savePrefs,
  renderTable,
  toggleSort,
  renameColumn,
  lucide,
});

const renderBody = (pageRows) => renderBodyUI({
  els,
  getVisibleOrderedColumns,
  state,
  ALL_COLUMNS,
  boolToLabel,
  duplicateRow,
  deleteRow,
  lucide,
  pageRows,
});

const renderTable = () => {
  renderTableUI({
    applyFilters,
    applySort,
    renderPagination,
    renderColgroup,
    renderHeader,
    renderBody,
    renderColumnsPopover,
    syncTopScrollbar,
    renderCharts,
    hasActiveFilters,
    setServiceFilterIds,
    lucide,
  });
  updateEraseFiltersButton();
};

// ---------------- INLINE CELL EDITING ----------------
const withTimeout = (promise, ms = 20000) => {
  let t;
  const timeout = new Promise((_, reject) => {
    t = setTimeout(() => reject(new Error(`Request timed out after ${ms / 1000}s`)), ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(t));
};

const getRowById = (id) => state.rows.find(r => String(r.id) === String(id));

const endInlineEdit = (opts = { save: false }) => {
  const ses = state.inlineEdit;
  if (!ses.td) return;

  const td = ses.td;
  const rowId = ses.rowId;
  const colId = ses.colId;
  const original = ses.original;

  td.classList.remove('editing');
  td.classList.remove('failed');
  td.classList.remove('saved');

  // restore view (if not saving)
  if (!opts.save) {
    td.innerHTML = '';
    td.textContent = original === '' ? '—' : original;
    ses.td = null; ses.rowId = null; ses.colId = null; ses.original = null; ses.inputEl = null;
    return;
  }

  // if saving handled elsewhere, just cleanup refs
  ses.td = null; ses.rowId = null; ses.colId = null; ses.original = null; ses.inputEl = null;
};

const parseEditedValue = (colId, raw) => {
  if (colId === 'verified') {
    const v = String(raw ?? '').trim();
    if (v === '') return null;
    if (v === 'true') return true;
    if (v === 'false') return false;
    return v;
  }
  const v = String(raw ?? '').trim();
  return v === '' ? null : v;
};

const valueForDisplay = (colId, value) => {
  if (colId === 'verified') return boolToLabel(value);
  if (value === null || value === undefined || String(value).trim() === '') return '—';
  return String(value);
};

const selectColumns = new Set(['category', 'authorization', 'state', 'verified']);

const optionLabel = (colId, value) => {
  if (colId === 'state') return String(value).toUpperCase();
  if (colId === 'verified') return boolToLabel(value);
  return String(value);
};

const INSERT_NEW_VALUE = '__insert_new__';

const getSelectOptions = (colId) => {
  if (colId === 'verified') {
    const base = [
      { value: '', label: '—' },
      { value: 'true', label: 'Verified' },
      { value: 'false', label: 'Not verified' },
    ];
    const seen = new Set(base.map(o => o.value));
    const custom = [];
    state.rows.forEach((row) => {
      const val = row.verified;
      const value = val === true ? 'true' : val === false ? 'false' : (val === null || val === undefined ? '' : String(val));
      if (!value || seen.has(value)) return;
      seen.add(value);
      custom.push({ value, label: optionLabel(colId, val) });
    });
    return [...base, ...custom, { value: INSERT_NEW_VALUE, label: 'Insert new…' }];
  }

  const col = ALL_COLUMNS.find(c => c.id === colId);
  if (!col) return [];
  const seen = new Set();
  const opts = [];
  state.rows.forEach((row) => {
    const raw = row[col.key];
    const value = raw === null || raw === undefined ? '' : String(raw).trim();
    if (!value) return;
    const key = getText(value);
    if (seen.has(key)) return;
    seen.add(key);
    opts.push({ value, label: optionLabel(colId, value) });
  });
  return [{ value: '', label: '—' }, ...opts, { value: INSERT_NEW_VALUE, label: 'Insert new…' }];
};

const addSelectOption = (select, value, label) => {
  if ([...select.options].some(o => o.value === value)) return;
  const opt = document.createElement('option');
  opt.value = value;
  opt.textContent = label;
  const insertOpt = [...select.options].find(o => o.value === INSERT_NEW_VALUE);
  if (insertOpt) {
    select.insertBefore(opt, insertOpt);
  } else {
    select.appendChild(opt);
  }
};

const writeLocalRowValue = (rowId, colId, newValue) => {
  const row = getRowById(rowId);
  if (!row) return;
  const col = ALL_COLUMNS.find(c => c.id === colId);
  if (!col) return;
  row[col.key] = newValue;
};

const saveCellToSupabase = async ({ rowId, colId, newValue, td }) => {
  const dbField = DB_FIELD_BY_COL_ID[colId];
  if (!dbField) throw new Error(`No DB mapping for column "${colId}"`);

  td.classList.add('saving');

  const payload = { [dbField]: newValue };

  const { error } = await withTimeout(
    supabaseClient.from(WRITE_TABLE).update(payload).eq('id', rowId),
    20000
  );

  if (error) throw error;
};

const commitInlineEdit = () => commitInlineEditUI({
  state,
  parseEditedValue,
  getRowById,
  ALL_COLUMNS,
  valueForDisplay,
  writeLocalRowValue,
  saveCellToSupabase,
  endInlineEdit,
  renderCharts,
  applySort,
  applyFilters,
  logChange,
  showToast,
});

const startInlineEdit = (td) => startInlineEditUI({
  td,
  state,
  endInlineEdit,
  getRowById,
  ALL_COLUMNS,
  valueForDisplay,
  selectColumns,
  getSelectOptions,
  addSelectOption,
  optionLabel,
  toBool,
  insertNewValue: INSERT_NEW_VALUE,
  commitInlineEdit,
  focusNextEditableCell,
});

const focusNextEditableCell = (fromTd, dir) => {
  const tr = fromTd.closest('tr');
  if (!tr) return;
  const tds = Array.from(tr.querySelectorAll('td[data-editable="true"]'));
  const idx = tds.indexOf(fromTd);
  const next = tds[idx + dir];
  if (!next) return;
  startInlineEdit(next);
};

// delegate dblclick to tbody
const attachInlineEditing = () => {
  els.tbody.addEventListener('dblclick', (e) => {
    const td = e.target.closest('td');
    if (!td) return;
    startInlineEdit(td);
  });

  // click outside to commit (blur usually handles, but this is extra safety)
  document.addEventListener('mousedown', (e) => {
    const ses = state.inlineEdit;
    if (!ses.td) return;
    const inside = e.target.closest('td.editing');
    if (inside) return;
    // if clicked elsewhere, attempt to commit (blur will also fire)
    // no-op here to avoid double commits
  });
};

// ---------------- COLUMN PICKER ----------------
const renderColumnsPopover = () => {
  els.columnsList.innerHTML = '';

  const makeRow = (colId, label, locked = false) => {
    const wrapper = document.createElement('label');
    wrapper.className = 'flex items-center justify-between gap-3 rounded-xl border border-slate-800 bg-slate-900/40 px-3 py-2 text-sm text-slate-200 hover:border-blue-500/60';

    const left = document.createElement('div');
    left.className = 'flex items-center gap-2 min-w-0';
    left.innerHTML = `
      <span class="text-[11px] font-semibold uppercase tracking-wide text-slate-500">${locked ? 'Locked' : 'Show'}</span>
      <span class="truncate font-semibold">${state.columnLabels[colId] || label}</span>
    `;

    const input = document.createElement('input');
    input.type = 'checkbox';
    input.checked = !!state.columnVisibility[colId];
    input.disabled = locked;
    input.className = 'h-4 w-4 accent-blue-600';
    input.addEventListener('change', () => {
      state.columnVisibility[colId] = input.checked;
      state.columnVisibility.actions = true;
      savePrefs();
      renderTable();
    });

    wrapper.appendChild(left);
    wrapper.appendChild(input);
    return wrapper;
  };

  els.columnsList.appendChild(makeRow('actions', 'Actions', true));
  ALL_COLUMNS.forEach((c) => els.columnsList.appendChild(makeRow(c.id, c.label)));

  lucide.createIcons();
};

const toggleColumnsPopover = (force) => {
  const willShow = typeof force === 'boolean' ? force : els.columnsPopover.classList.contains('hidden');
  els.columnsPopover.classList.toggle('hidden', !willShow);
  if (willShow) renderColumnsPopover();
};

document.addEventListener('click', (e) => {
  const insideColumns = e.target.closest('#columns-popover') || e.target.closest('#columns-btn');
  if (!insideColumns) toggleColumnsPopover(false);
});

els.columnsBtn.addEventListener('click', (e) => {
  e.stopPropagation();
  toggleColumnsPopover();
});

els.colsAll.addEventListener('click', () => {
  Object.keys(state.columnVisibility).forEach((k) => state.columnVisibility[k] = true);
  state.columnVisibility.actions = true;
  savePrefs();
  renderTable();
});

els.colsNone.addEventListener('click', () => {
  Object.keys(state.columnVisibility).forEach((k) => state.columnVisibility[k] = false);
  state.columnVisibility.actions = true;
  savePrefs();
  renderTable();
});

// ---------------- CHARTS ----------------
const renderCharts = (filteredRows) => renderChartsUI({ filteredRows, syncTopScrollbar });

const attachAnalyticsResizeObserver = () => {
  if (!('ResizeObserver' in window)) return;
  const ro = new ResizeObserver(() => {
    resizeCharts();
  });
  ro.observe(els.analyticsWrap);
};

// ---------------- DATA FETCH ----------------
const refresh = async () => refreshServices({
  skeleton,
  state,
  normalizeRow,
  updateStats,
  renderTable,
  savePrefs,
  els,
  getVisibleOrderedColumns,
  showToast,
  readTable: READ_TABLE,
});

const buildInsertPayload = (row) => {
  const payload = {};
  ALL_COLUMNS.forEach((col) => {
    const dbField = DB_FIELD_BY_COL_ID[col.id];
    if (!dbField) return;
    payload[dbField] = row[col.key] ?? null;
  });
  return payload;
};

const duplicateRow = async (row) => duplicateServiceRow({
  row,
  state,
  buildInsertPayload,
  normalizeRow,
  renderTable,
  showToast,
  showConfirm,
  writeTable: WRITE_TABLE,
});

const deleteRow = async (row) => deleteServiceRow({
  row,
  state,
  showToast,
  showConfirm,
  writeTable: WRITE_TABLE,
  refresh,
});

// ---------------- EVENTS ----------------
const attachButtonEvents = () => {
  els.prevPage.addEventListener('click', () => {
    if (state.pagination.page > 1) {
      state.pagination.page -= 1;
      savePrefs();
      renderTable();
    }
  });

  els.nextPage.addEventListener('click', () => {
    const total = applySort(applyFilters()).length;
    const totalPages = Math.max(1, Math.ceil(total / state.pagination.pageSize));
    if (state.pagination.page < totalPages) {
      state.pagination.page += 1;
      savePrefs();
      renderTable();
    }
  });

  els.refresh.addEventListener('click', refresh);
  els.eraseFilters?.addEventListener('click', clearAllFilters);

  // Create new record inline (adds blank row in DB, then you edit cells)
  els.addRecord?.addEventListener('click', async () => {
    if (state.currentUserRole !== 'administrator') return;
    try {
      const payload = {
        company_name: '',
        authorization: '',
        category: '',
        verified: null,
        phone: '',
        contact: '',
        address: '',
        city: '',
        state: '',
        zip: '',
        email: '',
        notes: '',
        website: '',
        availability: '',
        lat: null,
        long: null,
      };
      const { data, error } = await supabaseClient.from(WRITE_TABLE).insert([payload]).select('*').single();
      if (error) throw error;
      const normalized = normalizeRow(data);
      if (normalized?.id === undefined || normalized?.id === null || normalized.id === '') {
        console.error('Insert returned row without a valid id', data);
        showToast('Error creating record: missing ID from server. Please refresh and try again.', 'error');
        return;
      }

      state.rows.unshift(normalized);
      state.pagination.page = 1;
      renderTable();
      savePrefs();

      logChange({
        action: 'insert',
        summary: `Created new service record (#${normalized.id})`,
        recordId: normalized.id,
      });
    } catch (err) {
      console.error(err);
      showToast('Error creating record: ' + (err?.message || JSON.stringify(err)), 'error');
    }
  });
};

// ---------------- INIT ----------------
const initBackground = async () => {
  const initialMode = await loadPreferredBackgroundMode();
  setupBackgroundManager({
    canvasId: 'constellation-canvas',
    initialMode,
    onModeChange: persistBackgroundMode,
  });
};

const init = async () => {
  await initBackground();
  const session = await requireSession();
  await applySessionState(session);
  attachButtonEvents();
  attachResizeHandles({ els, state, setColWidth });
  attachScrollSync({ els, syncTopScrollbar, resizeCharts });
  attachAnalyticsResizeObserver();
  attachInlineEditing();
  await refresh();
  renderColumnsPopover();
  syncTopScrollbar();
  lucide.createIcons();
};

init();
