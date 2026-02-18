const DB_FIELD_BY_COL_ID = {
  company: 'company_name',
  authorization: 'authorization',
  category: 'category',
  verified: 'verified',
  phone: 'phone',
  contact: 'contact',
  address: 'address',
  city: 'city',
  state: 'state',
  zip: 'zip',
  email: 'email',
  notes: 'notes',
  website: 'website',
  availability: 'availability',
  lat: 'lat',
  long: 'long',
};

const ALL_COLUMNS = [
  { id: 'company', label: 'Company Name', key: 'company', defaultWidth: 220 },
  { id: 'authorization', label: 'Authorization', key: 'authorization', defaultWidth: 160 },
  { id: 'category', label: 'Category', key: 'category', defaultWidth: 170 },
  { id: 'verified', label: 'Verified', key: 'verified', defaultWidth: 120 },
  { id: 'phone', label: 'Phone', key: 'phone', defaultWidth: 150 },
  { id: 'contact', label: 'Contact', key: 'contact', defaultWidth: 160 },
  { id: 'address', label: 'Address', key: 'address', defaultWidth: 260 },
  { id: 'city', label: 'City', key: 'city', defaultWidth: 140 },
  { id: 'state', label: 'State', key: 'state', defaultWidth: 100 },
  { id: 'zip', label: 'Zip', key: 'zip', defaultWidth: 100 },
  { id: 'email', label: 'Email', key: 'email', defaultWidth: 240 },
  { id: 'notes', label: 'Notes', key: 'notes', defaultWidth: 260 },
  { id: 'website', label: 'Website', key: 'website', defaultWidth: 220 },
  { id: 'availability', label: 'Availability', key: 'availability', defaultWidth: 180 },
  { id: 'lat', label: 'Lat', key: 'lat', defaultWidth: 120 },
  { id: 'long', label: 'Long', key: 'long', defaultWidth: 120 },
];

const state = {
  rows: [],
  currentUserRole: 'user',
  currentUserId: 'anon',
  currentUserEmail: '',
  filters: { columnFilters: {} },
  sort: { key: '', dir: 'asc' },
  pagination: { page: 1, pageSize: 20 },
  columnOrder: ['actions', ...ALL_COLUMNS.map(c => c.id)],
  columnVisibility: Object.fromEntries([['actions', true], ...ALL_COLUMNS.map(c => [c.id, true])]),
  columnWidths: Object.fromEntries([['actions', 140], ...ALL_COLUMNS.map(c => [c.id, c.defaultWidth])]),
  columnLabels: {},
  drag: { resizing: null, draggingCol: null },

  // inline edit session
  inlineEdit: { td: null, rowId: null, colId: null, original: null, inputEl: null, saving: false, skipBlurCommit: false },
};

// ================== PERSISTENCE ==================
const STORAGE_PREFIX = 'techloc_services_table_prefs_v1';
const storageKey = () => `${STORAGE_PREFIX}:${state.currentUserId}`;
const safeParse = (raw) => {
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
};

const savePrefs = () => {
  localStorage.setItem(storageKey(), JSON.stringify({
    columnOrder: state.columnOrder,
    columnVisibility: state.columnVisibility,
    columnWidths: state.columnWidths,
    columnLabels: state.columnLabels,
    filters: state.filters,
    pagination: state.pagination,
  }));
};

const loadPrefs = () => {
  const prefs = safeParse(localStorage.getItem(storageKey()) || '');
  if (!prefs) return;

  const validCols = new Set(['actions', ...ALL_COLUMNS.map(c => c.id)]);

  if (Array.isArray(prefs.columnOrder)) {
    const cleaned = prefs.columnOrder.filter(c => validCols.has(c));
    const missing = [...validCols].filter(c => !cleaned.includes(c));
    state.columnOrder = [...cleaned, ...missing];
  }

  if (prefs.columnVisibility && typeof prefs.columnVisibility === 'object') {
    const next = { ...state.columnVisibility };
    for (const [k, v] of Object.entries(prefs.columnVisibility)) {
      if (validCols.has(k)) next[k] = !!v;
    }
    next.actions = true;
    state.columnVisibility = next;
  }

  if (prefs.columnWidths && typeof prefs.columnWidths === 'object') {
    const next = { ...state.columnWidths };
    for (const [k, v] of Object.entries(prefs.columnWidths)) {
      if (validCols.has(k) && Number.isFinite(+v)) next[k] = +v;
    }
    state.columnWidths = next;
  }

  if (prefs.columnLabels && typeof prefs.columnLabels === 'object') {
    const next = {};
    for (const [k, v] of Object.entries(prefs.columnLabels)) {
      if (validCols.has(k) && typeof v === 'string') next[k] = v;
    }
    state.columnLabels = next;
  }

  if (prefs.filters && typeof prefs.filters === 'object') {
    const nextFilters = { columnFilters: {} };
    if (prefs.filters.columnFilters && typeof prefs.filters.columnFilters === 'object') {
      for (const [colId, filter] of Object.entries(prefs.filters.columnFilters)) {
        if (!validCols.has(colId)) continue;
        const values = Array.isArray(filter?.values) ? filter.values : [];
        const query = typeof filter?.query === 'string' ? filter.query : '';
        if (values.length || query) nextFilters.columnFilters[colId] = { values, query };
      }
    }
    state.filters = nextFilters;
  }

  if (prefs.pagination && typeof prefs.pagination === 'object') {
    const page = Number.isFinite(+prefs.pagination.page) ? +prefs.pagination.page : 1;
    state.pagination.page = Math.max(1, page);
    if (Number.isFinite(+prefs.pagination.pageSize)) state.pagination.pageSize = +prefs.pagination.pageSize;
  }
};

export { ALL_COLUMNS, DB_FIELD_BY_COL_ID, loadPrefs, savePrefs, state };
