export const DashboardState = {
  filters: {
    dateRange: { start: '', end: '' },
    dateKey: '',
    salesChannels: [],
    salesChannelKey: '',
    lastLeadKey: '',
    lastLeadSelection: true,
    lastLeadFilterActive: true,
    categoryFilters: {},
    columnFilters: {},
    chartFilters: {},
    unitTypeKey: '',
    unitTypeSelection: [],
    vehicleStatusKey: '',
    vehicleStatusSelection: [],
    locationFocusActive: false,
  },
  vehiclesRaw: new Map(),
  schema: [],
  table: {
    page: 1,
    perPage: 8,
    sort: { key: '', direction: 'asc' },
    columns: {},
    columnOrder: [],
    columnWidths: {},
    columnLabels: {},
  },
  derived: {
    filtered: [],
    kpis: { active: 0, hold: 0 },
    statusBars: [],
  },
  chartSegments: {},
  chartVisibility: {},
  chartVisibilityOptions: {},
  realtime: { channel: null },
  layout: {
    alertsPanelWidth: null,
    chartSplitWidth: null,
    dealPanelHeight: null,
    fullChartHeight: null,
    fullChartCollapsed: false,
    tertiarySplitWidth: null,
  },
  preferences: {
    userId: null,
    config: null,
    saveTimer: null,
  },
  ui: { isLoading: true },
};

export const DEAL_STATUS_COLORS = [
  'from-emerald-400 to-green-500',
  'from-blue-400 to-indigo-500',
  'from-amber-400 to-orange-500',
  'from-rose-400 to-red-500',
  'from-teal-400 to-cyan-500',
  'from-slate-400 to-slate-500',
];
export const DEAL_STATUS_COLORS_ALT = [
  'from-fuchsia-400 to-purple-500',
  'from-cyan-400 to-sky-500',
  'from-lime-400 to-emerald-500',
  'from-orange-400 to-amber-500',
  'from-pink-400 to-rose-500',
  'from-violet-400 to-indigo-500',
];
export const DEFAULT_SEGMENT_KEY = 'dealStatus';

export const COLUMN_STORAGE_KEY = 'inventoryControlTableColumns';
export const PREFERENCES_STORAGE_KEY = 'inventoryControlPreferences';
export const CONFIG_TABLE = 'user_table_configs';
export const CONFIG_TABLE_NAME = 'Inventory Control';

export const formatDate = (value) => {
  if (!value) return '--';
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return '--';
  return parsed.toLocaleDateString('en-US', { month: '2-digit', day: '2-digit', year: '2-digit' });
};
export const formatNumber = (value) => new Intl.NumberFormat('en-US').format(value);
export const MILEAGE_COLUMNS = new Set(['mileage']);
export const CURRENCY_COLUMNS = new Set([
  'price on contract',
  'scheduled amount',
  'open balance',
  'regular amount',
]);
export const formatMileage = (value) => {
  if (value === null || value === undefined || value === '') return '--';
  const numeric = Number(String(value).replace(/,/g, ''));
  if (!Number.isFinite(numeric)) return value;
  return formatNumber(numeric);
};
export const formatCurrency = (value) => {
  if (value === null || value === undefined || value === '') return '--';
  const numeric = Number(String(value).replace(/[$,]/g, ''));
  if (!Number.isFinite(numeric)) return value;
  return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(numeric);
};

export const formatRelativeTime = (timestamp) => {
  if (!timestamp) return 'Updated --';
  const diffSeconds = Math.max(0, Math.round((Date.now() - timestamp) / 1000));
  if (diffSeconds < 60) return `Updated ${diffSeconds}s ago`;
  const minutes = Math.floor(diffSeconds / 60);
  if (minutes < 60) return `Updated ${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `Updated ${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `Updated ${days}d ago`;
};

export const formatColumnLabel = (key) => key.replace(/_/g, ' ');
export const getColumnLabel = (key) =>
  DashboardState.table.columnLabels?.[key]
  || DashboardState.schema.find((col) => col.key === key)?.label
  || formatColumnLabel(key);
export const applyColumnLabelOverrides = () => {
  if (!DashboardState.schema.length) return;
  DashboardState.schema = DashboardState.schema.map((col) => ({
    ...col,
    label: DashboardState.table.columnLabels?.[col.key] || formatColumnLabel(col.key),
  }));
};
export const setColumnLabelOverride = (key, label) => {
  const trimmed = label?.trim() || '';
  if (!DashboardState.table.columnLabels || typeof DashboardState.table.columnLabels !== 'object') {
    DashboardState.table.columnLabels = {};
  }
  if (trimmed) {
    DashboardState.table.columnLabels[key] = trimmed;
  } else {
    delete DashboardState.table.columnLabels[key];
  }
  const column = DashboardState.schema.find((col) => col.key === key);
  if (column) column.label = trimmed || formatColumnLabel(key);
};

export const inferColumnType = (values) => {
  let sawString = false;
  let sawNumber = false;
  let sawBoolean = false;
  let sawDate = false;

  values.forEach((value) => {
    if (value === null || value === undefined || value === '') return;
    if (typeof value === 'boolean') {
      sawBoolean = true;
      return;
    }
    if (typeof value === 'number' && !Number.isNaN(value)) {
      sawNumber = true;
      return;
    }
    if (value instanceof Date) {
      sawDate = true;
      return;
    }
    if (typeof value === 'string') {
      const trimmed = value.trim();
      const parsed = Date.parse(trimmed);
      if (!Number.isNaN(parsed) && /[-/T:]/.test(trimmed)) {
        sawDate = true;
        return;
      }
      if (trimmed && !Number.isNaN(Number(trimmed))) {
        sawNumber = true;
        return;
      }
      sawString = true;
      return;
    }
    sawString = true;
  });

  if (sawString) return 'string';
  if (sawDate && !sawNumber && !sawBoolean) return 'date';
  if (sawNumber && !sawBoolean) return 'number';
  if (sawBoolean && !sawNumber) return 'boolean';
  if (sawDate) return 'date';
  if (sawNumber) return 'number';
  if (sawBoolean) return 'boolean';
  return 'string';
};

export const DATE_COLUMNS = new Set([
  'Deal Date',
  'Calc. End',
  'Inv. Prep. Stat. On',
  'Oldest Invoice (Open)',
  'Last Payment date',
  'Last Ping',
]);
export const buildSchemaFromData = (rows) => {
  if (!rows?.length) return [];
  const sample = rows.slice(0, 50);
  return Object.keys(rows[0]).map((key) => {
    const values = sample.map((row) => row?.[key]);
    const override = DashboardState.table.columnLabels?.[key];
    return {
      key,
      label: override || formatColumnLabel(key),
      type: DATE_COLUMNS.has(key) ? 'date' : inferColumnType(values),
    };
  });
};

export const getColumnValues = (rows, key) => rows.map((row) => row?.[key]).filter((v) => v !== null && v !== undefined && v !== '');

export const getUniqueValues = (rows, key) => {
  const values = new Set();
  rows.forEach((row) => {
    const value = row?.[key];
    if (value !== null && value !== undefined && value !== '') values.add(String(value));
  });
  return Array.from(values).sort((a, b) => a.localeCompare(b));
};

export const detectDateKey = (schema) => {
  const preferred = schema.find((col) => col.type === 'date' && /created_at|created|updated_at|updated/i.test(col.key));
  if (preferred) return preferred.key;
  return schema.find((col) => col.type === 'date')?.key || '';
};

export const detectSalesChannelKey = (schema) =>
  schema.find((col) => /sales channel/i.test(col.key))?.key || '';

export const detectLastLeadKey = (schema) =>
  schema.find((col) => /last\s*(lead|deal)/i.test(col.key) || /last\s*(lead|deal)/i.test(col.label))?.key || '';

export const detectUnitTypeKey = (schema) =>
  schema.find((col) => /unit\s*type/i.test(col.key) || /unit\s*type/i.test(col.label))?.key || '';

export const detectVehicleStatusKey = (schema) =>
  schema.find((col) => col.key === 'status')?.key
  || schema.find((col) => /vehicle\s*status/i.test(col.key) || /vehicle\s*status/i.test(col.label))?.key
  || '';

export const INV_PREP_STATUS_CLASSES = {
  'out for repo': 'bg-gradient-to-r from-pink-500/20 via-pink-400/10 to-pink-500/20 backdrop-blur-sm',
  'passtime unavailable': 'bg-gradient-to-r from-amber-700/25 via-amber-500/10 to-amber-700/25 backdrop-blur-sm',
  accident: 'bg-gradient-to-r from-orange-500/25 via-orange-400/10 to-orange-500/25 backdrop-blur-sm',
  'not stock': 'bg-gradient-to-r from-slate-500/25 via-slate-400/10 to-slate-500/25 backdrop-blur-sm',
  stolen: 'bg-gradient-to-r from-red-500/25 via-red-400/10 to-red-500/25 backdrop-blur-sm',
  'm&t repo title in process': 'bg-gradient-to-r from-blue-600/25 via-blue-500/10 to-blue-600/25 backdrop-blur-sm',
  'repoed on hold': 'bg-gradient-to-r from-blue-700/25 via-blue-500/10 to-blue-700/25 backdrop-blur-sm',
  'third party repair shop': 'bg-gradient-to-r from-yellow-300/20 via-yellow-200/10 to-yellow-300/20 backdrop-blur-sm',
  'available for deals': 'bg-gradient-to-r from-emerald-400/20 via-emerald-300/10 to-emerald-400/20 backdrop-blur-sm',
};

export const detectInvPrepStatusKey = (schema) =>
  schema.find((col) =>
    /inv\.?\s*prep\.?\s*stat/i.test(col.key)
      || /inv\.?\s*prep\.?\s*stat/i.test(col.label)
      || /inventory\s*preparation\s*status/i.test(col.key)
      || /inventory\s*preparation\s*status/i.test(col.label)
  )?.key || '';

export const getInvPrepStatusValue = (item, key) => {
  const value = key ? item?.[key] : undefined;
  if (value !== undefined && value !== null && value !== '') return String(value);
  const fallback = item?.prepStatus ?? item?.['Inventory Preparation Status'] ?? item?.['Inv. Prep. Stat'];
  return fallback !== undefined && fallback !== null && fallback !== '' ? String(fallback) : '';
};

export const getInvPrepStatusRowClass = (status) => {
  if (!status) return '';
  const normalized = String(status).trim().toLowerCase();
  return INV_PREP_STATUS_CLASSES[normalized] || '';
};

export const formatInvPrepStatusLabel = (status) => {
  if (status === null || status === undefined || status === '') return '';
  const raw = String(status).trim();
  if (!raw) return '';
  const normalized = raw.toLowerCase();
  if (normalized === 'available for delas' || normalized === 'available for deals') return 'AVAILABLE';
  if (normalized === 'm& t repo title in process' || normalized === 'm&t repo title in process') return 'TITLE IN PROCESS';
  if (normalized === 'pending auction - manhein' || normalized === 'pending auction - manheim') return 'AUCTION';
  if (normalized === 'third party repair shop') return 'REPAIR SHOP';
  return raw;
};

export const normalizeBoolean = (value) => {
  if (typeof value === 'boolean') return value;
  if (typeof value === 'number') return value !== 0;
  if (typeof value === 'string') {
    const normalized = value.trim().toLowerCase();
    return ['true', 'yes', 'y', '1'].includes(normalized);
  }
  return Boolean(value);
};

export const detectTruthyColumnValue = (rows, key) => {
  if (!key) return '';
  const truthyRow = rows.find((row) => normalizeBoolean(row?.[key]));
  if (truthyRow) return String(truthyRow[key]);
  const truthyValue = getUniqueValues(rows, key).find((value) => normalizeBoolean(value));
  return truthyValue || '';
};

export const detectCategoryKeys = (schema, rows, excludeKeys) => {
  const candidates = schema
    .filter((col) => !excludeKeys.includes(col.key))
    .filter((col) => ['string', 'number', 'boolean'].includes(col.type))
    .map((col) => ({
      key: col.key,
      label: col.label,
      values: getUniqueValues(rows, col.key),
    }))
    .filter((entry) => entry.values.length > 0 && entry.values.length <= 50)
    .sort((a, b) => b.values.length - a.values.length);

  return candidates.slice(0, 4);
};
