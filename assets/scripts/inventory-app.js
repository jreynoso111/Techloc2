import { setupBackgroundManager } from './backgroundManager.js';
setupBackgroundManager();

import {
  DashboardState,
  DEFAULT_SEGMENT_KEY,
  COLUMN_STORAGE_KEY,
  PREFERENCES_STORAGE_KEY,
  CONFIG_TABLE,
  CONFIG_TABLE_NAME,
  formatDate,
  formatColumnLabel,
  getColumnLabel,
  applyColumnLabelOverrides,
  setColumnLabelOverride,
  buildSchemaFromData,
  getColumnValues,
  getUniqueValues,
  detectDateKey,
  detectSalesChannelKey,
  detectLastLeadKey,
  detectUnitTypeKey,
  detectVehicleStatusKey,
  detectTruthyColumnValue,
  detectCategoryKeys,
  normalizeBoolean,
  detectInvPrepStatusKey,
  formatInvPrepStatusLabel,
} from './core/state.js';
import { initDashboardUI } from './ui/uiController.js';
import {
  getSupabaseClient,
  hydrateVehiclesFromSupabase,
  initializeSupabaseRealtime,
} from './api/supabase.js';
import { getVehicles } from './services/fleetService.js';

// ==========================================================
// 1) SUPABASE CLIENT (robusto)
// - Primero intenta importar tu supabaseClient.js
// - Si falla, crea el cliente aquí con URL/KEY
// ==========================================================

const createLucideSvg = (name, className = '') => {
  const iconDef = lucide?.icons?.[name];
  if (!iconDef) return null;
  const svgString = iconDef.toSvg({ class: className, 'data-lucide': name, 'data-lucide-initialized': 'true' });
  const wrapper = document.createElement('span');
  wrapper.innerHTML = svgString;
  return wrapper.firstElementChild;
};

const initializeLucideIcons = (root = document) => {
  if (!lucide?.icons) return;
  const icons = [];
  if (root.matches?.('[data-lucide]')) icons.push(root);
  root.querySelectorAll?.('[data-lucide]').forEach((icon) => icons.push(icon));
  icons.forEach((icon) => {
    if (icon.getAttribute('data-lucide-initialized') === 'true') return;
    const name = icon.getAttribute('data-lucide');
    const className = icon.getAttribute('class') || '';
    const svgEl = createLucideSvg(name, className);
    if (svgEl) icon.replaceWith(svgEl);
  });
};

const updateLucideIcon = (icon, name) => {
  if (!icon) return;
  icon.setAttribute('data-lucide', name);
  icon.removeAttribute('data-lucide-initialized');
  initializeLucideIcons(icon);
};

initializeLucideIcons();

const PILL_CLASSES = 'rounded-xl border border-slate-700 bg-slate-950/60 px-2 py-1 text-[10px] hover:bg-slate-900/60';
// ✅ Fallback: pega tus credenciales si tu módulo no exporta bien.
//    (Anon key es pública, pero igual cuida RLS.)
const SUPABASE_URL = '';       // <-- pega aquí
const SUPABASE_ANON_KEY = '';  // <-- pega aquí

const showDebug = (title, detail, obj) => {
  const banner = document.getElementById('debug-banner');
  const text = document.getElementById('debug-text');
  const pre = document.getElementById('debug-pre');
  banner.classList.remove('hidden');
  text.textContent = `${title} — ${detail || ''}`.trim();
  pre.textContent = obj ? JSON.stringify(obj, null, 2) : '';
  document.getElementById('debug-copy').onclick = async () => {
    const payload = `${title}\n${detail || ''}\n\n${pre.textContent}`.trim();
    try { await navigator.clipboard.writeText(payload); } catch {}
  };
};

const setConnectionStatus = (status) => {
  const statusEl = document.getElementById('connection-status');
  const dotEl = document.getElementById('connection-dot');
  statusEl.textContent = status;

  dotEl.className = 'h-2.5 w-2.5 rounded-full';
  if (status === 'Live') dotEl.classList.add('bg-emerald-400', 'shadow-[0_0_10px_rgba(52,211,153,0.8)]');
  else if (status.includes('Reconnect')) dotEl.classList.add('bg-amber-400', 'shadow-[0_0_10px_rgba(251,191,36,0.8)]');
  else dotEl.classList.add('bg-slate-500');
};

let supabaseClient = null;
let alertsDealsRows = [];
let alertsDealsFilter = 'all';
let alertsDealsFilterOptions = [];
const ALERTS_STORAGE_PREFIX = 'alertsDeals';
const ALERTS_COLUMNS_STORAGE_KEY = `${ALERTS_STORAGE_PREFIX}:columns`;
const ALERTS_COLUMNS_LABELS_KEY = `${ALERTS_STORAGE_PREFIX}:columnLabels`;
const ALERTS_LAST_CLICKS_TABLE = 'control_map_vehicle_clicks';
let alertsDealsColumns = [];
let alertsDealsColumnLabels = {};
let alertsDealsAvailableColumns = [];
let alertsDealsSortKey = '';
let alertsDealsSortDirection = 'asc';
let alertsDealsLastClickByVin = {};
let alertsDealsLastClickLoadedForUser = null;
const setAlertsDealCount = (count) => {
  const badge = document.getElementById('alerts-deals-count');
  const modalCount = document.querySelector('[data-alerts-deals-count]');
  const row = document.getElementById('alerts-deals-row');
  const rowCount = document.getElementById('alerts-deals-row-count');
  if (!badge) return;
  const safeCount = Number(count) || 0;
  if (safeCount <= 0) {
    badge.classList.add('hidden');
    badge.textContent = '';
    if (modalCount) modalCount.textContent = '0';
    if (rowCount) rowCount.textContent = '0';
    row?.classList.remove('hidden');
    return;
  }
  badge.textContent = String(safeCount);
  if (modalCount) modalCount.textContent = String(safeCount);
  badge.classList.remove('hidden');
  row?.classList.remove('hidden');
  if (rowCount) rowCount.textContent = String(safeCount);
};

const getAlertsColumnLabel = (key) => alertsDealsColumnLabels[key] || formatColumnLabel(key);
const getStatusBadgeClasses = (status) => {
  const normalized = String(status || '').trim().toUpperCase();
  const base = 'rounded-full border px-2 py-0.5 text-[9px] font-semibold uppercase';
  if (normalized === 'ACTIVE') return `${base} border-emerald-500/40 bg-emerald-500/10 text-emerald-200`;
  if (normalized === 'STOCK') return `${base} border-blue-500/40 bg-blue-500/10 text-blue-200`;
  if (normalized === 'STOLEN') return `${base} border-rose-500/40 bg-rose-500/10 text-rose-200`;
  return `${base} border-slate-500/40 bg-slate-500/10 text-slate-200`;
};

const getAlertsColumnValue = (row, key, vin, vinQuery) => {
  if (key === 'VIN') {
    return vin
      ? `<a class="text-blue-200 underline decoration-transparent underline-offset-2 transition hover:decoration-blue-200" href="https://www.google.com/search?q=%22${vinQuery}%22" target="_blank" rel="noreferrer">${vin}</a>`
      : '—';
  }
  const value = row?.[key];
  if (value === null || value === undefined || value === '') return '—';
  return String(value);
};

const getStoredAlertsLastClick = (vin) => {
  if (!vin) return '';
  return alertsDealsLastClickByVin[vin] || localStorage.getItem(`${ALERTS_STORAGE_PREFIX}:${vin}:lastClick`) || '';
};

const renderAlertsDealsList = (rows) => {
  const list = document.getElementById('alerts-deals-list');
  if (!list) return;
  list.innerHTML = '';
  if (!rows?.length) {
    list.innerHTML = '<p class="text-xs text-slate-400">No matching deals found.</p>';
    return;
  }
  rows.forEach((row) => {
    const vin = row.VIN || '';
    const vinQuery = encodeURIComponent(vin);
    const prepStatus = String(row['Inventory Preparation Status'] || '').trim();
    const storageKey = vin ? `${ALERTS_STORAGE_PREFIX}:${vin}` : '';
    const storedNote = storageKey ? localStorage.getItem(`${storageKey}:note`) : '';
    const storedClick = getStoredAlertsLastClick(vin);
    const visibleColumns = alertsDealsColumns.length
      ? alertsDealsColumns
      : ['VIN', 'Vehicle Status', 'Current Stock No', 'Physical Location', 'Inventory Preparation Status'];
    const columnMarkup = visibleColumns
      .map((key) => {
        const label = getAlertsColumnLabel(key);
        const value = getAlertsColumnValue(row, key, vin, vinQuery);
        return `<span><span class="text-slate-400">${label}:</span> ${value}</span>`;
      })
      .join('');
    const item = document.createElement('div');
    item.className = 'rounded-xl border border-slate-800 bg-slate-950/40 p-3';
    item.innerHTML = `
      <div class="flex items-center justify-between gap-3">
        <div class="flex flex-wrap items-center gap-2 text-[10px] font-semibold uppercase tracking-[0.2em]">
          <span class="${getStatusBadgeClasses(row['Vehicle Status'])}">${row['Vehicle Status'] || 'Unknown'}</span>
        </div>
        ${vin ? `
        <a class="shrink-0 rounded-full border border-slate-700 bg-slate-950/70 px-3 py-1 text-[10px] font-semibold uppercase tracking-[0.2em] text-slate-200 transition hover:border-blue-400 hover:text-white" href="https://www.google.com/search?q=%22${vinQuery}%22" target="_blank" rel="noreferrer" data-alerts-google-button data-alerts-google-target="${vin}">Google</a>
        ` : ''}
      </div>
      <div class="mt-2 flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-slate-200">
        ${columnMarkup}
        <span class="text-slate-400" data-alerts-google-last="${vin}">${storedClick || '—'}</span>
      </div>
      <div class="mt-2 flex flex-wrap items-center gap-3 text-xs text-slate-200">
        <label class="flex min-w-[220px] flex-1 items-center gap-2 text-slate-200">
          <span class="text-slate-400">Notes:</span>
          <input type="text" class="h-7 w-full rounded-lg border border-slate-800 bg-slate-950/70 px-2 text-xs text-slate-200" placeholder="Add notes" data-alerts-google-notes data-alerts-notes-key="${storageKey}" value="${storedNote || ''}">
        </label>
      </div>
    `;
    list.appendChild(item);
  });
};

const sortAlertsDealsRows = (rows) => {
  if (!alertsDealsSortKey) return rows;
  const direction = alertsDealsSortDirection === 'desc' ? -1 : 1;
  return [...rows].sort((a, b) => {
    const valueA = String(a?.[alertsDealsSortKey] ?? '').toLowerCase();
    const valueB = String(b?.[alertsDealsSortKey] ?? '').toLowerCase();
    if (valueA < valueB) return -1 * direction;
    if (valueA > valueB) return 1 * direction;
    return 0;
  });
};

const getFilteredAlertsDealsRows = () => {
  if (alertsDealsFilter === 'all') return alertsDealsRows;
  return alertsDealsRows.filter((row) => {
    const prepStatus = String(row['Inventory Preparation Status'] || '').trim().toLowerCase();
    return prepStatus === alertsDealsFilter;
  });
};

const renderAlertsDealsFilters = () => {
  const container = document.getElementById('alerts-deals-filters');
  if (!container) return;
  container.innerHTML = '';
  const options = ['all', ...alertsDealsFilterOptions];
  options.forEach((option) => {
    const button = document.createElement('button');
    button.type = 'button';
    button.dataset.alertsDealsFilter = option;
    button.className = 'rounded-full border border-slate-700 bg-slate-950/70 px-3 py-1 text-slate-200 transition hover:border-blue-400 hover:text-white';
    if (option === alertsDealsFilter) {
      button.classList.add('border-blue-400', 'text-white');
    }
    button.textContent = option === 'all' ? 'All' : option.replace(/\b\w/g, (char) => char.toUpperCase());
    container.appendChild(button);
  });
};

const renderAlertsDealsColumnHeaders = () => {
  const container = document.getElementById('alerts-deals-column-headers');
  if (!container) return;
  container.innerHTML = '';
  alertsDealsColumns.forEach((key) => {
    const label = getAlertsColumnLabel(key);
    const button = document.createElement('button');
    button.type = 'button';
    button.dataset.alertsDealsSortKey = key;
    button.className = 'rounded-full border border-slate-700 bg-slate-950/70 px-3 py-1 text-[10px] font-semibold uppercase tracking-[0.2em] text-slate-200 transition hover:border-blue-400 hover:text-white';
    const sortSuffix = alertsDealsSortKey === key ? (alertsDealsSortDirection === 'asc' ? ' ↑' : ' ↓') : '';
    button.textContent = `${label}${sortSuffix}`;
    container.appendChild(button);
  });
};

const renderAlertsDealsColumns = () => {
  const list = document.getElementById('alerts-deals-columns-list');
  if (!list) return;
  list.innerHTML = '';
  alertsDealsAvailableColumns.forEach((key) => {
    const row = document.createElement('div');
    row.className = 'flex items-center gap-2';
    row.innerHTML = `
      <input type="checkbox" class="h-3 w-3 rounded border-slate-600 bg-slate-950/70 text-blue-400" data-alerts-column-key="${key}" ${alertsDealsColumns.includes(key) ? 'checked' : ''}>
      <input type="text" class="h-7 flex-1 rounded-lg border border-slate-800 bg-slate-950/70 px-2 text-xs text-slate-200" data-alerts-column-label="${key}" value="${getAlertsColumnLabel(key)}">
    `;
    list.appendChild(row);
  });
};

const updateAlertsDealsList = () => {
  renderAlertsDealsList(sortAlertsDealsRows(getFilteredAlertsDealsRows()));
};

const formatAlertsTimestamp = (date) => {
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  const year = String(date.getFullYear()).slice(-2);
  const hours = String(date.getHours()).padStart(2, '0');
  const minutes = String(date.getMinutes()).padStart(2, '0');
  return `${month}/${day}/${year} ${hours}:${minutes}`;
};


const formatAlertsTimestampFromDbValue = (value) => {
  if (!value) return '';
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return '';
  return formatAlertsTimestamp(parsed);
};

const loadAlertsLastClicksFromSupabase = async (rows = []) => {
  const userId = DashboardState.preferences.userId;
  if (!supabaseClient?.from || !userId || !Array.isArray(rows) || !rows.length) return;
  const vins = Array.from(new Set(rows.map((row) => String(row?.VIN || '').trim()).filter(Boolean)));
  if (!vins.length) return;

  const { data, error } = await supabaseClient
    .from(ALERTS_LAST_CLICKS_TABLE)
    .select('vin, clicked_at')
    .eq('user_id', userId)
    .in('vin', vins)
    .order('clicked_at', { ascending: false });

  if (error || !Array.isArray(data)) return;

  const nextByVin = { ...alertsDealsLastClickByVin };
  data.forEach((entry) => {
    const vin = String(entry?.vin || '').trim();
    if (!vin || nextByVin[vin]) return;
    const formatted = formatAlertsTimestampFromDbValue(entry?.clicked_at);
    if (formatted) nextByVin[vin] = formatted;
  });
  alertsDealsLastClickByVin = nextByVin;
  alertsDealsLastClickLoadedForUser = userId;
};

const insertAlertsLastClickHistory = async (vin, clickedAtIso) => {
  const userId = DashboardState.preferences.userId;
  if (!supabaseClient?.from || !userId || !vin || !clickedAtIso) return;
  await supabaseClient
    .from(ALERTS_LAST_CLICKS_TABLE)
    .insert({ user_id: userId, vin, clicked_at: clickedAtIso, source: 'google_button' });
};

const getGpsOfflineCount = () => {
  const cutoff = Date.now() - (10 * 24 * 60 * 60 * 1000);
  const rows = DashboardState.derived.filtered || [];
  return rows.filter((row) => {
    const vin = row?.vin || row?.VIN || getField(row, 'VIN');
    if (!vin) return false;
    const dealStatus = String(getField(row, 'Deal Status', 'dealStatus') || '').trim().toUpperCase();
    if (dealStatus !== 'ACTIVE') return false;
    const lastPingValue = getField(row, 'Last Ping', 'last_ping', 'LastPing');
    const lastPingString = String(lastPingValue ?? '').trim();
    if (!lastPingString) return true;
    const parsed = new Date(lastPingString);
    if (Number.isNaN(parsed.getTime())) return true;
    return parsed.getTime() < cutoff;
  }).length;
};

const updateGpsOfflineCluster = () => {
  const count = getGpsOfflineCount();
  const badge = document.getElementById('alerts-gps-offline-count');
  const collapsedBadge = document.getElementById('alerts-gps-offline-badge');
  if (badge) badge.textContent = String(count);
  if (collapsedBadge) collapsedBadge.textContent = String(count);
};

const fetchAlertsDealCount = async () => {
  if (!supabaseClient?.from) {
    setAlertsDealCount(0);
    alertsDealsRows = [];
    updateAlertsDealsList();
    return;
  }
  const { data, error } = await supabaseClient
    .from('DealsJP1')
    .select('*');
  if (error || !Array.isArray(data)) {
    return;
  }
  const onlineRows = data.filter((row) => {
    if (!row?.VIN) return false;
    const status = String(row['Vehicle Status'] || '').trim().toUpperCase();
    if (!['ACTIVE', 'STOCK', 'STOLEN'].includes(status)) return false;
    const prepStatus = String(row['Inventory Preparation Status'] || '').trim().toLowerCase();
    return [
      'out for repo',
      'stolen',
      'accidented',
      'accident',
      'stolen vehicle',
      'third party repair shop',
    ].includes(prepStatus);
  });
  setAlertsDealCount(onlineRows.length);
  alertsDealsRows = onlineRows;
  alertsDealsFilterOptions = Array.from(
    new Set(
      onlineRows
        .map((row) => String(row['Inventory Preparation Status'] || '').trim().toLowerCase())
        .filter(Boolean),
    ),
  ).sort();
  alertsDealsAvailableColumns = Array.from(
    new Set(
      onlineRows.flatMap((row) => Object.keys(row || {})),
    ),
  ).sort();
  if (alertsDealsLastClickLoadedForUser !== DashboardState.preferences.userId) {
    await loadAlertsLastClicksFromSupabase(onlineRows);
  }
  if (!alertsDealsColumns.length) {
    const storedColumns = localStorage.getItem(ALERTS_COLUMNS_STORAGE_KEY);
    alertsDealsColumns = storedColumns ? JSON.parse(storedColumns) : [
      'VIN',
      'Vehicle Status',
      'Current Stock No',
      'Physical Location',
      'Inventory Preparation Status',
    ];
  }
  alertsDealsColumns = alertsDealsColumns.filter((key) => alertsDealsAvailableColumns.includes(key));
  alertsDealsColumnLabels = {
    ...alertsDealsColumnLabels,
    ...JSON.parse(localStorage.getItem(ALERTS_COLUMNS_LABELS_KEY) || '{}'),
  };
  if (alertsDealsFilter !== 'all' && !alertsDealsFilterOptions.includes(alertsDealsFilter)) {
    alertsDealsFilter = 'all';
  }
  renderAlertsDealsFilters();
  renderAlertsDealsColumns();
  renderAlertsDealsColumnHeaders();
  updateAlertsDealsList();
};

// ==========================================================
// 2) DASHBOARD STATE + HELPERS (tu lógica con mejoras mínimas)
// ==========================================================

const getVehicleKey = (vehicle) => vehicle.id ?? '';
const normalizeStatus = (value) => {
  const raw = String(value ?? '').trim();
  if (!raw) return 'Active';
  const normalized = raw.toLowerCase();
  if (normalized.includes('hold')) return 'On Hold';
  if (normalized.includes('transit')) return 'In Transit';
  if (normalized.includes('recover')) return 'Recovery';
  if (normalized.includes('ready')) return 'Ready';
  if (normalized.includes('write')) return 'Write-Off';
  if (normalized.includes('active')) return 'Active';
  return 'Active';
};
const normalizePhysicalLocation = (value) => {
  if (value === null || value === undefined) return value;
  if (typeof value !== 'string') return value;
  const trimmed = value.trim();
  if (!trimmed) return trimmed;
  const normalized = trimmed.toUpperCase();
  if (normalized === 'COPART' || normalized === 'CO PART - PENDING FUNDING' || normalized === 'CO PART- PENDING FUNDING') {
    return 'COPART';
  }
  if (normalized === 'EXTERNAL DEALER FOR FINANCE EXTERNAL DEAL') {
    return 'External Dealer';
  }
  if (normalized === 'HOUSBY TRUCK & EQUIPMENT SOLUTIONS, LLC') {
    return 'HOUSBY';
  }
  return trimmed;
};
const LOCATION_FILTER_VALUES = [
  'AAI',
  'Central CA Truck',
  'Copart',
  'DRS Truck Sales',
  'External Dealer',
  'Housby',
  'Remarket Place',
  'Ritchie Bros Auction',
  'Starpoint agent',
];
const LOCATION_FILTER_SET = new Set(LOCATION_FILTER_VALUES.map((value) => value.toLowerCase()));
const STATUS_FILTER_SET = new Set(['active', 'stock', 'stolen']);
const getField = (row, ...keys) => {
  for (const key of keys) {
    const value = row?.[key];
    if (value !== undefined && value !== null && value !== '') return value;
  }
  return '';
};
const normalizeVehicle = (vehicle) => {
  const updatedAt = getField(vehicle, 'Updated At', 'Updated', 'Last Updated');
  const createdAt = getField(vehicle, 'Created At');
  const dateValue = getField(vehicle, 'Date') || updatedAt || createdAt;
  const vin = getField(vehicle, 'VIN');
  const uniqueId = vehicle.id || getField(vehicle, 'Current Stock No') || vin;
  const physicalLocation = getField(vehicle, 'Physical Location');
  const normalizedPhysicalLocation = normalizePhysicalLocation(physicalLocation);
  return {
    ...vehicle,
    id: uniqueId || null,
    stockNo: getField(vehicle, 'Current Stock No'),
    vin,
    status: normalizeStatus(getField(vehicle, 'Vehicle Status')),
    hold: normalizeBoolean(getField(vehicle, 'HOLD')),
    isLastDeal: normalizeBoolean(getField(vehicle, 'Last Deal', 'last_deal')),
    brand: getField(vehicle, 'Brand'),
    model: getField(vehicle, 'Model'),
    modelYear: getField(vehicle, 'Model Year'),
    dealStatus: getField(vehicle, 'Deal Status'),
    gpsStatus: getField(vehicle, 'gps_status'),
    gpsFlag: getField(vehicle, 'gps_review_flag'),
    completion: getField(vehicle, 'Deal Completion'),
    psrCategory: getField(vehicle, 'PSR Category'),
    prepStatus: getField(vehicle, 'Inventory Preparation Status'),
    createdAt: createdAt ? String(createdAt).slice(0, 10) : '',
    updatedAt: updatedAt ? String(updatedAt).slice(0, 10) : '',
    date: dateValue ? String(dateValue).slice(0, 10) : '',
    lastEventAt: vehicle.lastEventAt || (updatedAt ? new Date(updatedAt).getTime() : null),
    'Physical Location': normalizedPhysicalLocation,
  };
};

const getOrderedColumns = (columns) => {
  const order = DashboardState.table.columnOrder;
  if (!Array.isArray(order) || !order.length) return columns;
  const byKey = new Map(columns.map((col) => [col.key, col]));
  const ordered = order.map((key) => byKey.get(key)).filter(Boolean);
  columns.forEach((col) => {
    if (!order.includes(col.key)) ordered.push(col);
  });
  return ordered;
};

const buildPreferencesPayload = () => ({
  table: {
    columns: DashboardState.table.columns,
    columnOrder: DashboardState.table.columnOrder,
    columnWidths: DashboardState.table.columnWidths,
    sort: DashboardState.table.sort,
    perPage: DashboardState.table.perPage,
    columnLabels: DashboardState.table.columnLabels,
  },
  chart: {
    chartSegments: DashboardState.chartSegments,
    chartVisibility: DashboardState.chartVisibility,
  },
  filters: {
    dateRange: DashboardState.filters.dateRange,
    salesChannels: DashboardState.filters.salesChannels,
    lastLeadSelection: DashboardState.filters.lastLeadSelection,
    categoryFilters: DashboardState.filters.categoryFilters,
    columnFilters: DashboardState.filters.columnFilters,
    chartFilters: DashboardState.filters.chartFilters,
    unitTypeSelection: DashboardState.filters.unitTypeSelection,
    vehicleStatusSelection: DashboardState.filters.vehicleStatusSelection,
    locationFocusActive: DashboardState.filters.locationFocusActive,
  },
  layout: {
    alertsPanelWidth: DashboardState.layout.alertsPanelWidth,
    chartSplitWidth: DashboardState.layout.chartSplitWidth,
    dealPanelHeight: DashboardState.layout.dealPanelHeight,
    fullChartHeight: DashboardState.layout.fullChartHeight,
    fullChartHeights: DashboardState.layout.fullChartHeights,
    fullChartCollapsed: DashboardState.layout.fullChartCollapsed,
    tertiarySplitWidth: DashboardState.layout.tertiarySplitWidth,
  },
  alerts: {
    lastClicksByVin: alertsDealsLastClickByVin,
  },
});

const applyPreferences = (config) => {
  if (!config || typeof config !== 'object') return;
  if (config.table?.sort) DashboardState.table.sort = config.table.sort;
  if (typeof config.table?.perPage === 'number') DashboardState.table.perPage = config.table.perPage;
  if (Array.isArray(config.table?.columnOrder)) DashboardState.table.columnOrder = config.table.columnOrder;
  if (config.table?.columnWidths) DashboardState.table.columnWidths = config.table.columnWidths;
  if (config.table?.columnLabels && typeof config.table.columnLabels === 'object') {
    DashboardState.table.columnLabels = config.table.columnLabels;
  }
  applyColumnLabelOverrides();
  if (config.chart?.chartSegments && typeof config.chart.chartSegments === 'object') {
    DashboardState.chartSegments = { ...config.chart.chartSegments };
  }
  if (config.chart?.chartVisibility && typeof config.chart.chartVisibility === 'object') {
    DashboardState.chartVisibility = { ...config.chart.chartVisibility };
  }
  if (config.chart?.segmentKey && (!config.chart.chartSegments || typeof config.chart.chartSegments !== 'object')) {
    DashboardState.chartSegments = { default: config.chart.segmentKey };
  }
  if (config.filters?.dateRange) DashboardState.filters.dateRange = { ...DashboardState.filters.dateRange, ...config.filters.dateRange };
  if (Array.isArray(config.filters?.salesChannels)) DashboardState.filters.salesChannels = config.filters.salesChannels;
  if (config.filters?.lastLeadSelection === 'all' || typeof config.filters?.lastLeadSelection === 'boolean') {
    DashboardState.filters.lastLeadSelection = config.filters.lastLeadSelection;
  }
  if (config.filters?.categoryFilters) DashboardState.filters.categoryFilters = config.filters.categoryFilters;
  if (config.filters?.columnFilters) DashboardState.filters.columnFilters = config.filters.columnFilters;
  if (config.filters?.chartFilters && typeof config.filters?.chartFilters === 'object') {
    DashboardState.filters.chartFilters = Object.entries(config.filters.chartFilters).reduce((acc, [chartId, filter]) => {
      if (!filter || typeof filter !== 'object') return acc;
      const key = filter.key || DEFAULT_SEGMENT_KEY;
      const values = Array.isArray(filter.values)
        ? filter.values.map((value) => String(value))
        : (filter.value ? [String(filter.value)] : []);
      if (!values.length) return acc;
      acc[chartId] = { key, values };
      return acc;
    }, {});
  }
  if (!Object.keys(DashboardState.filters.chartFilters || {}).length && config.filters?.chartFilter && typeof config.filters.chartFilter === 'object') {
    const fallbackKey = config.filters.chartFilter.key || DEFAULT_SEGMENT_KEY;
    const fallbackValue = config.filters.chartFilter.value;
    DashboardState.filters.chartFilters = fallbackValue
      ? { default: { key: fallbackKey, values: [String(fallbackValue)] } }
      : {};
  }
  if (Array.isArray(config.filters?.unitTypeSelection)) {
    DashboardState.filters.unitTypeSelection = config.filters.unitTypeSelection.map((value) => String(value));
  } else if (typeof config.filters?.unitTypeSelection === 'string') {
    DashboardState.filters.unitTypeSelection = [config.filters.unitTypeSelection];
  }
  if (Array.isArray(config.filters?.vehicleStatusSelection)) {
    DashboardState.filters.vehicleStatusSelection = config.filters.vehicleStatusSelection.map((value) => String(value));
  } else if (typeof config.filters?.vehicleStatusSelection === 'string') {
    DashboardState.filters.vehicleStatusSelection = [config.filters.vehicleStatusSelection];
  }
  if (typeof config.filters?.locationFocusActive === 'boolean') {
    DashboardState.filters.locationFocusActive = config.filters.locationFocusActive;
  }
  if (typeof config.layout?.alertsPanelWidth === 'number') DashboardState.layout.alertsPanelWidth = config.layout.alertsPanelWidth;
  if (typeof config.layout?.chartSplitWidth === 'number') DashboardState.layout.chartSplitWidth = config.layout.chartSplitWidth;
  if (typeof config.layout?.dealPanelHeight === 'number') DashboardState.layout.dealPanelHeight = config.layout.dealPanelHeight;
  if (typeof config.layout?.fullChartHeight === 'number') DashboardState.layout.fullChartHeight = config.layout.fullChartHeight;
  if (config.layout?.fullChartHeights && typeof config.layout.fullChartHeights === 'object') {
    DashboardState.layout.fullChartHeights = { ...config.layout.fullChartHeights };
  }
  if (typeof config.layout?.fullChartCollapsed === 'boolean') DashboardState.layout.fullChartCollapsed = config.layout.fullChartCollapsed;
  if (typeof config.layout?.tertiarySplitWidth === 'number') DashboardState.layout.tertiarySplitWidth = config.layout.tertiarySplitWidth;
  if (config.alerts?.lastClicksByVin && typeof config.alerts.lastClicksByVin === 'object') {
    alertsDealsLastClickByVin = { ...config.alerts.lastClicksByVin };
  }
};

const fetchTableConfig = async (userId) => {
  if (!supabaseClient?.from || !userId) return null;
  const { data, error } = await supabaseClient
    .from(CONFIG_TABLE)
    .select('config')
    .eq('user_id', userId)
    .eq('table_name', CONFIG_TABLE_NAME)
    .maybeSingle();
  if (error) return null;
  return data?.config || null;
};

const fetchAdminUserId = async () => {
  if (!supabaseClient?.from) return null;
  const { data, error } = await supabaseClient
    .from('profiles')
    .select('id')
    .eq('role', 'administrator')
    .limit(1)
    .maybeSingle();
  if (error) return null;
  return data?.id || null;
};

const loadDashboardPreferences = async () => {
  let config = null;
  if (supabaseClient?.auth) {
    const { data } = await supabaseClient.auth.getSession();
    const userId = data?.session?.user?.id || null;
    DashboardState.preferences.userId = userId;
    if (userId) {
      config = await fetchTableConfig(userId);
      if (!config) {
        const adminId = await fetchAdminUserId();
        if (adminId) config = await fetchTableConfig(adminId);
      }
    }
  }

  if (!config) {
    try {
      const raw = localStorage.getItem(PREFERENCES_STORAGE_KEY);
      config = raw ? JSON.parse(raw) : null;
    } catch {}
  }

  if (config) {
    DashboardState.preferences.config = config;
    applyPreferences(config);
    updateLocationFilterToggle();
  }
};

const applyLayoutPreferencesToDom = () => {
  const alertsPanel = document.getElementById('alerts-panel');
  const dealPanel = document.getElementById('deal-status-panel');
  const primaryChart = document.getElementById('status-primary-card');
  const secondaryChart = document.getElementById('status-secondary-card');
  const tertiaryChartsLayout = document.getElementById('status-tertiary-layout');
  const tertiaryChartResizer = document.getElementById('tertiary-chart-resizer');
  const tertiaryChart = document.getElementById('status-tertiary-card');
  const tertiarySecondaryChart = document.getElementById('status-tertiary-secondary-card');
  const fullWidthCharts = document.querySelectorAll('[data-full-chart-card]');
  const fullWidthChartBodies = document.querySelectorAll('[data-full-chart-body]');
  const fullWidthChartToggles = document.querySelectorAll('[data-full-chart-toggle]');
  const fullWidthChartHandles = document.querySelectorAll('[data-full-chart-resizer]');
  if (!alertsPanel || !dealPanel) return;
  if (typeof DashboardState.layout.alertsPanelWidth === 'number' && window.innerWidth >= 1024) {
    alertsPanel.style.flex = `0 0 ${DashboardState.layout.alertsPanelWidth}px`;
    dealPanel.style.flex = '1 1 auto';
  }
  if (typeof DashboardState.layout.chartSplitWidth === 'number' && window.innerWidth >= 1024 && primaryChart && secondaryChart) {
    primaryChart.style.flex = `0 0 ${DashboardState.layout.chartSplitWidth}px`;
    secondaryChart.style.flex = '1 1 auto';
  }
  if (tertiaryChartsLayout && tertiaryChartResizer && tertiaryChart && tertiarySecondaryChart) {
    if (typeof DashboardState.layout.tertiarySplitWidth === 'number' && window.innerWidth >= 1024) {
      tertiaryChart.style.flex = `0 0 ${DashboardState.layout.tertiarySplitWidth}px`;
      tertiarySecondaryChart.style.flex = '1 1 auto';
    }
  }
  if (fullWidthCharts.length && fullWidthChartBodies.length && fullWidthChartToggles.length) {
    const fullChartMinHeight = '220px';
    const isCollapsed = Boolean(DashboardState.layout.fullChartCollapsed);
    fullWidthCharts.forEach((chart) => {
      chart.dataset.collapsed = String(isCollapsed);
    });
    fullWidthChartBodies.forEach((body) => body.classList.toggle('hidden', isCollapsed));
    fullWidthChartHandles.forEach((handle) => handle.classList.toggle('hidden', isCollapsed));
    fullWidthChartToggles.forEach((toggle) => {
      toggle.setAttribute('aria-expanded', String(!isCollapsed));
      const icon = toggle.querySelector('i[data-lucide]');
      if (icon) icon.setAttribute('data-lucide', isCollapsed ? 'chevron-down' : 'chevron-up');
    });
    if (isCollapsed) {
      fullWidthCharts.forEach((chart) => {
        chart.style.height = '';
        chart.style.minHeight = '0px';
      });
    }
    if (!isCollapsed) {
      fullWidthCharts.forEach((chart) => {
        chart.style.minHeight = fullChartMinHeight;
        if (typeof DashboardState.layout.fullChartHeight === 'number') {
          chart.style.height = `${DashboardState.layout.fullChartHeight}px`;
        }
      });
    }
  }
  if (typeof DashboardState.layout.dealPanelHeight === 'number' && window.innerWidth >= 1024) {
    const heightValue = `${DashboardState.layout.dealPanelHeight}px`;
    dealPanel.style.height = heightValue;
    alertsPanel.style.height = heightValue;
    if (primaryChart) primaryChart.style.height = heightValue;
    if (secondaryChart) secondaryChart.style.height = heightValue;
  }
};

const persistDashboardPreferences = async () => {
  const payload = buildPreferencesPayload();
  try {
    localStorage.setItem(PREFERENCES_STORAGE_KEY, JSON.stringify(payload));
    localStorage.setItem(COLUMN_STORAGE_KEY, JSON.stringify(DashboardState.table.columns));
  } catch {}

  if (!supabaseClient?.from || !DashboardState.preferences.userId) return;

  await supabaseClient
    .from(CONFIG_TABLE)
    .upsert(
      { user_id: DashboardState.preferences.userId, table_name: CONFIG_TABLE_NAME, config: payload },
      { onConflict: 'user_id, table_name' }
    );
};

const schedulePersistPreferences = () => {
  if (DashboardState.preferences.saveTimer) window.clearTimeout(DashboardState.preferences.saveTimer);
  DashboardState.preferences.saveTimer = window.setTimeout(() => {
    persistDashboardPreferences();
  }, 500);
};

const getCurrentDataset = () => getVehicles(DashboardState.vehiclesRaw);
const setVehiclesFromArray = (vehicles) => {
  DashboardState.vehiclesRaw.clear();
  vehicles.forEach((vehicle) => {
    const normalized = normalizeVehicle(vehicle);
    const key = getVehicleKey(normalized);
    if (key) DashboardState.vehiclesRaw.set(key, normalized);
  });
};

const setupFilters = ({ preserveSelections = false } = {}) => {
  const dataset = getCurrentDataset();
  const schema = DashboardState.schema;
  if (!schema.length) return;

  const dateKey = detectDateKey(schema);
  const salesChannelKey = detectSalesChannelKey(schema);
  const lastLeadKey = detectLastLeadKey(schema);
  const unitTypeKey = detectUnitTypeKey(schema);
  const vehicleStatusKey = detectVehicleStatusKey(schema);
  const categoryKeys = detectCategoryKeys(schema, dataset, [dateKey, salesChannelKey, unitTypeKey, vehicleStatusKey].filter(Boolean));

  if (!preserveSelections) {
    DashboardState.filters.categoryFilters = {};
    DashboardState.filters.columnFilters = {};
    DashboardState.filters.unitTypeSelection = [];
    DashboardState.filters.vehicleStatusSelection = [];
  }

  DashboardState.filters.dateKey = dateKey;
  DashboardState.filters.salesChannelKey = salesChannelKey;
  DashboardState.filters.lastLeadKey = lastLeadKey;
  DashboardState.filters.unitTypeKey = unitTypeKey;
  DashboardState.filters.vehicleStatusKey = vehicleStatusKey;

  const builder = document.getElementById('filter-builder');
  const categoryBlock = document.getElementById('filter-category-block');
  const dateRangeBlock = document.getElementById('filter-date-range');
  const salesChannelOptions = document.getElementById('sales-channel-options');
  const salesChannelSummary = document.getElementById('sales-channel-summary');
  const salesChannelLabel = document.getElementById('sales-channel-label');
  const lastDealSelect = document.getElementById('last-deal-select');
  const lastDealKey = lastLeadKey;

  if (!builder && !categoryBlock && !dateRangeBlock && !preserveSelections) {
    DashboardState.filters.dateRange = { start: '', end: '' };
  }

  if (categoryBlock) categoryBlock.innerHTML = '';
  if (dateRangeBlock) dateRangeBlock.innerHTML = '';

  if (categoryBlock && categoryKeys.length) {
    categoryKeys.forEach((entry) => {
      const currentValue = preserveSelections ? (DashboardState.filters.categoryFilters[entry.key] || 'all') : 'all';
      DashboardState.filters.categoryFilters[entry.key] = currentValue;
      categoryBlock.insertAdjacentHTML('beforeend', `
        <label class="col-span-12 flex flex-col gap-1 text-[10px] font-semibold uppercase tracking-[0.2em] text-slate-400 sm:col-span-6 lg:col-span-3">
          ${entry.label}
          <select data-filter-category="${entry.key}" class="h-9 rounded-xl border border-slate-800 bg-slate-950/70 px-3 text-sm font-semibold text-white">
            <option value="all">All ${entry.label}</option>
            ${entry.values.map((value) => `<option value="${value}" ${value === currentValue ? 'selected' : ''}>${value}</option>`).join('')}
          </select>
        </label>
      `);
    });
  }

  if (dateRangeBlock && dateKey) {
    const dateValues = getColumnValues(dataset, dateKey)
      .map((value) => new Date(value))
      .filter((value) => !Number.isNaN(value.getTime()))
      .map((value) => value.toISOString().slice(0, 10))
      .sort();

    if (dateValues.length && !preserveSelections) {
      const latest = new Date(dateValues[dateValues.length - 1]);
      const startDate = new Date(latest);
      startDate.setDate(startDate.getDate() - 29);
      DashboardState.filters.dateRange = { start: startDate.toISOString().slice(0, 10), end: '' };
    } else if (!dateValues.length) {
      DashboardState.filters.dateRange = { start: '', end: '' };
    } else {
      DashboardState.filters.dateRange.end = '';
    }

    dateRangeBlock.innerHTML = `
      <div class="grid gap-2 sm:grid-cols-2">
        <label class="flex items-center gap-2 text-[10px] font-semibold uppercase tracking-[0.2em] text-slate-400">
          <span>Start</span>
          <input id="filter-start-date" type="date" value="${DashboardState.filters.dateRange.start || ''}" class="h-8 flex-1 rounded-xl border border-slate-800 bg-slate-950/70 px-2 text-[11px] font-semibold text-white" />
        </label>
      </div>
    `;
  }

  if (salesChannelOptions && salesChannelSummary) {
    if (salesChannelKey) {
      const channelValues = getUniqueValues(dataset, salesChannelKey);
      const preferredValues = ['Finance-EX', 'Finance-EXT', 'Finance-ext', 'Finance-Ext'];
      const existingSelections = DashboardState.filters.salesChannels.filter((value) => channelValues.includes(value));
      if (!preserveSelections || !existingSelections.length) {
        const selections = preferredValues.filter((value) => channelValues.includes(value));
        if (selections.length) {
          DashboardState.filters.salesChannels = selections;
        } else {
          const nonFinance = channelValues.find((value) => value.toUpperCase() !== 'FINANCE');
          DashboardState.filters.salesChannels = nonFinance ? [nonFinance] : [...channelValues];
        }
      } else {
        DashboardState.filters.salesChannels = existingSelections;
      }
      salesChannelOptions.innerHTML = (channelValues.length ? channelValues : [''])
        .map((value) => `
          <label class="flex items-center gap-2 text-xs">
            <input type="checkbox" value="${value}" class="h-3.5 w-3.5 rounded border-slate-600 bg-slate-950" ${DashboardState.filters.salesChannels.includes(value) ? 'checked' : ''} />
            <span>${value || 'Unknown'}</span>
          </label>
        `).join('');
      const count = DashboardState.filters.salesChannels.filter(Boolean).length;
      salesChannelSummary.textContent = count ? String(count) : '0';
      if (salesChannelLabel) {
        salesChannelLabel.textContent = count === 1 ? DashboardState.filters.salesChannels[0] : 'Sales Channel';
      }
    } else {
      DashboardState.filters.salesChannels = [];
      salesChannelOptions.innerHTML = '<p class="text-[10px] text-slate-400">No channels available.</p>';
      salesChannelSummary.textContent = '0';
      if (salesChannelLabel) salesChannelLabel.textContent = 'Sales Channel';
    }
  }

  if (lastDealSelect) {
    if (lastDealKey) {
      const existingEntry = DashboardState.filters.columnFilters[lastDealKey];
      if (!preserveSelections) {
        DashboardState.filters.lastLeadSelection = true;
      }
      if (preserveSelections && existingEntry?.select && existingEntry.select !== 'all') {
        DashboardState.filters.lastLeadSelection = normalizeBoolean(existingEntry.select);
      } else if (preserveSelections && existingEntry?.select === 'all') {
        DashboardState.filters.lastLeadSelection = 'all';
      }
      DashboardState.filters.lastLeadFilterActive = DashboardState.filters.lastLeadSelection !== 'all';
      DashboardState.filters.columnFilters[lastDealKey] = {
        select: DashboardState.filters.lastLeadSelection === 'all'
          ? 'all'
          : (DashboardState.filters.lastLeadSelection ? 'true' : 'false'),
        search: '',
      };
    } else if (!preserveSelections) {
      DashboardState.filters.lastLeadFilterActive = false;
    }
    lastDealSelect.disabled = !lastDealKey;
    lastDealSelect.classList.toggle('opacity-50', !lastDealKey);
    lastDealSelect.classList.toggle('cursor-not-allowed', !lastDealKey);
    lastDealSelect.value = DashboardState.filters.lastLeadSelection === 'all'
      ? 'all'
      : (DashboardState.filters.lastLeadSelection ? 'true' : 'false');
  }

  categoryBlock?.classList.toggle('hidden', categoryKeys.length === 0);
  dateRangeBlock?.classList.toggle('hidden', !dateKey);
  builder?.classList.toggle('opacity-50', DashboardState.ui.isLoading);
};

const applyFilters = ({ ignoreChartFilter = false, ignoreChartId = null } = {}) => {
  const { filters } = DashboardState;

  return getCurrentDataset().filter((item) => {
    if (filters.lastLeadFilterActive && filters.lastLeadSelection !== 'all' && item.isLastDeal !== filters.lastLeadSelection) return false;
    const dateValue = filters.dateKey ? item[filters.dateKey] : null;
    const parsedDate = dateValue ? new Date(dateValue) : null;
    const dateString = parsedDate && !Number.isNaN(parsedDate.getTime()) ? parsedDate.toISOString().slice(0, 10) : '';
    const inDateRange = !filters.dateKey || (
      (!filters.dateRange.start || dateString >= filters.dateRange.start) &&
      (!filters.dateRange.end || dateString <= filters.dateRange.end)
    );

    if ((filters.dateRange.start || filters.dateRange.end) && !dateString) return false;

    const categoryMatch = Object.entries(filters.categoryFilters || {}).every(([key, value]) => {
      if (value === 'all') return true;
      return String(item[key] ?? '') === value;
    });

    const salesChannelMatch = !filters.salesChannelKey || !filters.salesChannels.length
      || filters.salesChannels.includes(String(item[filters.salesChannelKey] ?? ''));

    const unitTypeSelections = Array.isArray(filters.unitTypeSelection) ? filters.unitTypeSelection : [];
    const unitTypeMatch = !filters.unitTypeKey
      || !unitTypeSelections.length
      || unitTypeSelections.includes(String(item[filters.unitTypeKey] ?? ''));

    const vehicleStatusSelections = Array.isArray(filters.vehicleStatusSelection)
      ? filters.vehicleStatusSelection
      : [];
    const vehicleStatusMatch = !filters.vehicleStatusKey
      || !vehicleStatusSelections.length
      || vehicleStatusSelections.includes(String(item[filters.vehicleStatusKey] ?? ''));
    const locationValue = String(item['Physical Location'] ?? '').trim().toLowerCase();
    const isCopartLike = locationValue.includes('co part') || locationValue.includes('copart');
    const locationMatch = !filters.locationFocusActive
      || (
        (LOCATION_FILTER_SET.has(locationValue) || isCopartLike)
        && STATUS_FILTER_SET.has(String(item['Vehicle Status'] ?? '').trim().toLowerCase())
      );

    const columnMatch = Object.entries(filters.columnFilters || {}).every(([key, entry]) => {
      if (!entry) return true;
      const value = item[key];
      const valueString = String(value ?? '');
      if (filters.lastLeadKey && key === filters.lastLeadKey && filters.lastLeadFilterActive) {
        if (entry.select && entry.select !== 'all') {
          const desired = normalizeBoolean(entry.select);
          if (normalizeBoolean(value) !== desired) return false;
        }
      } else if (entry.select && entry.select !== 'all' && valueString !== entry.select) {
        return false;
      }
      if (entry.search && !valueString.toLowerCase().includes(entry.search)) return false;
      return true;
    });

    const activeChartFilters = !ignoreChartFilter && filters.chartFilters
      ? Object.entries(filters.chartFilters)
        .filter(([chartId]) => !ignoreChartId || chartId !== ignoreChartId)
        .map(([, filter]) => filter)
      : [];
    const chartMatch = ignoreChartFilter || !activeChartFilters.length
      || activeChartFilters.every((filter) => {
        const values = Array.isArray(filter.values) ? filter.values : [];
        if (!values.length) return true;
        return values.includes(getSegmentLabel(item[filter.key], filter.key));
      });

    return inDateRange && categoryMatch && salesChannelMatch && unitTypeMatch && vehicleStatusMatch && locationMatch && columnMatch && chartMatch;
  });
};

const getSegmentLabel = (value, segmentKey = '') => {
  const raw = String(value ?? '').trim();
  if (!raw) return 'Unassigned';
  const invPrepKey = detectInvPrepStatusKey(DashboardState.schema);
  if (segmentKey && invPrepKey && segmentKey === invPrepKey) {
    return formatInvPrepStatusLabel(raw) || 'Unassigned';
  }
  return raw;
};
const getSegmentOptions = () => {
  const options = [{ key: 'dealStatus', label: 'Deal Status' }];
  const seen = new Set(options.map((opt) => opt.key));
  DashboardState.schema
    .filter((col) => ['string', 'number', 'boolean'].includes(col.type))
    .forEach((col) => {
      if (seen.has(col.key)) return;
      seen.add(col.key);
      options.push({ key: col.key, label: col.label || formatColumnLabel(col.key) });
    });
  return options;
};

const ensureChartVisibilityState = (chartId, segmentKey) => {
  if (!DashboardState.chartVisibility[chartId]) DashboardState.chartVisibility[chartId] = {};
  if (!Array.isArray(DashboardState.chartVisibility[chartId][segmentKey])) {
    DashboardState.chartVisibility[chartId][segmentKey] = [];
  }
  return DashboardState.chartVisibility[chartId][segmentKey];
};

const setChartHiddenValues = (chartId, segmentKey, values) => {
  if (!DashboardState.chartVisibility[chartId]) DashboardState.chartVisibility[chartId] = {};
  DashboardState.chartVisibility[chartId][segmentKey] = values;
  const activeFilters = DashboardState.filters.chartFilters || {};
  Object.entries(activeFilters).forEach(([filterChartId, filter]) => {
    if (filter.key !== segmentKey) return;
    const currentValues = Array.isArray(filter.values) ? filter.values : [];
    const nextValues = currentValues.filter((value) => !values.includes(value));
    if (!nextValues.length) {
      delete DashboardState.filters.chartFilters[filterChartId];
    } else {
      DashboardState.filters.chartFilters[filterChartId] = { key: segmentKey, values: nextValues };
    }
  });
};

const initializeTablePreferences = () => {
  let saved = DashboardState.preferences.config?.table?.columns || {};
  if (!Object.keys(saved).length) {
    try {
      const stored = localStorage.getItem(COLUMN_STORAGE_KEY);
      saved = stored ? JSON.parse(stored) : {};
    } catch {
      saved = {};
    }
  }
  const defaults = DashboardState.schema.reduce((acc, c) => ((acc[c.key] = true), acc), {});
  DashboardState.table.columns = DashboardState.schema.reduce((acc, column) => {
    acc[column.key] = saved[column.key] ?? defaults[column.key] ?? true;
    return acc;
  }, {});
  if (!DashboardState.table.columnOrder.length) {
    DashboardState.table.columnOrder = DashboardState.schema.map((c) => c.key);
  } else {
    const known = new Set(DashboardState.schema.map((c) => c.key));
    DashboardState.table.columnOrder = DashboardState.table.columnOrder.filter((key) => known.has(key));
    DashboardState.schema.forEach((c) => {
      if (!DashboardState.table.columnOrder.includes(c.key)) {
        DashboardState.table.columnOrder.push(c.key);
      }
    });
  }
  if (!DashboardState.schema.some((c) => c.key === DashboardState.table.sort.key)) {
    DashboardState.table.sort = { key: DashboardState.schema[0]?.key || '', direction: 'asc' };
  }
  renderColumnChooser();
};

let renderDashboard = () => {};
let renderColumnChooser = () => {};
let openDrawer = () => {};
let closeDrawer = () => {};
let updateLocationFilterToggle = () => {};

const exportCsv = () => {
  const visibleColumns = DashboardState.schema.filter((c) => DashboardState.table.columns[c.key]);
  if (!visibleColumns.length) return;
  const rows = DashboardState.derived.filtered.map((item) =>
    visibleColumns.map((c) => {
      let value = item[c.key];
      if (c.type === 'boolean') value = value ? 'Yes' : 'No';
      if (c.type === 'date') value = value ? formatDate(value) : '';
      return `"${String(value ?? '').replace(/\"/g, '""')}"`;
    })
  );
  const header = visibleColumns.map((c) => `"${c.label}"`).join(',');
  const csv = [header, ...rows.map((r) => r.join(','))].join('\n');
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = 'inventory-export.csv';
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
};

let syncTopScrollbar = () => {};

const bindFilterEvents = () => {
  const builder = document.getElementById('filter-builder');
  const resetFiltersButton = document.getElementById('reset-filters');
  const tablePrev = document.getElementById('table-prev');
  const tableNext = document.getElementById('table-next');
  const perPageSelect = document.getElementById('table-per-page');
  const goToInput = document.getElementById('table-go-to');
  const tableHead = document.getElementById('inventory-table-head');
  const inventoryTable = document.getElementById('inventory-table');
  const tableScroll = document.getElementById('inventory-table-scroll');
  const tableScrollTop = document.getElementById('inventory-table-scroll-top');
  const tableScrollTopInner = document.getElementById('inventory-table-scroll-top-inner');
  const salesChannelToggle = document.getElementById('sales-channel-toggle');
  const salesChannelPanel = document.getElementById('sales-channel-panel');
  const salesChannelOptions = document.getElementById('sales-channel-options');
  const salesChannelSummary = document.getElementById('sales-channel-summary');
  const salesChannelLabel = document.getElementById('sales-channel-label');
  const unitTypeFilters = document.getElementById('unit-type-filters');
  const unitTypeToggle = document.getElementById('unit-type-toggle');
  const unitTypePanel = document.getElementById('unit-type-panel');
  const unitTypeOptions = document.getElementById('unit-type-options');
  const unitTypeSummary = document.getElementById('unit-type-summary');
  const unitTypeLabel = document.getElementById('unit-type-label');
  const vehicleStatusFilters = document.getElementById('vehicle-status-filters');
  const vehicleStatusToggle = document.getElementById('vehicle-status-toggle');
  const vehicleStatusPanel = document.getElementById('vehicle-status-panel');
  const vehicleStatusOptions = document.getElementById('vehicle-status-options');
  const vehicleStatusSummary = document.getElementById('vehicle-status-summary');
  const vehicleStatusLabel = document.getElementById('vehicle-status-label');
  const lastDealSelect = document.getElementById('last-deal-select');
  const alertsToggle = document.getElementById('alerts-toggle');
  const alertsList = document.getElementById('alerts-list');
  const alertsBadges = document.getElementById('alerts-badges');
  const alertsPanel = document.getElementById('alerts-panel');
  const alertsDealsBadge = document.getElementById('alerts-deals-count');
  const alertsDealsModal = document.getElementById('alerts-deals-modal');
  const alertsDealsModalClose = document.getElementById('alerts-deals-modal-close');
  const alertsDealsRowButton = document.getElementById('alerts-deals-row-button');
  const alertsDealsFilters = document.getElementById('alerts-deals-filters');
  const alertsDealsList = document.getElementById('alerts-deals-list');
  const alertsDealsColumnsToggle = document.getElementById('alerts-deals-columns-toggle');
  const alertsDealsColumnsPanel = document.getElementById('alerts-deals-columns-panel');
  const alertsDealsColumnsList = document.getElementById('alerts-deals-columns-list');
  const alertsDealsColumnHeaders = document.getElementById('alerts-deals-column-headers');
  const drawerClose = document.getElementById('drawer-close');
  const columnChooser = document.getElementById('column-chooser');
  const columnChooserToggle = document.getElementById('column-chooser-toggle');
  const columnChooserOptions = document.getElementById('column-chooser-options');
  const exportCsvButton = document.getElementById('export-csv');
  const locationFilterToggle = document.getElementById('location-filter-toggle');
  const headerActions = document.getElementById('inventory-header-actions');
  const connectionStatusWrapper = document.getElementById('connection-status-wrapper');
  const segmentSelects = document.querySelectorAll('[data-segment-select]');
  const chartContainers = document.querySelectorAll('[data-bar-chart]');
  const segmentFilterToggles = document.querySelectorAll('[data-segment-filter-toggle]');
  const segmentFilterPanels = document.querySelectorAll('[data-segment-filter-panel]');
  const fullChartToggles = document.querySelectorAll('[data-full-chart-toggle]');
  const fullChartBodies = document.querySelectorAll('[data-full-chart-body]');
  const fullChartCards = document.querySelectorAll('[data-full-chart-card]');
  const fullChartHandles = document.querySelectorAll('[data-full-chart-resizer]');

  const addListener = (element, event, handler, options) => {
    element?.addEventListener(event, handler, options);
  };

  const resetPagination = () => (DashboardState.table.page = 1);
  let activeDragKey = null;
  let resizing = null;
  if (locationFilterToggle && headerActions && connectionStatusWrapper && !headerActions.contains(locationFilterToggle)) {
    connectionStatusWrapper.after(locationFilterToggle);
  }
  updateLocationFilterToggle = () => {
    if (!locationFilterToggle) return;
    const isActive = DashboardState.filters.locationFocusActive;
    locationFilterToggle.setAttribute('aria-pressed', String(isActive));
    locationFilterToggle.classList.toggle('border-blue-400', isActive);
    locationFilterToggle.classList.toggle('text-white', isActive);
    locationFilterToggle.classList.toggle('bg-blue-500/10', isActive);
  };

  if (tableScroll && tableScrollTop && tableScrollTopInner) {
    const tableElement = tableScroll.querySelector('table');
    const updateTopWidth = () => {
      if (!tableElement) return;
      tableScrollTopInner.style.width = `${tableElement.scrollWidth}px`;
    };

    const syncScroll = (source, target) => {
      if (target.scrollLeft === source.scrollLeft) return;
      target.scrollLeft = source.scrollLeft;
    };

    syncTopScrollbar = () => {
      updateTopWidth();
    };

    updateTopWidth();
    if (tableElement) {
      const resizeObserver = new ResizeObserver(updateTopWidth);
      resizeObserver.observe(tableElement);
      resizeObserver.observe(tableScroll);
    }
    addListener(tableScroll, 'scroll', () => syncScroll(tableScroll, tableScrollTop));
    addListener(tableScrollTop, 'scroll', () => syncScroll(tableScrollTop, tableScroll));
  }

  if (alertsToggle && alertsList && alertsBadges && alertsPanel) {
    addListener(alertsToggle, 'click', () => {
      const isCollapsed = alertsPanel.dataset.collapsed === 'true';
      const nextCollapsed = !isCollapsed;
      alertsPanel.dataset.collapsed = String(nextCollapsed);
      alertsList.classList.toggle('hidden', nextCollapsed);
      alertsBadges.classList.toggle('hidden', !nextCollapsed);
      alertsToggle.setAttribute('aria-expanded', String(!nextCollapsed));
      const icon = alertsToggle.querySelector('[data-lucide]');
      updateLucideIcon(icon, nextCollapsed ? 'chevron-down' : 'chevron-up');
    });
  }

  if ((alertsDealsBadge || alertsDealsRowButton) && alertsDealsModal) {
    const openModal = () => {
      alertsDealsModal.classList.remove('hidden');
      alertsDealsModal.classList.add('flex');
      alertsDealsModal.setAttribute('aria-hidden', 'false');
    };
    const closeModal = () => {
      alertsDealsModal.classList.add('hidden');
      alertsDealsModal.classList.remove('flex');
      alertsDealsModal.setAttribute('aria-hidden', 'true');
    };

    addListener(alertsDealsBadge, 'click', openModal);
    addListener(alertsDealsRowButton, 'click', openModal);
    addListener(alertsDealsModalClose, 'click', closeModal);
    addListener(alertsDealsModal, 'click', (event) => {
      if (event.target === alertsDealsModal) closeModal();
    });
  }

  const setActiveFilter = (value) => {
    alertsDealsFilter = value;
    if (alertsDealsFilters) {
      alertsDealsFilters.querySelectorAll('[data-alerts-deals-filter]').forEach((button) => {
        const isActive = button.dataset.alertsDealsFilter === value;
        button.classList.toggle('border-blue-400', isActive);
        button.classList.toggle('text-white', isActive);
      });
    }
    updateAlertsDealsList();
  };

  if (alertsDealsFilters) {
    addListener(alertsDealsFilters, 'click', (event) => {
      const button = event.target.closest('[data-alerts-deals-filter]');
      if (!button) return;
      setActiveFilter(button.dataset.alertsDealsFilter);
    });
  }

  setActiveFilter(alertsDealsFilter);

  if (alertsDealsColumnsToggle && alertsDealsColumnsPanel) {
    addListener(alertsDealsColumnsToggle, 'click', () => {
      const isHidden = alertsDealsColumnsPanel.classList.toggle('hidden');
      alertsDealsColumnsToggle.setAttribute('aria-expanded', String(!isHidden));
    });
  }

  if (alertsDealsColumnsList) {
    addListener(alertsDealsColumnsList, 'change', (event) => {
      const checkbox = event.target.closest('[data-alerts-column-key]');
      if (!checkbox) return;
      const key = checkbox.dataset.alertsColumnKey;
      if (!key) return;
      if (checkbox.checked) {
        if (!alertsDealsColumns.includes(key)) alertsDealsColumns.push(key);
      } else {
        alertsDealsColumns = alertsDealsColumns.filter((value) => value !== key);
      }
      localStorage.setItem(ALERTS_COLUMNS_STORAGE_KEY, JSON.stringify(alertsDealsColumns));
      renderAlertsDealsColumns();
      updateAlertsDealsList();
    });

    addListener(alertsDealsColumnsList, 'input', (event) => {
      const input = event.target.closest('[data-alerts-column-label]');
      if (!input) return;
      const key = input.dataset.alertsColumnLabel;
      if (!key) return;
      alertsDealsColumnLabels[key] = input.value.trim() || formatColumnLabel(key);
      localStorage.setItem(ALERTS_COLUMNS_LABELS_KEY, JSON.stringify(alertsDealsColumnLabels));
      renderAlertsDealsColumnHeaders();
      updateAlertsDealsList();
    });
  }

  if (alertsDealsColumnHeaders) {
    addListener(alertsDealsColumnHeaders, 'click', (event) => {
      const button = event.target.closest('[data-alerts-deals-sort-key]');
      if (!button) return;
      const key = button.dataset.alertsDealsSortKey;
      if (!key) return;
      if (alertsDealsSortKey === key) {
        alertsDealsSortDirection = alertsDealsSortDirection === 'asc' ? 'desc' : 'asc';
      } else {
        alertsDealsSortKey = key;
        alertsDealsSortDirection = 'asc';
      }
      renderAlertsDealsColumnHeaders();
      updateAlertsDealsList();
    });
  }

  if (alertsDealsList) {
    addListener(alertsDealsList, 'click', (event) => {
      const target = event.target.closest('[data-alerts-google-button]');
      if (!target) return;
      const vinKey = target.dataset.alertsGoogleTarget;
      if (!vinKey) return;
      const label = alertsDealsList.querySelector(`[data-alerts-google-last="${vinKey}"]`);
      if (!label) return;
      const clickedAt = new Date();
      const timestamp = formatAlertsTimestamp(clickedAt);
      label.textContent = timestamp;
      alertsDealsLastClickByVin[vinKey] = timestamp;
      localStorage.setItem(`${ALERTS_STORAGE_PREFIX}:${vinKey}:lastClick`, timestamp);
      insertAlertsLastClickHistory(vinKey, clickedAt.toISOString());
      schedulePersistPreferences();
    });

    addListener(alertsDealsList, 'input', (event) => {
      const input = event.target.closest('[data-alerts-notes-key]');
      if (!input) return;
      const key = input.dataset.alertsNotesKey;
      if (!key) return;
      localStorage.setItem(`${key}:note`, input.value);
    });
  }

  if (builder) {
    addListener(builder, 'change', (event) => {
      const target = event.target;
      if (target.matches('[data-filter-category]')) {
        const key = target.dataset.filterCategory;
        DashboardState.filters.categoryFilters[key] = target.value;
        resetPagination();
        renderDashboard();
        schedulePersistPreferences();
      }
      if (target.matches('#filter-start-date')) {
        DashboardState.filters.dateRange.start = document.getElementById('filter-start-date')?.value || '';
        DashboardState.filters.dateRange.end = '';
        resetPagination();
        renderDashboard();
        schedulePersistPreferences();
      }
    });
  }

  if (unitTypeFilters) {
    addListener(unitTypeFilters, 'click', (event) => {
      const toggle = event.target.closest('#unit-type-toggle');
      if (!toggle) return;
      const panel = unitTypeFilters.querySelector('#unit-type-panel');
      if (!panel) return;
      const isHidden = panel.classList.toggle('hidden');
      toggle.setAttribute('aria-expanded', String(!isHidden));
    });

    addListener(unitTypeFilters, 'change', (event) => {
      const input = event.target;
      if (!input.matches('input[type="checkbox"]')) return;
      const options = unitTypeFilters.querySelector('#unit-type-options');
      const summary = unitTypeFilters.querySelector('#unit-type-summary');
      const label = unitTypeFilters.querySelector('#unit-type-label');
      const checked = Array.from(options?.querySelectorAll('input[type="checkbox"]') || [])
        .filter((item) => item.checked)
        .map((item) => item.value);
      DashboardState.filters.unitTypeSelection = checked;
      if (summary) summary.textContent = String(checked.length);
      if (label) {
        label.textContent = checked.length === 1 ? checked[0] : 'Unit Type';
      }
      resetPagination();
      renderDashboard();
      schedulePersistPreferences();
    });
  }

  if (vehicleStatusFilters) {
    addListener(vehicleStatusFilters, 'click', (event) => {
      const toggle = event.target.closest('#vehicle-status-toggle');
      if (!toggle) return;
      const panel = vehicleStatusFilters.querySelector('#vehicle-status-panel');
      if (!panel) return;
      const isHidden = panel.classList.toggle('hidden');
      toggle.setAttribute('aria-expanded', String(!isHidden));
    });

    addListener(vehicleStatusFilters, 'change', (event) => {
      const input = event.target;
      if (!input.matches('input[type="checkbox"]')) return;
      const options = vehicleStatusFilters.querySelector('#vehicle-status-options');
      const summary = vehicleStatusFilters.querySelector('#vehicle-status-summary');
      const label = vehicleStatusFilters.querySelector('#vehicle-status-label');
      const checked = Array.from(options?.querySelectorAll('input[type="checkbox"]') || [])
        .filter((item) => item.checked)
        .map((item) => item.value);
      DashboardState.filters.vehicleStatusSelection = checked;
      if (summary) summary.textContent = String(checked.length);
      if (label) {
        label.textContent = checked.length === 1 ? checked[0] : 'Vehicle Status';
      }
      resetPagination();
      renderDashboard();
      schedulePersistPreferences();
    });
  }

  addListener(resetFiltersButton, 'click', () => {
    setupFilters();
    DashboardState.filters.locationFocusActive = false;
    updateLocationFilterToggle();
    resetPagination();
    renderDashboard();
    schedulePersistPreferences();
  });

  addListener(tablePrev, 'click', () => { DashboardState.table.page = Math.max(1, DashboardState.table.page - 1); renderDashboard(); });
  addListener(tableNext, 'click', () => { DashboardState.table.page += 1; renderDashboard(); });

  addListener(perPageSelect, 'change', () => {
    DashboardState.table.perPage = Number(perPageSelect.value);
    DashboardState.table.page = 1;
    renderDashboard();
    schedulePersistPreferences();
  });

  addListener(goToInput, 'change', () => {
    const totalRows = DashboardState.derived.filtered.length;
    const totalPages = Math.max(1, Math.ceil(totalRows / DashboardState.table.perPage));
    const desired = Math.min(Math.max(1, Number(goToInput.value) || 1), totalPages);
    DashboardState.table.page = desired;
    renderDashboard();
  });

  addListener(columnChooserOptions, 'click', (event) => {
    const editButton = event.target.closest('[data-column-edit]');
    if (!editButton) return;
    const key = editButton.dataset.columnEdit;
    if (!key) return;
    const currentLabel = getColumnLabel(key);
    const nextLabel = window.prompt('Edit column name', currentLabel);
    if (nextLabel === null) return;
    setColumnLabelOverride(key, nextLabel);
    renderDashboard();
    schedulePersistPreferences();
  });

  addListener(tableHead, 'click', (event) => {
    const button = event.target.closest('[data-sort]');
    if (!button) return;
    const key = button.dataset.sort;
    if (DashboardState.table.sort.key === key) DashboardState.table.sort.direction = DashboardState.table.sort.direction === 'asc' ? 'desc' : 'asc';
    else { DashboardState.table.sort.key = key; DashboardState.table.sort.direction = 'asc'; }
    renderDashboard();
    schedulePersistPreferences();
  });

  const closeColumnFilterPanels = (exceptKey = null) => {
    tableHead?.querySelectorAll('[data-column-filter-panel]').forEach((panel) => {
      if (panel.dataset.columnFilterPanel === exceptKey) return;
      panel.classList.add('hidden');
    });
  };

  addListener(tableHead, 'click', (event) => {
    const toggle = event.target.closest('[data-column-filter-toggle]');
    if (!toggle) return;
    event.preventDefault();
    event.stopPropagation();
    const key = toggle.dataset.columnFilterToggle;
    const panel = tableHead.querySelector(`[data-column-filter-panel="${key}"]`);
    if (!panel) return;
    const isHidden = panel.classList.toggle('hidden');
    closeColumnFilterPanels(isHidden ? null : key);
  });

  addListener(tableHead, 'dragstart', (event) => {
    if (resizing) return;
    const th = event.target.closest('th[data-col-key]');
    if (!th || event.target.closest('[data-resize-handle]')) return;
    activeDragKey = th.dataset.colKey;
    event.dataTransfer.effectAllowed = 'move';
    event.dataTransfer.setData('text/plain', activeDragKey);
  });

  addListener(tableHead, 'dragover', (event) => {
    if (!activeDragKey) return;
    const th = event.target.closest('th[data-col-key]');
    if (!th) return;
    event.preventDefault();
  });

  addListener(tableHead, 'drop', (event) => {
    if (!activeDragKey) return;
    const th = event.target.closest('th[data-col-key]');
    if (!th) return;
    event.preventDefault();
    const targetKey = th.dataset.colKey;
    if (targetKey && targetKey !== activeDragKey) {
      const order = DashboardState.table.columnOrder.slice();
      const fromIndex = order.indexOf(activeDragKey);
      const toIndex = order.indexOf(targetKey);
      if (fromIndex !== -1 && toIndex !== -1) {
        order.splice(fromIndex, 1);
        order.splice(toIndex, 0, activeDragKey);
        DashboardState.table.columnOrder = order;
        renderDashboard();
        schedulePersistPreferences();
      }
    }
    activeDragKey = null;
  });

  addListener(tableHead, 'dragend', () => {
    activeDragKey = null;
  });

  addListener(inventoryTable, 'click', (event) => {
    const row = event.target.closest('[data-row-key]');
    if (!row) return;
    const record = DashboardState.vehiclesRaw.get(row.dataset.rowKey);
    if (record) openDrawer(record);
  });

  addListener(tableHead, 'change', (event) => {
    const target = event.target;
    if (!target.matches('[data-column-filter]')) return;
    const key = target.dataset.columnFilter;
    const entry = DashboardState.filters.columnFilters[key] || { select: 'all', search: '' };
    entry.select = target.value;
    DashboardState.filters.columnFilters[key] = entry;
    if (DashboardState.filters.lastLeadKey && key === DashboardState.filters.lastLeadKey) {
      DashboardState.filters.lastLeadSelection = target.value === 'all' ? 'all' : normalizeBoolean(target.value);
      DashboardState.filters.lastLeadFilterActive = target.value !== 'all';
      const lastDealSelect = document.getElementById('last-deal-select');
      if (lastDealSelect) {
        lastDealSelect.value = DashboardState.filters.lastLeadSelection === 'all'
          ? 'all'
          : (DashboardState.filters.lastLeadSelection ? 'true' : 'false');
      }
      setupFilters({ preserveSelections: true });
    }
    resetPagination();
    renderDashboard();
    schedulePersistPreferences();
  });

  addListener(tableHead, 'keydown', (event) => {
    const target = event.target;
    if (!target.matches('[data-column-search]')) return;
    if (event.key !== 'Enter') return;
    event.preventDefault();
    const key = target.dataset.columnSearch;
    const entry = DashboardState.filters.columnFilters[key] || { select: 'all', search: '' };
    entry.search = target.value.trim().toLowerCase();
    DashboardState.filters.columnFilters[key] = entry;
    resetPagination();
    renderDashboard();
    schedulePersistPreferences();
  });

  const onResizeMove = (event) => {
    if (!resizing) return;
    const delta = event.clientX - resizing.startX;
    const nextWidth = Math.max(36, resizing.startWidth + delta);
    resizing.currentWidth = nextWidth;
    if (resizing.col) resizing.col.style.width = `${nextWidth}px`;
  };

  const onResizeEnd = () => {
    if (!resizing) return;
    if (resizing.handle?.hasPointerCapture(resizing.pointerId)) {
      resizing.handle.releasePointerCapture(resizing.pointerId);
    }
    if (typeof resizing.currentWidth === 'number') {
      DashboardState.table.columnWidths[resizing.key] = resizing.currentWidth;
    }
    resizing = null;
    document.body.classList.remove('select-none');
    window.removeEventListener('pointermove', onResizeMove);
    schedulePersistPreferences();
    renderDashboard();
    syncTopScrollbar();
  };

  addListener(tableHead, 'pointerdown', (event) => {
    const explicitHandle = event.target.closest('[data-resize-handle]');
    const th = event.target.closest('th[data-col-key]');
    if (!explicitHandle && !th) return;
    if (!explicitHandle && th) {
      const bounds = th.getBoundingClientRect();
      const nearEdge = event.clientX >= bounds.right - 8;
      if (!nearEdge) return;
    }
    event.preventDefault();
    event.stopPropagation();
    const key = explicitHandle?.dataset.resizeHandle || th?.dataset.colKey;
    if (!key || !th) return;
    const colgroup = document.getElementById('inventory-table-cols');
    const col = colgroup?.querySelector(`col[data-col-key="${key}"]`);
    if (!col) return;
    const captureTarget = explicitHandle || th;
    captureTarget.setPointerCapture(event.pointerId);
    resizing = {
      key,
      startX: event.clientX,
      startWidth: col.getBoundingClientRect().width,
      currentWidth: col.getBoundingClientRect().width,
      handle: captureTarget,
      pointerId: event.pointerId,
      col,
    };
    document.body.classList.add('select-none');
    window.addEventListener('pointermove', onResizeMove);
    window.addEventListener('pointerup', onResizeEnd, { once: true });
  });

  addListener(drawerClose, 'click', closeDrawer);

  addListener(columnChooserToggle, 'click', () => {
    if (!columnChooser) return;
    const isHidden = columnChooser.classList.toggle('hidden');
    columnChooserToggle.setAttribute('aria-expanded', String(!isHidden));
  });

  document.addEventListener('click', (event) => {
    if (!columnChooser || !columnChooserToggle) return;
    if (columnChooser.classList.contains('hidden')) return;
    if (columnChooser.contains(event.target) || columnChooserToggle.contains(event.target)) return;
    columnChooser.classList.add('hidden');
  });

  document.addEventListener('click', (event) => {
    if (!tableHead) return;
    if (tableHead.contains(event.target)) return;
    closeColumnFilterPanels();
  });

  addListener(columnChooserOptions, 'change', (event) => {
    const input = event.target;
    if (!input.matches('input[type="checkbox"]')) return;
    DashboardState.table.columns[input.value] = input.checked;
    renderDashboard();
    schedulePersistPreferences();
  });

  addListener(exportCsvButton, 'click', exportCsv);
  if (locationFilterToggle) {
    updateLocationFilterToggle();
    addListener(locationFilterToggle, 'click', () => {
      DashboardState.filters.locationFocusActive = !DashboardState.filters.locationFocusActive;
      updateLocationFilterToggle();
      resetPagination();
      renderDashboard();
      schedulePersistPreferences();
    });
  }
  document.getElementById('erase-filters')?.addEventListener('click', () => {
    setupFilters();
    DashboardState.filters.chartFilters = {};
    DashboardState.filters.locationFocusActive = false;
    updateLocationFilterToggle();
    resetPagination();
    renderDashboard();
    schedulePersistPreferences();
  });

  const closeSegmentFilterPanels = (exceptChartId = null) => {
    segmentFilterPanels.forEach((panel) => {
      if (panel.dataset.chartId === exceptChartId) return;
      panel.classList.add('hidden');
      const toggle = document.querySelector(`[data-segment-filter-toggle][data-chart-id="${panel.dataset.chartId}"]`);
      toggle?.setAttribute('aria-expanded', 'false');
    });
  };

  segmentFilterToggles.forEach((toggle) => {
    toggle.addEventListener('click', (event) => {
      event.stopPropagation();
      const chartId = toggle.dataset.chartId || 'default';
      const panel = document.querySelector(`[data-segment-filter-panel][data-chart-id="${chartId}"]`);
      if (!panel) return;
      const isHidden = panel.classList.toggle('hidden');
      toggle.setAttribute('aria-expanded', String(!isHidden));
      closeSegmentFilterPanels(isHidden ? null : chartId);
    });
  });

  document.addEventListener('click', (event) => {
    if (event.target.closest('[data-segment-filter-panel]') || event.target.closest('[data-segment-filter-toggle]')) return;
    closeSegmentFilterPanels();
  });

  document.addEventListener('change', (event) => {
    const input = event.target;
    if (!input.matches('[data-segment-field-checkbox]')) return;
    const chartId = input.dataset.chartId || 'default';
    const segmentKey = input.dataset.segmentKey || DEFAULT_SEGMENT_KEY;
    const hiddenValues = ensureChartVisibilityState(chartId, segmentKey).slice();
    const value = input.value;
    if (input.checked) {
      const nextHidden = hiddenValues.filter((item) => item !== value);
      setChartHiddenValues(chartId, segmentKey, nextHidden);
    } else if (!hiddenValues.includes(value)) {
      hiddenValues.push(value);
      setChartHiddenValues(chartId, segmentKey, hiddenValues);
    }
    renderDashboard();
    schedulePersistPreferences();
  });

  document.addEventListener('click', (event) => {
    const selectAll = event.target.closest('[data-segment-select-all]');
    const clearAll = event.target.closest('[data-segment-clear-all]');
    if (!selectAll && !clearAll) return;
    const chartId = (selectAll || clearAll).dataset.chartId || 'default';
    const panel = document.querySelector(`[data-segment-filter-panel][data-chart-id="${chartId}"]`);
    const segmentKey = panel?.dataset.segmentKey || DEFAULT_SEGMENT_KEY;
    const values = DashboardState.chartVisibilityOptions?.[chartId]?.[segmentKey] || [];
    if (selectAll) {
      setChartHiddenValues(chartId, segmentKey, []);
    } else if (clearAll) {
      setChartHiddenValues(chartId, segmentKey, values.slice());
    }
    renderDashboard();
    schedulePersistPreferences();
  });

  if (salesChannelToggle && salesChannelPanel) {
    addListener(salesChannelToggle, 'click', () => {
      const isHidden = salesChannelPanel.classList.toggle('hidden');
      salesChannelToggle.setAttribute('aria-expanded', String(!isHidden));
    });
  }

  if (unitTypeToggle && unitTypePanel) {
    addListener(unitTypeToggle, 'click', () => {
      const isHidden = unitTypePanel.classList.toggle('hidden');
      unitTypeToggle.setAttribute('aria-expanded', String(!isHidden));
    });
  }

  if (vehicleStatusToggle && vehicleStatusPanel) {
    addListener(vehicleStatusToggle, 'click', () => {
      const isHidden = vehicleStatusPanel.classList.toggle('hidden');
      vehicleStatusToggle.setAttribute('aria-expanded', String(!isHidden));
    });
  }

  if (fullChartToggles.length && fullChartBodies.length && fullChartCards.length) {
    const setFullChartsCollapsed = (nextCollapsed) => {
      const fullChartMinHeight = '220px';
      fullChartCards.forEach((card) => {
        card.dataset.collapsed = String(nextCollapsed);
        if (nextCollapsed) {
          card.style.height = '';
          card.style.minHeight = '0px';
        } else if (card.dataset.chartId && typeof DashboardState.layout.fullChartHeights?.[card.dataset.chartId] === 'number') {
          card.style.height = `${DashboardState.layout.fullChartHeights[card.dataset.chartId]}px`;
          card.style.minHeight = fullChartMinHeight;
        } else if (typeof DashboardState.layout.fullChartHeight === 'number') {
          card.style.height = `${DashboardState.layout.fullChartHeight}px`;
          card.style.minHeight = fullChartMinHeight;
        } else {
          card.style.minHeight = fullChartMinHeight;
        }
      });
      fullChartBodies.forEach((body) => body.classList.toggle('hidden', nextCollapsed));
      fullChartHandles.forEach((handle) => handle.classList.toggle('hidden', nextCollapsed));
      fullChartToggles.forEach((toggle) => {
        toggle.setAttribute('aria-expanded', String(!nextCollapsed));
        const icon = toggle.querySelector('[data-lucide]');
        updateLucideIcon(icon, nextCollapsed ? 'chevron-down' : 'chevron-up');
      });
      DashboardState.layout.fullChartCollapsed = nextCollapsed;
      schedulePersistPreferences();
    };

    fullChartToggles.forEach((toggle) => {
      addListener(toggle, 'click', () => {
        const isCollapsed = fullChartCards[0]?.dataset.collapsed === 'true';
        setFullChartsCollapsed(!isCollapsed);
      });
    });
  }

  document.addEventListener('click', (event) => {
    if (!salesChannelPanel || salesChannelPanel.classList.contains('hidden')) return;
    if (salesChannelPanel.contains(event.target) || salesChannelToggle?.contains(event.target)) return;
    salesChannelPanel.classList.add('hidden');
    salesChannelToggle?.setAttribute('aria-expanded', 'false');
  });

  document.addEventListener('click', (event) => {
    const panel = document.getElementById('unit-type-panel');
    const toggle = document.getElementById('unit-type-toggle');
    if (!panel || panel.classList.contains('hidden')) return;
    if (panel.contains(event.target) || toggle?.contains(event.target)) return;
    panel.classList.add('hidden');
    toggle?.setAttribute('aria-expanded', 'false');
  });

  document.addEventListener('click', (event) => {
    const panel = document.getElementById('vehicle-status-panel');
    const toggle = document.getElementById('vehicle-status-toggle');
    if (!panel || panel.classList.contains('hidden')) return;
    if (panel.contains(event.target) || toggle?.contains(event.target)) return;
    panel.classList.add('hidden');
    toggle?.setAttribute('aria-expanded', 'false');
  });

  addListener(salesChannelOptions, 'change', (event) => {
    const input = event.target;
    if (!input.matches('input[type="checkbox"]')) return;
    const checked = Array.from(salesChannelOptions.querySelectorAll('input[type="checkbox"]'))
      .filter((item) => item.checked)
      .map((item) => item.value);
    DashboardState.filters.salesChannels = checked;
    if (salesChannelSummary) salesChannelSummary.textContent = String(checked.length);
    if (salesChannelLabel) {
      salesChannelLabel.textContent = checked.length === 1 ? checked[0] : 'Sales Channel';
    }
    resetPagination();
    renderDashboard();
    schedulePersistPreferences();
  });

  addListener(lastDealSelect, 'change', () => {
    const { lastLeadKey } = DashboardState.filters;
    if (!lastLeadKey) return;
    const selectionValue = lastDealSelect.value;
    const nextSelection = selectionValue === 'all' ? 'all' : selectionValue === 'true';
    DashboardState.filters.lastLeadSelection = nextSelection;
    DashboardState.filters.lastLeadFilterActive = selectionValue !== 'all';
    DashboardState.filters.columnFilters[lastLeadKey] = {
      select: selectionValue,
      search: '',
    };
    resetPagination();
    renderDashboard();
    schedulePersistPreferences();
    setupFilters({ preserveSelections: true });
  });

  segmentSelects.forEach((select) => {
    select.addEventListener('change', (event) => {
      const chartId = event.target.dataset.chartId || 'default';
      DashboardState.chartSegments[chartId] = event.target.value || DEFAULT_SEGMENT_KEY;
      renderDashboard();
      schedulePersistPreferences();
    });
  });

  chartContainers.forEach((container) => {
    container.addEventListener('click', (event) => {
      const button = event.target.closest('button[data-status][data-segment-key]');
      if (!button) return;
      const chartId = container.dataset.chartId || 'default';
      const segmentKey = button.dataset.segmentKey;
      const status = button.dataset.status;
      const current = DashboardState.filters.chartFilters?.[chartId];
      if (!current || current.key !== segmentKey) {
        DashboardState.filters.chartFilters[chartId] = { key: segmentKey, values: [status] };
      } else {
        const nextValues = Array.isArray(current.values) ? current.values.slice() : [];
        const statusIndex = nextValues.indexOf(status);
        if (statusIndex >= 0) {
          nextValues.splice(statusIndex, 1);
        } else {
          nextValues.push(status);
        }
        if (nextValues.length) {
          DashboardState.filters.chartFilters[chartId] = { key: segmentKey, values: nextValues };
        } else {
          delete DashboardState.filters.chartFilters[chartId];
        }
      }
      resetPagination();
      renderDashboard();
      schedulePersistPreferences();
    });
  });
};

const initializeResizablePanels = () => {
  const container = document.getElementById('deal-alerts-layout');
  const handle = document.getElementById('panel-resizer');
  const dealPanel = document.getElementById('deal-status-panel');
  const alertsPanel = document.getElementById('alerts-panel');
  const chartsLayout = document.getElementById('deal-charts-layout');
  const chartHandle = document.getElementById('chart-resizer');
  const primaryChart = document.getElementById('status-primary-card');
  const secondaryChart = document.getElementById('status-secondary-card');
  const tertiaryChartsLayout = document.getElementById('status-tertiary-layout');
  const tertiaryChartHandle = document.getElementById('tertiary-chart-resizer');
  const tertiaryChart = document.getElementById('status-tertiary-card');
  const tertiarySecondaryChart = document.getElementById('status-tertiary-secondary-card');
  const heightHandle = document.getElementById('panel-height-resizer');
  const fullChartCards = document.querySelectorAll('[data-full-chart-card]');
  const fullChartHandles = document.querySelectorAll('[data-full-chart-resizer]');
  if (!container || !handle || !dealPanel || !alertsPanel) return;

  const minPanelWidth = 220;

  let startX = 0;
  let startAlertsWidth = 0;
  let isDragging = false;

  const onPointerMove = (event) => {
    if (!isDragging) return;
    const deltaX = event.clientX - startX;
    const containerWidth = container.getBoundingClientRect().width;
    const handleWidth = handle.getBoundingClientRect().width;
    const maxAlertsWidth = Math.max(minPanelWidth, containerWidth - minPanelWidth - handleWidth);
    const nextAlertsWidth = Math.min(maxAlertsWidth, Math.max(minPanelWidth, startAlertsWidth - deltaX));
    alertsPanel.style.flex = `0 0 ${nextAlertsWidth}px`;
    dealPanel.style.flex = '1 1 auto';
  };

  const stopDragging = () => {
    if (!isDragging) return;
    isDragging = false;
    document.body.classList.remove('select-none', 'resize-col');
    window.removeEventListener('pointermove', onPointerMove);
    window.removeEventListener('pointerup', stopDragging);
    DashboardState.layout.alertsPanelWidth = alertsPanel.getBoundingClientRect().width;
    schedulePersistPreferences();
  };

  if (typeof DashboardState.layout.alertsPanelWidth === 'number' && window.innerWidth >= 1024) {
    alertsPanel.style.flex = `0 0 ${DashboardState.layout.alertsPanelWidth}px`;
    dealPanel.style.flex = '1 1 auto';
  }

  handle.addEventListener('pointerdown', (event) => {
    if (window.innerWidth < 1024) return;
    isDragging = true;
    startX = event.clientX;
    startAlertsWidth = alertsPanel.getBoundingClientRect().width;
    document.body.classList.add('select-none', 'resize-col');
    window.addEventListener('pointermove', onPointerMove);
    window.addEventListener('pointerup', stopDragging);
  });

  if (chartsLayout && chartHandle && primaryChart && secondaryChart) {
    const minChartWidth = 220;
    let chartStartX = 0;
    let startPrimaryWidth = 0;
    let isChartDragging = false;

    const onChartMove = (event) => {
      if (!isChartDragging) return;
      const deltaX = event.clientX - chartStartX;
      const containerWidth = chartsLayout.getBoundingClientRect().width;
      const handleWidth = chartHandle.getBoundingClientRect().width;
      const maxPrimaryWidth = Math.max(minChartWidth, containerWidth - minChartWidth - handleWidth);
      const nextPrimaryWidth = Math.min(maxPrimaryWidth, Math.max(minChartWidth, startPrimaryWidth + deltaX));
      primaryChart.style.flex = `0 0 ${nextPrimaryWidth}px`;
      secondaryChart.style.flex = '1 1 auto';
    };

    const stopChartDragging = () => {
      if (!isChartDragging) return;
      isChartDragging = false;
      document.body.classList.remove('select-none', 'resize-col');
      window.removeEventListener('pointermove', onChartMove);
      window.removeEventListener('pointerup', stopChartDragging);
      DashboardState.layout.chartSplitWidth = primaryChart.getBoundingClientRect().width;
      schedulePersistPreferences();
    };

    if (typeof DashboardState.layout.chartSplitWidth === 'number' && window.innerWidth >= 1024) {
      primaryChart.style.flex = `0 0 ${DashboardState.layout.chartSplitWidth}px`;
      secondaryChart.style.flex = '1 1 auto';
    }

    chartHandle.addEventListener('pointerdown', (event) => {
      if (window.innerWidth < 1024) return;
      isChartDragging = true;
      chartStartX = event.clientX;
      startPrimaryWidth = primaryChart.getBoundingClientRect().width;
      document.body.classList.add('select-none', 'resize-col');
      window.addEventListener('pointermove', onChartMove);
      window.addEventListener('pointerup', stopChartDragging);
    });
  }

  if (tertiaryChartsLayout && tertiaryChartHandle && tertiaryChart && tertiarySecondaryChart) {
    const minChartWidth = 220;
    let chartStartX = 0;
    let startPrimaryWidth = 0;
    let isChartDragging = false;

    const onChartMove = (event) => {
      if (!isChartDragging) return;
      const deltaX = event.clientX - chartStartX;
      const containerWidth = tertiaryChartsLayout.getBoundingClientRect().width;
      const handleWidth = tertiaryChartHandle.getBoundingClientRect().width;
      const maxPrimaryWidth = Math.max(minChartWidth, containerWidth - minChartWidth - handleWidth);
      const nextPrimaryWidth = Math.min(maxPrimaryWidth, Math.max(minChartWidth, startPrimaryWidth + deltaX));
      tertiaryChart.style.flex = `0 0 ${nextPrimaryWidth}px`;
      tertiarySecondaryChart.style.flex = '1 1 auto';
    };

    const stopChartDragging = () => {
      if (!isChartDragging) return;
      isChartDragging = false;
      document.body.classList.remove('select-none', 'resize-col');
      window.removeEventListener('pointermove', onChartMove);
      window.removeEventListener('pointerup', stopChartDragging);
      DashboardState.layout.tertiarySplitWidth = tertiaryChart.getBoundingClientRect().width;
      schedulePersistPreferences();
    };

    if (typeof DashboardState.layout.tertiarySplitWidth === 'number' && window.innerWidth >= 1024) {
      tertiaryChart.style.flex = `0 0 ${DashboardState.layout.tertiarySplitWidth}px`;
      tertiarySecondaryChart.style.flex = '1 1 auto';
    }

    tertiaryChartHandle.addEventListener('pointerdown', (event) => {
      if (window.innerWidth < 1024) return;
      isChartDragging = true;
      chartStartX = event.clientX;
      startPrimaryWidth = tertiaryChart.getBoundingClientRect().width;
      document.body.classList.add('select-none', 'resize-col');
      window.addEventListener('pointermove', onChartMove);
      window.addEventListener('pointerup', stopChartDragging);
    });
  }

  if (heightHandle) {
    const minPanelHeight = 220;
    let heightStartY = 0;
    let startHeight = 0;
    let isHeightDragging = false;

    const onHeightMove = (event) => {
      if (!isHeightDragging) return;
      const deltaY = event.clientY - heightStartY;
      const nextHeight = Math.max(minPanelHeight, startHeight + deltaY);
      const heightValue = `${nextHeight}px`;
      dealPanel.style.height = heightValue;
      alertsPanel.style.height = heightValue;
      if (primaryChart) primaryChart.style.height = heightValue;
      if (secondaryChart) secondaryChart.style.height = heightValue;
    };

    const stopHeightDragging = () => {
      if (!isHeightDragging) return;
      isHeightDragging = false;
      document.body.classList.remove('select-none', 'resize-row');
      window.removeEventListener('pointermove', onHeightMove);
      window.removeEventListener('pointerup', stopHeightDragging);
      DashboardState.layout.dealPanelHeight = dealPanel.getBoundingClientRect().height;
      schedulePersistPreferences();
    };

    if (typeof DashboardState.layout.dealPanelHeight === 'number' && window.innerWidth >= 1024) {
      const heightValue = `${DashboardState.layout.dealPanelHeight}px`;
      dealPanel.style.height = heightValue;
      alertsPanel.style.height = heightValue;
      if (primaryChart) primaryChart.style.height = heightValue;
      if (secondaryChart) secondaryChart.style.height = heightValue;
    }

    heightHandle.addEventListener('pointerdown', (event) => {
      if (window.innerWidth < 1024) return;
      isHeightDragging = true;
      heightStartY = event.clientY;
      startHeight = dealPanel.getBoundingClientRect().height;
      document.body.classList.add('select-none', 'resize-row');
      window.addEventListener('pointermove', onHeightMove);
      window.addEventListener('pointerup', stopHeightDragging);
    });
  }

  if (fullChartCards.length && fullChartHandles.length) {
    const minFullHeight = 220;
    fullChartHandles.forEach((handle) => {
      const chartId = handle.dataset.chartId;
      const chart = chartId
        ? document.querySelector(`[data-full-chart-card][data-chart-id="${chartId}"]`)
        : handle.closest('[data-full-chart-card]');
      if (!chart) return;
      let fullStartY = 0;
      let startFullHeight = 0;
      let isFullDragging = false;

      const onFullHeightMove = (event) => {
        if (!isFullDragging) return;
        const deltaY = event.clientY - fullStartY;
        const nextHeight = Math.max(minFullHeight, startFullHeight + deltaY);
        chart.style.height = `${nextHeight}px`;
      };

      const stopFullDragging = () => {
        if (!isFullDragging) return;
        isFullDragging = false;
        document.body.classList.remove('select-none', 'resize-row');
        window.removeEventListener('pointermove', onFullHeightMove);
        window.removeEventListener('pointerup', stopFullDragging);
        const height = chart.getBoundingClientRect().height;
        if (chartId) {
          if (!DashboardState.layout.fullChartHeights || typeof DashboardState.layout.fullChartHeights !== 'object') {
            DashboardState.layout.fullChartHeights = {};
          }
          DashboardState.layout.fullChartHeights[chartId] = height;
        }
        schedulePersistPreferences();
      };

      handle.addEventListener('pointerdown', (event) => {
        if (window.innerWidth < 1024) return;
        if (chart.dataset.collapsed === 'true') return;
        isFullDragging = true;
        fullStartY = event.clientY;
        startFullHeight = chart.getBoundingClientRect().height;
        document.body.classList.add('select-none', 'resize-row');
        window.addEventListener('pointermove', onFullHeightMove);
        window.addEventListener('pointerup', stopFullDragging);
      });
    });
  }
};

const initializeFilterPanel = () => {
  const toggle = document.getElementById('filter-toggle');
  const activeFilters = document.getElementById('active-filters');
  const builder = document.getElementById('filter-builder');
  const advancedToggle = document.getElementById('advanced-toggle');
  const categoryBlock = document.getElementById('filter-category-block');
  if (!toggle || !activeFilters || !builder) return;

  toggle.classList.add(...PILL_CLASSES.split(' '));
  document.querySelectorAll('[data-pill-control]').forEach((button) => {
    if (button === toggle) return;
    button.classList.add(...PILL_CLASSES.split(' '));
  });

  const mediaQuery = window.matchMedia('(min-width: 768px)');
  let userToggled = false;

  const setExpanded = (expanded) => {
    activeFilters.classList.toggle('hidden', !expanded);
    toggle.setAttribute('aria-expanded', String(expanded));
  };

  setExpanded(mediaQuery.matches);

  toggle.addEventListener('click', () => {
    userToggled = true;
    const isExpanded = toggle.getAttribute('aria-expanded') === 'true';
    setExpanded(!isExpanded);
  });

  const handleMediaChange = (event) => {
    if (!userToggled) setExpanded(event.matches);
  };

  if (mediaQuery.addEventListener) {
    mediaQuery.addEventListener('change', handleMediaChange);
  } else {
    mediaQuery.addListener(handleMediaChange);
  }

  if (advancedToggle && categoryBlock) {
    const setAdvanced = (expanded) => {
      categoryBlock.classList.toggle('hidden', !expanded);
      advancedToggle.setAttribute('aria-expanded', String(expanded));
    };

    setAdvanced(false);
    advancedToggle.addEventListener('click', () => {
      const isExpanded = advancedToggle.getAttribute('aria-expanded') === 'true';
      setAdvanced(!isExpanded);
    });
  }
};

// ==========================================================
// 3) REALTIME + HYDRATE
// ==========================================================

const handleVehicleChange = (payload) => {
  const record = payload.new || payload.old;
  if (!record) return;

  if (!DashboardState.schema.length) {
    DashboardState.schema = buildSchemaFromData([record]);
    initializeTablePreferences();
  }

  const normalized = normalizeVehicle(record);
  const eventTimestamp = payload.commit_timestamp ? new Date(payload.commit_timestamp).getTime() : Date.now();
  normalized.lastEventAt = eventTimestamp;

  const key = getVehicleKey(normalized);
  if (!key) return;

  if (payload.eventType === 'DELETE') DashboardState.vehiclesRaw.delete(key);
  else DashboardState.vehiclesRaw.set(key, normalized);

  renderDashboard();
};

// ==========================================================
// 4) BOOT
// ==========================================================

initializeFilterPanel();
setupFilters();
const ui = initDashboardUI({
  applyFilters,
  getCurrentDataset,
  getSegmentOptions,
  getSegmentLabel,
  ensureChartVisibilityState,
  setChartHiddenValues,
  getOrderedColumns,
  getVehicleKey,
  PILL_CLASSES,
  syncTopScrollbar: () => syncTopScrollbar(),
  createIcons: () => lucide?.createIcons?.(),
});
({ renderDashboard, renderColumnChooser, openDrawer, closeDrawer } = ui);
const baseRenderDashboard = renderDashboard;
renderDashboard = () => {
  baseRenderDashboard();
  updateGpsOfflineCluster();
};
bindFilterEvents();
initializeResizablePanels();

// Mobile nav
const mobileToggle = document.getElementById('mobile-menu-toggle');
const primaryNav = document.getElementById('primary-nav');
mobileToggle?.addEventListener('click', () => {
  const isOpen = primaryNav.classList.toggle('hidden');
  mobileToggle.setAttribute('aria-expanded', String(!isOpen));
});

// Start Supabase
(async () => {
  setConnectionStatus('Booting…');
  supabaseClient = await getSupabaseClient({
    supabaseUrl: SUPABASE_URL,
    supabaseAnonKey: SUPABASE_ANON_KEY,
    showDebug,
  });
  await loadDashboardPreferences();
  applyLayoutPreferencesToDom();

  if (!supabaseClient) {
    setConnectionStatus('Offline');
    DashboardState.ui.isLoading = false;
    renderDashboard();
    setAlertsDealCount(0);
    return;
  }

  // If RLS blocks, debug banner will show exact error
  await fetchAlertsDealCount();
  setInterval(() => {
    fetchAlertsDealCount();
  }, 60 * 60 * 1000);
  await hydrateVehiclesFromSupabase({
    supabaseClient,
    setConnectionStatus,
    renderDashboard,
    showDebug,
    buildSchemaFromData,
    setVehiclesFromArray,
    initializeTablePreferences,
    setupFilters,
    getField,
  });
  initializeSupabaseRealtime({
    supabaseClient,
    setConnectionStatus,
    handleVehicleChange,
  });
})();
