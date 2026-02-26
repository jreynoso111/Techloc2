const getDefaultHelpers = () => ({
  escapeHTML: (value = '') => `${value}`,
  formatDateTime: (value) => value
});

const getVehicleVin = (vehicle) => {
  const vin = vehicle?.VIN ?? vehicle?.vin ?? vehicle?.details?.VIN ?? '';
  return typeof vin === 'string' ? vin.trim().toUpperCase() : '';
};

const getVehicleId = (vehicle) => {
  const vehicleId = vehicle?.vehicle_id
    ?? vehicle?.vehicleId
    ?? vehicle?.id
    ?? vehicle?.details?.vehicle_id
    ?? vehicle?.details?.vehicleId
    ?? '';
  return vehicleId ? `${vehicleId}`.trim() : '';
};

const normalizeSerial = (value) => {
  if (value === null || value === undefined) return '';
  return String(value).trim().toUpperCase();
};

const getVehicleWinnerSerial = (vehicle = {}) => {
  const details = vehicle?.details || {};
  const candidates = [
    vehicle?.winnerSerial,
    vehicle?.winningSerial,
    vehicle?.serialWinner,
    details?.winner_serial,
    details?.winning_serial,
    details?.serial_winner,
    details?.['Winner Serial'],
    details?.['Winning Serial'],
    vehicle?.ptSerial,
    details?.['PT Serial'],
    details?.['PT Serial '],
    details?.pt_serial,
    details?.['PassTime Serial No'],
    details?.['PassTime Serial Number'],
    details?.['GPS Serial No'],
    vehicle?.encoreSerial,
    details?.['Encore Serial'],
    details?.encore_serial,
  ];

  for (const candidate of candidates) {
    const normalized = normalizeSerial(candidate);
    if (normalized) return normalized;
  }
  return '';
};

const getRecordSerial = (record = {}) => {
  const candidates = [
    record?.Serial,
    record?.serial,
    record?.['Serial No'],
    record?.serial_no,
    record?.serial_number,
    record?.['PT Serial'],
    record?.['PT Serial '],
    record?.pt_serial,
    record?.['PassTime Serial No'],
    record?.['PassTime Serial Number'],
    record?.['GPS Serial No'],
    record?.encore_serial,
    record?.['Encore Serial'],
  ];

  for (const candidate of candidates) {
    const normalized = normalizeSerial(candidate);
    if (normalized) return normalized;
  }
  return '';
};

const GPS_HISTORY_DAY_MS = 24 * 60 * 60 * 1000;
const GPS_WINNER_RECENCY_WINDOW_MS = 14 * GPS_HISTORY_DAY_MS;
const GPS_STATIONARY_CLUSTER_RADIUS_METERS = 220;

const parseCoordinate = (record = {}, key = 'lat') => {
  const candidates = key === 'lat'
    ? ['lat', 'Lat', 'latitude', 'Latitude']
    : ['long', 'Long', 'lng', 'Lng', 'longitude', 'Longitude', 'lon', 'Lon'];
  for (const candidate of candidates) {
    const raw = record?.[candidate];
    if (raw === null || raw === undefined || raw === '') continue;
    const parsed = Number.parseFloat(`${raw}`.replace(/[^\d.\-]/g, ''));
    if (Number.isFinite(parsed)) return parsed;
  }
  return null;
};

const toLocalDayStartMs = (timeMs) => {
  if (!Number.isFinite(timeMs) || timeMs <= 0) return null;
  const date = new Date(timeMs);
  if (Number.isNaN(date.getTime())) return null;
  return new Date(date.getFullYear(), date.getMonth(), date.getDate()).getTime();
};

const getPointDistanceMeters = (fromPoint, toPoint) => {
  if (!fromPoint || !toPoint) return Number.POSITIVE_INFINITY;
  if (!Number.isFinite(fromPoint.lat) || !Number.isFinite(fromPoint.lng)) return Number.POSITIVE_INFINITY;
  if (!Number.isFinite(toPoint.lat) || !Number.isFinite(toPoint.lng)) return Number.POSITIVE_INFINITY;
  const toRadians = (value) => value * (Math.PI / 180);
  const earthRadiusMeters = 6371000;
  const deltaLat = toRadians(toPoint.lat - fromPoint.lat);
  const deltaLng = toRadians(toPoint.lng - fromPoint.lng);
  const lat1 = toRadians(fromPoint.lat);
  const lat2 = toRadians(toPoint.lat);
  const a = (Math.sin(deltaLat / 2) ** 2)
    + (Math.cos(lat1) * Math.cos(lat2) * (Math.sin(deltaLng / 2) ** 2));
  const boundedA = Math.min(1, Math.max(0, a));
  const c = 2 * Math.atan2(Math.sqrt(boundedA), Math.sqrt(1 - boundedA));
  return earthRadiusMeters * c;
};

const applyWinnerDisplayOverrides = (records = []) => {
  if (!Array.isArray(records) || !records.length) return records;

  const entries = records.map((record, index) => {
    const timestamp = getRecordTimestampMs(record);
    return {
      record,
      index,
      timestamp,
      lat: parseCoordinate(record, 'lat'),
      lng: parseCoordinate(record, 'long'),
      dayStartMs: toLocalDayStartMs(timestamp),
      derivedMoved: null,
      derivedDaysStationary: null
    };
  });

  const ordered = [...entries].sort((a, b) => {
    if (a.timestamp === b.timestamp) return a.index - b.index;
    return a.timestamp - b.timestamp;
  });

  let previous = null;
  let sessionStartDayMs = null;
  ordered.forEach((entry) => {
    const hasCoords = Number.isFinite(entry.lat) && Number.isFinite(entry.lng);
    const previousHasCoords = Number.isFinite(previous?.lat) && Number.isFinite(previous?.lng);
    const distance = (hasCoords && previousHasCoords)
      ? getPointDistanceMeters({ lat: previous.lat, lng: previous.lng }, { lat: entry.lat, lng: entry.lng })
      : Number.POSITIVE_INFINITY;

    if (!previous || !Number.isFinite(distance) || distance > GPS_STATIONARY_CLUSTER_RADIUS_METERS) {
      entry.derivedMoved = previous ? 'Moving' : 'Parked';
      sessionStartDayMs = entry.dayStartMs;
    } else {
      entry.derivedMoved = 'Parked';
      if (!Number.isFinite(sessionStartDayMs) && Number.isFinite(entry.dayStartMs)) {
        sessionStartDayMs = entry.dayStartMs;
      }
    }

    if (Number.isFinite(entry.dayStartMs) && Number.isFinite(sessionStartDayMs)) {
      entry.derivedDaysStationary = Math.max(
        0,
        Math.floor((entry.dayStartMs - sessionStartDayMs) / GPS_HISTORY_DAY_MS)
      );
    }

    previous = entry;
  });

  const byRecord = new Map(ordered.map((entry) => [entry.record, entry]));
  return records.map((record) => {
    const entry = byRecord.get(record);
    if (!entry) return record;
    return {
      ...record,
      __derivedMoved: entry.derivedMoved,
      __derivedDaysStationary: entry.derivedDaysStationary
    };
  });
};

const getRecordTimestampMs = (record = {}) => {
  const candidates = [
    record?.['PT-LastPing'],
    record?.gps_time,
    record?.Date,
    record?.created_at,
    record?.updated_at
  ];
  for (const candidate of candidates) {
    if (!candidate) continue;
    const parsed = Date.parse(candidate);
    if (!Number.isNaN(parsed)) return parsed;
  }
  const numericId = Number(record?.id);
  return Number.isFinite(numericId) ? numericId : Number.NEGATIVE_INFINITY;
};

const getLocalDayBoundsMs = (referenceMs = Date.now()) => {
  const referenceDate = new Date(referenceMs);
  const dayStart = new Date(
    referenceDate.getFullYear(),
    referenceDate.getMonth(),
    referenceDate.getDate()
  ).getTime();
  return {
    start: dayStart,
    end: dayStart + GPS_HISTORY_DAY_MS
  };
};

const getMostRecentSerialFromRecords = (records = [], { excludeSerial = '' } = {}) => {
  if (!Array.isArray(records) || !records.length) return '';
  const excluded = normalizeSerial(excludeSerial);
  let selectedSerial = '';
  let selectedTimestamp = Number.NEGATIVE_INFINITY;
  records.forEach((record) => {
    const serial = getRecordSerial(record);
    if (!serial) return;
    if (excluded && serial === excluded) return;
    const timestamp = getRecordTimestampMs(record);
    if (timestamp >= selectedTimestamp) {
      selectedTimestamp = timestamp;
      selectedSerial = serial;
    }
  });
  return selectedSerial;
};

const resolveVehicleWinnerSerialFromRecords = (vehicle = {}, records = [], { nowMs = Date.now() } = {}) => {
  const configuredWinnerSerial = getVehicleWinnerSerial(vehicle);
  if (!Array.isArray(records) || !records.length) return configuredWinnerSerial;

  if (configuredWinnerSerial) {
    const { start, end } = getLocalDayBoundsMs(nowMs);
    const winnerHasReadingToday = records.some((record) => {
      const serial = getRecordSerial(record);
      if (!serial || serial !== configuredWinnerSerial) return false;
      const timestamp = getRecordTimestampMs(record);
      return Number.isFinite(timestamp) && timestamp >= start && timestamp < end;
    });
    if (winnerHasReadingToday) return configuredWinnerSerial;

    const recentWindowStart = nowMs - GPS_WINNER_RECENCY_WINDOW_MS;
    const winnerHasRecentReading = records.some((record) => {
      const serial = getRecordSerial(record);
      if (!serial || serial !== configuredWinnerSerial) return false;
      const timestamp = getRecordTimestampMs(record);
      return Number.isFinite(timestamp) && timestamp >= recentWindowStart;
    });
    if (winnerHasRecentReading) return configuredWinnerSerial;

    const winnerHasAnyReading = records.some((record) => getRecordSerial(record) === configuredWinnerSerial);
    if (winnerHasAnyReading) return configuredWinnerSerial;

    const fallbackSerial = getMostRecentSerialFromRecords(records, { excludeSerial: configuredWinnerSerial });
    if (fallbackSerial) return fallbackSerial;
  }

  return getMostRecentSerialFromRecords(records);
};

const createGpsHistoryManager = ({
  supabaseClient,
  ensureSupabaseSession,
  runWithTimeout,
  timeoutMs = 10000,
  tableName,
  escapeHTML,
  formatDateTime
} = {}) => {
  const helpers = getDefaultHelpers();
  const safeEscape = escapeHTML || helpers.escapeHTML;
  const safeFormatDateTime = formatDateTime || helpers.formatDateTime;

  const fetchGpsHistory = async ({ vin, vehicleId } = {}) => {
    const normalizedVin = typeof vin === 'string' ? vin.trim().toUpperCase() : '';
    const normalizedVehicleId = typeof vehicleId === 'string' ? vehicleId.trim() : '';
    if (!supabaseClient || (!normalizedVin && !normalizedVehicleId)) {
      return { records: [], error: supabaseClient ? new Error('VIN missing') : new Error('Supabase unavailable') };
    }
    try {
      await ensureSupabaseSession?.();
      const sourceTable = tableName || 'PT-LastPing';
      const records = [];
      const pageSize = 1000;
      let offset = 0;
      let hasMore = true;

      while (hasMore) {
        const pageBaseQuery = supabaseClient
          .from(sourceTable)
          .select('*')
          .range(offset, offset + pageSize - 1);
        const pageQuery = normalizedVin
          ? pageBaseQuery.eq('VIN', normalizedVin)
          : pageBaseQuery.eq('vehicle_id', normalizedVehicleId);
        const { data, error } = await runWithTimeout(
          pageQuery,
          timeoutMs,
          'GPS history request timed out.'
        );
        if (error) throw error;
        const pageRows = Array.isArray(data) ? data : [];
        if (!pageRows.length) break;
        records.push(...pageRows);
        hasMore = pageRows.length === pageSize;
        offset += pageSize;
      }

      records.sort((a, b) => {
        const idA = a?.id;
        const idB = b?.id;
        if (idA != null && idB != null) {
          return (Number(idB) || 0) - (Number(idA) || 0);
        }
        const dateCandidates = ['Date', 'created_at', 'gps_time', 'PT-LastPing'];
        const dateValue = (record) => {
          for (const key of dateCandidates) {
            const value = record?.[key];
            if (value) return Date.parse(value);
          }
          return NaN;
        };
        const timeA = dateValue(a);
        const timeB = dateValue(b);
        if (Number.isNaN(timeA) && Number.isNaN(timeB)) return 0;
        if (Number.isNaN(timeA)) return 1;
        if (Number.isNaN(timeB)) return -1;
        return timeB - timeA;
      });
      return { records, error: null };
    } catch (error) {
      console.error('Failed to load GPS history:', error);
      return { records: [], error };
    }
  };

  const setupGpsHistoryUI = ({ vehicle, body, signal, records: preloadedRecords, error: preloadedError }) => {
    const VIN = getVehicleVin(vehicle);
    const vehicleId = getVehicleId(vehicle);
    let winnerSerial = getVehicleWinnerSerial(vehicle);
    const historyBody = body.querySelector('[data-gps-history-body]');
    const historyHead = body.querySelector('[data-gps-history-head]');
    const columnsToggle = body.querySelector('[data-gps-columns-toggle]');
    const columnsPanel = body.querySelector('[data-gps-columns-panel]');
    const columnsList = body.querySelector('[data-gps-columns-list]');
    const widthColumnSelect = body.querySelector('[data-gps-width-column]');
    const widthValueInput = body.querySelector('[data-gps-width-value]');
    const widthApplyButton = body.querySelector('[data-gps-width-apply]');
    const widthAutoButton = body.querySelector('[data-gps-width-auto]');
    const searchInput = body.querySelector('[data-gps-search]');
    const statusText = body.querySelector('[data-gps-status]');
    const connectionStatus = body.querySelector('[data-gps-connection-status]');
    const viewWinnerButton = body.querySelector('[data-gps-view="winner"]');
    const viewAllButton = body.querySelector('[data-gps-view="all"]');
    const winnerInfo = body.querySelector('[data-gps-winner-info]');

    let gpsCache = [];
    let gpsColumns = [];
    let availableColumnKeys = [];
    let columnVisibility = {};
    let columnOrder = [];
    let columnWidths = {};
    let searchQuery = '';
    let sortKey = '';
    let sortDirection = 'desc';
    let activeResize = null;
    let activeHeaderDragKey = '';
    let suppressSortUntil = 0;
    let headerDragEnabledBeforeResize = [];
    let viewMode = winnerSerial ? 'winner' : 'all';

    const COLUMN_STORAGE_KEY = 'gpsHistoryColumnPrefs';
    const WIDTH_STORAGE_KEY = 'gpsHistoryColumnWidths';
    const MIN_COLUMN_WIDTH = 80;
    const MAX_COLUMN_WIDTH = 1200;

    const DEFAULT_COLUMN_ORDER = [
      'created_at',
      'PT-LastPing',
      'gps_time',
      'latitude',
      'lat',
      'longitude',
      'long',
      'lng',
      'speed',
      'heading',
      'VIN'
    ];

    const titleCase = (value) => value
      .replace(/_/g, ' ')
      .replace(/\b\w/g, (match) => match.toUpperCase());

    const parseWidthValue = (value) => {
      const parsed = Number.parseInt(`${value || ''}`.trim(), 10);
      if (!Number.isFinite(parsed)) return '';
      return String(Math.min(MAX_COLUMN_WIDTH, Math.max(MIN_COLUMN_WIDTH, parsed)));
    };

    const applyViewButtonState = (button, isActive) => {
      if (!button) return;
      button.classList.toggle('border-blue-400/60', isActive);
      button.classList.toggle('bg-blue-500/20', isActive);
      button.classList.toggle('text-blue-100', isActive);
      button.classList.toggle('border-slate-700', !isActive);
      button.classList.toggle('bg-slate-900', !isActive);
      button.classList.toggle('text-slate-300', !isActive);
    };

    const syncViewControls = () => {
      const winnerEnabled = Boolean(winnerSerial);
      if (viewWinnerButton) {
        viewWinnerButton.disabled = !winnerEnabled;
        viewWinnerButton.classList.toggle('opacity-50', !winnerEnabled);
        viewWinnerButton.classList.toggle('cursor-not-allowed', !winnerEnabled);
      }
      if (!winnerEnabled && viewMode === 'winner') {
        viewMode = 'all';
      }

      applyViewButtonState(viewWinnerButton, viewMode === 'winner');
      applyViewButtonState(viewAllButton, viewMode === 'all');

      if (winnerInfo) {
        winnerInfo.textContent = winnerEnabled
          ? `Winner serial: ${winnerSerial}`
          : 'Winner serial not available for this vehicle.';
      }
    };

    const setViewMode = (nextMode) => {
      if (nextMode === 'winner' && !winnerSerial) {
        viewMode = 'all';
      } else {
        viewMode = nextMode === 'winner' ? 'winner' : 'all';
      }
      syncViewControls();
      renderHistory(gpsCache);
    };

    const getModeFilteredRecords = (records = []) => {
      if (viewMode !== 'winner' || !winnerSerial) return records;
      return records.filter((record) => getRecordSerial(record) === winnerSerial);
    };

    const loadPreferences = () => {
      let savedPrefs = {};
      let savedWidths = {};
      try {
        savedPrefs = JSON.parse(localStorage.getItem(COLUMN_STORAGE_KEY) || '{}');
      } catch (error) {
        savedPrefs = {};
      }
      try {
        savedWidths = JSON.parse(localStorage.getItem(WIDTH_STORAGE_KEY) || '{}');
      } catch (error) {
        savedWidths = {};
      }
      columnVisibility = savedPrefs.visibility || {};
      columnOrder = savedPrefs.order || [];
      columnWidths = savedWidths || {};
    };

    const savePreferences = () => {
      localStorage.setItem(COLUMN_STORAGE_KEY, JSON.stringify({
        visibility: columnVisibility,
        order: columnOrder
      }));
      localStorage.setItem(WIDTH_STORAGE_KEY, JSON.stringify(columnWidths));
    };

    const buildColumnsFromKeys = (keys = []) => {
      const allKeys = Array.from(new Set(keys.filter(Boolean)));
      const ordered = [
        ...DEFAULT_COLUMN_ORDER.filter((key) => allKeys.includes(key)),
        ...allKeys.filter((key) => !DEFAULT_COLUMN_ORDER.includes(key)).sort()
      ];
      gpsColumns = ordered.map((key) => ({ key, label: titleCase(key) }));

      if (!columnOrder.length) {
        columnOrder = [...ordered];
      } else {
        const missing = ordered.filter((key) => !columnOrder.includes(key));
        columnOrder = columnOrder.filter((key) => ordered.includes(key)).concat(missing);
      }

      gpsColumns.forEach((col) => {
        if (columnVisibility[col.key] === undefined) {
          columnVisibility[col.key] = true;
        }
        if (columnWidths[col.key] === undefined) {
          columnWidths[col.key] = '';
        }
      });
      savePreferences();
    };

    const buildColumns = (records) => {
      const columnKeys = new Set(availableColumnKeys);
      records.forEach((record) => {
        Object.keys(record || {}).forEach((key) => {
          columnKeys.add(key);
        });
      });
      buildColumnsFromKeys(Array.from(columnKeys));
    };

    const getVisibleColumns = () => columnOrder
      .filter((key) => columnVisibility[key])
      .map((key) => gpsColumns.find((col) => col.key === key))
      .filter(Boolean);

    const normalizeAddressToken = (value = '') => value
      .toLowerCase()
      .replace(/\./g, '')
      .replace(/\s+/g, ' ')
      .trim();

    const ADDRESS_COUNTRY_SUFFIXES = new Set([
      'usa',
      'us',
      'u s a',
      'united states',
      'united states of america',
      'estados unidos',
      'eeuu',
    ]);

    const isAddressColumn = (normalizedKey = '') =>
      normalizedKey === 'address' || normalizedKey.endsWith('_address') || normalizedKey.includes('address');

    const stripAddressCountrySuffix = (value) => {
      const text = `${value ?? ''}`.trim();
      if (!text) return '';
      const parts = text.split(',').map((part) => part.trim()).filter(Boolean);
      if (!parts.length) return text;

      while (parts.length > 1) {
        const token = normalizeAddressToken(parts[parts.length - 1]);
        if (!ADDRESS_COUNTRY_SUFFIXES.has(token)) break;
        parts.pop();
      }

      return parts.join(', ') || text;
    };

    const formatValue = (key, value) => {
      if (value === null || value === undefined || value === '') return '—';
      const normalizedKey = key.toLowerCase();
      if (normalizedKey === 'moved') {
        const numeric = Number(value);
        if (numeric === 1) return 'Moving';
        if (numeric === -1) return 'Parked';
        if (numeric === 0) return '';
      }
      if (key === 'PT-LastPing' || normalizedKey.includes('date') || normalizedKey.includes('time')) {
        return safeEscape(`${safeFormatDateTime(value)}`);
      }
      if (isAddressColumn(normalizedKey)) {
        return safeEscape(stripAddressCountrySuffix(value));
      }
      if (typeof value === 'object') {
        return safeEscape(JSON.stringify(value));
      }
      return safeEscape(`${value}`);
    };

    const normalizeColumnKey = (key = '') => `${key}`.trim().toLowerCase().replace(/\s+/g, '_');
    const isMovedColumnKey = (normalizedKey = '') => normalizedKey === 'moved';
    const isDaysStationaryColumnKey = (normalizedKey = '') =>
      normalizedKey === 'days_stationary'
      || normalizedKey === 'days_parked'
      || normalizedKey === 'days_stationary_calc';

    const getDisplayValue = (record, key, rawValue) => {
      const normalizedKey = normalizeColumnKey(key);
      if (isMovedColumnKey(normalizedKey) && record?.__derivedMoved) {
        return record.__derivedMoved;
      }
      if (isDaysStationaryColumnKey(normalizedKey) && Number.isFinite(record?.__derivedDaysStationary)) {
        return record.__derivedDaysStationary;
      }
      return rawValue;
    };

    const renderTableHead = () => {
      if (!historyHead) return;
      const visibleColumns = getVisibleColumns();
      if (!visibleColumns.length) {
        historyHead.innerHTML = `
          <tr>
            <th class="py-2 pr-3">No columns selected</th>
          </tr>
        `;
        return;
      }
      historyHead.innerHTML = `
        <tr>
          ${visibleColumns.map((col) => {
            const width = columnWidths[col.key];
            const widthStyle = width ? `style="width:${width}px;min-width:${width}px"` : '';
            return `
              <th class="py-2 pr-3 align-bottom gps-history-th-resizable" data-gps-col-header="${col.key}" draggable="true" ${widthStyle}>
                <button type="button" class="group inline-flex items-center gap-1 text-left text-[10px] uppercase tracking-[0.08em] text-slate-400 hover:text-slate-200 transition-colors" data-gps-sort="${col.key}">
                  <span>${safeEscape(col.label)}</span>
                  <span class="text-[9px] text-slate-500 group-hover:text-slate-300">${sortKey === col.key ? (sortDirection === 'asc' ? '▲' : '▼') : ''}</span>
                </button>
                <button type="button" class="gps-history-resize-handle" data-gps-resize="${col.key}" aria-label="Resize ${safeEscape(col.label)} column"></button>
              </th>
            `;
          }).join('')}
        </tr>
      `;
    };

    const getFilteredRecords = (records) => {
      const query = searchQuery.trim().toLowerCase();
      if (!query) return records;
      return records.filter((record) => Object.values(record || {}).some((value) =>
        `${value ?? ''}`.toLowerCase().includes(query)
      ));
    };

    const getSortedRecords = (records) => {
      if (!sortKey) return records;
      const direction = sortDirection === 'asc' ? 1 : -1;
      return [...records].sort((a, b) => {
        const valueA = a?.[sortKey];
        const valueB = b?.[sortKey];
        if (valueA === valueB) return 0;
        if (valueA === null || valueA === undefined || valueA === '') return 1 * direction;
        if (valueB === null || valueB === undefined || valueB === '') return -1 * direction;
        if (typeof valueA === 'number' && typeof valueB === 'number') {
          return (valueA - valueB) * direction;
        }
        return `${valueA}`.localeCompare(`${valueB}`, undefined, { numeric: true, sensitivity: 'base' }) * direction;
      });
    };

    const renderHistory = (records = []) => {
      if (!historyBody) return;
      gpsCache = records;
      buildColumns(records);
      renderColumnsList();
      renderTableHead();
      const visibleColumns = getVisibleColumns();
      const modeRecords = getModeFilteredRecords(records);
      const displayRecords = (viewMode === 'winner' && winnerSerial)
        ? applyWinnerDisplayOverrides(modeRecords)
        : modeRecords;
      const filteredRecords = getSortedRecords(getFilteredRecords(displayRecords));

      if (statusText) {
        const modeSuffix = viewMode === 'winner' && winnerSerial
          ? ` (winner serial ${winnerSerial})`
          : '';
        statusText.textContent = `${filteredRecords.length} record${filteredRecords.length === 1 ? '' : 's'} shown${modeSuffix}.`;
      }

      if (!filteredRecords.length) {
        const colSpan = Math.max(visibleColumns.length, 1);
        const emptyMessage = viewMode === 'winner' && winnerSerial
          ? `No GPS history found for winner serial ${safeEscape(winnerSerial)}.`
          : 'No GPS history found.';
        historyBody.innerHTML = `
          <tr>
            <td class="py-2 pr-3 text-slate-400" colspan="${colSpan}">${emptyMessage}</td>
          </tr>
        `;
        return;
      }
      historyBody.innerHTML = filteredRecords.map((record) => `
        <tr>
          ${visibleColumns.map((col) => {
            const width = columnWidths[col.key];
            const widthStyle = width ? `style="width:${width}px;min-width:${width}px"` : '';
            const rawValue = record?.[col.key];
            const displayValue = getDisplayValue(record, col.key, rawValue);
            const normalizedKey = `${col.key || ''}`.toLowerCase();
            const tooltipValue = isAddressColumn(normalizedKey)
              ? stripAddressCountrySuffix(displayValue)
              : displayValue;
            const tooltip = displayValue === null || displayValue === undefined || displayValue === ''
              ? ''
              : safeEscape(typeof tooltipValue === 'object' ? JSON.stringify(tooltipValue) : `${tooltipValue}`);
            return `<td class="py-1.5 pr-3 text-slate-300 align-top" ${widthStyle}><div class="gps-history-cell-clamp" title="${tooltip}">${formatValue(col.key, displayValue)}</div></td>`;
          }).join('')}
        </tr>
      `).join('');
    };

    const renderColumnsList = () => {
      if (!columnsList) return;
      if (!gpsColumns.length) {
        columnsList.innerHTML = '<p class="text-xs text-slate-400">No columns available.</p>';
        syncWidthControls();
        return;
      }
      columnsList.innerHTML = columnOrder.map((key) => {
        const column = gpsColumns.find((col) => col.key === key);
        if (!column) return '';
        return `
          <div class="flex items-center gap-2 rounded-md border border-slate-800 bg-slate-950/70 px-2 py-1.5" draggable="true" data-gps-column="${column.key}">
            <span class="text-slate-500">⋮⋮</span>
            <label class="flex items-center gap-2 flex-1">
              <input type="checkbox" value="${column.key}" class="h-3.5 w-3.5 rounded border-slate-600 bg-slate-950" ${columnVisibility[column.key] ? 'checked' : ''} />
              <span class="text-slate-200">${safeEscape(column.label)}</span>
            </label>
          </div>
        `;
      }).join('');
      syncWidthControls();
    };

    const syncWidthControls = () => {
      if (!widthColumnSelect || !widthValueInput || !widthApplyButton || !widthAutoButton) return;

      if (!gpsColumns.length) {
        widthColumnSelect.innerHTML = '<option value="">No columns</option>';
        widthColumnSelect.disabled = true;
        widthValueInput.disabled = true;
        widthApplyButton.disabled = true;
        widthAutoButton.disabled = true;
        widthValueInput.value = '';
        return;
      }

      const previousKey = widthColumnSelect.value;
      widthColumnSelect.innerHTML = columnOrder.map((key) => {
        const column = gpsColumns.find((col) => col.key === key);
        if (!column) return '';
        return `<option value="${column.key}">${safeEscape(column.label)}</option>`;
      }).join('');

      const selectedKey = (previousKey && columnOrder.includes(previousKey))
        ? previousKey
        : (columnOrder[0] || gpsColumns[0]?.key || '');

      widthColumnSelect.value = selectedKey;
      widthColumnSelect.disabled = false;
      widthValueInput.disabled = false;
      widthApplyButton.disabled = false;
      widthAutoButton.disabled = false;
      widthValueInput.value = selectedKey ? (columnWidths[selectedKey] || '') : '';
    };

    const applySelectedWidth = () => {
      const key = widthColumnSelect?.value || '';
      if (!key) return;
      const normalized = parseWidthValue(widthValueInput?.value);
      if (widthValueInput) widthValueInput.value = normalized;
      columnWidths[key] = normalized;
      savePreferences();
      renderTableHead();
      renderHistory(gpsCache);
    };

    const resetSelectedWidth = () => {
      const key = widthColumnSelect?.value || '';
      if (!key) return;
      columnWidths[key] = '';
      if (widthValueInput) widthValueInput.value = '';
      savePreferences();
      renderTableHead();
      renderHistory(gpsCache);
    };

    const findHeaderCellByKey = (key) => {
      if (!historyHead || !key) return null;
      const headers = historyHead.querySelectorAll('[data-gps-col-header]');
      return [...headers].find((header) => header.dataset.gpsColHeader === key) || null;
    };

    const getVisibleColumnIndex = (key) => getVisibleColumns().findIndex((col) => col.key === key);

    const applyColumnWidthToRenderedTable = (key, widthPx) => {
      const widthStyleValue = widthPx ? `${widthPx}px` : '';
      const headerCell = findHeaderCellByKey(key);
      if (headerCell) {
        headerCell.style.width = widthStyleValue;
        headerCell.style.minWidth = widthStyleValue;
      }
      if (!historyBody) return;
      const columnIndex = getVisibleColumnIndex(key);
      if (columnIndex < 0) return;
      const rows = historyBody.querySelectorAll('tr');
      rows.forEach((row) => {
        const cell = row.children[columnIndex];
        if (!cell) return;
        cell.style.width = widthStyleValue;
        cell.style.minWidth = widthStyleValue;
      });
    };

    const stopColumnResize = () => {
      if (!activeResize) return;
      if (activeResize.target?.releasePointerCapture && activeResize.pointerId !== undefined) {
        try {
          activeResize.target.releasePointerCapture(activeResize.pointerId);
        } catch (_error) {
          // Ignore browsers that throw if capture was not set.
        }
      }
      activeResize = null;
      window.removeEventListener('pointermove', handleColumnResizeMove);
      window.removeEventListener('pointerup', handleColumnResizeEnd);
      document.body.classList.remove('gps-column-resizing');
      if (historyHead) {
        historyHead.style.userSelect = '';
        headerDragEnabledBeforeResize.forEach(({ node, draggable }) => {
          if (!node) return;
          node.setAttribute('draggable', draggable ? 'true' : 'false');
        });
      }
      headerDragEnabledBeforeResize = [];
      savePreferences();
      renderTableHead();
      renderHistory(gpsCache);
    };

    const handleColumnResizeMove = (event) => {
      if (!activeResize) return;
      if (activeResize.pointerId !== undefined && event.pointerId !== activeResize.pointerId) return;
      event.preventDefault();
      const delta = event.clientX - activeResize.startX;
      const nextWidth = Math.max(
        MIN_COLUMN_WIDTH,
        Math.min(MAX_COLUMN_WIDTH, Math.round(activeResize.startWidth + delta))
      );
      if (nextWidth === activeResize.currentWidth) return;
      activeResize.currentWidth = nextWidth;
      columnWidths[activeResize.key] = String(nextWidth);
      if (widthColumnSelect?.value === activeResize.key && widthValueInput) {
        widthValueInput.value = String(nextWidth);
      }
      applyColumnWidthToRenderedTable(activeResize.key, nextWidth);
    };

    const handleColumnResizeEnd = () => {
      stopColumnResize();
    };

    const startColumnResize = (event, key, headerCell) => {
      if (!key || !headerCell) return;
      if (event.button !== 0) return;
      const presetWidth = Number.parseInt(`${columnWidths[key] || ''}`, 10);
      const measuredWidth = Math.round(headerCell.getBoundingClientRect().width || MIN_COLUMN_WIDTH);
      const startWidth = Number.isFinite(presetWidth) ? presetWidth : measuredWidth;
      headerDragEnabledBeforeResize = historyHead
        ? [...historyHead.querySelectorAll('[data-gps-col-header]')].map((node) => ({
          node,
          draggable: node.getAttribute('draggable') === 'true'
        }))
        : [];
      headerDragEnabledBeforeResize.forEach(({ node }) => node.setAttribute('draggable', 'false'));
      if (historyHead) {
        historyHead.style.userSelect = 'none';
      }
      if (event.target?.setPointerCapture && event.pointerId !== undefined) {
        try {
          event.target.setPointerCapture(event.pointerId);
        } catch (_error) {
          // Ignore browsers that do not allow capture on this target.
        }
      }
      activeResize = {
        key,
        startX: event.clientX,
        startWidth: Math.max(MIN_COLUMN_WIDTH, Math.min(MAX_COLUMN_WIDTH, startWidth)),
        currentWidth: Math.max(MIN_COLUMN_WIDTH, Math.min(MAX_COLUMN_WIDTH, startWidth)),
        pointerId: event.pointerId,
        target: event.target
      };
      document.body.classList.add('gps-column-resizing');
      window.addEventListener('pointermove', handleColumnResizeMove);
      window.addEventListener('pointerup', handleColumnResizeEnd);
    };

    const updateColumnOrder = (dragKey, targetKey) => {
      if (!dragKey || !targetKey || dragKey === targetKey) return;
      const currentOrder = [...columnOrder];
      const fromIndex = currentOrder.indexOf(dragKey);
      const toIndex = currentOrder.indexOf(targetKey);
      if (fromIndex === -1 || toIndex === -1) return;
      currentOrder.splice(fromIndex, 1);
      currentOrder.splice(toIndex, 0, dragKey);
      columnOrder = currentOrder;
      savePreferences();
      renderColumnsList();
      renderTableHead();
      renderHistory(gpsCache);
    };

    const clearHeaderDropTargets = () => {
      if (!historyHead) return;
      historyHead
        .querySelectorAll('.gps-history-header-drop-target')
        .forEach((node) => node.classList.remove('gps-history-header-drop-target'));
    };

    const setConnectionStatus = (label, tone = 'neutral') => {
      if (!connectionStatus) return;
      connectionStatus.textContent = label;
      connectionStatus.classList.remove(
        'border-emerald-400/50',
        'bg-emerald-500/15',
        'text-emerald-100',
        'border-amber-400/50',
        'bg-amber-500/15',
        'text-amber-100',
        'border-rose-400/50',
        'bg-rose-500/15',
        'text-rose-100'
      );
      if (tone === 'success') {
        connectionStatus.classList.add('border-emerald-400/50', 'bg-emerald-500/15', 'text-emerald-100');
      } else if (tone === 'warning') {
        connectionStatus.classList.add('border-amber-400/50', 'bg-amber-500/15', 'text-amber-100');
      } else if (tone === 'error') {
        connectionStatus.classList.add('border-rose-400/50', 'bg-rose-500/15', 'text-rose-100');
      }
    };

    const finalizeRender = (records, error) => {
      winnerSerial = resolveVehicleWinnerSerialFromRecords(vehicle, records);
      syncViewControls();
      renderHistory(records);
      if (statusText && error) statusText.textContent = 'Unable to load GPS history.';
      if (error) {
        setConnectionStatus('Connection failed', 'error');
      } else {
        setConnectionStatus(`Connected · ${tableName}`, 'success');
      }
    };

    const loadColumnMetadata = async () => {
      if (!supabaseClient) return;
      const sourceTable = tableName || 'PT-LastPing';
      try {
        await ensureSupabaseSession?.();
        const sampleQuery = supabaseClient
          .from(sourceTable)
          .select('*')
          .limit(1);
        const { data, error } = runWithTimeout
          ? await runWithTimeout(
            sampleQuery,
            timeoutMs,
            'GPS history column request timed out.'
          )
          : await sampleQuery;
        if (error) throw error;
        const firstRow = Array.isArray(data) ? data[0] : null;
        availableColumnKeys = firstRow ? Object.keys(firstRow) : [];
        if (availableColumnKeys.length) {
          buildColumnsFromKeys(availableColumnKeys);
          renderColumnsList();
          renderTableHead();
        }
      } catch (error) {
        console.warn('Failed to load GPS history columns:', error);
      }
    };

    loadPreferences();
    syncViewControls();
    if (statusText) statusText.textContent = 'Loading GPS history...';
    loadColumnMetadata();
    if (!VIN && !vehicleId) {
      renderHistory([]);
      if (statusText) statusText.textContent = 'No VIN or vehicle ID available for this vehicle.';
      setConnectionStatus('Vehicle missing', 'warning');
    } else if (!supabaseClient) {
      renderHistory([]);
      if (statusText) statusText.textContent = 'Supabase connection not available.';
      setConnectionStatus('Disconnected', 'error');
    } else if (Array.isArray(preloadedRecords)) {
      finalizeRender(preloadedRecords, preloadedError);
    } else {
      setConnectionStatus('Connecting…', 'warning');
      fetchGpsHistory({ vin: VIN, vehicleId }).then(({ records, error }) => {
        finalizeRender(records, error);
      });
    }

    if (columnsToggle && columnsPanel) {
      columnsToggle.addEventListener('click', () => {
        columnsPanel.classList.toggle('hidden');
      }, { signal });
    }

    if (viewWinnerButton) {
      viewWinnerButton.addEventListener('click', () => {
        setViewMode('winner');
      }, { signal });
    }

    if (viewAllButton) {
      viewAllButton.addEventListener('click', () => {
        setViewMode('all');
      }, { signal });
    }

    if (columnsList) {
      columnsList.addEventListener('change', (event) => {
        const input = event.target;
        if (!input || input.tagName !== 'INPUT') return;
        if (input.type === 'checkbox') {
          columnVisibility[input.value] = input.checked;
          savePreferences();
          renderTableHead();
          renderHistory(gpsCache);
        }
      }, { signal });

      let draggedKey = null;
      columnsList.addEventListener('dragstart', (event) => {
        const row = event.target.closest('[data-gps-column]');
        if (!row) return;
        draggedKey = row.dataset.gpsColumn;
        row.classList.add('opacity-60');
        event.dataTransfer.effectAllowed = 'move';
        event.dataTransfer.setData('text/plain', draggedKey);
      }, { signal });

      columnsList.addEventListener('dragend', (event) => {
        const row = event.target.closest('[data-gps-column]');
        if (row) row.classList.remove('opacity-60');
        draggedKey = null;
      }, { signal });

      columnsList.addEventListener('dragover', (event) => {
        if (!draggedKey) return;
        event.preventDefault();
        event.dataTransfer.dropEffect = 'move';
      }, { signal });

      columnsList.addEventListener('drop', (event) => {
        event.preventDefault();
        const targetRow = event.target.closest('[data-gps-column]');
        const targetKey = targetRow?.dataset.gpsColumn;
        if (!draggedKey || !targetKey) return;
        updateColumnOrder(draggedKey, targetKey);
      }, { signal });
    }

    if (widthColumnSelect && widthValueInput && widthApplyButton && widthAutoButton) {
      widthColumnSelect.addEventListener('change', () => {
        const key = widthColumnSelect.value || '';
        widthValueInput.value = key ? (columnWidths[key] || '') : '';
      }, { signal });

      widthApplyButton.addEventListener('click', () => {
        applySelectedWidth();
      }, { signal });

      widthAutoButton.addEventListener('click', () => {
        resetSelectedWidth();
      }, { signal });

      widthValueInput.addEventListener('keydown', (event) => {
        if (event.key !== 'Enter') return;
        event.preventDefault();
        applySelectedWidth();
      }, { signal });
    }

    if (historyHead) {
      historyHead.addEventListener('dragstart', (event) => {
        if (activeResize) {
          event.preventDefault();
          return;
        }
        const header = event.target.closest('[data-gps-col-header]');
        if (!header) return;
        if (event.target.closest('[data-gps-resize]')) {
          event.preventDefault();
          return;
        }
        const key = header.dataset.gpsColHeader || '';
        if (!key) return;
        activeHeaderDragKey = key;
        header.classList.add('opacity-60');
        if (event.dataTransfer) {
          event.dataTransfer.effectAllowed = 'move';
          event.dataTransfer.setData('text/plain', key);
        }
      }, { signal });

      historyHead.addEventListener('dragend', () => {
        activeHeaderDragKey = '';
        clearHeaderDropTargets();
        historyHead
          .querySelectorAll('[data-gps-col-header].opacity-60')
          .forEach((node) => node.classList.remove('opacity-60'));
      }, { signal });

      historyHead.addEventListener('dragover', (event) => {
        if (!activeHeaderDragKey) return;
        const target = event.target.closest('[data-gps-col-header]');
        if (!target) return;
        event.preventDefault();
        clearHeaderDropTargets();
        if (target.dataset.gpsColHeader !== activeHeaderDragKey) {
          target.classList.add('gps-history-header-drop-target');
        }
      }, { signal });

      historyHead.addEventListener('drop', (event) => {
        if (!activeHeaderDragKey) return;
        const target = event.target.closest('[data-gps-col-header]');
        if (!target) return;
        event.preventDefault();
        const targetKey = target.dataset.gpsColHeader || '';
        if (targetKey && targetKey !== activeHeaderDragKey) {
          updateColumnOrder(activeHeaderDragKey, targetKey);
          suppressSortUntil = Date.now() + 250;
        }
        clearHeaderDropTargets();
      }, { signal });

      historyHead.addEventListener('pointerdown', (event) => {
        const handle = event.target.closest('[data-gps-resize]');
        if (handle) {
          event.preventDefault();
          event.stopPropagation();
          const key = handle.dataset.gpsResize;
          const headerCell = handle.closest('[data-gps-col-header]');
          startColumnResize(event, key, headerCell);
          return;
        }

        const headerCell = event.target.closest('[data-gps-col-header]');
        if (!headerCell) return;
        const rect = headerCell.getBoundingClientRect();
        const nearRightEdge = (rect.right - event.clientX) <= 12;
        if (!nearRightEdge) return;
        event.preventDefault();
        event.stopPropagation();
        const key = headerCell.dataset.gpsColHeader || '';
        startColumnResize(event, key, headerCell);
      }, { signal });

      historyHead.addEventListener('click', (event) => {
        if (Date.now() < suppressSortUntil) return;
        const button = event.target.closest('[data-gps-sort]');
        if (!button) return;
        const nextSortKey = button.dataset.gpsSort;
        if (!nextSortKey) return;
        if (sortKey === nextSortKey) {
          sortDirection = sortDirection === 'asc' ? 'desc' : 'asc';
        } else {
          sortKey = nextSortKey;
          sortDirection = 'asc';
        }
        renderHistory(gpsCache);
      }, { signal });
    }

    signal?.addEventListener('abort', () => {
      stopColumnResize();
    }, { once: true });

    if (searchInput) {
      searchInput.addEventListener('input', (event) => {
        searchQuery = event.target.value || '';
        renderHistory(gpsCache);
      }, { signal });
    }
  };

  return {
    getVehicleId,
    getVehicleVin,
    getVehicleWinnerSerial,
    resolveVehicleWinnerSerialFromRecords,
    getRecordSerial,
    fetchGpsHistory,
    setupGpsHistoryUI
  };
};

export { createGpsHistoryManager };
