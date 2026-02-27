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
const GPS_SERIAL_ALARM_MOVEMENT_DISTANCE_METERS = 220;

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

const isWiredSerial = (serial = '') => /^[0-7]/.test(normalizeSerial(serial));

const isWirelessSerial = (serial = '') => /^8/.test(normalizeSerial(serial));

const getLocalDayKeyFromMs = (timeMs) => {
  if (!Number.isFinite(timeMs) || timeMs <= 0) return '';
  const date = new Date(timeMs);
  if (Number.isNaN(date.getTime())) return '';
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
};

const resolveSerialMovementStateForDayPoints = (points = []) => {
  if (!Array.isArray(points) || points.length < 2) return 'unknown';
  const ordered = [...points].sort((a, b) => a.timestamp - b.timestamp);
  const first = ordered[0];
  const last = ordered[ordered.length - 1];

  let totalDistanceMeters = 0;
  let maxSegmentMeters = 0;
  for (let index = 1; index < ordered.length; index += 1) {
    const segmentDistance = getPointDistanceMeters(ordered[index - 1], ordered[index]);
    if (!Number.isFinite(segmentDistance)) continue;
    totalDistanceMeters += segmentDistance;
    if (segmentDistance > maxSegmentMeters) maxSegmentMeters = segmentDistance;
  }

  const netDistanceMeters = getPointDistanceMeters(first, last);
  const moved = (
    maxSegmentMeters > GPS_SERIAL_ALARM_MOVEMENT_DISTANCE_METERS
    || netDistanceMeters > GPS_SERIAL_ALARM_MOVEMENT_DISTANCE_METERS
    || totalDistanceMeters > (GPS_SERIAL_ALARM_MOVEMENT_DISTANCE_METERS * 1.2)
  );
  return moved ? 'moving' : 'stopped';
};

const detectWirelessSerialMovementAlarms = (
  records = [],
  { isSerialBlacklisted = null } = {}
) => {
  if (!Array.isArray(records) || !records.length) return new Set();
  const isBlacklisted = typeof isSerialBlacklisted === 'function'
    ? (serial) => {
      try {
        return Boolean(isSerialBlacklisted(serial));
      } catch (_error) {
        return false;
      }
    }
    : () => false;

  const daySerialPoints = new Map();
  records.forEach((record) => {
    const serial = getRecordSerial(record);
    if (!serial) return;
    const timestamp = getRecordTimestampMs(record);
    const dayKey = getLocalDayKeyFromMs(timestamp);
    if (!dayKey) return;
    const lat = parseCoordinate(record, 'lat');
    const lng = parseCoordinate(record, 'long');
    if (!Number.isFinite(lat) || !Number.isFinite(lng)) return;

    const dayEntry = daySerialPoints.get(dayKey) || new Map();
    const serialPoints = dayEntry.get(serial) || [];
    serialPoints.push({ lat, lng, timestamp });
    dayEntry.set(serial, serialPoints);
    daySerialPoints.set(dayKey, dayEntry);
  });

  const alarmSerials = new Set();
  daySerialPoints.forEach((serialMap) => {
    let hasWiredMoving = false;
    const wirelessStoppedSerials = [];

    serialMap.forEach((points, serial) => {
      const movementState = resolveSerialMovementStateForDayPoints(points);
      if (movementState === 'moving' && isWiredSerial(serial)) {
        hasWiredMoving = true;
      }
      if (
        movementState === 'stopped'
        && isWirelessSerial(serial)
        && !isBlacklisted(serial)
      ) {
        wirelessStoppedSerials.push(serial);
      }
    });

    if (!hasWiredMoving || !wirelessStoppedSerials.length) return;
    wirelessStoppedSerials.forEach((serial) => alarmSerials.add(serial));
  });

  return alarmSerials;
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

const GPS_DATE_COLUMN_CANDIDATES = [
  'PT-LastPing',
  'Date',
  'gps_time',
  'created_at',
  'updated_at'
];

const getPreferredDateColumnKey = (keys = []) => {
  if (!Array.isArray(keys) || !keys.length) return '';
  const normalized = new Map(keys.map((key) => [`${key}`.toLowerCase(), key]));
  for (const candidate of GPS_DATE_COLUMN_CANDIDATES) {
    const hit = normalized.get(candidate.toLowerCase());
    if (hit) return hit;
  }
  return '';
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

const getMostRecentSerialFromRecords = (
  records = [],
  {
    excludeSerial = '',
    isSerialAllowed = null
  } = {}
) => {
  if (!Array.isArray(records) || !records.length) return '';
  const excluded = normalizeSerial(excludeSerial);
  const canUseSerial = typeof isSerialAllowed === 'function'
    ? (serial) => Boolean(isSerialAllowed(serial))
    : () => true;
  let selectedSerial = '';
  let selectedTimestamp = Number.NEGATIVE_INFINITY;
  records.forEach((record) => {
    const serial = getRecordSerial(record);
    if (!serial) return;
    if (excluded && serial === excluded) return;
    if (!canUseSerial(serial)) return;
    const timestamp = getRecordTimestampMs(record);
    if (timestamp >= selectedTimestamp) {
      selectedTimestamp = timestamp;
      selectedSerial = serial;
    }
  });
  return selectedSerial;
};

const GPS_WINNER_STICKY_WINDOW_MS = 2 * 60 * 60 * 1000;

const resolveVehicleWinnerSerialFromRecords = (
  vehicle = {},
  records = [],
  {
    nowMs = Date.now(),
    isSerialBlacklisted = null
  } = {}
) => {
  const configuredWinnerSerial = getVehicleWinnerSerial(vehicle);
  if (!Array.isArray(records) || !records.length) return configuredWinnerSerial;
  const isBlocked = typeof isSerialBlacklisted === 'function'
    ? (serial) => {
      try {
        return Boolean(isSerialBlacklisted(serial));
      } catch (_error) {
        return false;
      }
    }
    : () => false;
  const isAllowed = (serial) => serial && !isBlocked(serial);
  const recentWindowStart = nowMs - GPS_WINNER_RECENCY_WINDOW_MS;
  const { start: todayStart, end: todayEnd } = getLocalDayBoundsMs(nowMs);

  const serialStats = new Map();
  records.forEach((record) => {
    const serial = getRecordSerial(record);
    if (!serial || !isAllowed(serial)) return;
    const timestamp = getRecordTimestampMs(record);
    if (!Number.isFinite(timestamp)) return;
    const existing = serialStats.get(serial) || {
      latestTimestamp: Number.NEGATIVE_INFINITY,
      hasReadingToday: false,
      hasRecentReading: false
    };
    if (timestamp >= existing.latestTimestamp) {
      existing.latestTimestamp = timestamp;
    }
    if (timestamp >= todayStart && timestamp < todayEnd) {
      existing.hasReadingToday = true;
    }
    if (timestamp >= recentWindowStart) {
      existing.hasRecentReading = true;
    }
    serialStats.set(serial, existing);
  });

  const freshestAllowedSerial = [...serialStats.entries()]
    .sort((a, b) => b[1].latestTimestamp - a[1].latestTimestamp)
    .map(([serial]) => serial)[0] || '';
  const freshestAllowedTodaySerial = [...serialStats.entries()]
    .filter(([, stats]) => stats.hasReadingToday)
    .sort((a, b) => b[1].latestTimestamp - a[1].latestTimestamp)
    .map(([serial]) => serial)[0] || '';

  if (configuredWinnerSerial && isAllowed(configuredWinnerSerial)) {
    const configuredStats = serialStats.get(configuredWinnerSerial);
    if (configuredStats?.hasReadingToday) {
      return configuredWinnerSerial;
    }

    // If configured serial has no reading today, prefer the freshest serial from today.
    if (freshestAllowedTodaySerial && freshestAllowedTodaySerial !== configuredWinnerSerial) {
      return freshestAllowedTodaySerial;
    }

    if (configuredStats?.hasRecentReading && freshestAllowedSerial) {
      const freshestStats = serialStats.get(freshestAllowedSerial);
      if (!freshestStats || freshestAllowedSerial === configuredWinnerSerial) {
        return configuredWinnerSerial;
      }

      const freshnessGap = freshestStats.latestTimestamp - configuredStats.latestTimestamp;
      // Avoid flapping when both serials are effectively tied in recency.
      if (freshnessGap <= GPS_WINNER_STICKY_WINDOW_MS) {
        return configuredWinnerSerial;
      }
    }
  }

  if (freshestAllowedTodaySerial) return freshestAllowedTodaySerial;
  if (freshestAllowedSerial) return freshestAllowedSerial;

  const fallbackSerial = getMostRecentSerialFromRecords(records, {
    isSerialAllowed: (serial) => !isBlocked(serial)
  });
  if (fallbackSerial) return fallbackSerial;

  return getMostRecentSerialFromRecords(records);
};

const createGpsHistoryManager = ({
  supabaseClient,
  ensureSupabaseSession,
  runWithTimeout,
  timeoutMs = 10000,
  tableName,
  isSerialBlacklisted,
  escapeHTML,
  formatDateTime
} = {}) => {
  const helpers = getDefaultHelpers();
  const safeEscape = escapeHTML || helpers.escapeHTML;
  const safeFormatDateTime = formatDateTime || helpers.formatDateTime;
  const serialIsBlacklisted = (serial = '') => {
    if (typeof isSerialBlacklisted !== 'function') return false;
    try {
      const normalized = normalizeSerial(serial);
      if (!normalized) return false;
      return Boolean(isSerialBlacklisted(normalized));
    } catch (_error) {
      return false;
    }
  };

  const resolveWinnerSerial = (vehicle = {}, records = [], options = {}) => (
    resolveVehicleWinnerSerialFromRecords(vehicle, records, {
      ...options,
      isSerialBlacklisted: options?.isSerialBlacklisted || serialIsBlacklisted
    })
  );

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
    let viewMode = 'all';
    const serialSectionState = new Map();

    const COLUMN_STORAGE_KEY_BASE = 'gpsHistoryColumnPrefs';
    const WIDTH_STORAGE_KEY_BASE = 'gpsHistoryColumnWidths';
    const SERIAL_UNASSIGNED = '__UNASSIGNED_SERIAL__';
    let preferenceScope = 'anonymous';
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

    const getStorageKey = (baseKey) => `${baseKey}:${preferenceScope}`;

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

    const resolvePreferenceScope = async () => {
      if (!supabaseClient?.auth?.getSession) return;
      try {
        const { data, error } = await supabaseClient.auth.getSession();
        if (error) return;
        const userId = `${data?.session?.user?.id || ''}`.trim();
        if (!userId || userId === preferenceScope) return;
        preferenceScope = userId;
        loadPreferences();
        buildColumns(gpsCache);
        renderColumnsList();
        renderTableHead();
        renderHistory(gpsCache);
      } catch (_error) {
        // best effort
      }
    };

    const loadPreferences = () => {
      let savedPrefs = {};
      let savedWidths = {};
      const parseStored = (storageKey) => {
        try {
          return JSON.parse(localStorage.getItem(storageKey) || '{}');
        } catch (_error) {
          return null;
        }
      };
      savedPrefs = parseStored(getStorageKey(COLUMN_STORAGE_KEY_BASE))
        || parseStored(COLUMN_STORAGE_KEY_BASE)
        || {};
      savedWidths = parseStored(getStorageKey(WIDTH_STORAGE_KEY_BASE))
        || parseStored(WIDTH_STORAGE_KEY_BASE)
        || {};
      columnVisibility = savedPrefs.visibility || {};
      columnOrder = savedPrefs.order || [];
      columnWidths = savedWidths || {};
    };

    const savePreferences = () => {
      localStorage.setItem(getStorageKey(COLUMN_STORAGE_KEY_BASE), JSON.stringify({
        visibility: columnVisibility,
        order: columnOrder
      }));
      localStorage.setItem(getStorageKey(WIDTH_STORAGE_KEY_BASE), JSON.stringify(columnWidths));
    };

    const ensureDateDescendingSort = (keys = []) => {
      sortKey = getPreferredDateColumnKey(keys);
      sortDirection = 'desc';
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
      ensureDateDescendingSort(ordered);
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
      return [...records].sort((a, b) => {
        const timestampA = getRecordTimestampMs(a);
        const timestampB = getRecordTimestampMs(b);
        if (timestampA !== timestampB) return timestampB - timestampA;
        if (!sortKey) return 0;
        const valueA = a?.[sortKey];
        const valueB = b?.[sortKey];
        return `${valueA ?? ''}`.localeCompare(`${valueB ?? ''}`, undefined, { numeric: true, sensitivity: 'base' });
      });
    };

    const getSerialGroupKey = (record) => getRecordSerial(record) || SERIAL_UNASSIGNED;

    const getDateDisplayFromRecord = (record = {}) => {
      if (sortKey && record?.[sortKey]) return safeFormatDateTime(record[sortKey]);
      const timestamp = getRecordTimestampMs(record);
      if (!Number.isFinite(timestamp)) return 'N/A';
      return safeFormatDateTime(new Date(timestamp).toISOString());
    };

    const buildSerialGroups = (records = []) => {
      const grouped = new Map();
      records.forEach((record) => {
        const groupKey = getSerialGroupKey(record);
        const groupRecords = grouped.get(groupKey) || [];
        groupRecords.push(record);
        grouped.set(groupKey, groupRecords);
      });

      return [...grouped.entries()]
        .map(([serial, groupRecords]) => ({
          serial,
          records: groupRecords,
          latestTimestamp: getRecordTimestampMs(groupRecords[0])
        }))
        .sort((a, b) => {
          if (a.latestTimestamp !== b.latestTimestamp) return b.latestTimestamp - a.latestTimestamp;
          return `${a.serial}`.localeCompare(`${b.serial}`);
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
      const serialGroups = buildSerialGroups(filteredRecords);
      const wirelessAlarmSerials = detectWirelessSerialMovementAlarms(modeRecords, {
        isSerialBlacklisted: serialIsBlacklisted
      });

      if (statusText) {
        const modeSuffix = viewMode === 'winner' && winnerSerial
          ? ` (winner serial ${winnerSerial})`
          : '';
        const alertSuffix = wirelessAlarmSerials.size
          ? ` · ${wirelessAlarmSerials.size} wireless alarm${wirelessAlarmSerials.size === 1 ? '' : 's'}`
          : '';
        statusText.textContent = `${filteredRecords.length} record${filteredRecords.length === 1 ? '' : 's'} shown in ${serialGroups.length} serial section${serialGroups.length === 1 ? '' : 's'}${modeSuffix}${alertSuffix}.`;
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
      const colSpan = Math.max(visibleColumns.length, 1);
      historyBody.innerHTML = serialGroups.map((group) => {
        const serialLabel = group.serial === SERIAL_UNASSIGNED ? 'No serial' : group.serial;
        const isWinnerSerial = Boolean(winnerSerial) && group.serial === winnerSerial;
        const hasWirelessAlarm = wirelessAlarmSerials.has(group.serial);
        const expanded = serialSectionState.get(group.serial) === true;
        const latestLabel = getDateDisplayFromRecord(group.records[0]);
        const oldestLabel = getDateDisplayFromRecord(group.records[group.records.length - 1]);
        const rowsMarkup = expanded
          ? group.records.map((record) => `
            <tr class="gps-history-record-row" data-gps-serial-row="${safeEscape(group.serial)}">
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
          `).join('')
          : '';

        return `
          <tr class="gps-history-serial-section-row">
            <td colspan="${colSpan}" class="py-0 pr-0">
              <button type="button" class="gps-history-serial-toggle${isWinnerSerial ? ' is-winner' : ''}${hasWirelessAlarm ? ' is-alarm' : ''}" data-gps-serial-toggle="${safeEscape(group.serial)}" aria-expanded="${expanded ? 'true' : 'false'}">
                <span class="gps-history-serial-toggle-main">
                  <span class="gps-history-serial-chevron">${expanded ? '▼' : '▶'}</span>
                  <span class="gps-history-serial-label">Serial: ${safeEscape(serialLabel)}</span>
                  ${isWinnerSerial ? '<span class="gps-history-serial-winner-badge" title="Winner serial" aria-label="Winner serial"></span>' : ''}
                  ${hasWirelessAlarm ? '<span class="gps-history-serial-alarm-badge">ALARM</span>' : ''}
                </span>
                <span class="gps-history-serial-stats">${group.records.length} row${group.records.length === 1 ? '' : 's'} · Latest: ${safeEscape(`${latestLabel}`)} · Oldest: ${safeEscape(`${oldestLabel}`)}</span>
              </button>
            </td>
          </tr>
          ${rowsMarkup}
        `;
      }).join('');
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
      winnerSerial = resolveWinnerSerial(vehicle, records);
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
    resolvePreferenceScope();
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
        if (!sortKey || nextSortKey !== sortKey) return;
        sortDirection = 'desc';
        renderHistory(gpsCache);
      }, { signal });
    }

    if (historyBody) {
      historyBody.addEventListener('click', (event) => {
        const toggle = event.target.closest('[data-gps-serial-toggle]');
        if (!toggle) return;
        const serial = toggle.dataset.gpsSerialToggle;
        if (!serial) return;
        const isExpanded = serialSectionState.get(serial) === true;
        serialSectionState.set(serial, !isExpanded);
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
    resolveVehicleWinnerSerialFromRecords: resolveWinnerSerial,
    getRecordSerial,
    fetchGpsHistory,
    setupGpsHistoryUI
  };
};

export { createGpsHistoryManager };
