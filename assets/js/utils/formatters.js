export const toStateCode = (value = '') => {
  if (!value) return '';
  const match = `${value}`.match(/([A-Z]{2})/i);
  return match ? match[1].toUpperCase() : '';
};

export const normalizeKey = (key = '') => `${key}`.trim().toLowerCase().replace(/\s+/g, '_');

export const escapeHTML = (value = '') => `${value}`
  .replace(/&/g, '&amp;')
  .replace(/</g, '&lt;')
  .replace(/>/g, '&gt;')
  .replace(/"/g, '&quot;')
  .replace(/'/g, '&#39;');

export const formatDateTime = (value) => {
  if (!value) return '—';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return '—';
  return date.toLocaleString('en-US', {
    month: '2-digit',
    day: '2-digit',
    year: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false
  }).replace(',', '');
};

export const normalizeFilterValue = (value) => (value ?? '').toString().trim().toLowerCase();

export const parseDealCompletion = (value) => {
  if (value === null || value === undefined) return null;
  const raw = String(value).replace(/%/g, '').trim();
  const num = parseFloat(raw);
  if (!Number.isFinite(num)) return null;
  if (num <= 1) return num * 100;
  return num;
};

export const getStatusCardStyles = (value, type) => {
  const normalized = normalizeFilterValue(value);
  if (type === 'deal' && normalized === 'active') {
    return {
      card: 'border-emerald-500/60 bg-emerald-500/15 text-emerald-100',
      label: 'text-emerald-200/80',
      value: 'text-emerald-50'
    };
  }
  if (type === 'prep' && normalized === 'out for repo') {
    return {
      card: 'border-red-500/60 bg-red-500/15 text-red-100',
      label: 'text-red-200/80',
      value: 'text-red-50'
    };
  }
  if (type === 'pt' && normalized === 'disabled') {
    return {
      card: 'border-red-500/60 bg-red-500/15 text-red-100',
      label: 'text-red-200/80',
      value: 'text-red-50'
    };
  }
  return {
    card: 'border-slate-800 bg-slate-900 text-slate-200',
    label: 'text-slate-500',
    value: 'text-slate-100'
  };
};

export const getVehicleMarkerColor = (vehicle = {}) => {
  const fix = (vehicle.gpsFix || '').trim().toLowerCase();
  if (fix.includes('fully working')) return '#22c55e';
  if (fix === 'all' || fix.includes('all')) return '#ef4444';
  return '#f59e0b';
};

export const getVehicleMarkerBorderColor = (fillColor) => {
  const color = (fillColor || '').toLowerCase();
  if (color === '#22c55e') return '#166534';
  if (color === '#f59e0b') return '#92400e';
  return '#991b1b';
};

const parsePtLastReadDate = (value) => {
  if (!value) return null;
  const raw = String(value).trim();
  if (!raw) return null;
  const parsed = new Date(raw);
  if (!Number.isNaN(parsed.getTime())) return parsed;

  const match = raw.match(/^(\d{1,2})[\/-](\d{1,2})[\/-](\d{2,4})(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?$/);
  if (!match) return null;
  const month = Number(match[1]);
  const day = Number(match[2]);
  let year = Number(match[3]);
  if (year < 100) year += 2000;
  const hours = Number(match[4] || 0);
  const minutes = Number(match[5] || 0);
  const seconds = Number(match[6] || 0);
  const date = new Date(year, month - 1, day, hours, minutes, seconds);
  if (Number.isNaN(date.getTime())) return null;
  return date;
};

const getPtLastReadAgeMs = (value) => {
  const readDate = parsePtLastReadDate(value);
  if (!readDate) return null;
  return Date.now() - readDate.getTime();
};

const getPtLastReadStatus = (value) => {
  const ageMs = getPtLastReadAgeMs(value);
  if (ageMs === null) return 'fresh';
  if (ageMs <= 0) return 'fresh';
  const twoDaysMs = 2 * 24 * 60 * 60 * 1000;
  const sevenDaysMs = 7 * 24 * 60 * 60 * 1000;
  if (ageMs > sevenDaysMs) return 'unknown';
  if (ageMs > twoDaysMs) return 'stale';
  return 'fresh';
};

const parseMovingIndicator = (...candidates) => {
  for (const candidate of candidates) {
    if (candidate === null || candidate === undefined) continue;
    const raw = String(candidate).trim();
    if (!raw) continue;
    const numeric = Number.parseInt(raw, 10);
    if (Number.isFinite(numeric)) {
      if (numeric === 1) return 'moving';
      if (numeric === -1) return 'stopped';
    }
    const normalized = raw.toLowerCase();
    if (normalized === 'moving' || normalized === 'move' || normalized === 'true' || normalized === 'yes') {
      return 'moving';
    }
    if (
      normalized === 'not moving' ||
      normalized === 'stopped' ||
      normalized === 'stop' ||
      normalized === 'false' ||
      normalized === 'no'
    ) {
      return 'stopped';
    }
  }
  return null;
};

export const getMovingStatus = (vehicle = {}) => {
  const historyOverride = `${vehicle?.historyMovingOverride || vehicle?.details?.historyMovingOverride || ''}`
    .trim()
    .toLowerCase();
  if (historyOverride === 'moving' || historyOverride === 'stopped' || historyOverride === 'unknown') {
    return historyOverride;
  }

  const explicitStatus = parseMovingIndicator(
    vehicle?.moving,
    vehicle?.movingCalc,
    vehicle?.gpsMoving,
    vehicle?.details?.moving,
    vehicle?.details?.moving_calc,
    vehicle?.details?.gps_moving,
    vehicle?.details?.['Moving'],
    vehicle?.details?.['Moving (Calc)'],
    vehicle?.details?.['GPS Moving']
  );
  if (explicitStatus) return explicitStatus;

  const ptReadStatus = getPtLastReadStatus(vehicle?.lastRead);
  if (ptReadStatus === 'stale') return 'stopped';
  if (ptReadStatus === 'unknown') return 'unknown';
  return 'unknown';
};

export const getMovingMeta = (vehicle = {}) => {
  const status = getMovingStatus(vehicle);
  if (status === 'moving') {
    return { label: 'Moving', text: 'text-emerald-200', bg: 'bg-emerald-500/10', dot: 'bg-emerald-400' };
  }
  if (status === 'stopped') {
    return { label: 'Not moving', text: 'text-amber-200', bg: 'bg-amber-500/10', dot: 'bg-amber-400' };
  }
  return { label: 'Unknown', text: 'text-slate-300', bg: 'bg-slate-800/70', dot: 'bg-slate-400' };
};

export const isVehicleNotMoving = (vehicle = {}) => {
  return getMovingStatus(vehicle) === 'stopped';
};

export const getMovingLabel = (value) => {
  if (value === 'moving') return 'Moving';
  if (value === 'stopped') return 'Not moving';
  if (value === 'unknown') return 'Unknown';
  return value;
};

export const getGpsFixLabel = (value, emptyValueToken = '__empty__') => {
  if (value === emptyValueToken) return 'Empty';
  return value;
};
