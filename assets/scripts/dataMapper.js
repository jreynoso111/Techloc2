const normalizeKey = (key = '') => `${key}`.trim().toLowerCase().replace(/\s+/g, '_');

const getField = (row, ...keys) => {
  for (const key of keys) {
    const rawValue = row?.[key];
    if (rawValue !== undefined && rawValue !== null && String(rawValue).trim() !== '') return rawValue;

    const lowerKey = typeof key === 'string' ? key.toLowerCase() : key;
    const lowerValue = row?.[lowerKey];
    if (lowerValue !== undefined && lowerValue !== null && String(lowerValue).trim() !== '') return lowerValue;

    const snakeKey = normalizeKey(key);
    const snakeValue = row?.[snakeKey];
    if (snakeValue !== undefined && snakeValue !== null && String(snakeValue).trim() !== '') return snakeValue;
  }

  if (row?.metadata) {
    for (const key of keys) {
      const metaValue = row.metadata?.[key] ?? row.metadata?.[normalizeKey(key)];
      if (metaValue !== undefined && metaValue !== null && String(metaValue).trim() !== '') return metaValue;
    }
  }

  return '';
};

const formatPhoneNumber = (value) => {
  const raw = String(value ?? '').replace(/\s+/g, ' ').trim();
  if (!raw) return { display: '', tel: '' };

  const digits = raw.replace(/\D+/g, '');

  if (digits.length === 11 && digits.startsWith('1')) {
    const ten = digits.slice(1);
    return { display: `${ten.slice(0, 3)}-${ten.slice(3, 6)}-${ten.slice(6)}`, tel: digits };
  }

  if (digits.length === 10) {
    return { display: `${digits.slice(0, 3)}-${digits.slice(3, 6)}-${digits.slice(6)}`, tel: digits };
  }

  const compact = raw.replace(/\s+/g, '');
  return { display: digits || compact, tel: digits || compact };
};

const defaultResolveCoords = (_row, { fallbackLatLng = null } = {}) => {
  if (fallbackLatLng && Number.isFinite(fallbackLatLng.lat) && Number.isFinite(fallbackLatLng.lng)) {
    return { coords: fallbackLatLng, hasExactCoords: true, accuracy: 'exact' };
  }
  return { coords: { lat: 0, lng: 0 }, hasExactCoords: false, accuracy: 'state' };
};

const normalizeInstaller = (row, idx = 0, { getField: fieldGetter, toStateCode, resolveCoords } = {}) => {
  const field = fieldGetter || getField;
  const toState = toStateCode || ((value = '') => `${value}`.slice(0, 2).toUpperCase());
  const resolve = resolveCoords || defaultResolveCoords;
  const company = field(row, 'company_name', 'Installation Company', 'Company', 'company', 'name');
  const stateCode = toState(field(row, 'State', 'state', 'state_code')) || 'US';
  const city = field(row, 'City', 'city');
  const zip = field(row, 'Zip', 'zip');
  const { coords, accuracy } = resolve(row, { zip, city, stateCode, seed: idx, getField: field });
  const { display: phone, tel: phoneDial } = formatPhoneNumber(field(row, 'Phone', 'phone'));
  return {
    id: row?.id ?? idx,
    type: 'technicians',
    name: company || 'Installer',
    company: company || 'Installer',
    email: field(row, 'Email', 'email') || '-',
    phone: phone || '-',
    phoneDial,
    city: city || '',
    state: stateCode,
    zip: zip || '',
    note: field(row, 'notes', 'note', 'Notes', 'Note') || '',
    lat: coords.lat,
    lng: coords.lng,
    locationAccuracy: accuracy,
    details: row || {},
    seed: idx,
  };
};

const normalizePartner = (row, type = 'tech', idx = 0, { getField: fieldGetter, toStateCode, resolveCoords } = {}) => {
  const field = fieldGetter || getField;
  const toState = toStateCode || ((value = '') => `${value}`.slice(0, 2).toUpperCase());
  const resolve = resolveCoords || defaultResolveCoords;
  const stateCode = toState(field(row, 'state', 'region', 'State', 'Region', 'state_code')) || 'US';
  const city = field(row, 'city', 'City');
  const zip = field(row, 'zip', 'Zip', 'zipcode', 'postal_code');
  const note = field(row, 'notes', 'note', 'Notes', 'Note');
  const { coords, accuracy } = resolve(row, { zip, city, stateCode, seed: idx, getField: field });
  const { display: phone, tel: phoneDial } = formatPhoneNumber(field(row, 'phone', 'Phone'));
  return {
    id: row?.id ?? `${type}-${idx}`,
    type,
    company: field(row, 'company_name', 'company', 'Company', 'name') || 'Partner',
    region: field(row, 'region', 'Region', 'state') || stateCode,
    phone,
    phoneDial,
    contact: field(row, 'contact', 'Contact', 'contact_name'),
    availability: field(row, 'availability', 'tier', 'Availability'),
    authorization: field(row, 'authorization', 'Authorization'),
    notes: note || '',
    note: note || '',
    city: city || '',
    state: stateCode,
    zip: zip || '',
    lat: coords.lat,
    lng: coords.lng,
    locationAccuracy: accuracy,
    details: row,
    seed: idx
  };
};

const normalizeVehicle = (row, idx = 0, { getField: fieldGetter, toStateCode, resolveCoords } = {}) => {
  const field = fieldGetter || getField;
  const toState = toStateCode || ((value = '') => `${value}`.slice(0, 2).toUpperCase());
  const stateCode = toState(field(row, 'State Loc', 'State', 'state', 'state_code'));
  const rawLat = parseFloat(field(row, 'Lat', 'lat'));
  const rawLng = parseFloat(field(row, 'Long', 'Lng', 'long', 'lng'));
  const hasExactCoords = Number.isFinite(rawLat) && Number.isFinite(rawLng) && rawLat !== 0 && rawLng !== 0;
  const zip = field(row, 'PT ZipCode', 'Zip', 'zip');
  const city = field(row, 'PT City', 'City', 'city');
  const coords = hasExactCoords ? { lat: rawLat, lng: rawLng } : { lat: 0, lng: 0 };
  const accuracy = hasExactCoords ? 'exact' : 'missing';
  const dealCompletion = field(row, 'Deal Completion', 'Deal completion', 'Deal completition', 'Remaining');
  const dealStatus = field(row, 'Deal Status', 'deal status');
  const vehicleStatus = field(row, 'Vehicle Status', 'vehicle status', 'vehicle_status');
  const physicalLocation = field(row, 'Physical Location', 'phys_loc', 'physical_location');
  const shortLocation = field(row, 'short_location', 'Short Location', 'short location');
  const customerName = field(
    row,
    'Customer Name',
    'customer_name',
    'Customer',
    'customer',
    'CustomerName',
    'customername',
    'Borrower Name',
    'borrower_name'
  );
  const v = {
    id: row?.id ?? idx,
    status: dealStatus || 'ACTIVE',
    dealStatus: dealStatus || 'ACTIVE',
    vehicleStatus: vehicleStatus || '',
    invPrepStatus: field(row, 'INV Prep Stat', 'Inv. Prep. Stat.', 'Inv Prep Stat'),
    physicalLocation: physicalLocation || '',
    dealCompletion,
    type: field(row, 'Unit Type', 'type') || 'Vehicle',
    year: field(row, 'Model Year', 'year'),
    model: field(row, 'Model', 'model') || 'Vehicle',
    vin: field(row, 'VIN', 'vin', 'ShortVIN') || 'N/A',
    gpsFix: field(row, 'GPS Fix', 'gps_fix'),
    gpsReason: field(row, 'GPS Fix Reason', 'gps_fix_reason'),
    gpsMoving: field(row, 'GPS Moving', 'gps_moving'),
    moving: field(row, 'Moving', 'moving'),
    movingCalc: field(row, 'Moving (Calc)', 'moving_calc'),
    ptStatus: field(row, 'PT Status', 'pt_status'),
    ptSerial: field(row, 'PT Serial ', 'PT Serial', 'pt_serial'),
    winnerSerial: field(
      row,
      'Winner Serial',
      'winner_serial',
      'Winning Serial',
      'winning_serial',
      'serial_winner',
      'Serial Winner'
    ),
    encoreSerial: field(row, 'Encore Serial', 'encore_serial'),
    firstRead: field(row, 'PT First Read', 'pt_first_read'),
    lastRead: field(row, 'PT Last Read', 'pt_last_read'),
    daysStationary: field(row, 'days_stationary', 'Days Stationary', 'Days stationary', 'Days Stationary (Calc)', 'Days Parked'),
    stockNo: field(row, 'Current Stock No', 'current_stock_no', 'Stock No', 'Stock'),
    openBalance: field(row, 'Open Balance', 'open_balance'),
    shortLocation: shortLocation || '',
    state: stateCode,
    city: city || '',
    zipcode: zip || '',
    lat: coords.lat,
    lng: coords.lng,
    locationAccuracy: accuracy,
    customerId: field(row, 'Customer ID', 'Customer', 'customer_id'),
    customerName: customerName || '',
    customer: field(row, 'PT City', 'City', 'city')
      ? `${field(row, 'PT City', 'City', 'city')}, ${stateCode || 'US'}`
      : stateCode || 'Unknown area',
    lastLocation: `${field(row, 'PT City', 'City', 'city') || 'Unknown'}, ${stateCode || 'USA'}${zip ? ' ' + zip : ''}`.trim(),
    payment: field(row, 'Payment Schedule', 'payment'),
    details: row,
  };

  const normalizedId = `${v.id ?? ''}`.trim();
  const normalizedVin = `${v.vin ?? ''}`.trim().toUpperCase();
  const normalizedStock = `${v.stockNo ?? ''}`.trim().toUpperCase();
  v.uiKey = [normalizedId, normalizedVin, normalizedStock, `${idx}`].join('::');
  v._searchBlob = `${v.model} ${v.vin} ${v.lastLocation} ${v.shortLocation} ${v.physicalLocation} ${v.customerId} ${v.customerName} ${v.customer}`.toLowerCase();

  return v;
};

export {
  formatPhoneNumber,
  getField,
  normalizeInstaller,
  normalizePartner,
  normalizeVehicle
};
