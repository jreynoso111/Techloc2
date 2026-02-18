const DEFAULT_STATE_CENTER = [39.8, -98.5];
const MILES_TO_METERS = 1609.34;
const HOTSPOT_RADIUS_MILES = 50;

let STATE_CENTERS = {};
const locationCache = new Map();

const STATE_CENTERS_URL = new URL('./state-centers.json', import.meta.url);

const loadStateCenters = async (url = STATE_CENTERS_URL) => {
  try {
    const response = await fetch(url);
    if (!response.ok) throw new Error(`Failed to fetch state centers: ${response.status}`);
    const data = await response.json();
    STATE_CENTERS = data || {};
  } catch (error) {
    console.error('Failed to load state centers configuration', error);
    STATE_CENTERS = {};
  }

  if (!Array.isArray(STATE_CENTERS.DEFAULT)) {
    STATE_CENTERS.DEFAULT = DEFAULT_STATE_CENTER;
  }
};

const getStateCenter = (stateCode = '') => {
  if (STATE_CENTERS[stateCode]) return STATE_CENTERS[stateCode];
  return STATE_CENTERS.DEFAULT || DEFAULT_STATE_CENTER;
};

const getDistance = (lat1, lon1, lat2, lon2) => {
  const R = 3958.8;
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLon = (lon2 - lon1) * Math.PI / 180;
  const a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
    + Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) * Math.sin(dLon / 2) * Math.sin(dLon / 2);
  return R * (2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a)));
};

const deriveCoords = ({ zip = '', city = '', stateCode = '', fallbackLatLng = null, seed = 0 }) => {
  if (fallbackLatLng && Number.isFinite(fallbackLatLng.lat) && Number.isFinite(fallbackLatLng.lng)) {
    return { lat: fallbackLatLng.lat, lng: fallbackLatLng.lng };
  }

  const base = getStateCenter(stateCode);

  const cleanZip = `${zip}`.trim();
  if (cleanZip) {
    const zipKey = `${cleanZip}-${stateCode}`.toLowerCase();
    if (locationCache.has(zipKey)) return locationCache.get(zipKey);

    const zipNumber = parseInt(cleanZip.replace(/\D/g, ''), 10);
    if (Number.isFinite(zipNumber)) {
      const angle = (zipNumber % 360) * (Math.PI / 180);
      const radius = 0.15 + ((zipNumber % 700) / 1200);
      const coords = { lat: base[0] + (radius * Math.cos(angle)), lng: base[1] + (radius * Math.sin(angle) * 1.3) };
      locationCache.set(zipKey, coords);
      return coords;
    }
  }

  const cleanCity = `${city}`.trim().toLowerCase();
  if (cleanCity) {
    const cityKey = `${cleanCity}-${stateCode}`;
    if (locationCache.has(cityKey)) return locationCache.get(cityKey);

    const cityHash = cleanCity.split('').reduce((acc, ch) => acc + ch.charCodeAt(0), seed || 1);
    const angle = (cityHash % 360) * (Math.PI / 180);
    const radius = 0.18 + ((cityHash % 500) / 1400);
    const coords = { lat: base[0] + (radius * Math.cos(angle)), lng: base[1] + (radius * Math.sin(angle) * 1.25) };
    locationCache.set(cityKey, coords);
    return coords;
  }

  return { lat: base[0], lng: base[1] };
};

const defaultGetField = (row, ...keys) => {
  for (const key of keys) {
    const value = row?.[key];
    if (value !== undefined && value !== null && String(value).trim() !== '') return value;
  }
  return '';
};

const resolveCoords = (row, {
  zip = '',
  city = '',
  stateCode = '',
  seed = 0,
  fallbackLatLng = null,
  getField = defaultGetField
} = {}) => {
  const rawLat = parseFloat(getField(row, 'lat', 'Lat', 'latitude', 'Latitude'));
  const rawLng = parseFloat(getField(row, 'lng', 'Lng', 'long', 'Long', 'longitude', 'Longitude'));
  const hasExactCoords = Number.isFinite(rawLat) && Number.isFinite(rawLng) && rawLat !== 0 && rawLng !== 0;
  if (hasExactCoords) {
    return { coords: { lat: rawLat, lng: rawLng }, hasExactCoords: true, accuracy: 'exact' };
  }

  if (fallbackLatLng && Number.isFinite(fallbackLatLng.lat) && Number.isFinite(fallbackLatLng.lng)) {
    return { coords: fallbackLatLng, hasExactCoords: true, accuracy: 'exact' };
  }

  const coordsFromZip = deriveCoords({ zip, stateCode, seed });
  if (`${zip}`.trim()) {
    return { coords: coordsFromZip, hasExactCoords: false, accuracy: 'zip' };
  }

  const coordsFromCity = deriveCoords({ city, stateCode, seed });
  if (`${city}`.trim()) {
    return { coords: coordsFromCity, hasExactCoords: false, accuracy: 'city' };
  }

  return { coords: deriveCoords({ stateCode, seed }), hasExactCoords: false, accuracy: 'state' };
};

export {
  DEFAULT_STATE_CENTER,
  HOTSPOT_RADIUS_MILES,
  MILES_TO_METERS,
  STATE_CENTERS,
  locationCache,
  deriveCoords,
  getDistance,
  loadStateCenters,
  resolveCoords
};
