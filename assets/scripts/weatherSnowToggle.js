import { getCoordsIpFirst } from './geoResolver.js';

const SNOW_CODES = new Set([71, 73, 75, 77, 85, 86]);
const WEATHER_URL = (lat, lon) =>
  `https://api.open-meteo.com/v1/forecast?latitude=${lat}&longitude=${lon}&current=weather_code&timezone=auto`;

export const fetchIsSnowing = async () => {
  try {
    const coords = await getCoordsIpFirst();
    if (!coords?.lat || !coords?.lon) {
      return { snowing: false, reason: null };
    }

    const response = await fetch(WEATHER_URL(coords.lat, coords.lon), { cache: 'no-store' });
    if (!response.ok) {
      return { snowing: false, reason: null };
    }

    const data = await response.json();
    const code = Number(data?.current?.weather_code);
    const snowing = SNOW_CODES.has(code);

    return {
      snowing,
      reason: snowing ? 'Snow detected in your area' : 'No snow right now',
    };
  } catch (error) {
    return { snowing: false, reason: null };
  }
};
