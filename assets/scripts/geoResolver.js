/**
 * Resolve approximate location from the public IP without prompting for browser permissions.
 * @returns {Promise<{lat:number, lon:number, source:"ip", city?:string, region?:string, country?:string} | null>}
 */
export const resolveLocationFromIp = async () => {
  try {
    const response = await fetch('https://ipapi.co/json/', { cache: 'no-store' });
    if (!response.ok) return null;

    const data = await response.json();
    const lat = Number(data?.latitude);
    const lon = Number(data?.longitude);

    if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;

    const city = data?.city;
    const region = data?.region;
    const country = data?.country_name || data?.country;

    return {
      lat,
      lon,
      source: 'ip',
      ...(city ? { city } : {}),
      ...(region ? { region } : {}),
      ...(country ? { country } : {}),
    };
  } catch (error) {
    return null;
  }
};

export const getCoordsIpFirst = resolveLocationFromIp;
