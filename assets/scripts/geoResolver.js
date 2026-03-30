/**
 * Resolve approximate location from the public IP without prompting for browser permissions.
 * @returns {Promise<{lat:number, lon:number, source:"ip", city?:string, region?:string, country?:string} | null>}
 */
<<<<<<< HEAD
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
=======
const GEO_PROVIDER_TIMEOUT_MS = 4500;

const fetchGeoProvider = async (url) => {
  if (typeof fetch !== 'function') return null;

  const hasAbortController = typeof AbortController === 'function';
  const controller = hasAbortController ? new AbortController() : null;
  const timeoutId = controller
    ? setTimeout(() => controller.abort('geo-timeout'), GEO_PROVIDER_TIMEOUT_MS)
    : null;

  try {
    return await fetch(url, {
      cache: 'no-store',
      techlocSilent: true,
      headers: {
        'x-techloc-silent-request': '1',
      },
      ...(controller ? { signal: controller.signal } : {}),
    });
  } finally {
    if (timeoutId) clearTimeout(timeoutId);
  }
};

export const resolveLocationFromIp = async () => {
  const providers = [
    {
      url: 'https://ipwho.is/',
      parse: (data) => ({
        lat: Number(data?.latitude),
        lon: Number(data?.longitude),
        city: data?.city,
        region: data?.region,
        country: data?.country,
      }),
    },
    {
      url: 'https://ipapi.co/json/',
      parse: (data) => ({
        lat: Number(data?.latitude),
        lon: Number(data?.longitude),
        city: data?.city,
        region: data?.region,
        country: data?.country_name || data?.country,
      }),
    },
  ];

  try {
    for (const provider of providers) {
      try {
        const response = await fetchGeoProvider(provider.url);
        if (!response) continue;
        if (!response.ok) continue;
        const data = await response.json();
        const parsed = provider.parse(data);
        if (!Number.isFinite(parsed.lat) || !Number.isFinite(parsed.lon)) continue;

        return {
          lat: parsed.lat,
          lon: parsed.lon,
          source: 'ip',
          ...(parsed.city ? { city: parsed.city } : {}),
          ...(parsed.region ? { region: parsed.region } : {}),
          ...(parsed.country ? { country: parsed.country } : {}),
        };
      } catch (_providerError) {
        // try the next provider
      }
    }
    return null;
>>>>>>> impte
  } catch (error) {
    return null;
  }
};

export const getCoordsIpFirst = resolveLocationFromIp;
