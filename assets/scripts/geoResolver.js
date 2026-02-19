/**
 * Resolve approximate location from the public IP without prompting for browser permissions.
 * @returns {Promise<{lat:number, lon:number, source:"ip", city?:string, region?:string, country?:string} | null>}
 */
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
        const response = await fetch(provider.url, { cache: 'no-store' });
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
  } catch (error) {
    return null;
  }
};

export const getCoordsIpFirst = resolveLocationFromIp;
