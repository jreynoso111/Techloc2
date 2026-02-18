export const getOriginKey = (origin) => origin ? `${Number(origin.lat).toFixed(6)},${Number(origin.lng).toFixed(6)}` : null;

export const attachDistances = (list, origin, cache, distanceCalculator) => {
  if (!origin) return [...list];
  const originKey = getOriginKey(origin);
  if (cache.originKey !== originKey) {
    cache.distances.clear();
    cache.originKey = originKey;
  }

  return list.map((item) => {
    const cached = cache.distances.get(item.id);
    if (cached !== undefined) return { ...item, distance: cached };
    const distance = distanceCalculator(item);
    cache.distances.set(item.id, distance);
    return { ...item, distance };
  });
};

export const debounce = (fn, delay = 200) => {
  let timeoutId;
  return (...args) => {
    clearTimeout(timeoutId);
    timeoutId = setTimeout(() => fn(...args), delay);
  };
};

export const debounceAsync = (fn, delay = 300) => {
  let timeoutId;
  return (...args) => new Promise((resolve, reject) => {
    clearTimeout(timeoutId);
    timeoutId = setTimeout(async () => {
      try {
        resolve(await fn(...args));
      } catch (error) {
        reject(error);
      }
    }, delay);
  });
};

export const runWithTimeout = (promise, timeoutMs, message) => new Promise((resolve, reject) => {
  const timeoutId = setTimeout(() => {
    reject(new Error(message));
  }, timeoutMs);
  promise
    .then((result) => {
      clearTimeout(timeoutId);
      resolve(result);
    })
    .catch((error) => {
      clearTimeout(timeoutId);
      reject(error);
    });
});
