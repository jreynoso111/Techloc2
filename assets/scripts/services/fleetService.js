const isPlainObject = (value) => Boolean(value) && typeof value === 'object' && !Array.isArray(value);

export const getVehicles = (vehiclesSource = []) => {
  if (Array.isArray(vehiclesSource)) return [...vehiclesSource];
  if (vehiclesSource instanceof Map) return Array.from(vehiclesSource.values());
  if (isPlainObject(vehiclesSource)) return Object.values(vehiclesSource);
  return [];
};

export const updateDealStatus = (
  vehicles = [],
  vehicleId,
  nextStatus,
  { idKey = 'id', statusKey = 'deal_status' } = {}
) => {
  if (!Array.isArray(vehicles)) return [];

  return vehicles.map((vehicle) => {
    if (!vehicle || `${vehicle[idKey]}` !== `${vehicleId}`) return vehicle;
    return { ...vehicle, [statusKey]: nextStatus };
  });
};
