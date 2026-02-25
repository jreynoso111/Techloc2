export const createVehicleMarkerIcon = (markerColor, borderColor, isStopped) => L.divIcon({
  className: 'vehicle-marker-wrapper',
  html: `<div class="vehicle-marker-icon"><span class="vehicle-marker-dot" style="background:${markerColor}; border-color:${borderColor}"></span>${isStopped ? '<span class="vehicle-cross-badge">✕</span>' : ''}</div>`,
  iconSize: [18, 18],
  iconAnchor: [9, 9]
});

export const syncVehicleMarkers = ({
  vehiclesWithCoords,
  vehicleLayer,
  vehicleMarkers,
  visible,
  getVehicleMarkerKey,
  getVehicleMarkerColor,
  getVehicleMarkerBorderColor,
  isVehicleNotMoving
}) => {
  if (!vehicleLayer) return;

  if (!visible) {
    vehicleLayer.clearLayers();
    vehicleMarkers.clear();
    return;
  }

  const activeIds = new Set();

  vehiclesWithCoords.forEach(({ vehicle, coords, focusHandler }) => {
    if (!coords) return;
    const markerKey = typeof getVehicleMarkerKey === 'function'
      ? getVehicleMarkerKey(vehicle)
      : `${vehicle?.id ?? ''}`;
    if (!markerKey) return;

    const markerColor = getVehicleMarkerColor(vehicle);
    const borderColor = getVehicleMarkerBorderColor(markerColor);
    const isStopped = isVehicleNotMoving(vehicle);
    const icon = createVehicleMarkerIcon(markerColor, borderColor, isStopped);

    const stored = vehicleMarkers.get(markerKey);
    let marker = stored?.marker;

    if (marker) {
      marker.setLatLng([coords.lat, coords.lng]);
      marker.setIcon(icon);
      marker.setZIndexOffset(isStopped ? 500 : 0);
      marker.options.vehicleData = vehicle;
      marker.options.focusHandler = focusHandler;
      marker.options.markerColor = markerColor;
      marker.options.isStopped = isStopped;
      marker.options.vehicleKey = markerKey;
      marker.options.cycleRole = 'vehicle';
      marker.options.cycleKey = `vehicle-${markerKey}`;
    } else {
      marker = L.marker(
        [coords.lat, coords.lng],
        {
          icon,
          zIndexOffset: isStopped ? 500 : 0,
          vehicleData: vehicle,
          focusHandler,
          markerColor,
          isStopped,
          vehicleKey: markerKey,
          cycleRole: 'vehicle',
          cycleKey: `vehicle-${markerKey}`
        }
      ).addTo(vehicleLayer);
      marker.on('click', (event) => {
        const activeVehicle = marker?.options?.vehicleData || vehicle;
        const activeFocusHandler = marker?.options?.focusHandler || focusHandler;
        const activeMarkerKey = `${marker?.options?.vehicleKey || markerKey}`;
        if (event?.originalEvent) {
          event.originalEvent.handledByMarker = true;
          event.originalEvent.cycleRole = 'vehicle';
          event.originalEvent.cycleKey = `vehicle-${activeMarkerKey}`;
        }
        activeFocusHandler({ event, marker, vehicle: activeVehicle });
      });
    }

    vehicleMarkers.set(markerKey, { marker });
    activeIds.add(markerKey);
  });

  [...vehicleMarkers.keys()].forEach((id) => {
    if (activeIds.has(id)) return;
    const stored = vehicleMarkers.get(id);
    if (stored?.marker) vehicleLayer.removeLayer(stored.marker);
    vehicleMarkers.delete(id);
  });
};
