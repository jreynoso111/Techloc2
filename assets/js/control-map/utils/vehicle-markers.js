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

    const markerColor = getVehicleMarkerColor(vehicle);
    const borderColor = getVehicleMarkerBorderColor(markerColor);
    const isStopped = isVehicleNotMoving(vehicle);
    const icon = createVehicleMarkerIcon(markerColor, borderColor, isStopped);

    const stored = vehicleMarkers.get(vehicle.id);
    let marker = stored?.marker;

    if (marker) {
      marker.setLatLng([coords.lat, coords.lng]);
      marker.setIcon(icon);
      marker.setZIndexOffset(isStopped ? 500 : 0);
      marker.options.vehicleData = vehicle;
      marker.options.focusHandler = focusHandler;
      marker.options.markerColor = markerColor;
      marker.options.isStopped = isStopped;
      marker.options.cycleRole = 'vehicle';
      marker.options.cycleKey = `vehicle-${vehicle.id}`;
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
          cycleRole: 'vehicle',
          cycleKey: `vehicle-${vehicle.id}`
        }
      ).addTo(vehicleLayer);
      marker.on('click', (event) => {
        const activeVehicle = marker?.options?.vehicleData || vehicle;
        const activeFocusHandler = marker?.options?.focusHandler || focusHandler;
        const vehicleId = activeVehicle?.id ?? vehicle?.id;
        if (event?.originalEvent) {
          event.originalEvent.handledByMarker = true;
          event.originalEvent.cycleRole = 'vehicle';
          event.originalEvent.cycleKey = `vehicle-${vehicleId}`;
        }
        activeFocusHandler({ event, marker, vehicle: activeVehicle });
      });
    }

    vehicleMarkers.set(vehicle.id, { marker });
    activeIds.add(vehicle.id);
  });

  [...vehicleMarkers.keys()].forEach((id) => {
    if (activeIds.has(id)) return;
    const stored = vehicleMarkers.get(id);
    if (stored?.marker) vehicleLayer.removeLayer(stored.marker);
    vehicleMarkers.delete(id);
  });
};
