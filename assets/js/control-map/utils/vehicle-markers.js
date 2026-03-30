const buildVehicleTruckSvg = () => `
  <svg class="vehicle-marker-truck" viewBox="0 0 28 20" aria-hidden="true" focusable="false">
    <ellipse class="vehicle-marker-shadow" cx="13.8" cy="16.9" rx="9.8" ry="1.6"></ellipse>
    <rect class="vehicle-marker-body" x="2.8" y="7" width="11.8" height="6.1" rx="1.7"></rect>
    <path class="vehicle-marker-body" d="M14.2 8h4.1c0.56 0 1.1 0.22 1.5 0.62l2.26 2.26c0.4 0.4 0.62 0.94 0.62 1.5v0.76c0 0.81-0.65 1.46-1.46 1.46h-0.98a2.63 2.63 0 0 1-5.06 0H9.78a2.63 2.63 0 0 1-5.06 0H4.26A1.26 1.26 0 0 1 3 13.34V8.26C3 7.56 3.56 7 4.26 7h9.94V8Z"></path>
    <path class="vehicle-marker-window" d="M16.2 8.9h2c0.33 0 0.65 0.13 0.88 0.36l1.45 1.45H16.2V8.9Z"></path>
    <path class="vehicle-marker-detail" d="M5.2 9.45h6.8"></path>
    <circle class="vehicle-marker-wheel" cx="7.2" cy="14.6" r="2.15"></circle>
    <circle class="vehicle-marker-wheel-core" cx="7.2" cy="14.6" r="0.92"></circle>
    <circle class="vehicle-marker-wheel" cx="17.8" cy="14.6" r="2.15"></circle>
    <circle class="vehicle-marker-wheel-core" cx="17.8" cy="14.6" r="0.92"></circle>
  </svg>
`;

const normalizeRotationDegrees = (value) => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return 0;
  const normalized = ((parsed % 360) + 360) % 360;
  return Number.isFinite(normalized) ? normalized : 0;
};

const buildMarkerRotationStyle = (rotationDegrees) => {
  const parsed = Number(rotationDegrees);
  if (!Number.isFinite(parsed)) return '';
  return `transform:rotate(${normalizeRotationDegrees(parsed - 90).toFixed(2)}deg);`;
};

export const createVehicleMarkerIcon = (
  markerColor,
  borderColor,
  isStopped,
  opacity = 1,
  rotationDegrees = null
) => L.divIcon({
  className: 'vehicle-marker-wrapper',
  html: `<div class="vehicle-marker-icon${isStopped ? ' is-stopped' : ''}" style="--vehicle-marker-fill:${markerColor}; --vehicle-marker-stroke:${borderColor}; opacity:${Math.max(0.2, Math.min(1, Number(opacity) || 1)).toFixed(2)}"><span class="vehicle-marker-truck-wrap" style="display:inline-flex;${buildMarkerRotationStyle(rotationDegrees)}transform-origin:center center;">${buildVehicleTruckSvg()}</span>${isStopped ? '<span class="vehicle-marker-overlay"><span class="vehicle-cross-badge vehicle-marker-cross">✕</span></span>' : ''}</div>`,
  iconSize: [28, 20],
  iconAnchor: [14, 10]
});

export const syncVehicleMarkers = ({
  vehiclesWithCoords,
  vehicleLayer,
  vehicleMarkers,
  visible,
  getVehicleMarkerKey,
  getVehicleMarkerColor,
  getVehicleMarkerBorderColor,
  isVehicleNotMoving,
  getVehicleMarkerOpacity,
  getVehicleMarkerHeading
}) => {
  if (!vehicleLayer) return;

  if (!visible) {
    vehicleLayer.clearLayers();
    vehicleMarkers.clear();
    return;
  }

  const previousMarkerStates = new Map(vehicleMarkers);
  const nextMarkerKeys = new Set();

  vehiclesWithCoords.forEach(({ vehicle, coords, focusHandler }) => {
    if (!coords) return;
    const markerKey = typeof getVehicleMarkerKey === 'function'
      ? getVehicleMarkerKey(vehicle)
      : `${vehicle?.id ?? ''}`;
    if (!markerKey) return;

    const markerColor = getVehicleMarkerColor(vehicle);
    const borderColor = getVehicleMarkerBorderColor(markerColor);
    const isStopped = isVehicleNotMoving(vehicle);
    const markerOpacity = typeof getVehicleMarkerOpacity === 'function'
      ? getVehicleMarkerOpacity(vehicle)
      : 1;
    const nextHeading = typeof getVehicleMarkerHeading === 'function'
      ? getVehicleMarkerHeading(vehicle)
      : null;
    const existingState = previousMarkerStates.get(markerKey) || null;
    const existingHeading = Number(existingState?.marker?.options?.markerHeading);
    const markerHeading = Number.isFinite(Number(nextHeading))
      ? Number(nextHeading)
      : (Number.isFinite(existingHeading) ? existingHeading : null);
    const icon = createVehicleMarkerIcon(markerColor, borderColor, isStopped, markerOpacity, markerHeading);

    let marker = existingState?.marker || null;
    if (marker) {
      marker.setLatLng([coords.lat, coords.lng]);
      marker.setIcon(icon);
      if (typeof marker.setZIndexOffset === 'function') {
        marker.setZIndexOffset(isStopped ? 500 : 0);
      } else {
        marker.options.zIndexOffset = isStopped ? 500 : 0;
      }
      marker.options.vehicleData = vehicle;
      marker.options.focusHandler = focusHandler;
      marker.options.markerColor = markerColor;
      marker.options.isStopped = isStopped;
      marker.options.vehicleKey = markerKey;
      marker.options.markerHeading = markerHeading;
      if (!vehicleLayer.hasLayer(marker)) {
        marker.addTo(vehicleLayer);
      }
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
          markerHeading,
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

    nextMarkerKeys.add(markerKey);
    vehicleMarkers.set(markerKey, { marker });
  });

  previousMarkerStates.forEach((state, markerKey) => {
    if (nextMarkerKeys.has(markerKey)) return;
    const marker = state?.marker;
    if (marker && vehicleLayer.hasLayer(marker)) {
      vehicleLayer.removeLayer(marker);
    }
    vehicleMarkers.delete(markerKey);
  });
};
