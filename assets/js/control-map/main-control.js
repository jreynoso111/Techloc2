import '../../scripts/authManager.js';
import { setupBackgroundManager } from '../../scripts/backgroundManager.js';
    import { getWebAdminSession } from '../../scripts/web-admin-session.js';
    import { supabase as supabaseClient } from '../supabaseClient.js';
    import { getDistance, loadStateCenters, resolveCoords, MILES_TO_METERS, HOTSPOT_RADIUS_MILES } from '../../scripts/geoUtils.js';
    import { getField, normalizeInstaller, normalizePartner, normalizeVehicle } from '../../scripts/dataMapper.js';
    import { createGpsHistoryManager } from '../../scripts/gpsHistory.js';
    import { createRepairHistoryManager } from '../../scripts/repairHistory.js';
    import { createPartnerClusterGroup } from './utils/cluster.js';
    import { attachDistances, debounce, debounceAsync, getOriginKey, runWithTimeout } from './utils/helpers.js';
    import { startLoading } from './utils/loading.js';
    import {
      escapeHTML,
      formatDateTime,
      getGpsFixLabel,
      getMovingStatus,
      getMovingLabel,
      getMovingMeta,
      getStatusCardStyles,
      getVehicleMarkerBorderColor,
      getVehicleMarkerColor,
      isVehicleNotMoving,
      normalizeFilterValue,
      normalizeKey,
      parseDealCompletion,
      toStateCode
    } from '../utils/formatters.js';
    import { ensureSupabaseSession as ensureSupabaseSessionBase, SERVICE_CATEGORY_HINTS, SERVICE_TABLE, SUPABASE_TIMEOUT_MS, TABLES } from './services/supabase.js';
    import { createControlMapApiService } from './services/apiService.js';
    import { startSupabaseKeepAlive } from './services/realtime.js';
    import { createVehicleService } from './services/vehicleService.js';
    import { vehiclePopupTemplate } from '../templates/vehiclePopup.js';
    import { VEHICLE_HEADER_LABELS, getVehicleModalHeaders, loadVehicleModalPrefs, renderVehicleModalColumnsList, saveVehicleModalPrefs } from './components/vehicle-modal.js';
    import { createLayerToggle } from './utils/layer-toggles.js';
    import { syncVehicleMarkers } from './utils/vehicle-markers.js';
    import {
      bindNavigationStorageListener,
      getSelectedVehicle,
      getServiceFilterIds,
      setSelectedVehicle,
      subscribeSelectedVehicle,
      subscribeServiceFilterIds,
    } from '../shared/navigationStore.js';
    
    // --- Base Config ---

    let map, techLayer, targetLayer, connectionLayer, serviceLayer, serviceConnectionLayer, vehicleLayer, highlightLayer, gpsTrailLayer, resellerLayer, repairLayer, customServiceLayer, hotspotLayer, blacklistLayer;
    let technicians = [];
    let blacklistSites = [];
    let hotspots = [];
    let resellers = [];
    let repairShops = [];
    let customServices = [];
    let customCategories = new Map();
    let customToggleRef = null;
    let selectedCustomCategoryKey = null;
    let serviceCacheByCategory = new Map();
    let serviceFetchPromise = null;
    let vehicles = [];
    let vehicleHeaders = [];
    let vehicleMarkersVisible = true;
    let hotspotsVisible = false;
    let blacklistMarkersVisible = false;
    let routeLinesVisible = true;
    const ROUTE_LAYER_VISIBILITY_MODE = Object.freeze({
      ALL: 'all',
      LABELS_HIDDEN: 'labels-hidden',
      HIDDEN: 'hidden'
    });
    let routeLayerVisibilityMode = ROUTE_LAYER_VISIBILITY_MODE.ALL;
    const serviceHeadersByCategory = {};
    let selectedVehicleId = null;
    let selectedVehicleKey = null;
    const checkedVehicleIds = new Set();
    const checkedVehicleClickTimes = new Map();
    const checkedVehicleClickTimesByVin = new Map();
    const checkedVehicleStateByVin = new Map();
    const VEHICLE_CLICK_HISTORY_TABLE = 'control_map_vehicle_clicks';
    const VEHICLE_FILTERS_STORAGE_KEY = 'controlMapVehicleFilters';
    const VEHICLE_FILTERS_COLLAPSED_STORAGE_KEY = 'controlMapVehicleFiltersCollapsed';
    const DEFAULT_GPS_TRAIL_POINT_LIMIT = 10;
    const MIN_GPS_TRAIL_POINT_LIMIT = 1;
    const MAX_GPS_TRAIL_POINT_LIMIT = 120;
    let syncingVehicleSelection = false;
    let selectedTechId = null;
    const vehicleMarkers = new Map();
    let sidebarStateController = null;
    let techSidebarVisible = false;
    let resellerSidebarVisible = false;
    let repairSidebarVisible = false;
    let customSidebarVisible = false;
    let lastCustomSidebarVisible = false;
    let activeLeftPanel = null;
    let lastClientLocation = null;
    let renderedTechIds = '';
    let lastOriginPoint = null;
    const serviceSearchFilters = { tech: '', reseller: '', repair: '', custom: '' };
    const serviceFilterIds = { ...getServiceFilterIds() };
    const vehicleFilters = {
      invPrep: [],
      physLoc: [],
      gpsFix: [],
      moving: [],
      dealStatus: [],
      ptStatus: [],
      dealMin: 0,
      dealMax: 100,
      trailPoints: DEFAULT_GPS_TRAIL_POINT_LIMIT,
      payKpiPositiveOnly: false,
    };
    let gpsTrailPointLimit = DEFAULT_GPS_TRAIL_POINT_LIMIT;
    let vehicleFiltersCollapsed = false;

    const getVehicleFiltersStorageKey = (userId) => `${VEHICLE_FILTERS_STORAGE_KEY}:${userId || 'anonymous'}`;

    const normalizeVehicleFilterPayload = (payload = {}) => {
      const toArray = (value) => (Array.isArray(value)
        ? value.map((item) => String(item).trim()).filter(Boolean)
        : []);

      const toBoundedNumber = (value, fallback) => {
        const parsed = Number(value);
        if (!Number.isFinite(parsed)) return fallback;
        return Math.max(0, Math.min(parsed, 100));
      };

      const min = toBoundedNumber(payload.dealMin, 0);
      const max = toBoundedNumber(payload.dealMax, 100);
      const parsedTrailPoints = Number.parseInt(payload.trailPoints, 10);
      const trailPoints = Number.isFinite(parsedTrailPoints)
        ? Math.max(MIN_GPS_TRAIL_POINT_LIMIT, Math.min(parsedTrailPoints, MAX_GPS_TRAIL_POINT_LIMIT))
        : DEFAULT_GPS_TRAIL_POINT_LIMIT;
      const payKpiPositiveOnly = Boolean(payload.payKpiPositiveOnly);

      return {
        invPrep: toArray(payload.invPrep),
        physLoc: toArray(payload.physLoc),
        gpsFix: toArray(payload.gpsFix),
        moving: toArray(payload.moving),
        dealStatus: toArray(payload.dealStatus),
        ptStatus: toArray(payload.ptStatus),
        dealMin: Math.min(min, max),
        dealMax: max,
        trailPoints,
        payKpiPositiveOnly,
      };
    };

    const applyVehicleFilterPayload = (payload = {}) => {
      const normalized = normalizeVehicleFilterPayload(payload);
      Object.assign(vehicleFilters, normalized);
      gpsTrailPointLimit = normalized.trailPoints;
    };

    const loadVehicleFilterPrefs = async () => {
      if (typeof window === 'undefined' || !window.localStorage) return;
      try {
        const userId = await getCurrentUserId();
        const raw = localStorage.getItem(getVehicleFiltersStorageKey(userId));
        if (!raw) return;
        const parsed = JSON.parse(raw);
        applyVehicleFilterPayload(parsed);
      } catch (error) {
        console.warn('Failed to load vehicle filter preferences.', error);
      }
    };

    const persistVehicleFilterPrefs = async () => {
      if (typeof window === 'undefined' || !window.localStorage) return;
      try {
        const userId = await getCurrentUserId();
        localStorage.setItem(
          getVehicleFiltersStorageKey(userId),
          JSON.stringify(normalizeVehicleFilterPayload(vehicleFilters))
        );
      } catch (error) {
        console.warn('Failed to save vehicle filter preferences.', error);
      }
    };

    const VEHICLE_FILTER_DROPDOWN_IDS = [
      { toggleId: 'filter-invprep-toggle', panelId: 'filter-invprep-panel' },
      { toggleId: 'filter-physloc-toggle', panelId: 'filter-physloc-panel' },
      { toggleId: 'filter-gps-toggle', panelId: 'filter-gps-panel' },
      { toggleId: 'filter-moving-toggle', panelId: 'filter-moving-panel' },
      { toggleId: 'filter-deal-status-toggle', panelId: 'filter-deal-status-panel' },
      { toggleId: 'filter-pt-toggle', panelId: 'filter-pt-panel' }
    ];

    const closeVehicleFilterDropdowns = () => {
      VEHICLE_FILTER_DROPDOWN_IDS.forEach(({ toggleId, panelId }) => {
        const toggle = document.getElementById(toggleId);
        const panel = document.getElementById(panelId);
        if (!toggle || !panel) return;
        panel.classList.add('hidden');
        toggle.setAttribute('aria-expanded', 'false');
      });
    };

    const loadVehicleFiltersCollapsedPref = () => {
      if (typeof window === 'undefined' || !window.localStorage) return false;
      try {
        const raw = localStorage.getItem(VEHICLE_FILTERS_COLLAPSED_STORAGE_KEY);
        return raw === '1';
      } catch (_error) {
        return false;
      }
    };

    const persistVehicleFiltersCollapsedPref = () => {
      if (typeof window === 'undefined' || !window.localStorage) return;
      try {
        localStorage.setItem(VEHICLE_FILTERS_COLLAPSED_STORAGE_KEY, vehicleFiltersCollapsed ? '1' : '0');
      } catch (_error) {
        // Non-blocking: collapse state persistence can fail silently.
      }
    };

    const applyVehicleFiltersCollapsedState = (collapsed = false, { persist = true } = {}) => {
      vehicleFiltersCollapsed = Boolean(collapsed);
      const filtersBody = document.getElementById('vehicle-filters-body');
      const collapseBtn = document.getElementById('vehicle-filters-collapse');

      if (filtersBody) {
        filtersBody.hidden = vehicleFiltersCollapsed;
        filtersBody.setAttribute('aria-hidden', String(vehicleFiltersCollapsed));
        filtersBody.classList.toggle('is-collapsed', vehicleFiltersCollapsed);
      }

      if (collapseBtn) {
        collapseBtn.textContent = vehicleFiltersCollapsed ? 'Show' : 'Hide';
        collapseBtn.setAttribute('aria-expanded', String(!vehicleFiltersCollapsed));
        collapseBtn.setAttribute(
          'aria-label',
          vehicleFiltersCollapsed ? 'Show vehicle filters' : 'Hide vehicle filters'
        );
      }

      if (vehicleFiltersCollapsed) {
        closeVehicleFilterDropdowns();
      }

      if (persist) {
        persistVehicleFiltersCollapsedPref();
      }
    };

    const selectedServiceByType = {};
    const distanceCaches = {
      tech: { originKey: null, distances: new Map() },
      partners: { originKey: null, distances: new Map() }
    };
    const OVERLAP_CYCLE_HIT_RADIUS_PX = 18;
    const OVERLAP_CYCLE_MAX_AGE_MS = 7000;
    let overlapCycleState = null;
    let parkingSpotCascadeArmed = false;

    const resetOverlapCycleState = () => {
      overlapCycleState = null;
    };

    const clearParkingSpotCascade = () => {
      parkingSpotCascadeArmed = false;
    };

    const armParkingSpotCascade = () => {
      parkingSpotCascadeArmed = true;
    };

    const hasVisibleParkingSpotPopup = () => Boolean(document.querySelector('.vehicle-history-hotspot-popup'));
    const hasVisibleRoutePointPopup = () => Boolean(document.querySelector('.gps-record-popup'));

    const getRouteToggleLabel = (mode = routeLayerVisibilityMode) => {
      if (mode === ROUTE_LAYER_VISIBILITY_MODE.ALL) return 'Hide Route Labels';
      if (mode === ROUTE_LAYER_VISIBILITY_MODE.LABELS_HIDDEN) return 'Hide Route Lines';
      return 'Show Route Lines';
    };

    const isGpsTrailDistanceLabelLayer = (layer) => {
      const className = `${layer?.options?.icon?.options?.className || ''}`.trim();
      return className.split(/\s+/).includes('gps-trail-distance');
    };

    const updateRouteLayerToggleUi = () => {
      const toggle = document.getElementById('toggle-route-lines');
      if (!toggle) return;
      const routesVisible = routeLayerVisibilityMode !== ROUTE_LAYER_VISIBILITY_MODE.HIDDEN;
      const buttonLabel = getRouteToggleLabel(routeLayerVisibilityMode);
      toggle.textContent = buttonLabel;
      toggle.classList.toggle('active', routesVisible);
      toggle.setAttribute('aria-pressed', routesVisible ? 'true' : 'false');
      toggle.setAttribute('aria-label', buttonLabel);
    };

    const applyRouteLayerVisibilityMode = () => {
      routeLinesVisible = routeLayerVisibilityMode !== ROUTE_LAYER_VISIBILITY_MODE.HIDDEN;
      const hideLabelsOnly = routeLayerVisibilityMode === ROUTE_LAYER_VISIBILITY_MODE.LABELS_HIDDEN;
      const hideEverything = routeLayerVisibilityMode === ROUTE_LAYER_VISIBILITY_MODE.HIDDEN;

      if (gpsTrailLayer && map) {
        if (hideEverything) {
          if (map.hasLayer(gpsTrailLayer)) map.removeLayer(gpsTrailLayer);
        } else if (!map.hasLayer(gpsTrailLayer)) {
          gpsTrailLayer.addTo(map);
        }

        gpsTrailLayer.eachLayer((layer) => {
          if (!isGpsTrailDistanceLabelLayer(layer)) return;
          if (typeof layer.setOpacity === 'function') {
            layer.setOpacity(hideLabelsOnly ? 0 : 1);
          }
          const layerElement = typeof layer.getElement === 'function' ? layer.getElement() : null;
          if (layerElement) {
            layerElement.style.display = hideLabelsOnly ? 'none' : '';
          }
        });
      }

      updateRouteLayerToggleUi();
    };

    const cycleRouteLayerVisibilityMode = () => {
      if (routeLayerVisibilityMode === ROUTE_LAYER_VISIBILITY_MODE.ALL) {
        routeLayerVisibilityMode = ROUTE_LAYER_VISIBILITY_MODE.LABELS_HIDDEN;
      } else if (routeLayerVisibilityMode === ROUTE_LAYER_VISIBILITY_MODE.LABELS_HIDDEN) {
        routeLayerVisibilityMode = ROUTE_LAYER_VISIBILITY_MODE.HIDDEN;
      } else {
        routeLayerVisibilityMode = ROUTE_LAYER_VISIBILITY_MODE.ALL;
      }
      applyRouteLayerVisibilityMode();
    };

    const bindRouteLayerToggle = () => {
      const toggle = document.getElementById('toggle-route-lines');
      if (!toggle) return;
      if (toggle.dataset.routeCycleBound === '1') {
        applyRouteLayerVisibilityMode();
        return;
      }
      toggle.dataset.routeCycleBound = '1';
      toggle.addEventListener('click', (event) => {
        event.preventDefault();
        cycleRouteLayerVisibilityMode();
      });
      applyRouteLayerVisibilityMode();
    };

    const getOverlapHitRadiusMeters = (latlng) => {
      if (!map || !latlng) return 45;
      const centerPoint = map.latLngToContainerPoint(latlng);
      const edgePoint = L.point(centerPoint.x + OVERLAP_CYCLE_HIT_RADIUS_PX, centerPoint.y);
      const edgeLatLng = map.containerPointToLatLng(edgePoint);
      return Math.max(12, map.distance(latlng, edgeLatLng));
    };

    const getCycleRolePriority = (role = 'other') => {
      if (role === 'vehicle') return 0;
      if (role === 'parking-spot') return 1;
      if (role === 'route-point') return 2;
      return 3;
    };

    const getLayerCycleRole = (layer) => {
      const explicitRole = `${layer?.options?.cycleRole || ''}`.trim();
      if (explicitRole) return explicitRole;

      if (layer?.options?.vehicleData) return 'vehicle';

      const popupClassName = `${layer?.getPopup?.()?.options?.className || ''}`.toLowerCase();
      if (popupClassName.includes('vehicle-popup')) return 'vehicle';
      if (popupClassName.includes('vehicle-history-hotspot-popup')) return 'parking-spot';
      if (popupClassName.includes('gps-record-popup')) return 'route-point';

      const layerClassName = `${layer?.options?.className || ''}`.toLowerCase();
      if (layerClassName.includes('vehicle-history-hotspot')) return 'parking-spot';
      if (layerClassName.includes('gps-trail-point')) return 'route-point';

      return 'other';
    };

    const getLayerCycleKey = (layer, role) => {
      const explicitKey = `${layer?.options?.cycleKey || ''}`.trim();
      if (explicitKey) return explicitKey;

      const latlng = typeof layer?.getLatLng === 'function' ? layer.getLatLng() : null;
      const coordKey = latlng ? `${latlng.lat.toFixed(6)},${latlng.lng.toFixed(6)}` : '';

      if (role === 'vehicle') {
        const vehicleId = layer?.options?.vehicleData?.id;
        if (vehicleId !== undefined && vehicleId !== null) return `vehicle-${vehicleId}`;
      }

      if (role === 'parking-spot') return `parking-${coordKey}`;
      if (role === 'route-point') return `route-${coordKey}`;
      return `layer-${L.stamp(layer)}-${coordKey}`;
    };

    const collectOverlapCycleCandidates = (latlng) => {
      if (!map || !latlng) return [];
      const hitRadiusMeters = getOverlapHitRadiusMeters(latlng);
      const candidateMap = new Map();

      const registerCandidate = (candidate) => {
        if (!candidate?.key) return;
        const existing = candidateMap.get(candidate.key);
        if (!existing) {
          candidateMap.set(candidate.key, candidate);
          return;
        }

        if (candidate.priority < existing.priority) {
          candidateMap.set(candidate.key, candidate);
          return;
        }

        if (candidate.priority === existing.priority && candidate.distanceMeters < existing.distanceMeters) {
          candidateMap.set(candidate.key, candidate);
        }
      };

      vehicleMarkers.forEach(({ marker }) => {
        if (!marker || typeof marker.getLatLng !== 'function') return;
        const markerLatLng = marker.getLatLng();
        const distanceMeters = map.distance(latlng, markerLatLng);
        if (!Number.isFinite(distanceMeters) || distanceMeters > hitRadiusMeters) return;
        const role = 'vehicle';
        const key = `${marker?.options?.cycleKey || getLayerCycleKey(marker, role)}`;
        const vehicle = marker?.options?.vehicleData || null;
        const vehicleKey = `${marker?.options?.vehicleKey || getVehicleKey(vehicle)}`;
        registerCandidate({
          key,
          role,
          priority: getCycleRolePriority(role),
          distanceMeters,
          layer: marker,
          vehicle,
          vehicleId: vehicle?.id ?? null,
          vehicleKey
        });
      });

      const collectFromLayerGroup = (layerGroup) => {
        if (!layerGroup?.eachLayer) return;
        layerGroup.eachLayer((layer) => {
          if (!layer || typeof layer.getLatLng !== 'function') return;
          if (layer?.options?.interactive === false) return;
          const hasPopup = !!layer.getPopup?.();
          if (!hasPopup && !layer?.options?.vehicleData) return;

          const layerLatLng = layer.getLatLng();
          const distanceMeters = map.distance(latlng, layerLatLng);
          if (!Number.isFinite(distanceMeters) || distanceMeters > hitRadiusMeters) return;

          const role = getLayerCycleRole(layer);
          const key = getLayerCycleKey(layer, role);
          registerCandidate({
            key,
            role,
            priority: getCycleRolePriority(role),
            distanceMeters,
            layer,
            vehicle: layer?.options?.vehicleData || null,
            vehicleId: layer?.options?.vehicleData?.id ?? null,
            vehicleKey: layer?.options?.vehicleData
              ? `${layer?.options?.vehicleKey || getVehicleKey(layer?.options?.vehicleData || null)}`
              : ''
          });
        });
      };

      collectFromLayerGroup(highlightLayer);
      collectFromLayerGroup(gpsTrailLayer);

      return Array.from(candidateMap.values())
        .sort((a, b) => {
          if (a.priority !== b.priority) return a.priority - b.priority;
          return a.distanceMeters - b.distanceMeters;
        });
    };

    const activateOverlapCycleCandidate = (candidate) => {
      if (!candidate) return false;

      if (candidate.role === 'vehicle') {
        const targetVehicle = candidate.vehicle
          || vehicles.find((vehicle) => `${vehicle.id}` === `${candidate.vehicleId}`);
        if (!targetVehicle || !hasValidCoords(targetVehicle)) return false;
        const targetVehicleKey = `${candidate?.vehicleKey || getVehicleKey(targetVehicle)}`;
        const matchesSelectedVehicle = (
          `${selectedVehicleId ?? ''}` === `${targetVehicle.id ?? ''}`
          && (!selectedVehicleKey || `${selectedVehicleKey}` === targetVehicleKey)
        );
        if (matchesSelectedVehicle) {
          if (candidate.layer && typeof candidate.layer.openPopup === 'function') {
            candidate.layer.openPopup();
          }
          return true;
        }
        applySelection(targetVehicle.id, null, targetVehicleKey);
        focusVehicle(targetVehicle);
        return true;
      }

      if (candidate.layer && typeof candidate.layer.openPopup === 'function') {
        candidate.layer.openPopup();
        return true;
      }

      return false;
    };

    const cycleOverlappingTargetsAtLatLng = (latlng, originalEvent = null) => {
      if (!map || !latlng || selectedVehicleId === null) return false;
      const candidates = collectOverlapCycleCandidates(latlng);
      if (candidates.length < 2) {
        resetOverlapCycleState();
        return false;
      }

      const signature = candidates.map((candidate) => candidate.key).join('|');
      const now = Date.now();
      const previous = overlapCycleState;
      const previousLatLng = previous
        ? L.latLng(previous.lat, previous.lng)
        : null;
      const sameSequence = Boolean(
        previous
        && previous.signature === signature
        && previousLatLng
        && map.distance(latlng, previousLatLng) <= previous.hitRadiusMeters
        && (now - previous.timestamp) <= OVERLAP_CYCLE_MAX_AGE_MS
      );

      let nextIndex = 0;
      if (sameSequence) {
        nextIndex = (previous.index + 1) % candidates.length;
      } else {
        const clickedKey = `${originalEvent?.cycleKey || ''}`.trim();
        const clickedRole = `${originalEvent?.cycleRole || ''}`.trim();
        const vehicleSelectionChanged = Boolean(originalEvent?.vehicleSelectionChanged);
        const clickedIndex = clickedKey
          ? candidates.findIndex((candidate) => candidate.key === clickedKey)
          : -1;
        const selectedVehicleIndex = candidates.findIndex((candidate) =>
          candidate.role === 'vehicle'
          && (
            (selectedVehicleKey && `${candidate.vehicleKey ?? ''}` === `${selectedVehicleKey}`)
            || (!selectedVehicleKey && `${candidate.vehicle?.id ?? candidate.vehicleId ?? ''}` === `${selectedVehicleId}`)
          )
        );
        if (vehicleSelectionChanged) {
          nextIndex = selectedVehicleIndex >= 0
            ? selectedVehicleIndex
            : (clickedIndex >= 0 ? clickedIndex : 0);
        } else if (clickedRole === 'vehicle' && selectedVehicleIndex >= 0 && candidates.length > 1) {
          nextIndex = (selectedVehicleIndex + 1) % candidates.length;
        } else if (clickedIndex >= 0) {
          nextIndex = clickedIndex;
        } else {
          nextIndex = selectedVehicleIndex >= 0 ? selectedVehicleIndex : 0;
        }
      }

      const targetCandidate = candidates[nextIndex];
      if (!activateOverlapCycleCandidate(targetCandidate)) return false;

      overlapCycleState = {
        signature,
        index: nextIndex,
        lat: latlng.lat,
        lng: latlng.lng,
        hitRadiusMeters: getOverlapHitRadiusMeters(latlng),
        timestamp: now
      };
      return true;
    };

    const ensureSupabaseSession = () => ensureSupabaseSessionBase(supabaseClient);
    const vehicleService = createVehicleService({ client: supabaseClient, tableName: TABLES.vehicles });

    const areDepsEqual = (next = [], prev = []) =>
      next.length === prev.length && next.every((value, index) => Object.is(value, prev[index]));

    const createCallbackMemo = () => {
      const cache = new Map();
      return (key, factory, deps = []) => {
        const previous = cache.get(key);
        if (previous && areDepsEqual(deps, previous.deps)) return previous.fn;
        const fn = factory();
        cache.set(key, { deps: [...deps], fn });
        return fn;
      };
    };

    const useCallback = createCallbackMemo();

    const parseNumber = (value) => {
      if (value === undefined || value === null) return null;
      const cleaned = typeof value === 'string' ? value.replace(/[$,]/g, '').trim() : value;
      if (cleaned === '') return null;
      const num = Number(cleaned);
      return Number.isFinite(num) ? num : null;
    };

    const normalizeStockNumber = (value) => {
      if (value === undefined || value === null) return '';
      const normalized = String(value).trim().toUpperCase();
      return normalized === '' ? '' : normalized;
    };


    const normalizeVin = (value) => String(value || '').trim().toUpperCase();

    const getVehicleVin = (vehicle) => normalizeVin(
      getField(vehicle?.details || {}, 'VIN', 'Vin', 'vin')
      || vehicle?.VIN
      || vehicle?.vin
      || ''
    );

    const getVehicleKey = (vehicle = {}) => {
      const explicitKey = `${vehicle?.uiKey || ''}`.trim();
      if (explicitKey) return explicitKey;
      const idPart = `${vehicle?.id ?? ''}`.trim();
      const vinPart = normalizeVin(vehicle?.vin);
      const stockPart = normalizeStockNumber(vehicle?.stockNo);
      return [idPart, vinPart, stockPart].join('::');
    };

    const findVehicleByKey = (vehicleKey = '') => {
      const normalizedKey = `${vehicleKey || ''}`.trim();
      if (!normalizedKey) return null;
      return vehicles.find((vehicle) => getVehicleKey(vehicle) === normalizedKey) || null;
    };

    const getSelectedVehicleEntry = () => {
      if (selectedVehicleKey) {
        const byKey = findVehicleByKey(selectedVehicleKey);
        if (byKey) return byKey;
      }
      if (selectedVehicleId === null) return null;
      return vehicles.find((vehicle) => `${vehicle.id}` === `${selectedVehicleId}`) || null;
    };

    const getCurrentUserId = async () => {
      if (!supabaseClient?.auth?.getSession) return null;
      try {
        await ensureSupabaseSession();
      } catch (_) {
        // keep fallback below; auth state can still resolve through an existing session
      }

      const { data, error } = await supabaseClient.auth.getSession();
      if (error) return null;
      return data?.session?.user?.id || null;
    };

    const hasAuthenticatedAccess = async () => {
      return Boolean(window.currentUserRole || getWebAdminSession());
    };

    const isAuthenticatedCached = () => {
      return Boolean(window.currentUserRole || getWebAdminSession());
    };

    const notifyAuthRequired = () => {
      if (typeof window.reportGlobalIssue === 'function') {
        window.reportGlobalIssue(
          'Authentication Required',
          'Repair History is available only for logged users.',
          'Sign in first to view or edit repair history.'
        );
        return;
      }
      window.alert('Repair History is available only for logged users.');
    };

    const hydrateVehicleClickHistory = async (vehicleRows = []) => {
      if (!supabaseClient?.from || !Array.isArray(vehicleRows) || !vehicleRows.length) return;
      const userId = await getCurrentUserId();
      if (!userId) return;

      const vins = Array.from(new Set(vehicleRows.map((vehicle) => getVehicleVin(vehicle)).filter(Boolean)));
      if (!vins.length) return;

      const queryVins = Array.from(new Set(vins.flatMap((vin) => [vin, vin.toLowerCase()])));
      const { data, error } = await supabaseClient
        .from(VEHICLE_CLICK_HISTORY_TABLE)
        .select('vin, clicked_at, metadata')
        .eq('user_id', userId)
        .in('vin', queryVins)
        .order('clicked_at', { ascending: false });

      if (error || !Array.isArray(data)) return;

      const latestByVin = new Map();
      data.forEach((row) => {
        const vin = normalizeVin(row?.vin);
        const clickedAt = String(row?.clicked_at || '').trim();
        if (!vin || !clickedAt || latestByVin.has(vin)) return;
        latestByVin.set(vin, {
          clickedAt,
          checked: row?.metadata?.checked === true,
        });
      });

      latestByVin.forEach((entry, vin) => {
        checkedVehicleClickTimesByVin.set(vin, entry.clickedAt);
        checkedVehicleStateByVin.set(vin, entry.checked);
      });

      vehicleRows.forEach((vehicle) => {
        const vin = getVehicleVin(vehicle);
        const entry = vin ? latestByVin.get(vin) : null;
        if (!entry) return;
        checkedVehicleClickTimes.set(vehicle.id, entry.clickedAt);
        if (entry.checked) checkedVehicleIds.add(vehicle.id);
        else checkedVehicleIds.delete(vehicle.id);
      });
    };

    const saveVehicleClickHistory = async (vehicle, clickedAtIso, isChecked) => {
      if (!supabaseClient?.from || !vehicle || !clickedAtIso || typeof isChecked !== 'boolean') return;
      const userId = await getCurrentUserId();
      const vin = getVehicleVin(vehicle);
      if (!userId || !vin) return;
      const { error } = await supabaseClient
        .from(VEHICLE_CLICK_HISTORY_TABLE)
        .insert({
          user_id: userId,
          vin,
          clicked_at: clickedAtIso,
          source: 'vehicle_select_checkbox',
          page: 'control-map',
          action: 'toggle_on_rev',
          metadata: { checked: isChecked },
        });
      if (error) {
        console.warn('Vehicle click history save warning: ' + (error?.message || error));
      }
    };

    const formatPayKpi = (value) => {
      if (value === null || value === undefined || !Number.isFinite(value)) return '—';
      return value.toFixed(2);
    };
    const formatInvoiceDate = (value) => {
      if (!value) return '—';
      const parsed = new Date(value);
      if (Number.isNaN(parsed.getTime())) return '—';
      return parsed.toLocaleDateString('en-US', {
        month: '2-digit',
        day: '2-digit',
        year: '2-digit'
      });
    };
    let oldestOpenInvoiceLookupWarningShown = false;
    const GPS_READ_BOUNDS_CACHE_TTL_MS = 10 * 60 * 1000;
    let gpsReadBoundsByVinCache = new Map();
    let gpsReadBoundsByVinCacheUpdatedAt = 0;
    let gpsReadBoundsByVinPending = null;

    const fetchDealsByStockNumbers = async (stockNumbers = []) => {
      if (!supabaseClient?.from || !stockNumbers.length) return new Map();
      const unique = Array.from(new Set(stockNumbers.filter(Boolean)));
      if (!unique.length) return new Map();

      const stockNoColumn = '"Current Stock No"';
      const chunkSize = 800;
      const results = new Map();
      for (let i = 0; i < unique.length; i += chunkSize) {
        const chunk = unique.slice(i, i + chunkSize);
        const { data, error } = await runWithTimeout(
          supabaseClient
            .from(TABLES.deals)
            .select('"Current Stock No","Regular Amount","Open Balance","Vehicle Status"')
            .in(stockNoColumn, chunk),
          8000,
          'Database communication error.'
        );
        if (error) throw error;
        (data || []).forEach((row) => {
          const stockNo = normalizeStockNumber(row?.['Current Stock No']);
          const regularAmount = parseNumber(row?.['Regular Amount']);
          const openBalance = parseNumber(row?.['Open Balance']);
          const vehicleStatus = String(row?.['Vehicle Status'] ?? '').trim();
          if (!stockNo || regularAmount === null || openBalance === null) return;
          results.set(stockNo, { regularAmount, openBalance, vehicleStatus });
        });
      }
      return results;
    };

    const fetchOpenInvoiceSummaryByStockNumbers = async (stockNumbers = []) => {
      if (!supabaseClient?.from || !stockNumbers.length) return new Map();
      const unique = Array.from(new Set(stockNumbers.filter(Boolean)));
      if (!unique.length) return new Map();
      const tableCandidates = Array.from(new Set([
        `${TABLES.invoices || ''}`.trim(),
        'NS-Invoices&pays'
      ].filter(Boolean)));
      const stockKeyCandidates = ['Current Stock No', 'current_stock_no', 'Stock No', 'stock_no', 'stock'];
      const remainingKeyCandidates = ['Amount Remaining', 'amount_remaining', 'Remaining Balance', 'remaining_balance', 'Open Balance', 'open_balance', 'remaining'];
      const dateKeyCandidates = ['Date', 'date', 'Invoice Date', 'invoice_date', 'created_at', 'updated_at'];
      const normalizeName = (value = '') => `${value}`.trim().toLowerCase();
      const quoteColumn = (column = '') => {
        const normalized = `${column || ''}`.trim();
        if (!normalized) return '';
        if (normalized.startsWith('"') && normalized.endsWith('"')) return normalized;
        if (/^[a-z_][a-z0-9_]*$/i.test(normalized)) return normalized;
        return `"${normalized.replace(/"/g, '""')}"`;
      };
      const resolveColumnKey = (availableKeys = [], candidates = []) => {
        if (!Array.isArray(availableKeys) || !availableKeys.length) return '';
        const lookup = new Map(availableKeys.map((key) => [normalizeName(key), key]));
        for (const candidate of candidates) {
          const hit = lookup.get(normalizeName(candidate));
          if (hit) return hit;
        }
        return '';
      };

      const chunkSize = 500;
      const pageSize = 1000;
      for (const tableName of tableCandidates) {
        try {
          const probe = await runWithTimeout(
            supabaseClient.from(tableName).select('*').limit(1),
            8000,
            'Invoice probe query timed out.'
          );
          if (probe.error) throw probe.error;
          const probeRows = Array.isArray(probe.data) ? probe.data : [];
          const availableKeys = probeRows.length ? Object.keys(probeRows[0] || {}) : [];
          if (!availableKeys.length) return new Map();

          const stockKey = resolveColumnKey(availableKeys, stockKeyCandidates);
          const remainingKey = resolveColumnKey(availableKeys, remainingKeyCandidates);
          const dateKey = resolveColumnKey(availableKeys, dateKeyCandidates);

          if (!stockKey || !remainingKey || !dateKey) return new Map();

          const stockColumn = quoteColumn(stockKey);
          const remainingColumn = quoteColumn(remainingKey);
          const dateColumn = quoteColumn(dateKey);
          const selectColumns = [stockColumn, dateColumn, remainingColumn].filter(Boolean).join(',');
          const results = new Map();

          for (let i = 0; i < unique.length; i += chunkSize) {
            const chunk = unique.slice(i, i + chunkSize);
            let offset = 0;
            let hasMore = true;

            while (hasMore) {
              let query = supabaseClient
                .from(tableName)
                .select(selectColumns)
                .in(stockColumn, chunk)
                .range(offset, offset + pageSize - 1);

              if (remainingColumn) {
                query = query.gt(remainingColumn, 0);
              }

              const { data, error } = await runWithTimeout(
                query,
                8000,
                'Invoice query timed out.'
              );
              if (error) throw error;

              const pageRows = Array.isArray(data) ? data : [];
              pageRows.forEach((row) => {
                const stockNo = normalizeStockNumber(row?.[stockKey]);
                if (!stockNo) return;

                const remaining = parseNumber(row?.[remainingKey]);
                if (remainingColumn && (!Number.isFinite(remaining) || remaining <= 0)) return;

                const dateRaw = row?.[dateKey];
                if (!dateRaw) return;
                const timestamp = Date.parse(dateRaw);
                if (Number.isNaN(timestamp)) return;

                const existing = results.get(stockNo) || {
                  timestamp: null,
                  value: '',
                  openBalanceSum: 0
                };
                existing.openBalanceSum += remaining;
                if (existing.timestamp === null || timestamp < existing.timestamp) {
                  existing.timestamp = timestamp;
                  existing.value = new Date(timestamp).toISOString();
                }
                results.set(stockNo, existing);
              });

              hasMore = pageRows.length === pageSize;
              offset += pageSize;
            }
          }

          return results;
        } catch (lastError) {
          const code = `${lastError?.code || ''}`.trim();
          const message = `${lastError?.message || ''}`.toLowerCase();
          const missingTable = code === 'PGRST205' || message.includes('could not find the table');
          if (missingTable) {
            continue;
          }
          if (!oldestOpenInvoiceLookupWarningShown) {
            oldestOpenInvoiceLookupWarningShown = true;
            console.warn('Oldest open invoice lookup warning: ' + (lastError?.message || lastError));
          }
          throw lastError;
        }
      }

      if (!oldestOpenInvoiceLookupWarningShown) {
        oldestOpenInvoiceLookupWarningShown = true;
        console.warn('Oldest open invoice lookup warning: invoice table not found.');
      }
      throw new Error('Invoice table not found.');
    };

    const normalizeVehicleIdKey = (value) => `${value ?? ''}`.trim();
    const normalizeVinKey = (value) => `${value ?? ''}`.trim().toUpperCase();
    const normalizeVinComparableKey = (value) => normalizeVinKey(value).replace(/[^A-Z0-9]/g, '');
    const getVinSuffixKey = (value) => {
      const normalized = normalizeVinComparableKey(value);
      if (!normalized) return '';
      return normalized.slice(-6);
    };
    const getVehicleVinSuffixKey = (vehicle = {}) => {
      const candidates = [
        vehicle?.shortVin,
        vehicle?.shortvin,
        vehicle?.details?.shortvin,
        vehicle?.details?.ShortVIN,
        vehicle?.details?.['ShortVIN'],
        vehicle?.details?.['Short Vin'],
        getVehicleVin(vehicle),
        vehicle?.vin,
        vehicle?.details?.VIN
      ];
      for (const candidate of candidates) {
        const suffix = getVinSuffixKey(candidate);
        if (suffix) return suffix;
      }
      return '';
    };
    const parseGpsReadBoundsTimestamp = (record = {}) => {
      const candidates = [
        record?.Date,
        record?.['Date'],
        record?.gps_time,
        record?.created_at,
        record?.updated_at
      ];
      for (const candidate of candidates) {
        if (!candidate) continue;
        const parsed = Date.parse(candidate);
        if (Number.isFinite(parsed)) return parsed;
      }
      return Number.NaN;
    };

    const updateGpsReadTimestampBounds = (targetMap, key, timestampMs) => {
      if (!(targetMap instanceof Map) || !key || !Number.isFinite(timestampMs)) return;
      const current = targetMap.get(key) || {
        firstTimestamp: Number.POSITIVE_INFINITY,
        lastTimestamp: Number.NEGATIVE_INFINITY
      };
      if (timestampMs < current.firstTimestamp) current.firstTimestamp = timestampMs;
      if (timestampMs > current.lastTimestamp) current.lastTimestamp = timestampMs;
      targetMap.set(key, current);
    };

    const normalizeGpsReadBoundsMap = (rawBounds = new Map()) => {
      const normalized = new Map();
      if (!(rawBounds instanceof Map)) return normalized;
      rawBounds.forEach((value, key) => {
        const firstRead = Number.isFinite(value?.firstTimestamp)
          ? new Date(value.firstTimestamp).toISOString()
          : '';
        const lastRead = Number.isFinite(value?.lastTimestamp)
          ? new Date(value.lastTimestamp).toISOString()
          : '';
        if (firstRead || lastRead) {
          normalized.set(key, { firstRead, lastRead });
        }
      });
      return normalized;
    };

    const fetchGpsReadBoundsByVehicleIds = async (vehicleIds = []) => {
      if (!supabaseClient?.from || !Array.isArray(vehicleIds) || !vehicleIds.length) return new Map();
      await ensureSupabaseSession();
      const uniqueVehicleIds = Array.from(new Set(
        vehicleIds.map((value) => normalizeVehicleIdKey(value)).filter(Boolean)
      ));
      if (!uniqueVehicleIds.length) return new Map();

      const pageSize = 1000;
      const idChunkSize = 20;
      const gpsReadBoundsTimeoutMs = 20000;
      const boundsByVehicleId = new Map();

      for (let index = 0; index < uniqueVehicleIds.length; index += idChunkSize) {
        const idChunk = uniqueVehicleIds.slice(index, index + idChunkSize);
        let offset = 0;
        let hasMore = true;
        while (hasMore) {
          const query = supabaseClient
            .from(TABLES.gpsHistory)
            .select('*')
            .in('vehicle_id', idChunk)
            .range(offset, offset + pageSize - 1);

          const { data, error } = await runWithTimeout(
            query,
            gpsReadBoundsTimeoutMs,
            'GPS read-bound lookup timed out.'
          );
          if (error) throw error;

          const rows = Array.isArray(data) ? data : [];
          rows.forEach((row) => {
            const vehicleId = normalizeVehicleIdKey(row?.vehicle_id);
            if (!vehicleId) return;
            const timestamp = parseGpsReadBoundsTimestamp(row);
            if (!Number.isFinite(timestamp)) return;
            updateGpsReadTimestampBounds(boundsByVehicleId, vehicleId, timestamp);
          });

          hasMore = rows.length === pageSize;
          offset += pageSize;
        }
      }

      return normalizeGpsReadBoundsMap(boundsByVehicleId);
    };

    const fetchGpsReadBoundsByVins = async (vins = []) => {
      if (!supabaseClient?.from || !Array.isArray(vins) || !vins.length) return new Map();
      await ensureSupabaseSession();
      const uniqueVins = Array.from(new Set(vins.map((value) => normalizeVinKey(value)).filter(Boolean)));
      if (!uniqueVins.length) return new Map();

      const pageSize = 1000;
      const vinChunkSize = 25;
      const gpsReadBoundsTimeoutMs = 20000;
      const boundsByVin = new Map();

      for (let index = 0; index < uniqueVins.length; index += vinChunkSize) {
        const vinChunk = uniqueVins.slice(index, index + vinChunkSize);
        let offset = 0;
        let hasMore = true;
        while (hasMore) {
          const query = supabaseClient
            .from(TABLES.gpsHistory)
            .select('*')
            .in('VIN', vinChunk)
            .range(offset, offset + pageSize - 1);

          const { data, error } = await runWithTimeout(
            query,
            gpsReadBoundsTimeoutMs,
            'GPS read-bound VIN lookup timed out.'
          );
          if (error) throw error;

          const rows = Array.isArray(data) ? data : [];
          rows.forEach((row) => {
            const vin = normalizeVinKey(row?.VIN ?? row?.vin);
            if (!vin) return;
            const timestamp = parseGpsReadBoundsTimestamp(row);
            if (!Number.isFinite(timestamp)) return;
            updateGpsReadTimestampBounds(boundsByVin, vin, timestamp);
          });

          hasMore = rows.length === pageSize;
          offset += pageSize;
        }
      }

      return normalizeGpsReadBoundsMap(boundsByVin);
    };

    const fetchGpsReadBoundsByVinSuffixes = async (vinSuffixes = []) => {
      if (!supabaseClient?.from || !Array.isArray(vinSuffixes) || !vinSuffixes.length) return new Map();
      await ensureSupabaseSession();

      const uniqueSuffixes = Array.from(new Set(
        vinSuffixes
          .map((value) => getVinSuffixKey(value))
          .filter((value) => value.length === 6)
      ));
      if (!uniqueSuffixes.length) return new Map();

      const targetSuffixes = new Set(uniqueSuffixes);
      const pageSize = 1000;
      const gpsReadBoundsTimeoutMs = 20000;
      const boundsBySuffix = new Map();

      let offset = 0;
      let hasMore = true;
      while (hasMore) {
        const query = supabaseClient
          .from(TABLES.gpsHistory)
          .select('VIN,Date')
          .not('VIN', 'is', null)
          .range(offset, offset + pageSize - 1);

        const { data, error } = await runWithTimeout(
          query,
          gpsReadBoundsTimeoutMs,
          'GPS read-bound VIN suffix lookup timed out.'
        );
        if (error) throw error;

        const rows = Array.isArray(data) ? data : [];
        rows.forEach((row) => {
          const suffix = getVinSuffixKey(row?.VIN ?? row?.vin);
          if (!suffix || !targetSuffixes.has(suffix)) return;
          const timestamp = parseGpsReadBoundsTimestamp(row);
          if (!Number.isFinite(timestamp)) return;
          updateGpsReadTimestampBounds(boundsBySuffix, suffix, timestamp);
        });

        hasMore = rows.length === pageSize;
        offset += pageSize;
      }

      return normalizeGpsReadBoundsMap(boundsBySuffix);
    };

    const hasParsableTime = (value) => {
      if (!value) return false;
      return Number.isFinite(Date.parse(value));
    };

    const fetchGpsReadBoundsByHistoryManager = async (vehicleList = [], { concurrency = 3 } = {}) => {
      if (!Array.isArray(vehicleList) || !vehicleList.length) return new Map();
      if (!gpsHistoryManager || typeof gpsHistoryManager.fetchGpsHistory !== 'function') return new Map();

      const results = new Map();
      const pendingVehicles = vehicleList.filter((vehicle) => {
        const vehicleId = normalizeVehicleIdKey(vehicle?.id);
        return Boolean(vehicleId) && (!hasParsableTime(vehicle?.firstRead) || !hasParsableTime(vehicle?.lastRead));
      });
      if (!pendingVehicles.length) return results;

      const workerCount = Math.max(1, Math.min(Number(concurrency) || 3, pendingVehicles.length));
      let cursor = 0;
      const runWorker = async () => {
        while (cursor < pendingVehicles.length) {
          const index = cursor;
          cursor += 1;
          const vehicle = pendingVehicles[index];
          const vehicleId = normalizeVehicleIdKey(vehicle?.id);
          if (!vehicleId) continue;
          const vin = getVehicleVin(vehicle);
          const historyVehicleId = normalizeVehicleIdKey(gpsHistoryManager.getVehicleId(vehicle) || vehicleId);
          if (!vin && !historyVehicleId) continue;
          try {
            const { records, error } = await gpsHistoryManager.fetchGpsHistory({
              vin,
              vehicleId: historyVehicleId
            });
            if (error || !Array.isArray(records) || !records.length) continue;
            const bounds = { firstTimestamp: Number.POSITIVE_INFINITY, lastTimestamp: Number.NEGATIVE_INFINITY };
            records.forEach((record) => {
              const timestamp = parseGpsReadBoundsTimestamp(record);
              if (!Number.isFinite(timestamp)) return;
              if (timestamp < bounds.firstTimestamp) bounds.firstTimestamp = timestamp;
              if (timestamp > bounds.lastTimestamp) bounds.lastTimestamp = timestamp;
            });
            if (!Number.isFinite(bounds.firstTimestamp) && !Number.isFinite(bounds.lastTimestamp)) continue;
            results.set(vehicleId, {
              firstRead: Number.isFinite(bounds.firstTimestamp) ? new Date(bounds.firstTimestamp).toISOString() : '',
              lastRead: Number.isFinite(bounds.lastTimestamp) ? new Date(bounds.lastTimestamp).toISOString() : ''
            });
          } catch (_error) {
            // Best-effort fallback path.
          }
        }
      };

      await Promise.all(Array.from({ length: workerCount }, () => runWorker()));
      return results;
    };

    const fetchVehicleFirstReadsByTargetedQueries = async (vehicleList = [], { concurrency = 6 } = {}) => {
      if (!supabaseClient?.from || !Array.isArray(vehicleList) || !vehicleList.length) return new Map();
      await ensureSupabaseSession();

      const normalizeSerialKey = (value = '') => `${value ?? ''}`.trim().toUpperCase();
      const resolveVehicleWinnerSerial = (vehicle = {}) => {
        const managerSerial = typeof gpsHistoryManager?.getVehicleWinnerSerial === 'function'
          ? normalizeSerialKey(gpsHistoryManager.getVehicleWinnerSerial(vehicle))
          : '';
        if (managerSerial) return managerSerial;
        const details = vehicle?.details || {};
        const candidates = [
          vehicle?.winnerSerial,
          vehicle?.winningSerial,
          vehicle?.serialWinner,
          details?.winner_serial,
          details?.winning_serial,
          details?.serial_winner,
          details?.['Winner Serial'],
          details?.['Winning Serial'],
          vehicle?.ptSerial,
          details?.['PT Serial'],
          details?.['PT Serial '],
          details?.pt_serial,
          vehicle?.encoreSerial,
          details?.['Encore Serial'],
          details?.encore_serial,
        ];
        for (const candidate of candidates) {
          const normalized = normalizeSerialKey(candidate);
          if (normalized) return normalized;
        }
        return '';
      };

      const getFirstTimestampFromRows = (rows = []) => {
        if (!Array.isArray(rows) || !rows.length) return Number.NaN;
        let firstTimestamp = Number.POSITIVE_INFINITY;
        rows.forEach((row) => {
          const timestamp = parseGpsReadBoundsTimestamp(row);
          if (!Number.isFinite(timestamp)) return;
          if (timestamp < firstTimestamp) firstTimestamp = timestamp;
        });
        return Number.isFinite(firstTimestamp) ? firstTimestamp : Number.NaN;
      };

      const pendingVehicles = vehicleList.filter((vehicle) => {
        const vehicleId = normalizeVehicleIdKey(vehicle?.id);
        return Boolean(vehicleId);
      });
      if (!pendingVehicles.length) return new Map();

      const results = new Map();
      const workerCount = Math.max(1, Math.min(Number(concurrency) || 6, pendingVehicles.length));
      let cursor = 0;

      const runWorker = async () => {
        while (cursor < pendingVehicles.length) {
          const index = cursor;
          cursor += 1;
          const vehicle = pendingVehicles[index];
          const vehicleId = normalizeVehicleIdKey(vehicle?.id);
          if (!vehicleId) continue;

          let firstTimestamp = Number.NaN;
          try {
            const firstByIdQuery = supabaseClient
              .from(TABLES.gpsHistory)
              .select('Date')
              .eq('vehicle_id', vehicleId)
              .not('Date', 'is', null)
              .order('Date', { ascending: true })
              .limit(1);
            const { data: firstByIdData, error: firstByIdError } = await runWithTimeout(
              firstByIdQuery,
              12000,
              'GPS first-read by vehicle_id lookup timed out.'
            );
            if (!firstByIdError) {
              firstTimestamp = getFirstTimestampFromRows(firstByIdData);
            }
          } catch (_error) {
            // fallback path below
          }

          const vinSuffix = getVehicleVinSuffixKey(vehicle);
          const winnerSerial = resolveVehicleWinnerSerial(vehicle);
          if (!Number.isFinite(firstTimestamp) && vinSuffix) {
            if (winnerSerial) {
              try {
                const firstBySuffixWinnerQuery = supabaseClient
                  .from(TABLES.gpsHistory)
                  .select('Date,VIN,Serial')
                  .ilike('VIN', `%${vinSuffix}`)
                  .eq('Serial', winnerSerial)
                  .not('Date', 'is', null)
                  .order('Date', { ascending: true })
                  .limit(1);
                const { data: firstBySuffixWinnerData, error: firstBySuffixWinnerError } = await runWithTimeout(
                  firstBySuffixWinnerQuery,
                  12000,
                  'GPS first-read by winner serial lookup timed out.'
                );
                if (!firstBySuffixWinnerError) {
                  firstTimestamp = getFirstTimestampFromRows(firstBySuffixWinnerData);
                }
              } catch (_error) {
                // fallback without winner serial
              }
            }

            if (!Number.isFinite(firstTimestamp)) {
              try {
                const firstBySuffixQuery = supabaseClient
                  .from(TABLES.gpsHistory)
                  .select('Date,VIN')
                  .ilike('VIN', `%${vinSuffix}`)
                  .not('Date', 'is', null)
                  .order('Date', { ascending: true })
                  .limit(1);
                const { data: firstBySuffixData, error: firstBySuffixError } = await runWithTimeout(
                  firstBySuffixQuery,
                  12000,
                  'GPS first-read by VIN suffix lookup timed out.'
                );
                if (!firstBySuffixError) {
                  firstTimestamp = getFirstTimestampFromRows(firstBySuffixData);
                }
              } catch (_error) {
                // no-op
              }
            }
          }

          if (Number.isFinite(firstTimestamp)) {
            results.set(vehicleId, new Date(firstTimestamp).toISOString());
          }
        }
      };

      await Promise.all(Array.from({ length: workerCount }, () => runWorker()));
      return results;
    };

    const fetchGpsReadBoundsByVehicles = async (vehicleList = []) => {
      if (!Array.isArray(vehicleList) || !vehicleList.length) return new Map();

      const pendingVehicles = vehicleList.filter((vehicle) => (
        !hasParsableTime(vehicle?.firstRead) || !hasParsableTime(vehicle?.lastRead)
      ));
      if (!pendingVehicles.length) return new Map();

      const results = new Map();
      const requestedVehicleIds = Array.from(new Set(
        pendingVehicles.map((vehicle) => normalizeVehicleIdKey(vehicle?.id)).filter(Boolean)
      ));
      let boundsByVehicleId = new Map();
      try {
        boundsByVehicleId = await fetchGpsReadBoundsByVehicleIds(requestedVehicleIds);
      } catch (error) {
        console.warn('GPS read-bounds by vehicle_id warning: ' + (error?.message || error));
      }
      pendingVehicles.forEach((vehicle) => {
        const vehicleId = normalizeVehicleIdKey(vehicle?.id);
        if (!vehicleId) return;
        const byId = boundsByVehicleId.get(vehicleId);
        if (byId) results.set(vehicleId, byId);
      });

      const unresolvedVehicles = pendingVehicles.filter((vehicle) => {
        const vehicleId = normalizeVehicleIdKey(vehicle?.id);
        if (!vehicleId) return false;
        return !results.has(vehicleId);
      });
      if (!unresolvedVehicles.length) return results;

      const unresolvedVinSuffixes = Array.from(new Set(
        unresolvedVehicles.map((vehicle) => getVehicleVinSuffixKey(vehicle)).filter((value) => value.length === 6)
      ));
      if (unresolvedVinSuffixes.length) {
        try {
          const boundsBySuffix = await fetchGpsReadBoundsByVinSuffixes(unresolvedVinSuffixes);
          unresolvedVehicles.forEach((vehicle) => {
            const vehicleId = normalizeVehicleIdKey(vehicle?.id);
            const suffix = getVehicleVinSuffixKey(vehicle);
            if (!vehicleId || !suffix) return;
            const bySuffix = boundsBySuffix.get(suffix);
            if (bySuffix) results.set(vehicleId, bySuffix);
          });
        } catch (error) {
          console.warn('GPS read-bounds by VIN suffix warning: ' + (error?.message || error));
        }
      }

      const stillUnresolvedAfterSuffix = pendingVehicles.filter((vehicle) => {
        const vehicleId = normalizeVehicleIdKey(vehicle?.id);
        return Boolean(vehicleId) && !results.has(vehicleId);
      });
      if (!stillUnresolvedAfterSuffix.length) return results;

      const unresolvedVins = Array.from(new Set(
        stillUnresolvedAfterSuffix.map((vehicle) => getVehicleVin(vehicle)).filter(Boolean)
      ));
      if (unresolvedVins.length) {
        try {
          const boundsByVin = await fetchGpsReadBoundsByVins(unresolvedVins);
          stillUnresolvedAfterSuffix.forEach((vehicle) => {
            const vehicleId = normalizeVehicleIdKey(vehicle?.id);
            const vin = getVehicleVin(vehicle);
            if (!vehicleId || !vin) return;
            const byVin = boundsByVin.get(vin);
            if (byVin) results.set(vehicleId, byVin);
          });
        } catch (error) {
          console.warn('GPS read-bounds by VIN warning: ' + (error?.message || error));
        }
      }

      const vehiclesMissingFirstRead = pendingVehicles.filter((vehicle) => {
        const vehicleId = normalizeVehicleIdKey(vehicle?.id);
        if (!vehicleId) return false;
        const currentReadBounds = results.get(vehicleId);
        return !hasParsableTime(currentReadBounds?.firstRead);
      });
      if (vehiclesMissingFirstRead.length) {
        const targetedFirstReads = await fetchVehicleFirstReadsByTargetedQueries(vehiclesMissingFirstRead, {
          concurrency: 6
        });
        targetedFirstReads.forEach((firstRead, vehicleId) => {
          if (!hasParsableTime(firstRead)) return;
          const existing = results.get(vehicleId) || { firstRead: '', lastRead: '' };
          results.set(vehicleId, {
            ...existing,
            firstRead
          });
        });
      }

      const stillUnresolved = pendingVehicles.filter((vehicle) => {
        const vehicleId = normalizeVehicleIdKey(vehicle?.id);
        return Boolean(vehicleId) && !results.has(vehicleId);
      });
      if (stillUnresolved.length) {
        const fallbackBounds = await fetchGpsReadBoundsByHistoryManager(stillUnresolved, { concurrency: 3 });
        fallbackBounds.forEach((value, vehicleId) => results.set(vehicleId, value));
      }

      return results;
    };

    const applyGpsReadBoundsToVehicleList = (vehicleList = [], boundsByVin = new Map()) => {
      if (!Array.isArray(vehicleList) || !vehicleList.length || !(boundsByVin instanceof Map)) return false;
      let changed = false;

      vehicleList.forEach((vehicle) => {
        const vehicleId = `${vehicle?.id ?? ''}`.trim();
        if (!vehicleId) return;
        const readBounds = boundsByVin.get(vehicleId);
        if (!readBounds) return;

        if (readBounds.firstRead && !hasParsableTime(vehicle?.firstRead)) {
          vehicle.firstRead = readBounds.firstRead;
          if (vehicle?.details && typeof vehicle.details === 'object') {
            vehicle.details.pt_first_read = readBounds.firstRead;
            vehicle.details['PT First Read'] = readBounds.firstRead;
          }
          changed = true;
        }

        if (readBounds.lastRead && !hasParsableTime(vehicle?.lastRead)) {
          vehicle.lastRead = readBounds.lastRead;
          if (vehicle?.details && typeof vehicle.details === 'object') {
            vehicle.details.pt_last_read = readBounds.lastRead;
            vehicle.details['PT Last Read'] = readBounds.lastRead;
          }
          changed = true;
        }
      });

      return changed;
    };

    const hydrateVehiclePtReadsInBackground = async (vehicleList = []) => {
      if (!Array.isArray(vehicleList) || !vehicleList.length) return;

      const vehicleIdsNeedingBounds = Array.from(new Set(
        vehicleList
          .filter((vehicle) => !hasParsableTime(vehicle?.firstRead) || !hasParsableTime(vehicle?.lastRead))
          .map((vehicle) => `${vehicle?.id ?? ''}`.trim())
          .filter(Boolean)
      ));
      if (!vehicleIdsNeedingBounds.length) return;

      const cacheIsFresh = (Date.now() - gpsReadBoundsByVinCacheUpdatedAt) <= GPS_READ_BOUNDS_CACHE_TTL_MS;
      const cacheHasAllNeeded = cacheIsFresh && vehicleList.every((vehicle) => {
        const vehicleId = `${vehicle?.id ?? ''}`.trim();
        if (!vehicleId || !vehicleIdsNeedingBounds.includes(vehicleId)) return true;
        const cachedBounds = gpsReadBoundsByVinCache.get(vehicleId) || {};
        const hasFirstRead = hasParsableTime(vehicle?.firstRead) || hasParsableTime(cachedBounds?.firstRead);
        const hasLastRead = hasParsableTime(vehicle?.lastRead) || hasParsableTime(cachedBounds?.lastRead);
        return hasFirstRead && hasLastRead;
      });
      if (cacheHasAllNeeded) {
        if (applyGpsReadBoundsToVehicleList(vehicleList, gpsReadBoundsByVinCache)) {
          renderVehicles();
        }
        return;
      }

      if (gpsReadBoundsByVinPending) {
        if (applyGpsReadBoundsToVehicleList(vehicleList, gpsReadBoundsByVinCache)) {
          renderVehicles();
        }
        await gpsReadBoundsByVinPending;
        if (applyGpsReadBoundsToVehicleList(vehicleList, gpsReadBoundsByVinCache)) {
          renderVehicles();
        }
        return;
      }

      gpsReadBoundsByVinPending = (async () => {
        try {
          const vehiclesNeedingBounds = vehicleList.filter((vehicle) => (
            vehicleIdsNeedingBounds.includes(`${vehicle?.id ?? ''}`.trim())
          ));
          const fetchedBounds = await fetchGpsReadBoundsByVehicles(vehiclesNeedingBounds);
          if (fetchedBounds instanceof Map && fetchedBounds.size) {
            const merged = new Map(gpsReadBoundsByVinCache);
            fetchedBounds.forEach((value, vehicleId) => merged.set(vehicleId, value));
            gpsReadBoundsByVinCache = merged;
            gpsReadBoundsByVinCacheUpdatedAt = Date.now();
          }
        } catch (error) {
          console.warn('GPS read-bounds load warning: ' + (error?.message || error));
        } finally {
          gpsReadBoundsByVinPending = null;
        }
      })();

      await gpsReadBoundsByVinPending;
      if (applyGpsReadBoundsToVehicleList(vehicleList, gpsReadBoundsByVinCache)) {
        renderVehicles();
      }
    };

    const normalizeSelectionValue = (value) => `${value ?? ''}`.trim().toLowerCase();
    const matchesVehicleSelection = (vehicle, selection) => {
      if (!vehicle || !selection) return false;
      const selectionKey = `${selection.key || ''}`.trim();
      if (selectionKey && getVehicleKey(vehicle) === selectionKey) return true;
      if (selection.id !== null && selection.id !== undefined && `${vehicle.id}` === `${selection.id}`) return true;
      const selectionVin = normalizeSelectionValue(selection.vin);
      const vehicleVin = normalizeSelectionValue(vehicle.vin);
      if (selectionVin && vehicleVin && selectionVin === vehicleVin) return true;
      const selectionCustomer = normalizeSelectionValue(selection.customerId);
      const vehicleCustomer = normalizeSelectionValue(vehicle.customerId);
      return !!(selectionCustomer && vehicleCustomer && selectionCustomer === vehicleCustomer);
    };

    const syncVehicleSelectionFromStore = (selection, { shouldFocus = true } = {}) => {
      if (!selection) {
        if (selectedVehicleId === null && selectedVehicleKey === null) return;
        syncingVehicleSelection = true;
        applySelection(null, selectedTechId);
        syncingVehicleSelection = false;
        return;
      }

      const match = vehicles.find((vehicle) => matchesVehicleSelection(vehicle, selection));
      if (!match) return;
      const matchKey = getVehicleKey(match);
      if (`${selectedVehicleId ?? ''}` === `${match.id ?? ''}` && `${selectedVehicleKey ?? ''}` === matchKey) return;

      syncingVehicleSelection = true;
      applySelection(match.id, null, matchKey);
      syncingVehicleSelection = false;
      if (shouldFocus) focusVehicle(match);
    };

    const serviceCategoryLabelsByType = new Map();
    const getServiceCategoryLabel = (type) => serviceCategoryLabelsByType.get(type) || '';

    const getCategoryCacheKey = (value = '') => normalizeCategoryLabel(value).toLowerCase();

    const buildServiceCache = (rows = []) => {
      const cache = new Map();
      rows.forEach((row) => {
        const key = getCategoryCacheKey(row?.category || 'uncategorized');
        const list = cache.get(key) || [];
        list.push(row);
        cache.set(key, list);
      });
      return cache;
    };

    const ensureServiceCache = async () => {
      if (!supabaseClient) return null;
      if (serviceCacheByCategory.size) return serviceCacheByCategory;
      if (serviceFetchPromise) return serviceFetchPromise;

      const stopLoading = startLoading('Loading Services…');
      serviceFetchPromise = (async () => {
        const { data, error } = await supabaseClient.from(SERVICE_TABLE).select('*');
        if (error) throw error;
        serviceCacheByCategory = buildServiceCache(data || []);
        return serviceCacheByCategory;
      })().finally(() => {
        stopLoading();
        serviceFetchPromise = null;
      });

      return serviceFetchPromise;
    };

    const getCachedServices = (categoryLabel) => {
      if (!categoryLabel) return [];
      return serviceCacheByCategory.get(getCategoryCacheKey(categoryLabel)) || [];
    };

    const updateServiceCategoryLabels = (rawCategories = new Map()) => {
      serviceCategoryLabelsByType.clear();

      const normalizedToOriginal = new Map();
      rawCategories.forEach((original) => {
        const normalizedKey = normalizeCategoryLabel(original).toLowerCase();
        if (!normalizedToOriginal.has(normalizedKey)) normalizedToOriginal.set(normalizedKey, original);
      });

      SERVICE_TYPES.filter((type) => type !== 'custom').forEach((type) => {
        const hints = SERVICE_CATEGORY_HINTS[type] || [];
        const matchKey = hints
          .map((hint) => normalizeCategoryLabel(hint).toLowerCase())
          .find((hintKey) => normalizedToOriginal.has(hintKey));

        if (matchKey) {
          serviceCategoryLabelsByType.set(type, normalizedToOriginal.get(matchKey));
        }
      });
    };

    const normalizeCategoryLabel = (value = '') => `${value}`.trim();
    const getCustomCategoryKey = (label = '') => normalizeKey(label) || `custom-${customCategories.size + 1}`;
    const ensureCustomCategoryMeta = (label = '') => {
      const normalizedLabel = normalizeCategoryLabel(label) || 'Custom';
      const key = getCustomCategoryKey(normalizedLabel);
      if (!customCategories.has(key)) {
        const color = CUSTOM_COLOR_PALETTE[customCategories.size % CUSTOM_COLOR_PALETTE.length] || SERVICE_COLORS.custom;
        const layer = createPartnerClusterGroup(color);
        customCategories.set(key, { key, label: normalizedLabel, color, layer });
      }
      return customCategories.get(key);
    };

    function rebuildCustomLegendItems() {
      const legendSlot = document.getElementById('custom-legend-slot');
      if (!legendSlot) return;

      legendSlot.innerHTML = '';

      if (!customCategoryLabels.size) {
        legendSlot.classList.add('hidden');
        return;
      }

      legendSlot.classList.remove('hidden');

      Array.from(customCategoryLabels)
        .sort((a, b) => `${a}`.localeCompare(`${b}`))
        .forEach((label) => {
          const meta = ensureCustomCategoryMeta(label);
          const pill = document.createElement('div');
          pill.className = 'legend-pill';
          pill.dataset.legend = meta.key || 'custom';
          pill.innerHTML = `
            <span class="inline-flex items-center justify-center">
              <svg class="h-3 w-3" viewBox="0 0 18 16" fill="${meta.color}" stroke="#0f172a" stroke-width="1.2">
                <path d="M9 1.2L16.2 14.5H1.8L9 1.2Z"></path>
              </svg>
            </span>
          `;
          pill.appendChild(document.createTextNode(` ${meta.label}`));
          legendSlot.appendChild(pill);
        });
    }

    function rebuildCustomCategoryToggles() {
      const slot = document.getElementById('custom-category-toggles');
      if (!slot) return;

      slot.innerHTML = '';
      selectedCustomCategoryKey = null;

      if (!customCategoryLabels.size) {
        slot.classList.add('hidden');
        customToggleRef = null;
        rebuildCustomLegendItems();
        return;
      }

      const buttons = [];

      const updateCustomToggleStates = () => {
        buttons.forEach((button) => {
          const key = button.dataset.categoryKey || '';
          const isActive = key && selectedCustomCategoryKey === key;
          button.classList.toggle('active', isActive);
        });
      };

      const buildButton = (label, key = null) => {
        const button = document.createElement('button');
        button.className = 'sidebar-toggle-btn';
        button.textContent = label;
        button.dataset.categoryKey = key || '';
        button.addEventListener('click', () => {
          selectedCustomCategoryKey = selectedCustomCategoryKey === key ? null : key;
          sidebarStateController?.setState?.('custom', true);
          renderCategorySidebar(selectedCustomCategoryKey);
          updateCustomToggleStates();

          // Force recalculation when switching dynamic categories (e.g., Parking to Repo)
          const origin = getCurrentOrigin();
          if (origin && selectedCustomCategoryKey) {
            showServicesFromOrigin(origin, { forceType: 'custom' });
          }
        });
        slot.appendChild(button);
        buttons.push(button);
      };

      Array.from(customCategoryLabels)
        .sort((a, b) => `${a}`.localeCompare(`${b}`))
        .forEach((label) => {
          const meta = ensureCustomCategoryMeta(label);
          buildButton(meta.label, meta.key);
        });

      rebuildCustomLegendItems();

      customToggleRef = buttons[0] || null;
      slot.classList.remove('hidden');
      updateCustomToggleStates();
      sidebarStateController?.updateTogglePositions?.();
    }

    const isServiceTypeEnabled = (type) => enabledServiceTypes.has(type);

    const getEnabledServiceTypes = () => SERVICE_TYPES.filter(isServiceTypeEnabled);

    async function syncAvailableServiceTypes() {
      if (!supabaseClient) return;

      try {
        const { data, error } = await supabaseClient.from(SERVICE_TABLE).select('category');
        if (error) throw error;

        const rawCategories = new Map();
        (data || []).forEach((row) => {
          const raw = `${row?.category ?? ''}`.trim();
          if (!raw) return;
          const key = normalizeCategoryLabel(raw).toLowerCase();
          if (!rawCategories.has(key)) rawCategories.set(key, raw);
        });

        updateServiceCategoryLabels(rawCategories);

        const detected = SERVICE_TYPES.filter((type) => {
          if (type === 'custom') return false;
          return serviceCategoryLabelsByType.has(type);
        });

        const knownLabels = new Set(
          Array.from(serviceCategoryLabelsByType.values()).map((c) => `${c}`.trim().toLowerCase())
        );
        const customLabelMap = new Map();
        rawCategories.forEach((original, key) => {
          if (!knownLabels.has(key)) customLabelMap.set(key, original);
        });
        customCategoryLabels = new Set(customLabelMap.values());

        if (customCategoryLabels.size) detected.push('custom');

        rebuildCustomCategoryToggles();

        enabledServiceTypes = detected.length ? new Set(detected) : new Set();
      } catch (err) {
        console.warn(`Service availability warning: ${err.message}`);
      }

      updateServiceVisibilityUI();
    }

    const updateServiceVisibilityUI = () => {
      SERVICE_TYPES.forEach((type) => {
        const config = SERVICE_UI_CONFIG[type];
        if (!config) return;

        const enabled = isServiceTypeEnabled(type);
        if (type === 'custom') {
          const slot = document.getElementById('custom-category-toggles');
          if (slot) slot.classList.toggle('hidden', !enabled || !customCategoryLabels.size);
        }

        const toggle = config.toggleId ? document.getElementById(config.toggleId) : null;
        const sidebar = document.getElementById(config.sidebarId);
        const layer = config.layer?.();

        if (toggle) {
          toggle.classList.toggle('hidden', !enabled);
          toggle.setAttribute('aria-hidden', (!enabled).toString());
        }

        if (sidebar) {
          sidebar.classList.toggle('hidden', !enabled);
          if (!enabled && config.collapsedClass) sidebar.classList.add(config.collapsedClass);
        }

        if (config.legendSelector) {
          document.querySelectorAll(config.legendSelector).forEach((el) => {
            el.classList.remove('hidden');
          });
        }

        if (!enabled) layer?.clearLayers?.();
      });

      document.querySelectorAll('[data-legend]').forEach((el) => el.classList.remove('hidden'));
    };

    const setServiceHeaders = (category, data = []) => {
      if (!category || !Array.isArray(data) || !data.length) return;
      serviceHeadersByCategory[category] = Object.keys(data[0]);
    };

    const getServiceHeaders = (category) => serviceHeadersByCategory[category] || [];

    const isAdminUser = () => `${window.currentUserRole || ''}`.toLowerCase() === 'administrator';

    const normalizeVehicleHeader = (value = '') => `${value}`.trim().toLowerCase().replace(/[_\s]+/g, ' ');

    const EDITABLE_VEHICLE_FIELDS = {
      'gps fix': { fieldKey: 'gpsFix', updateColumn: 'gps fix', table: TABLES.vehicles },
      'gps fix reason': { fieldKey: 'gpsReason', updateColumn: 'gps fix reason', table: TABLES.vehicles }
    };

    const REQUIRED_VEHICLE_HEADERS = [
      'gps fix',
      'gps fix reason',
      'moving',
      'pt last read',
      'lat',
      'long',
    ];

    const ensureRequiredVehicleHeaders = (headers = []) => {
      const normalizedHeaders = new Set(headers.map((header) => normalizeVehicleHeader(header)));
      const nextHeaders = [...headers];
      REQUIRED_VEHICLE_HEADERS.forEach((header) => {
        const normalized = normalizeVehicleHeader(header);
        if (normalizedHeaders.has(normalized)) return;
        nextHeaders.push(header);
        normalizedHeaders.add(normalized);
      });
      return nextHeaders;
    };

    const repairHistoryManager = createRepairHistoryManager({
      supabaseClient,
      startLoading,
      runWithTimeout,
      timeoutMs: SUPABASE_TIMEOUT_MS,
      tableName: TABLES.repairHistory,
      escapeHTML,
      formatDateTime
    });

    const gpsHistoryManager = createGpsHistoryManager({
      supabaseClient,
      ensureSupabaseSession,
      runWithTimeout,
      timeoutMs: SUPABASE_TIMEOUT_MS,
      tableName: TABLES.gpsHistory,
      isSerialBlacklisted: (serial = '', timestampMs = Date.now()) => (
        isGpsDeviceSerialBlacklistedAt(serial, timestampMs)
      ),
      escapeHTML,
      formatDateTime
    });


    const normalizeHotspot = (row, idx = 0) => {
      const stateCode = toStateCode(getField(row, 'state', 'State')) || 'US';
      const city = getField(row, 'city', 'City');
      const zip = getField(row, 'zip', 'Zip');
      const rawLat = parseFloat(getField(row, 'lat', 'Lat'));
      const rawLng = parseFloat(getField(row, 'long', 'Long', 'lng', 'Lng', 'longitude', 'Longitude'));
      const rawRadius = parseFloat(getField(row, 'radius', 'Radius'));
      const fallbackLatLng = Number.isFinite(rawLat) && Number.isFinite(rawLng) ? { lat: rawLat, lng: rawLng } : null;

      const { coords } = resolveCoords(row, { zip, city, stateCode, seed: idx, fallbackLatLng, getField });

      return {
        id: row?.id ?? `hotspot-${idx}`,
        city: city || '',
        state: stateCode,
        zip: zip || '',
        lat: coords.lat,
        lng: coords.lng,
        radiusMiles: Number.isFinite(rawRadius) ? rawRadius : HOTSPOT_RADIUS_MILES,
      };
    };

    const normalizeBlacklistEntry = (row, idx = 0) => {
      // Use explicit lat/long columns from Supabase (no geocoding fallbacks)
      const lat = parseFloat(getField(row, 'lat', 'Lat', 'latitude', 'Latitude'));
      const lng = parseFloat(getField(row, 'long', 'Long', 'lng', 'Lng', 'longitude', 'Longitude'));

      return {
        id: row?.id ?? `blacklist-${idx}`,
        company: getField(row, 'company_name', 'Company', 'company', 'name'),
        assocUnit: getField(row, 'Assoc.Unit', 'assoc_unit', 'AssocUnit'),
        note: getField(row, 'Note', 'note', 'notes', 'Notes'),
        lat,
        lng,
      };
    };

    const MAP_DEFAULT_CENTER = { lat: 39.8, lng: -98.5 };
    const MISSING_VEHICLE_GPS_NOTE = 'No GPS coordinates available (shown at map center).';

    const hasValidCoords = (entry) => Number.isFinite(entry?.lat) && Number.isFinite(entry?.lng) && entry.lat !== 0 && entry.lng !== 0;

    const getVehicleMapCoords = (vehicle = {}) => {
      if (hasValidCoords(vehicle)) {
        return {
          lat: Number(vehicle.lat),
          lng: Number(vehicle.lng),
          isFallback: false
        };
      }
      return {
        lat: MAP_DEFAULT_CENTER.lat,
        lng: MAP_DEFAULT_CENTER.lng,
        isFallback: true
      };
    };

    const getLocationNote = (accuracy = '') => {
      if (accuracy === 'zip') return 'Approximate location (based on ZIP code)';
      if (accuracy === 'city') return 'Approximate location (based on city)';
      if (accuracy === 'state') return 'Approximate location (state center)';
      return '';
    };

    const ADDRESS_COUNTRY_SUFFIXES = new Set([
      'usa',
      'us',
      'u s a',
      'united states',
      'united states of america',
      'estados unidos',
      'eeuu'
    ]);

    const normalizeAddressCountryToken = (value = '') => `${value}`
      .toLowerCase()
      .replace(/\./g, '')
      .replace(/\s+/g, ' ')
      .trim();

    const stripCountryMentionsFromFreeText = (value = '') => `${value}`
      .replace(/\bUnited\s+States(?:\s+of\s+America)?\b/gi, '')
      .replace(/\bU\s*\.?\s*S\s*\.?\s*A\b/gi, '')
      .replace(/\bEstados\s+Unidos\b/gi, '')
      .replace(/\bEEUU\b/gi, '')
      .replace(/\s+,/g, ',')
      .replace(/,\s*,+/g, ', ')
      .replace(/\s{2,}/g, ' ')
      .trim();

    const looksLikeStreetSegment = (segment = '') => (
      /^\d/.test(`${segment}`.trim())
      || /\b(st|street|ave|avenue|blvd|boulevard|rd|road|dr|drive|hwy|highway|ln|lane|ct|court|cir|circle)\b/i
        .test(`${segment}`.trim())
    );

    const formatVehicleSidebarAddress = (rawAddress = '', zipCode = '') => {
      const normalizedZip = `${zipCode || ''}`.trim();
      let addressText = stripCountryMentionsFromFreeText(`${rawAddress || ''}`.trim());

      if (addressText.includes('.')) {
        const dotIndex = addressText.indexOf('.');
        const trailing = addressText.slice(dotIndex + 1).trim();
        if (trailing) addressText = trailing;
      }

      if (!addressText && normalizedZip) return normalizedZip;
      if (!addressText) return 'No location provided';

      let parts = addressText
        .split(',')
        .map((part) => part.trim())
        .filter(Boolean);

      while (parts.length > 1) {
        const token = normalizeAddressCountryToken(parts[parts.length - 1]);
        if (!ADDRESS_COUNTRY_SUFFIXES.has(token)) break;
        parts.pop();
      }

      const zipRegex = /\b\d{5}(?:-\d{4})?\b/;
      const zipIndex = parts.findIndex((part) => zipRegex.test(part));

      if (zipIndex >= 0) {
        const fromIndex = Math.max(0, zipIndex - 1);
        parts = parts.slice(fromIndex, zipIndex + 1);
      } else if (parts.length >= 3) {
        parts = parts.slice(-2);
      }

      if (parts.length > 1 && looksLikeStreetSegment(parts[0])) {
        parts = parts.slice(1);
      }

      let formatted = stripCountryMentionsFromFreeText(parts.join(', '));
      const escapedZip = normalizedZip.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      const zipAlreadyIncluded = normalizedZip && escapedZip
        ? new RegExp(`\\b${escapedZip}\\b`).test(formatted)
        : false;

      if (normalizedZip && !zipAlreadyIncluded) {
        formatted = formatted ? `${formatted}, ${normalizedZip}` : normalizedZip;
      }

      return formatted || normalizedZip || 'No location provided';
    };

    const getAccuracyDot = (accuracy = '') => {
      return accuracy === 'exact' ? 'bg-emerald-300' : 'bg-amber-300';
    };

    const getCurrentTrailPointLimit = () => {
      const parsed = Number.parseInt(gpsTrailPointLimit, 10);
      if (!Number.isFinite(parsed)) return DEFAULT_GPS_TRAIL_POINT_LIMIT;
      return Math.max(MIN_GPS_TRAIL_POINT_LIMIT, Math.min(parsed, MAX_GPS_TRAIL_POINT_LIMIT));
    };

    const GPS_TRAIL_ALIGNMENT_MAX_DISTANCE_METERS = 120000;
    const GPS_TRAIL_ALIGNMENT_MIN_IMPROVEMENT_METERS = 20000;
    const GPS_TRAIL_ALIGNMENT_MIN_FRESHNESS_ADVANTAGE_MS = 90 * 60 * 1000;
    const GPS_TRAIL_LABEL_MERGE_RADIUS_METERS = 90;
    const GPS_MOVING_ANALYSIS_MAX_POINTS = 8;
    const GPS_MOVING_STOPPED_TOTAL_DISTANCE_METERS = 550;
    const GPS_MOVING_STOPPED_NET_DISTANCE_METERS = 180;
    const GPS_MOVING_STOPPED_MAX_RADIUS_METERS = 220;
    const GPS_MOVING_STOPPED_MAX_SEGMENT_METERS = 220;
    const GPS_MOVING_MOVING_TOTAL_DISTANCE_METERS = 1300;
    const GPS_MOVING_MOVING_NET_DISTANCE_METERS = 420;
    const GPS_MOVING_MOVING_MAX_SEGMENT_METERS = 320;
    const GPS_TRUCK_TRAILER_STOPPED_DISTANCE_THRESHOLD_METERS = 25000;
    const GPS_NON_TRUCK_TRAILER_STOPPED_DISTANCE_THRESHOLD_METERS = 1000;
    const GPS_HISTORY_HOTSPOT_MAX = 6;
    const GPS_HISTORY_HOTSPOT_CLUSTER_RADIUS_METERS = 260;
    const GPS_HISTORY_HOTSPOT_MIN_CONSECUTIVE_DAYS = 2;
    const GPS_HISTORY_HOTSPOT_MIN_MULTIDAY_STAYS = 1;
    const GPS_HISTORY_HOTSPOT_COLOR = '#1e3a8a';
    const GPS_HISTORY_HOTSPOT_HIERARCHY_VISIT_WEIGHT = 10;
    const GPS_HISTORY_HOTSPOT_HIERARCHY_TOTAL_DAYS_WEIGHT = 14;
    const GPS_HISTORY_HOTSPOT_LEVEL_PRIMARY_THRESHOLD = 0.78;
    const GPS_HISTORY_HOTSPOT_LEVEL_SECONDARY_THRESHOLD = 0.48;
    const GPS_HISTORY_DAY_MS = 24 * 60 * 60 * 1000;
    const GPS_MOVING_UNKNOWN_STALE_READ_MS = 3 * GPS_HISTORY_DAY_MS;
    const VEHICLE_HISTORY_HOTSPOT_CACHE_TTL_MS = 12 * 60 * 1000;
    const GPS_DEVICE_BLACKLIST_CACHE_TTL_MS = 5 * 60 * 1000;
    const HOTSPOT_SHARED_MATCH_RADIUS_METERS = 320;
    const HOTSPOT_RELATED_VEHICLE_LIMIT = 8;
    const HOTSPOT_RELATED_CANDIDATE_LIMIT = 120;
    const HOTSPOT_RELATED_CONCURRENCY = 4;
    const HOTSPOT_FAST_QUERY_MAX_ROWS = 3200;
    const HOTSPOT_FAST_QUERY_PAGE_SIZE = 1000;
    const HOTSPOT_FAST_QUERY_COORDINATE_COLUMNS = [
      { lat: 'lat', lng: 'long' },
      { lat: 'lat', lng: 'lng' },
      { lat: 'Lat', lng: 'Long' },
      { lat: 'Lat', lng: 'Lng' },
      { lat: 'latitude', lng: 'longitude' },
      { lat: 'Latitude', lng: 'Longitude' }
    ];
    let gpsTrailRequestCounter = 0;
    const vehicleHistoryHotspotCache = new Map();
    const vehicleHistoryHotspotPendingRequests = new Map();
    const relatedHotspotVehiclesCache = new Map();
    let hotspotFastCoordinatePairs = [];
    let hotspotFastCoordinatePairLookupPromise = null;
    let gpsDeviceBlacklistSerialsCache = new Map();
    let gpsDeviceBlacklistSerialsCacheUpdatedAt = 0;
    let gpsDeviceBlacklistSerialsPending = null;

    const parseGpsNumericValue = (value) => {
      if (value === null || value === undefined) return null;
      const normalized = `${value}`.replace(/[^\d.\-]/g, '').trim();
      if (!normalized) return null;
      const parsed = Number.parseFloat(normalized);
      return Number.isFinite(parsed) ? parsed : null;
    };

    const parseGpsTrailCoordinate = (record = {}, key = 'lat') => {
      const candidates = key === 'lat'
        ? ['lat', 'Lat', 'latitude', 'Latitude']
        : ['long', 'Long', 'lng', 'Lng', 'longitude', 'Longitude', 'lon', 'Lon'];

      for (const candidate of candidates) {
        const parsed = parseGpsNumericValue(record?.[candidate]);
        if (Number.isFinite(parsed)) return parsed;
      }

      return null;
    };

    const parseGpsTrailTimeMs = (record = {}) => {
      const dateCandidates = [
        record?.['PT-LastPing'],
        record?.gps_time,
        record?.Date,
        record?.created_at,
        record?.updated_at
      ];

      for (const candidate of dateCandidates) {
        if (!candidate) continue;
        const parsed = Date.parse(candidate);
        if (!Number.isNaN(parsed)) return parsed;
      }

      return null;
    };

    const parseGpsTrailTimestamp = (record = {}) => {
      const timeMs = parseGpsTrailTimeMs(record);
      if (Number.isFinite(timeMs)) return timeMs;

      const numericId = Number(record?.id);
      if (Number.isFinite(numericId)) return numericId;
      return 0;
    };

    const toGpsTrailPoint = (record = {}) => {
      // Route visualization must be driven by lat/long from GPS history records.
      const lat = parseGpsTrailCoordinate(record, 'lat');
      const lng = parseGpsTrailCoordinate(record, 'long');
      if (!Number.isFinite(lat) || !Number.isFinite(lng) || lat === 0 || lng === 0) return null;
      const timeMs = parseGpsTrailTimeMs(record);
      return {
        lat,
        lng,
        timestamp: Number.isFinite(timeMs) ? timeMs : parseGpsTrailTimestamp(record),
        timeMs
      };
    };

    const getVehicleTrailAnchorPoint = (vehicle = {}) => {
      const lat = parseGpsNumericValue(
        vehicle?.lat
        ?? vehicle?.Lat
        ?? vehicle?.latitude
        ?? vehicle?.Latitude
      );
      const lng = parseGpsNumericValue(
        vehicle?.lng
        ?? vehicle?.long
        ?? vehicle?.Long
        ?? vehicle?.longitude
        ?? vehicle?.Longitude
      );
      if (!Number.isFinite(lat) || !Number.isFinite(lng) || lat === 0 || lng === 0) return null;
      return { lat, lng };
    };

    const getLatestTrailPointFromRecords = (records = []) => {
      if (!Array.isArray(records) || !records.length) return null;
      let latestRecord = null;
      let latestTimestamp = Number.NEGATIVE_INFINITY;

      records.forEach((record) => {
        const timestamp = parseGpsTrailTimestamp(record);
        if (timestamp >= latestTimestamp) {
          latestTimestamp = timestamp;
          latestRecord = record;
        }
      });

      if (!latestRecord) return null;
      const point = toGpsTrailPoint(latestRecord);
      if (!point) return null;
      return { ...point, record: latestRecord };
    };

    const getMostRecentRecordSerial = (records = [], getRecordSerial = () => '') => {
      if (!Array.isArray(records) || !records.length || typeof getRecordSerial !== 'function') return '';
      let latestSerial = '';
      let latestTimestamp = Number.NEGATIVE_INFINITY;

      records.forEach((record) => {
        const serial = getRecordSerial(record);
        if (!serial) return;
        const timestamp = parseGpsTrailTimestamp(record);
        if (timestamp >= latestTimestamp) {
          latestTimestamp = timestamp;
          latestSerial = serial;
        }
      });

      return latestSerial;
    };

    const getLocalDayBoundsMs = (referenceMs = Date.now()) => {
      const referenceDate = new Date(referenceMs);
      const dayStart = new Date(
        referenceDate.getFullYear(),
        referenceDate.getMonth(),
        referenceDate.getDate()
      ).getTime();
      return {
        start: dayStart,
        end: dayStart + GPS_HISTORY_DAY_MS
      };
    };

    const hasSerialReadingInRange = ({
      records = [],
      serial = '',
      getRecordSerial = () => '',
      startMs = Number.NEGATIVE_INFINITY,
      endMs = Number.POSITIVE_INFINITY
    } = {}) => {
      const normalizedSerial = `${serial || ''}`.trim();
      if (!normalizedSerial || !Array.isArray(records) || !records.length || typeof getRecordSerial !== 'function') {
        return false;
      }
      return records.some((record) => {
        const recordSerial = `${getRecordSerial(record) || ''}`.trim();
        if (!recordSerial || recordSerial !== normalizedSerial) return false;
        const timestamp = parseGpsTrailTimestamp(record);
        return Number.isFinite(timestamp) && timestamp >= startMs && timestamp < endMs;
      });
    };

    const countGpsHistoryRecordsBySerial = (records = [], getRecordSerial = () => '') => {
      const counts = new Map();
      if (!Array.isArray(records) || !records.length || typeof getRecordSerial !== 'function') return counts;
      records.forEach((record) => {
        const serial = `${getRecordSerial(record) || ''}`.trim();
        if (!serial) return;
        counts.set(serial, (counts.get(serial) || 0) + 1);
      });
      return counts;
    };

    const resolveVehicleMovingHistoryOverride = ({
      records = [],
      vehicle = null,
      winnerSerial = '',
      latestSerial = '',
      serialCountsBySerial = new Map(),
      getRecordSerial = () => '',
    } = {}) => {
      const recordsForSerial = (serial = '') => {
        const normalizedSerial = `${serial || ''}`.trim();
        if (!normalizedSerial) return [];
        if (!Array.isArray(records) || !records.length || typeof getRecordSerial !== 'function') return [];
        return records.filter((record) => `${getRecordSerial(record) || ''}`.trim() === normalizedSerial);
      };

      const winnerScopedRecords = recordsForSerial(winnerSerial);
      const latestScopedRecords = latestSerial && latestSerial !== winnerSerial
        ? recordsForSerial(latestSerial)
        : [];
      const movementSourceRecords = winnerScopedRecords.length
        ? winnerScopedRecords
        : (latestScopedRecords.length ? latestScopedRecords : records);

      const inferredStatus = inferMovingHistoryStatusFromRecords(movementSourceRecords, { vehicle });
      if (inferredStatus === 'moving' || inferredStatus === 'stopped') return inferredStatus;

      const readCount = (serial = '') => {
        const normalizedSerial = `${serial || ''}`.trim();
        if (!normalizedSerial) return 0;
        return Number(serialCountsBySerial.get(normalizedSerial) || 0);
      };

      const winnerCount = readCount(winnerSerial);
      if (winnerCount > 0) return winnerCount <= 1 ? 'unknown' : '';

      const latestCount = readCount(latestSerial);
      if (latestCount > 0) return latestCount <= 1 ? 'unknown' : '';

      return Array.isArray(movementSourceRecords) && movementSourceRecords.length <= 1 ? 'unknown' : '';
    };

    const inferStationaryDaysFromRecords = (records = [], { vehicle = null } = {}) => {
      if (!Array.isArray(records) || !records.length) return null;

      const stoppedDistanceThreshold = getStoppedDistanceThresholdForVehicle(vehicle);
      const entries = records
        .map((record, index) => {
          const point = toGpsTrailPoint(record);
          const timeMs = parseGpsTrailTimeMs(record);
          return {
            index,
            timestamp: parseGpsTrailTimestamp(record),
            lat: Number.isFinite(point?.lat) ? point.lat : null,
            lng: Number.isFinite(point?.lng) ? point.lng : null,
            dayStartMs: toLocalDayStartMs(timeMs),
            derivedDaysStationary: null
          };
        })
        .sort((a, b) => {
          if (a.timestamp === b.timestamp) return a.index - b.index;
          return a.timestamp - b.timestamp;
        });
      if (!entries.length) return null;

      let previousEntry = null;
      let sessionStartDayMs = null;
      entries.forEach((entry) => {
        const hasCoords = Number.isFinite(entry.lat) && Number.isFinite(entry.lng);
        const previousHasCoords = Number.isFinite(previousEntry?.lat) && Number.isFinite(previousEntry?.lng);
        const distanceMeters = (hasCoords && previousHasCoords)
          ? getGpsPointDistanceMeters(
            { lat: previousEntry.lat, lng: previousEntry.lng },
            { lat: entry.lat, lng: entry.lng }
          )
          : Number.POSITIVE_INFINITY;

        if (!previousEntry || !Number.isFinite(distanceMeters) || distanceMeters > stoppedDistanceThreshold) {
          sessionStartDayMs = entry.dayStartMs;
        } else if (!Number.isFinite(sessionStartDayMs) && Number.isFinite(entry.dayStartMs)) {
          sessionStartDayMs = entry.dayStartMs;
        }

        if (Number.isFinite(entry.dayStartMs) && Number.isFinite(sessionStartDayMs)) {
          entry.derivedDaysStationary = Math.max(
            0,
            Math.floor((entry.dayStartMs - sessionStartDayMs) / GPS_HISTORY_DAY_MS)
          );
        }

        previousEntry = entry;
      });

      const latestEntry = entries[entries.length - 1];
      return Number.isFinite(latestEntry?.derivedDaysStationary) ? latestEntry.derivedDaysStationary : null;
    };

    const toComparableTimeMs = (value) => {
      if (!value) return null;
      const parsed = Date.parse(value);
      return Number.isFinite(parsed) ? parsed : null;
    };

    const applyVehiclePtReadBoundsFromRecords = (
      vehicle,
      records = [],
      {
        winnerSerial = '',
        getRecordSerial = () => ''
      } = {}
    ) => {
      if (!vehicle || !Array.isArray(records) || !records.length) return false;

      const scopedRecords = winnerSerial
        ? records.filter((record) => `${getRecordSerial(record) || ''}`.trim() === winnerSerial)
        : records;
      const sourceRecords = scopedRecords.length ? scopedRecords : records;

      let earliestMs = Number.POSITIVE_INFINITY;
      let latestMs = Number.NEGATIVE_INFINITY;
      sourceRecords.forEach((record) => {
        const timeMs = parseGpsTrailTimeMs(record);
        if (!Number.isFinite(timeMs)) return;
        if (timeMs < earliestMs) earliestMs = timeMs;
        if (timeMs > latestMs) latestMs = timeMs;
      });

      const nextFirstRead = Number.isFinite(earliestMs) ? new Date(earliestMs).toISOString() : null;
      const nextLastRead = Number.isFinite(latestMs) ? new Date(latestMs).toISOString() : null;

      const currentFirstMs = toComparableTimeMs(vehicle?.firstRead ?? vehicle?.details?.pt_first_read ?? vehicle?.details?.['PT First Read']);
      const currentLastMs = toComparableTimeMs(vehicle?.lastRead ?? vehicle?.details?.pt_last_read ?? vehicle?.details?.['PT Last Read']);
      const nextFirstMs = Number.isFinite(earliestMs) ? earliestMs : null;
      const nextLastMs = Number.isFinite(latestMs) ? latestMs : null;

      const firstChanged = (currentFirstMs ?? null) !== (nextFirstMs ?? null);
      const lastChanged = (currentLastMs ?? null) !== (nextLastMs ?? null);
      if (!firstChanged && !lastChanged) return false;

      if (nextFirstRead) {
        vehicle.firstRead = nextFirstRead;
      }
      if (nextLastRead) {
        vehicle.lastRead = nextLastRead;
      }
      if (vehicle?.details && typeof vehicle.details === 'object') {
        if (nextFirstRead) {
          vehicle.details.pt_first_read = nextFirstRead;
          vehicle.details['PT First Read'] = nextFirstRead;
        }
        if (nextLastRead) {
          vehicle.details.pt_last_read = nextLastRead;
          vehicle.details['PT Last Read'] = nextLastRead;
        }
      }

      return true;
    };

    const applyVehicleMovingOverrideFromGpsHistory = (vehicle, records = []) => {
      if (!vehicle || !Array.isArray(records) || !records.length) return false;
      const getRecordSerial = typeof gpsHistoryManager?.getRecordSerial === 'function'
        ? gpsHistoryManager.getRecordSerial
        : () => '';
      const configuredWinnerSerial = typeof gpsHistoryManager?.getVehicleWinnerSerial === 'function'
        ? gpsHistoryManager.getVehicleWinnerSerial(vehicle)
        : '';
      const resolvedWinnerSerial = typeof gpsHistoryManager?.resolveVehicleWinnerSerialFromRecords === 'function'
        ? gpsHistoryManager.resolveVehicleWinnerSerialFromRecords(vehicle, records)
        : configuredWinnerSerial;
      const winnerSerial = resolvedWinnerSerial || configuredWinnerSerial;
      const movementSourceRecords = winnerSerial
        ? records.filter((record) => `${getRecordSerial(record) || ''}`.trim() === winnerSerial)
        : records;
      const ptReadBoundsChanged = applyVehiclePtReadBoundsFromRecords(vehicle, records, {
        winnerSerial,
        getRecordSerial
      });

      const latestSerial = getMostRecentRecordSerial(movementSourceRecords, getRecordSerial);
      const serialCountsBySerial = countGpsHistoryRecordsBySerial(movementSourceRecords, getRecordSerial);
      const movingHistoryOverride = resolveVehicleMovingHistoryOverride({
        records: movementSourceRecords,
        vehicle,
        winnerSerial,
        latestSerial,
        serialCountsBySerial,
        getRecordSerial
      });
      const latestMovementRecord = [...movementSourceRecords]
        .sort((a, b) => parseGpsTrailTimestamp(b) - parseGpsTrailTimestamp(a))[0] || null;
      const explicitLatestStationaryDays = parseGpsRecordDaysParked(latestMovementRecord || {});
      const inferredStationaryDays = inferStationaryDaysFromRecords(movementSourceRecords, { vehicle });
      let stationaryDays = Number.isFinite(inferredStationaryDays)
        ? inferredStationaryDays
        : (Number.isFinite(explicitLatestStationaryDays) ? explicitLatestStationaryDays : null);

      const latestMovementRecordTimeMs = parseGpsTrailTimeMs(latestMovementRecord || {});
      const isLatestReadStale = Number.isFinite(latestMovementRecordTimeMs)
        && (Date.now() - latestMovementRecordTimeMs) > GPS_MOVING_UNKNOWN_STALE_READ_MS;
      const latestRecordMove = parseGpsRecordMovingStatus(latestMovementRecord || {});
      const latestRecordLooksParked = (
        latestRecordMove === 'stopped'
        || (Number.isFinite(explicitLatestStationaryDays) && explicitLatestStationaryDays > 0)
      );
      if (!Number.isFinite(stationaryDays) && latestRecordLooksParked) {
        stationaryDays = 0;
      }

      const currentMovingOverride = `${vehicle?.historyMovingOverride || ''}`.trim().toLowerCase();
      let normalizedOverride = `${movingHistoryOverride || ''}`.trim().toLowerCase();
      if (isLatestReadStale) {
        normalizedOverride = 'unknown';
      }
      if (normalizedOverride === 'moving') {
        stationaryDays = 0;
      }
      const movingOverrideChanged = currentMovingOverride !== normalizedOverride;
      const currentDaysRaw = vehicle?.historyDaysStationaryOverride ?? vehicle?.details?.historyDaysStationaryOverride;
      const currentDays = Number.isFinite(Number(currentDaysRaw)) ? Number(currentDaysRaw) : null;
      const nextDays = Number.isFinite(stationaryDays)
        ? Math.max(0, Math.floor(stationaryDays))
        : null;
      const daysChanged = (currentDays === null && nextDays !== null)
        || (currentDays !== null && nextDays === null)
        || (currentDays !== null && nextDays !== null && currentDays !== nextDays);
      if (!movingOverrideChanged && !daysChanged && !ptReadBoundsChanged) return false;

      if (normalizedOverride) {
        vehicle.historyMovingOverride = normalizedOverride;
        if (vehicle?.details && typeof vehicle.details === 'object') {
          vehicle.details.historyMovingOverride = normalizedOverride;
        }
      } else {
        delete vehicle.historyMovingOverride;
        if (vehicle?.details && typeof vehicle.details === 'object') {
          delete vehicle.details.historyMovingOverride;
        }
      }

      if (nextDays !== null) {
        vehicle.historyDaysStationaryOverride = nextDays;
        vehicle.daysStationary = nextDays;
        if (vehicle?.details && typeof vehicle.details === 'object') {
          vehicle.details.historyDaysStationaryOverride = nextDays;
          vehicle.details.days_stationary = nextDays;
        }
      } else {
        delete vehicle.historyDaysStationaryOverride;
        if (vehicle?.details && typeof vehicle.details === 'object') {
          delete vehicle.details.historyDaysStationaryOverride;
        }
      }
      return true;
    };

    const normalizeGpsSerial = (serial = '') => `${serial ?? ''}`.trim().toUpperCase();

    const isWiredGpsSerial = (serial = '') => /^[0-7]/.test(normalizeGpsSerial(serial));

    const normalizeGpsBlacklistColumnName = (value = '') => `${value ?? ''}`
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9]/g, '');

    const GPS_BLACKLIST_EFFECTIVE_DATE_COLUMN_CANDIDATES = [
      'effective_from',
      'effective_date',
      'effective_start_date',
      'blacklist_from',
      'blacklist_date',
      'start_date',
      'date_from',
      'from_date',
      'date'
    ];

    const parseGpsBlacklistEffectiveFromMs = (row = {}) => {
      if (!row || typeof row !== 'object') return null;

      const keyLookup = new Map();
      Object.keys(row).forEach((key) => {
        keyLookup.set(normalizeGpsBlacklistColumnName(key), key);
      });

      for (const candidate of GPS_BLACKLIST_EFFECTIVE_DATE_COLUMN_CANDIDATES) {
        const actualKey = keyLookup.get(normalizeGpsBlacklistColumnName(candidate));
        if (!actualKey) continue;
        const rawValue = row?.[actualKey];
        if (rawValue === null || rawValue === undefined || `${rawValue}`.trim() === '') {
          return null;
        }
        const parsed = Date.parse(rawValue);
        if (!Number.isNaN(parsed)) return parsed;
        return null;
      }

      return null;
    };

    const isGpsDeviceSerialBlacklistedAt = (
      serial = '',
      timestampMs = Date.now(),
      { blacklistBySerial = gpsDeviceBlacklistSerialsCache } = {}
    ) => {
      const normalized = normalizeGpsSerial(serial);
      if (!normalized) return false;

      if (blacklistBySerial instanceof Set) {
        return blacklistBySerial.has(normalized);
      }

      if (!(blacklistBySerial instanceof Map)) return false;
      if (!blacklistBySerial.has(normalized)) return false;

      const effectiveFromMs = blacklistBySerial.get(normalized);
      if (effectiveFromMs === null || effectiveFromMs === undefined) return true;

      const evaluationTimestamp = Number.isFinite(timestampMs) ? timestampMs : Date.now();
      return evaluationTimestamp >= effectiveFromMs;
    };

    const getGpsDeviceBlacklistSerials = async ({ force = false } = {}) => {
      if (!supabaseClient?.from) return new Map();

      const now = Date.now();
      const cacheIsFresh = (now - gpsDeviceBlacklistSerialsCacheUpdatedAt) <= GPS_DEVICE_BLACKLIST_CACHE_TTL_MS;
      if (!force && cacheIsFresh && gpsDeviceBlacklistSerialsCacheUpdatedAt > 0) {
        return gpsDeviceBlacklistSerialsCache;
      }

      if (!force && gpsDeviceBlacklistSerialsPending) return gpsDeviceBlacklistSerialsPending;

      gpsDeviceBlacklistSerialsPending = (async () => {
        await ensureSupabaseSession();
        const tableName = TABLES.gpsDeviceBlacklist || 'gps_blacklist';
        const { data, error } = await runWithTimeout(
          supabaseClient
            .from(tableName)
            .select('*'),
          8000,
          'GPS blacklist request timed out.'
        );

        if (error) throw error;

        const serials = new Map();
        (data || []).forEach((row) => {
          if (row?.is_active === false) return;
          const serial = normalizeGpsSerial(row?.serial);
          if (!serial) return;
          const effectiveFromMs = parseGpsBlacklistEffectiveFromMs(row);
          const currentValue = serials.get(serial);
          if (currentValue === undefined) {
            serials.set(serial, effectiveFromMs);
            return;
          }
          if (currentValue === null || effectiveFromMs === null) {
            serials.set(serial, null);
            return;
          }
          if (effectiveFromMs < currentValue) {
            serials.set(serial, effectiveFromMs);
          }
        });

        gpsDeviceBlacklistSerialsCache = serials;
        gpsDeviceBlacklistSerialsCacheUpdatedAt = Date.now();
        return serials;
      })().catch((error) => {
        console.warn('GPS device blacklist load warning: ' + (error?.message || error));
        return gpsDeviceBlacklistSerialsCache;
      }).finally(() => {
        gpsDeviceBlacklistSerialsPending = null;
      });

      return gpsDeviceBlacklistSerialsPending;
    };

    const parseGpsRecordMovingStatus = (record = {}) => {
      const movementCandidates = [
        record?.moved,
        record?.Moved,
        record?.moving,
        record?.Moving,
        record?.moving_calc,
        record?.['Moving (Calc)'],
        record?.gps_moving,
        record?.['GPS Moving']
      ];

      for (const candidate of movementCandidates) {
        if (candidate === null || candidate === undefined) continue;
        const raw = `${candidate}`.trim();
        if (!raw) continue;
        const numeric = Number.parseInt(raw, 10);
        if (Number.isFinite(numeric)) {
          if (numeric === 1) return 'moving';
          if (numeric === -1 || numeric === 0) return 'stopped';
        }
        const normalized = raw.toLowerCase();
        if (normalized === 'moving' || normalized === 'move' || normalized === 'true' || normalized === 'yes') {
          return 'moving';
        }
        if (
          normalized === 'stopped' ||
          normalized === 'not moving' ||
          normalized === 'stop' ||
          normalized === 'false' ||
          normalized === 'no' ||
          normalized === 'parked'
        ) {
          return 'stopped';
        }
      }

      const speedCandidates = [
        record?.speed,
        record?.Speed,
        record?.gps_speed,
        record?.['GPS Speed'],
        record?.mph
      ];

      for (const speedCandidate of speedCandidates) {
        const speed = parseGpsNumericValue(speedCandidate);
        if (!Number.isFinite(speed)) continue;
        return speed <= 1.5 ? 'stopped' : 'moving';
      }

      return null;
    };

    const inferMovingHistoryStatusFromRecords = (records = [], { vehicle = null } = {}) => {
      if (!Array.isArray(records) || !records.length) return '';

      const analysisRecords = [...records]
        .sort((a, b) => parseGpsTrailTimestamp(b) - parseGpsTrailTimestamp(a))
        .slice(0, GPS_MOVING_ANALYSIS_MAX_POINTS)
        .sort((a, b) => parseGpsTrailTimestamp(a) - parseGpsTrailTimestamp(b));

      const movementVotes = analysisRecords
        .map((record) => parseGpsRecordMovingStatus(record))
        .filter((status) => status === 'moving' || status === 'stopped');
      const movingVotes = movementVotes.filter((status) => status === 'moving').length;
      const stoppedVotes = movementVotes.filter((status) => status === 'stopped').length;

      const trailPoints = analysisRecords
        .map((record) => toGpsTrailPoint(record))
        .filter(Boolean);

      if (trailPoints.length < 2) {
        if (movementVotes.length) {
          return stoppedVotes >= movingVotes ? 'stopped' : 'moving';
        }
        return '';
      }

      let totalDistanceMeters = 0;
      let maxSegmentMeters = 0;
      for (let index = 1; index < trailPoints.length; index += 1) {
        const segmentDistance = getGpsPointDistanceMeters(trailPoints[index - 1], trailPoints[index]);
        if (!Number.isFinite(segmentDistance)) continue;
        totalDistanceMeters += segmentDistance;
        if (segmentDistance > maxSegmentMeters) maxSegmentMeters = segmentDistance;
      }

      const firstPoint = trailPoints[0];
      const lastPoint = trailPoints[trailPoints.length - 1];
      const netDistanceMeters = getGpsPointDistanceMeters(firstPoint, lastPoint);
      let maxRadiusMeters = 0;
      trailPoints.forEach((point) => {
        const pointDistance = getGpsPointDistanceMeters(lastPoint, point);
        if (!Number.isFinite(pointDistance)) return;
        if (pointDistance > maxRadiusMeters) maxRadiusMeters = pointDistance;
      });

      // Treat short-segment clusters as stopped even when cumulative jitter inflates total distance.
      const tightClusterStopped = (
        netDistanceMeters <= GPS_MOVING_STOPPED_NET_DISTANCE_METERS
        && maxRadiusMeters <= GPS_MOVING_STOPPED_MAX_RADIUS_METERS
        && maxSegmentMeters <= GPS_MOVING_STOPPED_MAX_SEGMENT_METERS
      );
      const lowTravelStopped = (
        totalDistanceMeters <= GPS_MOVING_STOPPED_TOTAL_DISTANCE_METERS
        && netDistanceMeters <= GPS_MOVING_STOPPED_NET_DISTANCE_METERS
        && maxSegmentMeters <= GPS_MOVING_STOPPED_MAX_SEGMENT_METERS
      );
      if (tightClusterStopped || lowTravelStopped) return 'stopped';

      const likelyMoving = (
        totalDistanceMeters >= GPS_MOVING_MOVING_TOTAL_DISTANCE_METERS
        || netDistanceMeters >= GPS_MOVING_MOVING_NET_DISTANCE_METERS
        || maxSegmentMeters >= GPS_MOVING_MOVING_MAX_SEGMENT_METERS
      );
      if (likelyMoving && movingVotes >= stoppedVotes) return 'moving';

      const stoppedDistanceThreshold = getStoppedDistanceThresholdForVehicle(vehicle);
      const withinDistanceThreshold = (
        totalDistanceMeters <= stoppedDistanceThreshold
        && netDistanceMeters <= stoppedDistanceThreshold
      );
      if (withinDistanceThreshold) return 'stopped';

      if (movementVotes.length) {
        return stoppedVotes >= movingVotes ? 'stopped' : 'moving';
      }

      return '';
    };

    const isTruckOrTrailerUnit = (vehicle = {}) => {
      const unitType = `${vehicle?.type || vehicle?.unit_type || vehicle?.details?.['Unit Type'] || ''}`
        .trim()
        .toLowerCase();
      if (!unitType) return false;
      return unitType.includes('truck') || unitType.includes('trailer');
    };

    const getStoppedDistanceThresholdForVehicle = (vehicle = {}) => (
      isTruckOrTrailerUnit(vehicle)
        ? GPS_TRUCK_TRAILER_STOPPED_DISTANCE_THRESHOLD_METERS
        : GPS_NON_TRUCK_TRAILER_STOPPED_DISTANCE_THRESHOLD_METERS
    );

    const getGpsPointDistanceMeters = (fromPoint, toPoint) => {
      if (!fromPoint || !toPoint) return Number.POSITIVE_INFINITY;
      const toRadians = (value) => value * (Math.PI / 180);
      const earthRadiusMeters = 6371000;
      const deltaLat = toRadians(toPoint.lat - fromPoint.lat);
      const deltaLng = toRadians(toPoint.lng - fromPoint.lng);
      const lat1 = toRadians(fromPoint.lat);
      const lat2 = toRadians(toPoint.lat);

      const a = (Math.sin(deltaLat / 2) ** 2)
        + (Math.cos(lat1) * Math.cos(lat2) * (Math.sin(deltaLng / 2) ** 2));
      const boundedA = Math.min(1, Math.max(0, a));
      const c = 2 * Math.atan2(Math.sqrt(boundedA), Math.sqrt(1 - boundedA));
      return earthRadiusMeters * c;
    };

    const toUtcDayStartMs = (timeMs) => {
      if (!Number.isFinite(timeMs) || timeMs <= 0) return null;
      const date = new Date(timeMs);
      if (Number.isNaN(date.getTime())) return null;
      return Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate());
    };

    const toLocalDayStartMs = (timeMs) => {
      if (!Number.isFinite(timeMs) || timeMs <= 0) return null;
      const date = new Date(timeMs);
      if (Number.isNaN(date.getTime())) return null;
      return new Date(date.getFullYear(), date.getMonth(), date.getDate()).getTime();
    };

    const selectParkingSpotRecordsByDay = (
      records = [],
      {
        getRecordSerial = () => '',
        blacklistedWirelessSerials = new Map()
      } = {}
    ) => {
      if (!Array.isArray(records) || !records.length || typeof getRecordSerial !== 'function') return [];

      const dayBuckets = new Map();
      const fallbackWiredRecords = [];
      const fallbackWirelessRecords = [];
      const blacklistedSerials = (
        blacklistedWirelessSerials instanceof Set
        || blacklistedWirelessSerials instanceof Map
      )
        ? blacklistedWirelessSerials
        : new Map();

      records.forEach((record) => {
        const serial = normalizeGpsSerial(getRecordSerial(record));
        if (!serial) return;
        const timeMs = parseGpsTrailTimeMs(record);
        const isWired = isWiredGpsSerial(serial);
        const isWirelessAllowed = (
          !isWired
          && !isGpsDeviceSerialBlacklistedAt(serial, timeMs, {
            blacklistBySerial: blacklistedSerials
          })
        );
        if (!isWired && !isWirelessAllowed) return;

        if (isWired) {
          fallbackWiredRecords.push(record);
        } else {
          fallbackWirelessRecords.push(record);
        }

        const dayStartMs = toUtcDayStartMs(timeMs);
        if (!Number.isFinite(dayStartMs)) return;

        const bucket = dayBuckets.get(dayStartMs) || { wired: [], wireless: [] };
        if (isWired) {
          bucket.wired.push(record);
        } else if (isWirelessAllowed) {
          bucket.wireless.push(record);
        }
        dayBuckets.set(dayStartMs, bucket);
      });

      if (!dayBuckets.size) {
        return fallbackWiredRecords.length ? fallbackWiredRecords : fallbackWirelessRecords;
      }

      const selectedRecords = [];
      const seenRecords = new Set();
      [...dayBuckets.keys()].sort((a, b) => a - b).forEach((dayStartMs) => {
        const bucket = dayBuckets.get(dayStartMs);
        if (!bucket) return;
        const preferredRecords = bucket.wired.length ? bucket.wired : bucket.wireless;
        preferredRecords.forEach((record) => {
          if (!record || seenRecords.has(record)) return;
          seenRecords.add(record);
          selectedRecords.push(record);
        });
      });

      return selectedRecords;
    };

    const buildHotspotSessions = (points = []) => {
      if (!Array.isArray(points) || !points.length) return [];
      const sortedPoints = [...points].sort((a, b) => a.timestamp - b.timestamp);
      const dayBuckets = new Map();

      sortedPoints.forEach((point) => {
        const pointTimeMs = Number.isFinite(point.timeMs) ? point.timeMs : null;
        const dayStartMs = toUtcDayStartMs(pointTimeMs);
        if (!Number.isFinite(dayStartMs)) return;
        const existing = dayBuckets.get(dayStartMs) || {
          dayStartMs,
          points: 0,
          firstMs: pointTimeMs,
          lastMs: pointTimeMs
        };
        existing.points += 1;
        if (!Number.isFinite(existing.firstMs) || pointTimeMs < existing.firstMs) existing.firstMs = pointTimeMs;
        if (!Number.isFinite(existing.lastMs) || pointTimeMs > existing.lastMs) existing.lastMs = pointTimeMs;
        dayBuckets.set(dayStartMs, existing);
      });

      // If timestamps are missing, keep a conservative fallback: one 1-day visit per point.
      if (!dayBuckets.size) {
        return sortedPoints.map((point) => {
          const pointTimeMs = Number.isFinite(point.timeMs) ? point.timeMs : null;
          return {
            points: 1,
            activeDays: 1,
            startMs: pointTimeMs,
            endMs: pointTimeMs,
            durationMs: GPS_HISTORY_DAY_MS
          };
        });
      }

      const sortedDays = [...dayBuckets.values()].sort((a, b) => a.dayStartMs - b.dayStartMs);
      const sessions = [];
      let activeSession = null;

      const pushSession = () => {
        if (!activeSession) return;
        const startMs = Number.isFinite(activeSession.startMs) ? activeSession.startMs : null;
        const endMs = Number.isFinite(activeSession.endMs) ? activeSession.endMs : null;
        const observedDurationMs = startMs !== null && endMs !== null && endMs >= startMs
          ? endMs - startMs
          : 0;
        const activeDays = Math.max(1, activeSession.activeDays);
        const inferredDurationMs = activeDays * GPS_HISTORY_DAY_MS;
        sessions.push({
          points: activeSession.points,
          activeDays,
          startMs,
          endMs,
          durationMs: Math.max(observedDurationMs, inferredDurationMs)
        });
        activeSession = null;
      };

      sortedDays.forEach((dayBucket) => {
        if (!activeSession) {
          activeSession = {
            points: dayBucket.points,
            activeDays: 1,
            startMs: dayBucket.firstMs,
            endMs: dayBucket.lastMs,
            lastDayStartMs: dayBucket.dayStartMs
          };
          return;
        }

        const dayGap = Math.round((dayBucket.dayStartMs - activeSession.lastDayStartMs) / GPS_HISTORY_DAY_MS);
        const isConsecutiveDay = dayGap <= 1;
        if (!isConsecutiveDay) {
          pushSession();
          activeSession = {
            points: dayBucket.points,
            activeDays: 1,
            startMs: dayBucket.firstMs,
            endMs: dayBucket.lastMs,
            lastDayStartMs: dayBucket.dayStartMs
          };
          return;
        }

        activeSession.points += dayBucket.points;
        activeSession.activeDays += 1;
        activeSession.startMs = Number.isFinite(activeSession.startMs)
          ? Math.min(activeSession.startMs, dayBucket.firstMs)
          : dayBucket.firstMs;
        activeSession.endMs = Number.isFinite(activeSession.endMs)
          ? Math.max(activeSession.endMs, dayBucket.lastMs)
          : dayBucket.lastMs;
        activeSession.lastDayStartMs = dayBucket.dayStartMs;
      });

      pushSession();
      return sessions;
    };

    const buildVehicleHistoryHotspots = (
      records = [],
      {
        getRecordSerial = () => '',
        serialCountsBySerial = null
      } = {}
    ) => {
      if (!Array.isArray(records) || !records.length) return [];

      // Hotspot analysis consumes the scoped record set chosen by the caller.
      const sortedRecords = [...records]
        .sort((a, b) => parseGpsTrailTimestamp(a) - parseGpsTrailTimestamp(b));
      const resolvedSerialCounts = serialCountsBySerial instanceof Map
        ? serialCountsBySerial
        : countGpsHistoryRecordsBySerial(sortedRecords, getRecordSerial);

      const points = sortedRecords
        .map((record) => {
          const trailPoint = toGpsTrailPoint(record);
          if (!trailPoint) return null;
          const rawRecordSerial = `${getRecordSerial(record) || ''}`.trim();
          const recordSerial = normalizeGpsSerial(rawRecordSerial);
          const readingCountForSerial = recordSerial
            ? Number(resolvedSerialCounts.get(recordSerial) || resolvedSerialCounts.get(rawRecordSerial) || 0)
            : 0;
          return {
            ...trailPoint,
            serial: recordSerial,
            movingStatus: readingCountForSerial <= 1
              ? 'unknown'
              : parseGpsRecordMovingStatus(record)
          };
        })
        .filter(Boolean);

      if (!points.length) return [];

      // Parking spots require multi-day stays (readings on different days in the same location cluster).
      const sourcePoints = points;

      if (!sourcePoints.length) return [];

      const clusters = [];

      sourcePoints.forEach((point) => {
        let selectedCluster = null;
        let selectedDistance = Number.POSITIVE_INFINITY;

        clusters.forEach((cluster) => {
          const distance = getGpsPointDistanceMeters(point, cluster.center);
          if (distance < selectedDistance) {
            selectedDistance = distance;
            selectedCluster = cluster;
          }
        });

        if (!selectedCluster || selectedDistance > GPS_HISTORY_HOTSPOT_CLUSTER_RADIUS_METERS) {
          clusters.push({
            center: { lat: point.lat, lng: point.lng },
            points: [point],
            firstSeen: point.timestamp,
            lastSeen: point.timestamp
          });
          return;
        }

        const previousCount = selectedCluster.points.length;
        selectedCluster.points.push(point);
        const nextCount = previousCount + 1;
        selectedCluster.center.lat = ((selectedCluster.center.lat * previousCount) + point.lat) / nextCount;
        selectedCluster.center.lng = ((selectedCluster.center.lng * previousCount) + point.lng) / nextCount;
        selectedCluster.firstSeen = Math.min(selectedCluster.firstSeen, point.timestamp);
        selectedCluster.lastSeen = Math.max(selectedCluster.lastSeen, point.timestamp);
      });

      const computeHotspotScore = ({
        uniqueDays = 0,
        visits = 0,
        pingCount = 0,
        stoppedCount = 0,
        totalDurationMs = 0,
        longestDurationDays = 0
      } = {}) => {
        const safeUniqueDays = Number.isFinite(uniqueDays) ? uniqueDays : 0;
        const safeVisits = Number.isFinite(visits) ? visits : 0;
        const safePingCount = Number.isFinite(pingCount) ? pingCount : 0;
        const safeStoppedCount = Number.isFinite(stoppedCount) ? stoppedCount : 0;
        const safeTotalDurationHours = Number.isFinite(totalDurationMs) ? (totalDurationMs / (1000 * 60 * 60)) : 0;
        const safeLongestDurationDays = Number.isFinite(longestDurationDays) ? longestDurationDays : 0;
        return (safeUniqueDays * 7)
          + (safeVisits * 4)
          + (Math.min(safePingCount, 250) * 1.35)
          + (safeStoppedCount * 1.5)
          + (Math.min(safeTotalDurationHours, 240) * 1.2)
          + (Math.min(safeLongestDurationDays, 60) * 8.5);
      };

      const computeParkingHierarchyScore = ({
        visits = 0,
        totalParkedDays = 0
      } = {}) => {
        const safeVisits = Number.isFinite(visits) ? Math.max(0, visits) : 0;
        const safeTotalParkedDays = Number.isFinite(totalParkedDays) ? Math.max(0, totalParkedDays) : 0;
        return (safeVisits * GPS_HISTORY_HOTSPOT_HIERARCHY_VISIT_WEIGHT)
          + (safeTotalParkedDays * GPS_HISTORY_HOTSPOT_HIERARCHY_TOTAL_DAYS_WEIGHT);
      };

      const resolveParkingHierarchyLevel = (ratio = 0) => {
        if (Number.isFinite(ratio) && ratio >= GPS_HISTORY_HOTSPOT_LEVEL_PRIMARY_THRESHOLD) {
          return { code: 'L1', label: 'Primary parking' };
        }
        if (Number.isFinite(ratio) && ratio >= GPS_HISTORY_HOTSPOT_LEVEL_SECONDARY_THRESHOLD) {
          return { code: 'L2', label: 'Frequent parking' };
        }
        return { code: 'L3', label: 'Occasional parking' };
      };

      const normalizedHotspots = clusters
        .map((cluster, index) => {
          const sessions = buildHotspotSessions(cluster.points);
          const sessionDurations = sessions.map((session) => session.durationMs).filter((value) => Number.isFinite(value) && value >= 0);
          const totalDurationMs = sessionDurations.reduce((sum, durationMs) => sum + durationMs, 0);
          const avgDurationMs = sessionDurations.length ? totalDurationMs / sessionDurations.length : 0;
          const longestDurationMs = sessionDurations.length ? Math.max(...sessionDurations) : 0;
          const latestSession = sessions[sessions.length - 1] || null;
          const firstSeenTimeMs = Number.isFinite(sessions[0]?.startMs) ? sessions[0].startMs : cluster.firstSeen;
          const lastSeenTimeMs = Number.isFinite(latestSession?.endMs) ? latestSession.endMs : cluster.lastSeen;
          const lastStayDurationMs = Number.isFinite(latestSession?.durationMs) ? latestSession.durationMs : 0;
          const spreadMeters = cluster.points.reduce((maxDistance, point) => {
            const pointDistance = getGpsPointDistanceMeters(point, cluster.center);
            return Math.max(maxDistance, pointDistance);
          }, 0);
          const radiusMeters = Math.max(80, Math.min(280, Math.round((spreadMeters * 2.2) + 70)));
          const stoppedCount = cluster.points.filter((point) => point.movingStatus === 'stopped').length;
          const visits = sessions.length || 1;
          const uniqueDayKeys = new Set(
            cluster.points
              .map((point) => {
                if (!Number.isFinite(point?.timeMs) || point.timeMs <= 0) return '';
                return new Date(point.timeMs).toISOString().slice(0, 10);
              })
              .filter(Boolean)
          );
          const uniqueDays = uniqueDayKeys.size;
          const pingCount = cluster.points.length;
          const serialCounts = new Map();
          cluster.points.forEach((point) => {
            const serial = normalizeGpsSerial(point?.serial);
            if (!serial) return;
            serialCounts.set(serial, (serialCounts.get(serial) || 0) + 1);
          });
          const serials = [...serialCounts.entries()]
            .sort((a, b) => {
              if (b[1] !== a[1]) return b[1] - a[1];
              return a[0].localeCompare(b[0]);
            })
            .map(([serial, pings]) => ({
              serial,
              pings,
              type: isWiredGpsSerial(serial) ? 'wired' : 'wireless'
            }));
          const longestDurationDays = longestDurationMs / GPS_HISTORY_DAY_MS;
          const totalParkedDays = totalDurationMs / GPS_HISTORY_DAY_MS;
          const longestStayActiveDays = sessions.reduce((maxDays, session) => {
            const activeDays = Number.isFinite(session?.activeDays) ? session.activeDays : 0;
            return Math.max(maxDays, activeDays);
          }, 0);
          const multiDayStayCount = sessions.filter((session) => (
            Number.isFinite(session?.activeDays)
            && session.activeDays >= GPS_HISTORY_HOTSPOT_MIN_CONSECUTIVE_DAYS
          )).length;
          const score = computeHotspotScore({
            uniqueDays,
            visits,
            pingCount,
            stoppedCount,
            totalDurationMs,
            longestDurationDays
          });
          const hierarchyScore = computeParkingHierarchyScore({
            visits,
            totalParkedDays
          });

          return {
            id: `vehicle-history-hotspot-${index + 1}`,
            lat: cluster.center.lat,
            lng: cluster.center.lng,
            points: cluster.points.length,
            pingCount,
            serials,
            uniqueDays,
            visits,
            stoppedCount,
            firstSeen: firstSeenTimeMs,
            lastSeen: lastSeenTimeMs,
            radiusMeters,
            totalDurationMs,
            totalParkedDays,
            avgDurationMs,
            longestDurationMs,
            longestDurationDays,
            lastStayDurationMs,
            longestStayActiveDays,
            multiDayStayCount,
            score,
            hierarchyScore
          };
        });

      const strictHotspots = normalizedHotspots.filter((hotspot) => {
        const hasEnoughPoints = hotspot.points >= 2;
        const hasMinConsecutiveDays = hotspot.longestStayActiveDays >= GPS_HISTORY_HOTSPOT_MIN_CONSECUTIVE_DAYS;
        const hasMinMultiDayStays = hotspot.multiDayStayCount >= GPS_HISTORY_HOTSPOT_MIN_MULTIDAY_STAYS;
        return hasEnoughPoints && hasMinConsecutiveDays && hasMinMultiDayStays;
      });

      let candidates = strictHotspots;
      candidates = candidates
        .sort((a, b) => {
          if (b.hierarchyScore !== a.hierarchyScore) return b.hierarchyScore - a.hierarchyScore;
          if (b.visits !== a.visits) return b.visits - a.visits;
          if (b.totalParkedDays !== a.totalParkedDays) return b.totalParkedDays - a.totalParkedDays;
          if (b.score !== a.score) return b.score - a.score;
          if (b.lastSeen !== a.lastSeen) return b.lastSeen - a.lastSeen;
          return b.points - a.points;
        })
        .slice(0, GPS_HISTORY_HOTSPOT_MAX);

      const topHierarchyScore = candidates.reduce((bestScore, hotspot) => {
        const value = Number.isFinite(hotspot?.hierarchyScore) ? hotspot.hierarchyScore : 0;
        return Math.max(bestScore, value);
      }, 0);

      return candidates.map((hotspot, index) => {
        const hierarchyRatio = topHierarchyScore > 0
          ? hotspot.hierarchyScore / topHierarchyScore
          : 0;
        const hierarchyLevel = resolveParkingHierarchyLevel(hierarchyRatio);
        return {
          ...hotspot,
          hierarchyRank: index + 1,
          hierarchyRatio,
          hierarchyLevel: hierarchyLevel.code,
          hierarchyLevelLabel: hierarchyLevel.label
        };
      });
    };

    const buildVehicleHistoryHotspotPopupKey = (vehicleId, hotspot, index = 0) => {
      const keyVehicleId = `${vehicleId ?? 'vehicle'}`.trim() || 'vehicle';
      const latBucket = Number.isFinite(Number(hotspot?.lat)) ? Math.round(Number(hotspot.lat) * 10000) : 0;
      const lngBucket = Number.isFinite(Number(hotspot?.lng)) ? Math.round(Number(hotspot.lng) * 10000) : 0;
      return `vehicle-hotspot-${keyVehicleId}-${index + 1}-${latBucket}-${lngBucket}`;
    };

    const buildVehicleHistoryHotspotSummary = async (vehicle = {}) => {
      const vehicleId = `${vehicle?.id ?? ''}`.trim();
      if (!vehicleId) return [];

      const now = Date.now();
      const cached = vehicleHistoryHotspotCache.get(vehicleId);
      if (cached && (now - cached.updatedAt) <= VEHICLE_HISTORY_HOTSPOT_CACHE_TTL_MS) {
        return cached.hotspots;
      }

      const pending = vehicleHistoryHotspotPendingRequests.get(vehicleId);
      if (pending) return pending;

      const request = (async () => {
        const vin = gpsHistoryManager.getVehicleVin(vehicle);
        const sourceVehicleId = gpsHistoryManager.getVehicleId(vehicle);
        if (!vin && !sourceVehicleId) return [];

        const { records, error } = await gpsHistoryManager.fetchGpsHistory({ vin, vehicleId: sourceVehicleId });
        if (error || !Array.isArray(records) || !records.length) {
          vehicleHistoryHotspotCache.set(vehicleId, { updatedAt: Date.now(), hotspots: [] });
          return [];
        }

        const blacklistedWirelessSerials = await getGpsDeviceBlacklistSerials();
        const winnerSerial = typeof gpsHistoryManager.resolveVehicleWinnerSerialFromRecords === 'function'
          ? gpsHistoryManager.resolveVehicleWinnerSerialFromRecords(vehicle, records)
          : gpsHistoryManager.getVehicleWinnerSerial(vehicle);
        const getRecordSerial = typeof gpsHistoryManager.getRecordSerial === 'function'
          ? gpsHistoryManager.getRecordSerial
          : () => '';
        const serialCountsBySerial = countGpsHistoryRecordsBySerial(records, getRecordSerial);
        const winnerScopedRecords = winnerSerial
          ? records.filter((record) => getRecordSerial(record) === winnerSerial)
          : records;

        let hotspotSourceRecords = selectParkingSpotRecordsByDay(records, {
          getRecordSerial,
          blacklistedWirelessSerials
        });
        if (!hotspotSourceRecords.length) {
          hotspotSourceRecords = winnerScopedRecords.length ? winnerScopedRecords : records;
        }

        const hotspots = buildVehicleHistoryHotspots(hotspotSourceRecords, {
          getRecordSerial,
          serialCountsBySerial
        }).map((hotspot, index) => ({
          ...hotspot,
          sourceVehicleId: vehicle.id,
          popupKey: buildVehicleHistoryHotspotPopupKey(vehicle.id, hotspot, index)
        }));

        vehicleHistoryHotspotCache.set(vehicleId, {
          updatedAt: Date.now(),
          hotspots
        });
        return hotspots;
      })().catch((error) => {
        console.warn('Vehicle hotspot summary warning: ' + (error?.message || error));
        return [];
      }).finally(() => {
        vehicleHistoryHotspotPendingRequests.delete(vehicleId);
      });

      vehicleHistoryHotspotPendingRequests.set(vehicleId, request);
      return request;
    };

    const mapWithConcurrency = async (items = [], worker, concurrency = 4) => {
      if (!Array.isArray(items) || !items.length || typeof worker !== 'function') return [];
      const boundedConcurrency = Math.max(1, Math.min(concurrency, items.length));
      const results = new Array(items.length);
      let cursor = 0;

      const runWorker = async () => {
        while (cursor < items.length) {
          const currentIndex = cursor;
          cursor += 1;
          try {
            results[currentIndex] = await worker(items[currentIndex], currentIndex);
          } catch (error) {
            results[currentIndex] = null;
            console.warn('Hotspot relation scan warning: ' + (error?.message || error));
          }
        }
      };

      await Promise.all(Array.from({ length: boundedConcurrency }, () => runWorker()));
      return results;
    };

    const toMiles = (meters) => {
      if (!Number.isFinite(meters) || meters < 0) return null;
      return meters / 1609.344;
    };

    const buildGoogleMapsSearchUrl = (lat, lng) => {
      const latValue = Number(lat);
      const lngValue = Number(lng);
      if (!Number.isFinite(latValue) || !Number.isFinite(lngValue)) return '';
      const query = `${latValue.toFixed(6)},${lngValue.toFixed(6)}`;
      return `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(query)}`;
    };

    const buildGoogleMapsLinkHtml = (
      lat,
      lng,
      {
        label = 'Google Maps',
        tight = false,
      } = {}
    ) => {
      const href = buildGoogleMapsSearchUrl(lat, lng);
      if (!href) return '';
      const className = tight ? 'map-coord-link-btn is-tight' : 'map-coord-link-btn';
      return `<a class="${className}" href="${escapeHTML(href)}" target="_blank" rel="noopener noreferrer">${escapeHTML(label)}</a>`;
    };

    const buildRelatedHotspotCacheKey = (sourceVehicleId, hotspot) => {
      const source = `${sourceVehicleId ?? ''}`.trim() || 'none';
      const latBucket = Number.isFinite(Number(hotspot?.lat)) ? Number(hotspot.lat).toFixed(4) : '0';
      const lngBucket = Number.isFinite(Number(hotspot?.lng)) ? Number(hotspot.lng).toFixed(4) : '0';
      const radiusBucket = Number.isFinite(Number(hotspot?.radiusMeters)) ? Math.round(Number(hotspot.radiusMeters)) : 0;
      return `${source}:${latBucket}:${lngBucket}:${radiusBucket}`;
    };

    const detectCoordinatePairsFromKeys = (keys = []) => {
      const keySet = new Set(keys);
      return HOTSPOT_FAST_QUERY_COORDINATE_COLUMNS.filter((pair) => keySet.has(pair.lat) && keySet.has(pair.lng));
    };

    const setHotspotFastCoordinatePairsFromRecords = (records = []) => {
      if (!Array.isArray(records) || !records.length) return;
      const rowWithKeys = records.find((row) => row && typeof row === 'object' && Object.keys(row).length);
      if (!rowWithKeys) return;
      const detectedPairs = detectCoordinatePairsFromKeys(Object.keys(rowWithKeys));
      if (detectedPairs.length) {
        hotspotFastCoordinatePairs = detectedPairs;
      }
    };

    const resolveHotspotFastCoordinatePairs = async () => {
      if (hotspotFastCoordinatePairs.length) return hotspotFastCoordinatePairs;
      if (hotspotFastCoordinatePairLookupPromise) return hotspotFastCoordinatePairLookupPromise;
      if (!supabaseClient?.from) return [];

      hotspotFastCoordinatePairLookupPromise = (async () => {
        try {
          const { data, error } = await runWithTimeout(
            supabaseClient
              .from(TABLES.gpsHistory)
              .select('*')
              .limit(1),
            SUPABASE_TIMEOUT_MS,
            'GPS hotspot metadata lookup timed out.'
          );
          if (error) return [];
          const firstRow = Array.isArray(data) ? data[0] : null;
          if (!firstRow || typeof firstRow !== 'object') return [];
          const detectedPairs = detectCoordinatePairsFromKeys(Object.keys(firstRow));
          if (detectedPairs.length) {
            hotspotFastCoordinatePairs = detectedPairs;
          }
          return hotspotFastCoordinatePairs;
        } catch (_) {
          return [];
        } finally {
          hotspotFastCoordinatePairLookupPromise = null;
        }
      })();

      return hotspotFastCoordinatePairLookupPromise;
    };

    const metersToLatitudeDegrees = (meters) => {
      if (!Number.isFinite(meters) || meters <= 0) return 0;
      return meters / 111320;
    };

    const metersToLongitudeDegrees = (meters, latitude) => {
      if (!Number.isFinite(meters) || meters <= 0) return 0;
      const latRad = (Number(latitude) || 0) * (Math.PI / 180);
      const cosLat = Math.max(0.2, Math.abs(Math.cos(latRad)));
      return meters / (111320 * cosLat);
    };

    const buildHotspotBounds = (hotspot, radiusMeters) => {
      if (!hotspot || !Number.isFinite(Number(hotspot.lat)) || !Number.isFinite(Number(hotspot.lng))) return null;
      const lat = Number(hotspot.lat);
      const lng = Number(hotspot.lng);
      const latDelta = metersToLatitudeDegrees(radiusMeters);
      const lngDelta = metersToLongitudeDegrees(radiusMeters, lat);
      return {
        minLat: lat - latDelta,
        maxLat: lat + latDelta,
        minLng: lng - lngDelta,
        maxLng: lng + lngDelta,
      };
    };

    const queryGpsHistoryByBounds = async ({ bounds, latColumn, lngColumn } = {}) => {
      if (!supabaseClient?.from || !bounds || !latColumn || !lngColumn) return [];

      const records = [];
      const maxRows = HOTSPOT_FAST_QUERY_MAX_ROWS;
      let offset = 0;
      while (offset < maxRows) {
        const upper = Math.min(offset + HOTSPOT_FAST_QUERY_PAGE_SIZE - 1, maxRows - 1);
        const query = supabaseClient
          .from(TABLES.gpsHistory)
          .select('*')
          .gte(latColumn, bounds.minLat)
          .lte(latColumn, bounds.maxLat)
          .gte(lngColumn, bounds.minLng)
          .lte(lngColumn, bounds.maxLng)
          .range(offset, upper);

        const { data, error } = await runWithTimeout(
          query,
          SUPABASE_TIMEOUT_MS,
          'GPS hotspot lookup timed out.'
        );
        if (error) throw error;

        const pageRows = Array.isArray(data) ? data : [];
        if (!pageRows.length) break;
        records.push(...pageRows);
        if (pageRows.length < HOTSPOT_FAST_QUERY_PAGE_SIZE) break;
        offset += HOTSPOT_FAST_QUERY_PAGE_SIZE;
      }

      return records;
    };

    const buildVehicleIdentityIndexes = (vehicleList = []) => {
      const vehiclesById = new Map();
      const vehiclesByUniqueVin = new Map();
      const ambiguousVins = new Set();

      vehicleList.forEach((vehicle) => {
        const vehicleId = `${vehicle?.id ?? ''}`.trim();
        if (vehicleId) vehiclesById.set(vehicleId, vehicle);

        const vin = getVehicleVin(vehicle);
        if (!vin) return;
        if (ambiguousVins.has(vin)) return;
        if (vehiclesByUniqueVin.has(vin)) {
          vehiclesByUniqueVin.delete(vin);
          ambiguousVins.add(vin);
          return;
        }
        vehiclesByUniqueVin.set(vin, vehicle);
      });

      return { vehiclesById, vehiclesByUniqueVin, ambiguousVins };
    };

    const isVehicleVinAmbiguous = (vehicle = {}, ambiguousVins = new Set()) => {
      const vin = getVehicleVin(vehicle);
      return Boolean(vin && ambiguousVins.has(vin));
    };

    const resolveVehicleFromGpsRecord = (record = {}, vehiclesById = new Map(), vehiclesByUniqueVin = new Map()) => {
      const recordVehicleId = `${record?.vehicle_id ?? record?.vehicleId ?? ''}`.trim();
      if (recordVehicleId && vehiclesById.has(recordVehicleId)) {
        return vehiclesById.get(recordVehicleId) || null;
      }

      const recordVin = normalizeVin(record?.VIN ?? record?.vin ?? '');
      if (recordVin && vehiclesByUniqueVin.has(recordVin)) {
        return vehiclesByUniqueVin.get(recordVin) || null;
      }

      return null;
    };

    const findVehiclesSharingHistoryHotspotFast = async ({ hotspot, sourceVehicleId } = {}) => {
      if (!hotspot || !supabaseClient?.from) return null;

      const sourceId = `${sourceVehicleId ?? ''}`.trim();
      const matchRadiusMeters = Math.max(
        HOTSPOT_SHARED_MATCH_RADIUS_METERS,
        Number(hotspot?.radiusMeters) || 0,
        GPS_HISTORY_HOTSPOT_CLUSTER_RADIUS_METERS
      );
      const bounds = buildHotspotBounds(hotspot, matchRadiusMeters);
      if (!bounds) return null;

      const coordinatePairs = hotspotFastCoordinatePairs.length
        ? hotspotFastCoordinatePairs
        : await resolveHotspotFastCoordinatePairs();
      if (!Array.isArray(coordinatePairs) || !coordinatePairs.length) return null;

      let nearbyRecords = [];
      let hadQueryError = false;
      for (const columnPair of coordinatePairs) {
        try {
          nearbyRecords = await queryGpsHistoryByBounds({
            bounds,
            latColumn: columnPair.lat,
            lngColumn: columnPair.lng
          });
          hotspotFastCoordinatePairs = [columnPair, ...coordinatePairs.filter((pair) => pair !== columnPair)];
          break;
        } catch (_) {
          hadQueryError = true;
          continue;
        }
      }

      if (!nearbyRecords.length) {
        return hadQueryError ? null : [];
      }

      const { vehiclesById, vehiclesByUniqueVin } = buildVehicleIdentityIndexes(vehicles);

      const grouped = new Map();
      nearbyRecords.forEach((record) => {
        const point = toGpsTrailPoint(record);
        if (!point) return;
        const distanceMeters = getGpsPointDistanceMeters(
          { lat: hotspot.lat, lng: hotspot.lng },
          { lat: point.lat, lng: point.lng }
        );
        if (!Number.isFinite(distanceMeters) || distanceMeters > matchRadiusMeters) return;

        const vehicle = resolveVehicleFromGpsRecord(record, vehiclesById, vehiclesByUniqueVin);
        if (!vehicle) return;
        if (`${vehicle?.id ?? ''}` === sourceId) return;

        const vehicleKey = `${vehicle.id}`;
        if (!vehicleKey) return;

        const existing = grouped.get(vehicleKey) || {
          vehicle,
          vehicleId: vehicle.id,
          pingCount: 0,
          sumDistanceMeters: 0,
          uniqueDayKeys: new Set(),
          lastSeen: Number.NEGATIVE_INFINITY
        };

        existing.pingCount += 1;
        existing.sumDistanceMeters += distanceMeters;
        if (Number.isFinite(point.timeMs) && point.timeMs > 0) {
          existing.uniqueDayKeys.add(new Date(point.timeMs).toISOString().slice(0, 10));
        }
        existing.lastSeen = Math.max(existing.lastSeen, point.timestamp);
        grouped.set(vehicleKey, existing);
      });

      const matches = [...grouped.values()]
        .map((entry) => {
          const uniqueDays = entry.uniqueDayKeys.size;
          const estimatedVisits = Math.max(1, uniqueDays || Math.round(entry.pingCount / 3) || 1);
          return {
            vehicle: entry.vehicle,
            vehicleId: entry.vehicleId,
            distanceMeters: entry.pingCount ? (entry.sumDistanceMeters / entry.pingCount) : Number.POSITIVE_INFINITY,
            visits: estimatedVisits,
            uniqueDays,
            pingCount: entry.pingCount,
            lastSeen: entry.lastSeen
          };
        })
        .sort((a, b) => {
          if (b.pingCount !== a.pingCount) return b.pingCount - a.pingCount;
          if (b.uniqueDays !== a.uniqueDays) return b.uniqueDays - a.uniqueDays;
          if (a.distanceMeters !== b.distanceMeters) return a.distanceMeters - b.distanceMeters;
          return b.lastSeen - a.lastSeen;
        })
        .slice(0, HOTSPOT_RELATED_VEHICLE_LIMIT)
        .map(({ vehicle, vehicleId, distanceMeters, visits, uniqueDays }) => ({
          vehicle,
          vehicleId,
          distanceMeters,
          visits,
          uniqueDays
        }));

      return matches;
    };

    const findVehiclesSharingHistoryHotspot = async ({ hotspot, sourceVehicleId } = {}) => {
      if (!hotspot) return [];
      const sourceId = `${sourceVehicleId ?? ''}`.trim();
      const cacheKey = buildRelatedHotspotCacheKey(sourceId, hotspot);
      const now = Date.now();
      const { ambiguousVins } = buildVehicleIdentityIndexes(vehicles);
      const cached = relatedHotspotVehiclesCache.get(cacheKey);
      if (cached && (now - cached.updatedAt) <= VEHICLE_HISTORY_HOTSPOT_CACHE_TTL_MS) {
        return cached.matches
          .map((entry) => {
            const vehicle = vehicles.find((item) => `${item?.id ?? ''}` === `${entry.vehicleId}`);
            if (!vehicle) return null;
            if (isVehicleVinAmbiguous(vehicle, ambiguousVins)) return null;
            return { ...entry, vehicle };
          })
          .filter(Boolean);
      }

      const fastMatches = await findVehiclesSharingHistoryHotspotFast({
        hotspot,
        sourceVehicleId: sourceId
      });
      if (Array.isArray(fastMatches)) {
        relatedHotspotVehiclesCache.set(cacheKey, {
          updatedAt: now,
          matches: fastMatches.map((match) => ({
            vehicleId: match.vehicleId,
            distanceMeters: match.distanceMeters,
            visits: match.visits,
            uniqueDays: match.uniqueDays
          }))
        });
        return fastMatches;
      }

      const candidates = vehicles
        .filter((vehicle) =>
          `${vehicle?.id ?? ''}`.trim() !== ''
          && `${vehicle?.id ?? ''}` !== sourceId
          && !isVehicleVinAmbiguous(vehicle, ambiguousVins)
        )
        .map((vehicle) => {
          if (!hasValidCoords(vehicle)) {
            return { vehicle, proximity: Number.POSITIVE_INFINITY };
          }
          return {
            vehicle,
            proximity: getGpsPointDistanceMeters(
              { lat: vehicle.lat, lng: vehicle.lng },
              { lat: hotspot.lat, lng: hotspot.lng }
            )
          };
        })
        .sort((a, b) => a.proximity - b.proximity)
        .slice(0, HOTSPOT_RELATED_CANDIDATE_LIMIT)
        .map((entry) => entry.vehicle);

      if (!candidates.length) return [];

      const scanned = await mapWithConcurrency(
        candidates,
        async (candidateVehicle) => {
          const candidateHotspots = await buildVehicleHistoryHotspotSummary(candidateVehicle);
          if (!candidateHotspots.length) return null;

          let bestMatch = null;
          let bestDistanceMeters = Number.POSITIVE_INFINITY;
          candidateHotspots.forEach((candidateHotspot) => {
            const distanceMeters = getGpsPointDistanceMeters(
              { lat: hotspot.lat, lng: hotspot.lng },
              { lat: candidateHotspot.lat, lng: candidateHotspot.lng }
            );
            const matchRadiusMeters = Math.max(
              HOTSPOT_SHARED_MATCH_RADIUS_METERS,
              Number(hotspot.radiusMeters) || 0,
              Number(candidateHotspot.radiusMeters) || 0,
              GPS_HISTORY_HOTSPOT_CLUSTER_RADIUS_METERS
            );
            if (!Number.isFinite(distanceMeters) || distanceMeters > matchRadiusMeters) return;
            if (distanceMeters < bestDistanceMeters) {
              bestDistanceMeters = distanceMeters;
              bestMatch = candidateHotspot;
            }
          });

          if (!bestMatch) return null;
          return {
            vehicle: candidateVehicle,
            vehicleId: candidateVehicle.id,
            distanceMeters: bestDistanceMeters,
            visits: bestMatch.visits,
            uniqueDays: bestMatch.uniqueDays
          };
        },
        HOTSPOT_RELATED_CONCURRENCY
      );

      const matches = scanned
        .filter(Boolean)
        .sort((a, b) => a.distanceMeters - b.distanceMeters)
        .slice(0, HOTSPOT_RELATED_VEHICLE_LIMIT);

      relatedHotspotVehiclesCache.set(cacheKey, {
        updatedAt: Date.now(),
        matches: matches.map((match) => ({
          vehicleId: match.vehicleId,
          distanceMeters: match.distanceMeters,
          visits: match.visits,
          uniqueDays: match.uniqueDays
        }))
      });

      return matches;
    };

    const prefetchRelatedVehiclesForHotspots = (hotspots = [], sourceVehicleId = '') => {
      if (!Array.isArray(hotspots) || !hotspots.length) return;
      const sourceId = `${sourceVehicleId ?? ''}`.trim();

      hotspots.slice(0, Math.min(3, hotspots.length)).forEach((hotspot, index) => {
        setTimeout(() => {
          void findVehiclesSharingHistoryHotspotFast({
            hotspot,
            sourceVehicleId: sourceId
          }).then((matches) => {
            if (!Array.isArray(matches)) return;
            const cacheKey = buildRelatedHotspotCacheKey(sourceId, hotspot);
            relatedHotspotVehiclesCache.set(cacheKey, {
              updatedAt: Date.now(),
              matches: matches.map((match) => ({
                vehicleId: match.vehicleId,
                distanceMeters: match.distanceMeters,
                visits: match.visits,
                uniqueDays: match.uniqueDays
              }))
            });
          }).catch(() => {
            // Silent by design: prefetch is best-effort only.
          });
        }, 110 * (index + 1));
      });
    };

    const getGpsTrailBearingDegrees = (fromPoint, toPoint) => {
      const toRadians = (value) => value * (Math.PI / 180);
      const lat1 = toRadians(fromPoint.lat);
      const lat2 = toRadians(toPoint.lat);
      const deltaLng = toRadians(toPoint.lng - fromPoint.lng);
      const y = Math.sin(deltaLng) * Math.cos(lat2);
      const x = (Math.cos(lat1) * Math.sin(lat2))
        - (Math.sin(lat1) * Math.cos(lat2) * Math.cos(deltaLng));
      let bearing = Math.atan2(y, x) * (180 / Math.PI);
      if (!Number.isFinite(bearing)) return 0;
      if (bearing < 0) bearing += 360;
      return bearing;
    };

    const isGpsRecordDateField = (key = '') => {
      const normalized = `${key}`.toLowerCase();
      return (
        normalized.includes('date')
        || normalized.includes('time')
        || normalized.includes('ping')
        || normalized.endsWith('_at')
      );
    };

    const formatGpsRecordFieldValue = (key, value) => {
      if (value === null || value === undefined || value === '') return '—';
      if (typeof value === 'object') return escapeHTML(JSON.stringify(value));

      if (isGpsRecordDateField(key)) {
        const parsed = Date.parse(value);
        if (!Number.isNaN(parsed)) return escapeHTML(formatDateTime(value));
      }

      return escapeHTML(`${value}`);
    };

    const buildGpsTrailRecordPopup = (point, index, total) => {
      const record = point?.record && typeof point.record === 'object' ? point.record : {};
      const entries = Object.entries(record);
      const coordText = `${Number(point?.lat || 0).toFixed(5)}, ${Number(point?.lng || 0).toFixed(5)}`;
      const timestampText = Number.isFinite(point?.timeMs) ? formatDateTime(point.timeMs) : 'Insufficient data';
      const mapsLinkHtml = buildGoogleMapsLinkHtml(point?.lat, point?.lng, { tight: true });

      if (!entries.length) {
        return `
          <div class="gps-record-card">
            <p class="gps-record-title">GPS Record ${index + 1}/${total}</p>
            <p class="gps-record-meta">Time: ${escapeHTML(timestampText)}</p>
            <p class="gps-record-meta">Coords: ${escapeHTML(coordText)}</p>
            ${mapsLinkHtml}
            <p class="gps-record-empty">No columns available for this point.</p>
          </div>
        `;
      }

      const rows = entries.map(([key, value]) => `
        <tr>
          <th>${escapeHTML(key)}</th>
          <td>${formatGpsRecordFieldValue(key, value)}</td>
        </tr>
      `).join('');

      return `
        <div class="gps-record-card">
          <p class="gps-record-title">GPS Record ${index + 1}/${total}</p>
          <p class="gps-record-meta">Time: ${escapeHTML(timestampText)}</p>
          <p class="gps-record-meta">Coords: ${escapeHTML(coordText)}</p>
          ${mapsLinkHtml}
          <div class="gps-record-table-wrap">
            <table class="gps-record-table">
              <tbody>${rows}</tbody>
            </table>
          </div>
        </div>
      `;
    };

    const clampUnitInterval = (value) => {
      if (!Number.isFinite(value)) return 0;
      if (value < 0) return 0;
      if (value > 1) return 1;
      return value;
    };

    const parseTrailColorToRgb = (value, fallback = { r: 20, g: 83, b: 45 }) => {
      if (typeof value !== 'string') return fallback;
      const normalized = value.trim();
      if (!normalized) return fallback;

      if (normalized.startsWith('#')) {
        const hex = normalized.slice(1);
        if (hex.length === 3) {
          const r = Number.parseInt(hex[0] + hex[0], 16);
          const g = Number.parseInt(hex[1] + hex[1], 16);
          const b = Number.parseInt(hex[2] + hex[2], 16);
          if (Number.isFinite(r) && Number.isFinite(g) && Number.isFinite(b)) return { r, g, b };
        }
        if (hex.length === 6) {
          const r = Number.parseInt(hex.slice(0, 2), 16);
          const g = Number.parseInt(hex.slice(2, 4), 16);
          const b = Number.parseInt(hex.slice(4, 6), 16);
          if (Number.isFinite(r) && Number.isFinite(g) && Number.isFinite(b)) return { r, g, b };
        }
      }

      const rgbMatch = normalized.match(/^rgba?\((\d{1,3})\s*,\s*(\d{1,3})\s*,\s*(\d{1,3})(?:\s*,\s*[\d.]+)?\)$/i);
      if (!rgbMatch) return fallback;
      const r = Number.parseInt(rgbMatch[1], 10);
      const g = Number.parseInt(rgbMatch[2], 10);
      const b = Number.parseInt(rgbMatch[3], 10);
      if (!Number.isFinite(r) || !Number.isFinite(g) || !Number.isFinite(b)) return fallback;
      return {
        r: Math.max(0, Math.min(255, r)),
        g: Math.max(0, Math.min(255, g)),
        b: Math.max(0, Math.min(255, b))
      };
    };

    const interpolateNumber = (fromValue, toValue, progress) => {
      const boundedProgress = clampUnitInterval(progress);
      return fromValue + ((toValue - fromValue) * boundedProgress);
    };

    const interpolateTrailRgb = (fromColor, toColor, progress) => {
      return {
        r: Math.round(interpolateNumber(fromColor.r, toColor.r, progress)),
        g: Math.round(interpolateNumber(fromColor.g, toColor.g, progress)),
        b: Math.round(interpolateNumber(fromColor.b, toColor.b, progress))
      };
    };

    const rgbToHex = ({ r, g, b }) => {
      const toHex = (value) => Math.max(0, Math.min(255, Number(value) || 0)).toString(16).padStart(2, '0');
      return `#${toHex(r)}${toHex(g)}${toHex(b)}`;
    };

    const rgbToRgba = ({ r, g, b }, alpha) => `rgba(${Math.max(0, Math.min(255, Math.round(r)))},${Math.max(0, Math.min(255, Math.round(g)))},${Math.max(0, Math.min(255, Math.round(b)))},${clampUnitInterval(alpha).toFixed(3)})`;
    const MILES_TO_KILOMETERS = MILES_TO_METERS / 1000;
    const formatTrailSegmentDistanceKm = (fromPoint, toPoint) => {
      if (!fromPoint || !toPoint) return '';
      const miles = getDistance(fromPoint.lat, fromPoint.lng, toPoint.lat, toPoint.lng);
      if (!Number.isFinite(miles) || miles < 0) return '';
      const kilometers = miles * MILES_TO_KILOMETERS;
      if (kilometers >= 100) return `${Math.round(kilometers)} km`;
      if (kilometers >= 1) return `${kilometers.toFixed(1)} km`;
      return `${kilometers.toFixed(2)} km`;
    };

    const formatTrailElapsedMs = (elapsedMs) => {
      if (!Number.isFinite(elapsedMs) || elapsedMs <= 0) return '0s';

      const totalSeconds = Math.round(elapsedMs / 1000);
      const days = Math.floor(totalSeconds / 86400);
      const hours = Math.floor((totalSeconds % 86400) / 3600);
      const minutes = Math.floor((totalSeconds % 3600) / 60);
      const seconds = totalSeconds % 60;
      const parts = [];
      if (days > 0) parts.push(`${days}d`);
      if (hours > 0) parts.push(`${hours}h`);
      if (minutes > 0 && parts.length < 2) parts.push(`${minutes}m`);
      if (!parts.length) parts.push(`${seconds}s`);
      return parts.slice(0, 2).join(' ');
    };

    const formatTrailSegmentElapsedTime = (fromPoint, toPoint) => {
      const fromTime = Number(fromPoint?.timeMs);
      const toTime = Number(toPoint?.timeMs);
      if (!Number.isFinite(fromTime) || !Number.isFinite(toTime)) return '';
      return formatTrailElapsedMs(Math.abs(toTime - fromTime));
    };

    const parseGpsRecordDaysParked = (record = {}) => {
      if (!record || typeof record !== 'object') return null;
      const candidates = [
        record?.days_stationary,
        record?.days_parked,
        record?.days_stationary_calc,
        record?.DaysParked,
        record?.['Days Parked'],
        record?.['Days Stationary']
      ];
      for (const candidate of candidates) {
        const parsed = parseGpsNumericValue(candidate);
        if (Number.isFinite(parsed)) return Math.max(0, Math.floor(parsed));
      }
      return null;
    };

    const getTrailPointDayStartMs = (point = {}) => {
      const timeMs = Number.isFinite(point?.timeMs) ? point.timeMs : point?.timestamp;
      return toLocalDayStartMs(timeMs);
    };

    const getTrailPointMovementStatus = (
      point = {},
      previousPoint = null,
      {
        stoppedDistanceThreshold = GPS_MOVING_STOPPED_MAX_SEGMENT_METERS
      } = {}
    ) => {
      const explicit = parseGpsRecordMovingStatus(point?.record || {});
      if (explicit === 'moving' || explicit === 'stopped') return explicit;

      const explicitDaysParked = parseGpsRecordDaysParked(point?.record || {});
      if (Number.isFinite(explicitDaysParked)) {
        return explicitDaysParked <= 0 ? 'moving' : 'stopped';
      }

      if (previousPoint) {
        const segmentDistance = getGpsPointDistanceMeters(previousPoint, point);
        if (Number.isFinite(segmentDistance)) {
          return segmentDistance <= stoppedDistanceThreshold ? 'stopped' : 'moving';
        }
      }

      return 'unknown';
    };

    const buildTrailDerivedParkedDays = (
      points = [],
      {
        stoppedDistanceThreshold = GPS_MOVING_STOPPED_MAX_SEGMENT_METERS
      } = {}
    ) => {
      if (!Array.isArray(points) || !points.length) return [];
      const result = new Array(points.length).fill(null);
      let stationaryStartDayMs = null;

      points.forEach((point, index) => {
        const previousPoint = index > 0 ? points[index - 1] : null;
        const movement = getTrailPointMovementStatus(point, previousPoint, {
          stoppedDistanceThreshold
        });
        const dayStartMs = getTrailPointDayStartMs(point);

        if (movement === 'moving') {
          stationaryStartDayMs = null;
          result[index] = 0;
          return;
        }

        if (movement !== 'stopped' || !Number.isFinite(dayStartMs)) {
          result[index] = null;
          return;
        }

        if (!Number.isFinite(stationaryStartDayMs)) {
          stationaryStartDayMs = dayStartMs;
        }

        result[index] = Math.max(
          0,
          Math.floor((dayStartMs - stationaryStartDayMs) / GPS_HISTORY_DAY_MS)
        );
      });

      return result;
    };

    const buildTrailParkedStartTimeByPoint = (
      points = [],
      {
        stoppedDistanceThreshold = GPS_MOVING_STOPPED_MAX_SEGMENT_METERS
      } = {}
    ) => {
      if (!Array.isArray(points) || !points.length) return [];
      const parkedStartByIndex = new Array(points.length).fill(null);
      let parkedStartMs = null;

      points.forEach((point, index) => {
        const previousPoint = index > 0 ? points[index - 1] : null;
        const movement = getTrailPointMovementStatus(point, previousPoint, {
          stoppedDistanceThreshold
        });
        const pointTimeMs = Number(point?.timeMs);

        if (movement !== 'stopped') {
          parkedStartMs = null;
          parkedStartByIndex[index] = null;
          return;
        }

        if (!Number.isFinite(parkedStartMs) && Number.isFinite(pointTimeMs)) {
          parkedStartMs = pointTimeMs;
        }
        parkedStartByIndex[index] = parkedStartMs;
      });

      return parkedStartByIndex;
    };

    const drawVehicleGpsTrail = (points = [], vehicleColor = '', vehicle = null) => {
      if (!gpsTrailLayer || !Array.isArray(points) || !points.length) return;
      const stoppedDistanceThreshold = getStoppedDistanceThresholdForVehicle(vehicle || {});
      const oldestLineRgb = parseTrailColorToRgb('#14532d');
      const oldestFillRgb = parseTrailColorToRgb('#166534', oldestLineRgb);
      const newestVehicleRgb = parseTrailColorToRgb(vehicleColor, oldestLineRgb);
      const oldestLabelRgb = parseTrailColorToRgb('#334155');
      const newestLabelRgb = parseTrailColorToRgb('#14532d');
      const oldestLabelTextRgb = parseTrailColorToRgb('#cbd5e1');
      const newestLabelTextRgb = parseTrailColorToRgb('#dcfce7');
      const pointStepDenominator = Math.max(1, points.length - 1);
      const segmentStepDenominator = Math.max(1, points.length - 1);
      const derivedParkedDaysByPoint = buildTrailDerivedParkedDays(points, {
        stoppedDistanceThreshold
      });
      const parkedStartTimeByPoint = buildTrailParkedStartTimeByPoint(points, {
        stoppedDistanceThreshold
      });
      const segmentLabelEntries = [];

      for (let index = 0; index < points.length - 1; index += 1) {
        const fromPoint = points[index];
        const toPoint = points[index + 1];
        const segmentProgress = clampUnitInterval((index + 0.5) / segmentStepDenominator);
        const segmentRgb = interpolateTrailRgb(oldestLineRgb, newestVehicleRgb, segmentProgress);
        const segmentLine = L.polyline([
          [fromPoint.lat, fromPoint.lng],
          [toPoint.lat, toPoint.lng]
        ], {
          color: rgbToHex(segmentRgb),
          weight: interpolateNumber(2.25, 2.85, segmentProgress),
          opacity: interpolateNumber(0.56, 0.8, segmentProgress),
          dashArray: '4 6',
          lineCap: 'round',
          lineJoin: 'round',
          interactive: false
        }).addTo(gpsTrailLayer);
        segmentLine.bringToBack();
      }

      points.forEach((point, index) => {
        const pointProgress = clampUnitInterval(index / pointStepDenominator);
        const pointStrokeRgb = interpolateTrailRgb(oldestLineRgb, newestVehicleRgb, pointProgress);
        const pointFillRgb = interpolateTrailRgb(oldestFillRgb, newestVehicleRgb, pointProgress);
        const pointMarker = L.circleMarker([point.lat, point.lng], {
          radius: interpolateNumber(3.9, 5.6, pointProgress),
          color: rgbToHex(pointStrokeRgb),
          weight: interpolateNumber(1.2, 1.45, pointProgress),
          fillColor: rgbToHex(pointFillRgb),
          fillOpacity: interpolateNumber(0.28, 0.45, pointProgress),
          opacity: interpolateNumber(0.54, 0.76, pointProgress),
          interactive: true,
          className: 'gps-trail-point',
          cycleRole: 'route-point',
          cycleKey: `route-point-${index}`
        }).addTo(gpsTrailLayer);

        pointMarker.on('click', (event) => {
          if (event?.originalEvent) {
            event.originalEvent.handledByMarker = true;
            event.originalEvent.cycleRole = pointMarker.options?.cycleRole || 'route-point';
            event.originalEvent.cycleKey = pointMarker.options?.cycleKey || `route-point-${index}`;
          }
        });
        pointMarker.bindPopup(buildGpsTrailRecordPopup(point, index, points.length), {
          className: 'gps-record-popup',
          autoPan: true,
          maxWidth: 430
        });
      });

      for (let index = 0; index < points.length - 1; index += 1) {
        const fromPoint = points[index];
        const toPoint = points[index + 1];
        const midpoint = {
          lat: (fromPoint.lat + toPoint.lat) / 2,
          lng: (fromPoint.lng + toPoint.lng) / 2
        };
        const segmentProgress = clampUnitInterval((index + 0.5) / segmentStepDenominator);
        const arrowRgb = interpolateTrailRgb(oldestLineRgb, newestVehicleRgb, segmentProgress);
        const arrowColor = rgbToRgba(arrowRgb, interpolateNumber(0.5, 0.82, segmentProgress));
        const bearing = getGpsTrailBearingDegrees(fromPoint, toPoint);
        const segmentVisualPriority = 520 + index;
        L.marker([midpoint.lat, midpoint.lng], {
          icon: L.divIcon({
            className: 'gps-trail-arrow',
            html: `<span style="display:block;transform:rotate(${bearing}deg);transform-origin:center;color:${arrowColor};font-size:12px;line-height:12px;text-shadow:0 0 2px rgba(2,6,23,0.72)">▲</span>`,
            iconSize: [12, 12],
            iconAnchor: [6, 6]
          }),
          interactive: false,
          zIndexOffset: segmentVisualPriority
        }).addTo(gpsTrailLayer);

        const segmentDistanceKm = formatTrailSegmentDistanceKm(fromPoint, toPoint);
        if (!segmentDistanceKm) continue;
        const toPointIndex = index + 1;
        const toPointMovement = getTrailPointMovementStatus(toPoint, fromPoint, {
          stoppedDistanceThreshold
        });
        const explicitDaysParked = parseGpsRecordDaysParked(toPoint?.record || {});
        const derivedDaysParked = Number.isFinite(derivedParkedDaysByPoint[toPointIndex])
          ? derivedParkedDaysByPoint[toPointIndex]
          : null;
        const resolvedDaysParked = derivedDaysParked ?? explicitDaysParked;
        const parkedStartMs = parkedStartTimeByPoint[toPointIndex];
        const toPointTimeMs = Number(toPoint?.timeMs);
        const parkedElapsedMs = (
          toPointMovement === 'stopped'
          && Number.isFinite(parkedStartMs)
          && Number.isFinite(toPointTimeMs)
        )
          ? Math.max(0, toPointTimeMs - parkedStartMs)
          : null;
        const parkedElapsedWithDaysMs = Number.isFinite(resolvedDaysParked)
          ? Math.max(parkedElapsedMs || 0, resolvedDaysParked * GPS_HISTORY_DAY_MS)
          : parkedElapsedMs;
        const segmentElapsedTime = toPointMovement === 'stopped'
          ? formatTrailElapsedMs(parkedElapsedWithDaysMs)
          : formatTrailSegmentElapsedTime(fromPoint, toPoint);
        const parkedSuffix = toPointMovement === 'stopped'
          ? ` · Parked${Number.isFinite(resolvedDaysParked) ? ` ${resolvedDaysParked}d` : ''}`
          : '';
        const segmentLabel = segmentElapsedTime
          ? `${segmentDistanceKm} · ${segmentElapsedTime}`
          : segmentDistanceKm;
        const segmentLabelWithParked = `${segmentLabel}${parkedSuffix}`;
        const labelToneRgb = interpolateTrailRgb(oldestLabelRgb, newestLabelRgb, segmentProgress);
        const labelTextRgb = interpolateTrailRgb(oldestLabelTextRgb, newestLabelTextRgb, segmentProgress);
        const labelBorder = rgbToRgba(labelToneRgb, interpolateNumber(0.46, 0.9, segmentProgress));
        const labelBackground = rgbToRgba(labelToneRgb, interpolateNumber(0.24, 0.58, segmentProgress));
        const labelTextColor = rgbToHex(labelTextRgb);
        const labelShadow = rgbToRgba(labelToneRgb, interpolateNumber(0.2, 0.48, segmentProgress));
        const labelFontWeight = Math.round(interpolateNumber(690, 820, segmentProgress));
        const labelMarker = L.marker([midpoint.lat, midpoint.lng], {
          icon: L.divIcon({
            className: 'gps-trail-distance',
            html: `<span class="gps-trail-distance-wrap"><span class="gps-trail-distance-badge" style="border-color:${labelBorder};background:${labelBackground};color:${labelTextColor};font-weight:${labelFontWeight};box-shadow:0 0 0 1px ${labelShadow};">${segmentLabelWithParked}</span></span>`,
            iconSize: [1, 1],
            iconAnchor: [0, 0]
          }),
          interactive: false,
          zIndexOffset: segmentVisualPriority + 1
        }).addTo(gpsTrailLayer);

        const previousEntryIndex = segmentLabelEntries.findIndex((entry) => {
          const distance = getGpsPointDistanceMeters(
            { lat: entry.lat, lng: entry.lng },
            midpoint
          );
          return Number.isFinite(distance) && distance <= GPS_TRAIL_LABEL_MERGE_RADIUS_METERS;
        });

        if (previousEntryIndex >= 0) {
          const previousEntry = segmentLabelEntries[previousEntryIndex];
          if (previousEntry?.marker) {
            gpsTrailLayer.removeLayer(previousEntry.marker);
          }
          segmentLabelEntries[previousEntryIndex] = {
            lat: midpoint.lat,
            lng: midpoint.lng,
            marker: labelMarker
          };
        } else {
          segmentLabelEntries.push({
            lat: midpoint.lat,
            lng: midpoint.lng,
            marker: labelMarker
          });
        }
      }
    };

    const formatHotspotDuration = (durationMs) => {
      if (!Number.isFinite(durationMs) || durationMs <= 0) return 'Insufficient data';
      const totalMinutes = Math.max(1, Math.round(durationMs / 60000));
      const days = Math.floor(totalMinutes / (24 * 60));
      const hours = Math.floor((totalMinutes % (24 * 60)) / 60);
      const minutes = totalMinutes % 60;
      const parts = [];
      if (days) parts.push(`${days}d`);
      if (hours) parts.push(`${hours}h`);
      if (minutes || !parts.length) parts.push(`${minutes}m`);
      return parts.join(' ');
    };

    const formatHotspotDateTime = (value) => {
      if (!Number.isFinite(value) || value <= 0) return 'Insufficient data';
      return formatDateTime(value);
    };

    const formatHotspotTotalParkedDays = (value) => {
      if (!Number.isFinite(value) || value <= 0) return '0.0d';
      return `${Math.max(0, value).toFixed(1)}d`;
    };

    const formatHotspotHierarchyScore = (value) => {
      if (!Number.isFinite(value) || value <= 0) return '0';
      return `${Math.round(value)}`;
    };

    const buildVehicleHistoryHotspotPopup = (hotspot, index = 0) => {
      const rank = Number.isFinite(Number(hotspot?.hierarchyRank))
        ? Math.max(1, Number(hotspot.hierarchyRank))
        : (index + 1);
      const hierarchyLevel = `${hotspot?.hierarchyLevel || 'L3'}`.trim().toUpperCase() || 'L3';
      const hierarchyLevelLabel = `${hotspot?.hierarchyLevelLabel || 'Occasional parking'}`.trim() || 'Occasional parking';
      const hierarchyLevelClass = hierarchyLevel === 'L1'
        ? 'is-l1'
        : (hierarchyLevel === 'L2' ? 'is-l2' : 'is-l3');
      const coords = `${Number(hotspot.lat).toFixed(5)}, ${Number(hotspot.lng).toFixed(5)}`;
      const serialsLabel = (Array.isArray(hotspot?.serials) ? hotspot.serials : [])
        .map((entry) => {
          const serial = normalizeGpsSerial(entry?.serial);
          if (!serial) return '';
          const serialType = `${entry?.type || ''}`.trim().toLowerCase() === 'wired' ? 'Wired' : 'Wireless';
          const pingCount = Number(entry?.pings);
          const pingLabel = Number.isFinite(pingCount) && pingCount > 0 ? `, ${pingCount} pings` : '';
          return `${serial} (${serialType}${pingLabel})`;
        })
        .filter(Boolean)
        .join(', ');
      const popupKey = hotspot.popupKey || buildVehicleHistoryHotspotPopupKey(hotspot.sourceVehicleId, hotspot, index);
      const mapsLinkHtml = buildGoogleMapsLinkHtml(hotspot.lat, hotspot.lng, { tight: true });
      return `
        <div class="vehicle-history-hotspot-card">
          <p class="vehicle-history-hotspot-card-title">
            <span>Parking Spot #${rank}</span>
            <span class="vehicle-history-hotspot-level ${hierarchyLevelClass}">${escapeHTML(`${hierarchyLevel} · ${hierarchyLevelLabel}`)}</span>
          </p>
          <div class="vehicle-history-hotspot-metrics">
            <p><span class="vehicle-history-hotspot-k">Pings</span><span class="vehicle-history-hotspot-v">${hotspot.pingCount || hotspot.points}</span></p>
            <p><span class="vehicle-history-hotspot-k">Active days</span><span class="vehicle-history-hotspot-v">${hotspot.uniqueDays || 0}</span></p>
            <p><span class="vehicle-history-hotspot-k">Visits</span><span class="vehicle-history-hotspot-v">${hotspot.visits}</span></p>
            <p><span class="vehicle-history-hotspot-k">Total parked</span><span class="vehicle-history-hotspot-v">${formatHotspotTotalParkedDays(hotspot.totalParkedDays)}</span></p>
            <p><span class="vehicle-history-hotspot-k">Avg stay</span><span class="vehicle-history-hotspot-v">${formatHotspotDuration(hotspot.avgDurationMs)}</span></p>
            <p><span class="vehicle-history-hotspot-k">Longest</span><span class="vehicle-history-hotspot-v">${formatHotspotDuration(hotspot.longestDurationMs)}</span></p>
            <p><span class="vehicle-history-hotspot-k">Latest</span><span class="vehicle-history-hotspot-v">${formatHotspotDuration(hotspot.lastStayDurationMs)}</span></p>
            <p><span class="vehicle-history-hotspot-k">Hierarchy</span><span class="vehicle-history-hotspot-v">${formatHotspotHierarchyScore(hotspot.hierarchyScore)}</span></p>
          </div>
          <div class="vehicle-history-hotspot-meta">
            <p><span>First</span><span>${formatHotspotDateTime(hotspot.firstSeen)}</span></p>
            <p><span>Last</span><span>${formatHotspotDateTime(hotspot.lastSeen)}</span></p>
            <p><span>Serial(s)</span><span class="vehicle-history-hotspot-serial-text">${escapeHTML(serialsLabel || 'Insufficient data')}</span></p>
            <p><span>Coords</span><span>${coords}</span></p>
            ${mapsLinkHtml}
          </div>
          <div class="vehicle-history-hotspot-related-wrap">
            <p class="vehicle-history-hotspot-related-title">Other vehicles using this parking spot</p>
            <div class="vehicle-history-hotspot-related-body" data-hotspot-related-container data-hotspot-key="${escapeHTML(popupKey)}">
              <p class="vehicle-history-hotspot-related-loading">Searching related vehicles...</p>
            </div>
          </div>
        </div>
      `;
    };

    const buildHotspotRelatedVehicleMarkup = (matches = []) => {
      if (!Array.isArray(matches) || !matches.length) {
        return '<p class="vehicle-history-hotspot-related-empty">No other vehicles found for this parking spot.</p>';
      }

      return `
        <div class="vehicle-history-hotspot-related-list">
          ${matches.map((entry) => {
            const vehicle = entry?.vehicle || {};
            const vehicleLabel = [vehicle.model || 'Vehicle', vehicle.year || ''].filter(Boolean).join(' ').trim() || 'Vehicle';
            const vehicleVin = vehicle.vin || vehicle.VIN || 'VIN N/A';
            const vehicleCustomer = vehicle.customerId
              || vehicle.customer
              || vehicle.customer_id
              || vehicle.details?.customer_id
              || vehicle.details?.Customer
              || '—';
            const distanceMiles = toMiles(entry?.distanceMeters);
            const distanceLabel = distanceMiles === null ? '—' : `${distanceMiles.toFixed(2)} mi`;
            const visits = Number.isFinite(Number(entry?.visits)) ? Number(entry.visits) : 0;
            const uniqueDays = Number.isFinite(Number(entry?.uniqueDays)) ? Number(entry.uniqueDays) : 0;
            return `
              <button
                type="button"
                class="vehicle-history-hotspot-related-btn"
                data-action="hotspot-related-vehicle"
                data-vehicle-key="${escapeHTML(getVehicleKey(vehicle))}"
                data-vehicle-id="${escapeHTML(`${vehicle.id}`)}"
              >
                <span class="vehicle-history-hotspot-related-btn-title">${escapeHTML(vehicleLabel)}</span>
                <span class="vehicle-history-hotspot-related-btn-vin">${escapeHTML(vehicleVin)}</span>
                <span class="vehicle-history-hotspot-related-btn-customer">Customer: ${escapeHTML(`${vehicleCustomer}`)}</span>
                <span class="vehicle-history-hotspot-related-btn-meta">${escapeHTML(distanceLabel)} · ${visits} visits · ${uniqueDays} active days</span>
              </button>
            `;
          }).join('')}
        </div>
      `;
    };

    const revealVehicleCardByKey = (vehicleKey) => {
      const list = document.getElementById('vehicle-list');
      if (!list) return;
      const card = [...list.querySelectorAll('[data-type="vehicle"]')]
        .find((node) => (
          `${node?.dataset?.vehicleKey ?? ''}` === `${vehicleKey}`
          || `${node?.dataset?.id ?? ''}` === `${vehicleKey}`
        ));
      if (!card) return;
      card.scrollIntoView({ behavior: 'smooth', block: 'center' });
      card.classList.add('vehicle-hotspot-related-target');
      window.setTimeout(() => {
        card.classList.remove('vehicle-hotspot-related-target');
      }, 1300);
    };

    const focusMapOnVehicleQuick = (vehicle, { targetZoom = 14, preserveZoom = false } = {}) => {
      if (!map || !vehicle) return;
      const vehicleCoords = getVehicleMapCoords(vehicle);

      const destination = L.latLng(vehicleCoords.lat, vehicleCoords.lng);
      const currentCenter = typeof map.getCenter === 'function' ? map.getCenter() : null;
      const currentZoom = typeof map.getZoom === 'function' ? map.getZoom() : targetZoom;
      const distanceMeters = currentCenter ? map.distance(currentCenter, destination) : 0;
      const zoomDelta = Math.abs((Number.isFinite(currentZoom) ? currentZoom : targetZoom) - targetZoom);
      const preferDirectSetView = distanceMeters > 90000 || zoomDelta >= 2;

      if (typeof map.stop === 'function') {
        map.stop();
      }

      if (preserveZoom) {
        map.panTo(destination, {
          animate: true,
          duration: 0.38,
          easeLinearity: 0.35,
          noMoveStart: true
        });
        return;
      }

      if (preferDirectSetView) {
        map.setView(destination, targetZoom, {
          animate: true,
          duration: 0.42,
          easeLinearity: 0.35,
          noMoveStart: true
        });
        return;
      }

      map.flyTo(destination, targetZoom, {
        duration: 0.52,
        easeLinearity: 0.35,
        noMoveStart: true
      });
    };

    const jumpToVehicleFromHotspotRelation = (vehicleKey = '', vehicleId = '') => {
      const vehicle = findVehicleByKey(vehicleKey)
        || vehicles.find((item) => `${item?.id ?? ''}` === `${vehicleId ?? ''}`);
      if (!vehicle) return false;

      clearParkingSpotCascade();
      if (map) {
        const currentZoom = typeof map.getZoom === 'function' ? map.getZoom() : 14;
        focusMapOnVehicleQuick(vehicle, {
          targetZoom: currentZoom,
          preserveZoom: true
        });
      }
      if (vehicleMarkersVisible) {
        focusVehicle(vehicle);
      } else {
        applySelection(vehicle.id, null, getVehicleKey(vehicle));
      }
      requestAnimationFrame(() => revealVehicleCardByKey(getVehicleKey(vehicle)));
      return true;
    };

    const renderHotspotRelatedVehicles = async (popup, hotspot) => {
      const popupElement = popup?.getElement?.();
      if (!popupElement || !hotspot) return;
      const container = popupElement.querySelector('[data-hotspot-related-container]');
      if (!container) return;

      if (container.dataset.relationBound !== '1') {
        container.dataset.relationBound = '1';
        container.addEventListener('click', (event) => {
          const target = event.target instanceof Element
            ? event.target.closest('[data-action="hotspot-related-vehicle"]')
            : null;
          if (!target) return;
          event.preventDefault();
          event.stopPropagation();
          const targetVehicleKey = `${target.dataset.vehicleKey || ''}`.trim();
          const targetVehicleId = `${target.dataset.vehicleId || ''}`.trim();
          if (!targetVehicleKey && !targetVehicleId) return;
          jumpToVehicleFromHotspotRelation(targetVehicleKey, targetVehicleId);
        });
      }

      const requestToken = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
      container.dataset.requestToken = requestToken;
      container.innerHTML = '<p class="vehicle-history-hotspot-related-loading">Searching related vehicles...</p>';

      try {
        const sourceVehicleId = hotspot.sourceVehicleId ?? selectedVehicleId;
        const matches = await findVehiclesSharingHistoryHotspot({
          hotspot,
          sourceVehicleId
        });
        if (container.dataset.requestToken !== requestToken || !container.isConnected) return;
        container.innerHTML = buildHotspotRelatedVehicleMarkup(matches);
      } catch (error) {
        console.warn('Related hotspot vehicle lookup warning: ' + (error?.message || error));
        if (container.dataset.requestToken !== requestToken || !container.isConnected) return;
        container.innerHTML = '<p class="vehicle-history-hotspot-related-error">Unable to load related vehicles for this spot.</p>';
      }
    };

    const drawVehicleHistoryHotspots = (hotspots = []) => {
      if (!highlightLayer || !Array.isArray(hotspots) || !hotspots.length) return;

      hotspots.forEach((hotspot, index) => {
        const hotspotStrength = Math.min(1, hotspot.points / 12);
        const hierarchyRatioRaw = Number(hotspot?.hierarchyRatio);
        const hierarchyRatio = Number.isFinite(hierarchyRatioRaw)
          ? Math.max(0, Math.min(1, hierarchyRatioRaw))
          : 0;
        const visualStrength = Math.max(hotspotStrength, hierarchyRatio * 0.82);
        const hotspotLevel = `${hotspot?.hierarchyLevel || ''}`.trim().toUpperCase();
        const hotspotColor = hotspotLevel === 'L1'
          ? '#22d3ee'
          : (hotspotLevel === 'L2' ? GPS_HISTORY_HOTSPOT_COLOR : '#475569');
        const visualRadiusMeters = Math.max(300, Math.round(hotspot.radiusMeters * (1.55 + (visualStrength * 0.55))));
        const popupHtml = buildVehicleHistoryHotspotPopup(hotspot, index);

        const halo = L.circle([hotspot.lat, hotspot.lng], {
          radius: Math.round(visualRadiusMeters * 1.35),
          color: hotspotColor,
          weight: 0.9,
          fillColor: hotspotColor,
          fillOpacity: 0.02 + (visualStrength * 0.03),
          opacity: 0.18 + (visualStrength * 0.08),
          interactive: false,
          className: 'vehicle-history-hotspot-glass-halo'
        }).addTo(highlightLayer);
        halo.bringToBack();

        const ring = L.circle([hotspot.lat, hotspot.lng], {
          radius: visualRadiusMeters,
          color: hotspotColor,
          weight: 1.2 + (visualStrength * 0.6),
          fillColor: hotspotColor,
          fillOpacity: 0.05 + (visualStrength * 0.06),
          opacity: 0.42 + (visualStrength * 0.14),
          interactive: true,
          className: 'vehicle-history-hotspot-ring',
          cycleRole: 'parking-spot',
          cycleKey: `parking-spot-${hotspot.id || index + 1}`
        }).addTo(highlightLayer);

        ring.on('click', (event) => {
          if (event?.originalEvent) {
            event.originalEvent.handledByMarker = true;
            event.originalEvent.cycleRole = ring.options?.cycleRole || 'parking-spot';
            event.originalEvent.cycleKey = ring.options?.cycleKey || `parking-spot-${hotspot.id || index + 1}`;
          }
        });

        ring.bindPopup(popupHtml, {
          className: 'vehicle-history-hotspot-popup',
          autoPan: true
        });
        ring.on('popupopen', (event) => {
          armParkingSpotCascade();
          void renderHotspotRelatedVehicles(event?.popup, hotspot);
        });

        const coreHalo = L.circleMarker([hotspot.lat, hotspot.lng], {
          radius: 11 + (visualStrength * 2),
          color: hotspotColor,
          weight: 1.2,
          fillColor: hotspotColor,
          fillOpacity: 0.14,
          opacity: 0.45,
          interactive: false,
          className: 'vehicle-history-hotspot-core-halo'
        }).addTo(highlightLayer);

        const core = L.circleMarker([hotspot.lat, hotspot.lng], {
          radius: 7.8 + (visualStrength * 1.8),
          color: '#cbd5e1',
          weight: 1.8,
          fillColor: hotspotColor,
          fillOpacity: 0.45,
          opacity: 0.72,
          interactive: true,
          className: 'vehicle-history-hotspot-core',
          cycleRole: 'parking-spot',
          cycleKey: `parking-spot-${hotspot.id || index + 1}`
        }).addTo(highlightLayer);

        core.on('click', (event) => {
          if (event?.originalEvent) {
            event.originalEvent.handledByMarker = true;
            event.originalEvent.cycleRole = core.options?.cycleRole || 'parking-spot';
            event.originalEvent.cycleKey = core.options?.cycleKey || `parking-spot-${hotspot.id || index + 1}`;
          }
        });

        core.bindPopup(popupHtml, {
          className: 'vehicle-history-hotspot-popup',
          autoPan: true
        });
        core.on('popupopen', (event) => {
          armParkingSpotCascade();
          void renderHotspotRelatedVehicles(event?.popup, hotspot);
        });

        coreHalo.bringToFront();
        core.bringToFront();
      });
    };

    const syncFocusedVehicleNotMovingBadge = (vehicle) => {
      if (!highlightLayer) return;

      const staleBadgeLayers = [];
      highlightLayer.eachLayer((layer) => {
        const layerIconClass = `${layer?.options?.icon?.options?.className || ''}`.trim();
        if (layerIconClass === 'vehicle-cross') staleBadgeLayers.push(layer);
      });
      staleBadgeLayers.forEach((layer) => highlightLayer.removeLayer(layer));

      if (!vehicle || !hasValidCoords(vehicle) || !isVehicleNotMoving(vehicle)) return;
      L.marker([vehicle.lat, vehicle.lng], {
        icon: L.divIcon({ className: 'vehicle-cross', html: '<div class="vehicle-cross-badge">✕</div>', iconSize: [16, 16], iconAnchor: [8, 8] }),
        interactive: false,
        zIndexOffset: 500
      }).addTo(highlightLayer);
    };

    const renderVehicleGpsTrail = async (vehicle, requestId) => {
      if (!vehicle || !gpsTrailLayer || requestId !== gpsTrailRequestCounter) return;

      const vin = gpsHistoryManager.getVehicleVin(vehicle);
      const vehicleId = gpsHistoryManager.getVehicleId(vehicle);
      if (!vin && !vehicleId) return;

      const { records, error } = await gpsHistoryManager.fetchGpsHistory({ vin, vehicleId });
      if (requestId !== gpsTrailRequestCounter) return;
      if (error || !Array.isArray(records) || !records.length) return;
      setHotspotFastCoordinatePairsFromRecords(records);

      const blacklistedWirelessSerials = await getGpsDeviceBlacklistSerials();
      const configuredWinnerSerial = gpsHistoryManager.getVehicleWinnerSerial(vehicle);
      const winnerSerial = typeof gpsHistoryManager.resolveVehicleWinnerSerialFromRecords === 'function'
        ? gpsHistoryManager.resolveVehicleWinnerSerialFromRecords(vehicle, records)
        : configuredWinnerSerial;
      const getRecordSerial = typeof gpsHistoryManager.getRecordSerial === 'function'
        ? gpsHistoryManager.getRecordSerial
        : () => '';
      const winnerScopedRecords = winnerSerial
        ? records.filter((record) => getRecordSerial(record) === winnerSerial)
        : records;

      const serialCountsBySerial = countGpsHistoryRecordsBySerial(records, getRecordSerial);
      const movingOverrideChanged = applyVehicleMovingOverrideFromGpsHistory(vehicle, records);
      const latestRecords = [...records]
        .sort((a, b) => parseGpsTrailTimestamp(b) - parseGpsTrailTimestamp(a))
        .slice(0, getCurrentTrailPointLimit());

      const trailPoints = latestRecords
        .map((record) => {
          const point = toGpsTrailPoint(record);
          if (!point) return null;
          return { ...point, record };
        })
        .filter((point) => point !== null)
        .sort((a, b) => a.timestamp - b.timestamp);

      let hotspotSourceRecords = selectParkingSpotRecordsByDay(records, {
        getRecordSerial,
        blacklistedWirelessSerials
      });
      if (!hotspotSourceRecords.length) {
        hotspotSourceRecords = winnerScopedRecords.length ? winnerScopedRecords : records;
      }
      const historyHotspots = buildVehicleHistoryHotspots(hotspotSourceRecords, {
        getRecordSerial,
        serialCountsBySerial
      }).map((hotspot, index) => ({
        ...hotspot,
        sourceVehicleId: vehicle.id,
        popupKey: buildVehicleHistoryHotspotPopupKey(vehicle.id, hotspot, index)
      }));
      prefetchRelatedVehiclesForHotspots(historyHotspots, vehicle.id);

      if (requestId !== gpsTrailRequestCounter) return;
      const selectedVehicleMatchesCurrent = (
        `${selectedVehicleId ?? ''}` === `${vehicle?.id ?? ''}`
        && (!selectedVehicleKey || `${selectedVehicleKey}` === getVehicleKey(vehicle))
      );
      if (movingOverrideChanged) {
        updateVehicleFilterOptions();
        renderVehicles();
        if (selectedVehicleMatchesCurrent) {
          syncFocusedVehicleNotMovingBadge(vehicle);
        }
      }
      if (!trailPoints.length && !historyHotspots.length) {
        applyRouteLayerVisibilityMode();
        return;
      }
      drawVehicleHistoryHotspots(historyHotspots);
      if (trailPoints.length) {
        drawVehicleGpsTrail(trailPoints, getVehicleMarkerColor(vehicle), vehicle);
      }
      applyRouteLayerVisibilityMode();
    };

    const ICON_PATHS = {
      mapPin: '<path d="M21 10c0 7-9 13-9 13S3 17 3 10a9 9 0 1 1 18 0Z"></path><circle cx="12" cy="10" r="3"></circle>',
      layers: '<path d="M12 2 2 7l10 5 10-5-10-5Z"></path><path d="m2 17 10 5 10-5"></path><path d="m2 12 10 5 10-5"></path>',
      navigation: '<polygon points="3 11 12 2 21 11 12 20 3 11"></polygon><line x1="12" y1="22" x2="12" y2="13"></line>',
      mail: '<rect x="3" y="5" width="18" height="14" rx="2"></rect><polyline points="3 7 12 13 21 7"></polyline>',
      wifiOff: '<line x1="1" y1="1" x2="23" y2="23"></line><path d="M16.72 11.06A10.94 10.94 0 0 0 12 9.5 10.94 10.94 0 0 0 4.08 12.74"></path><path d="M5.1 16.5A6.95 6.95 0 0 1 12 14.5a6.95 6.95 0 0 1 3.53.96"></path><line x1="12" y1="20" x2="12.01" y2="20"></line>',
      hash: '<line x1="4" y1="9" x2="20" y2="9"></line><line x1="4" y1="15" x2="20" y2="15"></line><line x1="10" y1="3" x2="8" y2="21"></line><line x1="16" y1="3" x2="14" y2="21"></line>',
      clock: '<circle cx="12" cy="12" r="9"></circle><polyline points="12 7 12 12 15 15"></polyline>',
      info: '<circle cx="12" cy="12" r="10"></circle><line x1="12" y1="16" x2="12" y2="12"></line><line x1="12" y1="8" x2="12.01" y2="8"></line>',
      search: '<circle cx="11" cy="11" r="8"></circle><line x1="21" y1="21" x2="16.65" y2="16.65"></line>',
      save: '<path d="M19 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11l5 5v11a2 2 0 0 1-2 2Z"></path><path d="M17 21v-8H7v8"></path><path d="M7 3v5h8"></path>',
      alertCircle: '<circle cx="12" cy="12" r="10"></circle><line x1="12" y1="8" x2="12" y2="12"></line><line x1="12" y1="16" x2="12.01" y2="16"></line>',
      fileWarning: '<path d="M14.5 2H5a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2V9.5L14.5 2Z"></path><path d="M12 9v4"></path><path d="M12 17h.01"></path>',
      loader2: '<path d="M21 12a9 9 0 1 1-6.219-8.56"></path>'
    };

    const svgIcon = (name, className = 'h-3 w-3') => {
      const paths = ICON_PATHS[name];
      if (!paths) return '';
      return `<svg class="${className}" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${paths}</svg>`;
    };

    const VEHICLE_RENDER_LIMIT = 500;
    const PARTNER_CARD_LIMIT = 35;

    const SERVICE_TYPES = ['tech', 'reseller', 'repair', 'custom'];
    const SERVICE_COLORS = {
      tech: '#3b82f6',
      reseller: '#34d399',
      repair: '#fb923c',
      custom: '#f472b6'
    };
    const SERVICE_SIDEBAR_KEYS = {
      tech: 'left',
      reseller: 'reseller',
      repair: 'repair',
      custom: 'custom'
    };
    const SIDEBAR_TYPE_BY_KEY = Object.fromEntries(
      Object.entries(SERVICE_SIDEBAR_KEYS).map(([type, key]) => [key, type])
    );

    const SERVICE_UI_CONFIG = {
      tech: { sidebarId: 'left-sidebar', toggleId: 'open-left-sidebar', legendSelector: '[data-legend="tech"]', collapsedClass: 'collapsed-left', layer: () => techLayer },
      reseller: { sidebarId: 'reseller-sidebar', toggleId: 'open-reseller-sidebar', legendSelector: '[data-legend="reseller"]', collapsedClass: 'collapsed-reseller', layer: () => resellerLayer },
      repair: { sidebarId: 'repair-sidebar', toggleId: 'open-repair-sidebar', legendSelector: '[data-legend="repair"]', collapsedClass: 'collapsed-repair', layer: () => repairLayer },
      custom: { sidebarId: 'dynamic-service-sidebar', toggleId: null, legendSelector: '#custom-legend-slot [data-legend]', collapsedClass: 'collapsed-dynamic', layer: () => customServiceLayer }
    };

    const PARTNER_SIDEBAR_CONFIGS = [
      { type: 'reseller', eyebrow: 'Reseller network', title: 'Nearby resellers', accentClass: 'text-emerald-300', filterPlaceholder: 'Search by name, city, state, ZIP, or note', emptyText: 'Resellers will appear here.' },
      { type: 'repair', eyebrow: 'Repair network', title: 'Repair shops nearby', accentClass: 'text-orange-200', filterPlaceholder: 'Search by name, city, state, ZIP, or note', emptyText: 'Repair shops will appear here.' },
    ];

    const CUSTOM_SIDEBAR_CONFIG = {
      eyebrow: 'Dynamic services',
      title: 'New categories',
      accentClass: 'text-indigo-200',
      description: 'Showing every new category detected in Supabase.'
    };

    const partnerSidebarTemplate = ({ type, eyebrow, title, accentClass, filterPlaceholder, emptyText }) => `
      <aside id="${type}-sidebar" class="resizable-sidebar left-0 border-r border-slate-800 flex flex-col z-20 shadow-2xl collapsed-${type}" style="width: var(--${type}-sidebar-width)">
        <div class="p-5 border-b border-slate-800 bg-slate-900 shadow-md z-10">
          <div class="flex items-center justify-between gap-3">
            <div>
              <p class="text-[10px] font-bold uppercase ${accentClass} mb-1">${eyebrow}</p>
              <h2 class="text-xl font-black text-white leading-tight">${title}</h2>
            </div>
            <div class="flex items-center gap-3">
              <div class="text-right">
                <p class="text-[10px] uppercase text-slate-400 font-bold">Total</p>
                <p id="${type}-count" class="text-xl font-black text-white leading-none">0</p>
              </div>
              <button id="collapse-${type}" class="h-8 w-8 rounded-full border border-slate-700 bg-slate-800/80 text-lg font-bold text-slate-200 hover:border-amber-400 hover:text-amber-200" aria-label="Minimize ${type} sidebar">-</button>
            </div>
          </div>
          <div class="mt-4 space-y-2">
            <p class="text-[10px] font-semibold uppercase tracking-wide text-slate-400">Client starting point</p>
            <form id="${type}-search-form" class="relative">
              <input
                id="${type}-address-input"
                type="text"
                class="block w-full rounded-xl border border-slate-700 bg-slate-800 pl-4 pr-24 py-3 text-sm text-white placeholder-slate-500 focus:border-blue-500 focus:ring-1 focus:ring-blue-500 outline-none transition-all"
                placeholder="Ex: 33101, Phoenix, AZ..."
                autocomplete="off"
              >
              <button type="submit" id="${type}-search-btn" class="absolute inset-y-1.5 right-1.5 bg-blue-600 hover:bg-blue-500 text-white rounded-lg px-4 text-xs font-bold transition-colors flex items-center justify-center min-w-[90px]">
                CALCULATE
              </button>
            </form>
            <div class="flex justify-between items-end">
              <div id="${type}-search-status" class="hidden text-xs text-blue-400 animate-pulse font-medium">Locating address...</div>
            </div>
            <input
              id="${type}-service-filter"
              type="text"
              class="block w-full rounded-xl border border-slate-700 bg-slate-800 px-3 py-2 text-sm text-white placeholder-slate-500 focus:border-blue-500 focus:ring-1 focus:ring-blue-500 outline-none transition-all"
              placeholder="${filterPlaceholder}"
              autocomplete="off"
            >
          </div>
        </div>
        <div id="${type}-list" class="flex-1 overflow-y-auto p-4 space-y-2 bg-slate-900 scroll-smooth relative">
          <div class="text-center mt-10 text-slate-600 text-xs">${emptyText}</div>
        </div>
      </aside>
    `;

    const dynamicSidebarTemplate = (config) => `
      <aside id="dynamic-service-sidebar" class="resizable-sidebar left-0 border-r border-slate-800 flex flex-col z-20 shadow-2xl collapsed-dynamic" style="width: var(--dynamic-sidebar-width)">
        <div class="p-5 border-b border-slate-800 bg-slate-900 shadow-md z-10">
          <div class="flex items-center justify-between gap-3">
            <div>
              <p id="dynamic-service-eyebrow" class="text-[10px] font-bold uppercase ${config.accentClass} mb-1">${config.eyebrow}</p>
              <h2 id="dynamic-service-title" class="text-xl font-black text-white leading-tight">${config.title}</h2>
              <p id="dynamic-service-description" class="mt-1 text-[12px] text-slate-400">${config.description}</p>
            </div>
            <div class="flex items-center gap-3">
              <div class="text-right">
                <p class="text-[10px] uppercase text-slate-400 font-bold">Total</p>
                <p id="dynamic-service-count" class="text-xl font-black text-white leading-none">0</p>
              </div>
              <button id="collapse-dynamic" class="h-8 w-8 rounded-full border border-slate-700 bg-slate-800/80 text-lg font-bold text-slate-200 hover:border-amber-400 hover:text-amber-200" aria-label="Minimize dynamic services sidebar">-</button>
            </div>
          </div>
          <div class="mt-4 space-y-2">
            <input
              id="dynamic-service-filter"
              type="text"
              class="block w-full rounded-xl border border-slate-700 bg-slate-800 px-3 py-2 text-sm text-white placeholder-slate-500 focus:border-blue-500 focus:ring-1 focus:ring-blue-500 outline-none transition-all"
              placeholder="Search by name, city, state, ZIP, or note"
              autocomplete="off"
            >
          </div>
        </div>
        <div id="dynamic-service-list" class="flex-1 overflow-y-auto p-4 space-y-4 bg-slate-900 scroll-smooth relative">
          <div class="text-center mt-10 text-slate-600 text-xs">New categories will appear here.</div>
        </div>
      </aside>
    `;

    function buildPartnerSidebars() {
      const slot = document.getElementById('partner-sidebars');
      if (!slot) return;

      const sidebarMarkup = [
        ...PARTNER_SIDEBAR_CONFIGS.map((config) => partnerSidebarTemplate(config)),
        dynamicSidebarTemplate(CUSTOM_SIDEBAR_CONFIG)
      ].join('\n');

      slot.innerHTML = sidebarMarkup;
    }

    buildPartnerSidebars();

    function ensureTechSearchControl() {
      if (document.getElementById('tech-service-filter')) return;
      const sidebar = document.getElementById('left-sidebar');
      if (!sidebar) return;
      const header = sidebar.querySelector('.p-5.border-b.border-slate-800.bg-slate-900');
      if (!header) return;

      const wrapper = document.createElement('div');
      wrapper.className = 'space-y-2';
      wrapper.innerHTML = `
        <input
          id="tech-service-filter"
          type="text"
          class="block w-full rounded-xl border border-slate-700 bg-slate-800 px-3 py-2 text-sm text-white placeholder-slate-500 focus:border-blue-500 focus:ring-1 focus:ring-blue-500 outline-none transition-all"
          placeholder="Search by company, city, state, ZIP, or note"
          autocomplete="off"
        >
      `;

      header.appendChild(wrapper);
    }

    ensureTechSearchControl();

    const isServiceSidebarVisible = (type) => {
      if (!isServiceTypeEnabled(type)) return false;
      const config = SERVICE_UI_CONFIG[type];
      if (!config?.sidebarId) return false;
      const sidebar = document.getElementById(config.sidebarId);
      if (!sidebar || sidebar.classList.contains('hidden')) return false;
      return !config.collapsedClass || !sidebar.classList.contains(config.collapsedClass);
    };

    let enabledServiceTypes = new Set(SERVICE_TYPES);
    let customCategoryLabels = new Set();
    const CUSTOM_COLOR_PALETTE = ['#f472b6', '#f97316', '#14b8a6', '#a78bfa', '#ec4899', '#10b981', '#fb7185', '#22d3ee'];

    const isUnauthorizedPartner = (partner = {}) => {
      const value = `${partner.authorization || partner.details?.authorization || ''}`.trim().toLowerCase();
      return value === 'no' || value === 'unauthorized';
    };

    const isAuthorizedReseller = (partner = {}) => `${partner.authorization ?? ''}`.trim().toLowerCase() === 'authorized';

    const isRepoAgentPartner = (partner = {}) => `${partner.categoryLabel || partner.category || ''}`.trim().toLowerCase() === 'repo agent';
    const isParkingStoragePartner = (partner = {}) => `${partner.categoryLabel || partner.category || ''}`.trim().toLowerCase() === 'parking/storage';

    const getPartnerNoteText = (partner = {}) => String(
      partner.notes
      || partner.note
      || partner.details?.Note
      || partner.details?.note
      || partner.details?.notes
      || ''
    ).trim();
    const getTechNoteText = (tech = {}) => String(
      tech.notes
      || tech.note
      || tech.details?.Note
      || tech.details?.note
      || tech.details?.notes
      || ''
    ).trim();
    const getServiceNoteDisplay = (entry = {}, type = 'partner') => {
      const note = type === 'tech' ? getTechNoteText(entry) : getPartnerNoteText(entry);
      return note || '—';
    };
    const hasSpAgentNote = (partner = {}) => getPartnerNoteText(partner).toLowerCase() === 'sp agent';
    const hasSpStorageNote = (partner = {}) => getPartnerNoteText(partner).toLowerCase() === 'sp storage';

    const getPartnerColor = (partner, defaultColor) => isUnauthorizedPartner(partner) ? '#94a3b8' : defaultColor;

    const formatPartnerLocation = (partner = {}) => {
      const cityText = partner.city ? `${partner.city}, ` : '';
      const stateText = partner.state || partner.region || 'US';
      const zipText = partner.zip ? ` ${partner.zip}` : '';
      return `${cityText}${stateText}${zipText}`.trim();
    };

    const showServicePreviewCard = (partner) => {
      if (!map || !partner) return;
      const locationText = formatPartnerLocation(partner) || 'Location unavailable';
      const noteText = getPartnerNoteText(partner);
      const noteDisplay = noteText || '—';
      const mapsLinkHtml = buildGoogleMapsLinkHtml(partner.lat, partner.lng, { tight: true });
      const popup = L.popup({
        className: 'service-mini-popup',
        closeButton: true,
        autoClose: true,
        closeOnClick: true,
        maxWidth: 220,
        offset: [0, -6]
      })
        .setLatLng([partner.lat, partner.lng])
        .setContent(`
          <div class="service-mini-card">
            <p class="service-mini-title">${escapeHTML(partner.company || partner.name || 'Service')}</p>
            <p class="service-mini-location">${escapeHTML(locationText)}</p>
            <p class="service-mini-meta"><span class="service-mini-label">Note</span><span>${escapeHTML(noteDisplay)}</span></p>
            ${mapsLinkHtml}
          </div>
        `);

      popup.openOn(map);
    };

    const createServiceIcon = (color, { unauthorized = false, checkPulseColor = null } = {}) => L.divIcon({
      className: 'service-triangle-icon',
      html: `<div class="service-icon-wrapper"><svg width="18" height="16" viewBox="0 0 18 16" fill="${color}" stroke="#0f172a" stroke-width="1.4" xmlns="http://www.w3.org/2000/svg"><path d="M9 1.1L17 14.8H1L9 1.1Z"/></svg>${unauthorized ? '<span class="service-cross">✕</span>' : ''}${checkPulseColor ? `<span class="service-check" style="--service-check-accent:${checkPulseColor}">✓</span>` : ''}</div>`,
      iconSize: [18, 16],
      iconAnchor: [9, 14],
      popupAnchor: [0, -10]
    });

    const localDatasetKey = (key, suffix = 'csv') => `techloc:dataset:${key}:${suffix}`;

    async function loadDatasetText({ key, path }) {
      const override = localStorage.getItem(localDatasetKey(key));
      if (override) return override;
      const response = await fetch(path);
      if (!response.ok) throw new Error(`Unable to read ${path} (Status ${response.status})`);
      return response.text();
    }

    // --- 1. Initialize map ---
    function initMap() {
      map = L.map('tech-map', {
        renderer: L.canvas(),
        zoomControl: false,
        minZoom: 3,
        zoomSnap: 0.25,
        zoomDelta: 0.5
      }).setView([MAP_DEFAULT_CENTER.lat, MAP_DEFAULT_CENTER.lng], 5);
      L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', { maxZoom: 19, minZoom: 3 }).addTo(map);
      L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_only_labels/{z}/{x}/{y}{r}.png', { maxZoom: 19, minZoom: 3, opacity: 0.95 }).addTo(map);
      L.control.zoom({ position: 'bottomright' }).addTo(map);
      hotspotLayer = L.layerGroup();
      if (hotspotsVisible) hotspotLayer.addTo(map);
      blacklistLayer = L.layerGroup();
      if (blacklistMarkersVisible) blacklistLayer.addTo(map);
      techLayer = createPartnerClusterGroup(SERVICE_COLORS.tech);
      vehicleLayer = L.markerClusterGroup({
        maxClusterRadius: 35,
        disableClusteringAtZoom: 17,
        spiderfyOnMaxZoom: true,
        showCoverageOnHover: false,
        iconCreateFunction(cluster) {
          const count = cluster.getChildCount();
          let sizeClass = 'w-8 h-8 text-xs';
          let iconSize = [32, 32];

          if (count > 100) {
            sizeClass = 'w-10 h-10 text-sm';
            iconSize = [40, 40];
          } else if (count > 10) {
            sizeClass = 'w-9 h-9 text-sm';
            iconSize = [36, 36];
          }

          const childMarkers = typeof cluster.getAllChildMarkers === 'function' ? cluster.getAllChildMarkers() : [];
          let hasRed = false;
          let hasAmber = false;
          let allGreen = true;
          let hasStopped = false;

          childMarkers.forEach((marker) => {
            const markerColor = marker?.options?.markerColor || (marker?.options?.vehicleData ? getVehicleMarkerColor(marker.options.vehicleData) : null);
            const isStopped = marker?.options?.isStopped || (marker?.options?.vehicleData ? isVehicleNotMoving(marker.options.vehicleData) : false);

            if (isStopped) {
              hasStopped = true;
            }

            if (markerColor) {
              const colorValue = `${markerColor}`.toLowerCase();
              if (colorValue === '#ef4444') {
                hasRed = true;
                allGreen = false;
              } else if (colorValue === '#f59e0b' || colorValue === '#fbbf24') {
                hasAmber = true;
                allGreen = false;
              } else if (colorValue !== '#22c55e') {
                allGreen = false;
              }
            } else {
              allGreen = false;
            }
          });

          let clusterColor = '#f59e0b';
          if (hasRed) {
            clusterColor = '#ef4444';
          } else if (hasAmber) {
            clusterColor = '#f59e0b';
          } else if (allGreen && childMarkers.length > 0) {
            clusterColor = '#22c55e';
          }

          const badgeHtml = hasStopped ? '<span class="vehicle-cross-badge absolute inset-0 flex items-center justify-center pointer-events-none">✕</span>' : '';
          const [width, height] = iconSize;
          return L.divIcon({
            html: `<div class="relative border-2 font-bold rounded-full flex items-center justify-center shadow-lg bg-slate-900/90 ${sizeClass}" style="color:${clusterColor}; border-color:${clusterColor}">${badgeHtml}<span>${count}</span></div>`,
            className: 'vehicle-cluster-icon',
            iconSize,
            iconAnchor: [width / 2, height / 2]
          });
        }
      }).addTo(map);
      targetLayer = L.layerGroup().addTo(map);
      connectionLayer = L.layerGroup().addTo(map);
      serviceLayer = L.layerGroup().addTo(map);
      serviceConnectionLayer = L.layerGroup().addTo(map);
      highlightLayer = L.layerGroup().addTo(map);
      gpsTrailLayer = L.layerGroup();
      if (routeLinesVisible) gpsTrailLayer.addTo(map);
      resellerLayer = createPartnerClusterGroup(SERVICE_COLORS.reseller);
      repairLayer = createPartnerClusterGroup(SERVICE_COLORS.repair);
      customServiceLayer = createPartnerClusterGroup(SERVICE_COLORS.custom);

      const invalidateMapSize = () => map?.invalidateSize();

      const mapContainer = document.getElementById('tech-map');
      if (mapContainer && typeof ResizeObserver !== 'undefined') {
        const resizeObserver = new ResizeObserver(() => {
          invalidateMapSize();
        });
        resizeObserver.observe(mapContainer);
      }

      const authBlock = document.querySelector('[data-auth-protected]');
      if (authBlock && typeof MutationObserver !== 'undefined') {
        const authObserver = new MutationObserver(() => {
          if (!authBlock.classList.contains('hidden')) {
            requestAnimationFrame(() => invalidateMapSize());
          }
        });
        authObserver.observe(authBlock, { attributes: true, attributeFilter: ['class'] });
      }

      map.on('click', (e) => {
        if (e.originalEvent?.handledByMarker) {
          return;
        }

        const lat = parseFloat(e.latlng.lat.toFixed(6));
        const lng = parseFloat(e.latlng.lng.toFixed(6));

        const shouldCascadeToVehicleOnly = selectedVehicleId !== null
          && (parkingSpotCascadeArmed || hasVisibleParkingSpotPopup() || hasVisibleRoutePointPopup());
        if (shouldCascadeToVehicleOnly) {
          clearParkingSpotCascade();
          map.closePopup();
          const selectedVehicle = getSelectedVehicleEntry();
          if (selectedVehicle && vehicleMarkersVisible) {
            focusVehicle(selectedVehicle);
          } else {
            applySelection(selectedVehicleId, null, selectedVehicleKey);
          }
          return;
        }

        const hasActiveMapSelection = selectedVehicleId !== null || lastClientLocation !== null || lastOriginPoint !== null;
        if (hasActiveMapSelection) {
          resetSelection();
          lastOriginPoint = null;
          lastClientLocation = null;
          targetLayer?.clearLayers();
          renderVisibleSidebars();
          return;
        }

        resetSelection();
        targetLayer?.clearLayers();
        const locationPoint = { lat, lng, name: 'Pinned location' };
        lastClientLocation = locationPoint;
        lastOriginPoint = locationPoint;
        const targetIcon = L.divIcon({
          className: '',
          html: `<div class="w-4 h-4 bg-red-600 rounded-full border-2 border-white animate-ping"></div>`
        });
        L.marker([lat, lng], { icon: targetIcon }).addTo(targetLayer);
        const popup = L.marker([lat, lng]).addTo(targetLayer);
        popup.bindPopup(`<b>Selected point</b><br>${lat.toFixed(4)}, ${lng.toFixed(4)}`).openPopup();

        showServicesFromOrigin(locationPoint, { forceType: isAnyServiceSidebarOpen() ? getActivePartnerType() : null });
        renderVisibleSidebars();
      });
    }

    // --- 2. Load data from Supabase ---
    async function loadTechnicians() {
      if (!isServiceTypeEnabled('tech')) return;
      if (!supabaseClient) {
        console.warn('Supabase unavailable: showing local installer placeholders.');
        document.getElementById('tech-list').innerHTML = `
          <div class="text-center mt-10 px-6">
            ${svgIcon('alertCircle', 'mx-auto h-8 w-8 text-slate-600 mb-2')}
            <p class="text-slate-500 text-xs">Supabase is offline. Unable to load installers.</p>
          </div>
        `;
        return;
      }
      const stopLoading = startLoading('Loading Installers…');
      try {
        await ensureServiceCache();
        const data = getCachedServices(getServiceCategoryLabel('tech'));
        technicians = (data || []).map((row, idx) => normalizeInstaller(row, idx, { getField, toStateCode, resolveCoords })).filter(t => t.company || t.city || t.state);
        setServiceHeaders('technicians', data || []);
        renderedTechIds = '';
        document.getElementById('total-count').textContent = technicians.length;
        updateTechUI(technicians);
        renderVehicles();
      } catch (err) {
        console.warn("System warning: " + err.message);
        document.getElementById('file-error').classList.remove('hidden');
        document.getElementById('tech-list').innerHTML = `
          <div class="text-center mt-10 px-6">
            ${svgIcon('fileWarning', 'mx-auto h-8 w-8 text-slate-600 mb-2')}
            <p class="text-slate-500 text-xs">No installer data detected.</p>
          </div>
        `;
      } finally {
        stopLoading();
      }
    }

    async function loadVehicles({ force = false } = {}) {
      if (vehicles.length && !force) return;
      const stopLoading = startLoading('Loading Vehicles…');
      try {
        const data = await vehicleService.listVehicles();
        const normalizedVehicles = data.map((row, idx) =>
          normalizeVehicle(row, idx, { getField, toStateCode, resolveCoords })
        );

        let dealsByStockNo = new Map();
        let openInvoiceSummaryByStockNo = new Map();
        let invoiceSummaryAvailable = false;
        if (supabaseClient) {
          const stockNumbers = normalizedVehicles
            .map((vehicle) => normalizeStockNumber(vehicle.stockNo))
            .filter(Boolean);
          try {
            const [dealsResult, invoicesResult] = await Promise.allSettled([
              fetchDealsByStockNumbers(stockNumbers),
              fetchOpenInvoiceSummaryByStockNumbers(stockNumbers)
            ]);
            if (dealsResult.status === 'fulfilled') {
              dealsByStockNo = dealsResult.value;
            } else {
              console.warn('DealsJP1 load warning: ' + (dealsResult.reason?.message || dealsResult.reason));
            }
            if (invoicesResult.status === 'fulfilled') {
              openInvoiceSummaryByStockNo = invoicesResult.value;
              invoiceSummaryAvailable = true;
            } else {
              console.warn('Invoices load warning: ' + (invoicesResult.reason?.message || invoicesResult.reason));
            }
          } catch (error) {
            console.warn('Vehicle financial enrich warning: ' + (error?.message || error));
          }
        }

        const allowedDealStatuses = new Set(['ACTIVE', 'STOCK', 'STOLEN']);
        const filteredVehicles = normalizedVehicles.filter((vehicle) => {
          const stockNo = normalizeStockNumber(vehicle.stockNo);
          const dealValues = dealsByStockNo.get(stockNo) ?? null;
          const normalizedStatus = String(dealValues?.vehicleStatus ?? '').trim().toUpperCase();
          if (normalizedStatus && !allowedDealStatuses.has(normalizedStatus)) return false;
          if (normalizedStatus) {
            vehicle.vehicleStatus = normalizedStatus;
          }
          const regularAmount = dealValues?.regularAmount ?? null;
          const openBalance = dealValues?.openBalance ?? null;
          const openInvoiceSummary = openInvoiceSummaryByStockNo.get(stockNo) ?? null;
          const invoiceOpenBalance = Number.isFinite(openInvoiceSummary?.openBalanceSum)
            ? openInvoiceSummary.openBalanceSum
            : null;
          const effectiveOpenBalance = invoiceSummaryAvailable
            ? (invoiceOpenBalance ?? 0)
            : openBalance;
          const payKpi = effectiveOpenBalance !== null && regularAmount ? effectiveOpenBalance / regularAmount : null;
          const oldestOpenInvoice = openInvoiceSummary ?? null;
          vehicle.payKpi = payKpi;
          vehicle.payKpiDisplay = formatPayKpi(payKpi);
          vehicle.oldestOpenInvoiceDate = oldestOpenInvoice?.value || '';
          vehicle.oldestOpenInvoiceDateDisplay = formatInvoiceDate(vehicle.oldestOpenInvoiceDate);
          return true;
        });

        vehicles = filteredVehicles;
        vehicleHistoryHotspotCache.clear();
        vehicleHistoryHotspotPendingRequests.clear();
        relatedHotspotVehiclesCache.clear();
        hotspotFastCoordinatePairs = [];
        hotspotFastCoordinatePairLookupPromise = null;
        await hydrateVehicleClickHistory(vehicles);
        vehicleHeaders = data.length ? ensureRequiredVehicleHeaders(Object.keys(data[0])) : [...REQUIRED_VEHICLE_HEADERS];
        updateVehicleFilterOptions();
        syncVehicleFilterInputs();
        renderVehicles();
        // PT read bounds now come from vehicles table columns.
        syncVehicleSelectionFromStore(getSelectedVehicle(), { shouldFocus: true });
      } catch (err) {
        if (err?.message === 'Vehicle data provider unavailable.') {
          console.warn('Vehicle data provider unavailable: skipping vehicle load.');
          return;
        }
        console.warn("Vehicle load warning: " + err.message);
        document.getElementById('vehicle-list').innerHTML = `
          <div class="text-center mt-10 px-6">
            ${svgIcon('fileWarning', 'mx-auto h-8 w-8 text-slate-600 mb-2')}
            <p class="text-slate-500 text-xs">Unable to load vehicle data.</p>
          </div>
        `;
      } finally {
        stopLoading();
      }
    }

    function renderHotspots() {
      if (!hotspotLayer) return;
      hotspotLayer.clearLayers();

      hotspots.filter(hasValidCoords).forEach((hotspot) => {
        const circle = L.circle([hotspot.lat, hotspot.lng], {
          radius: hotspot.radiusMiles * MILES_TO_METERS,
          color: '#22c55e',
          weight: 1.5,
          fillColor: '#22c55e',
          fillOpacity: 0.18,
          opacity: 0.7,
        }).addTo(hotspotLayer);

        circle?.bringToBack?.();

        const locationText = [hotspot.city, hotspot.state, hotspot.zip].filter(Boolean).join(', ');
        circle.bindPopup(`
          <div class="text-xs text-slate-100 space-y-1">
            <p class="font-bold text-white text-sm">Hotspot</p>
            <p class="text-[11px] text-slate-300">${locationText || 'Location unavailable'}</p>
            <p class="text-[11px] text-emerald-300">Coverage radius: ${hotspot.radiusMiles} miles</p>
          </div>
        `);
      });
    }

    const createBlacklistIcon = () => L.divIcon({
      className: 'blacklist-triangle-icon',
      html: `<div class="blacklist-marker">!</div>`,
      iconSize: [18, 24],
      iconAnchor: [9, 20],
      popupAnchor: [0, -18]
    });

    function renderBlacklistSites() {
      if (!blacklistLayer) return;
      blacklistLayer.clearLayers();

      blacklistSites.filter(hasValidCoords).forEach((entry) => {
        const marker = L.marker([entry.lat, entry.lng], { icon: createBlacklistIcon() }).addTo(blacklistLayer);
        marker.on('click', (e) => { e.originalEvent.handledByMarker = true; });

        marker.bindPopup(`
          <div class="text-xs text-slate-100 space-y-1">
            <p class="font-bold text-amber-200 flex items-center gap-2 text-sm">
              <span class="inline-flex h-5 w-5 items-center justify-center rounded-full bg-amber-500/20 text-amber-200 border border-amber-400/60">!</span>
              Device removal location
            </p>
            <p class="text-[11px] text-slate-300"><span class="text-slate-400">Company:</span> ${escapeHTML(entry.company || 'Unknown')}</p>
            ${entry.assocUnit ? `<p class="text-[11px] text-slate-300"><span class="text-slate-400">Assoc. Unit:</span> ${escapeHTML(entry.assocUnit)}</p>` : ''}
            ${entry.note ? `<p class="text-[11px] text-amber-200 font-semibold">${escapeHTML(entry.note)}</p>` : ''}
          </div>
        `);
      });
    }

    async function loadHotspots() {
      if (!supabaseClient) {
        console.warn('Supabase unavailable: skipping hotspot load.');
        return;
      }

      const stopLoading = startLoading('Loading hotspots…');
      try {
        const { data, error } = await supabaseClient.from(TABLES.hotspots).select('*');
        if (error) throw error;
        hotspots = (data || []).map((row, idx) => normalizeHotspot(row, idx)).filter(hasValidCoords);
        renderHotspots();
      } catch (err) {
        console.warn(`Hotspot load warning: ${err.message}`);
      } finally {
        stopLoading();
      }
    }

    async function loadBlacklistSites() {
      if (!supabaseClient) {
        console.warn('Supabase unavailable: skipping blacklist load.');
        return;
      }

      const stopLoading = startLoading('Loading Blacklist Locations…');
      try {
        const { data, error } = await supabaseClient.from(TABLES.blacklist).select('*');
        if (error) throw error;
        blacklistSites = (data || []).map((row, idx) => normalizeBlacklistEntry(row, idx)).filter(hasValidCoords);
        renderBlacklistSites();
      } catch (err) {
        console.warn(`Blacklist load warning: ${err.message}`);
      } finally {
        stopLoading();
      }
    }

    const SERVICE_LOAD_CONFIG = {
      reseller: {
        uiUpdate: updateResellerUI,
        setList: (list) => { resellers = list; },
        warningLabel: 'Reseller'
      },
      repair: {
        uiUpdate: updateRepairUI,
        setList: (list) => { repairShops = list; },
        warningLabel: 'Repair shop'
      }
    };

    async function loadPartnerService(type) {
      if (!isServiceTypeEnabled(type)) return;
      if (!supabaseClient) return;

      const config = SERVICE_LOAD_CONFIG[type];
      if (!config) {
        console.warn(`No service config found for type "${type}".`);
        return;
      }

      try {
        await ensureServiceCache();
        const data = getCachedServices(getServiceCategoryLabel(type));
        setServiceHeaders(type, data);
        const normalized = (data || []).map((row, idx) => normalizePartner(row, type, idx, { getField, toStateCode, resolveCoords }));
        config.setList(normalized);
        config.uiUpdate(normalized);
      } catch (err) {
        const label = config.warningLabel || type;
        console.warn(`${label} load warning: ${err.message}`);
      }
    }

    async function loadAllServices() {
      if (!supabaseClient) return;

      await ensureServiceCache();

      const categoryMap = new Map();
      serviceCacheByCategory.forEach((rows, key) => {
        const sample = rows?.[0];
        const label = sample?.category || key || 'Custom';
        categoryMap.set(key, label);
      });

      updateServiceCategoryLabels(categoryMap);

      const knownLabels = new Set(
        Array.from(serviceCategoryLabelsByType.values()).map((label) => normalizeCategoryLabel(label).toLowerCase())
      );
      customCategoryLabels = new Set(
        Array.from(categoryMap.values()).filter((label) => !knownLabels.has(normalizeCategoryLabel(label).toLowerCase()))
      );
      rebuildCustomCategoryToggles();

      for (const type of Object.keys(SERVICE_LOAD_CONFIG)) {
        await loadPartnerService(type);
      }

      if (isServiceTypeEnabled('custom') && customCategoryLabels.size) {
        await loadCustomServices();
      }
    }

    async function loadCustomServices() {
      if (!isServiceTypeEnabled('custom') || !customCategoryLabels.size) return;
      if (!supabaseClient) {
        console.warn('Supabase unavailable: skipping custom categories.');
        return;
      }

      try {
        const labels = Array.from(customCategoryLabels);
        await ensureServiceCache();
        const data = labels.flatMap((label) => getCachedServices(label));

        customCategories.forEach(({ layer }) => layer?.clearLayers?.());
        customServices = (data || []).map((row, idx) => {
          const rawLabel = normalizeCategoryLabel(row?.category || 'Custom');
          const meta = ensureCustomCategoryMeta(rawLabel);
          const partner = normalizePartner(row, 'custom', idx, { getField, toStateCode, resolveCoords });
          return { ...partner, categoryKey: meta.key, categoryLabel: meta.label, color: meta.color, type: `custom-${meta.key}` };
        });

        if (!selectedCustomCategoryKey && customServices.length) {
          selectedCustomCategoryKey = customServices[0].categoryKey;
        }

        renderCategorySidebar(selectedCustomCategoryKey, customServices);
      } catch (err) {
        console.warn(`Custom service warning: ${err.message}`);
      }
    }

    // --- 4. Update map and list ---
    function updateTechUI(techList, target = null, clientLocation = null) {
      if (!techSidebarVisible) {
        techLayer?.clearLayers();
        return;
      }
      const listContainer = document.getElementById('tech-list');
      listContainer.innerHTML = '';
      const filteredTechList = applyServiceFilter(techList, 'tech');
      const totalCountEl = document.getElementById('total-count');
      if (totalCountEl) {
        totalCountEl.textContent = filteredTechList.length !== techList.length
          ? `${filteredTechList.length}/${techList.length}`
          : `${techList.length}`;
      }

      const origin = clientLocation || getCurrentOrigin();
      const techWithDistances = attachDistances(
        filteredTechList,
        origin,
        distanceCaches.tech,
        (tech) => getDistance(origin.lat, origin.lng, tech.lat, tech.lng)
      );
      const sortedList = origin
        ? techWithDistances.sort((a, b) => a.distance - b.distance)
        : [...techWithDistances];

      const selectedTech = selectedTechId !== null
        ? sortedList.find(t => t.id === selectedTechId) || filteredTechList.find(t => t.id === selectedTechId)
        : null;

      const displayList = selectedTech
        ? [selectedTech, ...sortedList.filter(t => t.id !== selectedTech.id)]
        : sortedList;

      const displayKey = displayList.map(t => t.id).join('|');
      const shouldRenderMarkers = techSidebarVisible && displayKey !== renderedTechIds;

      if (shouldRenderMarkers) {
        techLayer.clearLayers();
      }

      if (displayList.length === 0) {
        listContainer.innerHTML = `<div class="text-center mt-10 text-slate-600 text-xs">No technicians match this selection.</div>`;
      }

      if (shouldRenderMarkers) {
        displayList.forEach(tech => {
          const marker = L.marker([tech.lat, tech.lng], {
            icon: createServiceIcon(SERVICE_COLORS.tech)
          }).addTo(techLayer);

	          marker.bindPopup(`
	            <div class="text-slate-900 p-1 font-sans">
	              <strong class="block text-sm font-bold mb-1">${tech.company}</strong>
	              <div class="text-xs text-slate-500 mb-2">${tech.city}, ${tech.state} ${tech.zip}</div>
	              <a href="tel:${tech.phoneDial || tech.phone}" class="block bg-blue-100 text-blue-700 px-2 py-1 rounded text-center text-xs font-bold mb-1">${tech.phone}</a>
	              <div class="text-[10px] text-slate-400 truncate">${tech.email}</div>
	              ${buildGoogleMapsLinkHtml(tech.lat, tech.lng, { tight: true })}
	            </div>
	          `);

          marker.on('click', (e) => {
            e.originalEvent.handledByMarker = true;
            map.flyTo([tech.lat, tech.lng], 15, { duration: 1.2 });
            applySelection(null, tech.id);
          });
        });

        renderedTechIds = displayKey;
      }

      displayList.slice(0, 50).forEach((tech, idx) => {
        const isNearest = target && idx === 0;

        const techNoteText = getServiceNoteDisplay(tech, 'tech');
        const card = document.createElement('div');
        card.className = `p-3 rounded-lg border transition-colors cursor-pointer ${isNearest ? 'bg-slate-800 border-blue-500 ring-1 ring-blue-500' : 'bg-slate-900 border-slate-800 hover:border-slate-700'}`;
        card.dataset.id = tech.id;
        card.dataset.type = 'tech';

        const distanceLabel = origin && typeof tech.distance === 'number'
          ? `${Math.round(tech.distance)} mi`
          : '';

        card.innerHTML = `
          <div class="flex justify-between items-start gap-3">
            <div class="min-w-0 space-y-1">
              <h3 class="font-bold text-white text-sm break-words leading-tight" title="${tech.company}">${tech.company}</h3>
              <p class="flex items-center gap-1 text-[11px] text-slate-400 leading-tight">${svgIcon('mapPin', 'h-3 w-3')}<span class="truncate">${tech.city || 'Unknown'}, ${tech.state || 'US'}${tech.zip ? ` ${tech.zip}` : ''}</span></p>
            </div>
            <div class="text-right text-[11px] text-slate-400 leading-tight space-y-0.5">
              <p class="font-semibold text-slate-200">${tech.phone || ''}</p>
              ${tech.email ? `<p class="flex items-center gap-1 justify-end text-slate-400">${svgIcon('mail', 'h-3 w-3')}<span class="truncate max-w-[160px]">${tech.email}</span></p>` : ''}
              ${distanceLabel ? `<p class="flex items-center gap-1 justify-end text-slate-300">${svgIcon('navigation', 'h-3 w-3')}<span class="font-semibold text-slate-100">${distanceLabel}</span></p>` : ''}
            </div>
          </div>
          <p class="text-[10px] text-slate-500 mt-1">Note: ${escapeHTML(techNoteText)}</p>

        `;

        listContainer.appendChild(card);
      });

    }

    const isAnyServiceSidebarOpen = () => getVisibleServiceTypes().length > 0;

    const getServiceSearchFilterValue = (type = 'tech') => `${serviceSearchFilters[type] || ''}`.trim().toLowerCase();
    const getServiceSearchInputId = (type) => (type === 'custom' ? 'dynamic-service-filter' : `${type}-service-filter`);

    const applyServiceFilter = (list = [], type = 'tech') => {
      const query = getServiceSearchFilterValue(type);
      const allowedIds = Array.isArray(serviceFilterIds[type]) ? new Set(serviceFilterIds[type].map((id) => `${id}`)) : null;
      const baseList = allowedIds ? list.filter((partner) => allowedIds.has(`${partner.id}`)) : list;
      if (!query) return baseList;
      return baseList.filter((partner) => {
        const noteText = type === 'tech' ? getTechNoteText(partner) : getPartnerNoteText(partner);
        const haystack = [
          partner.company,
          partner.name,
          partner.contact,
          partner.vendor,
          partner.city,
          partner.state,
          partner.region,
          partner.zip,
          partner.zipcode,
          partner.phone,
          partner.email,
          partner.availability,
          partner.authorization,
          partner.categoryLabel,
          partner.category,
          noteText
        ]
          .map((value) => `${value || ''}`.toLowerCase())
          .join(' ');
        return haystack.includes(query);
      });
    };

    function filterSidebarForPartner(type, partner) {
      if (!type || !partner) return;
      serviceSearchFilters[type] = [partner.company || partner.name || '', partner.zip || partner.zipcode || '']
        .filter(Boolean)
        .join(' ')
        .trim();
      const searchInput = document.getElementById(getServiceSearchInputId(type));
      if (searchInput) searchInput.value = serviceSearchFilters[type];
      renderVisibleSidebars();
    }

    const updateServiceCount = (type, total, filteredTotal = total) => {
      const countId = type === 'custom' ? 'dynamic-service-count' : `${type}-count`;
      const el = document.getElementById(countId);
      if (el) {
        el.textContent = filteredTotal !== total ? `${filteredTotal}/${total}` : total;
      }
    };

    const getCurrentOrigin = () => {
      if (selectedVehicleId !== null) {
        const vehicle = getSelectedVehicleEntry();
        if (vehicle && hasValidCoords(vehicle)) return vehicle;
      }

      if (selectedTechId !== null) {
        const tech = technicians.find(t => t.id === selectedTechId && hasValidCoords(t));
        if (tech) return tech;
      }

      const selectedPartner = Object.values(selectedServiceByType).find(Boolean);
      if (selectedPartner && hasValidCoords(selectedPartner)) return selectedPartner;

      if (lastOriginPoint) return lastOriginPoint;
      if (lastClientLocation) return lastClientLocation;
      return null;
    };

    const getSelectedService = (type) => selectedServiceByType[type] || null;

    const syncServiceSearchInputs = () => {
      SERVICE_TYPES.forEach((type) => {
        const searchInput = document.getElementById(getServiceSearchInputId(type));
        const searchValue = serviceSearchFilters[type] || '';
        if (searchInput && searchInput.value !== searchValue) {
          searchInput.value = searchValue;
        }
      });
    };

    function renderPartnerUI(partners = [], options) {
      const { containerId, layer, visible, accentColor, badgeLabel, type } = options;
      if (!isServiceTypeEnabled(type)) return;
      const container = document.getElementById(containerId);
      if (!container) return;

      if (!visible) {
        const placeholder = document.createElement('div');
        placeholder.className = 'text-center mt-10 text-slate-600 text-xs';
        placeholder.textContent = 'Open this sidebar to view nearby partners.';
        container.replaceChildren(placeholder);
        layer?.clearLayers();
        updateServiceCount(type, partners.length, applyServiceFilter(partners, type).length);
        return;
      }

      container.replaceChildren();
      layer?.clearLayers();

      const totalPartners = partners.length;
      const filteredPartners = applyServiceFilter(partners, type);
      const selectedService = getSelectedService(type);
      const origin = getCurrentOrigin() || selectedService;
      const partnersWithDistances = attachDistances(
        filteredPartners,
        origin,
        distanceCaches.partners,
        (partner) => getDistance(origin.lat, origin.lng, partner.lat, partner.lng)
      );
      const sorted = origin
        ? partnersWithDistances.sort((a, b) => a.distance - b.distance)
        : [...partnersWithDistances];

      const prioritized = selectedService
        ? [sorted.find(p => p.id === selectedService.id) || selectedService, ...sorted.filter(p => p.id !== selectedService.id)]
        : sorted;

      updateServiceCount(type, totalPartners, prioritized.length);

        if (!prioritized.length) {
          const empty = document.createElement('div');
          empty.className = 'text-center mt-10 text-slate-600 text-xs';
          empty.textContent = 'No partners available.';
          container.replaceChildren(empty);
          return;
        }

        const fragment = document.createDocumentFragment();
        const markerLimit = Math.min(prioritized.length, 75);
        const cardLimit = Math.min(prioritized.length, PARTNER_CARD_LIMIT);

        prioritized.forEach((partner, idx) => {
          const unauthorized = isUnauthorizedPartner(partner);
          const markerColor = getPartnerColor(partner, accentColor);
          const checkPulseColor =
            !unauthorized && type === 'reseller' && isAuthorizedReseller(partner)
              ? markerColor
              : null;

          if (idx < markerLimit) {
            const marker = L.marker([partner.lat, partner.lng], {
              icon: createServiceIcon(markerColor, { unauthorized, checkPulseColor })
            }).addTo(layer);

            const locationText = formatPartnerLocation(partner);

	            marker.bindPopup(`
	              <div class="text-slate-900 p-1 font-sans">
	                <strong class="block text-sm font-bold mb-1">${partner.company}</strong>
	                <div class="text-xs text-slate-500 mb-2">${locationText || 'US'}</div>
	                <a href="tel:${partner.phoneDial || partner.phone}" class="block bg-slate-100 text-slate-800 px-2 py-1 rounded text-center text-xs font-bold mb-1">${partner.phone || 'Contact'}</a>
	                <div class="text-[10px] text-slate-500">${partner.availability || ''}</div>
	                ${buildGoogleMapsLinkHtml(partner.lat, partner.lng, { tight: true })}
	              </div>
	            `);

            marker.on('click', (e) => {
              e.originalEvent.handledByMarker = true;
              sidebarStateController?.setState?.(SERVICE_SIDEBAR_KEYS[type] || 'left', true);
              selectedServiceByType[type] = partner;
              filterSidebarForPartner(type, partner);
              const originPoint = getCurrentOrigin() || partner;
              if (originPoint) {
                showServicesFromOrigin(originPoint, { forceType: type });
              }
              showServicePreviewCard(partner);
              renderPartnerUI(partners, options);
              map.flyTo([partner.lat, partner.lng], 15, { duration: 1.2 });
            });
          }

          if (idx >= cardLimit) return;

	          const locationText = formatPartnerLocation(partner);
          const partnerNoteText = getServiceNoteDisplay(partner, 'partner');
	          const card = document.createElement('div');
          const isSelected = selectedService?.id === partner.id;
          card.className = `p-3 rounded-lg border transition-colors cursor-pointer ${isSelected || (idx === 0 && origin) ? 'bg-slate-800 border-amber-400 ring-1 ring-amber-400' : 'bg-slate-900 border-slate-800 hover:border-slate-700'}`;
          card.dataset.id = partner.id;
          card.dataset.type = 'partner';
          card.dataset.partnerType = type;
          const canOpenRepairHistory = isAuthenticatedCached();
          card.innerHTML = `
            <div class="flex justify-between items-start gap-3">
              <div class="min-w-0">
                <p class="text-[10px] font-semibold uppercase tracking-wide text-slate-400">${badgeLabel}</p>
                <h3 class="font-bold text-white text-sm break-words leading-tight" title="${partner.company}">${partner.company}</h3>
              </div>
              <div class="text-right text-[11px] text-slate-400 leading-tight">
                <p class="font-semibold text-slate-200">${partner.contact || 'Dispatch'}</p>
                <a class="block font-bold text-blue-200" href="tel:${partner.phoneDial || partner.phone}">${partner.phone || ''}</a>
                <p class="text-[10px] text-slate-500">${partner.availability || ''}</p>
              </div>
            </div>
	            <div class="grid grid-cols-2 gap-2 mt-2 text-[11px] text-slate-300">
	              <div class="flex items-center gap-1 text-slate-400">${svgIcon('mapPin')}<span>${locationText || 'US'}</span></div>
	              ${origin ? `<div class="flex items-center gap-1 justify-end text-slate-300">${svgIcon('navigation')}<span class="font-semibold text-slate-100">${Math.round(partner.distance)} mi</span></div>` : ''}
		              <div class="col-span-2 text-[10px] text-slate-400 leading-tight">Note: ${escapeHTML(partnerNoteText)}</div>
	            </div>

          `;

        fragment.appendChild(card);
      });

      if (prioritized.length > cardLimit) {
        const notice = document.createElement('p');
        notice.className = 'text-[11px] text-slate-500 px-1 pt-1';
        notice.textContent = `Showing the first ${cardLimit} of ${prioritized.length} partners.`;
        fragment.appendChild(notice);
      }

      container.replaceChildren(fragment);
    }

    function updateResellerUI(list = resellers) {
      renderPartnerUI(list, { containerId: 'reseller-list', layer: resellerLayer, visible: resellerSidebarVisible, accentColor: '#34d399', badgeLabel: 'Reseller', type: 'reseller', countId: 'reseller-count' });
    }

    function updateRepairUI(list = repairShops) {
      renderPartnerUI(list, { containerId: 'repair-list', layer: repairLayer, visible: repairSidebarVisible, accentColor: '#fb923c', badgeLabel: 'Repair shop', type: 'repair', countId: 'repair-count' });
    }

    function renderCategorySidebar(categoryKey = selectedCustomCategoryKey, list = customServices) {
      const container = document.getElementById('dynamic-service-list');
      const counter = document.getElementById('dynamic-service-count');
      const titleEl = document.getElementById('dynamic-service-title');
      const eyebrowEl = document.getElementById('dynamic-service-eyebrow');
      const descEl = document.getElementById('dynamic-service-description');
      if (!container) return;

      const meta = categoryKey ? customCategories.get(categoryKey) || ensureCustomCategoryMeta(categoryKey) : null;
      const label = meta?.label || 'Dynamic services';
      if (titleEl) titleEl.textContent = `${label} Network`;
      if (eyebrowEl) eyebrowEl.textContent = label;
      if (descEl) descEl.textContent = `Showing ${label} partners detected in Supabase.`;

      const normalizedList = list.filter((item) => !categoryKey || item?.categoryKey === categoryKey);
      const allowedIds = Array.isArray(serviceFilterIds.custom) ? new Set(serviceFilterIds.custom.map((id) => `${id}`)) : null;
      const constrainedList = allowedIds
        ? normalizedList.filter((partner) => allowedIds.has(`${partner.id}`))
        : normalizedList;
      const constrainedFiltered = applyServiceFilter(constrainedList, 'custom');
      const origin = getCurrentOrigin();
      const withDistances = attachDistances(constrainedFiltered, origin, distanceCaches.partners, (partner) => {
        if (!origin) return 0;
        return getDistance(origin.lat, origin.lng, partner.lat, partner.lng);
      });
      const sorted = origin ? [...withDistances].sort((a, b) => a.distance - b.distance) : [...withDistances];
      const filtered = sorted;

      const markersAllowed = customSidebarVisible && isServiceTypeEnabled('custom');
      customCategories.forEach(({ layer }) => layer?.clearLayers?.());

      container.innerHTML = '';

      if (!filtered.length) {
        container.innerHTML = '<div class="text-center mt-10 text-slate-600 text-xs">No services available.</div>';
        counter && (counter.textContent = '0');
        return;
      }

      const fragment = document.createDocumentFragment();
	      filtered.slice(0, PARTNER_CARD_LIMIT).forEach((partner) => {
	        const locationText = formatPartnerLocation(partner) || 'US';
        const partnerNoteText = getServiceNoteDisplay(partner, 'partner');
	        const card = document.createElement('article');
        card.className = 'rounded-xl border border-slate-800 bg-slate-900/70 p-3 shadow-sm hover:border-blue-500/70 transition-colors';
        card.dataset.id = partner.id;
        card.dataset.type = 'partner';
        card.dataset.partnerType = 'custom';

        const distanceLabel = origin && typeof partner.distance === 'number' ? `${Math.round(partner.distance)} mi` : '';
        card.innerHTML = `
          <div class="flex items-start justify-between gap-3">
            <div class="min-w-0">
              <p class="text-sm font-bold text-white leading-tight">${escapeHTML(partner.company || partner.name || 'Service')}</p>
              <p class="flex items-center gap-1 text-[11px] text-slate-400 leading-tight">${svgIcon('mapPin', 'h-3 w-3')}<span class="truncate">${escapeHTML(locationText)}</span></p>
            </div>
            <div class="text-right text-[11px] text-slate-400 leading-tight space-y-0.5">
              ${partner.phone ? `<a class="font-bold text-blue-200 block" href="tel:${partner.phoneDial || partner.phone}">${partner.phone}</a>` : ''}
              ${distanceLabel ? `<p class="flex items-center gap-1 justify-end text-slate-300">${svgIcon('navigation', 'h-3 w-3')}<span class="font-semibold text-slate-100">${distanceLabel}</span></p>` : ''}
            </div>
          </div>
		          <p class="mt-2 text-[11px] text-slate-400 leading-tight">Note: ${escapeHTML(partnerNoteText)}</p>
	        `;

        fragment.appendChild(card);

        if (markersAllowed && map && meta?.layer) {
          if (!map.hasLayer(meta.layer)) {
            meta.layer.addTo(map);
          }

          if (hasValidCoords(partner)) {
            const checkPulseColor =
              (isRepoAgentPartner(partner) && hasSpAgentNote(partner)) ||
              (isParkingStoragePartner(partner) && hasSpStorageNote(partner))
                ? meta.color
                : null;
            const icon = createServiceIcon(meta.color, { unauthorized: isUnauthorizedPartner(partner), checkPulseColor });
            const marker = L.marker([partner.lat, partner.lng], { icon });
            marker.on('click', (e) => {
              e.originalEvent.handledByMarker = true;
              showServicePopup(partner, meta.color);
            });
            marker.addTo(meta.layer);
          }
        }
      });

      container.appendChild(fragment);
      counter && (counter.textContent = filtered.length);
    }

    function renderVisibleSidebars() {
      if (techSidebarVisible && isServiceTypeEnabled('tech')) updateTechUI(getTechList(technicians));
      if (resellerSidebarVisible && isServiceTypeEnabled('reseller')) updateResellerUI(resellers);
      if (repairSidebarVisible && isServiceTypeEnabled('repair')) updateRepairUI(repairShops);
      if (customSidebarVisible && isServiceTypeEnabled('custom')) renderCategorySidebar(selectedCustomCategoryKey, customServices);
    }

    const PARTNER_TYPE_CONFIG = {
      reseller: { containerId: 'reseller-list', updater: updateResellerUI, getList: () => resellers },
      repair: { containerId: 'repair-list', updater: updateRepairUI, getList: () => repairShops },
      custom: { containerId: 'dynamic-service-list', updater: renderCategorySidebar, getList: () => customServices }
    };

    const findVehicleById = (idOrKey) =>
      findVehicleByKey(idOrKey) || vehicles.find((vehicle) => `${vehicle.id}` === `${idOrKey}`);
    const findTechById = (id) => technicians.find((tech) => `${tech.id}` === `${id}`);
    const findPartnerById = (type, id) => {
      const getter = PARTNER_TYPE_CONFIG[type]?.getList;
      const list = typeof getter === 'function' ? getter() : [];
      return list.find((partner) => `${partner.id}` === `${id}`);
    };

    function handleVehicleListClick(event) {
      const container = event.currentTarget;
      const target = event.target instanceof Element ? event.target : null;
      const composedPath = typeof event.composedPath === 'function' ? event.composedPath() : [];
      const findInPath = (action) => composedPath.find((node) => node?.dataset?.action === action);
      const card = target?.closest('[data-type="vehicle"]') || composedPath.find((node) => node?.dataset?.type === 'vehicle');
      const viewMoreBtn = target?.closest('[data-action="vehicle-view-more"]') || findInPath('vehicle-view-more');
      const repairHistoryBtn = target?.closest('[data-action="repair-history"]') || findInPath('repair-history');
      const gpsHistoryBtn = target?.closest('[data-action="gps-history"]') || findInPath('gps-history');
      const selectCheckbox = target?.closest('[data-action="vehicle-select-checkbox"]') || findInPath('vehicle-select-checkbox');
      if (!card || !container.contains(card)) return;

      const cardVehicleId = `${card.dataset.vehicleId || ''}`.trim();
      const cardVehicleKey = `${card.dataset.vehicleKey || card.dataset.id || ''}`.trim();
      const vehicle = vehicles.find((item) => `${item?.id ?? ''}` === cardVehicleId)
        || (cardVehicleKey ? findVehicleByKey(cardVehicleKey) : null)
        || findVehicleById(card.dataset.id);
      if (!vehicle) return;

      if (viewMoreBtn) {
        event.preventDefault();
        event.stopPropagation();
        event.stopImmediatePropagation?.();
        openVehicleModal(vehicle);
        return;
      }

      if (repairHistoryBtn) {
        event.preventDefault();
        event.stopPropagation();
        event.stopImmediatePropagation?.();
        void openRepairModal(vehicle);
        return;
      }

      if (gpsHistoryBtn) {
        event.preventDefault();
        event.stopPropagation();
        event.stopImmediatePropagation?.();
        void handleGpsHistoryRequest(vehicle);
        return;
      }

      if (selectCheckbox) {
        event.stopPropagation();
        return;
      }

      focusVehicle(vehicle);
    }

    function handleTechListClick(event) {
      const container = event.currentTarget;
      const card = event.target.closest('[data-type="tech"]');
      if (!card || !container.contains(card)) return;

      const tech = findTechById(card.dataset.id);
      if (!tech) return;

      map.flyTo([tech.lat, tech.lng], 15, { duration: 1.2 });
      applySelection(null, tech.id);
    }

    function handlePartnerListClick(event, type) {
      const container = event.currentTarget;
      const card = event.target.closest('[data-type="partner"]');
      if (!card || !container.contains(card) || card.dataset.partnerType !== type) return;

      const partner = findPartnerById(type, card.dataset.id);
      if (!partner) return;

      sidebarStateController?.setState?.(SERVICE_SIDEBAR_KEYS[type] || 'left', true);
      if (hasValidCoords(partner) && map) {
        map.flyTo([partner.lat, partner.lng], 15, { duration: 1.2 });
      }
      selectedServiceByType[type] = partner;

      const originPoint = type === 'custom' ? null : getCurrentOrigin() || partner;
      if (originPoint) {
        showServicesFromOrigin(originPoint, { forceType: type });
      }

      if (type === 'custom' && hasValidCoords(partner)) {
        showServicePopup(partner, partner.color || SERVICE_COLORS.custom);
      }

      const updater = PARTNER_TYPE_CONFIG[type]?.updater;
      if (typeof updater === 'function') updater();
    }

    function setupEventDelegation() {
      const vehicleList = document.getElementById('vehicle-list');
      if (vehicleList) vehicleList.addEventListener('click', handleVehicleListClick);

      const techList = document.getElementById('tech-list');
      if (techList) techList.addEventListener('click', handleTechListClick);

      Object.entries(PARTNER_TYPE_CONFIG).forEach(([type, config]) => {
        const container = document.getElementById(config.containerId);
        if (!container) return;
        container.addEventListener('click', (event) => handlePartnerListClick(event, type));
      });
    }

    function setupLayerToggles() {
      bindRouteLayerToggle();

      createLayerToggle({
        toggleId: 'toggle-hotspots',
        labelOn: 'Hide Hotspot Areas',
        labelOff: 'Show Hotspot Areas',
        getVisible: () => hotspotsVisible,
        setVisible: (value) => {
          hotspotsVisible = value;
        },
        onShow: () => {
          if (!hotspotLayer || !map) return;
          if (!map.hasLayer(hotspotLayer)) hotspotLayer.addTo(map);
          renderHotspots();
        },
        onHide: () => {
          if (!hotspotLayer || !map) return;
          if (map.hasLayer(hotspotLayer)) map.removeLayer(hotspotLayer);
        }
      });

      createLayerToggle({
        toggleId: 'toggle-blacklist',
        labelOn: 'Hide Blacklist Marks',
        labelOff: 'Show Blacklist Marks',
        getVisible: () => blacklistMarkersVisible,
        setVisible: (value) => {
          blacklistMarkersVisible = value;
        },
        onShow: () => {
          if (!blacklistLayer || !map) return;
          if (!map.hasLayer(blacklistLayer)) blacklistLayer.addTo(map);
          renderBlacklistSites();
        },
        onHide: () => {
          if (!blacklistLayer || !map) return;
          if (map.hasLayer(blacklistLayer)) map.removeLayer(blacklistLayer);
        }
      });

      createLayerToggle({
        toggleId: 'toggle-vehicle-markers',
        labelOn: 'Hide Vehicles Marks',
        labelOff: 'Show Vehicles Marks',
        getVisible: () => vehicleMarkersVisible,
        setVisible: (value) => {
          vehicleMarkersVisible = value;
        },
        onShow: () => {
          if (vehicleLayer && map && !map.hasLayer(vehicleLayer)) {
            vehicleLayer.addTo(map);
          }
          renderVehicles();
        },
        onHide: () => {
          if (vehicleLayer) {
            vehicleLayer.clearLayers();
            if (map?.hasLayer(vehicleLayer)) map.removeLayer(vehicleLayer);
          }
          vehicleMarkers.clear();
          highlightLayer?.clearLayers();
          gpsTrailLayer?.clearLayers();
          gpsTrailRequestCounter += 1;
        }
      });
    }

    function renderVehicles() {
      const container = document.getElementById('vehicle-list');
      if (!container) return;

      if (map) map.closePopup();
      container.replaceChildren();

      if (!vehicleMarkersVisible && map?.hasLayer(vehicleLayer)) {
        map.removeLayer(vehicleLayer);
      } else if (vehicleMarkersVisible && map && vehicleLayer && !map.hasLayer(vehicleLayer)) {
        vehicleLayer.addTo(map);
      }

      const searchBox = document.getElementById('vehicle-search');
      const filtered = getVehicleList(searchBox?.value || '');
      document.getElementById('vehicles-count').textContent = filtered.length;

      if (filtered.length === 0) {
        const empty = document.createElement('div');
        empty.className = 'text-center mt-10 text-slate-600 text-xs';
        empty.textContent = 'No vehicles match your search.';
        container.replaceChildren(empty);
        syncVehicleMarkers({
          vehiclesWithCoords: [],
          vehicleLayer,
          vehicleMarkers,
          visible: vehicleMarkersVisible,
          getVehicleMarkerKey: getVehicleKey,
          getVehicleMarkerColor,
          getVehicleMarkerBorderColor,
          isVehicleNotMoving
        });
        return;
      }

      const fragment = document.createDocumentFragment();
      const vehiclesForMarkers = [];
      const canOpenRepairHistory = isAuthenticatedCached();

      filtered.forEach((vehicle, idx) => {
        const currentVehicleKey = getVehicleKey(vehicle);
        const movingMeta = getMovingMeta(vehicle);

          const focusHandler = useCallback(
            `vehicle-focus-${currentVehicleKey}`,
            () => (payload = {}) => {
              const currentVehicle = findVehicleByKey(currentVehicleKey)
                || vehicles.find((item) => `${item.id}` === `${vehicle.id}`)
                || vehicle;
              if (!currentVehicle) return;
              const originalEvent = payload?.event?.originalEvent;
              const vehicleSelectionMatchesKey = !selectedVehicleKey || `${selectedVehicleKey}` === currentVehicleKey;
              const vehicleSelectionChanged = (
                `${selectedVehicleId ?? ''}` !== `${currentVehicle.id}`
                || !vehicleSelectionMatchesKey
              );
              if (originalEvent) {
                originalEvent.handledByMarker = true;
                originalEvent.cycleRole = 'vehicle';
                originalEvent.cycleKey = `vehicle-${currentVehicleKey}`;
                originalEvent.vehicleSelectionChanged = vehicleSelectionChanged;
              }
              if (!vehicleSelectionChanged) {
                if (payload?.marker && typeof payload.marker.openPopup === 'function') {
                  payload.marker.openPopup();
                } else {
                  focusVehicle(currentVehicle);
                }
                return;
              }
              focusVehicle(currentVehicle);
            },
            [currentVehicleKey]
          );

          const coordsInfo = getVehicleMapCoords(vehicle);
          const coords = { lat: coordsInfo.lat, lng: coordsInfo.lng };
          if (coordsInfo.isFallback) {
            vehicle.locationAccuracy = 'missing';
            vehicle.locationNote = MISSING_VEHICLE_GPS_NOTE;
          } else {
            vehicle.locationAccuracy = vehicle.locationAccuracy || 'exact';
            vehicle.locationNote = getLocationNote(vehicle.locationAccuracy);
          }

          if (coords && vehicleMarkersVisible) {
            vehiclesForMarkers.push({ vehicle, coords, focusHandler });
          }

          if (idx >= VEHICLE_RENDER_LIMIT) return;

          const card = document.createElement('div');
          const prepStatusStyles = getStatusCardStyles(vehicle.invPrepStatus, 'prep');
          const isThirdPartyRepairShop = String(vehicle.invPrepStatus || '').trim().toLowerCase() === 'third party repair shop';
          const prepStatusDisplay = isThirdPartyRepairShop ? 'Repair Shop' : (vehicle.invPrepStatus || '—');
          const ptStatusStyles = getStatusCardStyles(vehicle.ptStatus, 'pt');
          const gpsFixText = String(vehicle.gpsFix || 'GPS issue');
          const gpsReasonText = String(vehicle.gpsReason || 'Pending');
          const hasNoGps = `${gpsFixText} ${gpsReasonText}`.toLowerCase().includes('no gps');
          const gpsCardClasses = hasNoGps
            ? 'rounded-lg border border-red-500/70 bg-red-950/40 px-2 py-1.5 flex items-center gap-2 text-red-100'
            : 'rounded-lg border border-slate-800 bg-slate-950/70 px-2 py-1.5 flex items-center gap-2 text-slate-200';
          const gpsIconClasses = hasNoGps ? 'h-3 w-3 text-red-300' : 'h-3 w-3 text-amber-300';
          const gpsReasonClasses = hasNoGps ? 'text-[10px] text-red-200/90' : 'text-[10px] text-slate-400';
          const latLabel = Number.isFinite(Number(vehicle.lat)) ? Number(vehicle.lat).toFixed(5) : '—';
          const longLabel = Number.isFinite(Number(vehicle.lng)) ? Number(vehicle.lng).toFixed(5) : '—';
          const oldestOpenInvoiceDateLabel = escapeHTML(vehicle.oldestOpenInvoiceDateDisplay || '—');
          const ptLastReadLabel = formatDateTime(
            vehicle.lastRead
            ?? vehicle?.details?.pt_last_read
            ?? vehicle?.details?.['PT Last Read']
          );
          const ptFirstReadLabel = formatDateTime(
            vehicle.firstRead
            ?? vehicle?.details?.pt_first_read
            ?? vehicle?.details?.['PT First Read']
            ?? vehicle?.details?.pt_first_trip
            ?? vehicle?.details?.['PT First Trip']
          );
          const locationDisplay = formatVehicleSidebarAddress(
            vehicle.shortLocation || vehicle.lastLocation || '',
            vehicle.zipcode || ''
          );
          card.className = 'p-3 rounded-lg border border-slate-800 bg-slate-900/80 hover:border-amber-500/80 transition-all cursor-pointer shadow-sm hover:shadow-amber-500/20 backdrop-blur space-y-3';
          card.dataset.id = currentVehicleKey;
          card.dataset.vehicleKey = currentVehicleKey;
          card.dataset.vehicleId = `${vehicle.id ?? ''}`;
          card.dataset.type = 'vehicle';
          card.innerHTML = `
            <div class="flex items-start justify-between gap-3">
              <div class="space-y-1">
                <p class="text-[10px] font-black uppercase tracking-[0.15em] text-amber-300">${vehicle.model}</p>
                <h3 class="font-extrabold text-white text-sm leading-tight flex items-center gap-2">${vehicle.year || '—'} <span class="text-slate-600">•</span> ${vehicle.vin || 'VIN N/A'}</h3>
                <p class="text-[11px] text-slate-400 flex items-center gap-1">${svgIcon('mapPin')} ${escapeHTML(locationDisplay)}</p>
                <p class="text-[11px] text-slate-300 flex items-center gap-1">${svgIcon('layers', 'h-3.5 w-3.5 text-cyan-300')} Physical Location: <span class="text-slate-100">${vehicle.physicalLocation || '—'}</span></p>
                ${vehicle.locationNote ? `<p class="text-[10px] text-amber-200 font-semibold">${vehicle.locationNote}</p>` : ''}
                <p class="text-[11px] text-blue-200 font-semibold">Customer ID: <span class="text-slate-100">${vehicle.customerId || '—'}</span></p>
              </div>
              <div class="flex flex-col items-end gap-1 text-right">
                <div class="inline-flex items-center gap-2 text-[10px] font-bold text-slate-300">
                  <span class="px-2 py-1 rounded-full border border-amber-400/40 bg-amber-500/10 text-amber-100">${vehicle.dealStatus || vehicle.status || 'ACTIVE'}</span>
                </div>
                <div class="inline-flex items-center gap-2 text-[10px] font-semibold ${movingMeta.text} px-2 py-1 rounded-full border border-slate-800 ${movingMeta.bg}">
                  <span class="w-2 h-2 rounded-full ${movingMeta.dot}"></span>
                  <span>${movingMeta.label}</span>
                </div>
                <div class="text-[10px] font-semibold text-slate-300">
                  Days Parked <span class="text-slate-100">${getDaysParkedDisplay(vehicle)}</span>
                </div>
              </div>
            </div>
            <div class="grid grid-cols-2 gap-2 text-[11px]">
              <div class="${gpsCardClasses}">
                ${svgIcon('wifiOff', gpsIconClasses)}
                <div class="text-left leading-tight">
                  <p class="font-semibold">${gpsFixText}</p>
                  <p class="${gpsReasonClasses}">${gpsReasonText}</p>
                </div>
              </div>
              <div class="rounded-lg border border-slate-800 bg-slate-950/70 px-2 py-1.5 flex items-center gap-2 text-slate-200">
                ${svgIcon('hash', 'h-3 w-3 text-blue-400')}
                <div class="text-left leading-tight">
                  <p class="font-semibold">VIN</p>
                  <p class="text-[10px] text-slate-400">${vehicle.vin || 'Not available'}</p>
                </div>
              </div>
            </div>
            <div class="rounded-lg border border-slate-800 bg-slate-950/70 px-3 py-2 text-[10px] text-slate-200 space-y-2">
              <div class="grid grid-cols-4 gap-2">
                <div class="rounded border border-slate-800 bg-slate-900 px-2 py-1.5">
                  <p class="text-[9px] uppercase text-slate-500 font-bold">Pay KPI</p>
                  <p class="text-[11px] font-semibold text-slate-100">${vehicle.payKpiDisplay || '—'}</p>
                  <p class="text-[9px] text-slate-400 mt-0.5">${oldestOpenInvoiceDateLabel}</p>
                </div>
                <div class="rounded border px-2 py-1.5 ${prepStatusStyles.card}">
                  <p class="text-[9px] uppercase font-bold ${prepStatusStyles.label}">Prep</p>
                  <p class="text-[11px] font-semibold ${prepStatusStyles.value}">${prepStatusDisplay}</p>
                </div>
                <div class="rounded border border-slate-800 bg-slate-900 px-2 py-1.5">
                  <p class="text-[9px] uppercase text-slate-500 font-bold">Deal %</p>
                  <p class="text-[11px] font-semibold text-slate-100">${vehicle.dealCompletion || '—'}</p>
                </div>
                <div class="rounded border px-2 py-1.5 ${ptStatusStyles.card}">
                  <p class="text-[9px] uppercase font-bold ${ptStatusStyles.label}">PT</p>
                  <p class="text-[11px] font-semibold ${ptStatusStyles.value}">${vehicle.ptStatus || '—'}</p>
                </div>
              </div>
              <div class="flex items-center justify-between text-[11px] text-slate-400">
                <span class="flex items-center gap-2 font-semibold text-slate-200">${svgIcon('clock', 'h-3 w-3')} PT Last Read ${ptLastReadLabel}</span>
                <span class="text-slate-400">${vehicle.payment || ''}</span>
              </div>
              <div class="flex items-center text-[10px] text-slate-400">
                <span class="flex items-center gap-2">${svgIcon('clock', 'h-3 w-3')} PT First Read ${ptFirstReadLabel}</span>
              </div>
              <div class="flex items-center justify-between text-[10px] text-slate-400">
                <span>Lat <span class="text-slate-200">${latLabel}</span></span>
                <span>Long <span class="text-slate-200">${longLabel}</span></span>
              </div>
            </div>
            <div class="pt-1 flex items-end justify-between gap-2">
              <div class="flex flex-col items-start gap-1">
                <label class="inline-flex flex-col items-start gap-1 text-[10px] font-semibold text-slate-300 leading-tight">
                  <span>on rev?</span>
                  <input type="checkbox" data-action="vehicle-select-checkbox" class="h-3.5 w-3.5 rounded border-slate-600 bg-slate-900 text-amber-400 focus:ring-amber-400" ${(checkedVehicleStateByVin.get(getVehicleVin(vehicle)) ?? checkedVehicleIds.has(vehicle.id)) ? 'checked' : ''}>
                </label>
                <p data-action="vehicle-select-last-click" class="text-[9px] text-slate-500">${(checkedVehicleClickTimes.get(vehicle.id) || checkedVehicleClickTimesByVin.get(getVehicleVin(vehicle))) ? formatDateTime(checkedVehicleClickTimes.get(vehicle.id) || checkedVehicleClickTimesByVin.get(getVehicleVin(vehicle))) : '--'}</p>
              </div>
              <div class="flex items-center justify-end gap-2">
                <button type="button" data-view-more data-action="vehicle-view-more" class="inline-flex items-center gap-1.5 rounded-lg border border-amber-400/50 bg-amber-500/15 px-3 py-1 text-[10px] font-bold text-amber-100 hover:bg-amber-500/25 transition-colors">
                  ${svgIcon('info', 'h-3.5 w-3.5')}
                  See more
                </button>
                ${canOpenRepairHistory
                  ? '<button type="button" data-action="repair-history" class="inline-flex items-center gap-1.5 rounded-lg border border-blue-400/50 bg-blue-500/15 px-3 py-1 text-[10px] font-bold text-blue-100 hover:bg-blue-500/25 transition-colors">Service History</button>'
                  : ''}
                <button type="button" data-action="gps-history" class="inline-flex items-center gap-1.5 rounded-lg border border-emerald-400/50 bg-emerald-500/15 px-3 py-1 text-[10px] font-bold text-emerald-100 hover:bg-emerald-500/25 transition-colors">GPS Historic</button>
              </div>
            </div>
          `;

          const selectCheckbox = card.querySelector('[data-action="vehicle-select-checkbox"]');
          const selectLastClick = card.querySelector('[data-action="vehicle-select-last-click"]');
          if (selectCheckbox) {
            selectCheckbox.addEventListener('click', (event) => {
              event.stopPropagation();
            });
            selectCheckbox.addEventListener('change', (event) => {
              event.stopPropagation();
              const clickedAt = new Date().toISOString();
              const vin = getVehicleVin(vehicle);
              const isChecked = Boolean(event.currentTarget.checked);
              checkedVehicleClickTimes.set(vehicle.id, clickedAt);
              if (vin) {
                checkedVehicleClickTimesByVin.set(vin, clickedAt);
                checkedVehicleStateByVin.set(vin, isChecked);
              }
              if (selectLastClick) {
                selectLastClick.textContent = formatDateTime(clickedAt);
              }
              void saveVehicleClickHistory(vehicle, clickedAt, isChecked);
              if (isChecked) {
                checkedVehicleIds.add(vehicle.id);
              } else {
                checkedVehicleIds.delete(vehicle.id);
              }
            });
          }

          const viewMoreButton = card.querySelector('[data-action="vehicle-view-more"]');
          if (viewMoreButton) {
            viewMoreButton.addEventListener('click', (event) => {
              event.stopPropagation();
              openVehicleModal(vehicle);
            });
          }

          const repairHistoryButton = card.querySelector('[data-action="repair-history"]');
          if (repairHistoryButton) {
            repairHistoryButton.addEventListener('click', (event) => {
              event.stopPropagation();
              void openRepairModal(vehicle);
            });
          }

          const gpsHistoryButton = card.querySelector('[data-action="gps-history"]');
          if (gpsHistoryButton) {
            gpsHistoryButton.addEventListener('click', (event) => {
              event.stopPropagation();
              void handleGpsHistoryRequest(vehicle);
            });
          }

          fragment.appendChild(card);
        });

        if (filtered.length > VEHICLE_RENDER_LIMIT) {
          const notice = document.createElement('p');
          notice.className = 'text-[11px] text-slate-500 px-1 pt-1';
          notice.textContent = `Showing the first ${VEHICLE_RENDER_LIMIT} of ${filtered.length} vehicles. Refine filters to see more.`;
          fragment.appendChild(notice);
        }

        container.replaceChildren(fragment);

        syncVehicleMarkers({
          vehiclesWithCoords: vehiclesForMarkers,
          vehicleLayer,
          vehicleMarkers,
          visible: vehicleMarkersVisible,
          getVehicleMarkerKey: getVehicleKey,
          getVehicleMarkerColor,
          getVehicleMarkerBorderColor,
          isVehicleNotMoving
        });
      }

    function focusVehicle(vehicle) {
      const trailRequestId = ++gpsTrailRequestCounter;
      if (!vehicle) return;
      if (!vehicleMarkersVisible) return;
      const vehicleCoords = getVehicleMapCoords(vehicle);
      clearParkingSpotCascade();
      highlightLayer.clearLayers();
      gpsTrailLayer?.clearLayers();

      const vehicleKey = getVehicleKey(vehicle);
      const storedMarker = vehicleMarkers.get(vehicleKey)?.marker;
      const markerColor = getVehicleMarkerColor(vehicle);
      const anchorMarker = storedMarker || L.circleMarker([vehicleCoords.lat, vehicleCoords.lng], {
        radius: 9,
        color: '#0b1220',
        weight: 2.8,
        fillColor: markerColor,
        fillOpacity: 0.95,
        opacity: 0.98,
        className: 'vehicle-dot',
        cycleRole: 'vehicle',
        vehicleKey,
        cycleKey: `vehicle-${vehicleKey}`
      }).addTo(highlightLayer);

      const halo = L.circleMarker([vehicleCoords.lat, vehicleCoords.lng], { radius: 12, color: markerColor, weight: 1.2, fillColor: markerColor, fillOpacity: 0.18 }).addTo(highlightLayer);
      halo.bringToBack();

      syncFocusedVehicleNotMovingBadge(vehicle);

      anchorMarker.bindPopup(vehicleCard(vehicle), {
        className: 'vehicle-popup',
        autoPan: false,
        keepInView: false,
      }).openPopup();

      setTimeout(attachPopupHandlers, 50);

      showServicesFromOrigin(vehicle, { forceType: isAnyServiceSidebarOpen() ? getActivePartnerType() : null });
      const needsSelectionUpdate = (
        `${selectedVehicleId ?? ''}` !== `${vehicle.id ?? ''}`
        || `${selectedVehicleKey ?? ''}` !== `${vehicleKey}`
      );
      void renderVehicleGpsTrail(vehicle, trailRequestId);
      if (needsSelectionUpdate) {
        applySelection(vehicle.id, null, vehicleKey);
      }
    }

    function vehicleCard(vehicle) {
      const accuracyDot = getAccuracyDot(vehicle.locationAccuracy);
      const modelYear = [vehicle.model || 'Vehicle', vehicle.year].filter(Boolean).join(' · ');
      return vehiclePopupTemplate({
        modelYear: modelYear || 'Vehicle',
        vin: vehicle.vin || 'N/A',
        status: vehicle.dealStatus || vehicle.status || 'ACTIVE',
        customer: vehicle.customer || 'Customer pending',
        lastLocation: vehicle.lastLocation || 'No location provided',
        locationNote: vehicle.locationNote || '',
        accuracyDot,
        gpsFix: vehicle.gpsFix || 'Unknown',
        dealCompletion: vehicle.dealCompletion || '—',
        mapsUrl: buildGoogleMapsSearchUrl(vehicle.lat, vehicle.lng)
      });
    }

    function matchesRange(value, min, max) {
      const pct = parseDealCompletion(value);
      // Vehicles with unknown completion should remain visible in the map list.
      if (!Number.isFinite(pct)) return true;
      return pct >= min && pct <= max;
    }

    const EMPTY_FILTER_VALUE = '__empty__';

    function matchesVehicleFilters(vehicle, query) {
      const q = (query || '').toLowerCase();
      if (q && !vehicle._searchBlob.includes(q)) return false;

      const invPrep = normalizeFilterValue(vehicle.invPrepStatus);
      if (vehicleFilters.invPrep.length && !vehicleFilters.invPrep.includes(invPrep)) return false;

      const physicalLocation = normalizeFilterValue(vehicle.physicalLocation);
      if (vehicleFilters.physLoc.length && !vehicleFilters.physLoc.includes(physicalLocation)) return false;

      const gpsFixValue = normalizeFilterValue(vehicle.gpsFix) || EMPTY_FILTER_VALUE;
      if (vehicleFilters.gpsFix.length && !vehicleFilters.gpsFix.includes(gpsFixValue)) return false;

      const dealStatus = normalizeFilterValue(vehicle.dealStatus ?? vehicle.status);
      if (vehicleFilters.dealStatus.length && !vehicleFilters.dealStatus.includes(dealStatus)) return false;

      const ptStatus = normalizeFilterValue(vehicle.ptStatus);
      if (vehicleFilters.ptStatus.length && !vehicleFilters.ptStatus.includes(ptStatus)) return false;

      if (!matchesRange(vehicle.dealCompletion, vehicleFilters.dealMin, vehicleFilters.dealMax)) return false;

      const movingStatus = getMovingStatus(vehicle);
      if (vehicleFilters.moving.length && !vehicleFilters.moving.includes(movingStatus)) return false;

      if (vehicleFilters.payKpiPositiveOnly) {
        const payKpi = Number(vehicle?.payKpi);
        if (!Number.isFinite(payKpi) || payKpi <= 0) return false;
      }

      return true;
    }

    function filterVehicles(list, query) {
      return list.filter(v => matchesVehicleFilters(v, query));
    }

    function getVehiclesForTech(tech) {
      if (!tech || !map) return [];
      const withDistance = vehicles
        .filter(hasValidCoords)
        .map(v => ({
          vehicle: v,
          distance: getDistance(v.lat, v.lng, tech.lat, tech.lng)
        }));
      return withDistance
        .filter(item => item.distance <= 180)
        .sort((a, b) => a.distance - b.distance)
        .map(item => item.vehicle);
    }

    function getDaysParkedValue(vehicle) {
      const raw = vehicle?.historyDaysStationaryOverride
        ?? vehicle?.details?.historyDaysStationaryOverride
        ?? vehicle?.daysStationary
        ?? vehicle?.details?.days_stationary
        ?? vehicle?.details?.['Days Parked'];
      if (typeof raw === 'number') return Number.isFinite(raw) ? raw : Number.NEGATIVE_INFINITY;
      if (typeof raw === 'string') {
        const normalized = raw.replace(/,/g, '').trim();
        const parsed = Number.parseFloat(normalized);
        if (Number.isFinite(parsed)) return parsed;
      }
      return Number.NEGATIVE_INFINITY;
    }

    function getDaysParkedDisplay(vehicle) {
      const days = getDaysParkedValue(vehicle);
      if (!Number.isFinite(days)) return '—';
      return `${Math.max(0, Math.floor(days))}`;
    }

    function sortVehiclesByDaysParked(list) {
      return [...list].sort((a, b) => getDaysParkedValue(b) - getDaysParkedValue(a));
    }

    function isVehicleOnRevChecked(vehicle) {
      const vin = getVehicleVin(vehicle);
      const checkedByVin = vin ? checkedVehicleStateByVin.get(vin) : undefined;
      return checkedByVin ?? checkedVehicleIds.has(vehicle.id);
    }

    function sortVehiclesByOnRevChecked(list) {
      return [...list].sort((a, b) => Number(isVehicleOnRevChecked(b)) - Number(isVehicleOnRevChecked(a)));
    }

    function getPayKpiValue(vehicle) {
      const value = Number(vehicle?.payKpi);
      return Number.isFinite(value) ? value : Number.NEGATIVE_INFINITY;
    }

    function sortVehiclesByPayKpiDesc(list) {
      return [...list].sort((a, b) => getPayKpiValue(b) - getPayKpiValue(a));
    }

    function getUniqueVehicleValues(field, { includeEmpty = false } = {}) {
      const values = new Set();
      let hasEmpty = false;
      vehicles.forEach((vehicle) => {
        const normalized = normalizeFilterValue(vehicle?.[field]);
        if (!normalized) {
          if (includeEmpty) hasEmpty = true;
          return;
        }
        values.add(normalized);
      });
      const list = Array.from(values).sort();
      if (includeEmpty && hasEmpty) {
        list.unshift(EMPTY_FILTER_VALUE);
      }
      return list;
    }

    function getMovingOptions() {
      const values = new Set();
      vehicles.forEach((vehicle) => {
        values.add(getMovingStatus(vehicle));
      });
      return Array.from(values).sort();
    }

    function normalizeFilterSelections(values, selections) {
      if (!Array.isArray(selections)) return [];
      return selections.filter((value) => values.includes(value));
    }

    function renderCheckboxOptions(containerId, values, selections, labelResolver = (value) => value) {
      const container = document.getElementById(containerId);
      if (!container) return;
      container.innerHTML = '';
      if (!values.length) {
        const empty = document.createElement('p');
        empty.className = 'text-[9px] text-slate-600';
        empty.textContent = 'No options';
        container.appendChild(empty);
        return;
      }

      values.forEach((value) => {
        const label = document.createElement('label');
        label.className = 'flex items-center gap-1 text-[9px] text-slate-200';

        const input = document.createElement('input');
        input.type = 'checkbox';
        input.value = value;
        input.checked = selections.includes(value);
        input.className = 'h-2.5 w-2.5 rounded border border-slate-700 bg-slate-950 text-amber-400 focus:ring-1 focus:ring-amber-400';

        const span = document.createElement('span');
        span.textContent = labelResolver(value);

        label.appendChild(input);
        label.appendChild(span);
        container.appendChild(label);
      });
    }

    function updateVehicleFilterOptions() {
      const invPrepValues = getUniqueVehicleValues('invPrepStatus');
      const physicalLocationValues = getUniqueVehicleValues('physicalLocation');
      const gpsValues = getUniqueVehicleValues('gpsFix', { includeEmpty: true });
      const dealStatusValues = Array.from(
        new Set(
          vehicles
            .map((vehicle) => normalizeFilterValue(vehicle.dealStatus ?? vehicle.status))
            .filter(Boolean)
        )
      ).sort();
      const ptValues = getUniqueVehicleValues('ptStatus');
      const movingValues = getMovingOptions();

      vehicleFilters.invPrep = normalizeFilterSelections(invPrepValues, vehicleFilters.invPrep);
      vehicleFilters.physLoc = normalizeFilterSelections(physicalLocationValues, vehicleFilters.physLoc);
      vehicleFilters.gpsFix = normalizeFilterSelections(gpsValues, vehicleFilters.gpsFix);
      vehicleFilters.dealStatus = normalizeFilterSelections(dealStatusValues, vehicleFilters.dealStatus);
      vehicleFilters.ptStatus = normalizeFilterSelections(ptValues, vehicleFilters.ptStatus);
      vehicleFilters.moving = normalizeFilterSelections(movingValues, vehicleFilters.moving);

      renderCheckboxOptions('filter-invprep', invPrepValues, vehicleFilters.invPrep);
      renderCheckboxOptions('filter-physloc', physicalLocationValues, vehicleFilters.physLoc);
      renderCheckboxOptions('filter-gps', gpsValues, vehicleFilters.gpsFix, (value) => getGpsFixLabel(value, EMPTY_FILTER_VALUE));
      renderCheckboxOptions('filter-deal-status', dealStatusValues, vehicleFilters.dealStatus);
      renderCheckboxOptions('filter-pt', ptValues, vehicleFilters.ptStatus);
      renderCheckboxOptions('filter-moving', movingValues, vehicleFilters.moving, getMovingLabel);
      updateVehicleFilterLabels();
    }

    function updateVehicleFilterLabel(toggleId, selections, labelResolver = (value) => value) {
      const toggle = document.getElementById(toggleId);
      if (!toggle) return;
      const label = toggle.querySelector('span');
      if (!label) return;
      if (!selections.length) {
        label.textContent = 'Select';
        return;
      }
      if (selections.length === 1) {
        label.textContent = labelResolver(selections[0]);
        return;
      }
      label.textContent = `${selections.length} selected`;
    }

    function updateVehicleFilterLabels() {
      updateVehicleFilterLabel('filter-invprep-toggle', vehicleFilters.invPrep);
      updateVehicleFilterLabel('filter-physloc-toggle', vehicleFilters.physLoc);
      updateVehicleFilterLabel('filter-gps-toggle', vehicleFilters.gpsFix, (value) => getGpsFixLabel(value, EMPTY_FILTER_VALUE));
      updateVehicleFilterLabel('filter-moving-toggle', vehicleFilters.moving, getMovingLabel);
      updateVehicleFilterLabel('filter-deal-status-toggle', vehicleFilters.dealStatus);
      updateVehicleFilterLabel('filter-pt-toggle', vehicleFilters.ptStatus);
    }

    function syncVehicleFilterInputs() {
      const invPrepContainer = document.getElementById('filter-invprep');
      const physLocContainer = document.getElementById('filter-physloc');
      const gpsContainer = document.getElementById('filter-gps');
      const movingContainer = document.getElementById('filter-moving');
      const dealStatusContainer = document.getElementById('filter-deal-status');
      const ptContainer = document.getElementById('filter-pt');
      const minInput = document.getElementById('filter-deal-min');
      const maxInput = document.getElementById('filter-deal-max');
      const trailPointsInput = document.getElementById('filter-trail-points');
      const kpiPositiveToggle = document.getElementById('filter-kpi-positive-toggle');

      const syncCheckboxes = (container, selections) => {
        if (!container) return;
        container.querySelectorAll('input[type="checkbox"]').forEach((input) => {
          input.checked = selections.includes(input.value);
        });
      };

      syncCheckboxes(invPrepContainer, vehicleFilters.invPrep);
      syncCheckboxes(physLocContainer, vehicleFilters.physLoc);
      syncCheckboxes(gpsContainer, vehicleFilters.gpsFix);
      syncCheckboxes(movingContainer, vehicleFilters.moving);
      syncCheckboxes(dealStatusContainer, vehicleFilters.dealStatus);
      syncCheckboxes(ptContainer, vehicleFilters.ptStatus);
      if (minInput) minInput.value = vehicleFilters.dealMin;
      if (maxInput) maxInput.value = vehicleFilters.dealMax;
      if (trailPointsInput) trailPointsInput.value = String(vehicleFilters.trailPoints);
      if (kpiPositiveToggle) {
        const isActive = Boolean(vehicleFilters.payKpiPositiveOnly);
        const stateLabel = kpiPositiveToggle.querySelector('[data-kpi-filter-state]');
        kpiPositiveToggle.setAttribute('aria-pressed', String(isActive));
        kpiPositiveToggle.classList.toggle('border-emerald-400/60', isActive);
        kpiPositiveToggle.classList.toggle('bg-emerald-500/20', isActive);
        kpiPositiveToggle.classList.toggle('text-emerald-100', isActive);
        kpiPositiveToggle.classList.toggle('border-slate-800', !isActive);
        kpiPositiveToggle.classList.toggle('bg-slate-950', !isActive);
        kpiPositiveToggle.classList.toggle('text-slate-300', !isActive);
        if (stateLabel) {
          stateLabel.textContent = isActive ? 'On' : 'Off';
          stateLabel.classList.toggle('text-emerald-200', isActive);
          stateLabel.classList.toggle('text-slate-500', !isActive);
        }
      }
      updateVehicleFilterLabels();
    }

    function resetVehicleFilters() {
      vehicleFilters.invPrep = [];
      vehicleFilters.physLoc = [];
      vehicleFilters.gpsFix = [];
      vehicleFilters.moving = [];
      vehicleFilters.dealStatus = [];
      vehicleFilters.ptStatus = [];
      vehicleFilters.dealMin = 0;
      vehicleFilters.dealMax = 100;
      vehicleFilters.trailPoints = DEFAULT_GPS_TRAIL_POINT_LIMIT;
      vehicleFilters.payKpiPositiveOnly = false;
      gpsTrailPointLimit = DEFAULT_GPS_TRAIL_POINT_LIMIT;
      syncVehicleFilterInputs();
      renderVehicles();
      persistVehicleFilterPrefs();
    }

    function bindVehicleFilterDropdowns() {
      const entries = VEHICLE_FILTER_DROPDOWN_IDS
        .map(({ toggleId, panelId }) => ({
          toggle: document.getElementById(toggleId),
          panel: document.getElementById(panelId)
        }))
        .filter(({ toggle, panel }) => toggle && panel);

      if (!entries.length) return;

      const closeAll = (exceptPanel = null) => {
        entries.forEach(({ toggle, panel }) => {
          if (panel === exceptPanel) return;
          panel.classList.add('hidden');
          toggle.setAttribute('aria-expanded', 'false');
        });
      };

      entries.forEach(({ toggle, panel }) => {
        toggle.addEventListener('click', (event) => {
          event.preventDefault();
          if (vehicleFiltersCollapsed) return;
          const isHidden = panel.classList.contains('hidden');
          closeAll(isHidden ? panel : null);
          panel.classList.toggle('hidden', !isHidden);
          toggle.setAttribute('aria-expanded', String(isHidden));
        });
      });

      document.addEventListener('click', (event) => {
        const target = event.target;
        const clickedInside = entries.some(({ toggle, panel }) => toggle.contains(target) || panel.contains(target));
        if (!clickedInside) closeAll();
      });
    }

    function bindVehicleFilterHandlers() {
      const checkboxHandlers = [
        { id: 'filter-invprep', key: 'invPrep' },
        { id: 'filter-physloc', key: 'physLoc' },
        { id: 'filter-gps', key: 'gpsFix' },
        { id: 'filter-moving', key: 'moving' },
        { id: 'filter-deal-status', key: 'dealStatus' },
        { id: 'filter-pt', key: 'ptStatus' }
      ];

      const getCheckedValues = (container) => Array.from(container.querySelectorAll('input[type="checkbox"]:checked'))
        .map((input) => input.value);

      checkboxHandlers.forEach(({ id, key }) => {
        const container = document.getElementById(id);
        if (!container) return;
        container.addEventListener('change', () => {
          vehicleFilters[key] = getCheckedValues(container);
          updateVehicleFilterLabels();
          renderVehicles();
          persistVehicleFilterPrefs();
        });
      });

      const minInput = document.getElementById('filter-deal-min');
      const maxInput = document.getElementById('filter-deal-max');
      [minInput, maxInput].forEach((input, idx) => {
        if (!input) return;
        input.addEventListener('input', () => {
          const val = parseFloat(input.value);
          if (Number.isFinite(val)) {
            if (idx === 0) vehicleFilters.dealMin = Math.max(0, Math.min(val, 100));
            else vehicleFilters.dealMax = Math.max(0, Math.min(val, 100));
          } else {
            if (idx === 0) vehicleFilters.dealMin = 0;
            else vehicleFilters.dealMax = 100;
          }
          if (vehicleFilters.dealMin > vehicleFilters.dealMax) {
            vehicleFilters.dealMin = vehicleFilters.dealMax;
            syncVehicleFilterInputs();
          }
          renderVehicles();
          persistVehicleFilterPrefs();
        });
      });

      const trailPointsInput = document.getElementById('filter-trail-points');
      if (trailPointsInput) {
        const applyTrailPoints = () => {
          const parsed = Number.parseInt(trailPointsInput.value, 10);
          const nextLimit = Number.isFinite(parsed)
            ? Math.max(MIN_GPS_TRAIL_POINT_LIMIT, Math.min(parsed, MAX_GPS_TRAIL_POINT_LIMIT))
            : DEFAULT_GPS_TRAIL_POINT_LIMIT;
          vehicleFilters.trailPoints = nextLimit;
          gpsTrailPointLimit = nextLimit;
          trailPointsInput.value = String(nextLimit);
          persistVehicleFilterPrefs();

          const selectedVehicle = getSelectedVehicleEntry();
          if (selectedVehicle && vehicleMarkersVisible) {
            focusVehicle(selectedVehicle);
          }
        };

        trailPointsInput.addEventListener('change', applyTrailPoints);
        trailPointsInput.addEventListener('blur', applyTrailPoints);
      }

      const kpiPositiveToggle = document.getElementById('filter-kpi-positive-toggle');
      if (kpiPositiveToggle) {
        kpiPositiveToggle.addEventListener('click', () => {
          vehicleFilters.payKpiPositiveOnly = !vehicleFilters.payKpiPositiveOnly;
          syncVehicleFilterInputs();
          renderVehicles();
          persistVehicleFilterPrefs();
        });
      }

      const resetBtn = document.getElementById('vehicle-filters-reset');
      resetBtn?.addEventListener('click', (e) => {
        e.preventDefault();
        resetVehicleFilters();
      });

      const collapseBtn = document.getElementById('vehicle-filters-collapse');
      collapseBtn?.addEventListener('click', (event) => {
        event.preventDefault();
        applyVehicleFiltersCollapsedState(!vehicleFiltersCollapsed);
      });

      bindVehicleFilterDropdowns();
      applyVehicleFiltersCollapsedState(loadVehicleFiltersCollapsedPref(), { persist: false });
    }

    function getVehicleList(query) {
      if (selectedVehicleId !== null || selectedVehicleKey) {
        const selectedVehicle = getSelectedVehicleEntry();
        if (!selectedVehicle) return [];
        return [selectedVehicle];
      }

      let baseList = vehicles;
      if (selectedTechId !== null) {
        const tech = technicians.find(t => t.id === selectedTechId);
        const nearby = getVehiclesForTech(tech);
        baseList = nearby.length ? nearby : vehicles;
      }

      const filtered = filterVehicles(baseList, query);
      const shouldSortByPayKpiDesc = vehicleFilters.payKpiPositiveOnly;
      const shouldSortByDaysParked = vehicleFilters.moving.includes('stopped');
      const shouldPrioritizeOnRev = vehicleFilters.invPrep.includes('available for deals')
        && vehicleFilters.moving.includes('moving')
        && !shouldSortByPayKpiDesc;

      let list = shouldSortByPayKpiDesc
        ? sortVehiclesByPayKpiDesc(filtered)
        : (shouldSortByDaysParked ? sortVehiclesByDaysParked(filtered) : filtered);
      if (shouldPrioritizeOnRev) {
        list = sortVehiclesByOnRevChecked(list);
      }
      return list;
    }

    function getTechList(baseList = technicians) {
      const origin = getCurrentOrigin();

      const sorted = origin
        ? baseList
            .map(t => ({ ...t, distance: getDistance(origin.lat, origin.lng, t.lat, t.lng) }))
            .sort((a, b) => a.distance - b.distance)
        : [...baseList];

      if (selectedTechId !== null) {
        const selectedTech = sorted.find(t => t.id === selectedTechId) || baseList.find(t => t.id === selectedTechId);
        if (selectedTech) {
          return [selectedTech, ...sorted.filter(t => t.id !== selectedTech.id)];
        }
      }

      return sorted;
    }

    function findNearestTechnician(point, maxDistanceMeters = 40000) {
      if (!technicians.length || !map) return null;
      let closest = null;
      let closestDistance = Infinity;

      technicians.forEach((tech) => {
        const dist = map.distance([point.lat, point.lng], [tech.lat, tech.lng]);
        if (dist < closestDistance) {
          closestDistance = dist;
          closest = tech;
        }
      });

      return closestDistance <= maxDistanceMeters ? closest : null;
    }

    function toggleSelectionBanner() {
      const banner = document.getElementById('selection-banner');
      const text = document.getElementById('selection-text');
      if (!banner || !text) return;

      if (selectedVehicleId === null && selectedTechId === null) {
        banner.classList.add('hidden');
        return;
      }

      const vehicle = getSelectedVehicleEntry();
      const tech = technicians.find(t => t.id === selectedTechId);
      const parts = [];
      if (vehicle) parts.push(`Vehicle ${vehicle.vin || vehicle.model}`);
      if (tech) parts.push(`Technician ${tech.company}`);
      text.textContent = `Filtered by ${parts.join(' & ')}`;
      banner.classList.remove('hidden');
    }

    function applySelection(vehicleId = null, techId = null, vehicleKey = null) {
      const previousVehicleId = selectedVehicleId;
      const previousVehicleKey = selectedVehicleKey;
      const previousTechId = selectedTechId;
      const normalizedVehicleKey = `${vehicleKey || ''}`.trim();
      const resolvedVehicle = vehicleId !== null
        ? (normalizedVehicleKey
          ? findVehicleByKey(normalizedVehicleKey)
          : vehicles.find((vehicle) => `${vehicle.id}` === `${vehicleId}`))
        : null;
      const nextVehicleKey = vehicleId === null
        ? null
        : (normalizedVehicleKey || getVehicleKey(resolvedVehicle || { id: vehicleId }));

      if (
        `${previousVehicleId ?? ''}` !== `${vehicleId ?? ''}`
        || `${previousVehicleKey ?? ''}` !== `${nextVehicleKey ?? ''}`
        || vehicleId === null
      ) {
        clearParkingSpotCascade();
      }
      selectedVehicleId = vehicleId;
      selectedVehicleKey = nextVehicleKey;
      selectedTechId = techId;
      if (
        `${previousVehicleId ?? ''}` !== `${vehicleId ?? ''}`
        || `${previousVehicleKey ?? ''}` !== `${nextVehicleKey ?? ''}`
        || `${previousTechId ?? ''}` !== `${techId ?? ''}`
      ) {
        resetOverlapCycleState();
      }
      if (vehicleId !== null && !syncingVehicleSelection) {
        setSelectedVehicle({
          id: resolvedVehicle?.id ?? vehicleId,
          key: nextVehicleKey || '',
          vin: resolvedVehicle?.vin || '',
          customerId: resolvedVehicle?.customerId || ''
        });
      } else if (vehicleId === null && !syncingVehicleSelection) {
        setSelectedVehicle(null);
      }
      if (vehicleId === null) {
        gpsTrailRequestCounter += 1;
        highlightLayer?.clearLayers();
        gpsTrailLayer?.clearLayers();
      }
      if (vehicleId !== null) {
        sidebarStateController?.setState?.('right', true);
      }
      renderVehicles();
      renderVisibleSidebars();
      toggleSelectionBanner();
    }

    function resetSelection() {
      gpsTrailRequestCounter += 1;
      resetOverlapCycleState();
      clearParkingSpotCascade();
      selectedVehicleId = null;
      selectedVehicleKey = null;
      selectedTechId = null;
      Object.keys(selectedServiceByType).forEach(key => delete selectedServiceByType[key]);
      highlightLayer?.clearLayers();
      gpsTrailLayer?.clearLayers();
      connectionLayer?.clearLayers();
      serviceLayer?.clearLayers();
      serviceConnectionLayer?.clearLayers();
      lastOriginPoint = null;
      lastClientLocation = null;
      if (!syncingVehicleSelection) {
        setSelectedVehicle(null);
      }
      renderVehicles();
      renderVisibleSidebars();
      toggleSelectionBanner();
    }

    function findNearestVehicle(point, maxDistanceMeters = 80000) {
      if (!vehicles.length || !map) return null;
      let closest = null;
      let closestDistance = Infinity;

      vehicles.forEach((vehicle) => {
        if (!hasValidCoords(vehicle)) return;
        const dist = map.distance([point.lat, point.lng], [vehicle.lat, vehicle.lng]);
        if (dist < closestDistance) {
          closestDistance = dist;
          closest = vehicle;
        }
      });

      return closestDistance <= maxDistanceMeters ? closest : null;
    }

    // --- 5. Real search logic (Nominatim) ---
    async function geocodeAddress(query) {
      try {
        const response = await fetch(`https://nominatim.openstreetmap.org/search?format=json&q=${encodeURIComponent(query + ", USA")}`);
        const data = await response.json();
        if (data && data.length > 0) {
          return { lat: parseFloat(data[0].lat), lng: parseFloat(data[0].lon), name: data[0].display_name };
        }
        return null;
      } catch (e) {
        console.error("Geocoding error", e);
        return null;
      }
    }

    const debouncedGeocodeAddress = debounceAsync(geocodeAddress, 400);

    function processLocation(location, label = '', partnerTypeOverride = null) {
      if (!location) return;

      if (label) {
        document.getElementById('address-input').value = label;
      }

      targetLayer.clearLayers();
      connectionLayer.clearLayers();
      serviceLayer?.clearLayers();
      serviceConnectionLayer?.clearLayers();
      const targetIcon = L.divIcon({
        className: '',
        html: `<div class="w-4 h-4 bg-red-600 rounded-full border-2 border-white animate-ping"></div>`
      });
      L.marker([location.lat, location.lng], { icon: targetIcon }).addTo(targetLayer);
      const popup = L.marker([location.lat, location.lng]).addTo(targetLayer);
      popup.bindPopup(`<b>Client</b><br>${label || location.name || ''}`).openPopup();
      lastClientLocation = location;

      const partnerType = partnerTypeOverride || (isAnyServiceSidebarOpen() ? getActivePartnerType() : null);
      if (partnerType) {
        const partnerList = getPartnerListByType(partnerType);
        if (partnerType === 'tech') {
          updateTechUI(partnerList, true, location);
        } else if (partnerType === 'reseller') {
          updateResellerUI(partnerList);
        } else if (partnerType === 'repair') {
          updateRepairUI(partnerList);
        }
      }

      showServicesFromOrigin(location, { forceType: partnerType });
      map.flyTo([location.lat, location.lng], 13, { duration: 1.5 });
    }

    const GLOBAL_SEARCH_ZIP_REGEX = /^\d{5}(?:-\d{4})?$/;

    const normalizeGlobalSearchText = (value = '') => `${value || ''}`.trim().toLowerCase();

    const normalizeGlobalSearchCompact = (value = '') => normalizeGlobalSearchText(value).replace(/[^a-z0-9]/g, '');

    const findVehicleByGlobalSearch = (query = '') => {
      const normalizedQuery = normalizeGlobalSearchText(query);
      const compactQuery = normalizeGlobalSearchCompact(query);
      if (!normalizedQuery) return null;

      let bestMatch = null;
      let bestScore = 0;

      vehicles.forEach((vehicle) => {
        const vin = normalizeGlobalSearchText(vehicle?.vin || vehicle?.VIN || '');
        const compactVin = normalizeGlobalSearchCompact(vin);
        const customerId = normalizeGlobalSearchText(vehicle?.customerId || '');
        const customerName = normalizeGlobalSearchText(vehicle?.customerName || '');
        const searchBlob = normalizeGlobalSearchText(vehicle?._searchBlob || '');
        let score = 0;

        if (vin && vin === normalizedQuery) score = Math.max(score, 560);
        if (compactVin && compactQuery && compactVin === compactQuery) score = Math.max(score, 580);
        if (customerId && customerId === normalizedQuery) score = Math.max(score, 500);
        if (customerName && customerName === normalizedQuery) score = Math.max(score, 460);
        if (vin && vin.includes(normalizedQuery)) score = Math.max(score, 420);
        if (customerId && customerId.includes(normalizedQuery)) score = Math.max(score, 390);
        if (customerName && customerName.includes(normalizedQuery)) score = Math.max(score, 370);
        if (searchBlob && searchBlob.includes(normalizedQuery)) score = Math.max(score, 310);

        if (score > bestScore) {
          bestScore = score;
          bestMatch = vehicle;
        }
      });

      return bestMatch;
    };

    const getServiceGlobalSearchHaystack = (partner = {}, type = 'tech') => {
      const noteText = type === 'tech' ? getTechNoteText(partner) : getPartnerNoteText(partner);
      return [
        partner.company,
        partner.name,
        partner.contact,
        partner.vendor,
        partner.city,
        partner.state,
        partner.region,
        partner.zip,
        partner.zipcode,
        partner.phone,
        partner.email,
        partner.availability,
        partner.authorization,
        partner.categoryLabel,
        partner.category,
        noteText
      ]
        .map((value) => normalizeGlobalSearchText(value))
        .join(' ');
    };

    const collectServiceGlobalSearchEntries = () => {
      const entries = [];
      technicians.forEach((partner) => entries.push({ type: 'tech', partner }));
      resellers.forEach((partner) => entries.push({ type: 'reseller', partner }));
      repairShops.forEach((partner) => entries.push({ type: 'repair', partner }));
      customServices.forEach((partner) => entries.push({ type: 'custom', partner }));
      return entries;
    };

    const findServiceByGlobalSearch = (query = '') => {
      const normalizedQuery = normalizeGlobalSearchText(query);
      if (!normalizedQuery) return null;

      const entries = collectServiceGlobalSearchEntries();
      let bestMatch = null;
      let bestScore = 0;

      entries.forEach((entry) => {
        const partner = entry?.partner || {};
        const company = normalizeGlobalSearchText(partner.company || partner.name);
        const zip = normalizeGlobalSearchText(partner.zip || partner.zipcode);
        const haystack = getServiceGlobalSearchHaystack(partner, entry.type);
        let score = 0;

        if (company && company === normalizedQuery) score = Math.max(score, 440);
        if (zip && zip === normalizedQuery) score = Math.max(score, 420);
        if (company && company.includes(normalizedQuery)) score = Math.max(score, 360);
        if (haystack && haystack.includes(normalizedQuery)) score = Math.max(score, 290);

        if (score > bestScore) {
          bestScore = score;
          bestMatch = entry;
        }
      });

      return bestMatch;
    };

    const focusGlobalServiceMatch = (entry = null) => {
      const type = `${entry?.type || ''}`.trim();
      const partner = entry?.partner || null;
      if (!type || !partner) return false;

      if (type === 'custom' && partner.categoryKey) {
        selectedCustomCategoryKey = partner.categoryKey;
        renderCategorySidebar(selectedCustomCategoryKey, customServices);
      }

      const sidebarKey = SERVICE_SIDEBAR_KEYS[type] || (type === 'custom' ? 'custom' : 'left');
      sidebarStateController?.setState?.(sidebarKey, true);
      selectedServiceByType[type] = partner;
      filterSidebarForPartner(type, partner);
      renderVisibleSidebars();

      if (hasValidCoords(partner) && map) {
        map.flyTo([partner.lat, partner.lng], 15, { duration: 1.2 });
        showServicePreviewCard(partner);
      }

      const origin = getCurrentOrigin() || partner;
      if (origin) {
        showServicesFromOrigin(origin, { forceType: type });
      }
      return true;
    };

    const setLegendSearchFeedback = (feedbackEl, message = '', tone = 'neutral') => {
      if (!feedbackEl) return;
      feedbackEl.textContent = message;
      feedbackEl.classList.remove('text-slate-400', 'text-cyan-300', 'text-emerald-300', 'text-amber-300', 'text-rose-300');
      if (tone === 'location') {
        feedbackEl.classList.add('text-cyan-300');
      } else if (tone === 'vehicle') {
        feedbackEl.classList.add('text-emerald-300');
      } else if (tone === 'service') {
        feedbackEl.classList.add('text-amber-300');
      } else if (tone === 'error') {
        feedbackEl.classList.add('text-rose-300');
      } else {
        feedbackEl.classList.add('text-slate-400');
      }
    };

    const executeLegendGlobalSearch = async (rawQuery = '', feedbackEl = null) => {
      const query = `${rawQuery || ''}`.trim();
      if (!query) {
        setLegendSearchFeedback(feedbackEl, 'Type a ZIP, VIN, customer, or company.', 'neutral');
        return false;
      }

      const activeType = isAnyServiceSidebarOpen() ? getActivePartnerType() : 'tech';

      if (GLOBAL_SEARCH_ZIP_REGEX.test(query)) {
        const location = await geocodeAddress(query);
        if (!location) {
          setLegendSearchFeedback(feedbackEl, `ZIP ${query} not found.`, 'error');
          return false;
        }
        processLocation(location, location.name || query, activeType);
        setLegendSearchFeedback(feedbackEl, `Located ZIP ${query}.`, 'location');
        return true;
      }

      const vehicleMatch = findVehicleByGlobalSearch(query);
      if (vehicleMatch) {
        if (vehicleMarkersVisible) {
          focusVehicle(vehicleMatch);
        } else {
          const coords = getVehicleMapCoords(vehicleMatch);
          if (map && coords) {
            map.flyTo([coords.lat, coords.lng], 14, { duration: 1.2 });
          }
          applySelection(vehicleMatch.id, null, getVehicleKey(vehicleMatch));
        }
        const vehicleLabel = `${vehicleMatch?.vin || vehicleMatch?.model || 'Vehicle'}`;
        setLegendSearchFeedback(feedbackEl, `Vehicle found: ${vehicleLabel}`, 'vehicle');
        return true;
      }

      let serviceMatch = findServiceByGlobalSearch(query);
      if (!serviceMatch) {
        await Promise.allSettled([loadAllServices(), loadTechnicians()]);
        serviceMatch = findServiceByGlobalSearch(query);
      }
      if (serviceMatch && focusGlobalServiceMatch(serviceMatch)) {
        const serviceName = serviceMatch?.partner?.company || serviceMatch?.partner?.name || 'Service';
        setLegendSearchFeedback(feedbackEl, `Service found: ${serviceName}`, 'service');
        return true;
      }

      const locationMatch = await geocodeAddress(query);
      if (locationMatch) {
        processLocation(locationMatch, locationMatch.name || query, activeType);
        setLegendSearchFeedback(feedbackEl, `Location found: ${locationMatch.name || query}`, 'location');
        return true;
      }

      setLegendSearchFeedback(feedbackEl, 'No matches found for that search.', 'error');
      return false;
    };

    function setupLegendGlobalSearch() {
      const toggle = document.getElementById('legend-search-toggle');
      const panel = document.getElementById('legend-search-panel');
      const chevron = document.getElementById('legend-search-chevron');
      const form = document.getElementById('legend-search-form');
      const input = document.getElementById('legend-search-input');
      const button = document.getElementById('legend-search-btn');
      const status = document.getElementById('legend-search-status');
      const feedback = document.getElementById('legend-search-feedback');
      if (!toggle || !panel || !chevron || !form || !input || !button) return;

      const setExpanded = (expanded) => {
        panel.classList.toggle('hidden', !expanded);
        toggle.setAttribute('aria-expanded', expanded ? 'true' : 'false');
        chevron.textContent = expanded ? '▾' : '▸';
      };

      setExpanded(false);

      toggle.addEventListener('click', () => {
        const expanded = toggle.getAttribute('aria-expanded') === 'true';
        setExpanded(!expanded);
        if (!expanded) {
          input.focus();
        }
      });

      form.addEventListener('submit', async (event) => {
        event.preventDefault();
        const query = input.value.trim();
        if (!query) {
          setLegendSearchFeedback(feedback, 'Type a ZIP, VIN, customer, or company.', 'neutral');
          return;
        }

        const originalButtonLabel = button.textContent;
        status?.classList.remove('hidden');
        button.textContent = '...';
        button.disabled = true;
        const stopLoading = startLoading('Searching map…');

        try {
          await executeLegendGlobalSearch(query, feedback);
        } finally {
          stopLoading();
          status?.classList.add('hidden');
          button.textContent = originalButtonLabel;
          button.disabled = false;
        }
      });
    }

    function setupAddressSearch({ formId, inputId, buttonId, statusId, partnerType }) {
      const form = document.getElementById(formId);
      const input = document.getElementById(inputId);
      const button = document.getElementById(buttonId);
      const status = statusId ? document.getElementById(statusId) : null;
      if (!form || !input || !button) return;

      form.addEventListener('submit', async (e) => {
        e.preventDefault();
        const query = input.value.trim();
        const partners = getPartnerListByType(partnerType);
        if (!query || partners.length === 0) return;

        const originalBtnText = button.innerHTML;
        const stopLoading = startLoading('Searching address…');
        button.innerHTML = svgIcon('loader2', 'animate-spin h-4 w-4');
        button.disabled = true;
        status?.classList.remove('hidden');

        let location = null;
        try {
          location = await debouncedGeocodeAddress(query);
        } finally {
          stopLoading();
          button.innerHTML = originalBtnText;
          button.disabled = false;
          status?.classList.add('hidden');
        }

        if (!location) {
          alert("Address not found. Try a ZIP code or city name.");
          return;
        }

        processLocation(location, location.name || query, partnerType);
      });
    }

    setupAddressSearch({
      formId: 'search-form',
      inputId: 'address-input',
      buttonId: 'search-btn',
      statusId: 'search-status',
      partnerType: 'tech'
    });

    setupAddressSearch({
      formId: 'reseller-search-form',
      inputId: 'reseller-address-input',
      buttonId: 'reseller-search-btn',
      statusId: 'reseller-search-status',
      partnerType: 'reseller'
    });

    setupAddressSearch({
      formId: 'repair-search-form',
      inputId: 'repair-address-input',
      buttonId: 'repair-search-btn',
      statusId: 'repair-search-status',
      partnerType: 'repair'
    });

    function openVehicleModal(vehicle) {
      const modal = document.getElementById('vehicle-modal');
      const title = document.getElementById('vehicle-modal-title');
      const vinDisplay = document.getElementById('vehicle-modal-vin');
      const body = document.getElementById('vehicle-modal-body');
      const columnsToggle = document.getElementById('vehicle-modal-columns-toggle');
      const columnsPanel = document.getElementById('vehicle-modal-columns-panel');
      if (!modal || !title || !body) return;

      const VIN = repairHistoryManager.getRepairVehicleVin(vehicle);
      modal.dataset.vehicleId = vehicle.id;
      columnsToggle?.classList.remove('hidden');
      columnsPanel?.classList.add('hidden');
      title.textContent = `${vehicle.model || 'Vehicle'} ${vehicle.year || ''} • ${vehicle.vin || ''}`;
      if (vinDisplay) {
        vinDisplay.textContent = VIN ? `VIN: ${VIN}` : '';
      }
      const { headers, hidden } = getVehicleModalHeaders(vehicleHeaders);
      const detailRows = headers.map(header => {
        const displayHeader = VEHICLE_HEADER_LABELS[header] || header;
        const editConfig = EDITABLE_VEHICLE_FIELDS[normalizeVehicleHeader(header)];
        const fieldKey = editConfig?.fieldKey;
        const isEditable = Boolean(editConfig);
        const value = editConfig
          ? (vehicle?.[fieldKey] ?? '—')
          : (vehicle.details?.[header] || vehicle[header] || vehicle[header.toLowerCase()] || '—');
        return `
          <tr class="border-b border-slate-800/80 ${hidden.has(header) ? 'hidden' : ''}" draggable="true" data-header="${header}">
            <th class="text-left text-xs font-semibold text-slate-300 py-2 pr-3">
              <span class="inline-flex items-center gap-2">
                <span class="text-slate-600 text-sm">⋮⋮</span>
                ${displayHeader}
              </span>
            </th>
            <td class="text-xs text-slate-100 py-2">
              <div class="flex items-center justify-between gap-2">
                <span data-field-value>${value || '—'}</span>
                ${isEditable ? `
                  <button type="button" class="rounded border border-amber-400/40 px-2 py-1 text-[10px] font-semibold text-amber-200 hover:border-amber-300 hover:text-amber-100 transition-colors" data-edit-field="${fieldKey}" data-edit-header="${header}" data-edit-column="${editConfig.updateColumn}" data-edit-table="${editConfig.table}">Edit</button>
                ` : ''}
              </div>
            </td>
          </tr>
        `;
      }).join('');
      body.innerHTML = `<table class="w-full text-left">${detailRows}</table>`;

      modal.classList.remove('hidden');
      modal.classList.add('flex');
      renderVehicleModalColumnsList(headers, hidden);
      attachVehicleModalRowDrag();
      attachVehicleModalEditors(vehicle);
    }

    async function openRepairModal(vehicle) {
      const canAccessRepairHistory = await hasAuthenticatedAccess();
      if (!canAccessRepairHistory) {
        notifyAuthRequired();
        return;
      }

      const modal = document.getElementById('vehicle-modal');
      const title = document.getElementById('vehicle-modal-title');
      const vinDisplay = document.getElementById('vehicle-modal-vin');
      const body = document.getElementById('vehicle-modal-body');
      const columnsToggle = document.getElementById('vehicle-modal-columns-toggle');
      const columnsPanel = document.getElementById('vehicle-modal-columns-panel');
      if (!modal || !title || !body) return;

      if (modal.repairModalController) {
        modal.repairModalController.abort();
      }
      const repairModalController = new AbortController();
      modal.repairModalController = repairModalController;
      const { signal } = repairModalController;

      const VIN = repairHistoryManager.getRepairVehicleVin(vehicle);
      modal.dataset.vehicleId = vehicle.id;
      title.textContent = 'Repair Management';
      if (vinDisplay) {
        vinDisplay.textContent = VIN ? `VIN: ${VIN}` : '';
      }
      columnsToggle?.classList.add('hidden');
      columnsPanel?.classList.add('hidden');
      body.innerHTML = `
        <div class="space-y-4">
          <div class="sticky top-0 z-10 -mx-4 border-b border-slate-800 bg-slate-950/95 px-4 pb-3 pt-1 backdrop-blur">
            <div class="inline-flex rounded-lg border border-slate-800 bg-slate-950/70 p-1 text-[11px] font-semibold text-slate-300">
              <button type="button" class="repair-tab-btn rounded-md px-3 py-1.5 text-white bg-slate-800/80" data-tab="history">History</button>
              <button type="button" class="repair-tab-btn rounded-md px-3 py-1.5 text-slate-400 hover:text-white" data-tab="new-entry">New Entry</button>
            </div>
          </div>
          <div class="repair-tab-panel" data-tab-panel="history">
            <div class="rounded-lg border border-slate-800 bg-slate-950/70 p-3">
              <div class="flex flex-wrap items-center justify-between gap-3 pb-3">
                <div class="space-y-1">
                  <p class="text-[11px] font-semibold uppercase tracking-[0.2em] text-slate-400">Repair History</p>
                  <p class="text-[10px] text-slate-500" data-repair-connection>Status: checking connection…</p>
                  <p class="text-[10px] text-rose-300 hidden" data-repair-error></p>
                </div>
                <div class="flex flex-wrap items-center gap-2">
                  <input type="text" class="w-48 rounded border border-slate-700 bg-slate-900 px-2 py-1 text-[10px] text-slate-200 placeholder-slate-500" placeholder="Search notes, status, company" data-repair-search />
                  <div class="relative">
                  <button type="button" class="rounded border border-slate-700 bg-slate-900 px-3 py-1 text-[10px] font-semibold uppercase tracking-[0.2em] text-slate-200 hover:border-slate-500" data-repair-columns-toggle>
                    Columns
                  </button>
                  <div class="absolute right-0 z-10 mt-2 hidden w-64 rounded-lg border border-slate-800 bg-slate-950/95 p-3 text-[11px] text-slate-200 shadow-xl" data-repair-columns-panel>
                    <p class="text-[10px] font-semibold uppercase tracking-[0.3em] text-slate-400">Show columns</p>
                    <div class="mt-2 grid gap-2" data-repair-columns-list></div>
                  </div>
                  </div>
                </div>
              </div>
              <table class="w-full text-left text-[11px] text-slate-200">
                <thead class="text-[10px] uppercase text-slate-500" data-repair-history-head></thead>
                <tbody class="divide-y divide-slate-800/80" data-repair-history-body>
                  <tr data-repair-empty>
                    <td class="py-2 pr-3 text-slate-400" colspan="1">Loading history...</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>
          <div class="repair-tab-panel hidden" data-tab-panel="new-entry">
            <form class="space-y-3 rounded-lg border border-slate-800 bg-slate-950/70 p-4 text-[11px] text-slate-200" data-repair-form>
              <div class="grid gap-3 md:grid-cols-2">
                <label class="space-y-1">
                  <span class="text-[10px] uppercase text-slate-500 font-semibold">Status</span>
                  <input type="text" name="status" placeholder="Pending" class="w-full rounded border border-slate-700 bg-slate-900 px-2 py-1.5 text-[11px] text-slate-100" />
                </label>
                <label class="space-y-1">
                  <span class="text-[10px] uppercase text-slate-500 font-semibold">DOC</span>
                  <input type="text" name="doc" placeholder="DOC-12345" class="w-full rounded border border-slate-700 bg-slate-900 px-2 py-1.5 text-[11px] text-slate-100" />
                </label>
              </div>
              <div class="grid gap-3 md:grid-cols-2">
                <label class="space-y-1">
                  <span class="text-[10px] uppercase text-slate-500 font-semibold">Date</span>
                  <input type="date" name="cs_contact_date" class="w-full rounded border border-slate-700 bg-slate-900 px-2 py-1.5 text-[11px] text-slate-100" />
                </label>
                <label class="space-y-1">
                  <span class="text-[10px] uppercase text-slate-500 font-semibold">Shipping Date</span>
                  <input type="date" name="shipping_date" class="w-full rounded border border-slate-700 bg-slate-900 px-2 py-1.5 text-[11px] text-slate-100" />
                </label>
              </div>
              <div class="grid gap-3 md:grid-cols-2">
                <label class="space-y-1">
                  <span class="text-[10px] uppercase text-slate-500 font-semibold">POC Name</span>
                  <input type="text" name="poc_name" placeholder="Primary contact" class="w-full rounded border border-slate-700 bg-slate-900 px-2 py-1.5 text-[11px] text-slate-100" />
                </label>
                <label class="space-y-1">
                  <span class="text-[10px] uppercase text-slate-500 font-semibold">POC Phone</span>
                  <input type="tel" name="poc_phone" placeholder="(555) 555-5555" class="w-full rounded border border-slate-700 bg-slate-900 px-2 py-1.5 text-[11px] text-slate-100" />
                </label>
              </div>
              <div class="grid gap-3 md:grid-cols-2">
                <label class="space-y-1">
                  <span class="text-[10px] uppercase text-slate-500 font-semibold">Customer Availability</span>
                  <input type="text" name="customer_availability" placeholder="Mon-Fri afternoons" class="w-full rounded border border-slate-700 bg-slate-900 px-2 py-1.5 text-[11px] text-slate-100" />
                </label>
                <label class="space-y-1">
                  <span class="text-[10px] uppercase text-slate-500 font-semibold">Installer Request Date</span>
                  <input type="date" name="installer_request_date" class="w-full rounded border border-slate-700 bg-slate-900 px-2 py-1.5 text-[11px] text-slate-100" />
                </label>
              </div>
              <div class="grid gap-3 md:grid-cols-2">
                <label class="space-y-1">
                  <span class="text-[10px] uppercase text-slate-500 font-semibold">Installation Company</span>
                  <input type="text" name="installation_company" placeholder="Techloc Installers" class="w-full rounded border border-slate-700 bg-slate-900 px-2 py-1.5 text-[11px] text-slate-100" />
                </label>
                <label class="space-y-1">
                  <span class="text-[10px] uppercase text-slate-500 font-semibold">Technician Availability</span>
                  <input type="date" name="technician_availability_date" class="w-full rounded border border-slate-700 bg-slate-900 px-2 py-1.5 text-[11px] text-slate-100" />
                </label>
              </div>
              <div class="grid gap-3 md:grid-cols-2">
                <label class="space-y-1">
                  <span class="text-[10px] uppercase text-slate-500 font-semibold">Installation Place</span>
                  <input type="text" name="installation_place" placeholder="123 Main St" class="w-full rounded border border-slate-700 bg-slate-900 px-2 py-1.5 text-[11px] text-slate-100" />
                </label>
              </div>
              <div class="grid gap-3 md:grid-cols-2">
                <label class="space-y-1 block">
                  <span class="text-[10px] uppercase text-slate-500 font-semibold">Cost</span>
                  <input type="text" name="repair_price" placeholder="$0.00" class="w-full rounded border border-slate-700 bg-slate-900 px-2 py-1.5 text-[11px] text-slate-100" />
                </label>
                <label class="space-y-1 block">
                  <span class="text-[10px] uppercase text-slate-500 font-semibold">Notes</span>
                  <input type="text" name="repair_notes" placeholder="Internal notes..." class="w-full rounded border border-slate-700 bg-slate-900 px-2 py-1.5 text-[11px] text-slate-100" />
                </label>
              </div>
              <div class="flex items-center justify-between gap-3">
                <p class="text-[10px] text-slate-400" data-repair-status></p>
                <button type="submit" data-repair-submit class="rounded-lg border border-blue-400/50 bg-blue-500/20 px-4 py-1.5 text-[11px] font-semibold text-blue-100 hover:bg-blue-500/30 transition-colors">Save entry</button>
              </div>
            </form>
          </div>
        </div>
      `;

      const tabButtons = body.querySelectorAll('.repair-tab-btn');
      const tabPanels = body.querySelectorAll('.repair-tab-panel');
      const setActiveTab = (tabKey) => {
        tabButtons.forEach((button) => {
          const isActive = button.dataset.tab === tabKey;
          button.classList.toggle('bg-slate-800/80', isActive);
          button.classList.toggle('text-white', isActive);
          button.classList.toggle('text-slate-400', !isActive);
        });
        tabPanels.forEach((panel) => {
          panel.classList.toggle('hidden', panel.dataset.tabPanel !== tabKey);
        });
      };

      tabButtons.forEach((button) => {
        button.addEventListener('click', () => setActiveTab(button.dataset.tab), { signal });
      });

      setActiveTab('history');

      repairHistoryManager.setupRepairHistoryUI({
        vehicle,
        body,
        signal,
        setActiveTab
      });

      modal.classList.remove('hidden');
      modal.classList.add('flex');
    }

    function openGpsHistoryModal(vehicle, { records = null, error = null } = {}) {
      const modal = document.getElementById('vehicle-modal');
      const title = document.getElementById('vehicle-modal-title');
      const vinDisplay = document.getElementById('vehicle-modal-vin');
      const body = document.getElementById('vehicle-modal-body');
      const columnsToggle = document.getElementById('vehicle-modal-columns-toggle');
      const columnsPanel = document.getElementById('vehicle-modal-columns-panel');
      if (!modal || !title || !body) return;

      if (modal.repairModalController) {
        modal.repairModalController.abort();
      }
      if (modal.gpsModalController) {
        modal.gpsModalController.abort();
      }
      const gpsModalController = new AbortController();
      modal.gpsModalController = gpsModalController;
      const { signal } = gpsModalController;

      const VIN = gpsHistoryManager.getVehicleVin(vehicle);
      modal.dataset.vehicleId = vehicle.id;
      title.textContent = 'GPS History';
      if (vinDisplay) {
        vinDisplay.textContent = VIN ? `VIN: ${VIN}` : '';
      }
      columnsToggle?.classList.add('hidden');
      columnsPanel?.classList.add('hidden');
      body.innerHTML = `
        <div class="space-y-4">
          <div class="rounded-lg border border-slate-800 bg-slate-950/70 p-3">
            <div class="flex flex-wrap items-center justify-between gap-3 pb-3">
              <div>
                <p class="text-[11px] font-semibold uppercase tracking-[0.2em] text-slate-400">GPS History</p>
                <p class="text-[10px] text-slate-500" data-gps-status>Loading GPS history...</p>
                <p class="text-[10px] text-slate-500" data-gps-winner-info></p>
              </div>
              <div class="flex flex-wrap items-center gap-2">
                <button type="button" class="inline-flex items-center gap-1.5 rounded-full border border-slate-700 bg-slate-950/70 px-3 py-1 text-[10px] font-semibold uppercase tracking-[0.2em] text-slate-300" data-gps-connection-status>
                  Checking connection
                </button>
                <input type="text" class="w-52 rounded border border-slate-700 bg-slate-900 px-2 py-1 text-[10px] text-slate-200 placeholder-slate-500" placeholder="Search GPS records" data-gps-search />
                <div class="relative">
                  <button type="button" class="rounded border border-slate-700 bg-slate-900 px-3 py-1 text-[10px] font-semibold uppercase tracking-[0.2em] text-slate-200 hover:border-slate-500" data-gps-columns-toggle>
                    Columns
                  </button>
                  <div class="absolute right-0 z-10 mt-2 hidden w-[420px] max-w-[calc(100vw-2rem)] rounded-lg border border-slate-800 bg-slate-950/95 p-3 text-[11px] text-slate-200 shadow-xl" data-gps-columns-panel>
                    <div class="flex items-start justify-between gap-2">
                      <div>
                        <p class="text-[10px] font-semibold uppercase tracking-[0.3em] text-slate-400">Column controls</p>
                        <p class="text-[10px] text-slate-500">Drag to reorder, toggle visibility, and set width.</p>
                      </div>
                    </div>
                    <div class="mt-3 rounded-md border border-slate-800 bg-slate-950/70 p-2">
                      <p class="text-[10px] uppercase tracking-[0.12em] text-slate-400">Width controls</p>
                      <div class="mt-2 flex items-center gap-2">
                        <select class="min-w-0 flex-1 rounded border border-slate-700 bg-slate-900 px-2 py-1 text-[10px] text-slate-200" data-gps-width-column></select>
                        <input type="number" min="80" max="1200" step="20" placeholder="Auto" class="w-20 rounded border border-slate-700 bg-slate-900 px-2 py-1 text-[10px] text-slate-200" data-gps-width-value />
                        <button type="button" class="rounded border border-slate-700 bg-slate-900 px-2 py-1 text-[10px] font-semibold text-slate-200 hover:border-slate-500" data-gps-width-apply>Set</button>
                        <button type="button" class="rounded border border-slate-700 bg-slate-900 px-2 py-1 text-[10px] font-semibold text-slate-200 hover:border-slate-500" data-gps-width-auto>Auto</button>
                      </div>
                    </div>
                    <div class="mt-3 grid gap-2 max-h-64 overflow-auto" data-gps-columns-list></div>
                  </div>
                </div>
              </div>
            </div>
            <div class="overflow-auto">
              <table class="min-w-full table-fixed text-left text-[11px] text-slate-200">
                <thead class="text-[10px] uppercase text-slate-500" data-gps-history-head></thead>
                <tbody class="divide-y divide-slate-800/80" data-gps-history-body>
                  <tr>
                    <td class="py-2 pr-3 text-slate-400" colspan="1">Loading history...</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>
        </div>
      `;

      const normalizedVin = gpsHistoryManager.getVehicleVin(vehicle)?.trim().toUpperCase();
      if (normalizedVin) {
        vehicle.vin = normalizedVin;
        vehicle.VIN = normalizedVin;
      }
      gpsHistoryManager.setupGpsHistoryUI({
        vehicle,
        body,
        signal,
        records,
        error
      });

      modal.classList.remove('hidden');
      modal.classList.add('flex');
    }

    async function handleGpsHistoryRequest(vehicle) {
      const vin = gpsHistoryManager.getVehicleVin(vehicle);
      const stopLoading = startLoading('Loading GPS history…');
      try {
        const { records, error } = await gpsHistoryManager.fetchGpsHistory({ vin });
        await getGpsDeviceBlacklistSerials().catch(() => new Map());
        if (Array.isArray(records) && records.length) {
          const movingOverrideChanged = applyVehicleMovingOverrideFromGpsHistory(vehicle, records);
          if (movingOverrideChanged) {
            updateVehicleFilterOptions();
            renderVehicles();
            const selectedVehicle = getSelectedVehicleEntry();
            if (selectedVehicle && `${selectedVehicle.id ?? ''}` === `${vehicle?.id ?? ''}`) {
              syncFocusedVehicleNotMovingBadge(vehicle);
            }
          }
        }
        openGpsHistoryModal(vehicle, { records, error });
      } finally {
        stopLoading();
      }
    }

    function attachVehicleModalRowDrag() {
      const modal = document.getElementById('vehicle-modal');
      if (!modal) return;
      const rows = modal.querySelectorAll('tr[data-header]');
      let draggedRow = null;

      rows.forEach((row) => {
        row.addEventListener('dragstart', (event) => {
          draggedRow = row;
          row.classList.add('opacity-50');
          event.dataTransfer.effectAllowed = 'move';
          event.dataTransfer.setData('text/plain', row.dataset.header || '');
        });

        row.addEventListener('dragend', () => {
          row.classList.remove('opacity-50');
          draggedRow = null;
        });

        row.addEventListener('dragover', (event) => {
          event.preventDefault();
          event.dataTransfer.dropEffect = 'move';
        });

        row.addEventListener('drop', (event) => {
          event.preventDefault();
          if (!draggedRow || draggedRow === row) return;
          const tbody = row.parentElement;
          if (!tbody) return;
          const rowList = [...tbody.querySelectorAll('tr[data-header]')];
          const draggedIndex = rowList.indexOf(draggedRow);
          const targetIndex = rowList.indexOf(row);
          if (draggedIndex < targetIndex) {
            tbody.insertBefore(draggedRow, row.nextSibling);
          } else {
            tbody.insertBefore(draggedRow, row);
          }
          const prefs = loadVehicleModalPrefs();
          prefs.order = [...tbody.querySelectorAll('tr[data-header]')].map((entry) => entry.dataset.header);
          saveVehicleModalPrefs(prefs);
        });
      });
    }

    async function attachVehicleModalEditors(vehicle) {
      const modal = document.getElementById('vehicle-modal');
      if (!modal) return;
      const editButtons = modal.querySelectorAll('[data-edit-field]');
      editButtons.forEach((button) => {
        let saveInProgress = false;
        const saveEdit = async () => {
          if (saveInProgress) return;
          saveInProgress = true;
          button.disabled = true;
          button.textContent = 'Verifying...';
          const fieldKey = button.dataset.editField;
          const headerKey = button.dataset.editHeader;
          const updateColumn = button.dataset.editColumn || headerKey;
          const updateTable = button.dataset.editTable || TABLES.vehicles;
          const cell = button.closest('td');
          const valueNode = cell?.querySelector('[data-field-value]');
          if (!cell || !valueNode) {
            saveInProgress = false;
            button.disabled = false;
            button.textContent = button.dataset.editing === 'true' ? 'Save' : 'Edit';
            return;
          }

          const input = cell.querySelector('input[data-edit-input]');
          const newValue = input ? input.value.trim() : '';
          button.textContent = 'Saving...';
          const stopLoading = startLoading('Saving...');

          try {
            if (!supabaseClient || !headerKey || vehicle?.id === undefined) {
              throw new Error('Supabase unavailable');
            }

            const { data: sessionData, error: sessionError } = await supabaseClient.auth.getSession();
            if (sessionError || !sessionData?.session) {
              const { data: refreshed, error: refreshError } = await supabaseClient.auth.refreshSession();
              if (refreshError || !refreshed?.session) {
                throw refreshError || new Error('Supabase session unavailable');
              }
            }

            const updateRequest = supabaseClient
              .from(updateTable)
              .update({ [updateColumn]: newValue })
              .eq('id', vehicle.id);
            const { error } = await runWithTimeout(
              updateRequest,
              8000,
              'Database communication error.'
            );

            if (error) throw error;

            valueNode.textContent = newValue || '—';
            button.dataset.editing = 'false';
            if (input) input.remove();

            if (vehicle?.details) {
              vehicle.details[headerKey] = newValue;
            }
            if (fieldKey && vehicle) {
              vehicle[fieldKey] = newValue;
            }
            renderVehicles();
            return;
          } catch (error) {
            const message = error?.message || '';
            const loweredMessage = message.toLowerCase();
            const isConnectionIssue = loweredMessage.includes('timeout') || loweredMessage.includes('network');
            if (isConnectionIssue) {
              console.warn('Reconnect warning: timeout/network error while updating vehicle field.', error);
              if (input) input.focus();
            } else {
              console.warn('Failed to update vehicle field:', error);
            }
            if (message.includes('communication')) {
              alert('Database communication error.');
            } else {
              alert(message || 'Failed to save vehicle update.');
            }
          } finally {
            stopLoading();
            saveInProgress = false;
            button.disabled = false;
            button.textContent = button.dataset.editing === 'true' ? 'Save' : 'Edit';
          }
        };

        button.onclick = async () => {
          const cell = button.closest('td');
          const valueNode = cell?.querySelector('[data-field-value]');
          if (!cell || !valueNode) return;

          if (button.dataset.editing === 'true') {
            await saveEdit();
            return;
          }

          const currentValue = valueNode.textContent === '—' ? '' : valueNode.textContent;
          const input = document.createElement('input');
          input.type = 'text';
          input.value = currentValue;
          input.dataset.editInput = 'true';
          input.className = 'flex-1 rounded border border-slate-700 bg-slate-900 px-2 py-1 text-[10px] text-slate-100';
          valueNode.textContent = '';
          valueNode.appendChild(input);
          button.dataset.editing = 'true';
          button.textContent = 'Save';
          input.focus();
          input.addEventListener('keydown', (event) => {
            if (event.key === 'Enter') {
              event.preventDefault();
              saveEdit();
            }
          });
        };
      });
    }

    function closeVehicleModal() {
      const modal = document.getElementById('vehicle-modal');
      if (!modal) return;
      if (modal.repairModalController) {
        modal.repairModalController.abort();
        modal.repairModalController = null;
      }
      modal.classList.add('hidden');
      modal.classList.remove('flex');
      const panel = document.getElementById('vehicle-modal-columns-panel');
      panel?.classList.add('hidden');
    }

    function attachPopupHandlers() {
      const btn = document.querySelector('.vehicle-popup button[data-view-more-popup]');
      if (btn) {
        btn.addEventListener('click', () => {
          const vehicleId = selectedVehicleId;
          const vehicle = vehicles.find(v => v.id === vehicleId);
          if (vehicle) openVehicleModal(vehicle);
        });
      }
    }

    function getPartnerListByType(type) {
      if (type === 'reseller') return resellers;
      if (type === 'repair') return repairShops;
      if (type === 'custom') {
        return selectedCustomCategoryKey
          ? customServices.filter((service) => service?.categoryKey === selectedCustomCategoryKey)
          : customServices;
      }
      return technicians;
    }

    function getActivePartnerType() {
      const visibleTypes = getVisibleServiceTypes();
      if (visibleTypes.length) return visibleTypes[0];

      const sidebarType = SIDEBAR_TYPE_BY_KEY[activeLeftPanel];
      if (sidebarType && isServiceTypeEnabled(sidebarType)) return sidebarType;

      const [firstEnabled] = getEnabledServiceTypes();
      return firstEnabled || null;
    }

    function getVisibleServiceTypes() {
      return SERVICE_TYPES.filter((type) => isServiceSidebarVisible(type));
    }

    function getPartnerLabel(type) {
      if (type === 'reseller') return 'reseller';
      if (type === 'repair') return 'repair shop';
      return 'technician';
    }

    function getNearestFromList(point, list, type) {
      if (!point || !list.length) return null;
      let nearest = null;
      let bestDist = Infinity;
      list.forEach(item => {
        const dist = getDistance(point.lat, point.lng, item.lat, item.lng);
        if (dist < bestDist) {
          bestDist = dist;
          nearest = item;
        }
      });
      return nearest ? { entry: nearest, distance: bestDist, type } : null;
    }

    function getNearestPartner(vehicle) {
      const type = getActivePartnerType();
      const list = getPartnerListByType(type);
      return getNearestFromList(vehicle, list, type);
    }

    function addLabeledConnection(layer, start, end, distance, color, options = {}) {
      if (!layer || !start || !end) return null;
      const { dashArray = '6 4', weight = 3, opacity = 0.85 } = options;

      L.polyline(
        [
          [start.lat, start.lng],
          [end.lat, end.lng]
        ],
        { color, weight, opacity, dashArray }
      ).addTo(layer);

      const midLat = (start.lat + end.lat) / 2;
      const midLng = (start.lng + end.lng) / 2;
      const label = L.marker([midLat, midLng], {
        icon: L.divIcon({
          className: 'distance-label-wrapper',
          html: `<div class="distance-label" style="--line-color:${color}">${distance.toFixed(1)} mi</div>`,
          iconSize: [0, 0],
          iconAnchor: [0, 0]
        }),
        interactive: false
      });

      label.addTo(layer);
      return label;
    }

    function showServicesFromOrigin(origin, { forceType = null } = {}) {
      serviceLayer?.clearLayers();
      serviceConnectionLayer?.clearLayers();
      const hasSidebarOpen = isAnyServiceSidebarOpen();
      const hasExplicitType = !!forceType;
      if (!origin || (!hasSidebarOpen && !hasExplicitType)) return;

      lastOriginPoint = { lat: origin.lat, lng: origin.lng, name: origin.name || origin.customer || '' };

      const visibleTypes = getVisibleServiceTypes();
      const activeTypes = forceType
        ? [forceType]
        : visibleTypes;

      activeTypes.forEach((type) => {
        const baseList = getPartnerListByType(type);
        if (!baseList.length) return;
        const filtered = applyServiceFilter(baseList, type);
        const selectedPartner = getSelectedService(type);
        const chosen = selectedPartner || getNearestFromList(origin, filtered, type)?.entry;
        if (!chosen) return;

        const unauthorized = isUnauthorizedPartner(chosen);
        let color = SERVICE_COLORS[type] || '#38bdf8';

        if (type === 'custom' || type.startsWith('custom-')) {
          const categoryKey = chosen.categoryKey || selectedCustomCategoryKey;
          const meta = customCategories.get(categoryKey);
          if (meta) color = meta.color;
        } else {
          color = getPartnerColor(chosen, color);
        }
        const marker = L.marker([chosen.lat, chosen.lng], {
          icon: createServiceIcon(color, { unauthorized })
        }).addTo(serviceLayer);

        marker.on('click', (e) => {
          e.originalEvent.handledByMarker = true;
          sidebarStateController?.setState?.(SERVICE_SIDEBAR_KEYS[type] || 'left', true);
          selectedServiceByType[type] = chosen;
          filterSidebarForPartner(type, chosen);
          showServicePreviewCard(chosen);
          map.flyTo([chosen.lat, chosen.lng], 15, { duration: 1.2 });
        });

        const distance = getDistance(origin.lat, origin.lng, chosen.lat, chosen.lng);
        addLabeledConnection(
          serviceConnectionLayer,
          origin,
          chosen,
          Math.max(distance, 0),
          color,
          { dashArray: '6 4', weight: 3, opacity: 0.85 }
        );
      });
    }

    function drawConnection(origin, partnerInfo) {
      const type = partnerInfo?.type || getActivePartnerType();
      const target = partnerInfo?.entry || partnerInfo;
      const isVisible = isServiceSidebarVisible(type);
      if (!target || !origin || !isVisible) return;
      const baseColor = SERVICE_COLORS[type] || '#818cf8';
      const color = getPartnerColor(target, baseColor);
      connectionLayer.clearLayers();
      const miles = getDistance(origin.lat, origin.lng, target.lat, target.lng);
      addLabeledConnection(
        connectionLayer,
        origin,
        target,
        miles,
        color,
        { dashArray: '6 6', weight: 3, opacity: 0.8 }
      );
    }

    function setupResizableSidebars() {
      const layout = document.getElementById('map-layout');
      const leftSidebar = document.getElementById('left-sidebar');
      const rightSidebar = document.getElementById('right-sidebar');
      if (!layout || !leftSidebar || !rightSidebar) return;

      const rootStyle = document.documentElement.style;
      const minWidth = 260;
      const maxWidth = 720;
      let activeDrag = null;
      let resizePending = false;

      const clampWidth = (value) => Math.min(Math.max(value, minWidth), maxWidth);

      const applyWidth = (side, width) => {
        rootStyle.setProperty(side === 'left' ? '--left-sidebar-width' : '--right-sidebar-width', `${width}px`);
        if (map && !resizePending) {
          resizePending = true;
          requestAnimationFrame(() => {
            map.invalidateSize();
            resizePending = false;
          });
        }
      };

      const handleMove = (clientX) => {
        if (!activeDrag) return;
        const bounds = layout.getBoundingClientRect();

        if (activeDrag === 'left') {
          const newWidth = clampWidth(clientX - bounds.left);
          applyWidth('left', newWidth);
        }

        if (activeDrag === 'right') {
          const newWidth = clampWidth(bounds.right - clientX);
          applyWidth('right', newWidth);
        }
      };

      const onMouseMove = (event) => handleMove(event.clientX);
      const onTouchMove = (event) => {
        const touch = event.touches?.[0];
        if (!touch) return;
        handleMove(touch.clientX);
      };

      const stopDrag = () => {
        activeDrag = null;
        document.removeEventListener('mousemove', onMouseMove);
        document.removeEventListener('mouseup', stopDrag);
        document.removeEventListener('touchmove', onTouchMove);
        document.removeEventListener('touchend', stopDrag);
      };

      const startDrag = (side, event) => {
        event.preventDefault();
        activeDrag = side;
        document.addEventListener('mousemove', onMouseMove);
        document.addEventListener('mouseup', stopDrag);
        document.addEventListener('touchmove', onTouchMove, { passive: false });
        document.addEventListener('touchend', stopDrag);
      };

      const attachHandle = (element, side) => {
        const handle = element.querySelector('.resize-handle');
        if (!handle) return;

        handle.addEventListener('mousedown', (e) => startDrag(side, e));
        handle.addEventListener('touchstart', (e) => startDrag(side, e), { passive: false });
      };

      attachHandle(leftSidebar, 'left');
      attachHandle(rightSidebar, 'right');
    }

    const defaultSidebarVisible = false;

    function setupSidebarToggles() {
      const leftSidebar = document.getElementById('left-sidebar');
      const rightSidebar = document.getElementById('right-sidebar');
      const floatingSearchShell = document.getElementById('floating-search-shell');
      const resellerSidebar = document.getElementById('reseller-sidebar');
      const repairSidebar = document.getElementById('repair-sidebar');
      const customSidebar = document.getElementById('dynamic-service-sidebar');
      const openLeft = document.getElementById('open-left-sidebar');
      const openRight = document.getElementById('open-right-sidebar');
      const openReseller = document.getElementById('open-reseller-sidebar');
      const openRepair = document.getElementById('open-repair-sidebar');
      const openCustom = customToggleRef || document.querySelector('#custom-category-toggles .sidebar-toggle-btn');
      const collapseLeft = document.getElementById('collapse-left');
      const collapseRight = document.getElementById('collapse-right');
      const collapseReseller = document.getElementById('collapse-reseller');
      const collapseRepair = document.getElementById('collapse-repair');
      const collapseCustom = document.getElementById('collapse-dynamic');

      const defaultLeftOffset = 12;
      const defaultRightOffset = 12;
      const syncSidebarVisibility = () => {
        const previousCustomVisible = customSidebarVisible;
        techSidebarVisible = isServiceSidebarVisible('tech');
        resellerSidebarVisible = isServiceSidebarVisible('reseller');
        repairSidebarVisible = isServiceSidebarVisible('repair');
        customSidebarVisible = isServiceSidebarVisible('custom');

        if (!techSidebarVisible) {
          if (map?.hasLayer(techLayer)) map.removeLayer(techLayer);
          connectionLayer?.clearLayers();
        } else if (techLayer && map && !map.hasLayer(techLayer)) {
          techLayer.addTo(map);
        }

        if (!resellerSidebarVisible) {
          resellerLayer?.clearLayers();
        } else if (resellerLayer && map && !map.hasLayer(resellerLayer)) {
          resellerLayer.addTo(map);
        }

        if (!repairSidebarVisible) {
          repairLayer?.clearLayers();
        } else if (repairLayer && map && !map.hasLayer(repairLayer)) {
          repairLayer.addTo(map);
        }

        if (!customSidebarVisible) {
          customServiceLayer?.clearLayers?.();
          customCategories.forEach(({ layer }) => layer?.clearLayers?.());
        } else if (customServiceLayer && map && !map.hasLayer(customServiceLayer)) {
          customServiceLayer.addTo(map);
          customCategories.forEach(({ layer }) => layer && !map.hasLayer(layer) && layer.addTo(map));
        }

        if (customSidebarVisible !== previousCustomVisible) {
          lastCustomSidebarVisible = customSidebarVisible;
          if (customSidebarVisible) renderCategorySidebar(selectedCustomCategoryKey, customServices);
        }

          activeLeftPanel = leftSideKeys.find(key => {
            const cfg = configs[key];
            return cfg?.sidebar && !cfg.sidebar.classList.contains(cfg.collapsedClass);
        }) || null;

        renderVisibleSidebars();

        if (!isAnyServiceSidebarOpen()) {
          serviceLayer?.clearLayers();
          serviceConnectionLayer?.clearLayers();
        } else {
          const origin = getCurrentOrigin();
          if (origin) {
            showServicesFromOrigin(origin, { forceType: getActivePartnerType() });
          }
        }
      };

        const getSidebarWidth = (sidebar) => sidebar?.getBoundingClientRect().width || 0;

        const rawConfigs = {
          left: { sidebar: leftSidebar, toggle: openLeft, collapse: collapseLeft, collapsedClass: 'collapsed-left', group: 'left' },
          reseller: { sidebar: resellerSidebar, toggle: openReseller, collapse: collapseReseller, collapsedClass: 'collapsed-reseller', group: 'left', type: 'reseller' },
          repair: { sidebar: repairSidebar, toggle: openRepair, collapse: collapseRepair, collapsedClass: 'collapsed-repair', group: 'left', type: 'repair' },
          custom: { sidebar: customSidebar, toggle: openCustom, collapse: collapseCustom, collapsedClass: 'collapsed-dynamic', group: 'left', type: 'custom' },
          right: { sidebar: rightSidebar, toggle: openRight, collapse: collapseRight, collapsedClass: 'collapsed-right', group: 'right' }
        };

        const configs = Object.fromEntries(
          Object.entries(rawConfigs).filter(([, cfg]) => {
            if (!cfg.sidebar || !cfg.toggle) return false;
            if (!cfg.type) return true;
            return isServiceTypeEnabled(cfg.type);
          })
        );

        const syncMap = () => { if (map) setTimeout(() => map.invalidateSize(), 320); };

        const leftSideKeys = Object.keys(configs).filter((key) => configs[key]?.group === 'left');

        const updateTogglePositions = () => {
        const expandedEntry = leftSideKeys
          .map((key) => configs[key])
          .find(cfg => cfg?.sidebar && !cfg.sidebar.classList.contains(cfg.collapsedClass));

        const anchorLeft = expandedEntry
          ? getSidebarWidth(expandedEntry.sidebar) + defaultLeftOffset
          : defaultLeftOffset;

        leftSideKeys.forEach(key => {
          const toggle = configs[key]?.toggle;
          if (toggle) toggle.style.left = `${anchorLeft}px`;
        });

        const customSlot = document.getElementById('custom-toggle-slot');
        if (customSlot) customSlot.style.left = `${anchorLeft}px`;
        document.querySelectorAll('#custom-category-toggles .sidebar-toggle-btn').forEach((btn) => {
          btn.style.left = `${anchorLeft}px`;
        });

        const rightSidebarIsCollapsed = configs.right?.sidebar?.classList.contains(configs.right.collapsedClass);
        const anchorRight = rightSidebarIsCollapsed
          ? defaultRightOffset
          : getSidebarWidth(configs.right.sidebar) + defaultRightOffset;

        if (floatingSearchShell) {
          floatingSearchShell.classList.toggle('hidden', !rightSidebarIsCollapsed);
        }

        const rightToggleGroup = document.getElementById('right-toggle-group');
        if (rightToggleGroup) {
          rightToggleGroup.style.right = `${anchorRight}px`;
        }
      };

      const applyState = (side, expanded) => {
        const config = configs[side];
        if (!config?.sidebar || !config?.toggle) return false;
        const wasCollapsed = config.sidebar.classList.contains(config.collapsedClass);
        const shouldCollapse = !expanded;
        if (shouldCollapse && !wasCollapsed) {
          config.sidebar.classList.add(config.collapsedClass);
          config.toggle.classList.remove('active');
          config.toggle.setAttribute('aria-pressed', 'false');
        } else if (!shouldCollapse && wasCollapsed) {
          config.sidebar.classList.remove(config.collapsedClass);
          config.toggle.classList.add('active');
          config.toggle.setAttribute('aria-pressed', 'true');
        }
        return wasCollapsed !== shouldCollapse;
      };

      const setState = (side, expanded) => {
        const changed = applyState(side, expanded);
        if (!changed) return;

        if (expanded && configs[side]?.group === 'left') {
          leftSideKeys.forEach(key => {
            if (key !== side) applyState(key, false);
          });

          const origin = getCurrentOrigin();
          if (origin) {
            const newType = configs[side].type || SIDEBAR_TYPE_BY_KEY[side];
            setTimeout(() => {
              showServicesFromOrigin(origin, { forceType: newType });
            }, 50);
          }
        }

        updateTogglePositions();
        syncSidebarVisibility();
        syncMap();
      };

      sidebarStateController = { setState, updateTogglePositions };

      Object.entries(configs).forEach(([side, config]) => {
        if (config.toggle) config.toggle.addEventListener('click', () => setState(side, true));
        if (config.collapse) config.collapse.addEventListener('click', () => setState(side, false));
        setState(side, defaultSidebarVisible);
      });

      updateTogglePositions();
      window.addEventListener('resize', updateTogglePositions);
    }

    document.addEventListener('DOMContentLoaded', () => {
      bindNavigationStorageListener();
      subscribeSelectedVehicle((selection) => {
        if (!vehicles.length) return;
        syncVehicleSelectionFromStore(selection);
      });
      subscribeServiceFilterIds((filters) => {
        Object.assign(serviceFilterIds, filters);
        renderVisibleSidebars();
        const origin = getCurrentOrigin();
        if (origin) {
          showServicesFromOrigin(origin, { forceType: isAnyServiceSidebarOpen() ? getActivePartnerType() : null });
        }
      });
      (async () => {
        try {
          setupBackgroundManager();
          await loadStateCenters();
          initMap();
          await syncAvailableServiceTypes();
          updateServiceVisibilityUI();
          await loadVehicleFilterPrefs();
          await loadVehicles();

          const loadNonCriticalData = async () => {
            const backgroundTasks = [
              loadHotspots(),
              loadBlacklistSites(),
              loadAllServices(),
            ];
            if (isServiceTypeEnabled('tech')) {
              backgroundTasks.push(loadTechnicians());
            }
            await Promise.allSettled(backgroundTasks);
          };

          const scheduleNonCriticalDataLoad = () => {
            const run = () => {
              void loadNonCriticalData();
            };
            if (typeof window.requestIdleCallback === 'function') {
              window.requestIdleCallback(() => run(), { timeout: 1200 });
              return;
            }
            setTimeout(run, 0);
          };

          scheduleNonCriticalDataLoad();

          setupResizableSidebars();
          setupSidebarToggles();
          setupLayerToggles();
          setupLegendGlobalSearch();
          bindVehicleFilterHandlers();
          syncVehicleFilterInputs();
          const filterConfigs = [
            { type: 'tech' },
            { type: 'reseller' },
            { type: 'repair' },
            { type: 'custom' },
          ].filter(({ type }) => isServiceTypeEnabled(type));

          const refreshServiceSearch = () => {
            renderVisibleSidebars();
            const origin = getCurrentOrigin();
            if (origin) {
              showServicesFromOrigin(origin, { forceType: isAnyServiceSidebarOpen() ? getActivePartnerType() : null });
            }
          };

          filterConfigs.forEach(({ type }) => {
            const searchInput = document.getElementById(getServiceSearchInputId(type));
            if (searchInput) {
              searchInput.addEventListener('input', () => {
                serviceSearchFilters[type] = searchInput.value;
                refreshServiceSearch();
              });
            }
          });

          syncServiceSearchInputs();

          setupEventDelegation();

          let vehicleSearchTimer;
          document.getElementById('vehicle-search').addEventListener('input', () => {
            clearTimeout(vehicleSearchTimer);
            vehicleSearchTimer = setTimeout(() => renderVehicles(), 250);
          });

          document.getElementById('clear-selection')?.addEventListener('click', () => resetSelection());
          document.getElementById('vehicle-modal-close')?.addEventListener('click', closeVehicleModal);
          document.getElementById('vehicle-modal-columns-toggle')?.addEventListener('click', () => {
            const panel = document.getElementById('vehicle-modal-columns-panel');
            if (!panel) return;
            panel.classList.toggle('hidden');
          });
          document.getElementById('vehicle-modal')?.addEventListener('click', (e) => {
            if (e.target.id === 'vehicle-modal') closeVehicleModal();
          });

          const refreshVehicles = debounceAsync(async () => {
            await loadVehicles({ force: true });
          }, 600);
          const refreshHotspots = debounceAsync(async () => {
            await loadHotspots();
          }, 600);
          const refreshBlacklist = debounceAsync(async () => {
            await loadBlacklistSites();
          }, 600);
          const refreshServices = debounceAsync(async () => {
            await loadAllServices();
          }, 800);

          createControlMapApiService({
            supabaseClient,
            tables: TABLES,
            handlers: {
              vehicles: refreshVehicles,
              deals: refreshVehicles,
              invoices: refreshVehicles,
              hotspots: refreshHotspots,
              blacklist: refreshBlacklist,
              services: refreshServices
            }
          });

          startSupabaseKeepAlive({ supabaseClient, table: TABLES.vehicles });

          window.addEventListener('auth:role-ready', () => renderVehicles());
        } finally {
          document.dispatchEvent(new CustomEvent('control-map:boot-complete'));
        }
      })();
    });
