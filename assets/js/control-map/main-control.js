import '../../scripts/authManager.js';
import { setupBackgroundManager } from '../../scripts/backgroundManager.js';
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
      getServiceFilters,
      setSelectedVehicle,
      setServiceFilter,
      setServiceFilters,
      subscribeSelectedVehicle,
      subscribeServiceFilters,
      subscribeServiceFilterIds,
    } from '../shared/navigationStore.js';
    
    // --- Base Config ---

    let map, techLayer, targetLayer, connectionLayer, serviceLayer, serviceConnectionLayer, vehicleLayer, highlightLayer, resellerLayer, repairLayer, customServiceLayer, hotspotLayer, blacklistLayer;
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
    const serviceHeadersByCategory = {};
    let selectedVehicleId = null;
    const checkedVehicleIds = new Set();
    const checkedVehicleClickTimes = new Map();
    const checkedVehicleClickTimesByVin = new Map();
    const checkedVehicleStateByVin = new Map();
    const VEHICLE_CLICK_HISTORY_TABLE = 'control_map_vehicle_clicks';
    const VEHICLE_FILTERS_STORAGE_KEY = 'controlMapVehicleFilters';
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
    const serviceFilters = { ...getServiceFilters() };
    const serviceFilterIds = { ...getServiceFilterIds() };
    const vehicleFilters = {
      invPrep: [],
      gpsFix: [],
      moving: [],
      dealStatus: [],
      ptStatus: [],
      dealMin: 0,
      dealMax: 100,
    };

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

      return {
        invPrep: toArray(payload.invPrep),
        gpsFix: toArray(payload.gpsFix),
        moving: toArray(payload.moving),
        dealStatus: toArray(payload.dealStatus),
        ptStatus: toArray(payload.ptStatus),
        dealMin: Math.min(min, max),
        dealMax: max,
      };
    };

    const applyVehicleFilterPayload = (payload = {}) => {
      const normalized = normalizeVehicleFilterPayload(payload);
      Object.assign(vehicleFilters, normalized);
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

    const selectedServiceByType = {};
    const distanceCaches = {
      tech: { originKey: null, distances: new Map() },
      partners: { originKey: null, distances: new Map() }
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
          'Error de comunicación con la base de datos.'
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

    const normalizeSelectionValue = (value) => `${value ?? ''}`.trim().toLowerCase();
    const matchesVehicleSelection = (vehicle, selection) => {
      if (!vehicle || !selection) return false;
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
        if (selectedVehicleId === null) return;
        syncingVehicleSelection = true;
        applySelection(null, selectedTechId);
        syncingVehicleSelection = false;
        return;
      }

      const match = vehicles.find((vehicle) => matchesVehicleSelection(vehicle, selection));
      if (!match || selectedVehicleId === match.id) return;

      syncingVehicleSelection = true;
      applySelection(match.id, null);
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

    const EDITABLE_VEHICLE_FIELDS = {
      'gps fix': { fieldKey: 'gpsFix', updateColumn: 'gps to fix', table: TABLES.vehiclesUpdates },
      'gps fix reason': { fieldKey: 'gpsReason', updateColumn: 'gps fix reason', table: TABLES.vehiclesUpdates }
    };

    const repairHistoryManager = createRepairHistoryManager({
      supabaseClient,
      startLoading,
      ensureSupabaseSession,
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

    const hasValidCoords = (entry) => Number.isFinite(entry?.lat) && Number.isFinite(entry?.lng) && entry.lat !== 0 && entry.lng !== 0;

    const getLocationNote = (accuracy = '') => {
      if (accuracy === 'zip') return 'Approximate location (based on ZIP code)';
      if (accuracy === 'city') return 'Approximate location (based on city)';
      if (accuracy === 'state') return 'Approximate location (state center)';
      return '';
    };

    const getAccuracyDot = (accuracy = '') => {
      return accuracy === 'exact' ? 'bg-emerald-300' : 'bg-amber-300';
    };

    const ICON_PATHS = {
      mapPin: '<path d="M21 10c0 7-9 13-9 13S3 17 3 10a9 9 0 1 1 18 0Z"></path><circle cx="12" cy="10" r="3"></circle>',
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

    const VEHICLE_RENDER_LIMIT = 35;
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
      { type: 'reseller', eyebrow: 'Reseller network', title: 'Nearby resellers', accentClass: 'text-emerald-300', filterPlaceholder: 'Filter resellers by name, city, or state', emptyText: 'Resellers will appear here.' },
      { type: 'repair', eyebrow: 'Repair network', title: 'Repair shops nearby', accentClass: 'text-orange-200', filterPlaceholder: 'Filter repair shops by name, city, or state', emptyText: 'Repair shops will appear here.' },
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
              id="${type}-filter"
              type="text"
              class="mt-2 block w-full rounded-xl border border-slate-700 bg-slate-800 px-3 py-2 text-sm text-white placeholder-slate-500 focus:border-blue-500 focus:ring-1 focus:ring-blue-500 outline-none transition-all"
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
              placeholder="Filter services by name, city, or state"
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

    const hasSpAgentNote = (partner = {}) => `${partner.notes || ''}`.trim().toLowerCase() === 'sp agent';
    const hasSpStorageNote = (partner = {}) => `${partner.notes || ''}`.trim().toLowerCase() === 'sp storage';

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
      const zipText = partner.zip || 'N/A';
      const noteText = partner.notes || partner.note || partner.details?.note || '';
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
            <p class="service-mini-meta"><span class="service-mini-label">ZIP</span><span>${escapeHTML(zipText)}</span></p>
            ${noteText ? `<p class="service-mini-note"><span class="service-mini-label">Note</span> ${escapeHTML(noteText)}</p>` : ''}
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
      }).setView([39.8, -98.5], 5);
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
        if (e.originalEvent?.handledByMarker) return;

        const lat = parseFloat(e.latlng.lat.toFixed(6));
        const lng = parseFloat(e.latlng.lng.toFixed(6));

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

    async function loadVehicles() {
      if (vehicles.length) return;
      const stopLoading = startLoading('Loading Vehicles…');
      try {
        const data = await vehicleService.listVehicles();
        let updateRows = [];
        if (supabaseClient) {
          try {
            const { data: updatesData, error: updatesError } = await runWithTimeout(
              supabaseClient
                .from(TABLES.vehiclesUpdates)
                .select('VIN,"gps to fix","gps fix reason"'),
              8000,
              'Error de comunicación con la base de datos.'
            );
            if (updatesError) throw updatesError;
            updateRows = updatesData || [];
          } catch (error) {
            console.warn('Vehicle updates load warning: ' + (error?.message || error));
          }
        }

        const updatesByVin = new Map();
        updateRows.forEach((row) => {
          const vin = String(getField(row, 'VIN', 'vin') || '').trim();
          if (!vin) return;
          updatesByVin.set(vin.toLowerCase(), row);
        });

        const normalizedVehicles = data.map((row, idx) => {
          const normalized = normalizeVehicle(row, idx, { getField, toStateCode, resolveCoords });
          const vinValue = String(getField(row, 'VIN', 'vin', 'ShortVIN') || normalized.vin || '').trim();
          const update = updatesByVin.get(vinValue.toLowerCase());
          const gpsFix = update ? getField(update, 'gps to fix', 'gps_to_fix') : '';
          const gpsReason = update ? getField(update, 'gps fix reason', 'gps_fix_reason') : '';
          normalized.gpsFix = gpsFix;
          normalized.gpsReason = gpsReason;
          if (normalized.details) {
            normalized.details['GPS Fix'] = gpsFix;
            normalized.details['GPS Fix Reason'] = gpsReason;
          }
          return normalized;
        });

        let dealsByStockNo = new Map();
        if (supabaseClient) {
          try {
            const stockNumbers = normalizedVehicles
              .map((vehicle) => normalizeStockNumber(vehicle.stockNo))
              .filter(Boolean);
            dealsByStockNo = await fetchDealsByStockNumbers(stockNumbers);
          } catch (error) {
            console.warn('DealsJP1 load warning: ' + (error?.message || error));
          }
        }

        const allowedDealStatuses = new Set(['ACTIVE', 'STOCK']);
        const filteredVehicles = normalizedVehicles.filter((vehicle) => {
          const stockNo = normalizeStockNumber(vehicle.stockNo);
          const dealValues = dealsByStockNo.get(stockNo) ?? null;
          const normalizedStatus = String(dealValues?.vehicleStatus ?? '').trim().toUpperCase();
          if (normalizedStatus && !allowedDealStatuses.has(normalizedStatus)) return false;
          if (normalizedStatus) {
            vehicle.status = normalizedStatus;
          }
          const regularAmount = dealValues?.regularAmount ?? null;
          const openBalance = dealValues?.openBalance ?? null;
          const payKpi = openBalance !== null && regularAmount ? openBalance / regularAmount : null;
          vehicle.payKpi = payKpi;
          vehicle.payKpiDisplay = formatPayKpi(payKpi);
          return true;
        });

        vehicles = filteredVehicles;
        await hydrateVehicleClickHistory(vehicles);
        vehicleHeaders = data.length ? Object.keys(data[0]) : [];
        if (!vehicleHeaders.some((header) => header.toLowerCase() === 'gps fix')) {
          vehicleHeaders.push('GPS Fix');
        }
        if (!vehicleHeaders.some((header) => header.toLowerCase() === 'gps fix reason')) {
          vehicleHeaders.push('GPS Fix Reason');
        }
        updateVehicleFilterOptions();
        syncVehicleFilterInputs();
        renderVehicles();
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

      const origin = clientLocation || getCurrentOrigin();
      const techWithDistances = attachDistances(
        techList,
        origin,
        distanceCaches.tech,
        (tech) => getDistance(origin.lat, origin.lng, tech.lat, tech.lng)
      );
      const sortedList = origin
        ? techWithDistances.sort((a, b) => a.distance - b.distance)
        : [...techWithDistances];

      const selectedTech = selectedTechId !== null
        ? sortedList.find(t => t.id === selectedTechId) || techList.find(t => t.id === selectedTechId)
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
              <p class="flex items-center gap-1 text-[11px] text-slate-400 leading-tight">${svgIcon('mapPin', 'h-3 w-3')}<span class="truncate">${tech.city || 'Unknown'}, ${tech.state || 'US'}</span></p>
            </div>
            <div class="text-right text-[11px] text-slate-400 leading-tight space-y-0.5">
              <p class="font-semibold text-slate-200">${tech.phone || ''}</p>
              ${tech.email ? `<p class="flex items-center gap-1 justify-end text-slate-400">${svgIcon('mail', 'h-3 w-3')}<span class="truncate max-w-[160px]">${tech.email}</span></p>` : ''}
              ${distanceLabel ? `<p class="flex items-center gap-1 justify-end text-slate-300">${svgIcon('navigation', 'h-3 w-3')}<span class="font-semibold text-slate-100">${distanceLabel}</span></p>` : ''}
            </div>
          </div>
          ${tech.zip ? `<p class="text-[10px] text-slate-500 mt-1">${tech.zip}</p>` : ''}

        `;

        listContainer.appendChild(card);
      });

    }

    const isAnyServiceSidebarOpen = () => getVisibleServiceTypes().length > 0;

    const getPartnerFilterValue = (type = 'tech') => `${serviceFilters[type] || ''}`.trim().toLowerCase();

    const applyServiceFilter = (list = [], type = 'tech') => {
      const query = getPartnerFilterValue(type);
      const allowedIds = Array.isArray(serviceFilterIds[type]) ? new Set(serviceFilterIds[type].map((id) => `${id}`)) : null;
      const baseList = allowedIds ? list.filter((partner) => allowedIds.has(`${partner.id}`)) : list;
      if (!query) return baseList;
      return baseList.filter((partner) => {
        return [partner.company, partner.contact, partner.city, partner.state, partner.zip, partner.region]
          .some(value => `${value || ''}`.toLowerCase().includes(query));
      });
    };

    function filterSidebarForPartner(type, partner) {
      if (!type || !partner) return;
      const filterValue = partner.company || partner.name || partner.zip || '';
      serviceFilters[type] = filterValue;
      setServiceFilter(type, filterValue);
      const inputId = type === 'custom' ? 'dynamic-service-filter' : `${type}-filter`;
      const input = document.getElementById(inputId);
      if (input) {
        input.value = filterValue;
      }
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
        const vehicle = vehicles.find(v => v.id === selectedVehicleId && hasValidCoords(v));
        if (vehicle) return vehicle;
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

    const syncServiceFilterInputs = () => {
      const inputConfigs = [
        { id: 'tech-filter', type: 'tech' },
        { id: 'reseller-filter', type: 'reseller' },
        { id: 'repair-filter', type: 'repair' },
        { id: 'dynamic-service-filter', type: 'custom' },
      ];

      inputConfigs.forEach(({ id, type }) => {
        const input = document.getElementById(id);
        if (!input) return;
        const value = serviceFilters[type] || '';
        if (input.value !== value) {
          input.value = value;
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
          const card = document.createElement('div');
          const isSelected = selectedService?.id === partner.id;
          card.className = `p-3 rounded-lg border transition-colors cursor-pointer ${isSelected || (idx === 0 && origin) ? 'bg-slate-800 border-amber-400 ring-1 ring-amber-400' : 'bg-slate-900 border-slate-800 hover:border-slate-700'}`;
          card.dataset.id = partner.id;
          card.dataset.type = 'partner';
          card.dataset.partnerType = type;
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
              ${partner.notes ? `<div class="col-span-2 text-[10px] text-slate-400 leading-tight">${partner.notes}</div>` : ''}
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
      const filterInput = document.getElementById('dynamic-service-filter');
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
      const filterValue = `${serviceFilters.custom || filterInput?.value || ''}`.toLowerCase();
      const origin = getCurrentOrigin();
      const withDistances = attachDistances(constrainedList, origin, distanceCaches.partners, (partner) => {
        if (!origin) return 0;
        return getDistance(origin.lat, origin.lng, partner.lat, partner.lng);
      });
      const sorted = origin ? [...withDistances].sort((a, b) => a.distance - b.distance) : [...withDistances];
      const filtered = filterValue
        ? sorted.filter((partner) => {
            const haystack = `${partner.company || partner.name || ''} ${partner.city || ''} ${partner.state || ''}`.toLowerCase();
            return haystack.includes(filterValue);
          })
        : sorted;

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
          ${partner.notes ? `<p class="mt-2 text-[11px] text-slate-400 leading-tight">${escapeHTML(partner.notes)}</p>` : ''}
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

    const findVehicleById = (id) => vehicles.find((vehicle) => `${vehicle.id}` === `${id}`);
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

      const vehicle = findVehicleById(card.dataset.id);
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
        openRepairModal(vehicle);
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

      applySelection(vehicle.id, null);
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
          getVehicleMarkerColor,
          getVehicleMarkerBorderColor,
          isVehicleNotMoving
        });
        return;
      }

      const fragment = document.createDocumentFragment();
      const vehiclesForMarkers = [];

      filtered.forEach((vehicle, idx) => {
        const movingMeta = getMovingMeta(vehicle);

          const focusHandler = useCallback(
            `vehicle-focus-${vehicle.id}`,
            () => () => {
              const currentVehicle = vehicles.find((item) => item.id === vehicle.id);
              if (!currentVehicle) return;
              if (!hasValidCoords(currentVehicle)) {
                console.warn(`No coordinates available for vehicle ${currentVehicle.vin || currentVehicle.id || 'unknown'}`);
                return;
              }
              applySelection(currentVehicle.id, null);
              focusVehicle(currentVehicle);
            },
            [vehicle.id]
          );

          let coords = null;
          if (hasValidCoords(vehicle)) {
            vehicle.locationAccuracy = vehicle.locationAccuracy || 'exact';
            vehicle.locationNote = getLocationNote(vehicle.locationAccuracy);
            coords = { lat: vehicle.lat, lng: vehicle.lng };
          } else if (vehicle.zipcode || vehicle.city || vehicle.state) {
            const derived = resolveCoords(vehicle, { zip: vehicle.zipcode, city: vehicle.city, stateCode: vehicle.state });
            coords = derived.coords;
            vehicle.lat = coords.lat;
            vehicle.lng = coords.lng;
            vehicle.locationAccuracy = derived.accuracy;
            vehicle.locationNote = getLocationNote(vehicle.locationAccuracy);
          } else {
            console.warn(`No location data for vehicle ${vehicle.vin || vehicle.id || 'unknown'}`);
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
          card.className = 'p-3 rounded-lg border border-slate-800 bg-slate-900/80 hover:border-amber-500/80 transition-all cursor-pointer shadow-sm hover:shadow-amber-500/20 backdrop-blur space-y-3';
          card.dataset.id = vehicle.id;
          card.dataset.type = 'vehicle';
          card.innerHTML = `
            <div class="flex items-start justify-between gap-3">
              <div class="space-y-1">
                <p class="text-[10px] font-black uppercase tracking-[0.15em] text-amber-300">${vehicle.model}</p>
                <h3 class="font-extrabold text-white text-sm leading-tight flex items-center gap-2">${vehicle.year || '—'} <span class="text-slate-600">•</span> ${vehicle.vin || 'VIN N/A'}</h3>
                <p class="text-[11px] text-slate-400 flex items-center gap-1">${svgIcon('mapPin')} ${vehicle.lastLocation || 'No location provided'}</p>
                ${vehicle.locationNote ? `<p class="text-[10px] text-amber-200 font-semibold">${vehicle.locationNote}</p>` : ''}
                <p class="text-[11px] text-blue-200 font-semibold">Customer ID: <span class="text-slate-100">${vehicle.customerId || '—'}</span></p>
              </div>
              <div class="flex flex-col items-end gap-1 text-right">
                <div class="inline-flex items-center gap-2 text-[10px] font-bold text-slate-300">
                  <span class="px-2 py-1 rounded-full border border-amber-400/40 bg-amber-500/10 text-amber-100">${vehicle.status}</span>
                </div>
                <div class="inline-flex items-center gap-2 text-[10px] font-semibold ${movingMeta.text} px-2 py-1 rounded-full border border-slate-800 ${movingMeta.bg}">
                  <span class="w-2 h-2 rounded-full ${movingMeta.dot}"></span>
                  <span>${movingMeta.label}</span>
                </div>
                <div class="text-[10px] font-semibold text-slate-300">
                  Days Parked <span class="text-slate-100">${vehicle.daysStationary || '—'}</span>
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
                <span class="flex items-center gap-2 font-semibold text-slate-200">${svgIcon('clock', 'h-3 w-3')} PT Last Read ${formatDateTime(vehicle.lastRead)}</span>
                <span class="text-slate-400">${vehicle.payment || ''}</span>
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
                <button type="button" data-action="repair-history" class="inline-flex items-center gap-1.5 rounded-lg border border-blue-400/50 bg-blue-500/15 px-3 py-1 text-[10px] font-bold text-blue-100 hover:bg-blue-500/25 transition-colors">Service History</button>
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
              openRepairModal(vehicle);
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
          getVehicleMarkerColor,
          getVehicleMarkerBorderColor,
          isVehicleNotMoving
        });
      }

    function focusVehicle(vehicle) {
      if (!vehicle) return;
      if (!hasValidCoords(vehicle)) return;
      if (!vehicleMarkersVisible) return;
      highlightLayer.clearLayers();

      const storedMarker = vehicleMarkers.get(vehicle.id)?.marker;
      const markerColor = getVehicleMarkerColor(vehicle);
      const anchorMarker = storedMarker || L.circleMarker([vehicle.lat, vehicle.lng], {
        radius: 9,
        color: '#0b1220',
        weight: 2.8,
        fillColor: markerColor,
        fillOpacity: 0.95,
        opacity: 0.98,
        className: 'vehicle-dot'
      }).addTo(highlightLayer);

      const halo = L.circleMarker([vehicle.lat, vehicle.lng], { radius: 12, color: markerColor, weight: 1.2, fillColor: markerColor, fillOpacity: 0.18 }).addTo(highlightLayer);
      halo.bringToBack();

      if (isVehicleNotMoving(vehicle)) {
        L.marker([vehicle.lat, vehicle.lng], {
          icon: L.divIcon({ className: 'vehicle-cross', html: '<div class="vehicle-cross-badge">✕</div>', iconSize: [16, 16], iconAnchor: [8, 8] }),
          interactive: false,
          zIndexOffset: 500
        }).addTo(highlightLayer);
      }

      anchorMarker.bindPopup(vehicleCard(vehicle), {
        className: 'vehicle-popup',
        autoPan: false,
        keepInView: false,
      }).openPopup();

      setTimeout(attachPopupHandlers, 50);

      showServicesFromOrigin(vehicle, { forceType: isAnyServiceSidebarOpen() ? getActivePartnerType() : null });

      applySelection(vehicle.id, null);
    }

    function vehicleCard(vehicle) {
      const accuracyDot = getAccuracyDot(vehicle.locationAccuracy);
      const modelYear = [vehicle.model || 'Vehicle', vehicle.year].filter(Boolean).join(' · ');
      return vehiclePopupTemplate({
        modelYear: modelYear || 'Vehicle',
        vin: vehicle.vin || 'N/A',
        status: vehicle.status || 'ACTIVE',
        customer: vehicle.customer || 'Customer pending',
        lastLocation: vehicle.lastLocation || 'No location provided',
        locationNote: vehicle.locationNote || '',
        accuracyDot,
        gpsFix: vehicle.gpsFix || 'Unknown',
        dealCompletion: vehicle.dealCompletion || '—'
      });
    }

    function matchesRange(value, min, max) {
      const pct = parseDealCompletion(value);
      if (!Number.isFinite(pct)) return min <= 0; // allow unknowns only when the range starts at 0
      return pct >= min && pct <= max;
    }

    const EMPTY_FILTER_VALUE = '__empty__';

    function matchesVehicleFilters(vehicle, query) {
      const q = (query || '').toLowerCase();
      if (q && !vehicle._searchBlob.includes(q)) return false;

      const invPrep = normalizeFilterValue(vehicle.invPrepStatus);
      if (vehicleFilters.invPrep.length && !vehicleFilters.invPrep.includes(invPrep)) return false;

      const gpsFixValue = normalizeFilterValue(vehicle.gpsFix) || EMPTY_FILTER_VALUE;
      if (vehicleFilters.gpsFix.length && !vehicleFilters.gpsFix.includes(gpsFixValue)) return false;

      const dealStatus = normalizeFilterValue(vehicle.status);
      if (vehicleFilters.dealStatus.length && !vehicleFilters.dealStatus.includes(dealStatus)) return false;

      const ptStatus = normalizeFilterValue(vehicle.ptStatus);
      if (vehicleFilters.ptStatus.length && !vehicleFilters.ptStatus.includes(ptStatus)) return false;

      if (!matchesRange(vehicle.dealCompletion, vehicleFilters.dealMin, vehicleFilters.dealMax)) return false;

      const movingStatus = getMovingStatus(vehicle);
      if (vehicleFilters.moving.length && !vehicleFilters.moving.includes(movingStatus)) return false;

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
      const raw = vehicle?.daysStationary ?? vehicle?.details?.days_stationary ?? vehicle?.details?.['Days Parked'];
      if (typeof raw === 'number') return Number.isFinite(raw) ? raw : Number.NEGATIVE_INFINITY;
      if (typeof raw === 'string') {
        const normalized = raw.replace(/,/g, '').trim();
        const parsed = Number.parseFloat(normalized);
        if (Number.isFinite(parsed)) return parsed;
      }
      return Number.NEGATIVE_INFINITY;
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
        empty.className = 'text-[10px] text-slate-600';
        empty.textContent = 'No options';
        container.appendChild(empty);
        return;
      }

      values.forEach((value) => {
        const label = document.createElement('label');
        label.className = 'flex items-center gap-1.5 text-[10px] text-slate-200';

        const input = document.createElement('input');
        input.type = 'checkbox';
        input.value = value;
        input.checked = selections.includes(value);
        input.className = 'h-3 w-3 rounded border border-slate-700 bg-slate-950 text-amber-400 focus:ring-1 focus:ring-amber-400';

        const span = document.createElement('span');
        span.textContent = labelResolver(value);

        label.appendChild(input);
        label.appendChild(span);
        container.appendChild(label);
      });
    }

    function updateVehicleFilterOptions() {
      const invPrepValues = getUniqueVehicleValues('invPrepStatus');
      const gpsValues = getUniqueVehicleValues('gpsFix', { includeEmpty: true });
      const dealStatusValues = getUniqueVehicleValues('status');
      const ptValues = getUniqueVehicleValues('ptStatus');
      const movingValues = getMovingOptions();

      vehicleFilters.invPrep = normalizeFilterSelections(invPrepValues, vehicleFilters.invPrep);
      vehicleFilters.gpsFix = normalizeFilterSelections(gpsValues, vehicleFilters.gpsFix);
      vehicleFilters.dealStatus = normalizeFilterSelections(dealStatusValues, vehicleFilters.dealStatus);
      vehicleFilters.ptStatus = normalizeFilterSelections(ptValues, vehicleFilters.ptStatus);
      vehicleFilters.moving = normalizeFilterSelections(movingValues, vehicleFilters.moving);

      renderCheckboxOptions('filter-invprep', invPrepValues, vehicleFilters.invPrep);
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
      updateVehicleFilterLabel('filter-gps-toggle', vehicleFilters.gpsFix, (value) => getGpsFixLabel(value, EMPTY_FILTER_VALUE));
      updateVehicleFilterLabel('filter-moving-toggle', vehicleFilters.moving, getMovingLabel);
      updateVehicleFilterLabel('filter-deal-status-toggle', vehicleFilters.dealStatus);
      updateVehicleFilterLabel('filter-pt-toggle', vehicleFilters.ptStatus);
    }

    function syncVehicleFilterInputs() {
      const invPrepContainer = document.getElementById('filter-invprep');
      const gpsContainer = document.getElementById('filter-gps');
      const movingContainer = document.getElementById('filter-moving');
      const dealStatusContainer = document.getElementById('filter-deal-status');
      const ptContainer = document.getElementById('filter-pt');
      const minInput = document.getElementById('filter-deal-min');
      const maxInput = document.getElementById('filter-deal-max');

      const syncCheckboxes = (container, selections) => {
        if (!container) return;
        container.querySelectorAll('input[type="checkbox"]').forEach((input) => {
          input.checked = selections.includes(input.value);
        });
      };

      syncCheckboxes(invPrepContainer, vehicleFilters.invPrep);
      syncCheckboxes(gpsContainer, vehicleFilters.gpsFix);
      syncCheckboxes(movingContainer, vehicleFilters.moving);
      syncCheckboxes(dealStatusContainer, vehicleFilters.dealStatus);
      syncCheckboxes(ptContainer, vehicleFilters.ptStatus);
      if (minInput) minInput.value = vehicleFilters.dealMin;
      if (maxInput) maxInput.value = vehicleFilters.dealMax;
      updateVehicleFilterLabels();
    }

    function resetVehicleFilters() {
      vehicleFilters.invPrep = [];
      vehicleFilters.gpsFix = [];
      vehicleFilters.moving = [];
      vehicleFilters.dealStatus = [];
      vehicleFilters.ptStatus = [];
      vehicleFilters.dealMin = 0;
      vehicleFilters.dealMax = 100;
      syncVehicleFilterInputs();
      renderVehicles();
      persistVehicleFilterPrefs();
    }

    function bindVehicleFilterDropdowns() {
      const dropdowns = [
        { toggleId: 'filter-invprep-toggle', panelId: 'filter-invprep-panel' },
        { toggleId: 'filter-gps-toggle', panelId: 'filter-gps-panel' },
        { toggleId: 'filter-moving-toggle', panelId: 'filter-moving-panel' },
        { toggleId: 'filter-deal-status-toggle', panelId: 'filter-deal-status-panel' },
        { toggleId: 'filter-pt-toggle', panelId: 'filter-pt-panel' }
      ];

      const entries = dropdowns
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

      const resetBtn = document.getElementById('vehicle-filters-reset');
      resetBtn?.addEventListener('click', (e) => {
        e.preventDefault();
        resetVehicleFilters();
      });

      bindVehicleFilterDropdowns();
    }

    function getVehicleList(query) {
      if (selectedVehicleId !== null) {
        return vehicles.filter(v => v.id === selectedVehicleId);
      }

      let baseList = vehicles;
      if (selectedTechId !== null) {
        const tech = technicians.find(t => t.id === selectedTechId);
        const nearby = getVehiclesForTech(tech);
        baseList = nearby.length ? nearby : vehicles;
      }

      const filtered = filterVehicles(baseList, query);
      const shouldSortByDaysParked = vehicleFilters.moving.includes('stopped');
      const shouldPrioritizeOnRev = vehicleFilters.invPrep.includes('available for deals')
        && vehicleFilters.moving.includes('moving');

      let list = shouldSortByDaysParked ? sortVehiclesByDaysParked(filtered) : filtered;
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

      const vehicle = vehicles.find(v => v.id === selectedVehicleId);
      const tech = technicians.find(t => t.id === selectedTechId);
      const parts = [];
      if (vehicle) parts.push(`Vehicle ${vehicle.vin || vehicle.model}`);
      if (tech) parts.push(`Technician ${tech.company}`);
      text.textContent = `Filtered by ${parts.join(' & ')}`;
      banner.classList.remove('hidden');
    }

    function applySelection(vehicleId = null, techId = null) {
      selectedVehicleId = vehicleId;
      selectedTechId = techId;
      if (vehicleId !== null && !syncingVehicleSelection) {
        const selectedVehicle = vehicles.find((vehicle) => `${vehicle.id}` === `${vehicleId}`);
        setSelectedVehicle({
          id: selectedVehicle?.id ?? vehicleId,
          vin: selectedVehicle?.vin || '',
          customerId: selectedVehicle?.customerId || ''
        });
      } else if (vehicleId === null && !syncingVehicleSelection) {
        setSelectedVehicle(null);
      }
      if (vehicleId !== null) {
        sidebarStateController?.setState?.('right', true);
      }
      renderVehicles();
      renderVisibleSidebars();
      toggleSelectionBanner();
    }

    function resetSelection() {
      selectedVehicleId = null;
      selectedTechId = null;
      Object.keys(selectedServiceByType).forEach(key => delete selectedServiceByType[key]);
      highlightLayer?.clearLayers();
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
      sidebarStateController?.setState?.('right', false);
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
        const editConfig = EDITABLE_VEHICLE_FIELDS[header.toLowerCase()];
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

    function openRepairModal(vehicle) {
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
                  <div class="absolute right-0 z-10 mt-2 hidden w-[320px] rounded-lg border border-slate-800 bg-slate-950/95 p-3 text-[11px] text-slate-200 shadow-xl" data-gps-columns-panel>
                    <div class="flex items-start justify-between gap-2">
                      <div>
                        <p class="text-[10px] font-semibold uppercase tracking-[0.3em] text-slate-400">Column controls</p>
                        <p class="text-[10px] text-slate-500">Drag to reorder, toggle visibility, and set width.</p>
                      </div>
                    </div>
                    <div class="mt-3 grid gap-2 max-h-64 overflow-auto" data-gps-columns-list></div>
                  </div>
                </div>
              </div>
            </div>
            <div class="overflow-auto">
              <table class="min-w-full text-left text-[11px] text-slate-200">
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

            const vin = String(
              getField(vehicle?.details || {}, 'VIN', 'Vin', 'vin')
                || vehicle?.VIN
                || vehicle?.vin
                || ''
            ).trim();
            if (updateTable === TABLES.vehiclesUpdates && !vin) {
              throw new Error('VIN missing for update.');
            }
            const updateRequest = updateTable === TABLES.vehiclesUpdates
              ? supabaseClient
                  .from(updateTable)
                  .upsert({ VIN: vin, [updateColumn]: newValue }, { onConflict: 'VIN' })
              : supabaseClient
                  .from(updateTable)
                  .update({ [updateColumn]: newValue })
                  .eq('id', vehicle.id);
            const { error } = await runWithTimeout(
              updateRequest,
              8000,
              'Error de comunicación con la base de datos.'
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
            if (message.includes('comunicación')) {
              alert('Error de comunicación con la base de datos.');
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
      subscribeServiceFilters((filters) => {
        Object.assign(serviceFilters, filters);
        syncServiceFilterInputs();
        renderVisibleSidebars();
        const origin = getCurrentOrigin();
        if (origin) {
          showServicesFromOrigin(origin, { forceType: isAnyServiceSidebarOpen() ? getActivePartnerType() : null });
        }
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
        setupBackgroundManager();
        await loadStateCenters();
        initMap();
        await syncAvailableServiceTypes();
        updateServiceVisibilityUI();
        if (isServiceTypeEnabled('tech')) loadTechnicians();
        loadHotspots();
        loadBlacklistSites();
        await loadVehicleFilterPrefs();
        loadVehicles();
        await loadAllServices();
        setupResizableSidebars();
        setupSidebarToggles();
        setupLayerToggles();
        bindVehicleFilterHandlers();
        syncVehicleFilterInputs();
        const filterConfigs = [
          { id: 'tech-filter', type: 'tech' },
          { id: 'reseller-filter', type: 'reseller' },
          { id: 'repair-filter', type: 'repair' },
          { id: 'dynamic-service-filter', type: 'custom' },
        ].filter(({ type }) => isServiceTypeEnabled(type));

        filterConfigs.forEach(({ id, type }) => {
          const input = document.getElementById(id);
          if (!input) return;
          input.addEventListener('input', () => {
            serviceFilters[type] = input.value;
            setServiceFilter(type, input.value);
            renderVisibleSidebars();
            const origin = getCurrentOrigin();
            if (origin) {
              showServicesFromOrigin(origin, { forceType: isAnyServiceSidebarOpen() ? getActivePartnerType() : null });
            }
          });
        });

        syncServiceFilterInputs();

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
          await loadVehicles();
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
            hotspots: refreshHotspots,
            blacklist: refreshBlacklist,
            services: refreshServices
          }
        });

        startSupabaseKeepAlive({ supabaseClient, table: TABLES.vehicles });

        window.addEventListener('auth:role-ready', () => renderVehicles());
      })();
    });
