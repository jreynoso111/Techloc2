import { createConstellationBackground } from './ui/components/constellation.js';
import { createSnowBackground } from './snow.js';
import { createRainBackground } from './rain.js';
import { getCoordsIpFirst } from './geoResolver.js';

const STORAGE_KEY = 'techloc-background-mode';
const SNOW_CODES = new Set([71, 73, 75, 77, 85, 86]);
const RAIN_CODES = new Set([51, 53, 55, 56, 57, 61, 63, 65, 66, 67, 80, 81, 82]);
const WEATHER_URL = (lat, lon) =>
  `https://api.open-meteo.com/v1/forecast?latitude=${lat}&longitude=${lon}&current=weather_code&timezone=auto`;

const VALID_MODES = new Set(['snow', 'rain', 'constellation', 'auto']);

export const BACKGROUND_STORAGE_KEY = STORAGE_KEY;
export const BACKGROUND_MODES = Array.from(VALID_MODES);
export const normalizeBackgroundMode = (mode) => (VALID_MODES.has(mode) ? mode : 'auto');

const getStoredMode = () => normalizeBackgroundMode(localStorage.getItem(STORAGE_KEY));

const saveMode = (mode) => localStorage.setItem(STORAGE_KEY, normalizeBackgroundMode(mode));

export const setupBackgroundManager = ({
  canvasId = 'constellation-canvas',
  controlButton,
  controlMenu,
  statusLabel,
  initialMode,
  onModeChange,
} = {}) => {
  let currentMode = normalizeBackgroundMode(initialMode ?? getStoredMode());
  let activeCleanup = null;
  let menuOpen = false;
  let appliedVariant = null;
  let weatherIntervalId = null;

  const fetchIsSnowing = async () => {
    try {
      const coords = await getCoordsIpFirst();
      if (!coords?.lat || !coords?.lon) {
        return { snowing: false, raining: false, reason: null };
      }

      const response = await fetch(WEATHER_URL(coords.lat, coords.lon), { cache: 'no-store' });
      if (!response.ok) {
        return { snowing: false, raining: false, reason: null };
      }

      const data = await response.json();
      const code = Number(data?.current?.weather_code);
      const snowing = SNOW_CODES.has(code);
      const raining = RAIN_CODES.has(code);

      return {
        snowing,
        raining,
        reason: snowing ? 'Snow detected in your area' : raining ? 'Rain detected in your area' : 'No precipitation right now',
      };
    } catch (error) {
      return { snowing: false, raining: false, reason: null };
    }
  };

  const stopWeatherInterval = () => {
    if (weatherIntervalId) {
      clearInterval(weatherIntervalId);
      weatherIntervalId = null;
    }
  };

  const checkAutoWeather = async () => {
    if (currentMode !== 'auto') return;
    setStatus('Checking local weather for background…');
    const { snowing, raining, reason } = await fetchIsSnowing();
    if (currentMode !== 'auto') return;
    applyVariant(snowing ? 'snow' : raining ? 'rain' : 'constellation');
    setStatus(reason || '');
  };

  const startWeatherInterval = () => {
    stopWeatherInterval();
    checkAutoWeather();
    weatherIntervalId = setInterval(checkAutoWeather, 10 * 60 * 1000);
  };

  const setStatus = (text) => {
    if (statusLabel) statusLabel.textContent = text;
  };

  const closeMenu = () => {
    if (controlMenu) controlMenu.classList.add('hidden');
    menuOpen = false;
  };

  const openMenu = () => {
    if (controlMenu) controlMenu.classList.remove('hidden');
    menuOpen = true;
  };

  const toggleMenu = () => (menuOpen ? closeMenu() : openMenu());

  const applyVariant = (variant) => {
    appliedVariant = variant;
    if (activeCleanup) activeCleanup();
    const runner =
      variant === 'snow'
        ? createSnowBackground(canvasId)
        : variant === 'rain'
        ? createRainBackground(canvasId)
        : createConstellationBackground(canvasId);
    activeCleanup = runner?.cleanup || null;
  };

  const highlightActiveOption = () => {
    if (!controlMenu) return;
    controlMenu.querySelectorAll('[data-bg-option]').forEach((option) => {
      const optionValue = option.getAttribute('data-bg-option');
      if (optionValue === currentMode) {
        option.classList.add('bg-slate-800', 'text-white');
      } else {
        option.classList.remove('bg-slate-800', 'text-white');
      }
    });
  };

  const updateButtonLabel = () => {
    if (!controlButton) return;
    const labelEl = controlButton.querySelector('[data-bg-label]');
    if (labelEl) {
      const labels = {
        auto: 'Background: Auto',
        snow: 'Background: Snow',
        rain: 'Background: Rain',
        constellation: 'Background: Constellations',
      };
      labelEl.textContent = labels[currentMode] || 'Background';
    }

    const icon = controlButton.querySelector('[data-bg-icon]');
    if (icon) {
      icon.setAttribute(
        'data-lucide',
        currentMode === 'snow'
          ? 'snowflake'
          : currentMode === 'rain'
          ? 'cloud-rain'
          : currentMode === 'constellation'
          ? 'sparkles'
          : 'cloud-sun'
      );
      if (window.lucide) window.lucide.createIcons();
    }
  };

  const persistMode = (modeToPersist) => {
    const normalized = normalizeBackgroundMode(modeToPersist);
    saveMode(normalized);

    if (typeof onModeChange === 'function') {
      Promise.resolve()
        .then(() => onModeChange(normalized))
        .catch((error) => console.warn('Unable to sync background mode', error));
    }
  };

  const applyMode = async (mode = currentMode) => {
    currentMode = normalizeBackgroundMode(mode);
    persistMode(currentMode);
    highlightActiveOption();
    updateButtonLabel();

    if (mode === 'auto') {
      startWeatherInterval();
      return;
    }

    stopWeatherInterval();
    applyVariant(mode === 'snow' ? 'snow' : mode === 'rain' ? 'rain' : 'constellation');
    setStatus(
      mode === 'snow'
        ? 'Snow mode active'
        : mode === 'rain'
        ? 'Rain mode active'
        : 'Constellations mode active'
    );
  };

  const handleMenuSelection = (event) => {
    const option = event.target.closest('[data-bg-option]');
    if (!option) return;
    const selectedMode = option.getAttribute('data-bg-option');

    if (selectedMode === 'auto') {
      const cycle = ['auto', 'constellation', 'snow', 'rain'];
      const currentIndex = cycle.indexOf(currentMode);
      const nextMode = cycle[(currentIndex + 1) % cycle.length];
      applyMode(nextMode);
      return;
    }

    closeMenu();
    applyMode(selectedMode);
  };

  if (controlButton) {
    controlButton.addEventListener('click', (event) => {
      event.stopPropagation();
      toggleMenu();
    });
  }

  if (controlMenu) {
    controlMenu.addEventListener('click', handleMenuSelection);
  }

  document.addEventListener('click', (event) => {
    if (!menuOpen) return;
    if (controlMenu && controlMenu.contains(event.target)) return;
    if (controlButton && controlButton.contains(event.target)) return;
    closeMenu();
  });

  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape' && menuOpen) {
      closeMenu();
    }
  });

  applyMode(currentMode);

  return {
    applyMode,
    setAuto: () => applyMode('auto'),
    setConstellations: () => applyMode('constellation'),
    setSnow: () => applyMode('snow'),
    get mode() {
      return currentMode;
    },
    get variant() {
      return appliedVariant;
    },
  };
};
