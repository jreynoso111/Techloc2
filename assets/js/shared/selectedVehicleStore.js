const STORAGE_KEY = 'techloc:selected-vehicle';
const listeners = new Set();
let currentSelection = null;

const safeParse = (value) => {
  if (!value) return null;
  try {
    return JSON.parse(value);
  } catch (error) {
    console.warn('Unable to parse selected vehicle from storage.', error);
    return null;
  }
};

const normalizeSelection = (selection) => {
  if (!selection) return null;
  const vin = selection.vin ? String(selection.vin).trim() : '';
  const customerId = selection.customerId ? String(selection.customerId).trim() : '';
  return {
    id: selection.id ?? null,
    vin,
    customerId,
    updatedAt: selection.updatedAt || Date.now(),
  };
};

const notify = (next) => {
  listeners.forEach((listener) => listener(next));
  window.dispatchEvent(new CustomEvent('selected-vehicle-change', { detail: next }));
};

const getSelectedVehicle = () => {
  if (currentSelection) return currentSelection;
  const stored = safeParse(localStorage.getItem(STORAGE_KEY));
  currentSelection = normalizeSelection(stored);
  return currentSelection;
};

const setSelectedVehicle = (selection) => {
  currentSelection = normalizeSelection(selection);
  if (currentSelection) {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(currentSelection));
  } else {
    localStorage.removeItem(STORAGE_KEY);
  }
  notify(currentSelection);
};

const subscribeSelectedVehicle = (listener) => {
  if (typeof listener !== 'function') return () => {};
  listeners.add(listener);
  return () => listeners.delete(listener);
};

const bindStorageListener = () => {
  window.addEventListener('storage', (event) => {
    if (event.key !== STORAGE_KEY) return;
    currentSelection = normalizeSelection(safeParse(event.newValue));
    notify(currentSelection);
  });
};

export {
  bindStorageListener,
  getSelectedVehicle,
  setSelectedVehicle,
  subscribeSelectedVehicle,
};
