export const VEHICLE_HEADER_LABELS = {
  days_stationary: 'Days Parked',
  short_location: 'GPS City'
};

const VEHICLE_MODAL_STORAGE_KEY = 'vehicleModalColumns';

export const loadVehicleModalPrefs = () => {
  try {
    const raw = localStorage.getItem(VEHICLE_MODAL_STORAGE_KEY);
    const parsed = raw ? JSON.parse(raw) : {};
    return {
      order: Array.isArray(parsed.order) ? parsed.order : [],
      hidden: Array.isArray(parsed.hidden) ? parsed.hidden : []
    };
  } catch (error) {
    console.warn('Failed to load vehicle modal column preferences.', error);
    return { order: [], hidden: [] };
  }
};

export const saveVehicleModalPrefs = (prefs) => {
  localStorage.setItem(VEHICLE_MODAL_STORAGE_KEY, JSON.stringify(prefs));
};

export const getVehicleModalHeaders = (vehicleHeaders = []) => {
  const baseHeaders = vehicleHeaders.filter((header) => header.toLowerCase() !== 'pt city');
  const prefs = loadVehicleModalPrefs();
  const ordered = prefs.order.filter((header) => baseHeaders.includes(header));
  baseHeaders.forEach((header) => {
    if (!ordered.includes(header)) ordered.push(header);
  });
  return { headers: ordered, hidden: new Set(prefs.hidden || []) };
};

export const renderVehicleModalColumnsList = (headers, hiddenSet) => {
  const list = document.getElementById('vehicle-modal-columns-list');
  if (!list) return;
  list.innerHTML = '';
  headers.forEach((header) => {
    const id = `vehicle-col-${header.replace(/\s+/g, '-').toLowerCase()}`;
    const label = VEHICLE_HEADER_LABELS[header] || header;
    const item = document.createElement('label');
    item.className = 'flex items-center gap-2 rounded-md border border-slate-800 bg-slate-900/60 px-2 py-1';
    item.innerHTML = `
      <input type="checkbox" class="rounded border-slate-700 bg-slate-900 text-amber-400 focus:ring-amber-400" id="${id}">
      <span class="text-[11px] text-slate-200">${label}</span>
    `;
    const checkbox = item.querySelector('input');
    checkbox.checked = !hiddenSet.has(header);
    checkbox.addEventListener('change', () => {
      const prefs = loadVehicleModalPrefs();
      const hidden = new Set(prefs.hidden || []);
      if (checkbox.checked) {
        hidden.delete(header);
      } else {
        hidden.add(header);
      }
      prefs.hidden = [...hidden];
      saveVehicleModalPrefs(prefs);
      const safeHeader = (window.CSS && CSS.escape) ? CSS.escape(header) : header.replace(/"/g, '\\"');
      const row = document.querySelector(`tr[data-header="${safeHeader}"]`);
      if (row) row.classList.toggle('hidden', !checkbox.checked);
    });
    list.appendChild(item);
  });
};
