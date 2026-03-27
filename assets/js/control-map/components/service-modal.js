export const SERVICE_HEADER_LABELS = {
  company_name: 'Company',
  company: 'Company',
  contact_name: 'Contact',
  contact: 'Contact',
  phone: 'Phone',
  email: 'Email',
  city: 'City',
  state: 'State',
  region: 'Region',
  zip: 'ZIP',
  zipcode: 'ZIP',
  postal_code: 'Postal Code',
  availability: 'Availability',
  authorization: 'Authorization',
  notes: 'Notes',
  note: 'Note',
  lat: 'Lat',
  lng: 'Long',
  long: 'Long',
  longitude: 'Long',
  latitude: 'Lat'
};

const SERVICE_MODAL_STORAGE_KEY = 'serviceModalColumns';

export const loadServiceModalPrefs = () => {
  try {
    const raw = localStorage.getItem(SERVICE_MODAL_STORAGE_KEY);
    const parsed = raw ? JSON.parse(raw) : {};
    return {
      order: Array.isArray(parsed.order) ? parsed.order : [],
      hidden: Array.isArray(parsed.hidden) ? parsed.hidden : []
    };
  } catch (error) {
    console.warn('Failed to load service modal column preferences.', error);
    return { order: [], hidden: [] };
  }
};

export const saveServiceModalPrefs = (prefs) => {
  localStorage.setItem(SERVICE_MODAL_STORAGE_KEY, JSON.stringify(prefs));
};

const normalizeHeaderToken = (value = '') => `${value}`.trim().toLowerCase().replace(/[^a-z0-9]/g, '');

export const getServiceModalHeaders = (detailRecord = {}, fallbackHeaders = []) => {
  const baseHeaders = [
    ...Object.keys(detailRecord || {}),
    ...fallbackHeaders.filter(Boolean)
  ].filter(Boolean);

  const dedupedHeaders = [];
  const seen = new Set();
  baseHeaders.forEach((header) => {
    const key = normalizeHeaderToken(header);
    if (!key || seen.has(key)) return;
    seen.add(key);
    dedupedHeaders.push(header);
  });

  const prefs = loadServiceModalPrefs();
  const ordered = prefs.order.filter((header) => dedupedHeaders.includes(header));
  dedupedHeaders.forEach((header) => {
    if (!ordered.includes(header)) ordered.push(header);
  });

  return { headers: ordered, hidden: new Set(prefs.hidden || []) };
};

export const renderServiceModalColumnsList = (headers, hiddenSet) => {
  const list = document.getElementById('service-modal-columns-list');
  const modal = document.getElementById('service-modal');
  if (!list || !modal) return;
  list.innerHTML = '';
  headers.forEach((header) => {
    const id = `service-col-${header.replace(/\s+/g, '-').toLowerCase()}`;
    const label = SERVICE_HEADER_LABELS[header] || header.replace(/_/g, ' ');
    const item = document.createElement('label');
    item.className = 'flex items-center gap-2 rounded-md border border-slate-800 bg-slate-900/60 px-2 py-1';
    item.innerHTML = `
      <input type="checkbox" class="rounded border-slate-700 bg-slate-900 text-amber-400 focus:ring-amber-400" id="${id}">
      <span class="text-[11px] text-slate-200">${label}</span>
    `;
    const checkbox = item.querySelector('input');
    checkbox.checked = !hiddenSet.has(header);
    checkbox.addEventListener('change', () => {
      const prefs = loadServiceModalPrefs();
      const hidden = new Set(prefs.hidden || []);
      if (checkbox.checked) {
        hidden.delete(header);
      } else {
        hidden.add(header);
      }
      prefs.hidden = [...hidden];
      saveServiceModalPrefs(prefs);
      const safeHeader = (window.CSS && CSS.escape) ? CSS.escape(header) : header.replace(/"/g, '\\"');
      const row = modal.querySelector(`tr[data-service-header="${safeHeader}"]`);
      if (row) row.classList.toggle('hidden', !checkbox.checked);
    });
    list.appendChild(item);
  });
};
