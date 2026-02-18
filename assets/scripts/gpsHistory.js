const getDefaultHelpers = () => ({
  escapeHTML: (value = '') => `${value}`,
  formatDateTime: (value) => value
});

const getVehicleVin = (vehicle) => {
  const vin = vehicle?.VIN ?? vehicle?.vin ?? vehicle?.details?.VIN ?? '';
  return typeof vin === 'string' ? vin.trim() : '';
};

const getVehicleId = (vehicle) => {
  const vehicleId = vehicle?.vehicle_id
    ?? vehicle?.vehicleId
    ?? vehicle?.id
    ?? vehicle?.details?.vehicle_id
    ?? vehicle?.details?.vehicleId
    ?? '';
  return vehicleId ? `${vehicleId}`.trim() : '';
};

const createGpsHistoryManager = ({
  supabaseClient,
  ensureSupabaseSession,
  runWithTimeout,
  timeoutMs = 10000,
  tableName,
  escapeHTML,
  formatDateTime
} = {}) => {
  const helpers = getDefaultHelpers();
  const safeEscape = escapeHTML || helpers.escapeHTML;
  const safeFormatDateTime = formatDateTime || helpers.formatDateTime;

  const fetchGpsHistory = async ({ vin, vehicleId } = {}) => {
    const normalizedVin = typeof vin === 'string' ? vin.trim() : '';
    const normalizedVehicleId = typeof vehicleId === 'string' ? vehicleId.trim() : '';
    if (!supabaseClient || (!normalizedVin && !normalizedVehicleId)) {
      return { records: [], error: supabaseClient ? new Error('VIN missing') : new Error('Supabase unavailable') };
    }
    try {
      await ensureSupabaseSession?.();
      const sourceTable = tableName || '"PT-LastPing"';
      const baseQuery = supabaseClient
        .from(sourceTable)
        .select('*');
      const query = normalizedVin
        ? baseQuery.eq('VIN', normalizedVin)
        : baseQuery.eq('vehicle_id', normalizedVehicleId);
      const { data, error } = await runWithTimeout(
        query,
        timeoutMs,
        'GPS history request timed out.'
      );
      if (error) throw error;
      const records = Array.isArray(data) ? data : [];
      records.sort((a, b) => {
        const idA = a?.id;
        const idB = b?.id;
        if (idA != null && idB != null) {
          return (Number(idB) || 0) - (Number(idA) || 0);
        }
        const dateCandidates = ['Date', 'created_at', 'gps_time', 'PT-LastPing'];
        const dateValue = (record) => {
          for (const key of dateCandidates) {
            const value = record?.[key];
            if (value) return Date.parse(value);
          }
          return NaN;
        };
        const timeA = dateValue(a);
        const timeB = dateValue(b);
        if (Number.isNaN(timeA) && Number.isNaN(timeB)) return 0;
        if (Number.isNaN(timeA)) return 1;
        if (Number.isNaN(timeB)) return -1;
        return timeB - timeA;
      });
      return { records, error: null };
    } catch (error) {
      console.error('Failed to load GPS history:', error);
      return { records: [], error };
    }
  };

  const setupGpsHistoryUI = ({ vehicle, body, signal, records: preloadedRecords, error: preloadedError }) => {
    const VIN = getVehicleVin(vehicle);
    const vehicleId = getVehicleId(vehicle);
    const historyBody = body.querySelector('[data-gps-history-body]');
    const historyHead = body.querySelector('[data-gps-history-head]');
    const columnsToggle = body.querySelector('[data-gps-columns-toggle]');
    const columnsPanel = body.querySelector('[data-gps-columns-panel]');
    const columnsList = body.querySelector('[data-gps-columns-list]');
    const searchInput = body.querySelector('[data-gps-search]');
    const statusText = body.querySelector('[data-gps-status]');
    const connectionStatus = body.querySelector('[data-gps-connection-status]');

    let gpsCache = [];
    let gpsColumns = [];
    let availableColumnKeys = [];
    let columnVisibility = {};
    let columnOrder = [];
    let columnWidths = {};
    let searchQuery = '';
    let sortKey = '';
    let sortDirection = 'desc';

    const COLUMN_STORAGE_KEY = 'gpsHistoryColumnPrefs';
    const WIDTH_STORAGE_KEY = 'gpsHistoryColumnWidths';

    const DEFAULT_COLUMN_ORDER = [
      'created_at',
      'PT-LastPing',
      'gps_time',
      'latitude',
      'lat',
      'longitude',
      'long',
      'lng',
      'speed',
      'heading',
      'VIN'
    ];

    const titleCase = (value) => value
      .replace(/_/g, ' ')
      .replace(/\b\w/g, (match) => match.toUpperCase());

    const loadPreferences = () => {
      let savedPrefs = {};
      let savedWidths = {};
      try {
        savedPrefs = JSON.parse(localStorage.getItem(COLUMN_STORAGE_KEY) || '{}');
      } catch (error) {
        savedPrefs = {};
      }
      try {
        savedWidths = JSON.parse(localStorage.getItem(WIDTH_STORAGE_KEY) || '{}');
      } catch (error) {
        savedWidths = {};
      }
      columnVisibility = savedPrefs.visibility || {};
      columnOrder = savedPrefs.order || [];
      columnWidths = savedWidths || {};
    };

    const savePreferences = () => {
      localStorage.setItem(COLUMN_STORAGE_KEY, JSON.stringify({
        visibility: columnVisibility,
        order: columnOrder
      }));
      localStorage.setItem(WIDTH_STORAGE_KEY, JSON.stringify(columnWidths));
    };

    const buildColumnsFromKeys = (keys = []) => {
      const allKeys = Array.from(new Set(keys.filter(Boolean)));
      const ordered = [
        ...DEFAULT_COLUMN_ORDER.filter((key) => allKeys.includes(key)),
        ...allKeys.filter((key) => !DEFAULT_COLUMN_ORDER.includes(key)).sort()
      ];
      gpsColumns = ordered.map((key) => ({ key, label: titleCase(key) }));

      if (!columnOrder.length) {
        columnOrder = [...ordered];
      } else {
        const missing = ordered.filter((key) => !columnOrder.includes(key));
        columnOrder = columnOrder.filter((key) => ordered.includes(key)).concat(missing);
      }

      gpsColumns.forEach((col) => {
        if (columnVisibility[col.key] === undefined) {
          columnVisibility[col.key] = true;
        }
        if (columnWidths[col.key] === undefined) {
          columnWidths[col.key] = '';
        }
      });
      savePreferences();
    };

    const buildColumns = (records) => {
      const columnKeys = new Set(availableColumnKeys);
      records.forEach((record) => {
        Object.keys(record || {}).forEach((key) => {
          columnKeys.add(key);
        });
      });
      buildColumnsFromKeys(Array.from(columnKeys));
    };

    const getVisibleColumns = () => columnOrder
      .filter((key) => columnVisibility[key])
      .map((key) => gpsColumns.find((col) => col.key === key))
      .filter(Boolean);

    const formatValue = (key, value) => {
      if (value === null || value === undefined || value === '') return '—';
      const normalizedKey = key.toLowerCase();
      if (key === 'PT-LastPing' || normalizedKey.includes('date') || normalizedKey.includes('time')) {
        return safeFormatDateTime(value);
      }
      if (typeof value === 'object') {
        return safeEscape(JSON.stringify(value));
      }
      return safeEscape(`${value}`);
    };

    const renderTableHead = () => {
      if (!historyHead) return;
      const visibleColumns = getVisibleColumns();
      if (!visibleColumns.length) {
        historyHead.innerHTML = `
          <tr>
            <th class="py-2 pr-3">No columns selected</th>
          </tr>
        `;
        return;
      }
      historyHead.innerHTML = `
        <tr>
          ${visibleColumns.map((col) => {
            const width = columnWidths[col.key];
            const widthStyle = width ? `style="width:${width}px;min-width:${width}px"` : '';
            return `
              <th class="py-2 pr-3 align-bottom" ${widthStyle}>
                <button type="button" class="group inline-flex items-center gap-1 text-left text-[10px] uppercase tracking-[0.08em] text-slate-400 hover:text-slate-200 transition-colors" data-gps-sort="${col.key}">
                  <span>${safeEscape(col.label)}</span>
                  <span class="text-[9px] text-slate-500 group-hover:text-slate-300">${sortKey === col.key ? (sortDirection === 'asc' ? '▲' : '▼') : ''}</span>
                </button>
              </th>
            `;
          }).join('')}
        </tr>
      `;
    };

    const getFilteredRecords = (records) => {
      const query = searchQuery.trim().toLowerCase();
      if (!query) return records;
      return records.filter((record) => Object.values(record || {}).some((value) =>
        `${value ?? ''}`.toLowerCase().includes(query)
      ));
    };

    const getSortedRecords = (records) => {
      if (!sortKey) return records;
      const direction = sortDirection === 'asc' ? 1 : -1;
      return [...records].sort((a, b) => {
        const valueA = a?.[sortKey];
        const valueB = b?.[sortKey];
        if (valueA === valueB) return 0;
        if (valueA === null || valueA === undefined || valueA === '') return 1 * direction;
        if (valueB === null || valueB === undefined || valueB === '') return -1 * direction;
        if (typeof valueA === 'number' && typeof valueB === 'number') {
          return (valueA - valueB) * direction;
        }
        return `${valueA}`.localeCompare(`${valueB}`, undefined, { numeric: true, sensitivity: 'base' }) * direction;
      });
    };

    const renderHistory = (records = []) => {
      if (!historyBody) return;
      gpsCache = records;
      buildColumns(records);
      renderColumnsList();
      renderTableHead();
      const visibleColumns = getVisibleColumns();
      const filteredRecords = getSortedRecords(getFilteredRecords(records));
      if (!filteredRecords.length) {
        const colSpan = Math.max(visibleColumns.length, 1);
        historyBody.innerHTML = `
          <tr>
            <td class="py-2 pr-3 text-slate-400" colspan="${colSpan}">No GPS history found.</td>
          </tr>
        `;
        return;
      }
      historyBody.innerHTML = filteredRecords.map((record) => `
        <tr>
          ${visibleColumns.map((col) => {
            const width = columnWidths[col.key];
            const widthStyle = width ? `style="width:${width}px;min-width:${width}px"` : '';
            return `<td class="py-2 pr-3 text-slate-300 align-top" ${widthStyle}>${formatValue(col.key, record?.[col.key])}</td>`;
          }).join('')}
        </tr>
      `).join('');
    };

    const renderColumnsList = () => {
      if (!columnsList) return;
      if (!gpsColumns.length) {
        columnsList.innerHTML = '<p class="text-xs text-slate-400">No columns available.</p>';
        return;
      }
      columnsList.innerHTML = columnOrder.map((key) => {
        const column = gpsColumns.find((col) => col.key === key);
        if (!column) return '';
        const widthValue = columnWidths[key] || '';
        return `
          <div class="flex items-center gap-2 rounded-md border border-slate-800 bg-slate-950/70 px-2 py-1.5" draggable="true" data-gps-column="${column.key}">
            <span class="text-slate-500">⋮⋮</span>
            <label class="flex items-center gap-2 flex-1">
              <input type="checkbox" value="${column.key}" class="h-3.5 w-3.5 rounded border-slate-600 bg-slate-950" ${columnVisibility[column.key] ? 'checked' : ''} />
              <span class="text-slate-200">${safeEscape(column.label)}</span>
            </label>
            <input
              type="number"
              min="60"
              max="400"
              step="10"
              value="${widthValue}"
              placeholder="Auto"
              class="w-20 rounded border border-slate-700 bg-slate-900 px-2 py-1 text-[10px] text-slate-200"
              data-gps-width="${column.key}"
            />
          </div>
        `;
      }).join('');
    };

    const updateColumnOrder = (dragKey, targetKey) => {
      if (!dragKey || !targetKey || dragKey === targetKey) return;
      const currentOrder = [...columnOrder];
      const fromIndex = currentOrder.indexOf(dragKey);
      const toIndex = currentOrder.indexOf(targetKey);
      if (fromIndex === -1 || toIndex === -1) return;
      currentOrder.splice(fromIndex, 1);
      currentOrder.splice(toIndex, 0, dragKey);
      columnOrder = currentOrder;
      savePreferences();
      renderColumnsList();
      renderTableHead();
      renderHistory(gpsCache);
    };

    const setConnectionStatus = (label, tone = 'neutral') => {
      if (!connectionStatus) return;
      connectionStatus.textContent = label;
      connectionStatus.classList.remove(
        'border-emerald-400/50',
        'bg-emerald-500/15',
        'text-emerald-100',
        'border-amber-400/50',
        'bg-amber-500/15',
        'text-amber-100',
        'border-rose-400/50',
        'bg-rose-500/15',
        'text-rose-100'
      );
      if (tone === 'success') {
        connectionStatus.classList.add('border-emerald-400/50', 'bg-emerald-500/15', 'text-emerald-100');
      } else if (tone === 'warning') {
        connectionStatus.classList.add('border-amber-400/50', 'bg-amber-500/15', 'text-amber-100');
      } else if (tone === 'error') {
        connectionStatus.classList.add('border-rose-400/50', 'bg-rose-500/15', 'text-rose-100');
      }
    };

    const finalizeRender = (records, error) => {
      renderHistory(records);
      if (statusText) {
        statusText.textContent = error
          ? 'Unable to load GPS history.'
          : `${records.length} record${records.length === 1 ? '' : 's'} loaded.`;
      }
      if (error) {
        setConnectionStatus('Connection failed', 'error');
      } else {
        setConnectionStatus(`Connected · ${tableName}`, 'success');
      }
    };

    const loadColumnMetadata = async () => {
      if (!supabaseClient) return;
      const rawTable = (tableName || 'PT-LastPing').replace(/"/g, '');
      const tableKey = rawTable.includes('.') ? rawTable.split('.').pop() : rawTable;
      try {
        await ensureSupabaseSession?.();
        const { data, error } = await runWithTimeout(
          supabaseClient
            .from('information_schema.columns')
            .select('column_name,ordinal_position')
            .eq('table_name', tableKey)
            .order('ordinal_position', { ascending: true }),
          timeoutMs,
          'GPS history column request timed out.'
        );
        if (error) throw error;
        availableColumnKeys = (data || [])
          .map((row) => row?.column_name)
          .filter(Boolean);
        if (availableColumnKeys.length) {
          buildColumnsFromKeys(availableColumnKeys);
          renderColumnsList();
          renderTableHead();
        }
      } catch (error) {
        console.warn('Failed to load GPS history columns:', error);
      }
    };

    loadPreferences();
    if (statusText) statusText.textContent = 'Loading GPS history...';
    loadColumnMetadata();
    if (!VIN && !vehicleId) {
      renderHistory([]);
      if (statusText) statusText.textContent = 'No VIN or vehicle ID available for this vehicle.';
      setConnectionStatus('Vehicle missing', 'warning');
    } else if (!supabaseClient) {
      renderHistory([]);
      if (statusText) statusText.textContent = 'Supabase connection not available.';
      setConnectionStatus('Disconnected', 'error');
    } else if (Array.isArray(preloadedRecords)) {
      finalizeRender(preloadedRecords, preloadedError);
    } else {
      setConnectionStatus('Connecting…', 'warning');
      fetchGpsHistory({ vin: VIN, vehicleId }).then(({ records, error }) => {
        finalizeRender(records, error);
      });
    }

    if (columnsToggle && columnsPanel) {
      columnsToggle.addEventListener('click', () => {
        columnsPanel.classList.toggle('hidden');
      }, { signal });
    }

    if (columnsList) {
      columnsList.addEventListener('change', (event) => {
        const input = event.target;
        if (!input || input.tagName !== 'INPUT') return;
        if (input.type === 'checkbox') {
          columnVisibility[input.value] = input.checked;
          savePreferences();
          renderTableHead();
          renderHistory(gpsCache);
        }
      }, { signal });

      columnsList.addEventListener('input', (event) => {
        const input = event.target;
        if (!input || input.tagName !== 'INPUT' || input.type !== 'number') return;
        const key = input.dataset.gpsWidth;
        columnWidths[key] = input.value;
        savePreferences();
        renderTableHead();
        renderHistory(gpsCache);
      }, { signal });

      let draggedKey = null;
      columnsList.addEventListener('dragstart', (event) => {
        const row = event.target.closest('[data-gps-column]');
        if (!row) return;
        draggedKey = row.dataset.gpsColumn;
        row.classList.add('opacity-60');
        event.dataTransfer.effectAllowed = 'move';
        event.dataTransfer.setData('text/plain', draggedKey);
      }, { signal });

      columnsList.addEventListener('dragend', (event) => {
        const row = event.target.closest('[data-gps-column]');
        if (row) row.classList.remove('opacity-60');
        draggedKey = null;
      }, { signal });

      columnsList.addEventListener('dragover', (event) => {
        if (!draggedKey) return;
        event.preventDefault();
        event.dataTransfer.dropEffect = 'move';
      }, { signal });

      columnsList.addEventListener('drop', (event) => {
        event.preventDefault();
        const targetRow = event.target.closest('[data-gps-column]');
        const targetKey = targetRow?.dataset.gpsColumn;
        if (!draggedKey || !targetKey) return;
        updateColumnOrder(draggedKey, targetKey);
      }, { signal });
    }

    if (historyHead) {
      historyHead.addEventListener('click', (event) => {
        const button = event.target.closest('[data-gps-sort]');
        if (!button) return;
        const nextSortKey = button.dataset.gpsSort;
        if (!nextSortKey) return;
        if (sortKey === nextSortKey) {
          sortDirection = sortDirection === 'asc' ? 'desc' : 'asc';
        } else {
          sortKey = nextSortKey;
          sortDirection = 'asc';
        }
        renderHistory(gpsCache);
      }, { signal });
    }

    if (searchInput) {
      searchInput.addEventListener('input', (event) => {
        searchQuery = event.target.value || '';
        renderHistory(gpsCache);
      }, { signal });
    }
  };

  return {
    getVehicleId,
    getVehicleVin,
    fetchGpsHistory,
    setupGpsHistoryUI
  };
};

export { createGpsHistoryManager };
