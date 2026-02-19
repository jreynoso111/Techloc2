const getDefaultHelpers = () => ({
  escapeHTML: (value = '') => `${value}`,
  formatDateTime: (value) => value
});

const getVehicleVin = (vehicle) => {
  const vin = vehicle?.VIN ?? vehicle?.vin ?? vehicle?.details?.VIN ?? '';
  return typeof vin === 'string' ? vin.trim().toUpperCase() : '';
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
    const normalizedVin = typeof vin === 'string' ? vin.trim().toUpperCase() : '';
    const normalizedVehicleId = typeof vehicleId === 'string' ? vehicleId.trim() : '';
    if (!supabaseClient || (!normalizedVin && !normalizedVehicleId)) {
      return { records: [], error: supabaseClient ? new Error('VIN missing') : new Error('Supabase unavailable') };
    }
    try {
      await ensureSupabaseSession?.();
      const sourceTable = tableName || 'PT-LastPing';
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
    const widthColumnSelect = body.querySelector('[data-gps-width-column]');
    const widthValueInput = body.querySelector('[data-gps-width-value]');
    const widthApplyButton = body.querySelector('[data-gps-width-apply]');
    const widthAutoButton = body.querySelector('[data-gps-width-auto]');
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
    let activeResize = null;
    let activeHeaderDragKey = '';
    let suppressSortUntil = 0;
    let headerDragEnabledBeforeResize = [];

    const COLUMN_STORAGE_KEY = 'gpsHistoryColumnPrefs';
    const WIDTH_STORAGE_KEY = 'gpsHistoryColumnWidths';
    const MIN_COLUMN_WIDTH = 80;
    const MAX_COLUMN_WIDTH = 1200;

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

    const parseWidthValue = (value) => {
      const parsed = Number.parseInt(`${value || ''}`.trim(), 10);
      if (!Number.isFinite(parsed)) return '';
      return String(Math.min(MAX_COLUMN_WIDTH, Math.max(MIN_COLUMN_WIDTH, parsed)));
    };

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
      if (normalizedKey === 'moved') {
        const numeric = Number(value);
        if (numeric === 1) return 'Moving';
        if (numeric === -1) return 'Parked';
        if (numeric === 0) return '';
      }
      if (key === 'PT-LastPing' || normalizedKey.includes('date') || normalizedKey.includes('time')) {
        return safeEscape(`${safeFormatDateTime(value)}`);
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
              <th class="py-2 pr-3 align-bottom gps-history-th-resizable" data-gps-col-header="${col.key}" draggable="true" ${widthStyle}>
                <button type="button" class="group inline-flex items-center gap-1 text-left text-[10px] uppercase tracking-[0.08em] text-slate-400 hover:text-slate-200 transition-colors" data-gps-sort="${col.key}">
                  <span>${safeEscape(col.label)}</span>
                  <span class="text-[9px] text-slate-500 group-hover:text-slate-300">${sortKey === col.key ? (sortDirection === 'asc' ? '▲' : '▼') : ''}</span>
                </button>
                <button type="button" class="gps-history-resize-handle" data-gps-resize="${col.key}" aria-label="Resize ${safeEscape(col.label)} column"></button>
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
            const rawValue = record?.[col.key];
            const tooltip = rawValue === null || rawValue === undefined || rawValue === ''
              ? ''
              : safeEscape(typeof rawValue === 'object' ? JSON.stringify(rawValue) : `${rawValue}`);
            return `<td class="py-1.5 pr-3 text-slate-300 align-top" ${widthStyle}><div class="gps-history-cell-clamp" title="${tooltip}">${formatValue(col.key, rawValue)}</div></td>`;
          }).join('')}
        </tr>
      `).join('');
    };

    const renderColumnsList = () => {
      if (!columnsList) return;
      if (!gpsColumns.length) {
        columnsList.innerHTML = '<p class="text-xs text-slate-400">No columns available.</p>';
        syncWidthControls();
        return;
      }
      columnsList.innerHTML = columnOrder.map((key) => {
        const column = gpsColumns.find((col) => col.key === key);
        if (!column) return '';
        return `
          <div class="flex items-center gap-2 rounded-md border border-slate-800 bg-slate-950/70 px-2 py-1.5" draggable="true" data-gps-column="${column.key}">
            <span class="text-slate-500">⋮⋮</span>
            <label class="flex items-center gap-2 flex-1">
              <input type="checkbox" value="${column.key}" class="h-3.5 w-3.5 rounded border-slate-600 bg-slate-950" ${columnVisibility[column.key] ? 'checked' : ''} />
              <span class="text-slate-200">${safeEscape(column.label)}</span>
            </label>
          </div>
        `;
      }).join('');
      syncWidthControls();
    };

    const syncWidthControls = () => {
      if (!widthColumnSelect || !widthValueInput || !widthApplyButton || !widthAutoButton) return;

      if (!gpsColumns.length) {
        widthColumnSelect.innerHTML = '<option value="">No columns</option>';
        widthColumnSelect.disabled = true;
        widthValueInput.disabled = true;
        widthApplyButton.disabled = true;
        widthAutoButton.disabled = true;
        widthValueInput.value = '';
        return;
      }

      const previousKey = widthColumnSelect.value;
      widthColumnSelect.innerHTML = columnOrder.map((key) => {
        const column = gpsColumns.find((col) => col.key === key);
        if (!column) return '';
        return `<option value="${column.key}">${safeEscape(column.label)}</option>`;
      }).join('');

      const selectedKey = (previousKey && columnOrder.includes(previousKey))
        ? previousKey
        : (columnOrder[0] || gpsColumns[0]?.key || '');

      widthColumnSelect.value = selectedKey;
      widthColumnSelect.disabled = false;
      widthValueInput.disabled = false;
      widthApplyButton.disabled = false;
      widthAutoButton.disabled = false;
      widthValueInput.value = selectedKey ? (columnWidths[selectedKey] || '') : '';
    };

    const applySelectedWidth = () => {
      const key = widthColumnSelect?.value || '';
      if (!key) return;
      const normalized = parseWidthValue(widthValueInput?.value);
      if (widthValueInput) widthValueInput.value = normalized;
      columnWidths[key] = normalized;
      savePreferences();
      renderTableHead();
      renderHistory(gpsCache);
    };

    const resetSelectedWidth = () => {
      const key = widthColumnSelect?.value || '';
      if (!key) return;
      columnWidths[key] = '';
      if (widthValueInput) widthValueInput.value = '';
      savePreferences();
      renderTableHead();
      renderHistory(gpsCache);
    };

    const findHeaderCellByKey = (key) => {
      if (!historyHead || !key) return null;
      const headers = historyHead.querySelectorAll('[data-gps-col-header]');
      return [...headers].find((header) => header.dataset.gpsColHeader === key) || null;
    };

    const getVisibleColumnIndex = (key) => getVisibleColumns().findIndex((col) => col.key === key);

    const applyColumnWidthToRenderedTable = (key, widthPx) => {
      const widthStyleValue = widthPx ? `${widthPx}px` : '';
      const headerCell = findHeaderCellByKey(key);
      if (headerCell) {
        headerCell.style.width = widthStyleValue;
        headerCell.style.minWidth = widthStyleValue;
      }
      if (!historyBody) return;
      const columnIndex = getVisibleColumnIndex(key);
      if (columnIndex < 0) return;
      const rows = historyBody.querySelectorAll('tr');
      rows.forEach((row) => {
        const cell = row.children[columnIndex];
        if (!cell) return;
        cell.style.width = widthStyleValue;
        cell.style.minWidth = widthStyleValue;
      });
    };

    const stopColumnResize = () => {
      if (!activeResize) return;
      if (activeResize.target?.releasePointerCapture && activeResize.pointerId !== undefined) {
        try {
          activeResize.target.releasePointerCapture(activeResize.pointerId);
        } catch (_error) {
          // Ignore browsers that throw if capture was not set.
        }
      }
      activeResize = null;
      window.removeEventListener('pointermove', handleColumnResizeMove);
      window.removeEventListener('pointerup', handleColumnResizeEnd);
      document.body.classList.remove('gps-column-resizing');
      if (historyHead) {
        historyHead.style.userSelect = '';
        headerDragEnabledBeforeResize.forEach(({ node, draggable }) => {
          if (!node) return;
          node.setAttribute('draggable', draggable ? 'true' : 'false');
        });
      }
      headerDragEnabledBeforeResize = [];
      savePreferences();
      renderTableHead();
      renderHistory(gpsCache);
    };

    const handleColumnResizeMove = (event) => {
      if (!activeResize) return;
      if (activeResize.pointerId !== undefined && event.pointerId !== activeResize.pointerId) return;
      event.preventDefault();
      const delta = event.clientX - activeResize.startX;
      const nextWidth = Math.max(
        MIN_COLUMN_WIDTH,
        Math.min(MAX_COLUMN_WIDTH, Math.round(activeResize.startWidth + delta))
      );
      if (nextWidth === activeResize.currentWidth) return;
      activeResize.currentWidth = nextWidth;
      columnWidths[activeResize.key] = String(nextWidth);
      if (widthColumnSelect?.value === activeResize.key && widthValueInput) {
        widthValueInput.value = String(nextWidth);
      }
      applyColumnWidthToRenderedTable(activeResize.key, nextWidth);
    };

    const handleColumnResizeEnd = () => {
      stopColumnResize();
    };

    const startColumnResize = (event, key, headerCell) => {
      if (!key || !headerCell) return;
      if (event.button !== 0) return;
      const presetWidth = Number.parseInt(`${columnWidths[key] || ''}`, 10);
      const measuredWidth = Math.round(headerCell.getBoundingClientRect().width || MIN_COLUMN_WIDTH);
      const startWidth = Number.isFinite(presetWidth) ? presetWidth : measuredWidth;
      headerDragEnabledBeforeResize = historyHead
        ? [...historyHead.querySelectorAll('[data-gps-col-header]')].map((node) => ({
          node,
          draggable: node.getAttribute('draggable') === 'true'
        }))
        : [];
      headerDragEnabledBeforeResize.forEach(({ node }) => node.setAttribute('draggable', 'false'));
      if (historyHead) {
        historyHead.style.userSelect = 'none';
      }
      if (event.target?.setPointerCapture && event.pointerId !== undefined) {
        try {
          event.target.setPointerCapture(event.pointerId);
        } catch (_error) {
          // Ignore browsers that do not allow capture on this target.
        }
      }
      activeResize = {
        key,
        startX: event.clientX,
        startWidth: Math.max(MIN_COLUMN_WIDTH, Math.min(MAX_COLUMN_WIDTH, startWidth)),
        currentWidth: Math.max(MIN_COLUMN_WIDTH, Math.min(MAX_COLUMN_WIDTH, startWidth)),
        pointerId: event.pointerId,
        target: event.target
      };
      document.body.classList.add('gps-column-resizing');
      window.addEventListener('pointermove', handleColumnResizeMove);
      window.addEventListener('pointerup', handleColumnResizeEnd);
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

    const clearHeaderDropTargets = () => {
      if (!historyHead) return;
      historyHead
        .querySelectorAll('.gps-history-header-drop-target')
        .forEach((node) => node.classList.remove('gps-history-header-drop-target'));
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
      const sourceTable = tableName || 'PT-LastPing';
      try {
        await ensureSupabaseSession?.();
        const sampleQuery = supabaseClient
          .from(sourceTable)
          .select('*')
          .limit(1);
        const { data, error } = runWithTimeout
          ? await runWithTimeout(
            sampleQuery,
            timeoutMs,
            'GPS history column request timed out.'
          )
          : await sampleQuery;
        if (error) throw error;
        const firstRow = Array.isArray(data) ? data[0] : null;
        availableColumnKeys = firstRow ? Object.keys(firstRow) : [];
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

    if (widthColumnSelect && widthValueInput && widthApplyButton && widthAutoButton) {
      widthColumnSelect.addEventListener('change', () => {
        const key = widthColumnSelect.value || '';
        widthValueInput.value = key ? (columnWidths[key] || '') : '';
      }, { signal });

      widthApplyButton.addEventListener('click', () => {
        applySelectedWidth();
      }, { signal });

      widthAutoButton.addEventListener('click', () => {
        resetSelectedWidth();
      }, { signal });

      widthValueInput.addEventListener('keydown', (event) => {
        if (event.key !== 'Enter') return;
        event.preventDefault();
        applySelectedWidth();
      }, { signal });
    }

    if (historyHead) {
      historyHead.addEventListener('dragstart', (event) => {
        if (activeResize) {
          event.preventDefault();
          return;
        }
        const header = event.target.closest('[data-gps-col-header]');
        if (!header) return;
        if (event.target.closest('[data-gps-resize]')) {
          event.preventDefault();
          return;
        }
        const key = header.dataset.gpsColHeader || '';
        if (!key) return;
        activeHeaderDragKey = key;
        header.classList.add('opacity-60');
        if (event.dataTransfer) {
          event.dataTransfer.effectAllowed = 'move';
          event.dataTransfer.setData('text/plain', key);
        }
      }, { signal });

      historyHead.addEventListener('dragend', () => {
        activeHeaderDragKey = '';
        clearHeaderDropTargets();
        historyHead
          .querySelectorAll('[data-gps-col-header].opacity-60')
          .forEach((node) => node.classList.remove('opacity-60'));
      }, { signal });

      historyHead.addEventListener('dragover', (event) => {
        if (!activeHeaderDragKey) return;
        const target = event.target.closest('[data-gps-col-header]');
        if (!target) return;
        event.preventDefault();
        clearHeaderDropTargets();
        if (target.dataset.gpsColHeader !== activeHeaderDragKey) {
          target.classList.add('gps-history-header-drop-target');
        }
      }, { signal });

      historyHead.addEventListener('drop', (event) => {
        if (!activeHeaderDragKey) return;
        const target = event.target.closest('[data-gps-col-header]');
        if (!target) return;
        event.preventDefault();
        const targetKey = target.dataset.gpsColHeader || '';
        if (targetKey && targetKey !== activeHeaderDragKey) {
          updateColumnOrder(activeHeaderDragKey, targetKey);
          suppressSortUntil = Date.now() + 250;
        }
        clearHeaderDropTargets();
      }, { signal });

      historyHead.addEventListener('pointerdown', (event) => {
        const handle = event.target.closest('[data-gps-resize]');
        if (handle) {
          event.preventDefault();
          event.stopPropagation();
          const key = handle.dataset.gpsResize;
          const headerCell = handle.closest('[data-gps-col-header]');
          startColumnResize(event, key, headerCell);
          return;
        }

        const headerCell = event.target.closest('[data-gps-col-header]');
        if (!headerCell) return;
        const rect = headerCell.getBoundingClientRect();
        const nearRightEdge = (rect.right - event.clientX) <= 12;
        if (!nearRightEdge) return;
        event.preventDefault();
        event.stopPropagation();
        const key = headerCell.dataset.gpsColHeader || '';
        startColumnResize(event, key, headerCell);
      }, { signal });

      historyHead.addEventListener('click', (event) => {
        if (Date.now() < suppressSortUntil) return;
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

    signal?.addEventListener('abort', () => {
      stopColumnResize();
    }, { once: true });

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
