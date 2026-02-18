import {
  DashboardState,
  DEAL_STATUS_COLORS,
  DEAL_STATUS_COLORS_ALT,
  DEFAULT_SEGMENT_KEY,
  MILEAGE_COLUMNS,
  CURRENCY_COLUMNS,
  formatDate,
  formatNumber,
  formatMileage,
  formatCurrency,
  getColumnLabel,
  getInvPrepStatusValue,
  getInvPrepStatusRowClass,
  formatInvPrepStatusLabel,
  detectInvPrepStatusKey,
  getUniqueValues,
} from '../core/state.js';

export const initDashboardUI = ({
  applyFilters,
  getCurrentDataset,
  getSegmentOptions,
  getSegmentLabel,
  ensureChartVisibilityState,
  setChartHiddenValues,
  getOrderedColumns,
  getVehicleKey,
  PILL_CLASSES,
  syncTopScrollbar,
  createIcons,
}) => {
  const renderSegmentFieldOptions = (chartId, segmentKey, segmentValues) => {
    const panel = document.querySelector(`[data-segment-filter-panel][data-chart-id="${chartId}"]`);
    const optionsContainer = document.querySelector(`[data-segment-filter-options][data-chart-id="${chartId}"]`);
    const summary = document.querySelector(`[data-segment-filter-count][data-chart-id="${chartId}"]`);
    if (!panel || !optionsContainer || !summary) return;

    panel.dataset.segmentKey = segmentKey;
    if (!DashboardState.chartVisibilityOptions[chartId]) DashboardState.chartVisibilityOptions[chartId] = {};
    DashboardState.chartVisibilityOptions[chartId][segmentKey] = segmentValues;

    const hiddenValues = ensureChartVisibilityState(chartId, segmentKey);
    const hiddenSet = new Set(hiddenValues);
    const hiddenVisibleCount = segmentValues.reduce((count, value) => count + (hiddenSet.has(value) ? 1 : 0), 0);
    setChartHiddenValues(chartId, segmentKey, hiddenValues.slice());

    const visibleCount = Math.max(0, segmentValues.length - hiddenVisibleCount);
    summary.textContent = segmentValues.length ? `${visibleCount}/${segmentValues.length}` : '0';

    if (!segmentValues.length) {
      optionsContainer.innerHTML = '<p class="text-[11px] text-slate-400">No fields available.</p>';
      return;
    }

    optionsContainer.innerHTML = segmentValues.map((value) => {
      const safeValue = String(value);
      const safeValueAttr = safeValue.replace(/"/g, '&quot;');
      const isChecked = !hiddenValues.includes(safeValue);
      return `
        <label class="flex items-center gap-2 rounded-xl border border-slate-800 bg-slate-950/60 px-2 py-1 text-[11px] font-semibold text-slate-200">
          <input type="checkbox" class="h-3.5 w-3.5 rounded border-slate-700 bg-slate-950 text-blue-400" data-segment-field-checkbox data-chart-id="${chartId}" data-segment-key="${segmentKey}" value="${safeValueAttr}" ${isChecked ? 'checked' : ''} />
          <span class="truncate">${safeValue}</span>
        </label>
      `;
    }).join('');
  };

  const renderUnitTypeFilters = () => {
    const container = document.getElementById('unit-type-filters');
    if (!container) return;
    const unitTypeKey = DashboardState.filters.unitTypeKey;
    if (!unitTypeKey) {
      container.classList.add('hidden');
      container.innerHTML = '';
      return;
    }
    const values = getUniqueValues(getCurrentDataset(), unitTypeKey);
    const selections = Array.isArray(DashboardState.filters.unitTypeSelection)
      ? DashboardState.filters.unitTypeSelection.filter((value) => values.includes(value))
      : [];
    DashboardState.filters.unitTypeSelection = selections;
    if (!values.length) {
      container.classList.add('hidden');
      container.innerHTML = '';
      return;
    }
    container.classList.remove('hidden');
    const optionHtml = values.map((value) => `
      <label class="flex items-center gap-2 text-xs">
        <input type="checkbox" value="${value}" class="h-3.5 w-3.5 rounded border-slate-600 bg-slate-950" ${selections.includes(value) ? 'checked' : ''} />
        <span>${value}</span>
      </label>
    `).join('');
    const count = selections.length;
    const labelText = count === 1 ? selections[0] : 'Unit Type';
    container.innerHTML = `
      <div class="relative">
        <button id="unit-type-toggle" type="button" class="flex items-center gap-2 rounded-2xl border border-slate-700 bg-slate-950/60 px-3 py-2 text-[10px] font-semibold uppercase tracking-[0.2em] text-slate-200" aria-expanded="false">
          <span id="unit-type-label">${labelText}</span>
          <span id="unit-type-summary" class="rounded-full border border-slate-700 bg-slate-950/80 px-2 py-0.5 text-[10px] font-semibold text-slate-300">${count}</span>
        </button>
        <div id="unit-type-panel" class="absolute left-1/2 z-40 mt-2 hidden w-56 max-w-[calc(100vw-2rem)] -translate-x-1/2 rounded-2xl border border-slate-800 bg-slate-950/95 p-3 text-xs text-slate-200 shadow-lg">
          <p class="text-[10px] font-semibold uppercase tracking-[0.3em] text-slate-400">Choose unit types</p>
          <div id="unit-type-options" class="mt-2 grid gap-2">
            ${optionHtml}
          </div>
        </div>
      </div>
    `;
  };

  const renderVehicleStatusFilters = () => {
    const container = document.getElementById('vehicle-status-filters');
    if (!container) return;
    const vehicleStatusKey = DashboardState.filters.vehicleStatusKey;
    if (!vehicleStatusKey) {
      container.classList.add('hidden');
      container.innerHTML = '';
      return;
    }
    const values = getUniqueValues(getCurrentDataset(), vehicleStatusKey);
    const selections = Array.isArray(DashboardState.filters.vehicleStatusSelection)
      ? DashboardState.filters.vehicleStatusSelection.filter((value) => values.includes(value))
      : [];
    DashboardState.filters.vehicleStatusSelection = selections;
    if (!values.length) {
      container.classList.add('hidden');
      container.innerHTML = '';
      return;
    }
    container.classList.remove('hidden');
    const optionHtml = values.map((value) => `
      <label class="flex items-center gap-2 text-xs">
        <input type="checkbox" value="${value}" class="h-3.5 w-3.5 rounded border-slate-600 bg-slate-950" ${selections.includes(value) ? 'checked' : ''} />
        <span>${value}</span>
      </label>
    `).join('');
    const count = selections.length;
    const labelText = count === 1 ? selections[0] : 'Vehicle Status';
    container.innerHTML = `
      <div class="relative">
        <button id="vehicle-status-toggle" type="button" class="flex items-center gap-2 rounded-2xl border border-slate-700 bg-slate-950/60 px-3 py-2 text-[10px] font-semibold uppercase tracking-[0.2em] text-slate-200" aria-expanded="false">
          <span id="vehicle-status-label">${labelText}</span>
          <span id="vehicle-status-summary" class="rounded-full border border-slate-700 bg-slate-950/80 px-2 py-0.5 text-[10px] font-semibold text-slate-300">${count}</span>
        </button>
        <div id="vehicle-status-panel" class="absolute left-1/2 z-40 mt-2 hidden w-56 max-w-[calc(100vw-2rem)] -translate-x-1/2 rounded-2xl border border-slate-800 bg-slate-950/95 p-3 text-xs text-slate-200 shadow-lg">
          <p class="text-[10px] font-semibold uppercase tracking-[0.3em] text-slate-400">Choose vehicle status</p>
          <div id="vehicle-status-options" class="mt-2 grid gap-2">
            ${optionHtml}
          </div>
        </div>
      </div>
    `;
  };

  const renderSegmentOptions = () => {
    const selects = document.querySelectorAll('[data-segment-select]');
    if (!selects.length) return;
    const options = getSegmentOptions();
    const fallbackKey = options[0]?.key || DEFAULT_SEGMENT_KEY;
    const optionsHtml = options
      .map((opt) => `<option value="${opt.key}">${opt.label}</option>`)
      .join('');
    selects.forEach((select) => {
      const chartId = select.dataset.chartId || 'default';
      const storedValue = DashboardState.chartSegments[chartId] || DashboardState.chartSegments.default;
      const nextValue = options.some((opt) => opt.key === storedValue) ? storedValue : fallbackKey;
      DashboardState.chartSegments[chartId] = nextValue;
      select.innerHTML = optionsHtml;
      select.value = nextValue;
    });
  };

  const computeDerivedState = () => {
    const filtered = applyFilters();
    const uniqueRecords = Array.from(
      filtered.reduce((acc, item) => {
        const key = item.stockNo || item.vin || item.id;
        if (!key) return acc;
        if (!acc.has(key)) acc.set(key, item);
        return acc;
      }, new Map()).values()
    );
    const activeCount = uniqueRecords.filter((i) => i.status === 'Active').length;
    const holdCount = uniqueRecords.filter((i) => i.hold || i.status === 'On Hold').length;

    DashboardState.derived = {
      filtered,
      kpis: { active: activeCount, hold: holdCount },
    };
  };

  const renderKpis = () => {
    const { kpis } = DashboardState.derived;
    const isLoading = DashboardState.ui.isLoading;
    const active = document.querySelector('[data-metric="active"]');
    const hold = document.querySelector('[data-metric="hold"]');
    [active, hold].forEach((el) => el.classList.toggle('animate-pulse', isLoading));

    if (isLoading) {
      active.textContent = hold.textContent = '...';
      return;
    }
    active.textContent = formatNumber(kpis.active);
    hold.textContent = formatNumber(kpis.hold);
  };

  const renderStatusBars = () => {
    const containers = document.querySelectorAll('[data-bar-chart]');
    renderSegmentOptions();
    containers.forEach((container) => {
      const chartId = container.dataset.chartId || 'default';
      const segmentKey = DashboardState.chartSegments[chartId] || DashboardState.chartSegments.default || DEFAULT_SEGMENT_KEY;
      const palette = chartId === 'status-secondary' ? DEAL_STATUS_COLORS_ALT : DEAL_STATUS_COLORS;
      if (DashboardState.ui.isLoading) {
        renderSegmentFieldOptions(chartId, segmentKey, []);
        container.innerHTML = Array.from({ length: 6 }).map(() => `
          <div class="flex h-full flex-col items-center justify-end">
            <span class="mb-1 text-[11px] font-semibold text-slate-500">...</span>
            <div class="h-full min-h-[140px] w-8 rounded-full bg-slate-800/70 overflow-hidden">
              <div class="h-full w-full animate-pulse bg-slate-700/70"></div>
            </div>
            <span class="mt-2 text-[10px] uppercase tracking-[0.2em] text-slate-500">...</span>
          </div>
        `).join('');
        return;
      }

      const dataset = DashboardState.derived.filtered;
      const segmentDataset = applyFilters({ ignoreChartId: chartId });
      const uniqueRecords = Array.from(
        dataset.reduce((acc, item) => {
          const key = item.stockNo || item.vin || item.id;
          if (!key) return acc;
          if (!acc.has(key)) acc.set(key, item);
          return acc;
        }, new Map()).values()
      );
      const segmentRecords = Array.from(
        segmentDataset.reduce((acc, item) => {
          const key = item.stockNo || item.vin || item.id;
          if (!key) return acc;
          if (!acc.has(key)) acc.set(key, item);
          return acc;
        }, new Map()).values()
      );
      const dealStatusCounts = uniqueRecords.reduce((acc, item) => {
        const key = getSegmentLabel(item[segmentKey], segmentKey);
        acc[key] = (acc[key] || 0) + 1;
        return acc;
      }, {});
      const segmentValues = Array.from(
        new Set(segmentRecords.map((item) => getSegmentLabel(item[segmentKey], segmentKey)))
      ).sort((a, b) => a.localeCompare(b));
      renderSegmentFieldOptions(chartId, segmentKey, segmentValues);
      const hiddenValues = ensureChartVisibilityState(chartId, segmentKey);
      const hiddenSet = new Set(hiddenValues);
      const statusCounts = Object.entries(dealStatusCounts)
        .reduce((acc, [status, count]) => {
          acc[status] = count;
          return acc;
        }, {});
      const statusList = segmentValues
        .filter((status) => !hiddenSet.has(status))
        .map((status) => ({ status, count: statusCounts[status] || 0 }))
        .sort((a, b) => a.status.localeCompare(b.status));
      const maxCount = Math.max(1, ...statusList.map((i) => i.count));
      const statusBars = statusList.map((i, index) => ({
        ...i,
        percentage: Math.round((i.count / maxCount) * 100),
        color: palette[index % palette.length],
      }));

      if (!statusBars.length) {
        container.innerHTML = segmentValues.length
          ? '<p class="col-span-6 text-center text-xs text-slate-400">All fields are hidden.</p>'
          : '<p class="col-span-6 text-center text-xs text-slate-400">No vehicles match current filters.</p>';
        return;
      }

      container.innerHTML = statusBars.map((item) => {
        const activeFilter = DashboardState.filters.chartFilters?.[chartId];
        const activeValues = Array.isArray(activeFilter?.values) ? activeFilter.values : [];
        const isActive = activeFilter
          && activeFilter.key === segmentKey
          && activeValues.includes(item.status);
        const isDisabled = activeFilter && activeValues.length > 0 && !isActive;
        const activeClass = isActive ? 'ring-2 ring-blue-400/70 bg-slate-900/50' : '';
        const dimClass = isDisabled ? 'opacity-40 grayscale cursor-not-allowed' : '';
        const hoverClass = isDisabled ? '' : 'hover:bg-slate-900/60';
        const countClass = isDisabled ? 'text-slate-500' : 'text-slate-200';
        const labelClass = isDisabled ? 'text-slate-500' : 'text-slate-100';
        const disabledAttr = isDisabled ? 'disabled aria-disabled="true"' : '';
        return `
        <button type="button" data-status="${item.status}" data-segment-key="${segmentKey}" data-chart-id="${chartId}" class="group flex h-full flex-col items-center justify-end rounded-2xl px-1 py-1 transition ${hoverClass} ${activeClass} ${dimClass}" ${disabledAttr}>
          <span class="mb-1 text-[11px] font-semibold ${countClass}">${item.count}</span>
          <div class="flex h-full min-h-[140px] w-8 items-end rounded-full bg-slate-800/70">
            <div class="bar-fill w-full rounded-full bg-gradient-to-t ${item.color}" style="height: 0%;" data-height="${item.percentage}%"></div>
          </div>
          <span class="mt-2 text-[10px] uppercase tracking-[0.2em] transition duration-200 ease-out group-hover:scale-110 ${labelClass} chart-legend">${item.status}</span>
        </button>
      `;
      }).join('');
      requestAnimationFrame(() => {
        container.querySelectorAll('.bar-fill').forEach((bar) => {
          const targetHeight = bar.dataset.height;
          if (targetHeight) bar.style.height = targetHeight;
        });
      });
    });
  };

  const renderActiveFilters = () => {
    const chipsContainer = document.getElementById('active-filters');
    if (!chipsContainer) return;
    const { filters } = DashboardState;
    const chips = [];

    if (filters.dateRange.start) chips.push(`Date: From ${filters.dateRange.start}`);
    if (filters.salesChannelKey && filters.salesChannels.length) {
      chips.push(`${getColumnLabel(filters.salesChannelKey)}: ${filters.salesChannels.join(', ')}`);
    }
    const unitTypeSelections = Array.isArray(filters.unitTypeSelection) ? filters.unitTypeSelection : [];
    if (filters.unitTypeKey && unitTypeSelections.length) {
      chips.push(`${getColumnLabel(filters.unitTypeKey)}: ${unitTypeSelections.join(', ')}`);
    }
    const vehicleStatusSelections = Array.isArray(filters.vehicleStatusSelection)
      ? filters.vehicleStatusSelection
      : [];
    if (filters.vehicleStatusKey && vehicleStatusSelections.length) {
      chips.push(`${getColumnLabel(filters.vehicleStatusKey)}: ${vehicleStatusSelections.join(', ')}`);
    }
    Object.entries(filters.categoryFilters || {}).forEach(([key, value]) => {
      if (value !== 'all') chips.push(`${getColumnLabel(key)}: ${value}`);
    });
    Object.values(filters.chartFilters || {}).forEach((filter) => {
      const values = Array.isArray(filter.values) ? filter.values : [];
      if (!values.length) return;
      chips.push(`${getColumnLabel(filter.key)}: ${values.join(', ')}`);
    });

    chipsContainer.innerHTML = chips.length
      ? chips.map((c) => `<span class="${PILL_CLASSES} whitespace-nowrap text-slate-200">${c}</span>`).join('')
      : '<span class="text-slate-400">No active filters</span>';
  };

  const renderTableHead = () => {
    const head = document.getElementById('inventory-table-head');
    const visibleColumns = getOrderedColumns(DashboardState.schema)
      .filter((c) => DashboardState.table.columns[c.key]);
    const sortState = DashboardState.table.sort;
    const colgroup = document.getElementById('inventory-table-cols');
    const dataset = getCurrentDataset();

    if (!visibleColumns.length) {
      head.innerHTML = '<tr><th class="px-3 py-2 text-left text-[10px] uppercase tracking-[0.3em] text-slate-400">No columns</th></tr>';
      if (colgroup) colgroup.innerHTML = '';
      return;
    }

    if (colgroup) {
      colgroup.innerHTML = visibleColumns.map((c) => {
        const width = DashboardState.table.columnWidths[c.key];
        const style = typeof width === 'number' ? ` style="width: ${width}px"` : '';
        return `<col data-col-key="${c.key}"${style} />`;
      }).join('');
    }

    head.innerHTML = `
      <tr>${
        visibleColumns.map((c) => {
          const isSorted = sortState.key === c.key;
          const indicator = isSorted ? (sortState.direction === 'asc' ? '▲' : '▼') : '';
          const width = DashboardState.table.columnWidths[c.key];
          const style = typeof width === 'number' ? ` style="width: ${width}px"` : '';
          const values = getUniqueValues(dataset, c.key).slice(0, 50);
          const entry = DashboardState.filters.columnFilters[c.key] || { select: 'all', search: '' };
          DashboardState.filters.columnFilters[c.key] = entry;
          const headerLabel = c.label === 'Calc- End' ? 'Calc-<br>End' : c.label;
          return `
            <th class="relative px-3 py-2" data-col-key="${c.key}" draggable="true"${style}>
              <div class="flex flex-col gap-1">
                <div class="flex items-start justify-between gap-2 text-left text-[10px] uppercase tracking-[0.3em] text-slate-400">
                  <span class="whitespace-normal break-words">${headerLabel}</span>
                  <span class="text-[10px] text-blue-300">${indicator}</span>
                </div>
                <div class="flex items-center justify-end gap-1">
                  <button type="button" data-column-filter-toggle="${c.key}" class="inline-flex h-5 w-5 items-center justify-center rounded-full border border-slate-800 bg-slate-950/70 text-slate-300 transition hover:text-white" aria-label="Filter ${c.label}">
                    <i data-lucide="sliders-horizontal" class="h-3 w-3"></i>
                  </button>
                  <button type="button" data-sort="${c.key}" class="inline-flex h-5 w-5 items-center justify-center rounded-full border border-slate-800 bg-slate-950/70 text-slate-300 transition hover:text-white" aria-label="Sort by ${c.label}">
                    <i data-lucide="arrow-up-down" class="h-3 w-3"></i>
                  </button>
                </div>
              </div>
              <div class="column-filter-panel absolute left-0 top-full z-40 mt-2 hidden w-56 max-w-[calc(100vw-2rem)] rounded-2xl border border-slate-800 bg-slate-950/95 p-3 text-[10px] font-semibold text-slate-200 shadow-lg" data-column-filter-panel="${c.key}">
                <p class="text-[10px] uppercase tracking-[0.3em] text-slate-400">${c.label}</p>
                <div class="mt-2 grid gap-1">
                  <select data-column-filter="${c.key}" class="h-7 rounded-lg border border-slate-800 bg-slate-950/70 px-2 text-[10px] font-semibold text-slate-200">
                    <option value="all">All</option>
                    ${values.map((value) => `<option value="${value}" ${entry.select === value ? 'selected' : ''}>${value}</option>`).join('')}
                  </select>
                  <input type="search" data-column-search="${c.key}" value="${entry.search || ''}" placeholder="Search" class="h-7 rounded-lg border border-slate-800 bg-slate-950/70 px-2 text-[10px] font-semibold text-slate-200" />
                </div>
              </div>
              <span class="column-resizer" data-resize-handle="${c.key}" aria-hidden="true"></span>
            </th>
          `;
        }).join('')
      }</tr>
    `;
  };

  const renderTable = () => {
    const tableBody = document.getElementById('inventory-table');
    const visibleColumns = getOrderedColumns(DashboardState.schema)
      .filter((c) => DashboardState.table.columns[c.key]);
    const columnCount = Math.max(1, visibleColumns.length);
    const invPrepKey = detectInvPrepStatusKey(DashboardState.schema);

    if (DashboardState.ui.isLoading) {
      tableBody.innerHTML = Array.from({ length: DashboardState.table.perPage }).map(() => `
        <tr class="text-xs">
          <td class="px-3 py-3" colspan="${columnCount}">
            <div class="h-3 w-full rounded-full bg-slate-800/70 animate-pulse"></div>
          </td>
        </tr>
      `).join('');
      return;
    }

    if (!visibleColumns.length) {
      tableBody.innerHTML = `<tr><td class="px-3 py-6 text-center text-sm text-slate-400" colspan="${columnCount}">No columns available.</td></tr>`;
      return;
    }

    const sortedRows = [...DashboardState.derived.filtered].sort((a, b) => {
      const { key, direction } = DashboardState.table.sort;
      const column = DashboardState.schema.find((c) => c.key === key);
      if (!column) return 0;

      const getValue = (item) => item[key];

      const va = getValue(a);
      const vb = getValue(b);

      let comparison = 0;
      if (column.type === 'date') {
        const getDateValue = (value) => {
          if (value === null || value === undefined || value === '') return 0;
          if (value instanceof Date) {
            const time = value.getTime();
            return Number.isNaN(time) ? 0 : time;
          }
          if (typeof value === 'number') {
            return Number.isFinite(value) ? value : 0;
          }
          const parsed = Date.parse(String(value));
          return Number.isNaN(parsed) ? 0 : parsed;
        };
        const da = getDateValue(va);
        const db = getDateValue(vb);
        comparison = da - db;
      } else if (column.type === 'number') {
        comparison = (Number(va) || 0) - (Number(vb) || 0);
      } else if (column.type === 'boolean') {
        comparison = (va ? 1 : 0) - (vb ? 1 : 0);
      } else {
        comparison = String(va ?? '').localeCompare(String(vb ?? ''), undefined, { sensitivity: 'base' });
      }

      if (comparison === 0) return 0;
      return direction === 'asc' ? comparison : -comparison;
    });

    const totalRows = sortedRows.length;
    const totalPages = Math.max(1, Math.ceil(totalRows / DashboardState.table.perPage));
    const currentPage = Math.min(DashboardState.table.page, totalPages);
    DashboardState.table.page = currentPage;

    const startIndex = (currentPage - 1) * DashboardState.table.perPage;
    const pageItems = sortedRows.slice(startIndex, startIndex + DashboardState.table.perPage);
    const rangeStart = totalRows ? startIndex + 1 : 0;
    const rangeEnd = totalRows ? startIndex + pageItems.length : 0;

    const rowsHtml = pageItems.map((item) => {
      const prepStatus = getInvPrepStatusValue(item, invPrepKey);
      const prepClass = getInvPrepStatusRowClass(prepStatus);
      const cells = visibleColumns.map((col) => {
        let value = item[col.key];

        if (invPrepKey && col.key === invPrepKey) {
          value = formatInvPrepStatusLabel(prepStatus);
        } else if (col.type === 'boolean') value = value ? 'Yes' : 'No';
        else if (col.type === 'date') value = value ? formatDate(value) : '--';
        else if (MILEAGE_COLUMNS.has(col.key.toLowerCase()) || col.label?.toLowerCase() === 'mileage') {
          value = formatMileage(value);
        } else if (
          CURRENCY_COLUMNS.has(col.key.toLowerCase())
          || (col.label && CURRENCY_COLUMNS.has(col.label.toLowerCase()))
        ) {
          value = formatCurrency(value);
        }

        if (value === null || value === undefined || value === '') value = '--';

        const width = DashboardState.table.columnWidths[col.key];
        const style = typeof width === 'number' ? ` style="width: ${width}px"` : '';
        return `<td class="px-3 py-2 whitespace-normal break-words"${style}>${value}</td>`;
      }).join('');

      return `<tr class="text-xs ${prepClass} hover:bg-slate-900/60 cursor-pointer transition" data-row-key="${getVehicleKey(item)}">${cells}</tr>`;
    }).join('');

    tableBody.innerHTML = rowsHtml || `<tr><td class="px-3 py-6 text-center text-sm text-slate-400" colspan="${columnCount}">No vehicles match current filters.</td></tr>`;

    document.getElementById('table-summary').textContent = `${formatNumber(rangeStart)}–${formatNumber(rangeEnd)} of ${formatNumber(totalRows)}`;
    document.getElementById('table-page').textContent = `Page ${currentPage} of ${totalPages}`;
    document.getElementById('table-prev').disabled = currentPage <= 1;
    document.getElementById('table-next').disabled = currentPage >= totalPages;
    const goToInput = document.getElementById('table-go-to');
    if (goToInput) {
      goToInput.max = String(totalPages);
      goToInput.value = String(currentPage);
    }
    const perPageSelect = document.getElementById('table-per-page');
    if (perPageSelect) {
      perPageSelect.value = String(DashboardState.table.perPage);
    }
  };

  const renderDashboard = () => {
    renderUnitTypeFilters();
    renderVehicleStatusFilters();
    computeDerivedState();
    renderUnitTypeFilters();
    renderVehicleStatusFilters();
    renderKpis();
    renderStatusBars();
    renderTableHead();
    renderTable();
    renderActiveFilters();
    if (syncTopScrollbar) syncTopScrollbar();
    if (createIcons) createIcons();
    const loadingOverlay = document.getElementById('inventory-loading');
    if (loadingOverlay) {
      loadingOverlay.classList.toggle('hidden', !DashboardState.ui.isLoading);
    }
  };

  const renderColumnChooser = () => {
    const container = document.getElementById('column-chooser-options');
    if (!DashboardState.schema.length) {
      container.innerHTML = '<p class="text-xs text-slate-400">No columns available.</p>';
      return;
    }
    const sortedSchema = [...DashboardState.schema].sort((a, b) => a.label.localeCompare(b.label));
    container.innerHTML = sortedSchema.map((c) => `
      <label class="flex items-center gap-2 text-xs">
        <input type="checkbox" value="${c.key}" class="h-3.5 w-3.5 rounded border-slate-600 bg-slate-950" ${DashboardState.table.columns[c.key] ? 'checked' : ''} />
        <span class="flex-1">${c.label}</span>
        <button type="button" class="rounded-full border border-slate-700 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.2em] text-slate-300 transition hover:border-blue-500 hover:text-white" data-column-edit="${c.key}">
          Edit
        </button>
      </label>
    `).join('');
  };

  const openDrawer = (record) => {
    document.getElementById('drawer-vin').textContent = record.vin || '--';
    document.getElementById('drawer-status').textContent = record.status || '--';
    document.getElementById('drawer-gps-status').textContent = record.gpsStatus || '--';
    document.getElementById('drawer-gps-flag').textContent = record.gpsFlag ? `⚠️ ${record.gpsFlag}` : '—';
    const completionValue = record.completion;
    let completionText = '--';
    if (completionValue !== null && completionValue !== undefined && completionValue !== '') {
      const rawCompletion = String(completionValue).trim();
      completionText = rawCompletion ? (rawCompletion.endsWith('%') ? rawCompletion : `${rawCompletion}%`) : '--';
    }
    document.getElementById('drawer-completion').textContent = completionText;
    document.getElementById('drawer-location').textContent = `${record.yard || '--'}, ${record.state || '--'}`;
    document.getElementById('drawer-unit').textContent = `${record.brand || '--'} ${record.unitType || ''}`.trim() || '--';

    const flags = [
      record.gpsOffline ? 'GPS Offline' : null,
      record.hold ? 'Hold' : null,
      record.lien ? 'Lien' : null,
      record.recoveryPriority ? 'Recovery Priority' : null,
    ].filter(Boolean).join(' • ');

    document.getElementById('drawer-flags').textContent = flags || 'None';

    const timeline = [
      { label: 'Created', value: record.createdAt || record.date },
      { label: 'Last updated', value: record.updatedAt || record.date },
    ];

    document.getElementById('drawer-timeline').innerHTML = timeline.map((entry) => `
      <li class="flex items-center justify-between rounded-xl border border-slate-800 bg-slate-900/70 px-3 py-2">
        <span>${entry.label}</span>
        <span class="text-slate-200">${entry.value ? formatDate(entry.value) : '--'}</span>
      </li>
    `).join('');

    document.getElementById('row-drawer').classList.remove('translate-x-full');
  };

  const closeDrawer = () => document.getElementById('row-drawer').classList.add('translate-x-full');

  renderDashboard();

  return {
    renderDashboard,
    renderColumnChooser,
    openDrawer,
    closeDrawer,
    renderTable,
    renderTableHead,
    renderKpis,
    renderStatusBars,
    renderActiveFilters,
    renderSegmentOptions,
    renderSegmentFieldOptions,
    renderUnitTypeFilters,
    renderVehicleStatusFilters,
  };
};
