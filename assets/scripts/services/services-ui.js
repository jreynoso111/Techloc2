const renderHeader = ({
  els,
  getVisibleOrderedColumns,
  buildHeaderCell,
  state,
  savePrefs,
  renderTable,
  toggleSort,
  renameColumn,
  lucide,
}) => {
  const cols = getVisibleOrderedColumns();
  const tr = document.createElement('tr');
  const fragment = document.createDocumentFragment();

  cols.forEach((colId) => tr.appendChild(buildHeaderCell(colId)));
  fragment.appendChild(tr);
  els.thead.replaceChildren(fragment);

  els.thead.querySelectorAll('th[data-col]').forEach((th) => {
    const colId = th.getAttribute('data-col');
    const width = state.columnWidths[colId];
    if (!width) return;
    th.style.width = `${width}px`;
    th.style.minWidth = `${width}px`;
  });

  els.thead.querySelectorAll('th[draggable="true"]').forEach((th) => {
    th.addEventListener('dragstart', (e) => {
      if (state.drag.resizing) {
        e.preventDefault();
        return;
      }
      const colId = th.dataset.col;
      state.drag.draggingCol = colId;
      th.classList.add('dragging');
      e.dataTransfer.effectAllowed = 'move';
      e.dataTransfer.setData('text/plain', colId);
    });

    th.addEventListener('dragend', () => {
      th.classList.remove('dragging');
      state.drag.draggingCol = null;
      els.thead.querySelectorAll('th').forEach(x => x.classList.remove('drop-target'));
    });

    th.addEventListener('dragover', (e) => {
      e.preventDefault();
      const targetCol = th.dataset.col;
      if (!state.drag.draggingCol || state.drag.draggingCol === targetCol) return;
      th.classList.add('drop-target');
    });

    th.addEventListener('dragleave', () => th.classList.remove('drop-target'));

    th.addEventListener('drop', (e) => {
      e.preventDefault();
      const from = state.drag.draggingCol;
      const to = th.dataset.col;
      if (!from || !to || from === to) return;
      const order = [...state.columnOrder];
      const fromIdx = order.indexOf(from);
      const toIdx = order.indexOf(to);
      if (fromIdx === -1 || toIdx === -1) return;
      order.splice(fromIdx, 1);
      order.splice(toIdx, 0, from);
      state.columnOrder = order;
      savePrefs();
      renderTable();
    });
  });

  els.thead.querySelectorAll('[data-sort-col]').forEach((btn) => {
    btn.addEventListener('click', (e) => {
      e.stopPropagation();
      toggleSort(btn.getAttribute('data-sort-col'));
    });
  });

  els.thead.querySelectorAll('[data-rename-col]').forEach((el) => {
    el.addEventListener('dblclick', (e) => {
      e.stopPropagation();
      const colId = el.getAttribute('data-rename-col');
      renameColumn(colId);
    });
  });

  lucide.createIcons();
};

const renderBody = ({
  els,
  getVisibleOrderedColumns,
  state,
  ALL_COLUMNS,
  boolToLabel,
  duplicateRow,
  deleteRow,
  lucide,
  pageRows,
}) => {
  const cols = getVisibleOrderedColumns();
  const isAdmin = state.currentUserRole === 'administrator';
  const fragment = document.createDocumentFragment();

  pageRows.forEach((row) => {
    const tr = document.createElement('tr');
    tr.className = 'transition hover:bg-slate-800/40';

    cols.forEach((colId) => {
      const td = document.createElement('td');
      td.className = 'align-top text-slate-200 compact-td';
      if (state.columnWidths[colId]) {
        td.style.width = `${state.columnWidths[colId]}px`;
        td.style.minWidth = `${state.columnWidths[colId]}px`;
      }

      if (colId === 'actions') {
        td.className = 'text-center align-top compact-td';

        if (isAdmin) {
          const actions = document.createElement('div');
          actions.className = 'inline-flex items-center justify-center gap-2';

          const duplicateBtn = document.createElement('button');
          duplicateBtn.type = 'button';
          duplicateBtn.className = 'text-blue-300 transition hover:text-blue-200 hover:scale-110';
          duplicateBtn.innerHTML = '<i data-lucide="copy" class="h-4 w-4"></i>';
          duplicateBtn.onclick = () => duplicateRow(row);
          actions.appendChild(duplicateBtn);

          const deleteBtn = document.createElement('button');
          deleteBtn.type = 'button';
          deleteBtn.className = 'text-red-400 transition hover:text-red-300 hover:scale-110';
          deleteBtn.innerHTML = '<i data-lucide="trash-2" class="h-4 w-4"></i>';
          deleteBtn.onclick = () => deleteRow(row);
          actions.appendChild(deleteBtn);

          td.appendChild(actions);
        } else {
          td.innerHTML = '<span class="text-slate-600 text-xs">Read Only</span>';
        }

        tr.appendChild(td);
        return;
      }

      const col = ALL_COLUMNS.find(c => c.id === colId);
      let value = col ? row[col.key] : '';

      if (colId === 'verified') value = boolToLabel(row.verified);

      if (colId === 'company') td.className = 'font-semibold text-white align-top compact-td';

      td.textContent = (value === '' || value === null || value === undefined) ? '—' : String(value);
      td.dataset.rowId = row.id;
      td.dataset.colId = colId;
      td.dataset.editable = (isAdmin && colId !== 'actions') ? 'true' : 'false';

      tr.appendChild(td);
    });

    fragment.appendChild(tr);
  });

  els.tbody.replaceChildren(fragment);
  lucide.createIcons();
};

const renderTable = ({
  applyFilters,
  applySort,
  renderPagination,
  renderColgroup,
  renderHeader,
  renderBody,
  renderColumnsPopover,
  syncTopScrollbar,
  renderCharts,
  hasActiveFilters,
  setServiceFilterIds,
  lucide,
}) => {
  const filtered = applyFilters();
  const sorted = applySort(filtered);
  const { start, end } = renderPagination(sorted);

  renderColgroup();
  renderHeader();
  renderBody(sorted.slice(start, end));
  renderColumnsPopover();
  syncTopScrollbar();
  renderCharts(sorted);

  if (hasActiveFilters()) {
    setServiceFilterIds('tech', filtered.map((row) => row.id));
  } else {
    setServiceFilterIds('tech', null);
  }

  lucide.createIcons();
};

export { renderBody, renderHeader, renderTable };
