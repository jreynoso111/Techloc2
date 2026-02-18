const commitInlineEdit = async ({
  state,
  parseEditedValue,
  getRowById,
  ALL_COLUMNS,
  valueForDisplay,
  writeLocalRowValue,
  saveCellToSupabase,
  endInlineEdit,
  renderCharts,
  applySort,
  applyFilters,
  logChange,
  showToast,
}) => {
  const ses = state.inlineEdit;
  if (!ses.td || ses.saving) return;

  const td = ses.td;
  const rowId = ses.rowId;
  const colId = ses.colId;

  const input = ses.inputEl;
  if (!input) return;

  const raw = input.value;
  let newValue = parseEditedValue(colId, raw);

  if (colId === 'phone') {
    const digits = String(raw ?? '').replace(/\D/g, '');
    if (digits.length !== 10) {
      td.classList.add('failed');
      showToast('Phone numbers must include exactly 10 digits (e.g., 809-555-1234).', 'error');
      return;
    }
    newValue = `${digits.slice(0, 3)}-${digits.slice(3, 6)}-${digits.slice(6)}`;
  }

  if (colId === 'email') {
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (newValue && !emailRegex.test(newValue)) {
      td.classList.add('failed');
      showToast('Please enter a valid email address.', 'error');
      return;
    }
  }

  const row = getRowById(rowId);
  const col = ALL_COLUMNS.find(c => c.id === colId);
  const currentValue = colId === 'verified' ? (row?.verified ?? null) : (row?.[col?.key] ?? null);

  const same =
    (colId === 'verified')
      ? (String(currentValue ?? '').trim() === String(newValue ?? '').trim())
      : (String(currentValue ?? '') === String(newValue ?? ''));

  if (same) {
    td.innerHTML = '';
    td.textContent = valueForDisplay(colId, currentValue);
    endInlineEdit({ save: true });
    return;
  }

  try {
    if (rowId === null || rowId === undefined) {
      throw new Error('Cannot save cell: missing row id');
    }

    ses.saving = true;
    td.classList.remove('failed');
    td.classList.remove('saved');

    writeLocalRowValue(rowId, colId, newValue);
    td.innerHTML = '';
    td.textContent = valueForDisplay(colId, colId === 'verified' ? newValue : newValue);

    await saveCellToSupabase({ rowId, colId, newValue, td });

    td.classList.add('saved');
    setTimeout(() => td.classList.remove('saved'), 900);

    endInlineEdit({ save: true });

    renderCharts(applySort(applyFilters()));

    logChange({
      action: 'edit',
      summary: `Updated ${col?.label || colId} for ${row.company || 'Service'} (#${rowId})`,
      recordId: rowId,
      columnName: colId,
      previousValue: currentValue,
      newValue,
    });
  } catch (err) {
    console.error('Cell save failed:', err);

    writeLocalRowValue(rowId, colId, currentValue);

    td.classList.add('failed');
    td.innerHTML = '';
    td.textContent = valueForDisplay(colId, currentValue);

    endInlineEdit({ save: true });
    showToast('Error saving cell: ' + (err?.message || JSON.stringify(err)), 'error');
  } finally {
    ses.saving = false;
    td.classList.remove('saving');
  }
};

const startInlineEdit = ({
  td,
  state,
  endInlineEdit,
  getRowById,
  ALL_COLUMNS,
  valueForDisplay,
  selectColumns,
  getSelectOptions,
  addSelectOption,
  optionLabel,
  toBool,
  insertNewValue,
  commitInlineEdit,
  focusNextEditableCell,
}) => {
  const editable = td?.dataset?.editable === 'true';
  if (!editable) return;

  const rowId = td.dataset.rowId;
  const colId = td.dataset.colId;

  if (!rowId || !colId || colId === 'actions') return;

  if (state.inlineEdit.td) endInlineEdit({ save: false });

  const row = getRowById(rowId);
  if (!row) return;

  const col = ALL_COLUMNS.find(c => c.id === colId);
  if (!col) return;

  const rawCurrent = (colId === 'verified')
    ? row.verified
    : (row[col.key] ?? null);

  const displayCurrent = valueForDisplay(colId, rawCurrent);

  state.inlineEdit.td = td;
  state.inlineEdit.rowId = rowId;
  state.inlineEdit.colId = colId;
  state.inlineEdit.original = displayCurrent === '—' ? '—' : displayCurrent;
  state.inlineEdit.skipBlurCommit = false;

  td.classList.add('editing');
  td.innerHTML = '';

  let inputEl;

  if (selectColumns.has(colId)) {
    const wrapper = document.createElement('div');
    wrapper.className = 'flex items-center gap-2';

    const sel = document.createElement('select');
    sel.className = 'cell-select flex-1 min-w-0';

    const opts = getSelectOptions(colId);
    opts.forEach(({ value, label }) => addSelectOption(sel, value, label));

    const currentValue = colId === 'verified'
      ? (rawCurrent === true ? 'true' : rawCurrent === false ? 'false' : (rawCurrent === null || rawCurrent === undefined ? '' : String(rawCurrent)))
      : (rawCurrent === null || rawCurrent === undefined ? '' : String(rawCurrent));
    sel.value = [...sel.options].some(o => o.value === currentValue) ? currentValue : '';

    let previousValue = sel.value;

    sel.addEventListener('change', () => {
      if (sel.value !== insertNewValue) {
        previousValue = sel.value;
        return;
      }

      const label = colId === 'verified' ? 'verification' : col.label.toLowerCase();
      const entry = prompt(`Enter a new ${label} value:`);
      if (entry === null) {
        sel.value = previousValue;
        return;
      }

      const trimmed = entry.trim();
      if (trimmed === '') {
        sel.value = previousValue;
        return;
      }

      let value = trimmed;
      if (colId === 'verified') {
        const boolVal = toBool(trimmed);
        if (boolVal === true) value = 'true';
        else if (boolVal === false) value = 'false';
      }

      addSelectOption(sel, value, optionLabel(colId, trimmed));
      sel.value = value;
      previousValue = value;
    });

    wrapper.appendChild(sel);
    inputEl = sel;
    state.inlineEdit.inputEl = inputEl;
    td.appendChild(wrapper);
  } else {
    const inp = document.createElement('input');
    inp.type = 'text';
    inp.className = 'cell-input';
    inp.value = rawCurrent === null || rawCurrent === undefined ? '' : String(rawCurrent);
    inputEl = inp;
    state.inlineEdit.inputEl = inputEl;
    td.appendChild(inputEl);

    const validatePhone = (value) => {
      const digits = String(value ?? '').replace(/\D/g, '');
      if (digits.length === 0) return '';
      return digits.length === 10 ? '' : 'Phone numbers must include exactly 10 digits.';
    };

    const validateEmail = (value) => {
      if (!value) return '';
      const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
      return emailRegex.test(value) ? '' : 'Please enter a valid email address.';
    };

    const updateValidity = () => {
      let message = '';
      if (colId === 'phone') message = validatePhone(inputEl.value);
      if (colId === 'email') message = validateEmail(inputEl.value.trim());
      inputEl.setCustomValidity(message);
      td.classList.toggle('failed', Boolean(message));
      if (message) inputEl.title = message;
      else inputEl.removeAttribute('title');
    };

    if (colId === 'phone' || colId === 'email') {
      inputEl.addEventListener('input', updateValidity);
      inputEl.addEventListener('blur', updateValidity);
      updateValidity();
    }
  }

  setTimeout(() => {
    inputEl.focus();
    if (inputEl.select) inputEl.select();
  }, 0);

  inputEl.addEventListener('keydown', async (e) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      state.inlineEdit.skipBlurCommit = true;
      try {
        await commitInlineEdit();
      } finally {
        state.inlineEdit.skipBlurCommit = false;
      }
    } else if (e.key === 'Escape') {
      e.preventDefault();
      endInlineEdit({ save: false });
    } else if (e.key === 'Tab') {
      e.preventDefault();
      await commitInlineEdit();
      focusNextEditableCell(td, e.shiftKey ? -1 : 1);
    }
  });

  inputEl.addEventListener('blur', async () => {
    if (state.inlineEdit.skipBlurCommit) {
      state.inlineEdit.skipBlurCommit = false;
      return;
    }

    await commitInlineEdit();
  });
};

export { commitInlineEdit, startInlineEdit };
