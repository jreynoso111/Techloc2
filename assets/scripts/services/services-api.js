import { supabase as supabaseClient } from '../../js/supabaseClient.js';
import { logAdminEvent } from '../adminAudit.js';

const logChange = async ({
  state,
  action = 'edit',
  summary = '',
  tableName,
  recordId = null,
  columnName = null,
  previousValue = null,
  newValue = null,
} = {}) => {
  await logAdminEvent({
    client: supabaseClient,
    action,
    tableName,
    summary,
    recordId,
    columnName,
    previousValue,
    newValue,
    actor: state?.currentUserEmail || state?.currentUserId || null,
  });
};

const refresh = async ({
  skeleton,
  state,
  normalizeRow,
  updateStats,
  renderTable,
  savePrefs,
  els,
  getVisibleOrderedColumns,
  showToast,
  readTable,
}) => {
  skeleton.show();

  try {
    const { data, error } = await supabaseClient.from(readTable).select('*');
    if (error) throw error;

    state.rows = (data || []).map(normalizeRow);

    skeleton.hide();
    updateStats();
    renderTable();
    savePrefs();
  } catch (error) {
    console.error('Error fetching services:', error);
    skeleton.hide();
    els.status.textContent = 'Error loading data';
    els.tbody.innerHTML = `<tr><td class="compact-td text-sm text-red-200" colspan="${getVisibleOrderedColumns().length || 1}">Unable to load data.</td></tr>`;
    els.paginationSummary.textContent = 'No data available';
    els.pageIndicator.textContent = '—';
    showToast('Error loading services data. Please try again.', 'error');
  }
};

const duplicateRow = async ({
  row,
  state,
  buildInsertPayload,
  normalizeRow,
  renderTable,
  showToast,
  showConfirm,
  writeTable,
}) => {
  if (!row?.id) return;
  if (state.currentUserRole !== 'administrator') {
    showToast('Only administrators can duplicate records.', 'warning');
    return;
  }
  const confirmed = await showConfirm(`Duplicate "${row.company}"?`, { confirmText: 'Duplicate' });
  if (!confirmed) return;

  try {
    const payload = buildInsertPayload(row);
    const { data, error } = await supabaseClient
      .from(writeTable)
      .insert([payload])
      .select('*')
      .single();

    if (error) throw error;

    const duplicated = normalizeRow(data);
    const index = state.rows.findIndex(r => r.id === row.id);
    if (index >= 0) {
      state.rows.splice(index + 1, 0, duplicated);
    } else {
      state.rows.push(duplicated);
    }

    renderTable();
    showToast('Record duplicated successfully.', 'success');

    await logChange({
      state,
      action: 'insert',
      summary: `Duplicated ${row.company || 'service'} as #${duplicated.id}`,
      tableName: writeTable,
      recordId: duplicated.id,
    });
  } catch (err) {
    console.error(err);
    showToast('Error duplicating record: ' + (err?.message || JSON.stringify(err)), 'error');
  }
};

const deleteRow = async ({
  row,
  state,
  showToast,
  showConfirm,
  writeTable,
  refresh,
}) => {
  if (!row?.id) return;
  if (state.currentUserRole !== 'administrator') {
    showToast('Only administrators can delete records.', 'warning');
    return;
  }
  const confirmed = await showConfirm(`Delete "${row.company}"?`, { confirmText: 'Delete', cancelText: 'Cancel' });
  if (!confirmed) return;
  const { error } = await supabaseClient.from(writeTable).delete().eq('id', row.id);
  if (error) {
    showToast('Error deleting: ' + error.message, 'error');
    return;
  }

  showToast('Record deleted.', 'success');
  await logChange({
    state,
    action: 'delete',
    summary: `Deleted ${row.company || 'service'} (#${row.id})`,
    tableName: writeTable,
    recordId: row.id,
  });
  await refresh();
};

export { deleteRow, duplicateRow, refresh };
