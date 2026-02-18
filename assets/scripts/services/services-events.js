const attachResizeHandles = ({ els, state, setColWidth }) => {
  const onMouseMoveResize = (e) => {
    const session = state.drag.resizing;
    if (!session) return;
    if (session.pointerId !== undefined && session.pointerId !== null && e.pointerId !== undefined && e.pointerId !== session.pointerId) return;
    e.preventDefault();
    const { colId, startX, startW } = session;
    const dx = e.clientX - startX;
    setColWidth(colId, startW + dx);
  };

  const stopResize = (e) => {
    const session = state.drag.resizing;
    if (!session) return;
    if (session.pointerId !== undefined && session.pointerId !== null && e && e.pointerId !== undefined && e.pointerId !== session.pointerId) return;
    state.drag.resizing = null;
    document.body.style.cursor = '';
    document.body.style.userSelect = '';
    window.removeEventListener('pointermove', onMouseMoveResize);
    window.removeEventListener('pointerup', stopResize, true);
  };

  els.thead.addEventListener('pointerdown', (e) => {
    const handle = e.target.closest('[data-resize]');
    if (!handle) return;
    e.preventDefault();
    e.stopPropagation();

    const colId = handle.getAttribute('data-resize');
    const th = handle.closest('th');
    const thWidth = th ? parseFloat(getComputedStyle(th).width) : 0;
    const startWidth = Number.isFinite(state.columnWidths[colId]) ? state.columnWidths[colId] : (thWidth || 140);

    state.drag.resizing = { colId, startX: e.clientX, startW: startWidth, pointerId: e.pointerId };
    document.body.style.cursor = 'col-resize';
    document.body.style.userSelect = 'none';
    if (handle.setPointerCapture) handle.setPointerCapture(e.pointerId);
    window.addEventListener('pointermove', onMouseMoveResize);
    window.addEventListener('pointerup', stopResize, true);
  });
};

const attachScrollSync = ({ els, syncTopScrollbar, resizeCharts }) => {
  let syncing = false;

  els.tableScroll.addEventListener('scroll', () => {
    if (syncing) return;
    syncing = true;
    els.topScrollbar.scrollLeft = els.tableScroll.scrollLeft;
    syncing = false;
  });

  els.topScrollbar.addEventListener('scroll', () => {
    if (syncing) return;
    syncing = true;
    els.tableScroll.scrollLeft = els.topScrollbar.scrollLeft;
    syncing = false;
  });

  window.addEventListener('resize', () => {
    syncTopScrollbar();
    if (resizeCharts) resizeCharts();
  });
};

export { attachResizeHandles, attachScrollSync };
