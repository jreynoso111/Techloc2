let chartStates = null;
let chartCategories = null;

const buildCounts = (rows, key) => {
  const m = new Map();
  rows.forEach((r) => {
    const k = (r[key] || '—').toString().trim() || '—';
    m.set(k, (m.get(k) || 0) + 1);
  });
  return [...m.entries()].sort((a, b) => b[1] - a[1]);
};

const makeChartOptions = () => ({
  responsive: true,
  maintainAspectRatio: false,
  animation: {
    duration: 650,
    easing: 'easeOutQuart'
  },
  plugins: {
    legend: { labels: { color: 'rgba(226,232,240,.85)', boxWidth: 10, boxHeight: 10, usePointStyle: true } },
    tooltip: {
      backgroundColor: 'rgba(2,6,23,.95)',
      borderColor: 'rgba(51,65,85,.8)',
      borderWidth: 1,
      titleColor: 'rgba(226,232,240,.95)',
      bodyColor: 'rgba(226,232,240,.85)',
    }
  },
  scales: {
    x: { ticks: { color: 'rgba(148,163,184,.8)' }, grid: { color: 'rgba(51,65,85,.35)' } },
    y: { ticks: { color: 'rgba(148,163,184,.8)' }, grid: { color: 'rgba(51,65,85,.35)' } }
  }
});

const resizeCharts = () => {
  if (chartStates) chartStates.resize();
  if (chartCategories) chartCategories.resize();
};

const renderCharts = ({ filteredRows, syncTopScrollbar } = {}) => {
  const topStates = buildCounts(filteredRows, 'state').slice(0, 10);
  const topCats = buildCounts(filteredRows, 'category').slice(0, 8);

  const stateLabels = topStates.map(([k]) => (k === '—' ? 'Unknown' : k.toUpperCase()));
  const stateValues = topStates.map(([, v]) => v);

  const catLabels = topCats.map(([k]) => (k === '—' ? 'Unknown' : k));
  const catValues = topCats.map(([, v]) => v);

  const palette = [
    'rgba(59,130,246,.55)', 'rgba(99,102,241,.55)', 'rgba(16,185,129,.55)',
    'rgba(245,158,11,.55)', 'rgba(236,72,153,.55)', 'rgba(148,163,184,.45)',
    'rgba(34,197,94,.45)', 'rgba(14,165,233,.45)',
  ];

  const ctxStates = document.getElementById('chart-states');
  const ctxCats = document.getElementById('chart-categories');
  if (!ctxStates || !ctxCats) return;

  if (chartStates) chartStates.destroy();
  if (chartCategories) chartCategories.destroy();

  chartStates = new Chart(ctxStates, {
    type: 'bar',
    data: { labels: stateLabels, datasets: [{ label: 'Services', data: stateValues, backgroundColor: palette[0], borderColor: 'rgba(59,130,246,.85)', borderWidth: 1, borderRadius: 10 }] },
    options: { ...makeChartOptions() }
  });

  chartCategories = new Chart(ctxCats, {
    type: 'doughnut',
    data: { labels: catLabels, datasets: [{ label: 'Services', data: catValues, backgroundColor: catValues.map((_, i) => palette[i % palette.length]), borderColor: 'rgba(15,23,42,.9)', borderWidth: 2, hoverOffset: 6 }] },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      cutout: '62%',
      animation: {
        duration: 650,
        easing: 'easeOutQuart'
      },
      plugins: { legend: { position: 'right', labels: { color: 'rgba(226,232,240,.85)', boxWidth: 10, boxHeight: 10, usePointStyle: true } }, tooltip: makeChartOptions().plugins.tooltip },
    }
  });

  requestAnimationFrame(() => {
    resizeCharts();
    if (syncTopScrollbar) syncTopScrollbar();
  });
};

export { renderCharts, resizeCharts };
