(() => {
  const path = window.location.pathname.toLowerCase();
  const isAdminRoute = path.includes('/admin/');
  const isPagesRoute = path.includes('/pages/');
  const basePath = isAdminRoute ? '../../assets/data/' : isPagesRoute ? '../assets/data/' : 'assets/data/';
  const csvPath = (name) => `${basePath}${name}.csv`;

  const paths = {
    installers: csvPath('installers'),
    vehicles: csvPath('vehicles'),
    towing_companies: csvPath('towing_companies'),
    resellers: csvPath('resellers'),
    repair_shops: csvPath('repair_shops'),
    locksmiths: csvPath('locksmiths'),
    dispatchers: csvPath('dispatchers'),
    technicians: csvPath('technicians'),
    inspectors: csvPath('inspectors'),
  };

  window.TL_DATASETS = paths;
  window.getDatasetPath = (key) => paths[key] || csvPath(key);
})();
