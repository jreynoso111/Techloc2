const adminNavSlot = document.querySelector('[data-admin-nav]');

const getBasePath = () => {
  const bodyBase = document.body?.dataset.basePath;
  if (bodyBase) return bodyBase;
  const path = window.location.pathname;
  if (path.includes('/pages/admin/')) return '../../';
  if (path.includes('/pages/')) return '../';
  return './';
};

const getAdminBase = () => {
  const path = window.location.pathname;
  if (path.includes('/pages/admin/')) return '';
  return 'pages/admin/';
};

const setActiveAdminNav = (container, activeKey) => {
  if (!container || !activeKey) return;
  const activeClass = ['border-blue-500', 'bg-blue-600/90', 'text-white', 'shadow', 'shadow-blue-500/30'];
  const inactiveClass = ['border-slate-800/60', 'text-slate-300'];

  container.querySelectorAll('[data-admin-key]').forEach((link) => {
    link.classList.remove(...activeClass);
    link.classList.add(...inactiveClass);
    link.removeAttribute('aria-current');
  });

  const activeLink = container.querySelector(`[data-admin-key="${activeKey}"]`);
  if (activeLink) {
    activeLink.classList.remove(...inactiveClass);
    activeLink.classList.add(...activeClass);
    activeLink.setAttribute('aria-current', 'page');
  }
};

const hydrateAdminNav = async () => {
  if (!adminNavSlot) return;
  const basePath = getBasePath();
  const adminBase = getAdminBase();
  const activeNav = adminNavSlot.dataset.adminNav || document.body?.dataset.adminNav || '';

  try {
    const response = await fetch(`${basePath}assets/templates/admin-nav.html`);
    if (!response.ok) throw new Error(`Admin nav template not found (${response.status})`);
    const template = await response.text();
    adminNavSlot.innerHTML = template.replaceAll('{{ADMIN_BASE}}', adminBase);
    setActiveAdminNav(adminNavSlot, activeNav);
  } catch (error) {
    console.error('Admin navigation failed to load:', error);
  }
};

hydrateAdminNav();
