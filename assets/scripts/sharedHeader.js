import { initGlobalAlerts } from './globalAlerts.js';
import { initGlobalActivityTracker } from './globalActivityTracker.js';

initGlobalAlerts();
initGlobalActivityTracker();

const headerSlot = document.querySelector('[data-shared-header]');

const getBasePath = () => {
  const bodyBase = document.body?.dataset.basePath;
  if (bodyBase) return bodyBase;
  const path = window.location.pathname;
  if (path.includes('/pages/admin/')) return '../../';
  if (path.includes('/pages/')) return '../';
  return './';
};

const setActiveNav = (container, activeKey) => {
  if (!container || !activeKey) return;
  const activeClass = ['bg-blue-600', 'text-white', 'shadow', 'shadow-blue-500/20'];
  const links = container.querySelectorAll('[data-nav-key]');
  links.forEach((link) => {
    link.classList.remove(...activeClass);
    link.removeAttribute('aria-current');
  });

  const activeLink = container.querySelector(`[data-nav-key="${activeKey}"]`);
  if (activeLink) {
    activeLink.classList.add(...activeClass);
    activeLink.setAttribute('aria-current', 'page');
  }

  if (activeKey === 'contact') {
    const dropdown = container.querySelector('[data-contact-dropdown]');
    const toggle = container.querySelector('[data-contact-toggle]');
    if (dropdown) dropdown.classList.remove('hidden');
    if (toggle) toggle.setAttribute('aria-expanded', 'true');
  }
};

const setupMobileMenu = (container) => {
  const toggle = container.querySelector('#mobile-menu-toggle');
  const nav = container.querySelector('#primary-nav');
  if (!toggle || !nav) return;

  toggle.addEventListener('click', () => {
    const isHidden = nav.classList.toggle('hidden');
    toggle.setAttribute('aria-expanded', (!isHidden).toString());
  });
};

const setupContactDropdown = (container) => {
  const toggle = container.querySelector('[data-contact-toggle]');
  const dropdown = container.querySelector('[data-contact-dropdown]');
  if (!toggle || !dropdown) return;

  const closeDropdown = () => {
    dropdown.classList.add('hidden');
    toggle.setAttribute('aria-expanded', 'false');
  };

  const openDropdown = () => {
    dropdown.classList.remove('hidden');
    toggle.setAttribute('aria-expanded', 'true');
  };

  toggle.addEventListener('click', (event) => {
    event.preventDefault();
    event.stopPropagation();
    const isHidden = dropdown.classList.contains('hidden');
    if (isHidden) {
      openDropdown();
    } else {
      closeDropdown();
    }
  });

  document.addEventListener('click', (event) => {
    if (!container.contains(event.target)) {
      closeDropdown();
      return;
    }
    if (event.target.closest('[data-contact-toggle]') || event.target.closest('[data-contact-dropdown]')) return;
    closeDropdown();
  });

  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape') closeDropdown();
  });
};

const hydrateHeader = async () => {
  if (!headerSlot) return;
  const basePath = getBasePath();
  const pageTitle = document.body?.dataset.pageTitle || document.title;
  const activeNav = document.body?.dataset.activeNav || '';

  try {
    const response = await fetch(`${basePath}assets/templates/site-header.html`);
    if (!response.ok) throw new Error(`Header template not found (${response.status})`);
    const template = await response.text();
    const rendered = template
      .replaceAll('{{BASE}}', basePath)
      .replace('{{PAGE_TITLE}}', pageTitle);

    headerSlot.innerHTML = rendered;
    setActiveNav(headerSlot, activeNav);
    setupMobileMenu(headerSlot);
    setupContactDropdown(headerSlot);
  } catch (error) {
    console.error('Shared header failed to load:', error);
  }
};

hydrateHeader();
