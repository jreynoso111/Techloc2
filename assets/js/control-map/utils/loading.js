let loadingCounter = 0;

export function startLoading(message = 'Loading…') {
  const deferredAuthLoading = document.querySelector('[data-auth-loading][data-auth-loading-mode="defer"]');
  if (deferredAuthLoading) {
    return () => {};
  }

  const overlay = document.getElementById('loading-overlay');
  const text = document.getElementById('loading-text');
  if (!overlay || !text) return () => {};

  loadingCounter++;
  text.textContent = message;
  overlay.classList.remove('hidden');
  document.body.classList.add('loading');

  return () => {
    loadingCounter = Math.max(0, loadingCounter - 1);
    if (loadingCounter === 0) {
      overlay.classList.add('hidden');
      document.body.classList.remove('loading');
    }
  };
}
