export const createLayerToggle = ({
  toggleId,
  labelOn,
  labelOff,
  getVisible,
  setVisible,
  onShow,
  onHide
}) => {
  const toggle = document.getElementById(toggleId);
  if (!toggle) return null;

  const update = () => {
    const isVisible = !!getVisible();
    toggle.textContent = isVisible ? labelOn : labelOff;
    toggle.classList.toggle('active', isVisible);
    toggle.setAttribute('aria-pressed', isVisible ? 'true' : 'false');
  };

  const applyVisibility = (visible) => {
    setVisible(!!visible);
    update();
    if (getVisible()) {
      onShow?.();
    } else {
      onHide?.();
    }
  };

  toggle.addEventListener('click', () => applyVisibility(!getVisible()));
  update();

  return { update, setVisible: applyVisibility };
};
