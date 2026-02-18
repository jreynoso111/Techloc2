export const createSnowBackground = (canvasId = 'constellation-canvas') => {
  const canvas = document.getElementById(canvasId);
  if (!canvas) return { cleanup: () => {} };

  const ctx = canvas.getContext('2d');
  const flakes = [];
  let width = 0;
  let height = 0;
  let animationFrameId;
  let running = true;

  const resize = () => {
    width = window.innerWidth;
    height = Math.max(window.innerHeight, document.documentElement.scrollHeight);
    canvas.width = width;
    canvas.height = height;

    const desired = Math.min(260, Math.max(120, Math.floor((width * height) / 14000)));
    while (flakes.length < desired) {
      flakes.push({
        x: Math.random() * width,
        y: Math.random() * height,
        r: Math.random() * 2.2 + 0.8,
        speed: Math.random() * 0.7 + 0.6,
        drift: (Math.random() - 0.5) * 0.6,
        opacity: Math.random() * 0.4 + 0.4,
        wobble: Math.random() * Math.PI * 2,
      });
    }
    flakes.splice(desired);
  };

  const update = (delta) => {
    const driftFactor = delta * 0.02;
    const fallFactor = delta * 0.06;

    flakes.forEach((flake) => {
      flake.wobble += delta * 0.0025;
      flake.x += (Math.sin(flake.wobble) * 0.6 + flake.drift) * driftFactor;
      flake.y += (flake.speed + Math.cos(flake.wobble) * 0.08) * fallFactor;

      if (flake.x < -10) flake.x = width + 10;
      if (flake.x > width + 10) flake.x = -10;
      if (flake.y > height + 12) {
        flake.y = -12;
        flake.x = Math.random() * width;
      }
    });
  };

  const draw = () => {
    ctx.clearRect(0, 0, width, height);
    ctx.fillStyle = '#e2e8f0';

    flakes.forEach((flake) => {
      ctx.beginPath();
      ctx.globalAlpha = flake.opacity;
      ctx.arc(flake.x, flake.y, flake.r, 0, Math.PI * 2);
      ctx.fill();
    });
    ctx.globalAlpha = 1;
  };

  let lastTime = performance.now();
  const loop = (time) => {
    if (!running) return;
    const delta = Math.min(40, time - lastTime);
    lastTime = time;
    update(delta);
    draw();
    animationFrameId = requestAnimationFrame(loop);
  };

  resize();
  window.addEventListener('resize', resize);
  animationFrameId = requestAnimationFrame(loop);

  const cleanup = () => {
    running = false;
    if (animationFrameId) cancelAnimationFrame(animationFrameId);
    window.removeEventListener('resize', resize);
    ctx.clearRect(0, 0, width, height);
    flakes.splice(0, flakes.length);
  };

  return { cleanup };
};
