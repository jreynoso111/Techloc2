export const createRainBackground = (canvasId = 'constellation-canvas') => {
  const canvas = document.getElementById(canvasId);
  if (!canvas) return { cleanup: () => {} };

  const ctx = canvas.getContext('2d');
  const drops = [];
  let width = 0;
  let height = 0;
  let animationFrameId;
  let running = true;

  const resize = () => {
    width = window.innerWidth;
    height = Math.max(window.innerHeight, document.documentElement.scrollHeight);
    canvas.width = width;
    canvas.height = height;

    const desired = Math.min(320, Math.max(140, Math.floor((width * height) / 12000)));
    while (drops.length < desired) {
      drops.push({
        x: Math.random() * width,
        y: Math.random() * height,
        len: Math.random() * 18 + 12,
        speed: Math.random() * 0.9 + 0.9,
        opacity: Math.random() * 0.35 + 0.35,
        drift: (Math.random() - 0.5) * 0.4,
      });
    }
    drops.splice(desired);
  };

  const update = (delta) => {
    const fallFactor = delta * 0.35;
    const driftFactor = delta * 0.12;

    drops.forEach((drop) => {
      drop.x += drop.drift * driftFactor;
      drop.y += drop.speed * fallFactor;

      if (drop.x < -20) drop.x = width + 20;
      if (drop.x > width + 20) drop.x = -20;
      if (drop.y > height + 20) {
        drop.y = -20;
        drop.x = Math.random() * width;
      }
    });
  };

  const draw = () => {
    ctx.clearRect(0, 0, width, height);
    ctx.strokeStyle = 'rgba(125, 211, 252, 0.7)';
    ctx.lineWidth = 1.1;
    ctx.lineCap = 'round';

    drops.forEach((drop) => {
      ctx.globalAlpha = drop.opacity;
      ctx.beginPath();
      ctx.moveTo(drop.x, drop.y);
      ctx.lineTo(drop.x + drop.drift * 6, drop.y + drop.len);
      ctx.stroke();
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
    drops.splice(0, drops.length);
  };

  return { cleanup };
};
