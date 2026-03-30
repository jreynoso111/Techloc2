export const createConstellationBackground = (canvasId = 'constellation-canvas') => {
  const canvas = document.getElementById(canvasId);
  if (!canvas) return { cleanup: () => {} };

  const ctx = canvas.getContext('2d');
  const particles = [];
  let width = 0;
  let height = 0;
  let animationFrameId;
  let running = true;

  const resize = () => {
    width = window.innerWidth;
    height = Math.max(window.innerHeight, document.documentElement.scrollHeight);
    canvas.width = width;
    canvas.height = height;

    const desired = Math.min(160, Math.max(80, Math.floor((width * height) / 22000)));
    while (particles.length < desired) {
      particles.push({
        x: Math.random() * width,
        y: Math.random() * height,
        vx: (Math.random() - 0.5) * 0.35,
        vy: (Math.random() - 0.5) * 0.35,
<<<<<<< HEAD
        r: Math.random() * 1.8 + 0.7,
        alpha: Math.random() * 0.5 + 0.2,
        targetAlpha: Math.random() * 0.6 + 0.2,
=======
        r: Math.random() * 2.1 + 0.95,
        alpha: Math.random() * 0.58 + 0.28,
        targetAlpha: Math.random() * 0.68 + 0.26,
>>>>>>> impte
        pulse: Math.random() * Math.PI * 2,
      });
    }
    particles.splice(desired);
  };

  const update = (delta) => {
    const fadeSpeed = delta * 0.0009;
    particles.forEach((p) => {
      p.x += p.vx * delta * 0.06;
      p.y += p.vy * delta * 0.06;
      p.pulse += delta * 0.0012;

      if (p.x < -20) p.x = width + 20;
      if (p.x > width + 20) p.x = -20;
      if (p.y < -20) p.y = height + 20;
      if (p.y > height + 20) p.y = -20;

      if (Math.random() < 0.005) {
        p.targetAlpha = Math.random() * 0.6 + 0.2;
      }

      p.alpha += (p.targetAlpha - p.alpha) * fadeSpeed * 50;
    });
  };

  const draw = () => {
    ctx.clearRect(0, 0, width, height);

    for (let i = 0; i < particles.length; i += 1) {
      const p = particles[i];
      const flicker = 0.35 + 0.65 * Math.sin(p.pulse);
<<<<<<< HEAD
      ctx.beginPath();
      ctx.fillStyle = `rgba(99, 102, 241, ${p.alpha * flicker})`;
      ctx.arc(p.x, p.y, p.r, 0, Math.PI * 2);
      ctx.fill();
=======
      ctx.shadowBlur = 10;
      ctx.shadowColor = 'rgba(125, 211, 252, 0.35)';
      ctx.beginPath();
      ctx.fillStyle = `rgba(191, 219, 254, ${(p.alpha * flicker).toFixed(3)})`;
      ctx.arc(p.x, p.y, p.r, 0, Math.PI * 2);
      ctx.fill();
      ctx.shadowBlur = 0;
>>>>>>> impte

      for (let j = i + 1; j < particles.length; j += 1) {
        const q = particles[j];
        const dx = p.x - q.x;
        const dy = p.y - q.y;
        const dist = Math.hypot(dx, dy);

        const maxDist = 180;
        if (dist < maxDist) {
<<<<<<< HEAD
          const lineAlpha = ((1 - dist / maxDist) ** 2) * 0.35 * (0.35 + 0.65 * Math.sin((p.pulse + q.pulse) * 0.5));
          ctx.strokeStyle = `rgba(56, 189, 248, ${lineAlpha})`;
          ctx.lineWidth = 1;
=======
          const lineAlpha = ((1 - dist / maxDist) ** 2) * 0.5 * (0.42 + 0.68 * Math.sin((p.pulse + q.pulse) * 0.5));
          ctx.strokeStyle = `rgba(125, 211, 252, ${lineAlpha.toFixed(3)})`;
          ctx.lineWidth = 1.2;
          ctx.shadowBlur = 6;
          ctx.shadowColor = `rgba(56, 189, 248, ${(lineAlpha * 0.45).toFixed(3)})`;
>>>>>>> impte
          ctx.beginPath();
          ctx.moveTo(p.x, p.y);
          ctx.lineTo(q.x, q.y);
          ctx.stroke();
<<<<<<< HEAD
=======
          ctx.shadowBlur = 0;
>>>>>>> impte
        }
      }
    }
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
    particles.splice(0, particles.length);
  };

  return { cleanup };
};
