const INTRO_DURATION_MS = 3800;
const INTRO_EXIT_MS = 760;

const clamp = (v, min, max) => Math.min(max, Math.max(min, v));

const createNodes = (count) => {
  const nodes = [];
  for (let i = 0; i < count; i += 1) {
    nodes.push({
      x: (Math.random() - 0.5) * 820,
      y: (Math.random() - 0.5) * 460,
      z: Math.random() * 1200 + 160,
      vx: (Math.random() - 0.5) * 0.14,
      vy: (Math.random() - 0.5) * 0.14,
      phase: Math.random() * Math.PI * 2,
      size: Math.random() * 1.7 + 0.9,
    });
  }
  return nodes;
};

export const initHomeEntry3d = () => {
  const intro = document.getElementById('home-intro');
  const scene = document.getElementById('home-intro-scene');
  const canvas = document.getElementById('home-intro-canvas');

  if (!intro || !scene || !canvas) return;

  const ctx = canvas.getContext('2d');
  if (!ctx) {
    intro.hidden = true;
    return;
  }

  const reducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  const introDuration = reducedMotion ? 900 : INTRO_DURATION_MS;
  intro.style.setProperty('--intro-duration', `${introDuration}ms`);
  document.body.classList.add('home-intro-active');

  const nodes = createNodes(reducedMotion ? 24 : 52);
  const projection = 620;
  const bounds = { x: 440, y: 270 };

  let rafId = null;
  let finishing = false;
  let timeoutId = null;
  let startTs = 0;
  let pointerX = 0;
  let pointerY = 0;
  let width = 0;
  let height = 0;
  let dpr = 1;

  const resize = () => {
    const rect = scene.getBoundingClientRect();
    width = Math.max(1, rect.width);
    height = Math.max(1, rect.height);
    dpr = clamp(window.devicePixelRatio || 1, 1, 2);
    canvas.width = Math.round(width * dpr);
    canvas.height = Math.round(height * dpr);
    canvas.style.width = `${width}px`;
    canvas.style.height = `${height}px`;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  };

  const onPointerMove = (event) => {
    const rect = scene.getBoundingClientRect();
    const nx = (event.clientX - rect.left) / rect.width;
    const ny = (event.clientY - rect.top) / rect.height;
    pointerX = clamp((nx - 0.5) * 2, -1, 1);
    pointerY = clamp((ny - 0.5) * 2, -1, 1);
  };

  const onPointerLeave = () => {
    pointerX = 0;
    pointerY = 0;
  };

  const onTouchMove = (event) => {
    const point = event.touches?.[0];
    if (!point) return;
    onPointerMove(point);
  };

  const stop = () => {
    if (rafId !== null) {
      cancelAnimationFrame(rafId);
      rafId = null;
    }
  };

  const cleanup = () => {
    stop();
    window.removeEventListener('resize', resize);
    scene.removeEventListener('pointermove', onPointerMove);
    scene.removeEventListener('pointerleave', onPointerLeave);
    scene.removeEventListener('touchmove', onTouchMove);
    scene.removeEventListener('touchend', onPointerLeave);
    if (timeoutId !== null) {
      clearTimeout(timeoutId);
      timeoutId = null;
    }
  };

  const finishIntro = () => {
    if (finishing) return;
    finishing = true;
    intro.classList.add('is-finished');
    document.body.classList.remove('home-intro-active');
    cleanup();
    window.setTimeout(() => {
      intro.hidden = true;
    }, INTRO_EXIT_MS);
  };

  const render = (ts) => {
    if (!startTs) startTs = ts;
    const elapsed = ts - startTs;
    const progress = clamp(elapsed / introDuration, 0, 1);

    const cameraZ = 1260 - progress * 640;
    const yaw = pointerX * 0.26 + Math.sin(elapsed * 0.00045) * 0.08;
    const pitch = pointerY * 0.22 + Math.cos(elapsed * 0.00037) * 0.05;

    ctx.clearRect(0, 0, width, height);

    const centerX = width * 0.5;
    const centerY = height * 0.48;

    const projected = [];

    for (let i = 0; i < nodes.length; i += 1) {
      const node = nodes[i];
      node.phase += 0.014;
      node.x += node.vx + Math.sin(node.phase) * 0.12;
      node.y += node.vy + Math.cos(node.phase * 0.9) * 0.09;

      if (node.x > bounds.x || node.x < -bounds.x) node.vx *= -1;
      if (node.y > bounds.y || node.y < -bounds.y) node.vy *= -1;

      const x1 = node.x * Math.cos(yaw) - node.z * Math.sin(yaw);
      const z1 = node.x * Math.sin(yaw) + node.z * Math.cos(yaw);
      const y2 = node.y * Math.cos(pitch) - z1 * Math.sin(pitch);
      const z2 = node.y * Math.sin(pitch) + z1 * Math.cos(pitch);

      const depth = z2 - cameraZ;
      if (depth < -projection + 30) continue;

      const scale = projection / (projection + depth);
      const px = centerX + x1 * scale;
      const py = centerY + y2 * scale;
      const radius = node.size * scale * 1.5;
      const alpha = clamp(1 - Math.abs(depth) / 2100, 0.08, 0.95);

      projected.push({ x: px, y: py, d: depth, a: alpha, r: radius });
    }

    projected.sort((a, b) => b.d - a.d);

    for (let i = 0; i < projected.length; i += 1) {
      const a = projected[i];
      for (let j = i + 1; j < projected.length && j < i + 7; j += 1) {
        const b = projected[j];
        const dx = a.x - b.x;
        const dy = a.y - b.y;
        const dist = Math.hypot(dx, dy);
        if (dist > 88) continue;

        const linkAlpha = (1 - dist / 88) * Math.min(a.a, b.a) * 0.36;
        ctx.strokeStyle = `rgba(56, 189, 248, ${linkAlpha.toFixed(3)})`;
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.moveTo(a.x, a.y);
        ctx.lineTo(b.x, b.y);
        ctx.stroke();
      }
    }

    for (let i = 0; i < projected.length; i += 1) {
      const point = projected[i];
      const glow = point.r * 3.6;
      const gradient = ctx.createRadialGradient(point.x, point.y, 0, point.x, point.y, glow);
      gradient.addColorStop(0, `rgba(125, 211, 252, ${Math.min(point.a + 0.2, 0.95).toFixed(3)})`);
      gradient.addColorStop(1, 'rgba(14, 165, 233, 0)');

      ctx.fillStyle = gradient;
      ctx.beginPath();
      ctx.arc(point.x, point.y, glow, 0, Math.PI * 2);
      ctx.fill();

      ctx.fillStyle = `rgba(191, 219, 254, ${point.a.toFixed(3)})`;
      ctx.beginPath();
      ctx.arc(point.x, point.y, Math.max(0.65, point.r), 0, Math.PI * 2);
      ctx.fill();
    }

    const sweepY = height * (0.18 + progress * 0.64);
    const scan = ctx.createLinearGradient(0, sweepY - 70, 0, sweepY + 70);
    scan.addColorStop(0, 'rgba(56, 189, 248, 0)');
    scan.addColorStop(0.5, 'rgba(56, 189, 248, 0.1)');
    scan.addColorStop(1, 'rgba(56, 189, 248, 0)');
    ctx.fillStyle = scan;
    ctx.fillRect(0, sweepY - 70, width, 140);

    if (progress >= 1) {
      finishIntro();
      return;
    }

    rafId = requestAnimationFrame(render);
  };

  resize();

  window.addEventListener('resize', resize);
  scene.addEventListener('pointermove', onPointerMove);
  scene.addEventListener('pointerleave', onPointerLeave);
  scene.addEventListener('touchmove', onTouchMove, { passive: true });
  scene.addEventListener('touchend', onPointerLeave);

  rafId = requestAnimationFrame(render);
  timeoutId = window.setTimeout(() => finishIntro(), introDuration + 120);

  window.addEventListener('beforeunload', cleanup);
};
