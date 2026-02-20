const INTRO_DURATION_MS = 1800;
const INTRO_EXIT_MS = 760;

const clamp = (v, min, max) => Math.min(max, Math.max(min, v));

const SPHERE_PALETTE = [
  [248, 250, 252],
  [226, 232, 240],
  [191, 219, 254],
  [147, 197, 253],
  [196, 181, 253],
  [254, 240, 138],
];

const createSpheres = (count) => {
  const spheres = [];

  for (let i = 0; i < count; i += 1) {
    spheres.push({
      x: (Math.random() - 0.5) * 980,
      y: (Math.random() - 0.5) * 620,
      z: Math.random() * 1800 + 180,
      vx: (Math.random() - 0.5) * 0.28,
      vy: (Math.random() - 0.5) * 0.24,
      size: Math.random() * 0.42 + 0.08,
      twinklePhase: Math.random() * Math.PI * 2,
      twinkleSpeed: Math.random() * 0.0016 + 0.001,
      colorIndex: Math.floor(Math.random() * SPHERE_PALETTE.length),
      intensity: Math.random() * 0.62 + 0.3,
      sparkle: Math.random(),
    });
  }

  return spheres;
};

const createBackgroundStars = (count) => {
  const stars = [];

  for (let i = 0; i < count; i += 1) {
    stars.push({
      x: Math.random(),
      y: Math.random(),
      size: Math.random() * 0.85 + 0.08,
      alpha: Math.random() * 0.45 + 0.08,
      drift: (Math.random() - 0.5) * 0.00003,
      pulse: Math.random() * Math.PI * 2,
      pulseSpeed: Math.random() * 0.001 + 0.0003,
    });
  }

  return stars;
};

const drawGalaxyBackdrop = (ctx, width, height, elapsed, pointerX, pointerY, progress, stars) => {
  const base = ctx.createLinearGradient(0, 0, 0, height);
  base.addColorStop(0, '#02020a');
  base.addColorStop(0.5, '#05091a');
  base.addColorStop(1, '#02040d');
  ctx.fillStyle = base;
  ctx.fillRect(0, 0, width, height);

  // Main nebula cloud.
  const nebulaCore = ctx.createRadialGradient(
    width * (0.52 + pointerX * 0.03),
    height * (0.48 + pointerY * 0.03),
    0,
    width * (0.52 + pointerX * 0.03),
    height * (0.48 + pointerY * 0.03),
    width * (0.92 + progress * 0.08)
  );
  nebulaCore.addColorStop(0, 'rgba(67, 56, 202, 0.12)');
  nebulaCore.addColorStop(0.35, 'rgba(30, 64, 175, 0.08)');
  nebulaCore.addColorStop(0.62, 'rgba(15, 23, 42, 0.06)');
  nebulaCore.addColorStop(1, 'rgba(2, 6, 23, 0)');
  ctx.fillStyle = nebulaCore;
  ctx.fillRect(0, 0, width, height);

  const nebulaArmA = ctx.createRadialGradient(
    width * (0.2 - pointerX * 0.02),
    height * (0.28 + pointerY * 0.02),
    0,
    width * (0.2 - pointerX * 0.02),
    height * (0.28 + pointerY * 0.02),
    width * 0.58
  );
  nebulaArmA.addColorStop(0, 'rgba(190, 24, 93, 0.05)');
  nebulaArmA.addColorStop(0.45, 'rgba(79, 70, 229, 0.07)');
  nebulaArmA.addColorStop(1, 'rgba(2, 6, 23, 0)');
  ctx.fillStyle = nebulaArmA;
  ctx.fillRect(0, 0, width, height);

  const nebulaArmB = ctx.createRadialGradient(
    width * (0.82 + pointerX * 0.02),
    height * (0.74 - pointerY * 0.02),
    0,
    width * (0.82 + pointerX * 0.02),
    height * (0.74 - pointerY * 0.02),
    width * 0.6
  );
  nebulaArmB.addColorStop(0, 'rgba(8, 145, 178, 0.06)');
  nebulaArmB.addColorStop(0.42, 'rgba(30, 64, 175, 0.07)');
  nebulaArmB.addColorStop(1, 'rgba(2, 6, 23, 0)');
  ctx.fillStyle = nebulaArmB;
  ctx.fillRect(0, 0, width, height);

  // Dust lanes for depth.
  const dustLane = ctx.createLinearGradient(0, height * 0.26, 0, height * 0.78);
  dustLane.addColorStop(0, 'rgba(2, 6, 23, 0)');
  dustLane.addColorStop(0.45, 'rgba(2, 6, 23, 0.22)');
  dustLane.addColorStop(0.52, 'rgba(2, 6, 23, 0.34)');
  dustLane.addColorStop(0.62, 'rgba(2, 6, 23, 0.2)');
  dustLane.addColorStop(1, 'rgba(2, 6, 23, 0)');
  ctx.fillStyle = dustLane;
  ctx.fillRect(0, 0, width, height);

  for (let i = 0; i < stars.length; i += 1) {
    const star = stars[i];
    const twinkle = 0.65 + Math.sin(elapsed * star.pulseSpeed + star.pulse) * 0.35;
    const x = ((star.x + elapsed * star.drift + 1) % 1) * width;
    const y = star.y * height;
    const alpha = star.alpha * twinkle;

    ctx.fillStyle = `rgba(241, 245, 249, ${alpha.toFixed(3)})`;
    ctx.beginPath();
    ctx.arc(x, y, star.size, 0, Math.PI * 2);
    ctx.fill();
  }
};

export const initHomeEntry3d = () => {
  const intro = document.getElementById('home-intro');
  const scene = document.getElementById('home-intro-scene');
  const canvas = document.getElementById('home-intro-canvas');
  const loadingHost = intro?.closest?.('[data-auth-loading]') || null;
  const persistUntilLoadingDone = Boolean(loadingHost);

  if (!intro || !scene || !canvas) return;

  const ctx = canvas.getContext('2d');
  if (!ctx) {
    intro.hidden = true;
    return;
  }

  const reducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  const introDuration = reducedMotion ? 550 : INTRO_DURATION_MS;
  intro.style.setProperty('--intro-duration', `${introDuration}ms`);
  intro.classList.toggle('home-intro--loading', persistUntilLoadingDone);
  document.body.classList.add('home-intro-active');

  const spheres = createSpheres(reducedMotion ? 44 : 94);
  const backgroundStars = createBackgroundStars(reducedMotion ? 48 : 130);

  const world = {
    projection: 720,
    maxX: 520,
    maxY: 330,
    nearZ: 180,
    farZ: 1980,
  };

  let rafId = null;
  let finishing = false;
  let timeoutId = null;
  let startTs = 0;
  let pointerX = 0;
  let pointerY = 0;
  let width = 0;
  let height = 0;
  let dpr = 1;
  let cleanedUp = false;

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
    if (cleanedUp) return;
    cleanedUp = true;
    stop();
    window.removeEventListener('resize', resize);
    scene.removeEventListener('pointermove', onPointerMove);
    scene.removeEventListener('pointerleave', onPointerLeave);
    scene.removeEventListener('touchmove', onTouchMove);
    scene.removeEventListener('touchend', onPointerLeave);
    document.body.classList.remove('home-intro-active');

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

  const resetSphereDepth = (sphere, extraDepth = 0) => {
    sphere.x = (Math.random() - 0.5) * world.maxX * 2;
    sphere.y = (Math.random() - 0.5) * world.maxY * 2;
    sphere.z = world.farZ + Math.random() * 260 + extraDepth;
    sphere.colorIndex = Math.floor(Math.random() * SPHERE_PALETTE.length);
    sphere.intensity = Math.random() * 0.45 + 0.55;
  };

  const render = (ts) => {
    if (!intro.isConnected || (loadingHost && !loadingHost.isConnected)) {
      cleanup();
      return;
    }

    if (!startTs) startTs = ts;
    const elapsed = ts - startTs;
    const rawProgress = elapsed / introDuration;
    const progress = persistUntilLoadingDone
      ? rawProgress % 1
      : clamp(rawProgress, 0, 1);

    ctx.clearRect(0, 0, width, height);
    drawGalaxyBackdrop(ctx, width, height, elapsed, pointerX, pointerY, progress, backgroundStars);

    const centerX = width * 0.5;
    const centerY = height * 0.46;
    const cameraX = pointerX * 48;
    const cameraY = pointerY * 34;
    const worldSpeed = reducedMotion ? 3.2 : 6.2 + progress * 3.4;

    const projected = [];

    for (let i = 0; i < spheres.length; i += 1) {
      const sphere = spheres[i];

      sphere.x += sphere.vx + Math.sin(elapsed * 0.0006 + sphere.twinklePhase) * 0.08;
      sphere.y += sphere.vy + Math.cos(elapsed * 0.00055 + sphere.twinklePhase) * 0.08;
      sphere.z -= worldSpeed;

      if (sphere.x > world.maxX || sphere.x < -world.maxX) sphere.vx *= -1;
      if (sphere.y > world.maxY || sphere.y < -world.maxY) sphere.vy *= -1;
      if (sphere.z < world.nearZ) resetSphereDepth(sphere);

      const dz = sphere.z;
      const scale = world.projection / dz;

      const px = centerX + (sphere.x - cameraX) * scale;
      const py = centerY + (sphere.y - cameraY) * scale;
      if (px < -120 || px > width + 120 || py < -120 || py > height + 120) continue;

      const twinkle = 0.6 + Math.sin(elapsed * sphere.twinkleSpeed + sphere.twinklePhase) * 0.4;
      const depthFade = clamp(1 - dz / world.farZ, 0.08, 1);
      const alpha = depthFade * twinkle * sphere.intensity;
      const radius = clamp(sphere.size * scale * 2.4, 0.12, 2.2);

      projected.push({
        x: px,
        y: py,
        r: radius,
        a: alpha,
        z: dz,
        color: SPHERE_PALETTE[sphere.colorIndex],
        sparkle: sphere.sparkle,
      });
    }

    projected.sort((a, b) => b.z - a.z);

    for (let i = 0; i < projected.length; i += 1) {
      const a = projected[i];

      for (let j = i + 1; j < projected.length && j < i + 8; j += 1) {
        const b = projected[j];
        const dist = Math.hypot(a.x - b.x, a.y - b.y);
        if (dist > 62) continue;

        const linkAlpha = (1 - dist / 62) * Math.min(a.a, b.a) * 0.15;
        if (linkAlpha < 0.018) continue;

        ctx.strokeStyle = `rgba(125, 211, 252, ${linkAlpha.toFixed(3)})`;
        ctx.lineWidth = 0.75;
        ctx.beginPath();
        ctx.moveTo(a.x, a.y);
        ctx.lineTo(b.x, b.y);
        ctx.stroke();
      }
    }

    for (let i = 0; i < projected.length; i += 1) {
      const sphere = projected[i];
      const [r, g, b] = sphere.color;
      const glowSize = sphere.r * 2.6;

      const glow = ctx.createRadialGradient(
        sphere.x,
        sphere.y,
        0,
        sphere.x,
        sphere.y,
        glowSize
      );
      glow.addColorStop(0, `rgba(${r}, ${g}, ${b}, ${Math.min(sphere.a + 0.2, 0.9).toFixed(3)})`);
      glow.addColorStop(1, `rgba(${r}, ${g}, ${b}, 0)`);

      ctx.fillStyle = glow;
      ctx.beginPath();
      ctx.arc(sphere.x, sphere.y, glowSize, 0, Math.PI * 2);
      ctx.fill();

      ctx.fillStyle = `rgba(${r}, ${g}, ${b}, ${Math.min(sphere.a + 0.28, 1).toFixed(3)})`;
      ctx.beginPath();
      ctx.arc(sphere.x, sphere.y, sphere.r, 0, Math.PI * 2);
      ctx.fill();

      if (sphere.sparkle > 0.84 && sphere.a > 0.3) {
        const spikeAlpha = Math.min(sphere.a * 0.35, 0.22);
        const spikeLen = sphere.r * 5.2;
        ctx.strokeStyle = `rgba(${r}, ${g}, ${b}, ${spikeAlpha.toFixed(3)})`;
        ctx.lineWidth = 0.65;
        ctx.beginPath();
        ctx.moveTo(sphere.x - spikeLen, sphere.y);
        ctx.lineTo(sphere.x + spikeLen, sphere.y);
        ctx.moveTo(sphere.x, sphere.y - spikeLen);
        ctx.lineTo(sphere.x, sphere.y + spikeLen);
        ctx.stroke();
      }
    }

    const sweepY = height * (0.14 + progress * 0.72);
    const scan = ctx.createLinearGradient(0, sweepY - 78, 0, sweepY + 78);
    scan.addColorStop(0, 'rgba(129, 140, 248, 0)');
    scan.addColorStop(0.5, 'rgba(129, 140, 248, 0.035)');
    scan.addColorStop(1, 'rgba(129, 140, 248, 0)');
    ctx.fillStyle = scan;
    ctx.fillRect(0, sweepY - 78, width, 156);

    if (!persistUntilLoadingDone && progress >= 1) {
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
  if (!persistUntilLoadingDone) {
    timeoutId = window.setTimeout(() => finishIntro(), introDuration + 120);
  }

  window.addEventListener('beforeunload', cleanup);
};
