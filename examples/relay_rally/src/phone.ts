// Relay Rally's phone: the court, the paddle and the puck, and the tilt or the finger that
// moves the paddle. It runs on the server's clock, through ohra/clock.
import { connect } from "ohra/clock";
import { blip, crt, fit, glow, heart, text, unlock, DIM, GREEN } from "./arcade";
import type { Frame, State, You } from "./frames";

// The court, in court units: 9 wide, 16 tall, the paddle near the bottom.
const W = 9, H = 16, PADDLE_Y = H - 1.4, PADDLE_HALF = 0.9, PUCK = 0.45;
const MAX_ANGLE = Math.PI / 3; // 60 degrees off vertical, at the paddle's edge
const BASE_SPEED = 10; // the server's: the speed of a serve, which every rally starts from

const canvas = document.getElementById("court") as HTMLCanvasElement;
const ctx = canvas.getContext("2d")!;
const form = document.getElementById("join") as HTMLFormElement;
const nameInput = document.getElementById("name") as HTMLInputElement;
crt();

const store = (() => { try { return sessionStorage; } catch { return null; } })();
// getRandomValues, as UUID helpers need https and the join URL is plain http on the LAN
const newId = () => [...crypto.getRandomValues(new Uint8Array(16))]
  .map((b) => b.toString(16).padStart(2, "0")).join("");
const id = store?.getItem("rr-id") ?? newId();
store?.setItem("rr-id", id);
nameInput.value = store?.getItem("rr-name") ?? "";

type Puck = {
  serial: number;
  t0: number;
  x0: number;
  vx: number;
  vy: number;
  speed: number;
  up: { t1: number; x1: number; vx: number; vy: number } | null; // after the hit
  done: boolean;
  bounces: number;
};
type Point = { x: number; y: number };

let conn: ReturnType<typeof connect> | null = null;
let me: You | null = null;
let state: State | null = null; // the last state frame
let puck: Puck | null = null;
let paddle = W / 2;
let flash = 0; // until when the miss flash shows, on the page clock
let trail: Point[] = [];
let sparks: (Point & { vx: number; vy: number })[] = []; // {x, y, vx, vy}, court units, thrown off by the last hit, at sparked
let sparked = 0; // when the last hit threw them, on the page clock (ms)
let shake = 0; // until when a miss shakes the court, on the page clock (ms)
let view: { s: number; ox: number; oy: number } | null = null; // court units to canvas pixels: {s, ox, oy}
let full = false; // 8 are playing: clock.js reconnects, and we get in when one leaves

form.addEventListener("submit", (e) => {
  e.preventDefault();
  unlock(); // a tap: browsers allow audio from here on
  startTilt(); // and motion, once the player allows it
  const name = nameInput.value.trim().slice(0, 12);
  store?.setItem("rr-name", name);
  form.hidden = true;
  conn = connect(`/ws/player?${new URLSearchParams({ id, name })}`, onMessage);
});

function onMessage(m: Frame) {
  switch (m.type) {
    case "full":
      full = true;
      break;
    case "you":
      me = m;
      full = false;
      break;
    case "state":
      state = m;
      if (m.phase !== "playing") puck = null;
      break;
    case "incoming":
      puck = { serial: m.serial, t0: m.t, x0: m.x * W, vx: m.vx, vy: m.vy,
        speed: Math.hypot(m.vx, m.vy), up: null, done: false, bounces: 0 };
      trail = [];
      // heads up: a buzz and a rising chime now, then a blip as it drops in
      navigator.vibrate?.([40, 60, 40]);
      blip(523, 0.08, 0.2);
      setTimeout(() => blip(784, 0.12, 0.2), 110);
      conn!.at(m.t, () => blip(880, 0.05));
      break;
  }
}

// The puck bounces off the side walls: fold a straight line into [0, W]. k is how many
// times it bounced; an odd k means it is going the other way now.
function fold(x: number) {
  const k = Math.floor(x / W);
  const u = x - k * W;
  return { x: k % 2 === 0 ? u : W - u, k };
}

// Where the puck is at server time t, in court units, or null before it drops in.
function at(t: number): Point | null {
  if (!puck || t < puck.t0) return null;
  if (puck.up) {
    const dt = t - puck.up.t1;
    return { x: fold(puck.up.x1 + puck.up.vx * dt).x, y: PADDLE_Y + puck.up.vy * dt };
  }
  const dt = t - puck.t0;
  return { x: fold(puck.x0 + puck.vx * dt).x, y: puck.vy * dt };
}

function bounced(k: number) {
  if (k !== puck!.bounces) {
    puck!.bounces = k;
    blip(440, 0.03, 0.08);
  }
}

// Hit, miss and out, each judged at the exact moment it happens, not at the frame's time.
function step(now: number) {
  if (!puck || puck.done || now < puck.t0) return;
  if (!puck.up) {
    bounced(fold(puck.x0 + puck.vx * (now - puck.t0)).k);
    const tc = puck.t0 + PADDLE_Y / puck.vy; // when it reaches the paddle's line
    if (now < tc) return;
    const x = fold(puck.x0 + puck.vx * (tc - puck.t0)).x;
    if (Math.abs(x - paddle) > PADDLE_HALF + PUCK / 2) {
      puck.done = true;
      conn!.send({ type: "miss", serial: puck.serial });
      flash = performance.now() + 400;
      shake = performance.now() + 250;
      blip(110, 0.4, 0.2);
      navigator.vibrate?.([80, 40, 120]); // Android; iPhones let no page vibrate
      return;
    }
    // where it meets the paddle sets the angle: straight up in the middle, 60 at the edges
    const angle = Math.max(-1, Math.min(1, (x - paddle) / PADDLE_HALF)) * MAX_ANGLE;
    puck.up = { t1: tc, x1: x, vx: puck.speed * Math.sin(angle), vy: -puck.speed * Math.cos(angle) };
    puck.bounces = 0;
    blip(660 * Math.min(2, puck.speed / BASE_SPEED), 0.05); // higher as the rally speeds up
    navigator.vibrate?.(15);
    sparks = Array.from({ length: 14 }, () => {
      const a = (Math.random() - 0.5) * Math.PI * 0.9; // a fan, upwards
      const v = 4 + Math.random() * 6;
      return { x, y: PADDLE_Y, vx: v * Math.sin(a), vy: -v * Math.cos(a) };
    });
    sparked = performance.now();
    return;
  }
  const up = puck.up;
  bounced(fold(up.x1 + up.vx * (now - up.t1)).k);
  const te = up.t1 + PADDLE_Y / -up.vy; // when it leaves through the top
  if (now < te) return;
  const exit = fold(up.x1 + up.vx * (te - up.t1));
  const turned = exit.k % 2 === 0 ? 1 : -1;
  const angle = turned * Math.sign(up.vx) * Math.asin(Math.min(1, Math.abs(up.vx) / puck.speed));
  puck.done = true;
  conn!.send({ type: "out", serial: puck.serial, t: te, x: exit.x / W, angle });
}

// --- the paddle follows the finger, here on the phone: no round trip -------------------------

const inCourt = (x: number) => Math.max(PADDLE_HALF, Math.min(W - PADDLE_HALF, x));
let touching = false; // a finger on the court wins over the tilt

function pointer(e: PointerEvent) {
  if (!view) return;
  const rect = canvas.getBoundingClientRect();
  const px = (e.clientX - rect.left) * (canvas.width / rect.width);
  paddle = inCourt((px - view.ox) / view.s);
}
canvas.addEventListener("pointermove", pointer);
canvas.addEventListener("pointerdown", (e) => {
  touching = true;
  pointer(e);
});
for (const end of ["pointerup", "pointercancel"]) {
  canvas.addEventListener(end, () => {
    touching = false;
    anchor = null; // the tilt picks up from where the finger left the paddle
  });
}

// --- or the tilt of the phone, where the browser allows it: on https only --------------------

const TILT_SPAN = 25; // degrees of tilt from the middle of the court to an edge
let anchor: number | null = null; // the tilt that keeps the paddle where it is, taken from the next reading

const tiltStatus = document.getElementById("tilt")!;
const showTilt = (s: string) => (tiltStatus.textContent = `TILT: ${s}`);
let readings = 0;

function tilt(e: DeviceOrientationEvent) {
  if (e.gamma === null) return;
  if (readings++ === 0) showTilt("ON");
  if (touching) return;
  anchor ??= e.gamma - ((paddle - W / 2) / (W / 2)) * TILT_SPAN;
  const target = inCourt(W / 2 + ((e.gamma - anchor) / TILT_SPAN) * (W / 2));
  paddle += (target - paddle) * 0.5; // smooths the sensor's jitter
}

// iOS asks the player first, and only from a tap: the JOIN tap calls this
function startTilt() {
  // requestPermission is iOS only, and not in TypeScript's DOM types
  const D = globalThis.DeviceOrientationEvent as unknown as
    | { requestPermission?: () => Promise<string> }
    | undefined;
  if (!D) return showTilt("NO SENSOR");
  if (!isSecureContext) return showTilt("NEEDS HTTPS"); // plain http on the LAN: the finger it is
  const listen = () => {
    showTilt("WAITING");
    addEventListener("deviceorientation", tilt);
    setTimeout(() => readings || showTilt("NO READINGS"), 2000);
  };
  if (typeof D.requestPermission === "function") {
    showTilt("ASKING");
    D.requestPermission().then(
      (answer) => (answer === "granted" ? listen() : showTilt(String(answer).toUpperCase())),
      (err: { name?: string } | undefined) => showTilt(`ERROR ${err?.name ?? err}`),
    );
  } else {
    listen();
  }
}

// --- drawing ---------------------------------------------------------------------------------

function frame() {
  requestAnimationFrame(frame);
  const { w, h } = fit(canvas);
  ctx.shadowBlur = 0;
  ctx.fillStyle = "#000";
  ctx.fillRect(0, 0, w, h);
  if (!conn) return;
  const s = Math.min(w / W, h / (H + 2)); // room for the hearts and the clock above the court
  view = { s, ox: (w - W * s) / 2, oy: (h - (H + 2) * s) / 2 + 2 * s };
  const color = me?.color ?? GREEN;
  const px = Math.max(2, Math.round(s / 6));
  if (full) return gameFull(w, h, px);
  if (!state) return text(ctx, "CONNECTING", w / 2, h / 2, px, color, "center");
  if (state.phase === "lobby") return lobby(w, h, px, color);
  if (state.phase === "over") return over(w, h, px, color);

  const mine = state.players.find((p) => p.id === me?.id);
  if (!mine?.in_game) return nextGame(w, h, px, color);
  const now = conn.now();
  step(now);
  const { ox, oy } = view;
  const shaking = (shake - performance.now()) / 250; // 1 at the miss, down to 0
  ctx.save();
  if (shaking > 0) {
    const d = 3 * px * shaking;
    ctx.translate((Math.random() - 0.5) * 2 * d, (Math.random() - 0.5) * 2 * d);
  }

  glow(ctx, color, 8); // the side walls
  ctx.globalAlpha = 0.6;
  ctx.fillRect(ox - px, oy, px, H * s);
  ctx.fillRect(ox + W * s, oy, px, H * s);
  ctx.globalAlpha = 1;

  for (let i = 0; i < 5; i++) {
    heart(ctx, ox + i * 7 * px, oy - 1.6 * s, px, color, i < (mine?.lives ?? 0));
  }
  const left = Math.max(0, Math.ceil(state.ends_at! - now)); // set while playing
  const clock = `${Math.floor(left / 60)}:${String(left % 60).padStart(2, "0")}`;
  text(ctx, clock, ox + W * s, oy - 1.6 * s, px, color, "right");

  glow(ctx, color, 16); // the paddle
  ctx.fillRect(ox + (paddle - PADDLE_HALF) * s, oy + PADDLE_Y * s, 2 * PADDLE_HALF * s, 0.35 * s);

  const p = at(now);
  ctx.save(); // the puck stays inside the court, never over the hearts and the clock
  ctx.beginPath();
  ctx.rect(ox, oy, W * s, H * s);
  ctx.clip();
  if (p && !(puck!.done && !puck!.up)) { // the puck, with a short trail
    trail = [...trail, p].slice(-6);
    trail.forEach((q, i) => {
      ctx.globalAlpha = ((i + 1) / trail.length) * 0.5;
      ctx.fillRect(ox + (q.x - PUCK / 2) * s, oy + (q.y - PUCK / 2) * s, PUCK * s, PUCK * s);
    });
    ctx.globalAlpha = 1;
    glow(ctx, "#ffffff", 20);
    ctx.fillRect(ox + (p.x - PUCK / 2) * s, oy + (p.y - PUCK / 2) * s, PUCK * s, PUCK * s);
  } else if (puck && now < puck.t0) { // incoming: a bar pulses over where it drops in
    const pulse = 0.5 + 0.5 * Math.sin(now * 18);
    ctx.globalAlpha = 0.35 + 0.65 * pulse;
    glow(ctx, color, 24);
    ctx.fillRect(ox + (puck.x0 - 1.2) * s, oy, 2.4 * s, 0.3 * s);
    ctx.globalAlpha = 1;
    text(ctx, "V", ox + puck.x0 * s, oy + 0.6 * s, px, color, "center");
  }
  const age = (performance.now() - sparked) / 1000;
  if (age < 0.4) { // the sparks of a hit fly, fall and fade
    glow(ctx, color, 10);
    ctx.globalAlpha = 1 - age / 0.4;
    for (const k of sparks) {
      const x = k.x + k.vx * age, y = k.y + k.vy * age + 12 * age * age;
      ctx.fillRect(ox + x * s - px / 2, oy + y * s - px / 2, px, px);
    }
    ctx.globalAlpha = 1;
  }
  ctx.restore(); // the clip
  ctx.restore(); // the shake

  if (mine && mine.lives === 0) text(ctx, "OUT", w / 2, oy + (H * s) / 2, px * 2, color, "center");
  if (performance.now() < flash) {
    ctx.globalAlpha = 0.25;
    glow(ctx, color, 0);
    ctx.fillRect(0, 0, w, h);
    ctx.globalAlpha = 1;
  }
}

function lobby(w: number, h: number, px: number, color: string) {
  text(ctx, "RELAY RALLY", w / 2, h * 0.18, px * 2, GREEN, "center");
  text(ctx, me?.name ?? "", w / 2, h * 0.3, px * 2, color, "center");
  state!.players.forEach((p, i) => {
    text(ctx, p.name, w / 2, h * 0.42 + i * 8 * px, px, p.connected ? p.color : DIM, "center");
  });
  if (Math.floor(performance.now() / 600) % 2 === 0) {
    text(ctx, "WAITING FOR START", w / 2, h * 0.88, px, GREEN, "center");
  }
}

function gameFull(w: number, h: number, px: number) {
  text(ctx, "GAME FULL", w / 2, h * 0.4, px * 2, GREEN, "center");
  if (Math.floor(performance.now() / 600) % 2 === 0) {
    text(ctx, "WAITING FOR A SPOT", w / 2, h * 0.55, px, GREEN, "center");
  }
}

function nextGame(w: number, h: number, px: number, color: string) { // joined during a game: you are in the next one
  text(ctx, me?.name ?? "", w / 2, h * 0.3, px * 2, color, "center");
  text(ctx, "GAME ON", w / 2, h * 0.45, px * 2, GREEN, "center");
  if (Math.floor(performance.now() / 600) % 2 === 0) {
    text(ctx, "YOU PLAY NEXT GAME", w / 2, h * 0.6, px, GREEN, "center");
  }
}

function over(w: number, h: number, px: number, color: string) {
  const won = state!.winners.includes(me?.id ?? "");
  text(ctx, won ? "YOU WIN!" : "GAME OVER", w / 2, h * 0.3, px * 2, won ? color : GREEN, "center");
  state!.players.filter((p) => state!.winners.includes(p.id)).forEach((p, i) => {
    text(ctx, p.name, w / 2, h * 0.45 + i * 8 * px, px * 1.5, p.color, "center");
  });
}

requestAnimationFrame(frame);
