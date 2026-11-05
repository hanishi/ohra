// Relay Rally's look, shared by the phone and the wall: a pixel font, hearts, phosphor glow,
// scanlines and blips. An original late-1970s arcade feel, drawn on canvas: no font files.

// 3x5 glyphs, rows top to bottom, "1" a lit pixel
const GLYPHS = {
  A: "010101111101101", B: "110101110101110", C: "011100100100011", D: "110101101101110",
  E: "111100110100111", F: "111100110100100", G: "011100101101011", H: "101101111101101",
  I: "111010010010111", J: "001001001101010", K: "101101110101101", L: "100100100100111",
  M: "101111111101101", N: "110101101101101", O: "010101101101010", P: "110101110100100",
  Q: "010101101110011", R: "110101110101101", S: "011100010001110", T: "111010010010010",
  U: "101101101101111", V: "101101101101010", W: "101101111111101", X: "101101010101101",
  Y: "101101010010010", Z: "111001010100111",
  0: "111101101101111", 1: "010110010010111", 2: "110001010100111", 3: "110001010001110",
  4: "101101111001001", 5: "111100110001110", 6: "011100111101111", 7: "111001010010010",
  8: "111101111101111", 9: "111101111001110",
  ":": "000010000010000", ".": "000000000000010", "!": "010010010000010",
  "-": "000000111000000", "?": "110001010000010", ">": "100010001010100", " ": "000000000000000",
};
const HEART = ["01010", "11111", "11111", "01110", "00100"];

export const GREEN = "#39ff6a";
export const DIM = "#0d3a1a";

// Size the canvas's backing store to its box, for sharp pixels on any screen.
export function fit(canvas) {
  const dpr = window.devicePixelRatio || 1;
  const w = Math.round(canvas.clientWidth * dpr);
  const h = Math.round(canvas.clientHeight * dpr);
  if (canvas.width !== w || canvas.height !== h) Object.assign(canvas, { width: w, height: h });
  return { w, h };
}

export function glow(ctx, color, blur = 12) {
  ctx.fillStyle = color;
  ctx.strokeStyle = color;
  ctx.shadowColor = color;
  ctx.shadowBlur = blur;
}

// Width of str at pixel size px, for centering.
export const textWidth = (str, px) => Math.max(0, String(str).length * 4 - 1) * px;

export function text(ctx, str, x, y, px, color, align = "left") {
  const s = String(str).toUpperCase();
  let left = align === "center" ? x - textWidth(s, px) / 2 : align === "right" ? x - textWidth(s, px) : x;
  glow(ctx, color, px * 2);
  for (const ch of s) {
    const g = GLYPHS[ch] ?? GLYPHS["?"];
    for (let i = 0; i < 15; i++) if (g[i] === "1") ctx.fillRect(left + (i % 3) * px, y + Math.floor(i / 3) * px, px, px);
    left += 4 * px;
  }
}

export function heart(ctx, x, y, px, color, full = true) {
  glow(ctx, full ? color : DIM, full ? px * 2 : 0);
  HEART.forEach((row, r) => [...row].forEach((on, c) => {
    if (on === "1") ctx.fillRect(x + c * px, y + r * px, px, px);
  }));
}

// Scanlines and a soft vignette over the whole page, like an old tube.
export function crt() {
  const style = document.createElement("style");
  style.textContent = `
    .crt { position: fixed; inset: 0; pointer-events: none; z-index: 10;
      background: repeating-linear-gradient(to bottom, rgba(0,0,0,0.28) 0 1px, transparent 1px 3px),
        radial-gradient(ellipse at center, transparent 60%, rgba(0,0,0,0.55) 100%); }`;
  document.head.append(style);
  document.body.append(Object.assign(document.createElement("div"), { className: "crt" }));
}

// Square-wave blips with Web Audio. Browsers start audio only after a tap: call unlock() in one.
let audio = null;
export function unlock() {
  audio ??= new AudioContext();
  audio.resume();
}
export function blip(freq, seconds = 0.06, volume = 0.15) {
  if (!audio) return;
  const osc = audio.createOscillator();
  const gain = audio.createGain();
  osc.type = "square";
  osc.frequency.value = freq;
  gain.gain.setValueAtTime(volume, audio.currentTime);
  gain.gain.exponentialRampToValueAtTime(0.001, audio.currentTime + seconds);
  osc.connect(gain).connect(audio.destination);
  osc.start();
  osc.stop(audio.currentTime + seconds);
}
