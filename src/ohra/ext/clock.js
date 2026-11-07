// @ts-check
// The page side of ohra.ext.fastapi.synced: this page on the server's clock.
//
//   import { connect } from "/ohra/clock.js";
//   const conn = connect("/ws/player", (msg) => { ... });
//   conn.send({ type: "hit" });   // JSON; dropped while disconnected
//   conn.now();                   // the server's clock now, in seconds
//   conn.error();                 // how far off now() can be, in seconds
//   conn.at(t, () => { ... });    // run at server time t
//   conn.cancelAll();             // drop every pending at()
//
// Pings measure the offset of this page's clock from the server's, from the fastest ping of the
// last minute. Messages that arrive before a connection's first pong wait for it, so a t in them
// is never read against a clock that is not synced yet.
//
// The types are JSDoc, checked by tsc in CI: the file is served as it is, with no build.

/** A ping: sent (c) and its pong received (r) on the page clock, answered (s) on the server's.
 * @typedef {{ c: number, r: number, s: number }} Sample */

const local = () => performance.now() / 1000;

// Pings go every 10 s, so 6 samples is the last minute. Keeping the fastest of all time would
// freeze the offset, and a device's clock drifts against the server's.
export const SAMPLE_WINDOW = 6;

// The fastest of the last SAMPLE_WINDOW pings. A sample is {c: sent, r: received} on the page
// clock and {s} on the server clock.
/** @param {Sample[]} samples */
const fastest = (samples) =>
  samples.slice(-SAMPLE_WINDOW).reduce((a, b) => (b.r - b.c < a.r - a.c ? b : a));

/** The server clock minus the page clock.
 * @param {Sample[]} samples
 * @returns {number} */
export function offsetFrom(samples) {
  const best = fastest(samples);
  return best.s - (best.c + (best.r - best.c) / 2);
}

// How far offsetFrom can be off: the server read its clock somewhere in the round trip, and the
// offset assumes halfway. It leaves out the server's delay in answering and late page timers.
/** @param {Sample[]} samples
 * @returns {number} */
export function errorFrom(samples) {
  const best = fastest(samples);
  return (best.r - best.c) / 2;
}

// How long to wait, in ms, for server time t, with the page's clock at nowLocal: none once t has
// passed, however far ahead it is otherwise. Messages wait for the first pong, so the offset is
// synced by the time anything asks.
/** @param {number} t @param {number} offset @param {number} nowLocal
 * @returns {number} */
export function delayFor(t, offset, nowLocal) {
  return Math.max(0, (t - offset - nowLocal) * 1000);
}

/** Open a socket to path on this page's host, and hand every message but the pongs to
 * onMessage, as parsed JSON.
 * @param {string} path
 * @param {(msg: any) => void} onMessage */
export function connect(path, onMessage) {
  /** @type {WebSocket | undefined} */
  let ws;
  let offset = 0;
  /** @type {Sample[]} */
  let samples = [];
  /** @type {any[] | null} */
  let held = []; // messages of this connection before its first pong
  let backoff = 0.5;
  /** @type {Set<number>} */
  let pending = new Set();

  /** @param {unknown} frame */
  const send = (frame) => {
    if (ws?.readyState === WebSocket.OPEN) ws.send(JSON.stringify(frame));
  };
  const ping = () => send({ type: "ping", c: local() });

  function open() {
    const proto = location.protocol === "https:" ? "wss" : "ws";
    ws = new WebSocket(`${proto}://${location.host}${path}`);
    ws.onopen = () => {
      backoff = 0.5;
      samples = [];
      held = [];
      for (let i = 0; i < 5; i++) setTimeout(ping, i * 200);
    };
    ws.onmessage = (e) => {
      const m = JSON.parse(e.data);
      if (m.type === "pong") {
        samples = [...samples, { c: m.c, r: local(), s: m.s }].slice(-SAMPLE_WINDOW);
        offset = offsetFrom(samples);
        if (held !== null) {
          const ready = held;
          held = null;
          ready.forEach(onMessage);
        }
      } else if (held !== null) held.push(m);
      else onMessage(m);
    };
    ws.onclose = () => {
      setTimeout(open, backoff * 1000);
      backoff = Math.min(backoff * 2, 8);
    };
  }

  open();
  setInterval(ping, 10000);

  return {
    send,
    now: () => local() + offset,
    error: () => (samples.length ? errorFrom(samples) : Infinity), // Infinity until a pong
    /** @param {number} t @param {() => void} fn */
    at(t, fn) {
      const id = setTimeout(() => {
        pending.delete(id);
        fn();
      }, delayFor(t, offset, local()));
      pending.add(id);
    },
    cancelAll() {
      pending.forEach(clearTimeout);
      pending = new Set();
    },
  };
}
