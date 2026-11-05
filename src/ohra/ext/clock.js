// The page side of ohra.ext.fastapi.synced: this page on the server's clock.
//
//   import { connect } from "/ohra/clock.js";
//   const conn = connect("/ws/player", (msg) => { ... });
//   conn.send({ type: "hit" });   // JSON; dropped while disconnected
//   conn.now();                   // the server's clock now, in seconds
//   conn.at(t, () => { ... });    // run at server time t
//   conn.cancelAll();             // drop every pending at()
//
// Pings measure the offset of this page's clock from the server's, from the fastest ping of the
// last minute. Messages that arrive before a connection's first pong wait for it, so a t in them
// is never read against a clock that is not synced yet.

const local = () => performance.now() / 1000;

// Pings go every 10 s, so 6 samples is the last minute. Keeping the fastest of all time would
// freeze the offset, and a device's clock drifts against the server's.
export const SAMPLE_WINDOW = 6;

// A t further ahead than this means the offset is wrong: run it at once rather than never.
const MAX_AHEAD_MS = 5000;

// The server clock minus the page clock, from the fastest of the last SAMPLE_WINDOW pings. A
// sample is {c: sent, r: received} on the page clock and {s} on the server clock.
export function offsetFrom(samples) {
  const best = samples.slice(-SAMPLE_WINDOW).reduce((a, b) => (b.r - b.c < a.r - a.c ? b : a));
  return best.s - (best.c + (best.r - best.c) / 2);
}

export function connect(path, onMessage) {
  let ws;
  let offset = 0;
  let samples = [];
  let held = []; // messages of this connection before its first pong
  let backoff = 0.5;
  let pending = new Set();

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
    at(t, fn) {
      const ms = (t - offset - local()) * 1000;
      const id = setTimeout(() => {
        pending.delete(id);
        fn();
      }, ms > MAX_AHEAD_MS ? 0 : Math.max(0, ms));
      pending.add(id);
    },
    cancelAll() {
      pending.forEach(clearTimeout);
      pending = new Set();
    },
  };
}
