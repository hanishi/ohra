import { defineConfig, type Plugin } from "vite";

// The pages import clock.js as "ohra/clock" (tsconfig's paths types it against the real file).
// The Python app serves it at /ohra/clock.js, so the import is left to the browser, never bundled.
const ohraClock: Plugin = {
  name: "ohra-clock",
  enforce: "pre",
  resolveId(id) {
    if (id === "ohra/clock") return { id: "/ohra/clock.js", external: true };
  },
};

// npm run dev: the game runs in uvicorn on port 8000, and Vite proxies everything but the pages.
const server = "http://localhost:8000";

export default defineConfig({
  plugins: [ohraClock],
  build: {
    outDir: "dist",
    rollupOptions: { input: { phone: "phone.html", wall: "wall.html" } },
  },
  server: {
    proxy: {
      "/ohra": server,
      "/join": server,
      "/ws": { target: "ws://localhost:8000", ws: true },
    },
  },
});
