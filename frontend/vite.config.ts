import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";

// Dev API target. Defaults to the local backend; set VITE_API_PROXY (env var or
// frontend/.env.local) to point the dev server at a deployed environment instead
// — e.g. VITE_API_PROXY=https://www.over.org.il — to work on the frontend
// without running Postgres locally. changeOrigin is required for the remote case
// so the upstream sees its own Host. Production is unaffected: the built SPA is
// served by the same origin as the API, so this proxy is dev-only.
export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, __dirname, "");
  const target = env.VITE_API_PROXY || "http://localhost:8000";
  return {
    plugins: [react()],
    server: {
      proxy: {
        // followRedirects: version downloads 302 to R2, which sends no
        // CORS header for localhost. In production the redirect is
        // followed same-origin by the browser; in dev the proxy
        // follows it server-side so GovMap layers render locally.
        "/api": { target, changeOrigin: true, followRedirects: true },
      },
    },
  };
});
