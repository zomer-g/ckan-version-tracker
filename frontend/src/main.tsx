import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import App from "./App";
import "./i18n";
import "./index.css";

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <BrowserRouter>
      <App />
    </BrowserRouter>
  </React.StrictMode>
);

// Service worker (public/sw.js): installable app, cached hashed assets and an
// offline page. Production only — in dev it would cache Vite's unhashed
// modules and serve stale code across edits. After load, so registration
// never competes with the first render for bandwidth.
if (import.meta.env.PROD && "serviceWorker" in navigator) {
  window.addEventListener("load", () => {
    navigator.serviceWorker.register("/sw.js").catch(() => {
      /* unsupported context (private mode, file://) — the site works without it */
    });
  });
}
