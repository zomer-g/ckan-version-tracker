import { lazy, ComponentType } from "react";

// sessionStorage flag: a reload for a missing chunk was already tried in this
// tab. Cleared on the next successful chunk load.
const RELOADED_KEY = "over:chunk-reload";

/**
 * React.lazy for a route page, surviving a deploy.
 *
 * Chunk file names carry a content hash, and a deploy replaces the container —
 * the old chunks are gone from the server. A tab opened before the deploy still
 * runs the old bundle, so its first visit to a not-yet-loaded page asks for a
 * chunk that no longer exists and the import rejects. Reloading fetches the new
 * index.html and its new chunk names, which is what the visitor needed anyway.
 *
 * Once per tab: if the import still fails right after that reload, the problem
 * is not a stale bundle (offline, server down), so the error propagates instead
 * of reloading in a loop.
 */
export function lazyPage<T extends ComponentType<any>>(
  load: () => Promise<{ default: T }>,
) {
  return lazy(async () => {
    try {
      const mod = await load();
      try { sessionStorage.removeItem(RELOADED_KEY); } catch { /* storage off */ }
      return mod;
    } catch (err) {
      let alreadyReloaded = false;
      try { alreadyReloaded = sessionStorage.getItem(RELOADED_KEY) === "1"; } catch { /* storage off */ }
      if (!alreadyReloaded) {
        try { sessionStorage.setItem(RELOADED_KEY, "1"); } catch { /* storage off */ }
        window.location.reload();
        // Keep Suspense showing its fallback until the reload takes over.
        return new Promise<never>(() => {});
      }
      throw err;
    }
  });
}
