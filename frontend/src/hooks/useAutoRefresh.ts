import { useCallback, useEffect, useRef, useState } from "react";

const KEY = "over.autorefresh";

function readPaused(): boolean {
  try {
    return localStorage.getItem(KEY) === "off";
  } catch {
    return false;
  }
}

/**
 * A polling interval the user can stop (WCAG 2.2.2 Pause, Stop, Hide) and keep
 * stopped (WCAG 2.2.4 Interruptions).
 *
 * Eight panels across the app re-fetched every 10–60s with no way to intervene:
 * a screen-reader user reading row 40 of the activity log had it replaced under
 * them mid-sentence. Beyond the explicit toggle there is a second, quieter
 * rule here — a tick is skipped while the user's focus is inside the region.
 * Refreshing the DOM under a focused element moves or destroys it, which is the
 * same disruption the criterion is about, and no one wants to hit "pause"
 * before they are allowed to read a row.
 *
 * The preference is global and persisted: pausing one panel pauses all of them,
 * because "stop updating things at me" is a statement about the app.
 */
export function useAutoRefresh(
  fn: () => void,
  intervalMs: number,
  regionRef?: React.RefObject<HTMLElement | null>
) {
  const [paused, setPaused] = useState(readPaused);
  const saved = useRef(fn);
  saved.current = fn;

  useEffect(() => {
    const onStorage = (e: StorageEvent) => {
      if (e.key === KEY) setPaused(readPaused());
    };
    window.addEventListener("storage", onStorage);
    return () => window.removeEventListener("storage", onStorage);
  }, []);

  useEffect(() => {
    if (paused) return;
    const id = window.setInterval(() => {
      const el = regionRef?.current;
      if (el && el.contains(document.activeElement)) return;
      saved.current();
    }, intervalMs);
    return () => window.clearInterval(id);
  }, [paused, intervalMs, regionRef]);

  const toggle = useCallback(() => {
    setPaused((p) => {
      const next = !p;
      try {
        if (next) localStorage.setItem(KEY, "off");
        else localStorage.removeItem(KEY);
      } catch {
        /* ignore */
      }
      window.dispatchEvent(new StorageEvent("storage", { key: KEY }));
      return next;
    });
  }, []);

  return { paused, toggle };
}

export default useAutoRefresh;

/**
 * The current global pause state, for call sites that only refresh a status
 * line and do not warrant their own visible toggle. They inherit the user's
 * choice without adding another control to the page.
 */
export function autoRefreshPaused(): boolean {
  return readPaused();
}
