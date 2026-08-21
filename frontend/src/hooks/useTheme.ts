import { useCallback, useEffect, useState } from "react";

export type ThemePref = "system" | "light" | "dark";

const KEY = "over.theme";

function read(): ThemePref {
  try {
    const v = localStorage.getItem(KEY);
    return v === "light" || v === "dark" ? v : "system";
  } catch {
    return "system";
  }
}

/**
 * Apply the preference to <html>.
 *
 * "system" removes the attribute rather than stamping a value, because the
 * stylesheet's dark block is written as
 * `@media (prefers-color-scheme: dark) :root:not([data-theme="light"])` — an
 * absent attribute is what lets the OS setting through, and stamping
 * `data-theme="system"` would leave the media query matching while the
 * attribute rules sit inert.
 */
function apply(pref: ThemePref) {
  const root = document.documentElement;
  if (pref === "system") root.removeAttribute("data-theme");
  else root.setAttribute("data-theme", pref);
}

/**
 * The user-selectable colour scheme required by WCAG 1.4.8 (Visual
 * Presentation), which asks that foreground and background colours be
 * selectable by the user rather than fixed by the page.
 *
 * Read once before paint in index.html so the first frame is already correct;
 * this hook only keeps React in step and handles the toggle.
 */
export function useTheme() {
  const [pref, setPref] = useState<ThemePref>(read);

  useEffect(() => {
    apply(pref);
    try {
      if (pref === "system") localStorage.removeItem(KEY);
      else localStorage.setItem(KEY, pref);
    } catch {
      /* private mode — the choice just won't survive a reload */
    }
  }, [pref]);

  // Another tab changing the preference should not leave this one stale.
  useEffect(() => {
    const onStorage = (e: StorageEvent) => {
      if (e.key === KEY) setPref(read());
    };
    window.addEventListener("storage", onStorage);
    return () => window.removeEventListener("storage", onStorage);
  }, []);

  /** What the user is actually looking at right now, system resolved. */
  const resolved: "light" | "dark" =
    pref === "system"
      ? window.matchMedia("(prefers-color-scheme: dark)").matches
        ? "dark"
        : "light"
      : pref;

  // system → dark → light → system. Three stops, because "follow the OS" is a
  // real choice and collapsing it into a two-way switch takes it away.
  const cycle = useCallback(() => {
    setPref((p) => (p === "system" ? "dark" : p === "dark" ? "light" : "system"));
  }, []);

  return { pref, resolved, setPref, cycle };
}

export default useTheme;
