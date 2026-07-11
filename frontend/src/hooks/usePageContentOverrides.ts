import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { pageContent } from "../api/client";

/**
 * Fetch admin-edited text overrides for a static page (e.g. "about",
 * "rationale") and merge them over the bundled i18n defaults at runtime, so
 * edits made in the admin UI go live without a redeploy.
 *
 * Overrides use the same inline-tag convention as the bundle (<1>/<2>/<strong>),
 * so <Trans> keeps rendering them correctly. Returns `true` once the fetch has
 * settled (either way) — components can ignore it; the state change simply
 * triggers a re-render so the merged text shows. Until then the page renders the
 * bundled defaults, so there is no blank flash.
 */
export function usePageContentOverrides(page: string): boolean {
  const { i18n } = useTranslation();
  const [ready, setReady] = useState(false);

  useEffect(() => {
    let cancelled = false;
    setReady(false);
    pageContent
      .get(page)
      .then((data) => {
        if (cancelled) return;
        for (const lang of Object.keys(data || {})) {
          const values = data[lang];
          if (values && Object.keys(values).length) {
            // deep=true, overwrite=true: merge these keys into the page
            // namespace, replacing matching defaults.
            i18n.addResourceBundle(lang, "translation", { [page]: values }, true, true);
          }
        }
        setReady(true);
      })
      .catch(() => {
        if (!cancelled) setReady(true);
      });
    return () => {
      cancelled = true;
    };
  }, [page, i18n]);

  return ready;
}
