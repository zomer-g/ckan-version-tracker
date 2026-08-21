import { useEffect } from "react";

const SITE = "גרסאות לעם";

/**
 * Give the current route its own document title (WCAG 2.4.2, Page Titled).
 *
 * The SPA used to ship one static <title> in index.html and never touch it, so
 * all 41 routes announced themselves identically — a screen-reader user tabbing
 * between windows, and anyone reading their own history or bookmarks, had
 * nothing to tell the pages apart.
 *
 * Pass `undefined` while the page is still resolving its subject (a dataset
 * name, an organization) and the site name alone is used until it arrives;
 * calling again with the real title replaces it, so a page may narrow its own
 * title as data loads without a second hook.
 */
export function useDocumentTitle(title?: string | null) {
  useEffect(() => {
    const clean = (title || "").trim();
    document.title = clean ? `${clean} — ${SITE}` : SITE;
  }, [title]);
}

export default useDocumentTitle;
