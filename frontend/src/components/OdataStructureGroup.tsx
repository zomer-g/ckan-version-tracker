import { useCallback, useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  OdataDataset,
  odataDatasetUrl,
  odataPackageSearch,
} from "../api/odata";

/**
 * מידע לעם (odata) shown as the LAST group inside the SQL table browser — the
 * same narrow box as the other sources. It surfaces only the site's DATA
 * STRUCTURE: the list of items (datasets) and their files. Deliberately marked
 * as PROCESSED data, and NOT queryable — these are not system tables. Importing
 * specific files into SQL is a separate, later step.
 */
export default function OdataStructureGroup() {
  const { i18n } = useTranslation();
  const lang = i18n.language;

  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [activeQuery, setActiveQuery] = useState("");
  const [items, setItems] = useState<OdataDataset[]>([]);
  const [count, setCount] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(false);
  const [loadedOnce, setLoadedOnce] = useState(false);
  const [openItem, setOpenItem] = useState<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  const load = useCallback(async (q: string) => {
    abortRef.current?.abort();
    const ctrl = new AbortController();
    abortRef.current = ctrl;
    setLoading(true);
    setError(false);
    try {
      const { count, results } = await odataPackageSearch(q, 20, ctrl.signal);
      setItems(results);
      setCount(count);
      setLoadedOnce(true);
    } catch (e) {
      if ((e as Error).name === "AbortError") return;
      setError(true);
      setItems([]);
      setCount(0);
    } finally {
      if (abortRef.current === ctrl) setLoading(false);
    }
  }, []);

  // Lazy: only hit odata once the group is actually opened.
  useEffect(() => {
    if (open && !loadedOnce) load(activeQuery);
  }, [open, loadedOnce, activeQuery, load]);
  // Re-search on submitted query (only while open).
  useEffect(() => {
    if (open && loadedOnce) load(activeQuery);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeQuery]);
  useEffect(() => () => abortRef.current?.abort(), []);

  return (
    <div className="odata-group">
      <button
        type="button"
        className="odata-group-header"
        aria-expanded={open}
        onClick={() => setOpen((v) => !v)}
      >
        <span className="odata-group-caret" aria-hidden>
          {open ? "▼" : "◀"}
        </span>
        <span className="odata-group-badge">מידע מעובד</span>
        <span className="odata-group-name">מידע לעם</span>
        <span className="odata-group-sub">מבנה נתונים · לא לתשאול</span>
      </button>

      {open && (
        <div className="odata-group-body">
          <p className="odata-group-note">
            מבנה הנתונים של <strong>מידע לעם</strong> (odata.org.il) — רשימת הפריטים
            והקבצים. זהו <strong>מידע מעובד</strong> ולא מקור ציבורי מקורי, והוא אינו
            ניתן לתשאול. ייבוא קבצים ספציפיים ל-SQL — בהמשך.
          </p>

          <form
            className="odata-group-form"
            role="search"
            onSubmit={(e) => {
              e.preventDefault();
              setActiveQuery(query.trim());
            }}
          >
            <input
              type="search"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder="חיפוש פריט במידע לעם…"
              aria-label="חיפוש פריט במידע לעם"
            />
            <button type="submit" disabled={loading}>
              {loading ? "…" : "חפש"}
            </button>
          </form>

          {error && (
            <div className="odata-group-msg" role="alert">
              החיפוש נכשל. נסו שוב.
            </div>
          )}

          {!error && (
            <>
              <div className="odata-group-count">
                {loading
                  ? "טוען…"
                  : `${count.toLocaleString()} פריטים במידע לעם`}
              </div>
              <div className="odata-group-list">
                {items.map((d) => {
                  const org = d.organization?.title || d.organization?.name;
                  const itemOpen = openItem === d.name;
                  const resources = d.resources || [];
                  return (
                    <div key={d.name} className="odata-item">
                      <button
                        type="button"
                        className="odata-item-head"
                        aria-expanded={itemOpen}
                        onClick={() =>
                          setOpenItem(itemOpen ? null : d.name)
                        }
                      >
                        <span className="odata-item-caret" aria-hidden>
                          {itemOpen ? "▾" : "▸"}
                        </span>
                        <span className="odata-item-title">
                          {d.title?.trim() || d.name}
                        </span>
                        <span className="odata-item-count">
                          {resources.length}
                        </span>
                      </button>
                      {itemOpen && (
                        <div className="odata-item-files">
                          {org && <div className="odata-item-org">{org}</div>}
                          {resources.length === 0 && (
                            <div className="odata-item-empty">אין קבצים.</div>
                          )}
                          {resources.map((r, i) => (
                            <div key={r.id || i} className="odata-file-row">
                              <span className="odata-file-format">
                                {(r.format || "").toUpperCase() || "—"}
                              </span>
                              {r.url ? (
                                <a
                                  className="odata-file-link"
                                  href={r.url}
                                  target="_blank"
                                  rel="noopener noreferrer"
                                  title={r.name || ""}
                                >
                                  {r.name?.trim() || "קובץ"}
                                </a>
                              ) : (
                                <span className="odata-file-link">
                                  {r.name?.trim() || "קובץ"}
                                </span>
                              )}
                            </div>
                          ))}
                          <a
                            className="odata-item-open"
                            href={odataDatasetUrl(d.name, lang)}
                            target="_blank"
                            rel="noopener noreferrer"
                          >
                            פתיחה במידע לעם ↗
                          </a>
                        </div>
                      )}
                    </div>
                  );
                })}
              </div>
            </>
          )}
        </div>
      )}
    </div>
  );
}
