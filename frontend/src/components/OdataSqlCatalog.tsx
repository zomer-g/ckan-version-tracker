import { useCallback, useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  OdataDataset,
  odataDatasetUrl,
  odataPackageSearch,
  odataResourceQuery,
} from "../api/odata";

/**
 * Browsable catalog of מידע לעם (odata) datasets + their files, shown UNDER the
 * SQL table browser. Deliberately styled as a distinct, loud "processed data"
 * block: these are NOT tables of our system and NOT an original public source.
 * Each datastore-active resource gets a "query" button that seeds a passthrough
 * SQL (odata."<resource_id>") into the console — running it raises the banner.
 */
export default function OdataSqlCatalog({
  onQuery,
}: {
  onQuery: (sql: string) => void;
}) {
  const { i18n } = useTranslation();
  const lang = i18n.language;

  const [query, setQuery] = useState("");
  const [activeQuery, setActiveQuery] = useState("");
  const [items, setItems] = useState<OdataDataset[]>([]);
  const [count, setCount] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);
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
    } catch (e) {
      if ((e as Error).name === "AbortError") return;
      setError(true);
      setItems([]);
      setCount(0);
    } finally {
      if (abortRef.current === ctrl) setLoading(false);
    }
  }, []);

  useEffect(() => {
    load(activeQuery);
  }, [activeQuery, load]);
  useEffect(() => () => abortRef.current?.abort(), []);

  return (
    <div className="card odata-sql-catalog">
      <div className="odata-sql-head">
        <span className="processed-banner-badge">מידע מעובד</span>
        <strong>מידע לעם — קטלוג חיצוני (לא מקור ציבורי מקורי)</strong>
      </div>
      <p className="odata-sql-note">
        הפריטים כאן הם <strong>מידע מעובד</strong> מ־מידע לעם (odata.org.il)
        ו<strong>אינם מקור ציבורי מקורי</strong>. הם אינם טבלאות של המערכת — לחיצה על
        ״שאילתה״ מריצה SELECT ישירות מול odata, והתוצאה תסומן בבאנר בולט מעל שדה
        התוצאות.
      </p>

      <form
        className="odata-sql-form"
        onSubmit={(e) => {
          e.preventDefault();
          setActiveQuery(query.trim());
        }}
        role="search"
      >
        <input
          type="search"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="חיפוש פריט במידע לעם…"
          aria-label="חיפוש במידע לעם"
        />
        <button type="submit" disabled={loading}>
          {loading ? "מחפש…" : "חפש"}
        </button>
      </form>

      {error && (
        <div className="odata-sql-msg" role="alert">
          החיפוש נכשל. נסו שוב בעוד רגע.
        </div>
      )}

      {!error && (
        <>
          <div className="odata-sql-count" role="status">
            {loading ? "טוען…" : `${count.toLocaleString()} פריטים במידע לעם`}
          </div>
          <ul className="odata-sql-list">
            {items.map((d) => {
              const org = d.organization?.title || d.organization?.name;
              const open = openItem === d.name;
              const resources = d.resources || [];
              return (
                <li key={d.name} className="odata-sql-item">
                  <button
                    type="button"
                    className="odata-sql-item-head"
                    aria-expanded={open}
                    onClick={() => setOpenItem(open ? null : d.name)}
                  >
                    <span className="odata-sql-caret" aria-hidden>
                      {open ? "▼" : "◀"}
                    </span>
                    <span className="odata-sql-item-title">
                      {d.title?.trim() || d.name}
                    </span>
                    <span className="odata-sql-item-count">
                      {resources.length}
                    </span>
                  </button>
                  {open && (
                    <div className="odata-sql-files">
                      {org && <div className="odata-sql-org">{org}</div>}
                      {resources.length === 0 && (
                        <div className="odata-sql-empty">אין קבצים.</div>
                      )}
                      {resources.map((r, i) => (
                        <div key={r.id || i} className="odata-sql-file">
                          <span className="odata-file-format">
                            {(r.format || "").toUpperCase() || "—"}
                          </span>
                          <span className="odata-sql-file-name">
                            {r.name?.trim() || "קובץ"}
                          </span>
                          {r.datastore_active && r.id ? (
                            <button
                              type="button"
                              className="odata-sql-query-btn"
                              title="הרצת שאילתה על הקובץ (ישירות מול מידע לעם)"
                              onClick={() => onQuery(odataResourceQuery(r.id!))}
                            >
                              {"▶ שאילתה"}
                            </button>
                          ) : r.url ? (
                            <a
                              className="odata-sql-dl"
                              href={r.url}
                              target="_blank"
                              rel="noopener noreferrer"
                            >
                              הורדה ↓
                            </a>
                          ) : null}
                        </div>
                      ))}
                      <a
                        className="odata-sql-open"
                        href={odataDatasetUrl(d.name, lang)}
                        target="_blank"
                        rel="noopener noreferrer"
                      >
                        פתיחה במידע לעם ↗
                      </a>
                    </div>
                  )}
                </li>
              );
            })}
          </ul>
        </>
      )}
    </div>
  );
}
