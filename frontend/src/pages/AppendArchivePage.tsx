import { useState, useEffect, useMemo, useCallback, useRef } from "react";
import { useParams, Link } from "react-router-dom";
import { appendArchive, AppendSchema, AppendTableRef, AppendRows, AppendSqlResult } from "../api/client";
import SqlEditor, { SqlEditorHandle, SqlHelpNote, SqlSuggestion, SchemaReference, SchemaTable, CopySchemaButton } from "../components/SqlEditor";

import { useDocumentTitle } from "../hooks/useDocumentTitle";
import { Breadcrumbs } from "../components/a11y";
// DD.MM.YYYY HH:MM for the first_seen timestamps (Israel-style, like VersionsPage).
function fmtDate(value: string | null): string {
  if (!value) return "";
  const d = new Date(value);
  if (isNaN(d.getTime())) return value.slice(0, 19);
  const p = (n: number) => String(n).padStart(2, "0");
  return `${p(d.getDate())}.${p(d.getMonth() + 1)}.${d.getFullYear()} ${p(d.getHours())}:${p(d.getMinutes())}`;
}

const PAGE_SIZES = [50, 100, 200];

// Build a CSV (utf-8 BOM for Excel/Hebrew) from columns + rows and trigger a
// browser download — used for the SQL result (rows already in memory).
function downloadRowsCsv(
  filename: string,
  columns: string[],
  rows: Array<Record<string, unknown>>,
) {
  const esc = (v: unknown) => {
    const s = v == null ? "" : String(v);
    return /[",\n\r]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };
  const lines = [columns.map(esc).join(",")];
  for (const r of rows) lines.push(columns.map((c) => esc(r[c])).join(","));
  const blob = new Blob(["﻿" + lines.join("\r\n")], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

export default function AppendArchivePage() {
  const { datasetId } = useParams<{ datasetId: string }>();
  const [schema, setSchema] = useState<AppendSchema | null>(null);
  useDocumentTitle(schema?.dataset_title ? `ארכיון — ${schema.dataset_title}` : "ארכיון");
  const [data, setData] = useState<AppendRows | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Which table of a multi-resource dataset (append_db_multi) is being viewed.
  // undefined = whatever the server picks first, which is the only sane initial
  // state: we do not know the dataset's tables until /schema answers.
  const [table, setTable] = useState<string | undefined>(undefined);

  const [limit, setLimit] = useState(50);
  const [offset, setOffset] = useState(0);
  const [sort, setSort] = useState<string | undefined>(undefined);
  const [order, setOrder] = useState<"asc" | "desc">("desc");
  const [q, setQ] = useState("");
  const [filters, setFilters] = useState<Record<string, string>>({});
  // Sampling archives hold several rows per item — one per time it was sampled.
  // ON = the register as it stands now (newest sample of each item); OFF = the
  // full history. Only offered when the dataset declares an item key.
  const [latest, setLatest] = useState(false);

  // SQL console
  const sqlEditorRef = useRef<SqlEditorHandle>(null);
  const [sqlOpen, setSqlOpen] = useState(false);
  const [sqlText, setSqlText] = useState("");
  const [sqlResult, setSqlResult] = useState<AppendSqlResult | null>(null);
  const [sqlError, setSqlError] = useState<string | null>(null);
  const [sqlRunning, setSqlRunning] = useState(false);

  // Debounce text inputs so each keystroke doesn't fire a query.
  const [debounced, setDebounced] = useState({ q: "", filters: {} as Record<string, string> });
  const tRef = useRef<number | undefined>(undefined);
  useEffect(() => {
    window.clearTimeout(tRef.current);
    tRef.current = window.setTimeout(() => {
      setOffset(0);
      setDebounced({ q, filters });
    }, 350);
    return () => window.clearTimeout(tRef.current);
  }, [q, filters]);

  useEffect(() => {
    if (!datasetId) return;
    appendArchive
      .schema(datasetId, table)
      .then((s) => {
        setSchema(s);
        setSqlText((prev) => prev || `SELECT *\nFROM ${s.table}\nORDER BY first_seen DESC\nLIMIT 100`);
      })
      .catch((e) => setError(e?.message || "schema error"));
  }, [datasetId, table]);

  const runSql = useCallback(() => {
    if (!datasetId || !sqlText.trim()) return;
    setSqlRunning(true);
    setSqlError(null);
    appendArchive
      .sql(datasetId, sqlText)
      .then((r) => { setSqlResult(r); setSqlError(null); })
      .catch((e) => { setSqlResult(null); setSqlError(e?.message || "שגיאה"); })
      .finally(() => setSqlRunning(false));
  }, [datasetId, sqlText]);

  const load = useCallback(() => {
    if (!datasetId) return;
    setLoading(true);
    appendArchive
      .rows(datasetId, { limit, offset, sort, order, q: debounced.q, table, latest, filters: debounced.filters })
      .then((r) => {
        setData(r);
        setError(null);
      })
      .catch((e) => setError(e?.message || "rows error"))
      .finally(() => setLoading(false));
  }, [datasetId, limit, offset, sort, order, debounced, table, latest]);

  useEffect(() => {
    load();
  }, [load]);

  const cols = schema?.columns || data?.columns || [];

  // Autocomplete suggestions for the SQL editor: the table + each column.
  const sqlSuggestions = useMemo<SqlSuggestion[]>(() => {
    if (!schema) return [];
    return [
      { value: schema.table, kind: "table", hint: schema.dataset_title || "" },
      ...schema.columns.map((c) => ({
        value: c, kind: "column" as const,
        hint: c === schema.first_seen_column ? "זמן הוספה לארכיון" : c === schema.key ? "מפתח" : "",
      })),
    ];
  }, [schema]);

  const sqlSchemaTables = useMemo<SchemaTable[]>(
    () => (schema ? [{ table: schema.table, columns: schema.columns, description: schema.dataset_title }] : []),
    [schema],
  );

  const total = data?.total ?? schema?.total ?? 0;
  const pageStart = total === 0 ? 0 : offset + 1;
  const pageEnd = Math.min(offset + limit, total);

  function toggleSort(col: string) {
    if (sort === col) {
      setOrder((o) => (o === "asc" ? "desc" : "asc"));
    } else {
      setSort(col);
      setOrder("asc");
    }
    setOffset(0);
  }

  const hasFilter = !!(debounced.q || Object.values(debounced.filters).some(Boolean));
  const downloadHref = useMemo(
    () =>
      datasetId
        ? appendArchive.downloadUrl(datasetId, { sort, order, q: debounced.q, table, latest, filters: debounced.filters })
        : "#",
    [datasetId, sort, order, debounced, table, latest],
  );
  const downloadAllHref = useMemo(
    // "הכל" means all of the table on screen — for a multi-resource dataset the
    // tables can have different schemas, so there is no one CSV of the dataset.
    // It also follows the latest/history toggle: exporting the full sampling
    // history while the screen shows one row per item would be a different
    // dataset than the one being looked at.
    () => (datasetId ? appendArchive.downloadUrl(datasetId, { table, latest }) : "#"),
    [datasetId, table, latest],
  );

  if (error && !schema) {
    return (
      <div className="container mt-3">
        <div className="empty-state">{error}</div>
        <Link to="/" style={{ color: "var(--primary)" }}>&larr; כל המאגרים</Link>
      </div>
    );
  }

  return (
    <div className="container mt-3">
      <div className="page-header flex-between" style={{ flexWrap: "wrap", gap: "0.75rem" }}>
        <div>
          <Breadcrumbs items={[{ label: "מאגרים", to: "/" }, { label: schema?.dataset_title || "ארכיון מצטבר" }]} />
          <h1 style={{ margin: 0 }}>{schema?.dataset_title || "ארכיון מצטבר"}</h1>
          <div className="text-sm text-muted" style={{ marginTop: "0.25rem" }}>
            ארכיון מצטבר (APPEND) · {total.toLocaleString()} שורות
            {schema?.key ? <> · מפתח: <code>{schema.key}</code></> : <> · לכידת כל מצב</>}
            {" · עמודת "}<code>first_seen</code>{" = זמן הוספת השורה"}
            {schema?.supports_latest && (
              <> · ארכיון דגימות: <code>{schema.item_key}</code> = ישות, <code>{schema.sample_column}</code> = מועד הדגימה</>
            )}
          </div>
        </div>
        <div className="flex" style={{ alignItems: "center", gap: "0.6rem" }}>
          {/* Several rows per item is the whole point of a sampling archive and
              also the easiest thing to misread — without this toggle the grid
              shows one building file as N files. */}
          {schema?.supports_latest && (
            <label
              className="text-sm"
              style={{ display: "flex", alignItems: "center", gap: "0.3rem", whiteSpace: "nowrap" }}
              title="שורה אחת לכל ישות — הדגימה האחרונה שלה"
            >
              <input
                type="checkbox"
                checked={latest}
                onChange={(e) => { setLatest(e.target.checked); setOffset(0); }}
              />
              רק הדגימה האחרונה
            </label>
          )}
          <a
            href={downloadAllHref}
            style={{
              fontSize: "0.85rem", padding: "0.4rem 0.9rem",
              background: "var(--fill-brand)", color: "var(--on-fill-brand)",
              borderRadius: 4, textDecoration: "none", fontWeight: 500,
            }}
            title="הורדת כל הנתונים הגולמיים כ-CSV"
          >
            &#8595; CSV — הכל
          </a>
          {hasFilter && (
            <a
              href={downloadHref}
              style={{
                fontSize: "0.85rem", padding: "0.4rem 0.9rem",
                background: "none", color: "var(--primary)",
                border: "1px solid var(--primary)",
                borderRadius: 4, textDecoration: "none", fontWeight: 500,
              }}
              title="הורדת התוצאה המסוננת הנוכחית כ-CSV"
            >
              &#8595; CSV — מסונן
            </a>
          )}
          {/* Both of these used to be one word — "גרסאות" and "חזרה" — which
              named neither destination: this page is reached FROM the dataset
              page, so "חזרה" reads like it goes there while it actually goes to
              the catalog. Spell out where each one lands. */}
          {datasetId && (
            <Link
              to={`/versions/${datasetId}`}
              style={{ fontSize: "0.85rem", color: "var(--text-muted)", textDecoration: "none" }}
              title="היסטוריית הגרסאות והקבצים של המאגר הזה"
            >
              &larr; חזרה לעמוד המאגר
            </Link>
          )}
          <Link
            to="/"
            style={{ fontSize: "0.85rem", color: "var(--text-muted)", textDecoration: "none" }}
            title="הקטלוג — כל המאגרים במעקב"
          >
            כל המאגרים
          </Link>
        </div>
      </div>

      {schema?.multi_table && schema.tables && (
        <TablePicker
          tables={schema.tables}
          current={schema.table}
          onPick={(t) => {
            setTable(t);
            // A sibling table can have entirely different columns, so anything
            // scoped to the old one has to go — a stale sort or column filter
            // would come back as an error or, worse, as silently zero rows.
            setOffset(0);
            setSort(undefined);
            setFilters({});
            setQ("");
          }}
        />
      )}

      {schema && <StorageExplainBox schema={schema} />}

      <div className="flex" style={{ gap: "0.75rem", alignItems: "center", margin: "0.5rem 0 1rem", flexWrap: "wrap" }}>
        <input
          type="search"
          value={q}
          onChange={(e) => setQ(e.target.value)}
          placeholder="חיפוש חופשי בכל העמודות…"
          style={{ flex: "1 1 280px", padding: "0.45rem 0.7rem", border: "1px solid var(--border)", borderRadius: 4 }}
          aria-label="חיפוש חופשי"
        />
        <label className="text-sm text-muted">
          שורות בעמוד:{" "}
          <select value={limit} onChange={(e) => { setLimit(Number(e.target.value)); setOffset(0); }}>
            {PAGE_SIZES.map((n) => <option key={n} value={n}>{n}</option>)}
          </select>
        </label>
        <button
          type="button"
          onClick={() => setSqlOpen((o) => !o)}
          style={{
            padding: "0.45rem 0.9rem", borderRadius: 4, cursor: "pointer", fontWeight: 600,
            border: "1px solid var(--primary)",
            background: sqlOpen ? "var(--primary)" : "none",
            color: sqlOpen ? "white" : "var(--primary)",
          }}
          title="כתיבת שאילתות SQL (קריאה בלבד)"
        >
          {"</>"} SQL
        </button>
      </div>

      {sqlOpen && (
        <div className="card" style={{ marginBottom: "1rem", padding: "1rem" }}>
          <div className="flex" style={{ gap: "0.6rem", alignItems: "center", flexWrap: "wrap", marginBottom: "0.5rem" }}>
            <span className="text-sm text-muted">
              שאילתת <code>SELECT</code> בלבד (קריאה בלבד, מוגבלת בזמן ובמספר שורות). הטבלה:{" "}
              <code>{schema?.table}</code> · השלמה אוטומטית של שמות עמודות
            </span>
            {datasetId && (
              <span style={{ marginInlineStart: "auto" }}>
                <CopySchemaButton url={`/api/append/${datasetId}/schema.txt`} />
              </span>
            )}
          </div>
          <SqlHelpNote casing="preserve" />
          <SchemaReference
            tables={sqlSchemaTables}
            onInsert={(n) => sqlEditorRef.current?.insertIdentifier(n)}
            defaultOpen
          />
          <SqlEditor
            ref={sqlEditorRef}
            value={sqlText}
            onChange={setSqlText}
            onRun={runSql}
            suggestions={sqlSuggestions}
            rows={5}
          />
          <div className="flex" style={{ gap: "0.75rem", alignItems: "center", marginTop: "0.5rem" }}>
            <button
              type="button" onClick={runSql} disabled={sqlRunning}
              style={{
                padding: "0.4rem 1.1rem", borderRadius: 4, border: "none", fontWeight: 600,
                background: "var(--fill-brand)", color: "var(--on-fill-brand)",
                cursor: sqlRunning ? "wait" : "pointer", opacity: sqlRunning ? 0.7 : 1,
              }}
            >
              {sqlRunning ? "מריץ…" : "▶ הרץ"}
            </button>
            <span className="text-sm text-muted">Ctrl/⌘+Enter</span>
            {sqlResult && (
              <>
                <span className="text-sm text-muted">
                  {sqlResult.row_count.toLocaleString()} שורות{sqlResult.truncated ? " (נחתך)" : ""}
                </span>
                {sqlResult.rows.length > 0 && (
                  <button
                    type="button"
                    onClick={() => downloadRowsCsv(`${schema?.table || "query"}_sql.csv`, sqlResult.columns, sqlResult.rows)}
                    style={{
                      fontSize: "0.82rem", padding: "0.3rem 0.7rem",
                      background: "none", color: "var(--primary)",
                      border: "1px solid var(--primary)", borderRadius: 4, cursor: "pointer",
                    }}
                    title="הורדת תוצאת ה-SQL כ-CSV"
                  >
                    &#8595; CSV — תוצאת SQL
                  </button>
                )}
              </>
            )}
          </div>
          {sqlError && (
            <div style={{ marginTop: "0.6rem", color: "var(--danger)", fontSize: "0.85rem", whiteSpace: "pre-wrap" }}>
              {sqlError}
            </div>
          )}
          {sqlResult && !sqlError && (
            <div tabIndex={0} role="region" aria-label="תוצאות השאילתה" className="scroll-region" style={{ marginTop: "0.6rem", overflowX: "auto", maxHeight: 360 }}>
              <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.82rem", whiteSpace: "nowrap" }}>
                <thead>
                  <tr>
                    {sqlResult.columns.map((c) => (
                      <th scope="col" key={c} style={{ textAlign: "start", padding: "0.4rem 0.6rem", position: "sticky", top: 0, zIndex: 1, background: "var(--surface-2)", borderBottom: "2px solid var(--border)" }}>{c}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {sqlResult.rows.map((row, i) => (
                    <tr key={i} style={{ borderBottom: "1px solid var(--border)" }}>
                      {sqlResult.columns.map((c) => (
                        <td key={c} style={{ padding: "0.35rem 0.6rem" }}>{String(row[c] ?? "")}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}

      <div className="card scroll-region" tabIndex={0} role="region" aria-label="שורות הארכיון" style={{ padding: 0, overflowX: "auto" }}>
        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.85rem", whiteSpace: "nowrap" }}>
          <thead>
            <tr>
              {cols.map((c) => (
                <th
                  scope="col"
                  key={c}
                  // aria-sort tells a screen reader which column is ordering the
                  // table and in which direction — the arrow glyph said that to
                  // sighted users only (WCAG 1.3.1).
                  aria-sort={sort === c ? (order === "asc" ? "ascending" : "descending") : "none"}
                  style={{ textAlign: "start", padding: 0, position: "sticky", top: 0, zIndex: 1, background: "var(--surface-2)" }}
                >
                  {/* The click target is a real button: a <th onClick> takes no
                      focus and answers no key (WCAG 2.1.1). */}
                  <button
                    type="button"
                    onClick={() => toggleSort(c)}
                    aria-label={`מיון לפי ${c}`}
                    style={{
                      display: "flex", alignItems: "center", gap: "0.25rem",
                      width: "100%", padding: "0.5rem 0.7rem", textAlign: "start",
                      background: "none", border: 0, font: "inherit", fontWeight: 600,
                      color: "inherit", cursor: "pointer",
                    }}
                  >
                    {c}
                    <span aria-hidden="true">{sort === c ? (order === "asc" ? "▲" : "▼") : ""}</span>
                  </button>
                </th>
              ))}
            </tr>
            <tr style={{ borderBottom: "1px solid var(--border)", background: "var(--bg)" }}>
              {cols.map((c) => (
                <th scope="col" key={c} style={{ padding: "0.25rem 0.4rem" }}>
                  <input
                    value={filters[c] || ""}
                    onChange={(e) => setFilters((f) => ({ ...f, [c]: e.target.value }))}
                    placeholder="סנן…"
                    aria-label={`סנן ${c}`}
                    style={{ width: "100%", minWidth: 80, padding: "0.2rem 0.35rem", border: "1px solid var(--border)", borderRadius: 3, fontSize: "0.78rem", fontWeight: 400 }}
                  />
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {loading && (
              <tr><td colSpan={cols.length || 1} style={{ padding: "1rem", textAlign: "center", color: "var(--text-muted)" }}>טוען…</td></tr>
            )}
            {!loading && data && data.rows.length === 0 && (
              <tr><td colSpan={cols.length || 1} style={{ padding: "1rem", textAlign: "center", color: "var(--text-muted)" }}>אין שורות תואמות</td></tr>
            )}
            {!loading && data?.rows.map((row, i) => (
              <tr key={i} style={{ borderBottom: "1px solid var(--border)" }}>
                {cols.map((c) => (
                  <td key={c} style={{ padding: "0.4rem 0.7rem" }}>
                    {c === "first_seen" ? fmtDate(row[c]) : (row[c] ?? "")}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="flex-between" style={{ marginTop: "0.75rem", flexWrap: "wrap", gap: "0.5rem" }}>
        <span className="text-sm text-muted">
          {pageStart.toLocaleString()}–{pageEnd.toLocaleString()} מתוך {total.toLocaleString()}
        </span>
        <div className="flex" style={{ gap: "0.5rem" }}>
          <button type="button" className="btn" disabled={offset === 0 || loading} onClick={() => setOffset(Math.max(0, offset - limit))}
            style={{ padding: "0.3rem 0.8rem", border: "1px solid var(--border)", borderRadius: 4, background: "none", cursor: offset === 0 ? "not-allowed" : "pointer" }}>
            &rarr; הקודם
          </button>
          <button type="button" className="btn" disabled={pageEnd >= total || loading} onClick={() => setOffset(offset + limit)}
            style={{ padding: "0.3rem 0.8rem", border: "1px solid var(--border)", borderRadius: 4, background: "none", cursor: pageEnd >= total ? "not-allowed" : "pointer" }}>
            הבא &larr;
          </button>
        </div>
      </div>
    </div>
  );
}

// Config-aware "how is this data stored" box, shown at the top of the archive
// for specially-configured (append/NEON) datasets. Explains the anchor (dedup
// identity) and how changes are documented, tailored to the dataset's mode:
//   - DIFF (capture_changes): anchored on the key, changes detected by full-row
//     hash → each change becomes a new dated record (vehicle registry).
//   - keyed: only new keys captured; in-place changes are NOT recorded.
//   - keyless: every distinct row STATE captured (flights board).
/** Tab strip for a dataset archived as one NEON table PER datastore resource.
 *
 * Shown only when there really is more than one. Without it the page renders the
 * first table and says nothing — which is the same silent partial truth as the
 * bug this shipped with, just with rows in it. */
function TablePicker({
  tables, current, onPick,
}: {
  tables: AppendTableRef[];
  current: string;
  onPick: (table: string) => void;
}) {
  return (
    <div style={{ margin: "0.5rem 0 0.75rem" }}>
      <div className="text-sm text-muted" style={{ marginBottom: "0.35rem" }}>
        למאגר הזה {tables.length} טבלאות נפרדות (משאב אחד לכל טבלה) — לכל אחת עמודות משלה:
      </div>
      <div className="flex" style={{ gap: "0.4rem", flexWrap: "wrap" }}>
        {tables.map((t) => {
          const active = t.table === current;
          return (
            <button
              key={t.table}
              type="button"
              onClick={() => !active && onPick(t.table)}
              aria-current={active ? "true" : undefined}
              title={t.table}
              style={{
                fontSize: "0.85rem",
                padding: "0.35rem 0.8rem",
                cursor: active ? "default" : "pointer",
                borderRadius: 4,
                fontWeight: active ? 600 : 400,
                border: "1px solid var(--primary)",
                background: active ? "var(--primary)" : "none",
                color: active ? "white" : "var(--primary)",
              }}
            >
              {t.resource_name || t.table}
            </button>
          );
        })}
      </div>
    </div>
  );
}

function StorageExplainBox({ schema }: { schema: AppendSchema }) {
  const key = schema.key;
  const diff = !!schema.capture_changes;

  const anchor = diff ? (
    <>כל ישות מזוהה לפי <code>{key}</code> (העוגן), וזיהוי השינויים נעשה על <strong>כל תוכן השורה</strong> (טביעת-אצבע / hash).</>
  ) : key ? (
    <>כל רשומה מזוהה לפי <code>{key}</code> (העוגן).</>
  ) : (
    <>אין מפתח יחיד — העוגן הוא <strong>כל תוכן השורה</strong> (hash).</>
  );

  const changes = diff ? (
    <>בכל סריקה משווים את תוכן כל שורה מול מה שכבר נשמר. אם שורה של ישות <strong>קיימת השתנתה</strong> (למשל טסט, בעלות או צבע ברכב) — היא נשמרת כ<strong>רשומה חדשה</strong> עם <code>first_seen</code> חדש, והרשומה הקודמת נשמרת. כך נבנית <strong>היסטוריית שינויים מלאה</strong> לכל ישות; ישות חדשה לגמרי → רשומה חדשה.</>
  ) : key ? (
    <>נשמרות רק <strong>ישויות חדשות</strong> (מפתח שלא נראה קודם). <strong>שינוי</strong> בישות קיימת (אותו מפתח, תוכן אחר) <strong>אינו</strong> נלכד — נשמר המצב הראשון בלבד.</>
  ) : (
    <>כל <strong>מצב נבדל</strong> של שורה נשמר פעם אחת. כשערך משתנה (למשל סטטוס טיסה: ממתינה→המריאה→נחתה) — כל מצב נשמר כרשומה נפרדת עם <code>first_seen</code> משלו, כך שנשמרת היסטוריית כל המצבים.</>
  );

  return (
    <section
      className="card"
      aria-label="אופן שמירת הנתונים"
      style={{
        marginBottom: "1rem", padding: "0.9rem 1.1rem",
        background: "var(--surface-2)",
        borderInlineStart: `3px solid ${diff ? "#873E07" : "var(--primary)"}`,
      }}
    >
      <h2 style={{ margin: "0 0 0.4rem", fontSize: "0.95rem", fontWeight: 600 }}>
        {diff ? "⚠ אופן שמירה מיוחד — מצב DIFF (לכידת שינויים)" : "אופן שמירת הנתונים"}
      </h2>
      <ul style={{ margin: 0, paddingInlineStart: "1.1rem", fontSize: "0.85rem", lineHeight: 1.65, color: "var(--text)" }}>
        <li><strong>שמירה:</strong> כל סריקה מוסיפה שורות (APPEND) לטבלה שמורה כאן באתר — נתונים ניתנים-לתשאול, לא קובץ.</li>
        <li><strong>נקודת עוגן:</strong> {anchor}</li>
        <li><strong>תיעוד שינויים:</strong> {changes}</li>
        <li><strong>חותמת זמן:</strong> לכל רשומה עמודת <code>first_seen</code> — מתי נקלטה לראשונה לארכיון.</li>
      </ul>
    </section>
  );
}
