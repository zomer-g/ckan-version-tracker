import { useState, useEffect, useMemo, useCallback, useRef } from "react";
import { useParams, Link } from "react-router-dom";
import { appendArchive, AppendSchema, AppendRows, AppendSqlResult } from "../api/client";
import SqlEditor, { SqlEditorHandle, SqlHelpNote, SqlSuggestion, SchemaReference, SchemaTable } from "../components/SqlEditor";

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
  const [data, setData] = useState<AppendRows | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const [limit, setLimit] = useState(50);
  const [offset, setOffset] = useState(0);
  const [sort, setSort] = useState<string | undefined>(undefined);
  const [order, setOrder] = useState<"asc" | "desc">("desc");
  const [q, setQ] = useState("");
  const [filters, setFilters] = useState<Record<string, string>>({});

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
      .schema(datasetId)
      .then((s) => {
        setSchema(s);
        setSqlText((prev) => prev || `SELECT *\nFROM ${s.table}\nORDER BY first_seen DESC\nLIMIT 100`);
      })
      .catch((e) => setError(e?.message || "schema error"));
  }, [datasetId]);

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
      .rows(datasetId, { limit, offset, sort, order, q: debounced.q, filters: debounced.filters })
      .then((r) => {
        setData(r);
        setError(null);
      })
      .catch((e) => setError(e?.message || "rows error"))
      .finally(() => setLoading(false));
  }, [datasetId, limit, offset, sort, order, debounced]);

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
        ? appendArchive.downloadUrl(datasetId, { sort, order, q: debounced.q, filters: debounced.filters })
        : "#",
    [datasetId, sort, order, debounced],
  );
  const downloadAllHref = useMemo(
    () => (datasetId ? appendArchive.downloadUrl(datasetId, {}) : "#"),
    [datasetId],
  );

  if (error && !schema) {
    return (
      <div className="container mt-3">
        <div className="empty-state">{error}</div>
        <Link to="/" style={{ color: "var(--primary)" }}>&larr; חזרה</Link>
      </div>
    );
  }

  return (
    <div className="container mt-3">
      <div className="page-header flex-between" style={{ flexWrap: "wrap", gap: "0.75rem" }}>
        <div>
          <h1 style={{ margin: 0 }}>{schema?.dataset_title || "ארכיון מצטבר"}</h1>
          <div className="text-sm text-muted" style={{ marginTop: "0.25rem" }}>
            ארכיון מצטבר (APPEND) · {total.toLocaleString()} שורות
            {schema?.key ? <> · מפתח: <code>{schema.key}</code></> : <> · לכידת כל מצב</>}
            {" · עמודת "}<code>first_seen</code>{" = זמן הוספת השורה"}
          </div>
        </div>
        <div className="flex" style={{ alignItems: "center", gap: "0.6rem" }}>
          <a
            href={downloadAllHref}
            style={{
              fontSize: "0.85rem", padding: "0.4rem 0.9rem",
              background: "var(--primary, #0f766e)", color: "white",
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
                background: "none", color: "var(--primary, #0f766e)",
                border: "1px solid var(--primary, #0f766e)",
                borderRadius: 4, textDecoration: "none", fontWeight: 500,
              }}
              title="הורדת התוצאה המסוננת הנוכחית כ-CSV"
            >
              &#8595; CSV — מסונן
            </a>
          )}
          {datasetId && (
            <Link to={`/versions/${datasetId}`} style={{ fontSize: "0.85rem", color: "var(--text-muted)", textDecoration: "none" }}>
              גרסאות &rarr;
            </Link>
          )}
          <Link to="/" style={{ fontSize: "0.85rem", color: "var(--text-muted)", textDecoration: "none" }}>
            &larr; חזרה
          </Link>
        </div>
      </div>

      {schema && <StorageExplainBox schema={schema} />}

      <div className="flex" style={{ gap: "0.75rem", alignItems: "center", margin: "0.5rem 0 1rem", flexWrap: "wrap" }}>
        <input
          type="search"
          value={q}
          onChange={(e) => setQ(e.target.value)}
          placeholder="חיפוש חופשי בכל העמודות…"
          style={{ flex: "1 1 280px", padding: "0.45rem 0.7rem", border: "1px solid var(--border, #d1d5db)", borderRadius: 4 }}
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
            border: "1px solid var(--primary, #0f766e)",
            background: sqlOpen ? "var(--primary, #0f766e)" : "none",
            color: sqlOpen ? "white" : "var(--primary, #0f766e)",
          }}
          title="כתיבת שאילתות SQL (קריאה בלבד)"
        >
          {"</>"} SQL
        </button>
      </div>

      {sqlOpen && (
        <div className="card" style={{ marginBottom: "1rem", padding: "1rem" }}>
          <div className="text-sm text-muted" style={{ marginBottom: "0.5rem" }}>
            שאילתת <code>SELECT</code> בלבד (קריאה בלבד, מוגבלת בזמן ובמספר שורות). הטבלה:{" "}
            <code>{schema?.table}</code> · השלמה אוטומטית של שמות עמודות
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
                background: "var(--primary, #0f766e)", color: "white",
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
                      background: "none", color: "var(--primary, #0f766e)",
                      border: "1px solid var(--primary, #0f766e)", borderRadius: 4, cursor: "pointer",
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
            <div style={{ marginTop: "0.6rem", color: "var(--danger, #dc2626)", fontSize: "0.85rem", whiteSpace: "pre-wrap" }}>
              {sqlError}
            </div>
          )}
          {sqlResult && !sqlError && (
            <div style={{ marginTop: "0.6rem", overflowX: "auto", maxHeight: 360 }}>
              <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.82rem", whiteSpace: "nowrap" }}>
                <thead>
                  <tr>
                    {sqlResult.columns.map((c) => (
                      <th key={c} style={{ textAlign: "start", padding: "0.4rem 0.6rem", position: "sticky", top: 0, zIndex: 1, background: "var(--bg-muted, #eef2f5)", borderBottom: "2px solid var(--border, #cbd5e1)" }}>{c}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {sqlResult.rows.map((row, i) => (
                    <tr key={i} style={{ borderBottom: "1px solid var(--border, #f1f5f9)" }}>
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

      <div className="card" style={{ padding: 0, overflowX: "auto" }}>
        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.85rem", whiteSpace: "nowrap" }}>
          <thead>
            <tr>
              {cols.map((c) => (
                <th
                  key={c}
                  onClick={() => toggleSort(c)}
                  title="מיון"
                  style={{ textAlign: "start", padding: "0.5rem 0.7rem", cursor: "pointer", position: "sticky", top: 0, zIndex: 1, background: "var(--bg-muted, #eef2f5)", borderBottom: "2px solid var(--border, #cbd5e1)", userSelect: "none" }}
                >
                  {c}{sort === c ? (order === "asc" ? " ▲" : " ▼") : ""}
                </th>
              ))}
            </tr>
            <tr style={{ borderBottom: "1px solid var(--border, #e5e7eb)", background: "var(--bg, #fff)" }}>
              {cols.map((c) => (
                <th key={c} style={{ padding: "0.25rem 0.4rem" }}>
                  <input
                    value={filters[c] || ""}
                    onChange={(e) => setFilters((f) => ({ ...f, [c]: e.target.value }))}
                    placeholder="סנן…"
                    aria-label={`סנן ${c}`}
                    style={{ width: "100%", minWidth: 80, padding: "0.2rem 0.35rem", border: "1px solid var(--border, #e5e7eb)", borderRadius: 3, fontSize: "0.78rem", fontWeight: 400 }}
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
              <tr key={i} style={{ borderBottom: "1px solid var(--border, #f1f5f9)" }}>
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
            style={{ padding: "0.3rem 0.8rem", border: "1px solid var(--border, #d1d5db)", borderRadius: 4, background: "none", cursor: offset === 0 ? "not-allowed" : "pointer" }}>
            &rarr; הקודם
          </button>
          <button type="button" className="btn" disabled={pageEnd >= total || loading} onClick={() => setOffset(offset + limit)}
            style={{ padding: "0.3rem 0.8rem", border: "1px solid var(--border, #d1d5db)", borderRadius: 4, background: "none", cursor: pageEnd >= total ? "not-allowed" : "pointer" }}>
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
        background: "var(--bg-muted, #f8fafc)",
        borderInlineStart: `3px solid ${diff ? "#b45309" : "var(--primary, #0f766e)"}`,
      }}
    >
      <h2 style={{ margin: "0 0 0.4rem", fontSize: "0.95rem", fontWeight: 600 }}>
        {diff ? "⚠ אופן שמירה מיוחד — מצב DIFF (לכידת שינויים)" : "אופן שמירת הנתונים"}
      </h2>
      <ul style={{ margin: 0, paddingInlineStart: "1.1rem", fontSize: "0.85rem", lineHeight: 1.65, color: "var(--text)" }}>
        <li><strong>שמירה:</strong> כל סריקה מוסיפה שורות (APPEND) לטבלה ב-PostgreSQL (NEON) בתוך OVER — נתונים ניתנים-לתשאול, לא קובץ.</li>
        <li><strong>נקודת עוגן:</strong> {anchor}</li>
        <li><strong>תיעוד שינויים:</strong> {changes}</li>
        <li><strong>חותמת זמן:</strong> לכל רשומה עמודת <code>first_seen</code> — מתי נקלטה לראשונה לארכיון.</li>
      </ul>
    </section>
  );
}
