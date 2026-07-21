import { useState, useEffect, useMemo, useCallback, useRef } from "react";
import { Link, useSearchParams } from "react-router-dom";
import {
  dataCatalog,
  CatalogTable,
  CatalogTableDetail,
  KnessetDbSqlResult,
} from "../api/client";
import { sourceBadgeFor } from "../utils/sourceBadge";
import SourceChip from "../components/SourceChip";
import SqlChartPanel from "../components/SqlChartPanel";
import SqlEditor, {
  SqlEditorHandle,
  SqlHelpNote,
  SqlSuggestion,
  SchemaReference,
  SchemaTable,
  CopySchemaButton,
} from "../components/SqlEditor";

// DD.MM.YYYY HH:MM (Israel-style, like the other pages).
function fmtDate(value: unknown): string {
  if (!value) return "";
  const d = new Date(String(value));
  if (isNaN(d.getTime())) return String(value).slice(0, 19);
  const p = (n: number) => String(n).padStart(2, "0");
  return `${p(d.getDate())}.${p(d.getMonth() + 1)}.${d.getFullYear()} ${p(d.getHours())}:${p(d.getMinutes())}`;
}

// Client-side CSV of an in-memory SQL result (utf-8 BOM for Excel/Hebrew).
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

// Colour/label for a table's source group. Dataset tables reuse the single
// source of truth (sourceBadgeFor); Knesset schema tables get a fixed pill
// (source_type "knesset" isn't a scraper badge, so synthesize it here).
interface Badge { id: string; label: string; bg: string; fg: string; accent: string }
function badgeOf(t: CatalogTable): Badge {
  if (t.kind === "knesset") {
    return { id: "knesset", label: "כנסת", bg: "#e0e7ff", fg: "#3730a3", accent: "#4f46e5" };
  }
  const b = sourceBadgeFor(t.source_type, t.organization, t.ckan_id);
  return { id: b.id, label: b.label, bg: b.bg, fg: b.fg, accent: b.accent };
}

const PLACEHOLDER_SQL =
  "-- בחרו טבלה מהרשימה, או כתבו שאילתה חופשית מעל כל טבלאות האתר.\n" +
  "SELECT table_schema, table_name\nFROM information_schema.tables\n" +
  "WHERE table_schema IN ('public', 'knesset')\nORDER BY 1, 2\nLIMIT 100";

const EXAMPLES: { label: string; sql: string }[] = [
  {
    label: "כל הטבלאות במאגר",
    sql: "SELECT table_schema, table_name\nFROM information_schema.tables\nWHERE table_schema IN ('public', 'knesset')\nORDER BY 1, 2",
  },
  {
    label: "הצעות חוק לפי כנסת (טבלת כנסת)",
    sql: "SELECT knessetnum, count(*) AS bills\nFROM kns_bill\nGROUP BY knessetnum\nORDER BY knessetnum DESC",
  },
];

export default function DataSqlPage() {
  const [searchParams, setSearchParams] = useSearchParams();

  const [tables, setTables] = useState<CatalogTable[]>([]);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [filter, setFilter] = useState("");
  const [selected, setSelected] = useState<string | null>(searchParams.get("table"));
  const [detail, setDetail] = useState<CatalogTableDetail | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);

  const [sqlText, setSqlText] = useState(() => searchParams.get("sql") || PLACEHOLDER_SQL);
  const [sqlResult, setSqlResult] = useState<KnessetDbSqlResult | null>(null);
  const [sqlError, setSqlError] = useState<string | null>(null);
  const [sqlRunning, setSqlRunning] = useState(false);
  const sqlEditorRef = useRef<SqlEditorHandle>(null);
  const placeholderRef = useRef(!searchParams.get("sql"));

  useEffect(() => {
    dataCatalog
      .tables()
      .then((r) => { setTables(r.tables); setLoadError(null); })
      .catch((e) => setLoadError(e?.message || "שגיאה בטעינת רשימת הטבלאות"))
      .finally(() => setLoading(false));
  }, []);

  const byName = useMemo(() => {
    const m = new Map<string, CatalogTable>();
    for (const t of tables) m.set(t.table, t);
    return m;
  }, [tables]);

  // Autocomplete: every table + every distinct column (with the tables it's in).
  const sqlSuggestions = useMemo<SqlSuggestion[]>(() => {
    const out: SqlSuggestion[] = [];
    const colTables = new Map<string, Set<string>>();
    for (const t of tables) {
      out.push({ value: t.table, kind: "table", hint: t.title });
      for (const c of t.columns) {
        if (!colTables.has(c.name)) colTables.set(c.name, new Set());
        colTables.get(c.name)!.add(t.table);
      }
    }
    for (const [name, ts] of colTables) {
      const arr = [...ts];
      out.push({ value: name, kind: "column", hint: arr.length <= 3 ? arr.join(", ") : `${arr.length} טבלאות` });
    }
    return out;
  }, [tables]);

  // Grouped + filtered table browser. Search matches table name, dataset title,
  // source label, and tag names.
  const groups = useMemo(() => {
    const f = filter.trim().toLowerCase();
    const shown = tables.filter((t) => {
      if (!f) return true;
      const badge = badgeOf(t);
      return (
        t.table.toLowerCase().includes(f) ||
        t.title.toLowerCase().includes(f) ||
        (t.description || "").toLowerCase().includes(f) ||
        badge.label.toLowerCase().includes(f) ||
        t.tags.some((tag) => tag.toLowerCase().includes(f))
      );
    });
    const m = new Map<string, { badge: Badge; list: CatalogTable[] }>();
    for (const t of shown) {
      const badge = badgeOf(t);
      if (!m.has(badge.label)) m.set(badge.label, { badge, list: [] });
      m.get(badge.label)!.list.push(t);
    }
    // Sort groups by size desc (Knesset — one big group — sinks naturally); keep
    // tables alphabetical within a group.
    return new Map(
      [...m.entries()]
        .sort((a, b) => b[1].list.length - a[1].list.length)
        .map(([k, v]) => [k, { badge: v.badge, list: v.list.sort((x, y) => x.title.localeCompare(y.title, "he")) }]),
    );
  }, [filter, tables]);

  const shownTables = useMemo(
    () => [...groups.values()].flatMap((g) => g.list),
    [groups],
  );

  const sqlSchemaTables = useMemo<SchemaTable[]>(
    () => shownTables.slice(0, 80).map((t) => ({
      table: t.table,
      columns: t.columns.map((c) => c.name),
      description: t.title,
    })),
    [shownTables],
  );

  const loadDetail = useCallback((table: string) => {
    setDetailLoading(true);
    setDetail(null);
    dataCatalog
      .tableDetail(table)
      .then(setDetail)
      .catch(() => setDetail(null))
      .finally(() => setDetailLoading(false));
  }, []);

  // Restore a deep-linked ?table= once the catalog is present.
  useEffect(() => {
    if (selected && byName.has(selected) && !detail && !detailLoading) {
      loadDetail(selected);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [byName]);

  const pickTable = (t: CatalogTable) => {
    setSelected(t.table);
    loadDetail(t.table);
    const next = new URLSearchParams(searchParams);
    next.set("table", t.table);
    setSearchParams(next, { replace: true });
    // Seed the editor with a ready query (unless the user already typed one).
    if (placeholderRef.current || !sqlText.trim() || sqlText === PLACEHOLDER_SQL) {
      const ref = t.schema === "knesset" ? t.table : t.table;
      setSqlText(`SELECT *\nFROM ${ref}\nLIMIT 100`);
      placeholderRef.current = false;
    }
  };

  const runSql = useCallback(() => {
    if (!sqlText.trim()) return;
    if (sqlText.length <= 1800) {
      const next = new URLSearchParams(searchParams);
      next.set("sql", sqlText);
      setSearchParams(next, { replace: true });
    }
    setSqlRunning(true);
    setSqlError(null);
    dataCatalog
      .sql(sqlText)
      .then((r) => { setSqlResult(r); setSqlError(null); })
      .catch((e) => { setSqlResult(null); setSqlError(e?.message || "שגיאה"); })
      .finally(() => setSqlRunning(false));
  }, [sqlText, searchParams, setSearchParams]);

  const selectedTable = selected ? byName.get(selected) || null : null;

  return (
    <div className="container mt-3">
      <div className="page-header" style={{ marginBottom: "0.75rem" }}>
        <h1 style={{ margin: 0 }}>מאגר הנתונים — ממשק SQL מרכזי</h1>
        <div className="text-sm text-muted" style={{ marginTop: "0.35rem", lineHeight: 1.7 }}>
          תשאול חופשי (קריאה בלבד) מעל <strong>כל הטבלאות של האתר</strong> במקום אחד — כל
          מאגר שנשמר כטבלה ניתנת-לתשאול (NEON) ובנוסף 48 טבלאות מסד הנתונים של הכנסת.
          בחרו טבלה מהרשימה כדי לראות דוגמה מהתוכן, פרטי המקור, קישור למקור והורדת הקבצים
          הגולמיים — או כתבו SQL חופשי (כולל JOIN בין מאגרים).
        </div>
      </div>

      {/* SQL console */}
      <div className="card" style={{ padding: "1rem", marginBottom: "1rem" }}>
        <div className="flex" style={{ gap: "0.75rem", alignItems: "center", flexWrap: "wrap", marginBottom: "0.5rem" }}>
          <strong style={{ fontSize: "0.95rem" }}>{"</>"} קונסולת SQL</strong>
          <span className="text-sm text-muted">SELECT בלבד · עד 1,000 שורות בתצוגה · search_path: public, knesset</span>
          <CopySchemaButton url={dataCatalog.schemaTxtUrl(selectedTable?.table)} />
          <select
            aria-label="שאילתות לדוגמה"
            value=""
            onChange={(e) => {
              const ex = EXAMPLES.find((x) => x.label === e.target.value);
              if (ex) { setSqlText(ex.sql); setSqlResult(null); setSqlError(null); placeholderRef.current = false; }
            }}
            style={{ marginInlineStart: "auto", padding: "0.3rem 0.5rem", border: "1px solid var(--border, #d1d5db)", borderRadius: 4, fontSize: "0.82rem", maxWidth: 260 }}
          >
            <option value="">דוגמאות…</option>
            {EXAMPLES.map((ex) => <option key={ex.label} value={ex.label}>{ex.label}</option>)}
          </select>
        </div>
        <SqlHelpNote casing="preserve" />
        <SchemaReference
          tables={sqlSchemaTables}
          onInsert={(n) => sqlEditorRef.current?.insertIdentifier(n)}
        />
        <SqlEditor
          ref={sqlEditorRef}
          value={sqlText}
          onChange={setSqlText}
          onRun={runSql}
          suggestions={sqlSuggestions}
          rows={6}
        />
        <div className="flex" style={{ gap: "0.75rem", alignItems: "center", marginTop: "0.5rem", flexWrap: "wrap" }}>
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
            <span className="text-sm text-muted">
              {sqlResult.row_count.toLocaleString()} שורות{sqlResult.truncated ? " (נחתך ל-1,000)" : ""}
            </span>
          )}
          {sqlResult && sqlResult.rows.length > 0 && (
            <button
              type="button"
              onClick={() => downloadRowsCsv("over_query.csv", sqlResult.columns, sqlResult.rows)}
              style={{
                fontSize: "0.82rem", padding: "0.3rem 0.7rem", background: "none",
                color: "var(--primary, #0f766e)", border: "1px solid var(--primary, #0f766e)",
                borderRadius: 4, cursor: "pointer",
              }}
              title="הורדת התוצאה המוצגת כ-CSV"
            >
              &#8595; CSV — תוצאה
            </button>
          )}
          {sqlText.trim() && (
            <a
              href={dataCatalog.exportUrl(sqlText)}
              style={{ fontSize: "0.82rem", color: "var(--text-muted)", textDecoration: "underline" }}
              title="הרצת השאילתה בשרת וייצוא מלא (עד 200,000 שורות)"
            >
              ייצוא מלא מהשרת (עד 200 אלף שורות)
            </a>
          )}
        </div>
        {sqlError && (
          <div style={{ marginTop: "0.6rem", color: "var(--danger, #dc2626)", fontSize: "0.85rem", whiteSpace: "pre-wrap" }}>
            {sqlError}
          </div>
        )}
        {sqlResult && !sqlError && (
          <div style={{ marginTop: "0.6rem", overflowX: "auto", maxHeight: 420 }}>
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

      {/* Charts over the current result */}
      {sqlResult && !sqlError && sqlResult.rows.length > 0 && (
        <SqlChartPanel columns={sqlResult.columns} rows={sqlResult.rows} />
      )}

      {/* Table browser + detail cube */}
      <div style={{ display: "flex", gap: "1rem", alignItems: "flex-start", flexWrap: "wrap" }}>
        <div className="card" style={{ flex: "1 1 340px", minWidth: 300, padding: "0.75rem", maxHeight: 620, overflowY: "auto" }}>
          <input
            type="search"
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            placeholder="חיפוש טבלה, מאגר, מקור או תגית…"
            aria-label="חיפוש טבלאות"
            style={{ width: "100%", padding: "0.4rem 0.6rem", border: "1px solid var(--border, #d1d5db)", borderRadius: 4, marginBottom: "0.5rem" }}
          />
          {loading && <div className="text-sm text-muted" style={{ padding: "0.5rem" }}>טוען את רשימת הטבלאות…</div>}
          {loadError && <div className="text-sm" style={{ padding: "0.5rem", color: "var(--danger, #dc2626)" }}>{loadError}</div>}
          {!loading && shownTables.length === 0 && !loadError && (
            <div className="text-sm text-muted" style={{ padding: "0.5rem" }}>אין טבלאות תואמות.</div>
          )}
          {[...groups.entries()].map(([label, { badge, list }]) => (
            <div key={label} style={{ marginBottom: "0.6rem" }}>
              <div className="flex" style={{ gap: "0.4rem", alignItems: "center", padding: "0.25rem 0.2rem" }}>
                <span aria-hidden style={{ width: 9, height: 9, borderRadius: "50%", background: badge.accent, flex: "0 0 auto" }} />
                <span style={{ fontWeight: 700, fontSize: "0.85rem", color: "var(--text-muted)" }}>{label}</span>
                <span className="text-sm text-muted" style={{ fontSize: "0.75rem" }}>({list.length})</span>
              </div>
              {list.map((t) => (
                <button
                  key={t.table}
                  type="button"
                  onClick={() => pickTable(t)}
                  title={t.title}
                  style={{
                    display: "flex", width: "100%", gap: "0.5rem", alignItems: "center",
                    textAlign: "start", padding: "0.35rem 0.5rem", borderRadius: 4, cursor: "pointer",
                    border: "none",
                    background: selected === t.table ? "var(--bg-muted, #eef2f5)" : "none",
                  }}
                >
                  <span style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", flex: "1 1 auto" }}>
                    <code style={{ fontSize: "0.78rem" }}>{t.table}</code>
                    <span className="text-sm text-muted" style={{ fontSize: "0.72rem", display: "block", overflow: "hidden", textOverflow: "ellipsis" }}>{t.title}</span>
                  </span>
                  <span className="text-sm text-muted" style={{ marginInlineStart: "auto", fontSize: "0.72rem", flex: "0 0 auto" }}>
                    {t.est_rows != null && t.est_rows > 0 ? `~${t.est_rows.toLocaleString()}` : ""}
                  </span>
                </button>
              ))}
            </div>
          ))}
        </div>

        {/* Detail cube */}
        <div className="card" style={{ flex: "2 1 440px", minWidth: 320, padding: "1rem" }}>
          {!selectedTable && (
            <div className="text-sm text-muted" style={{ lineHeight: 1.7 }}>
              <p style={{ marginTop: 0 }}>בחרו טבלה מהרשימה כדי לראות דוגמה מהתוכן, פרטי המקור, קישור למקור והורדת הקבצים הגולמיים.</p>
              <p style={{ marginBottom: 0 }}>הרשימה כוללת כל מאגר שנשמר כטבלה ניתנת-לתשאול, מקובצת לפי המקור. אפשר גם לכתוב SQL חופשי מעל הכל, כולל JOIN בין מאגרים ומעל טבלאות הכנסת.</p>
            </div>
          )}
          {selectedTable && (
            <>
              <div className="flex-between" style={{ flexWrap: "wrap", gap: "0.5rem", alignItems: "flex-start" }}>
                <div>
                  <h2 style={{ margin: 0, fontSize: "1.05rem" }}>{selectedTable.title}</h2>
                  <div className="text-sm text-muted" style={{ marginTop: "0.2rem" }}>
                    <code>{selectedTable.table}</code>
                    {" · "}
                    {(detail?.row_count ?? selectedTable.est_rows) != null
                      ? `${(detail?.row_count ?? selectedTable.est_rows)!.toLocaleString()} שורות`
                      : ""}
                  </div>
                </div>
                <div className="flex" style={{ gap: "0.4rem", alignItems: "center", flexWrap: "wrap" }}>
                  {selectedTable.kind === "dataset" ? (
                    <SourceChip sourceType={selectedTable.source_type} organization={selectedTable.organization} ckanId={selectedTable.ckan_id} size="md" />
                  ) : (
                    <span style={{ display: "inline-block", padding: "0.3rem 0.7rem", borderRadius: 9999, fontSize: "0.8rem", fontWeight: 700, background: "#e0e7ff", color: "#3730a3" }}>כנסת</span>
                  )}
                </div>
              </div>

              {selectedTable.description && (
                <p className="text-sm" style={{ margin: "0.5rem 0 0.5rem", lineHeight: 1.6 }}>{selectedTable.description}</p>
              )}

              {selectedTable.tags.length > 0 && (
                <div className="flex" style={{ gap: "0.35rem", flexWrap: "wrap", margin: "0.4rem 0" }}>
                  {selectedTable.tags.map((tag) => (
                    <span key={tag} style={{ fontSize: "0.72rem", padding: "0.1rem 0.5rem", borderRadius: 9999, background: "var(--bg-muted, #eef2f5)", color: "var(--text-muted)" }}>{tag}</span>
                  ))}
                </div>
              )}

              {/* Actions */}
              <div className="flex" style={{ gap: "0.5rem", flexWrap: "wrap", margin: "0.6rem 0 0.4rem" }}>
                <button
                  type="button"
                  onClick={() => { setSqlText(`SELECT *\nFROM ${selectedTable.table}\nLIMIT 100`); placeholderRef.current = false; }}
                  style={{ fontSize: "0.8rem", padding: "0.3rem 0.7rem", borderRadius: 4, border: "1px solid var(--primary)", background: "none", color: "var(--primary)", cursor: "pointer" }}
                >
                  {"</>"} שאילתה מוכנה
                </button>
                <a href={selectedTable.source_url} target="_blank" rel="noreferrer"
                   style={{ fontSize: "0.8rem", padding: "0.3rem 0.7rem", borderRadius: 4, border: "1px solid var(--border, #d1d5db)", color: "var(--text)", textDecoration: "none" }}>
                  ↗ מקור
                </a>
                {selectedTable.kind === "dataset" && detail?.csv_url && (
                  <a href={detail.csv_url}
                     style={{ fontSize: "0.8rem", padding: "0.3rem 0.7rem", borderRadius: 4, background: "var(--primary, #0f766e)", color: "white", textDecoration: "none", fontWeight: 500 }}>
                    &#8595; CSV — כל הנתונים
                  </a>
                )}
                {selectedTable.kind === "knesset" && (
                  <a href={dataCatalog.exportUrl(`SELECT * FROM ${selectedTable.table}`)}
                     style={{ fontSize: "0.8rem", padding: "0.3rem 0.7rem", borderRadius: 4, background: "var(--primary, #0f766e)", color: "white", textDecoration: "none", fontWeight: 500 }}>
                    &#8595; CSV — כל הנתונים
                  </a>
                )}
                {selectedTable.archive_url && (
                  <Link to={selectedTable.archive_url}
                        style={{ fontSize: "0.8rem", padding: "0.3rem 0.7rem", borderRadius: 4, border: "1px solid var(--border, #d1d5db)", color: "var(--text)", textDecoration: "none" }}>
                    ארכיון מלא →
                  </Link>
                )}
                {selectedTable.page_url && (
                  <Link to={selectedTable.page_url}
                        style={{ fontSize: "0.8rem", padding: "0.3rem 0.7rem", borderRadius: 4, border: "1px solid var(--border, #d1d5db)", color: "var(--text)", textDecoration: "none" }}>
                    עמוד הכנסת →
                  </Link>
                )}
              </div>

              {/* Raw source files */}
              {detail && detail.files.length > 0 && (
                <div className="text-sm" style={{ margin: "0.4rem 0" }}>
                  <span className="text-muted">קבצים גולמיים: </span>
                  {detail.files.map((f, i) => (
                    <span key={f.name}>
                      {i > 0 && " · "}
                      <a href={f.url} style={{ color: "var(--primary)" }}>{f.name}</a>
                    </span>
                  ))}
                  {selectedTable.versions_url && (
                    <> · <Link to={selectedTable.versions_url} style={{ color: "var(--text-muted)" }}>כל הגרסאות והקבצים →</Link></>
                  )}
                </div>
              )}
              {detail && detail.files.length === 0 && selectedTable.versions_url && selectedTable.kind === "dataset" && (
                <div className="text-sm text-muted" style={{ margin: "0.4rem 0" }}>
                  אין קבצי מקור נפרדים (המאגר נשמר כטבלה בלבד).{" "}
                  <Link to={selectedTable.versions_url} style={{ color: "var(--text-muted)" }}>גרסאות →</Link>
                </div>
              )}

              {/* Sample rows cube */}
              <div style={{ marginTop: "0.6rem" }}>
                <div className="text-sm text-muted" style={{ marginBottom: "0.3rem" }}>דוגמה מהתוכן (עד 20 שורות):</div>
                {detailLoading && <div className="text-sm text-muted">טוען דוגמה…</div>}
                {detail && detail.sample.rows.length === 0 && !detailLoading && (
                  <div className="text-sm text-muted">אין עדיין שורות בטבלה.</div>
                )}
                {detail && detail.sample.rows.length > 0 && (
                  <div style={{ overflowX: "auto", maxHeight: 360, border: "1px solid var(--border, #e5e7eb)", borderRadius: 4 }}>
                    <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.8rem", whiteSpace: "nowrap" }}>
                      <thead>
                        <tr>
                          {detail.sample.columns.map((c) => (
                            <th key={c} style={{ textAlign: "start", padding: "0.35rem 0.6rem", position: "sticky", top: 0, background: "var(--bg-muted, #eef2f5)", borderBottom: "2px solid var(--border, #cbd5e1)" }}>{c}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {detail.sample.rows.map((row, i) => (
                          <tr key={i} style={{ borderBottom: "1px solid var(--border, #f1f5f9)" }}>
                            {detail.sample.columns.map((c) => (
                              <td key={c} style={{ padding: "0.3rem 0.6rem" }}>
                                {c === "first_seen" ? fmtDate(row[c]) : String(row[c] ?? "")}
                              </td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  );
}
