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
import SqlChartPanel, { CHART_PARAM_KEYS } from "../components/SqlChartPanel";
import QuickChartBuilder from "../components/QuickChartBuilder";
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

// The Knesset ODATA mirror is its own group — NOT the "כנסת" scraper source
// (committee-protocol datasets), which is a different thing with the same name.
const KNESSET_DB_BADGE: Badge = {
  id: "knesset-db", label: "מסד הנתונים של הכנסת",
  bg: "#e0e7ff", fg: "#3730a3", accent: "#4f46e5",
};

function badgeOf(t: CatalogTable): Badge {
  if (t.kind === "knesset") return KNESSET_DB_BADGE;
  const b = sourceBadgeFor(t.source_type, t.organization, t.ckan_id);
  return { id: b.id, label: b.label, bg: b.bg, fg: b.fg, accent: b.accent };
}

// Where a group's header links to: the source page for tracked sources (the
// same /sources/:id hierarchy), the Knesset page for the ODATA mirror.
function groupLink(b: Badge): string {
  return b.id === "knesset-db" ? "/knesset?tab=sql" : `/sources/${b.id}`;
}

// Human-readable table name. Knesset tables carry a Hebrew description that is
// far more useful than the entity-set name (KNS_Agenda) — and it's usually
// written as "שם — הסבר", so the part before the dash is the natural short
// name. Datasets use their title.
function displayName(t: CatalogTable): string {
  if (t.kind === "knesset") {
    const d = (t.description || "").trim();
    if (d) {
      const head = d.split(/\s+[—–-]\s+/)[0].trim();
      return head && head.length <= 40 ? head : d;
    }
    return t.title || t.table;
  }
  return t.title || t.table;
}

// Full text for the row tooltip: name, physical table, and the long description.
function rowTooltip(t: CatalogTable): string {
  const parts = [displayName(t), t.table];
  if (t.description && t.description !== displayName(t)) parts.push(t.description);
  return parts.join("\n");
}

const PLACEHOLDER_SQL =
  "-- בחרו טבלה מהרשימה, או כתבו שאילתה חופשית מעל כל טבלאות האתר.\n" +
  "SELECT table_schema, table_name\nFROM information_schema.tables\n" +
  "WHERE table_schema IN ('public', 'knesset')\nORDER BY 1, 2\nLIMIT 100";

// Example queries for the dropdown, ordered as a learning path: first how to
// find your way around (what tables/columns exist), then filtering and typing,
// and finally the point of this page — JOINs ACROSS sources that no single
// dataset can answer. Every one of these is verified to return rows live.
// The dataset tables are all-text with Hebrew column names (quoted, and note
// the gershayim ״ U+05F4 in "סה״כ"); the knesset tables are lower-case + typed.
const EXAMPLES: { label: string; group: string; sql: string }[] = [
  // ── צעדים ראשונים ────────────────────────────────────────────────────────
  {
    group: "צעדים ראשונים",
    label: "אילו טבלאות יש במאגר?",
    sql: `-- רשימת כל הטבלאות: public = מאגרי האתר, knesset = מסד הכנסת.
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema IN ('public', 'knesset')
ORDER BY 1, 2`,
  },
  {
    group: "צעדים ראשונים",
    label: "הצצה לטבלה — 20 השורות הראשונות",
    sql: `-- הדרך המהירה להבין מה יש בטבלה: * = כל העמודות, LIMIT = כמה שורות.
SELECT *
FROM kns_faction
LIMIT 20`,
  },
  {
    group: "צעדים ראשונים",
    label: "אילו עמודות יש בטבלה?",
    sql: `-- החליפו את שם הטבלה כדי לראות את העמודות והטיפוסים שלה.
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'kns_bill'
ORDER BY ordinal_position`,
  },
  {
    group: "צעדים ראשונים",
    label: "כמה שורות יש בטבלה?",
    sql: `-- count(*) סופר שורות. AS נותן לתוצאה שם קריא.
SELECT count(*) AS bills
FROM kns_bill`,
  },

  // ── סינון, חיפוש וקיבוץ ──────────────────────────────────────────────────
  {
    group: "סינון, חיפוש וקיבוץ",
    label: "חיפוש טקסט חופשי (ILIKE)",
    sql: `-- ILIKE = חיפוש ללא תלות ברישיות. % = "כל טקסט כאן".
SELECT knessetnum, name
FROM kns_bill
WHERE name ILIKE '%חופש המידע%'
ORDER BY knessetnum DESC
LIMIT 50`,
  },
  {
    group: "סינון, חיפוש וקיבוץ",
    label: "ספירה וקיבוץ (GROUP BY)",
    sql: `-- GROUP BY מקבץ שורות, ו-count(*) סופר כמה יש בכל קבוצה.
SELECT knessetnum, count(*) AS bills
FROM kns_bill
GROUP BY knessetnum
ORDER BY knessetnum DESC`,
  },
  {
    group: "סינון, חיפוש וקיבוץ",
    label: "עמודות בעברית — שימו לב לגרשיים",
    sql: `-- בטבלאות המאגרים שמות העמודות בעברית, ולכן חייבים גרשיים כפולים "כך".
SELECT "משרד", "שם השירות", "רמת דיגיטליות"
FROM append_servicescompass_services_fcfad5ad_4f92c758
WHERE "משרד" = 'משרד הבריאות'
LIMIT 50`,
  },
  {
    group: "סינון, חיפוש וקיבוץ",
    label: "טקסט למספר (::numeric) ומיון",
    sql: `-- במאגרים המגורדים הכל נשמר כטקסט, אז ממירים ל-numeric כדי למיין נכון.
SELECT "רשות", "סה״כ הכנסות"::numeric AS income
FROM append_munidata_budget_economy_total_income_6af4bfbf_757f42fe
WHERE "שנה" = '2024'
ORDER BY income DESC
LIMIT 20`,
  },

  // ── JOIN בין מאגרים שונים ────────────────────────────────────────────────
  {
    group: "JOIN בין מאגרים שונים",
    label: "משרדים: שירותים דיגיטליים מול שאילתות בכנסת",
    sql: `-- שני מקורות שונים לגמרי — "מצפן השירותים" של gov.il מול פיד ה-ODATA
-- של הכנסת — מחוברים לפי שם המשרד. היחס מראה כמה פיקוח פרלמנטרי סופג
-- כל משרד ביחס לגודל מערך השירותים שלו.
WITH services AS (
  SELECT "משרד" AS ministry, count(*) AS services
  FROM append_servicescompass_services_fcfad5ad_4f92c758
  GROUP BY "משרד"
),
queries AS (
  SELECT m.name AS ministry, count(*) AS queries
  FROM kns_query q
  JOIN kns_govministry m ON m.id = q.govministryid
  GROUP BY m.name
)
SELECT s.ministry, s.services, q.queries,
       round(q.queries::numeric / s.services, 2) AS queries_per_service
FROM services s
JOIN queries q USING (ministry)
ORDER BY queries_per_service DESC`,
  },
  {
    group: "JOIN בין מאגרים שונים",
    label: "רשויות מקומיות: הכנסות, ליקויי ביקורת וגרעון",
    sql: `-- שלושה מאגרים נפרדים של "מצב השלטון המקומי", מחוברים לפי רשות ושנה.
-- שימו לב: שם עמודת הגרעון נחתך ע"י Postgres (מגבלת אורך מזהה) — העתיקו כמו שהוא.
SELECT i."רשות" AS authority,
       i."סה״כ הכנסות"::numeric                       AS income,
       d.deficiencies,
       f."גרעון מצטבר נטו (גרעון מצטבר בניכו"::numeric AS net_deficit
FROM append_munidata_budget_economy_total_income_6af4bfbf_757f42fe i
JOIN (
  SELECT "רשות", sum("סה״כ מספר ליקויים"::numeric) AS deficiencies
  FROM append_munidata_budget_economy_audit_deficiencies_coun_f46b83a6
  GROUP BY "רשות"
) d ON d."רשות" = i."רשות"
JOIN append_munidata_budget_economy_net_accum_deficit_4aebd_48dad0c1 f
  ON f."רשות" = i."רשות" AND f."שנה" = i."שנה"
WHERE i."שנה" = '2024'
ORDER BY net_deficit DESC`,
  },
  {
    group: "JOIN בין מאגרים שונים",
    label: "מחוקק מול מבצע: הצבעות מליאה מול החלטות ממשלה",
    sql: `-- הכנסת שומרת תאריך אמיתי (timestamptz), והחלטות הממשלה נשמרות כטקסט
-- בפורמט DD.MM.YYYY — כאן מפרקים את שניהם לשנה ומחברים.
WITH votes AS (
  SELECT extract(year FROM votedatetime)::int AS yr, count(*) AS plenum_votes
  FROM kns_plenumvote
  GROUP BY 1
),
decisions AS (
  SELECT split_part("תאריך פרסום", '.', 3)::int AS yr, count(*) AS gov_decisions
  FROM append_policies_96dcbeac_03c59ca7
  WHERE "תאריך פרסום" ~ '^[0-9]{2}\\.[0-9]{2}\\.[0-9]{4}$'
  GROUP BY 1
)
SELECT v.yr, v.plenum_votes, d.gov_decisions,
       round(v.plenum_votes::numeric / d.gov_decisions, 2) AS votes_per_decision
FROM votes v
JOIN decisions d USING (yr)
WHERE v.yr BETWEEN 2010 AND 2026
ORDER BY v.yr`,
  },
];

// Dropdown groups, in the learning order above.
const EXAMPLE_GROUPS = ["צעדים ראשונים", "סינון, חיפוש וקיבוץ", "JOIN בין מאגרים שונים"];

export default function DataSqlPage() {
  const [searchParams, setSearchParams] = useSearchParams();

  const [tables, setTables] = useState<CatalogTable[]>([]);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [filter, setFilter] = useState("");
  const [selected, setSelected] = useState<string | null>(searchParams.get("table"));
  const [detail, setDetail] = useState<CatalogTableDetail | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  // Explicit per-source expand/collapse overrides (see isOpen below).
  const [openGroups, setOpenGroups] = useState<Record<string, boolean>>({});

  const [sqlText, setSqlText] = useState(() => searchParams.get("sql") || PLACEHOLDER_SQL);
  const [sqlResult, setSqlResult] = useState<KnessetDbSqlResult | null>(null);
  const [sqlError, setSqlError] = useState<string | null>(null);
  const [sqlRunning, setSqlRunning] = useState(false);
  const [runId, setRunId] = useState(0);
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
      out.push({ value: t.table, kind: "table", hint: displayName(t) });
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

  // Grouped + filtered table browser, mirroring the /sources hierarchy: one
  // collapsible group per source (by the same sourceBadgeFor id), sorted by size
  // like the Sources page. Search matches table name, title/description, source
  // label and tag names.
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
      if (!m.has(badge.id)) m.set(badge.id, { badge, list: [] });
      m.get(badge.id)!.list.push(t);
    }
    for (const g of m.values()) {
      g.list.sort((x, y) => displayName(x).localeCompare(displayName(y), "he"));
    }
    return [...m.values()].sort((a, b) => b.list.length - a.list.length);
  }, [filter, tables]);

  const shownTables = useMemo(() => groups.flatMap((g) => g.list), [groups]);

  // Which source group the currently-selected table lives in (kept open).
  const selectedGroupId = useMemo(() => {
    const t = selected ? tables.find((x) => x.table === selected) : null;
    return t ? badgeOf(t).id : null;
  }, [selected, tables]);

  const filterActive = filter.trim().length > 0;
  // Collapsed by default so the page opens as a readable list of SOURCES; a
  // search (or the selected table's group) auto-opens, and an explicit toggle
  // always wins.
  const isOpen = (id: string) =>
    openGroups[id] !== undefined ? openGroups[id] : (filterActive || id === selectedGroupId);
  const toggleGroup = (id: string) =>
    setOpenGroups((p) => ({ ...p, [id]: !isOpen(id) }));
  const setAllGroups = (open: boolean) =>
    setOpenGroups(Object.fromEntries(groups.map((g) => [g.badge.id, open])));

  const sqlSchemaTables = useMemo<SchemaTable[]>(
    () => shownTables.slice(0, 80).map((t) => ({
      table: t.table,
      columns: t.columns.map((c) => c.name),
      description: displayName(t),
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

  // The fetch half of "run" — no URL writes (callers own the searchParams
  // update; two setSearchParams in one tick clobber each other in react-router).
  const runFetch = useCallback((sql: string) => {
    setSqlRunning(true);
    setSqlError(null);
    dataCatalog
      .sql(sql)
      .then((r) => { setSqlResult(r); setSqlError(null); setRunId((n) => n + 1); })
      .catch((e) => { setSqlResult(null); setSqlError(e?.message || "שגיאה"); })
      .finally(() => setSqlRunning(false));
  }, []);

  const runSql = useCallback((sqlArg?: string) => {
    const sql = (typeof sqlArg === "string" ? sqlArg : sqlText).trim();
    if (!sql) return;
    if (sql.length <= 1800) {
      setSearchParams((prev) => {
        const next = new URLSearchParams(prev);
        next.set("sql", sql);
        return next;
      }, { replace: true });
    }
    runFetch(sql);
  }, [sqlText, setSearchParams, runFetch]);

  // "גרף מהיר" from the table cube: load the generated SQL into the editor, put
  // the chart config + sql in the URL in ONE write (SqlChartPanel opens the
  // chart when the result lands), run, and bring the console into view.
  const quickChart = useCallback((sql: string, chartParams: Record<string, string>) => {
    setSqlText(sql);
    placeholderRef.current = false;
    setSearchParams((prev) => {
      const next = new URLSearchParams(prev);
      for (const k of CHART_PARAM_KEYS) next.delete(k);
      for (const [k, v] of Object.entries(chartParams)) next.set(k, v);
      if (sql.length <= 1800) next.set("sql", sql);
      return next;
    }, { replace: true });
    runFetch(sql);
    window.scrollTo({ top: 0, behavior: "smooth" });
  }, [runFetch, setSearchParams]);

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
          {/* Mirrors data_catalog.CONSOLE_SEARCH_PATH. It drifted once already:
              this line still said "public, knesset" long after idx joined the
              path, so the console was telling users they had to qualify tables
              that in fact resolved bare. */}
          <span className="text-sm text-muted">SELECT בלבד · עד 1,000 שורות בתצוגה · search_path: public, knesset, idx, extensions</span>
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
            {EXAMPLE_GROUPS.map((g) => (
              <optgroup key={g} label={g}>
                {EXAMPLES.filter((ex) => ex.group === g).map((ex) => (
                  <option key={ex.label} value={ex.label}>{ex.label}</option>
                ))}
              </optgroup>
            ))}
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
            type="button" onClick={() => runSql()} disabled={sqlRunning}
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
        <SqlChartPanel columns={sqlResult.columns} rows={sqlResult.rows} resultId={runId} />
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
          {/* Expand/collapse all + total count */}
          {!loading && groups.length > 0 && (
            <div className="flex" style={{ gap: "0.5rem", alignItems: "center", marginBottom: "0.5rem" }}>
              <span className="text-sm text-muted" style={{ fontSize: "0.75rem" }}>
                {groups.length === 1 ? "מקור אחד" : `${groups.length} מקורות`}
                {" · "}
                {shownTables.length === 1 ? "טבלה אחת" : `${shownTables.length.toLocaleString()} טבלאות`}
              </span>
              <button type="button" onClick={() => setAllGroups(true)}
                style={{ marginInlineStart: "auto", fontSize: "0.72rem", background: "none", border: "none", color: "var(--primary)", cursor: "pointer", textDecoration: "underline" }}>
                הרחב הכל
              </button>
              <button type="button" onClick={() => setAllGroups(false)}
                style={{ fontSize: "0.72rem", background: "none", border: "none", color: "var(--text-muted)", cursor: "pointer", textDecoration: "underline" }}>
                כווץ הכל
              </button>
            </div>
          )}

          {loading && <div className="text-sm text-muted" style={{ padding: "0.5rem" }}>טוען את רשימת הטבלאות…</div>}
          {loadError && <div className="text-sm" style={{ padding: "0.5rem", color: "var(--danger, #dc2626)" }}>{loadError}</div>}
          {!loading && shownTables.length === 0 && !loadError && (
            <div className="text-sm text-muted" style={{ padding: "0.5rem" }}>אין טבלאות תואמות.</div>
          )}

          {groups.map(({ badge, list }) => {
            const open = isOpen(badge.id);
            return (
              <div key={badge.id} style={{ marginBottom: "0.4rem", borderInlineStart: `3px solid ${badge.accent}`, borderRadius: 4, background: "var(--bg-muted, #f8fafc)" }}>
                {/* Source header — click to expand/collapse; ↗ opens the source page */}
                <div className="flex" style={{ gap: "0.4rem", alignItems: "center", padding: "0.35rem 0.5rem" }}>
                  <button
                    type="button"
                    onClick={() => toggleGroup(badge.id)}
                    aria-expanded={open}
                    style={{
                      display: "flex", gap: "0.45rem", alignItems: "center", flex: "1 1 auto", minWidth: 0,
                      background: "none", border: "none", cursor: "pointer", textAlign: "start", padding: 0,
                    }}
                  >
                    <span aria-hidden style={{ color: "var(--text-muted)", fontSize: "0.7rem", width: 10, flex: "0 0 auto" }}>
                      {open ? "▼" : "◀"}
                    </span>
                    <span style={{
                      display: "inline-block", padding: "0.1rem 0.45rem", borderRadius: 9999,
                      background: badge.bg, color: badge.fg, fontWeight: 700, fontSize: "0.7rem",
                      whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis", maxWidth: "70%",
                    }}>
                      {badge.label}
                    </span>
                    <span className="text-muted" style={{ fontSize: "0.72rem", flex: "0 0 auto" }}>
                      {list.length === 1 ? "טבלה אחת" : `${list.length} טבלאות`}
                    </span>
                  </button>
                  <Link
                    to={groupLink(badge)}
                    title={`לעמוד המקור: ${badge.label}`}
                    style={{ fontSize: "0.72rem", color: "var(--text-muted)", textDecoration: "none", flex: "0 0 auto" }}
                  >
                    ↗
                  </Link>
                </div>

                {open && (
                  <div style={{ padding: "0 0.25rem 0.3rem" }}>
                    {list.map((t) => {
                      const active = selected === t.table;
                      return (
                        <button
                          key={t.table}
                          type="button"
                          onClick={() => pickTable(t)}
                          title={rowTooltip(t)}
                          style={{
                            display: "flex", width: "100%", gap: "0.5rem", alignItems: "center",
                            textAlign: "start", padding: "0.3rem 0.45rem", borderRadius: 4, cursor: "pointer",
                            border: "none", marginBottom: 1,
                            background: active ? "var(--bg, #fff)" : "transparent",
                            boxShadow: active ? `inset 2px 0 0 ${badge.accent}` : "none",
                          }}
                        >
                          <span style={{ flex: "1 1 auto", minWidth: 0 }}>
                            {/* Human name leads; the physical table name is the
                                secondary, LTR-isolated monospace line. */}
                            <span style={{
                              display: "block", fontSize: "0.8rem", color: "var(--text)",
                              fontWeight: active ? 700 : 500,
                              overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
                            }}>
                              {displayName(t)}
                            </span>
                            <code dir="ltr" style={{
                              display: "block", fontSize: "0.7rem", color: "var(--text-muted)",
                              unicodeBidi: "isolate", textAlign: "start",
                              overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
                            }}>
                              {t.table}
                            </code>
                          </span>
                          <span className="text-muted" style={{
                            fontSize: "0.7rem", flex: "0 0 auto", fontVariantNumeric: "tabular-nums",
                          }}>
                            {t.est_rows != null && t.est_rows > 0 ? `~${t.est_rows.toLocaleString()}` : ""}
                          </span>
                        </button>
                      );
                    })}
                  </div>
                )}
              </div>
            );
          })}
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
                  <h2 style={{ margin: 0, fontSize: "1.05rem" }}>{displayName(selectedTable)}</h2>
                  <div className="text-sm text-muted" style={{ marginTop: "0.2rem" }}>
                    <code dir="ltr" style={{ unicodeBidi: "isolate" }}>{selectedTable.table}</code>
                    {selectedTable.kind === "knesset" && selectedTable.title && (
                      <> · <code dir="ltr" style={{ unicodeBidi: "isolate" }}>{selectedTable.title}</code></>
                    )}
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
                    <span style={{ display: "inline-block", padding: "0.3rem 0.7rem", borderRadius: 9999, fontSize: "0.8rem", fontWeight: 700, background: KNESSET_DB_BADGE.bg, color: KNESSET_DB_BADGE.fg }}>
                      {KNESSET_DB_BADGE.label}
                    </span>
                  )}
                </div>
              </div>

              {/* Skip when the description is already the heading (Knesset tables). */}
              {selectedTable.description && selectedTable.description !== displayName(selectedTable) && (
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

              {/* No-SQL chart tool over the selected table */}
              <QuickChartBuilder key={selectedTable.table} table={selectedTable} onCreate={quickChart} />

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
