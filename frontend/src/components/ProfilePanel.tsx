import { useState } from "react";
import { dataCatalog } from "../api/client";
import type { TableProfile, TableProfileColumn } from "../api/client";

// Hebrew labels for the two classification axes the profiler produces.
const KIND_HE: Record<string, string> = {
  numeric: "מספר",
  date: "תאריך",
  text: "טקסט",
  empty: "ריק",
};

const ENTITY_HE: Record<string, string> = {
  locality: "יישוב",
  municipality: "רשות מקומית",
  person: "שם אדם",
  corporation: "תאגיד",
  date: "תאריך",
  amount: "סכום",
  id: "מזהה",
  number: "מספר",
  category: "קטגוריה",
  code: "קוד",
  coordinate: "קואורדינטה",
  free_text: "טקסט חופשי",
  text: "טקסט חופשי",
  other: "אחר",
};

const chip = (bg: string, fg: string): React.CSSProperties => ({
  display: "inline-block",
  padding: "0.05rem 0.4rem",
  borderRadius: 4,
  background: bg,
  color: fg,
  fontSize: "0.72rem",
  whiteSpace: "nowrap",
});

function fmtNum(v: unknown): string {
  if (v === null || v === undefined || v === "") return "—";
  const n = typeof v === "number" ? v : Number(v);
  if (!Number.isNaN(n)) return n.toLocaleString("he-IL", { maximumFractionDigits: 2 });
  return String(v).slice(0, 19); // ISO date etc.
}

function rangeText(c: TableProfileColumn): string {
  if (c.min === undefined && c.max === undefined) return "";
  if (c.min === null && c.max === null) return "";
  return `${fmtNum(c.min)} – ${fmtNum(c.max)}`;
}

// The semantic type the LLM assigned (preferred) or the SQL heuristic guess.
function entityLabel(col: string, profile: TableProfile): { he: string; llm: boolean } | null {
  const llm = profile.llm_enrichment?.columns?.[col]?.semantic_type;
  if (llm) return { he: ENTITY_HE[llm] || llm, llm: true };
  const guess = profile.sql_profile?.columns?.[col]?.entity_guess?.guess;
  if (guess && guess !== "text" && guess !== "free_text") return { he: ENTITY_HE[guess] || guess, llm: false };
  return null;
}

// ── JOIN-with-healing helper ─────────────────────────────────────────────────
// The state's raw locality/authority fields are dirty (variants, prefixes,
// spellings). These generators wrap the field in over_settlement()/over_authority()
// so a JOIN resolves the canonical name/code and mismatched values still line up.
function qi(col: string): string {
  return '"' + col.replace(/"/g, '""') + '"';
}
function tableRef(p: TableProfile): string {
  return p.schema_name && p.schema_name !== "public" ? `${p.schema_name}.${p.table_name}` : p.table_name;
}
// settlement (יישוב) vs authority (רשות); null = not a place column.
function localityKind(col: string, p: TableProfile): "settlement" | "authority" | null {
  const t = p.llm_enrichment?.columns?.[col]?.semantic_type
    || p.sql_profile?.columns?.[col]?.entity_guess?.guess;
  if (t === "locality") return "settlement";
  if (t === "municipality") return "authority";
  return null;
}
const SQL = {
  fixedCols(p: TableProfile, col: string): string {
    const r = tableRef(p), c = qi(col);
    return `-- עמודות תקניות: ערך היישוב/רשות המקורי + השם והסמל הרשמיים\n`
      + `SELECT *,\n`
      + `       COALESCE(over_settlement(${c}), over_authority(${c}))           AS over_settlement,\n`
      + `       COALESCE(over_settlement_code(${c}), over_authority_code(${c})) AS over_settlement_code\n`
      + `FROM ${r}\nLIMIT 100`;
  },
  enrich(p: TableProfile, col: string): string {
    const r = tableRef(p), c = qi(col);
    return `-- העשרה: הוספת מחוז/נפה/אוכלוסייה מאינדקס היישובים\n`
      + `SELECT t.*, s.name AS יישוב_רשמי, s.district AS מחוז, s.subdistrict AS נפה, s.population AS אוכלוסייה\n`
      + `FROM ${r} t\n`
      + `LEFT JOIN over_settlements s ON s.code = over_settlement_code(t.${c})\nLIMIT 100`;
  },
  joinTemplate(p: TableProfile, col: string): string {
    const r = tableRef(p), c = qi(col);
    return `-- הצלבה מתוקנת: שני הצדדים נרפאים לפי סמל יישוב/רשות.\n`
      + `-- החליפו את <טבלה_שנייה> ו-<עמודת_יישוב> בטבלה ובעמודה שלכם.\n`
      + `WITH a AS (\n`
      + `  SELECT *, COALESCE(over_settlement_code(${c}), over_authority_code(${c})) AS over_code\n`
      + `  FROM ${r}\n)\n`
      + `SELECT a.*, b.*\n`
      + `FROM a\n`
      + `JOIN <טבלה_שנייה> b\n`
      + `  ON a.over_code = COALESCE(over_settlement_code(b."<עמודת_יישוב>"),\n`
      + `                            over_authority_code(b."<עמודת_יישוב>"))\nLIMIT 100`;
  },
  coverage(p: TableProfile, col: string): string {
    const r = tableRef(p), c = qi(col);
    return `SELECT count(DISTINCT ${c}) AS distinct_values,\n`
      + `       count(DISTINCT ${c}) FILTER (WHERE COALESCE(over_settlement_code(${c}), over_authority_code(${c})) IS NOT NULL) AS healed\n`
      + `FROM ${r}`;
  },
};

function JoinHelper({ profile, columns, onUseSql }: {
  profile: TableProfile; columns: string[];
  onUseSql: (sql: string, run?: boolean) => void;
}) {
  const [cov, setCov] = useState<Record<string, { total: number; healed: number } | "loading" | "error">>({});
  const checkCoverage = async (col: string) => {
    setCov((s) => ({ ...s, [col]: "loading" }));
    try {
      const res = await dataCatalog.sql(SQL.coverage(profile, col));
      const row = (res.rows?.[0] || {}) as Record<string, unknown>;
      setCov((s) => ({ ...s, [col]: { total: Number(row.distinct_values) || 0, healed: Number(row.healed) || 0 } }));
    } catch {
      setCov((s) => ({ ...s, [col]: "error" }));
    }
  };
  const btn: React.CSSProperties = {
    fontSize: "0.72rem", padding: "0.15rem 0.5rem", borderRadius: 4, cursor: "pointer",
    border: "1px solid var(--primary, #2563eb)", background: "var(--bg, #fff)", color: "var(--primary, #2563eb)",
  };
  return (
    <div style={{ marginTop: "0.6rem", padding: "0.5rem 0.7rem", border: "1px dashed var(--primary, #93c5fd)", borderRadius: 6 }}>
      <div style={{ fontWeight: 600, fontSize: "0.85rem", marginBottom: "0.15rem" }}>
        🔗 הצלבה מתוקנת — תיקון שדות יישוב/רשות תוך כדי JOIN
      </div>
      <div className="text-muted" style={{ fontSize: "0.74rem", marginBottom: "0.5rem" }}>
        עוטף את השדה ב-<code>over_settlement()</code>/<code>over_authority()</code> כדי שערכים בכתיב שונה עדיין יצליבו.
      </div>
      {columns.map((col) => {
        const c = cov[col];
        return (
          <div key={col} style={{ display: "flex", flexWrap: "wrap", alignItems: "center", gap: 6, padding: "0.3rem 0", borderTop: "1px solid var(--border,#eef2f5)" }}>
            <span style={{ fontWeight: 600, fontSize: "0.8rem", minWidth: 110 }}>{col}</span>
            <button style={btn} onClick={() => onUseSql(SQL.fixedCols(profile, col), true)}>עמודות תקניות</button>
            <button style={btn} onClick={() => onUseSql(SQL.enrich(profile, col), true)}>העשרה (מחוז/אוכלוסייה)</button>
            <button style={btn} onClick={() => onUseSql(SQL.joinTemplate(profile, col), false)}>תבנית JOIN</button>
            <button style={{ ...btn, borderColor: "#94a3b8", color: "#475569" }} onClick={() => checkCoverage(col)}>בדוק כיסוי</button>
            {c === "loading" && <span className="text-muted" style={{ fontSize: "0.74rem" }}>בודק…</span>}
            {c === "error" && <span style={{ fontSize: "0.74rem", color: "#b91c1c" }}>שגיאה</span>}
            {c && c !== "loading" && c !== "error" && (
              <span style={chip(c.healed === c.total ? "#dcfce7" : "#fef9c3", c.healed === c.total ? "#15803d" : "#a16207")}>
                {c.healed}/{c.total} נפתרים ({c.total ? Math.round((c.healed / c.total) * 100) : 0}%)
              </span>
            )}
          </div>
        );
      })}
    </div>
  );
}

export default function ProfilePanel({ profile, onUseSql }: {
  profile: TableProfile | null | undefined;
  onUseSql?: (sql: string, run?: boolean) => void;
}) {
  const [open, setOpen] = useState(true);
  if (!profile) return null;

  const cols = profile.sql_profile?.columns || {};
  const colNames = Object.keys(cols);
  const summary = profile.summary_he || profile.llm_enrichment?.summary_he;
  const tags = profile.llm_enrichment?.tags || [];
  const keywords = (profile.sql_profile?.keywords || []).slice(0, 18);
  const candidateKey = profile.sql_profile?.candidate_key;
  const descOf = (col: string) => profile.llm_enrichment?.columns?.[col]?.description_he;

  return (
    <div
      dir="rtl"
      style={{
        marginTop: "0.6rem",
        border: "1px solid var(--border, #e5e7eb)",
        borderRadius: 6,
        background: "var(--bg-muted, #f8fafc)",
        overflow: "hidden",
      }}
    >
      <button
        onClick={() => setOpen((o) => !o)}
        style={{
          width: "100%",
          textAlign: "start",
          padding: "0.5rem 0.7rem",
          background: "transparent",
          border: "none",
          cursor: "pointer",
          display: "flex",
          alignItems: "center",
          gap: 8,
          fontWeight: 600,
          fontSize: "0.9rem",
        }}
      >
        <span style={{ transform: open ? "rotate(90deg)" : "none", transition: "transform .15s" }}>▶</span>
        פרופיל אוטומטי של המאגר
        <span style={chip("#e0f2fe", "#0369a1")}>{colNames.length} שדות</span>
        {profile.status === "enriched" && <span style={chip("#dcfce7", "#15803d")}>מועשר AI</span>}
      </button>

      {open && (
        <div style={{ padding: "0 0.7rem 0.7rem" }}>
          {summary && (
            <p style={{ margin: "0 0 0.6rem", fontSize: "0.85rem", lineHeight: 1.5 }}>{summary}</p>
          )}

          {(tags.length > 0 || keywords.length > 0) && (
            <div style={{ display: "flex", flexWrap: "wrap", gap: 4, marginBottom: "0.6rem" }}>
              {tags.map((t) => (
                <span key={`t-${t}`} style={chip("#ede9fe", "#6d28d9")}>{t}</span>
              ))}
              {keywords.map((k) => (
                <span key={`k-${k.token}`} style={chip("#f1f5f9", "#475569")} title={`${k.count} מופעים`}>
                  {k.token}
                </span>
              ))}
            </div>
          )}

          <div style={{ overflowX: "auto", border: "1px solid var(--border, #e5e7eb)", borderRadius: 4, background: "var(--bg, #fff)" }}>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.78rem", whiteSpace: "nowrap" }}>
              <thead>
                <tr style={{ background: "var(--bg-muted, #eef2f5)" }}>
                  {["שדה", "סוג", "טווח / min–max", "ישות", "מילוי", "ערכים ייחודיים", "תיאור"].map((h) => (
                    <th key={h} style={{ textAlign: "start", padding: "0.3rem 0.5rem", borderBottom: "2px solid var(--border, #cbd5e1)" }}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {colNames.map((name) => {
                  const c = cols[name];
                  const ent = entityLabel(name, profile);
                  const fill = c.fill_rate != null ? `${Math.round(c.fill_rate * 100)}%` : "—";
                  const df = c.date_format;
                  return (
                    <tr key={name} style={{ borderBottom: "1px solid var(--border, #eef2f5)" }}>
                      <td style={{ padding: "0.3rem 0.5rem", fontWeight: 600 }}>
                        {name}
                        {candidateKey === name && <span style={{ ...chip("#fef9c3", "#a16207"), marginInlineStart: 4 }}>מפתח</span>}
                      </td>
                      <td style={{ padding: "0.3rem 0.5rem" }}>
                        {KIND_HE[c.detected_kind || ""] || c.detected_kind || "—"}
                        {c.native && <span style={{ ...chip("#f1f5f9", "#64748b"), marginInlineStart: 4 }}>native</span>}
                      </td>
                      <td style={{ padding: "0.3rem 0.5rem" }} dir="ltr">
                        {rangeText(c) || "—"}
                        {df?.python && (
                          <span style={{ ...chip("#f1f5f9", "#475569"), marginInlineStart: 4 }} title={df.ambiguous ? "פורמט תאריך — ייתכן dd/mm מול mm/dd" : "פורמט תאריך שזוהה"}>
                            {df.python}{df.ambiguous ? " ⚠" : ""}
                          </span>
                        )}
                      </td>
                      <td style={{ padding: "0.3rem 0.5rem" }}>
                        {ent ? (
                          <span style={chip(ent.llm ? "#dcfce7" : "#f1f5f9", ent.llm ? "#15803d" : "#475569")} title={ent.llm ? "סיווג AI" : "זיהוי היוריסטי"}>
                            {ent.he}
                          </span>
                        ) : "—"}
                      </td>
                      <td style={{ padding: "0.3rem 0.5rem" }}>{fill}</td>
                      <td style={{ padding: "0.3rem 0.5rem" }}>{c.distinct_est != null ? c.distinct_est.toLocaleString("he-IL") : "—"}</td>
                      <td style={{ padding: "0.3rem 0.5rem", whiteSpace: "normal", maxWidth: 260, color: "var(--text-muted, #64748b)" }}>
                        {descOf(name) || ""}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>

          {onUseSql && (() => {
            const locCols = colNames.filter((c) => localityKind(c, profile));
            return locCols.length ? <JoinHelper profile={profile} columns={locCols} onUseSql={onUseSql} /> : null;
          })()}

          <div className="text-muted" style={{ fontSize: "0.72rem", marginTop: "0.4rem" }}>
            פרופיל מחושב אוטומטית (טווחים ופורמטים מ-SQL; תקציר וסיווג שדות ב-AI). ייתכנו אי-דיוקים.
          </div>
        </div>
      )}
    </div>
  );
}
