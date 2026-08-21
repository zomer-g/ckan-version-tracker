import { useEffect, useMemo, useState } from "react";
import { dataCatalog } from "../api/client";
import type { CatalogTable, TableProfile } from "../api/client";

/**
 * "בונה הצלבה מתוקנת" — cross-reference two datasets by locality/authority
 * WITHOUT writing SQL. The state's raw place fields are dirty (variant spellings,
 * prefixes), so both sides are healed with over_settlement()/over_authority() and
 * joined on the canonical CBS code. Also does index enrichment (bring
 * district/population from over_settlements). Generates the SQL, hands it to the
 * console to run, and previews coverage so the user sees the JOIN quality first.
 */
const qi = (id: string) => `"${id.replace(/"/g, '""')}"`;
const tref = (schema: string | undefined, table: string) =>
  schema && schema !== "public" ? `${schema}.${table}` : table;

// Which columns of a profile are a locality (יישוב) or municipality (רשות).
function localityCols(p: TableProfile | null | undefined): string[] {
  if (!p) return [];
  const cols = p.sql_profile?.columns || {};
  return Object.keys(cols).filter((c) => {
    const t = p.llm_enrichment?.columns?.[c]?.semantic_type || cols[c].entity_guess?.guess;
    return t === "locality" || t === "municipality";
  });
}

const chip = (bg: string, fg: string): React.CSSProperties => ({
  display: "inline-block", padding: "0.05rem 0.4rem", borderRadius: 4,
  background: bg, color: fg, fontSize: "0.72rem", whiteSpace: "nowrap",
});
const box: React.CSSProperties = {
  fontSize: "0.8rem", padding: "0.2rem 0.4rem", borderRadius: 4,
  border: "1px solid var(--border)", background: "var(--bg)",
};

// over_settlements / over_authorities columns offered for enrichment.
const ENRICH_FIELDS = {
  settlement: [
    { col: "name", as: "יישוב_רשמי" }, { col: "district", as: "מחוז" },
    { col: "subdistrict", as: "נפה" }, { col: "municipal_status", as: "מעמד_מוניציפלי" },
    { col: "local_authority", as: "רשות_מקומית" }, { col: "population", as: "אוכלוסייה" },
  ],
  authority: [
    { col: "name", as: "רשות_רשמית" }, { col: "district", as: "מחוז" },
    { col: "municipal_status", as: "מעמד_מוניציפלי" },
  ],
} as const;

export default function JoinBuilder({ profile, tables, onUseSql }: {
  profile: TableProfile;
  tables: CatalogTable[];
  onUseSql: (sql: string, run?: boolean) => void;
}) {
  const [open, setOpen] = useState(false);
  const leftCols = useMemo(() => localityCols(profile), [profile]);
  const [leftCol, setLeftCol] = useState(leftCols[0] || "");
  useEffect(() => { setLeftCol(leftCols[0] || ""); }, [leftCols]);

  const [mode, setMode] = useState<"dataset" | "enrich">("dataset");

  // ── right side: another dataset ──
  const [rightTable, setRightTable] = useState("");
  const [rightProfile, setRightProfile] = useState<TableProfile | null>(null);
  const [rightCol, setRightCol] = useState("");
  const rightTableRec = useMemo(() => tables.find((t) => t.table === rightTable), [tables, rightTable]);
  // Only tables the profiler/flags mark as carrying a locality/authority field.
  const joinableTables = useMemo(
    () => tables.filter((t) => t.table !== profile.table_name
      && (t.field_flags?.has_locality || t.field_flags?.has_authority)),
    [tables, profile.table_name],
  );
  useEffect(() => {
    if (!rightTable) { setRightProfile(null); setRightCol(""); return; }
    let alive = true;
    dataCatalog.tableProfile(rightTable).then((p) => {
      if (!alive) return;
      setRightProfile(p);
      setRightCol(localityCols(p)[0] || "");
    }).catch(() => { if (alive) { setRightProfile(null); setRightCol(""); } });
    return () => { alive = false; };
  }, [rightTable]);
  const rightLocCols = localityCols(rightProfile);

  // ── right side: index enrichment ──
  const [enrichEntity, setEnrichEntity] = useState<"settlement" | "authority">("settlement");
  const [enrichSel, setEnrichSel] = useState<string[]>(["district", "population"]);
  const toggleEnrich = (c: string) =>
    setEnrichSel((s) => s.includes(c) ? s.filter((x) => x !== c) : [...s, c]);

  // ── coverage preview ──
  const [cov, setCov] = useState<Record<string, { total: number; healed: number } | "loading" | "error">>({});
  const coverageSql = (schema: string | undefined, table: string, col: string) =>
    `SELECT count(DISTINCT ${qi(col)}) total, count(DISTINCT ${qi(col)}) `
    + `FILTER (WHERE COALESCE(over_settlement_code(${qi(col)}),over_authority_code(${qi(col)})) IS NOT NULL) healed `
    + `FROM ${tref(schema, table)}`;
  const checkCoverage = async (key: string, schema: string | undefined, table: string, col: string) => {
    if (!col) return;
    setCov((s) => ({ ...s, [key]: "loading" }));
    try {
      const r = await dataCatalog.sql(coverageSql(schema, table, col));
      const row = (r.rows?.[0] || {}) as Record<string, unknown>;
      setCov((s) => ({ ...s, [key]: { total: Number(row.total) || 0, healed: Number(row.healed) || 0 } }));
    } catch { setCov((s) => ({ ...s, [key]: "error" })); }
  };
  const covChip = (c: typeof cov[string]) => {
    if (!c) return null;
    if (c === "loading") return <span className="text-muted" style={{ fontSize: "0.72rem" }}>בודק…</span>;
    if (c === "error") return <span style={{ fontSize: "0.72rem", color: "var(--danger)" }}>שגיאה</span>;
    return <span style={chip(c.healed === c.total ? "var(--tint-good-bg)" : "var(--tint-note-bg)", c.healed === c.total ? "var(--success)" : "var(--tint-note-fg)")}>
      {c.healed}/{c.total} ({c.total ? Math.round((c.healed / c.total) * 100) : 0}%)
    </span>;
  };

  // ── SQL generation ──
  function buildSql(): string | null {
    const L = tref(profile.schema_name, profile.table_name);
    if (!leftCol) return null;
    if (mode === "dataset") {
      if (!rightTableRec || !rightCol) return null;
      const R = tref(rightTableRec.schema, rightTableRec.table);
      return `-- הצלבה מתוקנת: שני הצדדים נרפאים לפי סמל יישוב/רשות\n`
        + `WITH a AS (SELECT *, COALESCE(over_settlement_code(${qi(leftCol)}), over_authority_code(${qi(leftCol)})) AS _over_code FROM ${L}),\n`
        + `     b AS (SELECT *, COALESCE(over_settlement_code(${qi(rightCol)}), over_authority_code(${qi(rightCol)})) AS _over_code FROM ${R})\n`
        + `SELECT a.*, b.*\n`
        + `FROM a JOIN b ON a._over_code = b._over_code\n`
        + `LIMIT 100`;
    }
    // enrich
    const idx = enrichEntity === "settlement" ? "over_settlements" : "over_authorities";
    const codeFn = enrichEntity === "settlement" ? "over_settlement_code" : "over_authority_code";
    const fields = ENRICH_FIELDS[enrichEntity].filter((f) => enrichSel.includes(f.col));
    if (!fields.length) return null;
    const sel = fields.map((f) => `s.${qi(f.col)} AS ${qi(f.as)}`).join(", ");
    return `-- העשרה מאינדקס ${enrichEntity === "settlement" ? "היישובים" : "הרשויות"}\n`
      + `SELECT t.*, ${sel}\n`
      + `FROM ${L} t\n`
      + `LEFT JOIN ${idx} s ON s.code = ${codeFn}(t.${qi(leftCol)})\n`
      + `LIMIT 100`;
  }
  const canRun = mode === "dataset" ? !!(leftCol && rightCol) : !!(leftCol && enrichSel.length);

  if (!leftCols.length) return null;  // table has no locality/authority field

  return (
    <div dir="rtl" style={{ marginTop: "0.6rem", border: "1px solid var(--border)", borderRadius: 6, background: "var(--surface-2)", overflow: "hidden" }}>
      <button onClick={() => setOpen((o) => !o)} style={{ width: "100%", textAlign: "start", padding: "0.5rem 0.7rem", background: "transparent", border: "none", cursor: "pointer", display: "flex", alignItems: "center", gap: 8, fontWeight: 600, fontSize: "0.9rem" }}>
        <span style={{ transform: open ? "rotate(90deg)" : "none", transition: "transform .15s" }}>▶</span>
        <span aria-hidden="true">🔗</span> בונה הצלבה מתוקנת — הצלבת מאגרים לפי יישוב/רשות, בלי SQL
      </button>
      {open && (
        <div style={{ padding: "0 0.7rem 0.7rem", fontSize: "0.82rem" }}>
          {/* left */}
          <div style={{ display: "flex", flexWrap: "wrap", alignItems: "center", gap: 6, marginBottom: "0.5rem" }}>
            <b>צד שמאל:</b>
            <span style={chip("var(--tint-neutral-bg)", "var(--tint-neutral-fg)")}>{profile.table_name}</span>
            <span>שדה יישוב:</span>
            {leftCols.length > 1 ? (
              <select value={leftCol} onChange={(e) => setLeftCol(e.target.value)} aria-label="שדה היישוב בצד שמאל" style={box}>
                {leftCols.map((c) => <option key={c} value={c}>{c}</option>)}
              </select>
            ) : <span style={chip("var(--tint-sky-bg)", "var(--tint-sky-fg)")}>{leftCol}</span>}
            <button style={box} onClick={() => checkCoverage("left", profile.schema_name, profile.table_name, leftCol)}>בדוק כיסוי</button>
            {covChip(cov.left)}
          </div>

          {/* mode toggle */}
          <div style={{ display: "flex", gap: 6, marginBottom: "0.5rem" }}>
            <button onClick={() => setMode("dataset")} style={{ ...box, fontWeight: mode === "dataset" ? 700 : 400, background: mode === "dataset" ? "#dbeafe" : "var(--bg)" }}>הצלבה למאגר אחר</button>
            <button onClick={() => setMode("enrich")} style={{ ...box, fontWeight: mode === "enrich" ? 700 : 400, background: mode === "enrich" ? "#dbeafe" : "var(--bg)" }}>העשרה מהאינדקס</button>
          </div>

          {mode === "dataset" ? (
            <div style={{ display: "flex", flexWrap: "wrap", alignItems: "center", gap: 6, marginBottom: "0.5rem" }}>
              <b>צד ימין:</b>
              <input aria-label="חפש טבלה עם שדה יישוב/רשות…" list="jb-tables" value={rightTable} onChange={(e) => setRightTable(e.target.value)}
                placeholder="חפש טבלה עם שדה יישוב/רשות…" style={{ ...box, minWidth: 260 }} />
              <datalist id="jb-tables">
                {joinableTables.slice(0, 400).map((t) => <option key={t.table} value={t.table}>{t.title}</option>)}
              </datalist>
              {rightProfile && (rightLocCols.length ? (
                <>
                  <span>שדה יישוב:</span>
                  {rightLocCols.length > 1 ? (
                    <select value={rightCol} onChange={(e) => setRightCol(e.target.value)} aria-label="שדה היישוב בצד ימין" style={box}>
                      {rightLocCols.map((c) => <option key={c} value={c}>{c}</option>)}
                    </select>
                  ) : <span style={chip("var(--tint-sky-bg)", "var(--tint-sky-fg)")}>{rightCol}</span>}
                  <button style={box} onClick={() => rightTableRec && checkCoverage("right", rightTableRec.schema, rightTableRec.table, rightCol)}>בדוק כיסוי</button>
                  {covChip(cov.right)}
                </>
              ) : <span style={{ color: "var(--danger)", fontSize: "0.75rem" }}>לא נמצא שדה יישוב בטבלה זו</span>)}
            </div>
          ) : (
            <div style={{ marginBottom: "0.5rem" }}>
              <div style={{ display: "flex", gap: 6, marginBottom: 4 }}>
                <span>סוג:</span>
                <label><input type="radio" checked={enrichEntity === "settlement"} onChange={() => setEnrichEntity("settlement")} /> יישוב</label>
                <label><input type="radio" checked={enrichEntity === "authority"} onChange={() => setEnrichEntity("authority")} /> רשות</label>
              </div>
              <div style={{ display: "flex", flexWrap: "wrap", gap: 8 }}>
                {ENRICH_FIELDS[enrichEntity].map((f) => (
                  <label key={f.col} style={{ fontSize: "0.78rem" }}>
                    <input type="checkbox" checked={enrichSel.includes(f.col)} onChange={() => toggleEnrich(f.col)} /> {f.as}
                  </label>
                ))}
              </div>
            </div>
          )}

          <button
            disabled={!canRun}
            onClick={() => { const s = buildSql(); if (s) onUseSql(s, true); }}
            style={{ padding: "0.35rem 1rem", borderRadius: 4, border: "none", fontWeight: 600, background: canRun ? "var(--fill-brand)" : "var(--fill-neutral)", color: "var(--on-fill)", cursor: canRun ? "pointer" : "not-allowed" }}
          >
            ▶ צור והרץ
          </button>
          <span className="text-muted" style={{ fontSize: "0.72rem", marginInlineStart: 8 }}>
            מייצר JOIN שמרפא את ערכי היישוב/רשות לפי הסמל הקנוני. המקור לא משתנה.
          </span>
        </div>
      )}
    </div>
  );
}
