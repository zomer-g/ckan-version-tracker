import { useMemo, useState } from "react";
import { dataCatalog } from "../api/client";
import type { CatalogTable, TableProfile, TableProfileColumn } from "../api/client";

/**
 * "בונה סינון" — filter and aggregate a table without writing SQL, and without
 * a language model.
 *
 * The design constraint that shapes everything here: a filter that returns zero
 * rows with no explanation is the fastest way to make someone conclude the data
 * isn't there. So values are never free-typed — each text filter offers the
 * column's REAL values, taken from the profiler's `top_values`, with the count
 * next to each one. You can see there are 412 rows for "מסעדה" before you pick
 * it, so an empty result is impossible by construction rather than by luck.
 * (This is the facet pattern Datasette popularised; the counts are the point,
 * not the dropdown.)
 *
 * Costs nothing to run — no LLM, no new endpoint. The generated SQL goes to the
 * console like every other builder on this page, so the tool doubles as a way
 * to learn what the query looks like.
 */

const qid = (id: string) => `"${id.replace(/"/g, '""')}"`;

type Row = { col: string; op: string; value: string };

const OPS: Record<string, { label: string; kinds: string[]; noValue?: boolean }> = {
  "=": { label: "שווה ל", kinds: ["text", "number", "date"] },
  "!=": { label: "שונה מ", kinds: ["text", "number", "date"] },
  contains: { label: "מכיל", kinds: ["text"] },
  ">=": { label: "גדול או שווה", kinds: ["number", "date"] },
  "<=": { label: "קטן או שווה", kinds: ["number", "date"] },
  not_null: { label: "אינו ריק", kinds: ["text", "number", "date"], noValue: true },
  is_null: { label: "ריק", kinds: ["text", "number", "date"], noValue: true },
};

function kindOf(type: string, profiled?: string): "text" | "number" | "date" {
  if (profiled === "numeric") return "number";
  if (profiled === "date") return "date";
  const s = (type || "").toLowerCase();
  if (s.includes("timestamp") || s === "date" || s === "datetime") return "date";
  if (["int", "bigint", "smallint", "numeric", "decimal", "real", "double precision", "float", "number"]
    .some((n) => s === n || s.startsWith(n))) return "number";
  return "text";
}

const box: React.CSSProperties = {
  padding: "0.25rem 0.4rem", border: "1px solid var(--border, #d1d5db)",
  borderRadius: 4, fontSize: "0.82rem", maxWidth: 240,
};

export default function FilterBuilder({ table, profile, onUseSql }: {
  table: CatalogTable;
  profile?: TableProfile | null;
  onUseSql: (sql: string, run?: boolean) => void;
}) {
  const [open, setOpen] = useState(false);
  const pcols: Record<string, TableProfileColumn> = profile?.sql_profile?.columns || {};

  const cols = useMemo(
    () => table.columns.map((c) => ({
      name: c.name,
      kind: kindOf(c.type, pcols[c.name]?.detected_kind),
      // Real stored values, most frequent first. Present only for text columns
      // (the profiler records ranges for numbers and dates instead).
      values: pcols[c.name]?.top_values || [],
      fill: pcols[c.name]?.fill_rate,
      min: pcols[c.name]?.min,
      max: pcols[c.name]?.max,
    })),
    [table, profile],
  );
  const byName = useMemo(() => new Map(cols.map((c) => [c.name, c])), [cols]);
  const numCols = cols.filter((c) => c.kind === "number");

  const [rows, setRows] = useState<Row[]>([]);
  const [groupBy, setGroupBy] = useState("");
  const [measure, setMeasure] = useState("count");
  const [limit, setLimit] = useState(100);

  const addRow = () => setRows((r) => [...r, { col: cols[0]?.name || "", op: "=", value: "" }]);
  const setRow = (i: number, patch: Partial<Row>) =>
    setRows((r) => r.map((x, j) => (j === i ? { ...x, ...patch } : x)));
  const delRow = (i: number) => setRows((r) => r.filter((_, j) => j !== i));

  function whereSql(r: Row): string | null {
    const c = byName.get(r.col);
    if (!c) return null;
    const col = qid(r.col);
    if (r.op === "is_null") return `${col} IS NULL`;
    if (r.op === "not_null") return `${col} IS NOT NULL`;
    if (!r.value) return null;
    // Values are escaped for the literal; the server re-validates everything
    // and runs it read-only, so this is presentation, not the security layer.
    const lit = `'${r.value.replace(/'/g, "''")}'`;
    if (r.op === "contains") return `${col}::text ILIKE '%${r.value.replace(/'/g, "''")}%'`;
    if (c.kind === "number") {
      // Numeric-looking text is the norm in CSV-sourced tables; cast so a
      // range test compares numbers rather than strings ('9' > '10').
      const num = `NULLIF(regexp_replace(${col}::text, '[^0-9.\\-]', '', 'g'), '')::numeric`;
      return `${num} ${r.op} ${r.value.replace(/[^0-9.\-]/g, "") || "0"}`;
    }
    if (r.op === "=" || r.op === "!=") return `btrim(${col}::text) ${r.op} btrim(${lit})`;
    return `${col} ${r.op} ${lit}`;
  }

  function buildSql(): string {
    const where = rows.map(whereSql).filter(Boolean) as string[];
    const [op, mcol] = measure === "count" ? ["count", ""] : measure.split(":", 2);
    const lines: string[] = [`-- נוצר בבונה הסינון של over.org.il`];
    if (groupBy) {
      const key = byName.get(groupBy)?.kind === "date"
        ? `date_trunc('month', ${qid(groupBy)})::date AS ${qid(groupBy)}`
        : qid(groupBy);
      const agg = op === "count"
        ? "count(*)"
        : `${op}(NULLIF(regexp_replace(${qid(mcol)}::text, '[^0-9.\\-]', '', 'g'), '')::numeric)`;
      const alias = op === "count" ? "מספר שורות" : `${op === "sum" ? "סכום" : "ממוצע"} ${mcol}`;
      lines.push(`SELECT ${key}, ${agg} AS ${qid(alias)}`);
      lines.push(`FROM ${qid(table.table)}`);
      if (where.length) lines.push("WHERE " + where.join("\n  AND "));
      lines.push("GROUP BY 1", "ORDER BY 2 DESC", `LIMIT ${limit}`);
    } else {
      lines.push("SELECT *", `FROM ${qid(table.table)}`);
      if (where.length) lines.push("WHERE " + where.join("\n  AND "));
      lines.push(`LIMIT ${limit}`);
    }
    return lines.join("\n");
  }

  // Live count under the current filters. Cheap, and it is what turns "0 rows"
  // from a dead end into information you had before you ran anything.
  const [count, setCount] = useState<number | "loading" | "error" | null>(null);
  async function preview() {
    setCount("loading");
    const where = rows.map(whereSql).filter(Boolean) as string[];
    const sql = `SELECT count(*) AS n FROM ${qid(table.table)}`
      + (where.length ? `\nWHERE ${where.join("\n  AND ")}` : "");
    try {
      const r = await dataCatalog.sql(sql);
      setCount(Number((r.rows?.[0] as Record<string, unknown>)?.n) || 0);
    } catch { setCount("error"); }
  }

  if (!cols.length) return null;

  return (
    <div dir="rtl" style={{ border: "1px dashed var(--border, #d1d5db)", borderRadius: 6, margin: "0.6rem 0 0.4rem", overflow: "hidden" }}>
      <button onClick={() => setOpen((o) => !o)}
        style={{ width: "100%", textAlign: "start", padding: "0.5rem 0.8rem", background: "transparent", border: "none", cursor: "pointer", display: "flex", alignItems: "center", gap: 8, fontWeight: 600, fontSize: "0.85rem" }}>
        <span style={{ transform: open ? "rotate(90deg)" : "none", transition: "transform .15s" }}>▶</span>
        🔎 בונה סינון — סינון וקיבוץ בלי SQL, עם הערכים האמיתיים מהמאגר
      </button>

      {open && (
        <div style={{ padding: "0 0.8rem 0.8rem", fontSize: "0.82rem" }}>
          {rows.map((r, i) => {
            const c = byName.get(r.col);
            const opDef = OPS[r.op];
            const showValue = !opDef?.noValue;
            return (
              <div key={i} className="flex" style={{ gap: 6, alignItems: "center", flexWrap: "wrap", marginBottom: 6 }}>
                <select value={r.col} style={box}
                  onChange={(e) => setRow(i, { col: e.target.value, value: "", op: "=" })}>
                  {cols.map((x) => (
                    <option key={x.name} value={x.name}>
                      {x.name}{x.fill !== undefined && x.fill < 0.9 ? ` (מלא ${Math.round(x.fill * 100)}%)` : ""}
                    </option>
                  ))}
                </select>
                <select value={r.op} style={{ ...box, maxWidth: 130 }}
                  onChange={(e) => setRow(i, { op: e.target.value })}>
                  {Object.entries(OPS)
                    .filter(([, d]) => d.kinds.includes(c?.kind || "text"))
                    .map(([k, d]) => <option key={k} value={k}>{d.label}</option>)}
                </select>
                {showValue && (c?.values.length && r.op !== "contains" ? (
                  // The facet list: real values with their counts. Picking from
                  // here cannot produce an empty result.
                  <select value={r.value} style={box}
                    onChange={(e) => setRow(i, { value: e.target.value })}>
                    <option value="">— בחרו ערך —</option>
                    {c.values.map((v) => (
                      <option key={v.value} value={v.value}>
                        {v.value} ({v.count.toLocaleString("he-IL")})
                      </option>
                    ))}
                  </select>
                ) : (
                  <input value={r.value} style={box}
                    placeholder={c?.min != null ? `${c.min} … ${c.max}` : "ערך"}
                    onChange={(e) => setRow(i, { value: e.target.value })} />
                ))}
                <button onClick={() => delRow(i)} title="הסר תנאי"
                  style={{ ...box, cursor: "pointer", maxWidth: 32 }}>✕</button>
              </div>
            );
          })}

          <div className="flex" style={{ gap: 8, alignItems: "center", flexWrap: "wrap", margin: "0.5rem 0" }}>
            <button onClick={addRow} style={{ ...box, cursor: "pointer" }}>+ תנאי</button>
            <button onClick={preview} style={{ ...box, cursor: "pointer" }}>כמה שורות?</button>
            {count === "loading" && <span className="text-muted">בודק…</span>}
            {count === "error" && <span style={{ color: "#b91c1c" }}>שגיאה</span>}
            {typeof count === "number" && (
              <span style={{ padding: "0.05rem 0.45rem", borderRadius: 4, fontSize: "0.75rem",
                             background: count ? "#dcfce7" : "#fee2e2", color: count ? "#15803d" : "#b91c1c" }}>
                {count.toLocaleString("he-IL")} שורות
              </span>
            )}
          </div>

          <div className="flex" style={{ gap: "0.5rem 0.9rem", alignItems: "center", flexWrap: "wrap" }}>
            <label className="text-muted">
              קבץ לפי:{" "}
              <select value={groupBy} onChange={(e) => setGroupBy(e.target.value)} style={box}>
                <option value="">— ללא (הצג שורות) —</option>
                {cols.map((c) => (
                  <option key={c.name} value={c.name}>{c.name}{c.kind === "date" ? " (לפי חודש)" : ""}</option>
                ))}
              </select>
            </label>
            {groupBy && (
              <label className="text-muted">
                מה למדוד:{" "}
                <select value={measure} onChange={(e) => setMeasure(e.target.value)} style={box}>
                  <option value="count">מספר שורות</option>
                  {numCols.map((c) => <option key={`s${c.name}`} value={`sum:${c.name}`}>סכום — {c.name}</option>)}
                  {numCols.map((c) => <option key={`a${c.name}`} value={`avg:${c.name}`}>ממוצע — {c.name}</option>)}
                </select>
              </label>
            )}
            <label className="text-muted">
              עד:{" "}
              <select value={limit} onChange={(e) => setLimit(Number(e.target.value))} style={box}>
                {[50, 100, 500, 1000].map((n) => <option key={n} value={n}>{n} שורות</option>)}
              </select>
            </label>
            <button onClick={() => onUseSql(buildSql(), true)}
              style={{ padding: "0.3rem 0.9rem", borderRadius: 4, border: "none", fontWeight: 600, background: "var(--primary, #0f766e)", color: "white", cursor: "pointer", fontSize: "0.82rem" }}>
              ▶ צור והרץ
            </button>
          </div>

          <div className="text-muted" style={{ fontSize: "0.72rem", marginTop: "0.4rem" }}>
            הערכים ברשימות הם הערכים שקיימים בפועל במאגר, עם מספר השורות לכל אחד — כך שבחירה מהרשימה
            לעולם לא תחזיר תוצאה ריקה. השאילתה שנוצרת מופיעה בקונסולה למעלה וניתנת לעריכה.
          </div>
        </div>
      )}
    </div>
  );
}
