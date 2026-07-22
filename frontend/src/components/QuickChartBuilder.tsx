import { useMemo, useState } from "react";
import { CatalogTable } from "../api/client";

/**
 * "גרף מהיר" — build a chart from a table without writing SQL.
 *
 * Lives on the table cube of the /data console. The user picks a measure
 * (row count / sum / avg of a numeric column) and a group-by column; we
 * generate the real GROUP BY query, hand it to the console to run, and set the
 * chart URL params so SqlChartPanel opens the right chart automatically:
 * line for a date grouping (bucketed by month), bar for a numeric key
 * (e.g. year), horizontal bars for text labels (Hebrew names are long).
 *
 * The generated SQL lands in the editor, so the tool doubles as a teaching aid
 * — users see the query behind the chart and can tweak it.
 */

// Tolerant column-kind classifier: dataset tables carry CKAN-ish types
// (int/numeric/timestamp/text), Knesset tables carry Postgres type names.
function kindOfType(t: string): "number" | "date" | "text" {
  const s = (t || "").toLowerCase();
  if (s.includes("timestamp") || s === "date" || s === "datetime") return "date";
  if (["int", "integer", "bigint", "smallint", "numeric", "decimal", "real", "double precision", "float", "number"].some((n) => s === n || s.startsWith(n))) return "number";
  return "text";
}

const qid = (id: string) => `"${id.replace(/"/g, '""')}"`;

export default function QuickChartBuilder({ table, onCreate }: {
  table: CatalogTable;
  onCreate: (sql: string, chartParams: Record<string, string>) => void;
}) {
  const cols = useMemo(
    () => table.columns.map((c) => ({ name: c.name, kind: kindOfType(c.type) })),
    [table],
  );
  const numCols = cols.filter((c) => c.kind === "number");
  const groupables = useMemo(() => {
    // Text → date → number; within text, category-looking columns (types,
    // statuses, descriptions) lead — "name"-like unique columns make a bar per
    // row, which is never what the user meant by "לפי".
    const catScore = (n: string) => {
      const s = n.toLowerCase();
      if (/desc$|type|status|category|kind|group|סוג|סטטוס|קטגוריה|מחוז|רשות|משרד/.test(s)) return 0;
      if (/^name$|^id$|number|url|link|תאריך/.test(s)) return 2;
      return 1;
    };
    const text = cols.filter((c) => c.kind === "text").slice()
      .sort((a, b) => catScore(a.name) - catScore(b.name));
    const date = cols.filter((c) => c.kind === "date");
    const num = cols.filter((c) => c.kind === "number");
    return [...text, ...date, ...num];
  }, [cols]);

  const [measure, setMeasure] = useState("count");
  const [groupBy, setGroupBy] = useState(groupables[0]?.name || "");
  const [limit, setLimit] = useState(20);

  if (!groupables.length) return null;
  const groupKind = cols.find((c) => c.name === groupBy)?.kind || "text";

  function create() {
    const [op, col] = measure === "count" ? ["count", ""] : measure.split(":", 2);
    const alias = op === "count" ? "מספר שורות" : `${op === "sum" ? "סכום" : "ממוצע"} ${col}`;
    const measureSql = op === "count" ? `count(*)` : `${op}(${qid(col)})`;

    let keySql = qid(groupBy);
    let orderSql = "ORDER BY 2 DESC";
    let lim = limit;
    let chartType = "barh";
    if (groupKind === "date") {
      // Bucket timestamps by month so the X axis is a readable time series.
      keySql = `date_trunc('month', ${qid(groupBy)})::date AS ${qid(groupBy)}`;
      orderSql = "ORDER BY 1";
      lim = 500;
      chartType = "line";
    } else if (groupKind === "number") {
      orderSql = "ORDER BY 1";
      lim = Math.max(limit, 100);
      chartType = "bar";
    }

    const sql =
      `SELECT ${keySql}, ${measureSql} AS ${qid(alias)}\n` +
      `FROM ${qid(table.table)}\n` +
      `GROUP BY 1\n${orderSql}\nLIMIT ${lim}`;

    onCreate(sql, {
      chart: chartType,
      cx: groupBy,
      cy: alias,
      cagg: "sum",
      csort: groupKind === "text" ? "value_desc" : "result",
      ctop: "0",
      cflags: "nofold",
    });
  }

  const selStyle = {
    padding: "0.25rem 0.4rem", border: "1px solid var(--border, #d1d5db)",
    borderRadius: 4, fontSize: "0.82rem", maxWidth: 220,
  } as const;

  return (
    <div style={{ border: "1px dashed var(--border, #d1d5db)", borderRadius: 6, padding: "0.6rem 0.8rem", margin: "0.6rem 0 0.4rem" }}>
      <div className="flex" style={{ gap: "0.5rem 0.9rem", alignItems: "center", flexWrap: "wrap" }}>
        <strong style={{ fontSize: "0.85rem" }}>📊 גרף מהיר</strong>
        <label className="text-sm text-muted">
          מה למדוד:{" "}
          <select value={measure} onChange={(e) => setMeasure(e.target.value)} style={selStyle}>
            <option value="count">מספר שורות</option>
            {numCols.map((c) => <option key={`s${c.name}`} value={`sum:${c.name}`}>סכום — {c.name}</option>)}
            {numCols.map((c) => <option key={`a${c.name}`} value={`avg:${c.name}`}>ממוצע — {c.name}</option>)}
          </select>
        </label>
        <label className="text-sm text-muted">
          לפי:{" "}
          <select value={groupBy} onChange={(e) => setGroupBy(e.target.value)} style={selStyle}>
            {groupables.map((c) => (
              <option key={c.name} value={c.name}>
                {c.name}{c.kind === "date" ? " (לפי חודש)" : ""}
              </option>
            ))}
          </select>
        </label>
        {groupKind === "text" && (
          <label className="text-sm text-muted">
            הצג:{" "}
            <select value={limit} onChange={(e) => setLimit(Number(e.target.value))} style={selStyle}>
              {[10, 20, 50].map((n) => <option key={n} value={n}>{n} המובילים</option>)}
            </select>
          </label>
        )}
        <button
          type="button"
          onClick={create}
          style={{
            fontSize: "0.82rem", padding: "0.3rem 0.9rem", borderRadius: 4, border: "none",
            background: "var(--primary, #0f766e)", color: "white", fontWeight: 600, cursor: "pointer",
          }}
        >
          צור גרף ↑
        </button>
      </div>
      <div className="text-sm text-muted" style={{ fontSize: "0.72rem", marginTop: "0.3rem" }}>
        בלי לכתוב SQL — נריץ בשבילכם שאילתת GROUP BY ונפתח את התרשים המתאים. השאילתה תופיע בקונסולה למעלה ואפשר להמשיך לערוך אותה.
      </div>
    </div>
  );
}
