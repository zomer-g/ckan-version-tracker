import { useEffect, useMemo, useRef, useState } from "react";

/**
 * Charts for an in-memory SQL result (the /data console). Pie / bar / line, with
 * an advanced-settings drawer (custom per-series colors + an opt-in second Y
 * axis). For each chart type we show its data-shape requirement; a type becomes
 * an enabled *suggestion* only once the current result satisfies it. Rendered as
 * dependency-free inline SVG.
 *
 * Palette: the validated categorical order from the data-viz design system
 * (light surface). Axes/labels/grid use the app's ink tokens; series carry the
 * categorical hues (overridable). Charts render LTR (numbers + axes) in the RTL
 * page.
 */

// Validated categorical order (light). Assigned by slot, never cycled — past 8
// series we fold into "אחר" (pie) or cap (bar/line legend).
const PALETTE = [
  "#2a78d6", "#008300", "#e87ba4", "#eda100",
  "#1baf7a", "#eb6834", "#4a3aa7", "#e34948",
];
const MAX_SERIES = PALETTE.length;

export type ChartType = "bar" | "line" | "pie";
type ColKind = "number" | "date" | "text";
interface ColInfo { name: string; kind: ColKind }
type Axis = "left" | "right";

type Row = Record<string, unknown>;

// ── type inference ───────────────────────────────────────────────────────────

function toNum(v: unknown): number | null {
  if (typeof v === "number") return Number.isFinite(v) ? v : null;
  if (typeof v === "string") {
    const s = v.trim();
    if (s === "" || !/^-?\d+(\.\d+)?$/.test(s)) return null;
    const n = Number(s);
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

const DATEISH = /^\d{4}-\d{2}(-\d{2})?([ T]\d|$)|\d{2}[/.]\d{2}[/.]\d{4}/;

function inferKind(rows: Row[], col: string): ColKind {
  let seen = 0, allNum = true, allDate = true;
  for (const r of rows) {
    const v = r[col];
    if (v === null || v === undefined || v === "") continue;
    seen++;
    if (toNum(v) === null) allNum = false;
    const s = String(v);
    if (!(DATEISH.test(s) && !Number.isNaN(Date.parse(s)))) allDate = false;
    if (seen >= 60) break;
  }
  if (seen === 0) return "text";
  if (allNum) return "number";
  if (allDate) return "date";
  return "text";
}

function inferColumns(columns: string[], rows: Row[]): ColInfo[] {
  return columns.map((name) => ({ name, kind: inferKind(rows, name) }));
}

// ── formatting ───────────────────────────────────────────────────────────────

function fmtNum(n: number): string {
  if (!Number.isFinite(n)) return "";
  if (Number.isInteger(n)) return n.toLocaleString("he-IL");
  return n.toLocaleString("he-IL", { maximumFractionDigits: 2 });
}
function fmtCompact(n: number): string {
  const a = Math.abs(n);
  if (a >= 1e9) return (n / 1e9).toFixed(1).replace(/\.0$/, "") + "B";
  if (a >= 1e6) return (n / 1e6).toFixed(1).replace(/\.0$/, "") + "M";
  if (a >= 1e3) return (n / 1e3).toFixed(1).replace(/\.0$/, "") + "K";
  return fmtNum(n);
}

// ── chart requirements (shown to the user) ───────────────────────────────────

const REQS: { type: ChartType; label: string; icon: string; req: string }[] = [
  { type: "bar", label: "עמודות", icon: "📊",
    req: "עמודת תווית אחת (ציר X, כל טיפוס) + עמודה מספרית אחת או יותר (סדרות)." },
  { type: "line", label: "קו", icon: "📈",
    req: "עמודת ציר-X (מספר / תאריך / תווית) + עמודה מספרית אחת או יותר." },
  { type: "pie", label: "עוגה", icon: "🥧",
    req: "עמודת תווית אחת + עמודה מספרית אחת (הערך). כל שורה = פרוסה." },
];

// The value(s) must be numeric; the X/label may be any column. Shared
// requirement: ≥2 columns and ≥1 numeric column.
function applicable(_type: ChartType, cols: ColInfo[]): boolean {
  return cols.filter((c) => c.kind === "number").length >= 1 && cols.length >= 2;
}

// Pie aggregation (shared by the chart + the color settings): sum positive
// values per label, sort desc, fold past 8 into "אחר".
function aggregatePie(rows: Row[], labelCol: string, valueCol: string): { label: string; value: number }[] {
  const agg = new Map<string, number>();
  for (const r of rows) {
    const v = toNum(r[valueCol]);
    if (v === null || v <= 0) continue;
    const k = String(r[labelCol] ?? "");
    agg.set(k, (agg.get(k) || 0) + v);
  }
  let entries = [...agg.entries()].map(([label, value]) => ({ label, value })).sort((a, b) => b.value - a.value);
  if (entries.length > MAX_SERIES) {
    const head = entries.slice(0, MAX_SERIES - 1);
    const rest = entries.slice(MAX_SERIES - 1).reduce((s, e) => s + e.value, 0);
    entries = [...head, { label: "אחר", value: rest }];
  }
  return entries;
}

// ── component ────────────────────────────────────────────────────────────────

export default function SqlChartPanel({ columns, rows }: { columns: string[]; rows: Row[] }) {
  const cols = useMemo(() => inferColumns(columns, rows), [columns, rows]);
  const numCols = useMemo(() => cols.filter((c) => c.kind === "number").map((c) => c.name), [cols]);
  const catCols = useMemo(() => cols.filter((c) => c.kind !== "number").map((c) => c.name), [cols]);

  const suggestions = useMemo(
    () => REQS.filter((r) => applicable(r.type, cols)).map((r) => r.type),
    [cols],
  );

  const [type, setType] = useState<ChartType | null>(null);
  const [xCol, setXCol] = useState<string>("");
  const [yCols, setYCols] = useState<string[]>([]);
  const [showSettings, setShowSettings] = useState(false);
  const [colorOverrides, setColorOverrides] = useState<Record<string, string>>({});
  const [dualY, setDualY] = useState(false);
  const [axisOf, setAxisOf] = useState<Record<string, Axis>>({});

  // When the result shape changes, drop the open chart + reset all settings.
  const sig = columns.join("|") + "#" + rows.length;
  const prevSig = useRef(sig);
  useEffect(() => {
    if (prevSig.current !== sig) {
      prevSig.current = sig;
      setType(null);
    }
  }, [sig]);

  function openChart(t: ChartType) {
    setType(t);
    setShowSettings(false);
    setColorOverrides({});
    setDualY(false);
    setAxisOf({});
    // X/label default: text category → date → first column (SQL convention: the
    // GROUP BY key leads, values follow). Value = first numeric that isn't X.
    const defaultX = t === "line"
      ? (cols.find((c) => c.kind === "date")?.name || catCols[0] || columns[0] || "")
      : (catCols[0] || columns[0] || "");
    const firstY = numCols.find((n) => n !== defaultX) || numCols[0] || "";
    setXCol(defaultX);
    setYCols(firstY ? [firstY] : []);
  }

  if (!rows.length) return null;

  const singleValue = type === "pie";
  const yOptions = numCols.filter((n) => n !== xCol);
  const activeY = (singleValue ? yCols.slice(0, 1) : yCols).filter((y) => y !== xCol && numCols.includes(y));

  const colorFor = (name: string, i: number) => colorOverrides[name] || PALETTE[i % MAX_SERIES];
  const setColor = (name: string, hex: string) => setColorOverrides((p) => ({ ...p, [name]: hex }));
  const resetColor = (name: string) => setColorOverrides((p) => { const n = { ...p }; delete n[name]; return n; });

  const seriesColors = activeY.map((y, i) => colorFor(y, i));
  const seriesAxes: Axis[] = activeY.map((y) => (dualY ? axisOf[y] || "left" : "left"));

  // Pie slice labels for the color settings (same aggregation the chart uses).
  const pieSlices = type === "pie" && activeY[0] ? aggregatePie(rows, xCol, activeY[0]) : [];

  return (
    <div className="card" style={{ marginBottom: "1rem", padding: "1rem" }}>
      <div className="flex" style={{ gap: "0.6rem", alignItems: "center", flexWrap: "wrap", marginBottom: "0.5rem" }}>
        <strong style={{ fontSize: "0.95rem" }}>📊 תרשימים</strong>
        <span className="text-sm text-muted">בחרו סוג תרשים שמתאים למבנה התוצאה שלכם</span>
      </div>

      {/* Suggestion buttons — each chart type; enabled only when the result meets
          its requirement, with the requirement always shown. */}
      <div className="flex" style={{ gap: "0.5rem", flexWrap: "wrap", marginBottom: "0.6rem" }}>
        {REQS.map((r) => {
          const ok = suggestions.includes(r.type);
          const active = type === r.type;
          return (
            <button
              key={r.type}
              type="button"
              onClick={() => ok && openChart(r.type)}
              disabled={!ok}
              title={r.req}
              style={{
                display: "flex", flexDirection: "column", alignItems: "flex-start", gap: "0.15rem",
                padding: "0.45rem 0.7rem", borderRadius: 6, textAlign: "start",
                border: active ? "2px solid var(--primary, #0f766e)" : "1px solid var(--border, #d1d5db)",
                background: active ? "var(--bg-muted, #eef2f5)" : ok ? "var(--bg, #fff)" : "var(--bg-muted, #f3f4f6)",
                color: ok ? "var(--text)" : "var(--text-muted)",
                cursor: ok ? "pointer" : "not-allowed", opacity: ok ? 1 : 0.7, minWidth: 190, maxWidth: 300,
              }}
            >
              <span style={{ fontWeight: 700, fontSize: "0.9rem" }}>
                {r.icon} {r.label}{" "}
                <span style={{ fontSize: "0.72rem", fontWeight: 500, color: ok ? "var(--primary, #0f766e)" : "var(--text-muted)" }}>
                  {ok ? (active ? "· נבחר" : "· ניתן להציג") : "· לא מתאים"}
                </span>
              </span>
              <span style={{ fontSize: "0.72rem", fontWeight: 400, color: "var(--text-muted)", lineHeight: 1.4 }}>
                {r.req}
              </span>
            </button>
          );
        })}
      </div>

      {type && activeY.length === 0 && (
        <div className="text-sm text-muted">אין עמודה מספרית מתאימה להצגה בתרשים.</div>
      )}

      {type && (
        <>
          {/* Column mapping controls */}
          <div className="flex" style={{ gap: "0.75rem", alignItems: "center", flexWrap: "wrap", margin: "0.4rem 0 0.6rem" }}>
            <label className="text-sm text-muted">
              {type === "pie" ? "קטגוריה: " : "ציר X: "}
              <select
                value={xCol}
                onChange={(e) => setXCol(e.target.value)}
                style={{ padding: "0.25rem 0.4rem", border: "1px solid var(--border, #d1d5db)", borderRadius: 4, fontSize: "0.82rem" }}
              >
                {(type === "pie" ? (catCols.length ? catCols : columns) : columns).map((c) => (
                  <option key={c} value={c}>{c}</option>
                ))}
              </select>
            </label>

            {singleValue ? (
              <label className="text-sm text-muted">
                ערך:{" "}
                <select
                  value={activeY[0] || ""}
                  onChange={(e) => setYCols([e.target.value])}
                  style={{ padding: "0.25rem 0.4rem", border: "1px solid var(--border, #d1d5db)", borderRadius: 4, fontSize: "0.82rem" }}
                >
                  {yOptions.map((c) => <option key={c} value={c}>{c}</option>)}
                </select>
              </label>
            ) : (
              <span className="text-sm text-muted" style={{ display: "inline-flex", gap: "0.6rem", flexWrap: "wrap", alignItems: "center" }}>
                סדרות (Y):
                {yOptions.map((c) => (
                  <label key={c} style={{ display: "inline-flex", gap: "0.25rem", alignItems: "center" }}>
                    <input
                      type="checkbox"
                      checked={yCols.includes(c)}
                      onChange={(e) =>
                        setYCols((prev) => e.target.checked ? [...prev, c] : prev.filter((x) => x !== c))
                      }
                    />
                    <span style={{ color: "var(--text)" }}>{c}</span>
                  </label>
                ))}
              </span>
            )}

            <button
              type="button"
              onClick={() => setShowSettings((s) => !s)}
              style={{
                marginInlineStart: "auto", fontSize: "0.8rem", padding: "0.25rem 0.7rem", borderRadius: 4,
                border: "1px solid var(--border, #d1d5db)", background: showSettings ? "var(--bg-muted, #eef2f5)" : "none",
                color: "var(--text)", cursor: "pointer",
              }}
            >
              ⚙ הגדרות מתקדמות {showSettings ? "▲" : "▼"}
            </button>
          </div>

          {/* Advanced settings drawer */}
          {showSettings && activeY.length > 0 && (
            <div style={{ border: "1px solid var(--border, #e5e7eb)", borderRadius: 6, padding: "0.7rem 0.9rem", margin: "0 0 0.8rem", background: "var(--bg-muted, #f8fafc)" }}>
              {type !== "pie" && (
                <label className="text-sm" style={{ display: "flex", gap: "0.4rem", alignItems: "center", marginBottom: "0.6rem" }}>
                  <input type="checkbox" checked={dualY} onChange={(e) => setDualY(e.target.checked)} />
                  <span style={{ color: "var(--text)" }}>שני צירי Y נפרדים</span>
                  <span className="text-muted" style={{ fontSize: "0.72rem" }}>
                    (שימושי כששתי סדרות בסדרי גודל שונים — כל ציר בקנה-מידה משלו)
                  </span>
                </label>
              )}

              {/* Per-series color (+ axis when dual-Y) */}
              {type !== "pie" ? (
                <div className="flex" style={{ flexDirection: "column", gap: "0.4rem" }}>
                  {activeY.map((name, i) => (
                    <div key={name} className="flex" style={{ gap: "0.6rem", alignItems: "center", flexWrap: "wrap" }}>
                      <input
                        type="color"
                        value={colorFor(name, i)}
                        onChange={(e) => setColor(name, e.target.value)}
                        aria-label={`צבע ${name}`}
                        style={{ width: 34, height: 24, border: "1px solid var(--border, #d1d5db)", borderRadius: 4, padding: 0, cursor: "pointer" }}
                      />
                      <code style={{ fontSize: "0.82rem" }}>{name}</code>
                      {colorOverrides[name] && (
                        <button type="button" onClick={() => resetColor(name)} className="text-muted"
                          style={{ fontSize: "0.72rem", background: "none", border: "none", cursor: "pointer", textDecoration: "underline" }}>
                          איפוס צבע
                        </button>
                      )}
                      {dualY && (
                        <label className="text-sm text-muted" style={{ marginInlineStart: "auto" }}>
                          ציר:{" "}
                          <select
                            value={axisOf[name] || "left"}
                            onChange={(e) => setAxisOf((p) => ({ ...p, [name]: e.target.value as Axis }))}
                            style={{ padding: "0.15rem 0.35rem", border: "1px solid var(--border, #d1d5db)", borderRadius: 4, fontSize: "0.8rem" }}
                          >
                            <option value="left">שמאל</option>
                            <option value="right">ימין</option>
                          </select>
                        </label>
                      )}
                    </div>
                  ))}
                </div>
              ) : (
                <div className="flex" style={{ flexWrap: "wrap", gap: "0.5rem 1rem" }}>
                  {pieSlices.map((s, i) => (
                    <div key={s.label} className="flex" style={{ gap: "0.4rem", alignItems: "center" }}>
                      <input
                        type="color"
                        value={colorFor(s.label, i)}
                        onChange={(e) => setColor(s.label, e.target.value)}
                        aria-label={`צבע ${s.label}`}
                        style={{ width: 30, height: 22, border: "1px solid var(--border, #d1d5db)", borderRadius: 4, padding: 0, cursor: "pointer" }}
                      />
                      <span style={{ fontSize: "0.8rem", color: "var(--text)" }}>{s.label}</span>
                    </div>
                  ))}
                </div>
              )}
            </div>
          )}

          {activeY.length > 0 && (
            <div style={{ direction: "ltr", overflowX: "auto" }}>
              {type === "bar" && (
                <BarChart rows={rows} xCol={xCol} yCols={activeY} colors={seriesColors} axes={seriesAxes} dualY={dualY} />
              )}
              {type === "line" && (
                <LineChart rows={rows} xCol={xCol} yCols={activeY} colors={seriesColors} axes={seriesAxes} dualY={dualY}
                  xKind={cols.find((c) => c.name === xCol)?.kind || "text"} />
              )}
              {type === "pie" && (
                <PieChart rows={rows} labelCol={xCol} valueCol={activeY[0]} colorOverrides={colorOverrides} />
              )}
            </div>
          )}
        </>
      )}
    </div>
  );
}

// ── shared chart chrome ──────────────────────────────────────────────────────

const INK = "var(--text, #111827)";
const MUTED = "var(--text-muted, #6b7280)";
const GRID = "var(--border, #e5e7eb)";
const AXISLINE = "var(--text-muted, #9ca3af)";
const W = 760, H = 400;

function Legend({ names, colors }: { names: string[]; colors: string[] }) {
  if (names.length < 2) return null;
  return (
    <div className="flex" style={{ gap: "0.9rem", flexWrap: "wrap", justifyContent: "center", marginTop: "0.5rem", direction: "rtl" }}>
      {names.map((n, i) => (
        <span key={n} style={{ display: "inline-flex", gap: "0.35rem", alignItems: "center", fontSize: "0.78rem", color: INK }}>
          <span style={{ width: 12, height: 12, borderRadius: 3, background: colors[i], flex: "0 0 auto" }} />
          {n}
        </span>
      ))}
    </div>
  );
}

// Round only the top two corners (data-end anchored to the baseline).
function topRoundedRect(x: number, y: number, w: number, h: number, r: number): string {
  const rr = Math.max(0, Math.min(r, w / 2, h));
  return `M${x},${y + h} L${x},${y + rr} Q${x},${y} ${x + rr},${y} L${x + w - rr},${y} Q${x + w},${y} ${x + w},${y + rr} L${x + w},${y + h} Z`;
}

function niceTicks(min: number, max: number, count = 5): number[] {
  if (min === max) { min = Math.min(0, min); max = max || 1; }
  const span = max - min;
  const step0 = span / count;
  const mag = Math.pow(10, Math.floor(Math.log10(step0)));
  const norm = step0 / mag;
  const step = (norm >= 5 ? 10 : norm >= 2 ? 5 : norm >= 1 ? 2 : 1) * mag;
  const start = Math.floor(min / step) * step;
  const ticks: number[] = [];
  for (let v = start; v <= max + step * 0.5; v += step) ticks.push(Number(v.toFixed(6)));
  return ticks;
}

interface Scale { ticks: number[]; lo: number; hi: number }
function makeScale(vals: number[]): Scale {
  const max = Math.max(0, ...vals);
  const min = Math.min(0, ...vals);
  const ticks = niceTicks(min, max);
  return { ticks, lo: ticks[0], hi: ticks[ticks.length - 1] };
}
// Colour for an axis's tick labels: the series colour when that axis carries
// exactly one series (makes a dual-axis readable), else muted ink.
function axisInk(colorsOnAxis: string[]): string {
  return colorsOnAxis.length === 1 ? colorsOnAxis[0] : MUTED;
}

interface ChartProps {
  rows: Row[]; xCol: string; yCols: string[];
  colors: string[]; axes: Axis[]; dualY: boolean;
}

// ── Bar chart (grouped when multiple series; optional dual Y) ─────────────────

const BAR_CAP = 50;

function BarChart({ rows, xCol, yCols, colors, axes, dualY }: ChartProps) {
  const rightUsed = dualY && axes.includes("right");
  const m = { top: 16, right: rightUsed ? 52 : 16, bottom: 74, left: 56 };
  const capped = rows.slice(0, BAR_CAP);
  const labels = capped.map((r) => String(r[xCol] ?? ""));
  const series = yCols.map((y) => capped.map((r) => toNum(r[y]) ?? 0));

  const leftVals = series.filter((_, s) => axes[s] === "left").flat();
  const rightVals = series.filter((_, s) => axes[s] === "right").flat();
  const left = makeScale(leftVals.length ? leftVals : series.flat());
  const right = rightUsed ? makeScale(rightVals) : left;

  const plotW = W - m.left - m.right, plotH = H - m.top - m.bottom;
  const yOfIn = (sc: Scale, v: number) => m.top + plotH - ((v - sc.lo) / (sc.hi - sc.lo)) * plotH;
  const yOf = (s: number, v: number) => yOfIn(axes[s] === "right" ? right : left, v);
  const band = plotW / Math.max(1, labels.length);
  const groupW = Math.min(band * 0.8, 64);
  const barW = groupW / yCols.length;
  const rotate = labels.length > 8;
  const leftInk = axisInk(colors.filter((_, s) => axes[s] === "left"));
  const rightInk = axisInk(colors.filter((_, s) => axes[s] === "right"));

  return (
    <>
      <svg viewBox={`0 0 ${W} ${H}`} width="100%" style={{ maxWidth: W, fontFamily: "system-ui, sans-serif" }} role="img" aria-label="תרשים עמודות">
        {left.ticks.map((t) => (
          <g key={t}>
            <line x1={m.left} x2={W - m.right} y1={yOfIn(left, t)} y2={yOfIn(left, t)} stroke={GRID} strokeWidth={1} />
            <text x={m.left - 8} y={yOfIn(left, t) + 4} textAnchor="end" fontSize={11} fill={leftInk} style={{ fontVariantNumeric: "tabular-nums" }}>{fmtCompact(t)}</text>
          </g>
        ))}
        {rightUsed && right.ticks.map((t) => (
          <text key={t} x={W - m.right + 8} y={yOfIn(right, t) + 4} textAnchor="start" fontSize={11} fill={rightInk} style={{ fontVariantNumeric: "tabular-nums" }}>{fmtCompact(t)}</text>
        ))}
        <line x1={m.left} x2={W - m.right} y1={yOfIn(left, Math.max(0, left.lo))} y2={yOfIn(left, Math.max(0, left.lo))} stroke={AXISLINE} strokeWidth={1} />
        {labels.map((lab, i) => {
          const gx = m.left + i * band + (band - groupW) / 2;
          return (
            <g key={i}>
              {series.map((vals, s) => {
                const v = vals[i];
                const sc = axes[s] === "right" ? right : left;
                const y0 = yOfIn(sc, Math.max(0, sc.lo));
                const y = yOf(s, v);
                const top = Math.min(y, y0), h = Math.abs(y - y0);
                const bx = gx + s * barW;
                return (
                  <path key={s} d={topRoundedRect(bx + 1, top, Math.max(1, barW - 2), Math.max(0, h), 3)} fill={colors[s]}>
                    <title>{`${lab} · ${yCols[s]}: ${fmtNum(v)}`}</title>
                  </path>
                );
              })}
              <text
                x={m.left + i * band + band / 2}
                y={H - m.bottom + (rotate ? 12 : 16)}
                textAnchor={rotate ? "end" : "middle"}
                fontSize={11}
                fill={MUTED}
                transform={rotate ? `rotate(-35 ${m.left + i * band + band / 2} ${H - m.bottom + 12})` : undefined}
              >
                {lab.length > 16 ? lab.slice(0, 15) + "…" : lab}
              </text>
            </g>
          );
        })}
      </svg>
      <Legend names={yCols} colors={colors} />
      {rows.length > BAR_CAP && (
        <div className="text-sm text-muted" style={{ textAlign: "center", marginTop: "0.3rem", direction: "rtl" }}>
          מוצגות {BAR_CAP} הקטגוריות הראשונות מתוך {rows.length.toLocaleString()}.
        </div>
      )}
    </>
  );
}

// ── Line chart (optional dual Y) ─────────────────────────────────────────────

function LineChart({ rows, xCol, yCols, colors, axes, dualY, xKind }: ChartProps & { xKind: ColKind }) {
  const rightUsed = dualY && axes.includes("right");
  const m = { top: 16, right: rightUsed ? 52 : 16, bottom: 74, left: 56 };

  const idx = rows.map((_, i) => i);
  if (xKind === "number") idx.sort((a, b) => (toNum(rows[a][xCol]) ?? 0) - (toNum(rows[b][xCol]) ?? 0));
  else if (xKind === "date") idx.sort((a, b) => Date.parse(String(rows[a][xCol])) - Date.parse(String(rows[b][xCol])));
  const ordered = idx.map((i) => rows[i]);

  const labels = ordered.map((r) => String(r[xCol] ?? ""));
  const series = yCols.map((y) => ordered.map((r) => toNum(r[y])));
  const nn = (arr: (number | null)[]) => arr.filter((v): v is number => v !== null);
  const leftVals = nn(series.filter((_, s) => axes[s] === "left").flat());
  const rightVals = nn(series.filter((_, s) => axes[s] === "right").flat());
  const left = makeScale(leftVals.length ? leftVals : nn(series.flat()));
  const right = rightUsed ? makeScale(rightVals) : left;

  const plotW = W - m.left - m.right, plotH = H - m.top - m.bottom;
  const n = Math.max(1, ordered.length - 1);
  const xOf = (i: number) => m.left + (ordered.length === 1 ? plotW / 2 : (i / n) * plotW);
  const yOfIn = (sc: Scale, v: number) => m.top + plotH - ((v - sc.lo) / (sc.hi - sc.lo)) * plotH;
  const showMarks = ordered.length <= 40;
  const labelEvery = Math.ceil(labels.length / 10);
  const rotate = labels.length > 8;
  const leftInk = axisInk(colors.filter((_, s) => axes[s] === "left"));
  const rightInk = axisInk(colors.filter((_, s) => axes[s] === "right"));

  return (
    <>
      <svg viewBox={`0 0 ${W} ${H}`} width="100%" style={{ maxWidth: W, fontFamily: "system-ui, sans-serif" }} role="img" aria-label="תרשים קו">
        {left.ticks.map((t) => (
          <g key={t}>
            <line x1={m.left} x2={W - m.right} y1={yOfIn(left, t)} y2={yOfIn(left, t)} stroke={GRID} strokeWidth={1} />
            <text x={m.left - 8} y={yOfIn(left, t) + 4} textAnchor="end" fontSize={11} fill={leftInk} style={{ fontVariantNumeric: "tabular-nums" }}>{fmtCompact(t)}</text>
          </g>
        ))}
        {rightUsed && right.ticks.map((t) => (
          <text key={t} x={W - m.right + 8} y={yOfIn(right, t) + 4} textAnchor="start" fontSize={11} fill={rightInk} style={{ fontVariantNumeric: "tabular-nums" }}>{fmtCompact(t)}</text>
        ))}
        {series.map((vals, s) => {
          const sc = axes[s] === "right" ? right : left;
          const color = colors[s];
          const pts = vals.map((v, i) => (v === null ? null : `${xOf(i)},${yOfIn(sc, v)}`)).filter(Boolean).join(" ");
          return (
            <g key={s}>
              <polyline points={pts} fill="none" stroke={color} strokeWidth={2} strokeLinejoin="round" strokeLinecap="round" />
              {showMarks && vals.map((v, i) => v === null ? null : (
                <circle key={i} cx={xOf(i)} cy={yOfIn(sc, v)} r={3.5} fill={color}>
                  <title>{`${labels[i]} · ${yCols[s]}: ${fmtNum(v)}`}</title>
                </circle>
              ))}
            </g>
          );
        })}
        {labels.map((lab, i) => (i % labelEvery === 0 ? (
          <text
            key={i}
            x={xOf(i)}
            y={H - m.bottom + (rotate ? 12 : 16)}
            textAnchor={rotate ? "end" : "middle"}
            fontSize={11}
            fill={MUTED}
            transform={rotate ? `rotate(-35 ${xOf(i)} ${H - m.bottom + 12})` : undefined}
          >
            {lab.length > 16 ? lab.slice(0, 15) + "…" : lab}
          </text>
        ) : null))}
      </svg>
      <Legend names={yCols} colors={colors} />
    </>
  );
}

// ── Pie chart (custom slice colours) ─────────────────────────────────────────

function PieChart({ rows, labelCol, valueCol, colorOverrides }: {
  rows: Row[]; labelCol: string; valueCol: string; colorOverrides: Record<string, string>;
}) {
  const entries = aggregatePie(rows, labelCol, valueCol);
  const total = entries.reduce((s, e) => s + e.value, 0);
  if (total <= 0) return <div className="text-sm text-muted" style={{ direction: "rtl" }}>אין ערכים חיוביים להצגה בעוגה.</div>;

  const cx = 200, cy = H / 2, r = 150;
  let a0 = -Math.PI / 2;
  const arcs = entries.map(({ label, value }, i) => {
    const frac = value / total;
    const a1 = a0 + frac * Math.PI * 2;
    const large = a1 - a0 > Math.PI ? 1 : 0;
    const x0 = cx + r * Math.cos(a0), y0 = cy + r * Math.sin(a0);
    const x1 = cx + r * Math.cos(a1), y1 = cy + r * Math.sin(a1);
    const mid = (a0 + a1) / 2;
    const path = `M${cx},${cy} L${x0},${y0} A${r},${r} 0 ${large} 1 ${x1},${y1} Z`;
    const lx = cx + (r + 14) * Math.cos(mid), ly = cy + (r + 14) * Math.sin(mid);
    a0 = a1;
    return { label, value, frac, path, color: colorOverrides[label] || PALETTE[i % MAX_SERIES], mid, lx, ly };
  });

  return (
    <>
      <svg viewBox={`0 0 ${W} ${H}`} width="100%" style={{ maxWidth: W, fontFamily: "system-ui, sans-serif" }} role="img" aria-label="תרשים עוגה">
        {arcs.map((a, i) => (
          <path key={i} d={a.path} fill={a.color} stroke="var(--bg, #fff)" strokeWidth={2}>
            <title>{`${a.label}: ${fmtNum(a.value)} (${(a.frac * 100).toFixed(1)}%)`}</title>
          </path>
        ))}
        {arcs.filter((a) => a.frac >= 0.04).map((a, i) => (
          <text
            key={i}
            x={a.lx}
            y={a.ly}
            textAnchor={Math.cos(a.mid) >= 0 ? "start" : "end"}
            dominantBaseline="middle"
            fontSize={11}
            fill={INK}
          >
            {(a.label.length > 14 ? a.label.slice(0, 13) + "…" : a.label) + ` ${(a.frac * 100).toFixed(0)}%`}
          </text>
        ))}
      </svg>
      <div className="flex" style={{ gap: "0.9rem", flexWrap: "wrap", justifyContent: "center", marginTop: "0.4rem", direction: "rtl" }}>
        {arcs.map((a, i) => (
          <span key={i} style={{ display: "inline-flex", gap: "0.35rem", alignItems: "center", fontSize: "0.76rem", color: INK }}>
            <span style={{ width: 12, height: 12, borderRadius: 3, background: a.color, flex: "0 0 auto" }} />
            {a.label} <span className="text-muted">({(a.frac * 100).toFixed(1)}%)</span>
          </span>
        ))}
      </div>
    </>
  );
}
