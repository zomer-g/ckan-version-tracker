import { lazy, Suspense, useEffect, useMemo, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";
import {
  findGeomColumn, categoryColumns, categoryValues, buildFeatures, SIMPLIFY_ABOVE,
} from "../utils/mapFeatures";
import { simplifyFeatureCollection } from "../utils/geoSimplify";
import type { Basemap } from "./SqlMapLeaflet";

// Leaflet is heavy; it loads only when the map view is actually opened.
const SqlMapLeaflet = lazy(() => import("./SqlMapLeaflet"));

/**
 * Charts for an in-memory SQL result (the /data console).
 *
 * Types: bar (grouped/stacked/100%), horizontal bar, line, stacked area,
 * pie/donut, scatter, and a stat tile for single-row results — plus "✨ auto"
 * that picks the best fit. Each type shows its data-shape requirement and is
 * enabled only once the current result satisfies it.
 *
 * The panel also processes the result before drawing: duplicate X values are
 * aggregated (sum/avg/count/min/max — SQL-like, no GROUP BY needed), categories
 * can be sorted and capped to a top-N (with an optional "אחר" fold), and the
 * finished chart can be downloaded as PNG/SVG.
 *
 * The chart configuration lives in the URL (chart/cx/cy/…) next to ?sql=, so a
 * link reproduces the chart, and the quick-builder on the table cube can open a
 * chart by just setting the params before running its generated SQL.
 *
 * Palette: the validated categorical order from the data-viz design system
 * (light surface). Axes/labels/grid use the app's ink tokens; series carry the
 * categorical hues (overridable). Charts render LTR (numbers + axes) in the RTL
 * page. Rendered as dependency-free inline SVG.
 */

// Validated categorical order (light). Assigned by slot, never cycled — past 8
// series we fold into "אחר" or cap.
const PALETTE = [
  "#2a78d6", "#008300", "#e87ba4", "#eda100",
  "#1baf7a", "#eb6834", "#4a3aa7", "#e34948",
];
const MAX_SERIES = PALETTE.length;

export type ChartType = "bar" | "barh" | "line" | "area" | "pie" | "scatter" | "stat" | "map";
type BarMode = "group" | "stack" | "stack100";
type SortMode = "result" | "value_desc" | "value_asc" | "label";
type AggMode = "sum" | "avg" | "count" | "min" | "max" | "none";
type ColKind = "number" | "date" | "text";
interface ColInfo { name: string; kind: ColKind }
type Axis = "left" | "right";

type Row = Record<string, unknown>;

// URL params that hold the chart config (also cleared by the page when needed).
export const CHART_PARAM_KEYS = ["chart", "cx", "cy", "cagg", "csort", "ctop", "cmode", "cflags", "ctitle"];

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

// ── data preparation (aggregate → sort → top-N) ─────────────────────────────

interface Prepared {
  labels: string[];
  series: (number | null)[][]; // [seriesIndex][categoryIndex]
  names: string[];             // series display names (agg=count → "מספר שורות")
  totalCats: number;           // category count before the top-N cap
  truncated: boolean;          // capped without an "אחר" fold
}

interface PrepOpts {
  agg: AggMode; sort: SortMode; topN: number; fold: boolean;
  xKind: ColKind; sortByX: boolean;
}

function prepare(rows: Row[], xCol: string, yCols: string[], o: PrepOpts): Prepared {
  const count = o.agg === "count";
  const names = count ? ["מספר שורות"] : yCols;

  interface Acc { sum: number; n: number; min: number; max: number }
  let entries: { label: string; vals: (number | null)[] }[];

  if (o.agg === "none") {
    entries = rows.map((r) => ({
      label: String(r[xCol] ?? ""),
      vals: yCols.map((y) => toNum(r[y])),
    }));
  } else {
    const order: string[] = [];
    const acc = new Map<string, (Acc | null)[]>();
    const rowsOf = new Map<string, number>();
    for (const r of rows) {
      const k = String(r[xCol] ?? "");
      if (!acc.has(k)) { acc.set(k, yCols.map(() => null)); rowsOf.set(k, 0); order.push(k); }
      rowsOf.set(k, rowsOf.get(k)! + 1);
      const slots = acc.get(k)!;
      yCols.forEach((y, i) => {
        const v = toNum(r[y]);
        if (v === null) return;
        const a = slots[i] || (slots[i] = { sum: 0, n: 0, min: Infinity, max: -Infinity });
        a.sum += v; a.n++; a.min = Math.min(a.min, v); a.max = Math.max(a.max, v);
      });
    }
    entries = order.map((k) => ({
      label: k,
      vals: count
        ? [rowsOf.get(k)!]
        : acc.get(k)!.map((a) =>
            a === null ? null :
            o.agg === "sum" ? a.sum :
            o.agg === "avg" ? a.sum / a.n :
            o.agg === "min" ? a.min : a.max),
    }));
  }

  if (o.sortByX && (o.xKind === "number" || o.xKind === "date")) {
    const key = o.xKind === "number"
      ? (s: string) => toNum(s) ?? Number.MAX_VALUE
      : (s: string) => { const t = Date.parse(s); return Number.isNaN(t) ? Number.MAX_VALUE : t; };
    entries.sort((a, b) => key(a.label) - key(b.label));
  } else if (o.sort === "value_desc" || o.sort === "value_asc") {
    const tot = (e: { vals: (number | null)[] }) => e.vals.reduce((s: number, v) => s + (v ?? 0), 0);
    entries.sort((a, b) => (o.sort === "value_desc" ? tot(b) - tot(a) : tot(a) - tot(b)));
  } else if (o.sort === "label") {
    entries.sort((a, b) => a.label.localeCompare(b.label, "he", { numeric: true }));
  }

  const totalCats = entries.length;
  let truncated = false;
  if (o.topN > 0 && entries.length > o.topN) {
    const foldable = o.fold && (o.agg === "sum" || o.agg === "count");
    if (foldable) {
      const head = entries.slice(0, o.topN - 1);
      const rest = entries.slice(o.topN - 1);
      head.push({
        label: "אחר",
        vals: names.map((_, i) => rest.reduce((s, e) => s + (e.vals[i] ?? 0), 0)),
      });
      entries = head;
    } else {
      entries = entries.slice(0, o.topN);
      truncated = true;
    }
  }

  return {
    labels: entries.map((e) => e.label),
    series: names.map((_, i) => entries.map((e) => e.vals[i])),
    names, totalCats, truncated,
  };
}

// ── chart requirements (shown to the user) ───────────────────────────────────

const REQS: { type: ChartType; label: string; icon: string; req: string }[] = [
  { type: "bar", label: "עמודות", icon: "📊",
    req: "עמודת תווית (ציר X) + עמודה מספרית אחת או יותר. כפילויות בציר X מסוכמות אוטומטית." },
  { type: "barh", label: "עמודות אופקיות", icon: "📋",
    req: "כמו עמודות — מומלץ לתוויות ארוכות בעברית (שמות משרדים, רשויות…)." },
  { type: "line", label: "קו", icon: "📈",
    req: "ציר X (תאריך / מספר / תווית) + עמודה מספרית אחת או יותר. ממוין לפי X." },
  { type: "area", label: "שטח נערם", icon: "🏔️",
    req: "כמו קו — כמה סדרות נערמות זו על זו, מתאים להרכב שמשתנה לאורך זמן." },
  { type: "pie", label: "עוגה", icon: "🥧",
    req: "עמודת תווית + עמודה מספרית אחת. עד 8 פרוסות — היתר מקובץ ל'אחר'." },
  { type: "scatter", label: "פיזור", icon: "⚬",
    req: "שתי עמודות מספריות (X ו-Y) — כל שורה נקודה. לבדיקת קשר בין שני מדדים." },
  { type: "stat", label: "מספר", icon: "🔢",
    req: "תוצאה של שורה אחת — הערכים המספריים מוצגים כמספרים גדולים." },
  { type: "map", label: "מפה", icon: "🗺",
    req: "עמודת גיאומטריה — geometry_wkt, ST_AsText(geom) או ST_AsGeoJSON(geom). כל שורה מצטיירת כצורה על המפה." },
];

/** Whether the current result can be drawn as `t`.
 *
 *  `hasGeom` comes from the caller because the map's requirement is unlike the
 *  others: it is not about column KINDS but about cell CONTENT — a text column
 *  that happens to hold WKT. */
function applicableType(t: ChartType, cols: ColInfo[], rowCount: number,
                        hasGeom: boolean): boolean {
  const nNum = cols.filter((c) => c.kind === "number").length;
  switch (t) {
    case "map": return hasGeom;
    case "stat": return rowCount === 1 && nNum >= 1;
    case "scatter": return nNum >= 2;
    default: return cols.length >= 2 && nNum >= 1;
  }
}

// ── download / export helpers ────────────────────────────────────────────────

function downloadBlob(filename: string, blob: Blob) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

// Serialize an SVG with every CSS var() resolved (exports must stand alone).
function svgMarkup(svgEl: SVGSVGElement): string {
  const cs = getComputedStyle(svgEl);
  let s = new XMLSerializer().serializeToString(svgEl);
  s = s.replace(/var\((--[\w-]+)(?:,\s*([^()]+))?\)/g, (_m, name, fb) => {
    const v = cs.getPropertyValue(name).trim();
    return v || (fb ? String(fb).trim() : "#000");
  });
  if (!/xmlns=/.test(s)) s = s.replace("<svg", '<svg xmlns="http://www.w3.org/2000/svg"');
  return s;
}

function exportFilename(title: string, ext: string): string {
  const base = (title.trim() || "over_chart").replace(/[\\/:*?"<>|\s]+/g, "_").slice(0, 60);
  return `${base}.${ext}`;
}

function exportSvg(svgEl: SVGSVGElement, title: string) {
  const blob = new Blob([svgMarkup(svgEl)], { type: "image/svg+xml;charset=utf-8" });
  downloadBlob(exportFilename(title, "svg"), blob);
}

function exportPng(svgEl: SVGSVGElement, title: string) {
  const s = svgMarkup(svgEl);
  const vb = svgEl.viewBox.baseVal;
  const w = vb && vb.width ? vb.width : svgEl.clientWidth || 760;
  const h = vb && vb.height ? vb.height : svgEl.clientHeight || 400;
  const scale = 2;
  const bg = getComputedStyle(svgEl).getPropertyValue("--bg").trim() || "#ffffff";
  const img = new Image();
  img.onload = () => {
    const canvas = document.createElement("canvas");
    canvas.width = w * scale;
    canvas.height = h * scale;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;
    ctx.fillStyle = bg;
    ctx.fillRect(0, 0, canvas.width, canvas.height);
    ctx.drawImage(img, 0, 0, canvas.width, canvas.height);
    canvas.toBlob((b) => { if (b) downloadBlob(exportFilename(title, "png"), b); }, "image/png");
  };
  img.src = "data:image/svg+xml;charset=utf-8," + encodeURIComponent(s);
}

// ── component ────────────────────────────────────────────────────────────────

export default function SqlChartPanel({ columns, rows, resultId = 0 }: {
  columns: string[]; rows: Row[]; resultId?: number;
}) {
  const [searchParams, setSearchParams] = useSearchParams();

  const cols = useMemo(() => columns.map((name) => ({ name, kind: inferKind(rows, name) })), [columns, rows]);
  const numCols = useMemo(() => cols.filter((c) => c.kind === "number").map((c) => c.name), [cols]);
  const catCols = useMemo(() => cols.filter((c) => c.kind !== "number").map((c) => c.name), [cols]);

  const [type, setType] = useState<ChartType | null>(null);
  const [xCol, setXCol] = useState<string>("");
  const [yCols, setYCols] = useState<string[]>([]);
  const [agg, setAgg] = useState<AggMode>("sum");
  const [sort, setSort] = useState<SortMode>("result");
  const [topN, setTopN] = useState(30);
  const [fold, setFold] = useState(false);
  const [mode, setMode] = useState<BarMode>("group");
  const [donut, setDonut] = useState(false);
  const [showVals, setShowVals] = useState(false);
  const [title, setTitle] = useState("");
  const [showSettings, setShowSettings] = useState(false);
  const [colorOverrides, setColorOverrides] = useState<Record<string, string>>({});
  const [dualY, setDualY] = useState(false);
  const [axisOf, setAxisOf] = useState<Record<string, Axis>>({});
  const chartWrapRef = useRef<HTMLDivElement>(null);

  // ── map view state ────────────────────────────────────────────────────────
  const [mapCat, setMapCat] = useState<string>("");        // "" = single colour
  const [basemap, setBasemap] = useState<Basemap>("streets");
  const [fillOpacity, setFillOpacity] = useState(0.2);
  const [pointRadius, setPointRadius] = useState(6);

  const geomCol = useMemo(() => findGeomColumn(columns, rows), [columns, rows]);
  const mapCatOptions = useMemo(
    () => (geomCol ? categoryColumns(columns, rows, geomCol) : []),
    [columns, rows, geomCol],
  );
  const mapCatValues = useMemo(
    () => (mapCat ? categoryValues(rows, mapCat) : []),
    [rows, mapCat],
  );

  const xKind: ColKind = cols.find((c) => c.name === xCol)?.kind || "text";

  function applyDefaults(t: ChartType) {
    setAgg(t === "line" || t === "area" ? "none" : "sum");
    setSort(t === "barh" || t === "pie" ? "value_desc" : "result");
    setTopN(t === "pie" ? 8 : 30);
    setFold(t === "pie");
    setMode("group");
    setDonut(false);
    setShowVals(false);
    setShowSettings(false);
    setColorOverrides({});
    setDualY(false);
    setAxisOf({});
    if (t === "map") {
      // Default to colouring by the most selective category column, since a
      // mixed result (areas + the points inside them) is the case the map is
      // most often opened for and is unreadable in one colour.
      setMapCat(mapCatOptions[0] || "");
      setBasemap("streets");
      setFillOpacity(0.2);
      setPointRadius(6);
    }
  }

  function defaultMapping(t: ChartType): { x: string; y: string[] } {
    if (t === "scatter") {
      return { x: numCols[0] || "", y: numCols[1] ? [numCols[1]] : [] };
    }
    // X/label default: date (line/area) → text category → first column (SQL
    // convention: the GROUP BY key leads, values follow).
    const x = (t === "line" || t === "area")
      ? (cols.find((c) => c.kind === "date")?.name || catCols[0] || columns[0] || "")
      : (catCols[0] || columns[0] || "");
    const y = numCols.find((n) => n !== x);
    return { x, y: y ? [y] : numCols.length ? [numCols[0]] : [] };
  }

  function openChart(t: ChartType, over?: Partial<{ x: string; y: string[] }>) {
    applyDefaults(t);
    const def = defaultMapping(t);
    // Switching between chart types keeps the current X/Y mapping when it still
    // fits the new type, so bar→line→area explorations don't reset the picks.
    const keepX = type !== null && xCol &&
      (t === "scatter" ? numCols.includes(xCol) : columns.includes(xCol));
    const prevY = yCols.filter((y) => y !== xCol && numCols.includes(y));
    const keepY = type !== null && prevY.length > 0;
    setType(t);
    setXCol(over?.x ?? (keepX ? xCol : def.x));
    setYCols(over?.y ?? (keepY ? prevY : def.y));
  }

  // ✨ Auto: pick the best-fitting type + mapping for the current result.
  function autoPick() {
    if (rows.length === 1 && numCols.length) return openChart("stat");
    const dateCol = cols.find((c) => c.kind === "date")?.name;
    if (dateCol && numCols.length) return openChart("line", { x: dateCol, y: [numCols[0]] });
    if (!catCols.length && numCols.length >= 2) {
      // Two numeric columns: when the first reads like an ordinal key (unique
      // integers per row — year, knessetnum…) the intent is a distribution, not
      // a correlation → bars over that key. Real measure-vs-measure → scatter.
      const keyLabels = rows.map((r) => String(r[numCols[0]] ?? ""));
      const keyVals = rows.map((r) => toNum(r[numCols[0]]));
      const keyLike = rows.length <= 60 &&
        keyVals.every((v, i) => (v === null ? keyLabels[i] === "" : Number.isInteger(v))) &&
        new Set(keyLabels).size === rows.length;
      if (keyLike) return openChart("bar", { x: numCols[0], y: [numCols[1]] });
      return openChart("scatter");
    }
    const x = catCols[0] || columns[0] || "";
    const sample = rows.slice(0, 60);
    const distinct = new Set(sample.map((r) => String(r[x] ?? ""))).size;
    const avgLen = sample.reduce((s, r) => s + String(r[x] ?? "").length, 0) / Math.max(1, sample.length);
    openChart(avgLen > 14 || distinct > 12 ? "barh" : "bar");
  }

  // ── URL persistence: hydrate on new result, write on config change ─────────
  const sig = `${resultId}#${columns.join("|")}#${rows.length}`;
  const prevSig = useRef<string | null>(null);
  const firstWrite = useRef(true);

  useEffect(() => {
    if (prevSig.current === sig) return;
    prevSig.current = sig;
    const t = searchParams.get("chart") as ChartType | null;
    if (!t || !REQS.some((r) => r.type === t) || !applicableType(t, cols, rows.length, !!geomCol)) {
      setType(null);
      return;
    }
    applyDefaults(t);
    const def = defaultMapping(t);
    let x = searchParams.get("cx") || def.x;
    if (!columns.includes(x) || (t === "scatter" && !numCols.includes(x))) x = def.x;
    const ys = (searchParams.get("cy") || "").split(",").filter((c) => numCols.includes(c) && c !== x);
    const cagg = searchParams.get("cagg") as AggMode | null;
    if (cagg && ["sum", "avg", "count", "min", "max", "none"].includes(cagg)) setAgg(cagg);
    const csort = searchParams.get("csort") as SortMode | null;
    if (csort && ["result", "value_desc", "value_asc", "label"].includes(csort)) setSort(csort);
    const ctop = Number(searchParams.get("ctop"));
    if (Number.isFinite(ctop) && ctop >= 0) setTopN(ctop);
    const cmode = searchParams.get("cmode") as BarMode | null;
    if (cmode && ["group", "stack", "stack100"].includes(cmode)) setMode(cmode);
    const flags = (searchParams.get("cflags") || "").split(",");
    if (flags.includes("donut")) setDonut(true);
    if (flags.includes("vals")) setShowVals(true);
    if (flags.includes("fold")) setFold(true);
    if (flags.includes("nofold")) setFold(false);
    setTitle(searchParams.get("ctitle") || "");
    setType(t);
    setXCol(x);
    setYCols(ys.length || cagg === "count" ? ys : def.y);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sig]);

  const urlCfg = type
    ? [type, xCol, yCols.join(","), agg, sort, String(topN), mode,
       [donut && "donut", showVals && "vals", fold ? "fold" : "nofold"].filter(Boolean).join(","),
       title.slice(0, 80)].join("§")
    : null;
  useEffect(() => {
    if (firstWrite.current) { firstWrite.current = false; return; }
    setSearchParams((prev) => {
      const p = new URLSearchParams(prev);
      for (const k of CHART_PARAM_KEYS) p.delete(k);
      if (type) {
        p.set("chart", type);
        if (xCol) p.set("cx", xCol);
        if (yCols.length) p.set("cy", yCols.join(","));
        p.set("cagg", agg);
        p.set("csort", sort);
        p.set("ctop", String(topN));
        if (type === "bar" || type === "barh") p.set("cmode", mode);
        const flags = [donut && "donut", showVals && "vals", fold ? "fold" : "nofold"].filter(Boolean).join(",");
        p.set("cflags", flags);
        if (title.trim()) p.set("ctitle", title.trim().slice(0, 80));
      }
      return p;
    }, { replace: true });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [urlCfg]);

  if (!rows.length) return null;

  const suggestions = REQS
    .filter((r) => applicableType(r.type, cols, rows.length, !!geomCol))
    .map((r) => r.type);
  const singleValue = type === "pie";
  const yOptions = (type === "scatter" ? numCols : numCols).filter((n) => n !== xCol);
  const activeY = (singleValue ? yCols.slice(0, 1) : yCols)
    .filter((y) => y !== xCol && numCols.includes(y))
    .slice(0, MAX_SERIES);
  const countMode = agg === "count" && type !== "scatter" && type !== "stat";
  const hasSeries = countMode || activeY.length > 0;

  // Effective prep options per type.
  const isBarish = type === "bar" || type === "barh";
  const isXSorted = type === "line" || type === "area";
  const effTopN = type === "pie" ? Math.min(8, topN || 8) : isBarish ? (topN || 100) : 0;
  const effAgg: AggMode = type === "scatter" || type === "stat" ? "none" : agg;

  const prep = useMemo<Prepared | null>(() => {
    if (!type || type === "scatter" || type === "stat" || type === "map" || !hasSeries) return null;
    return prepare(rows, xCol, activeY, {
      agg: effAgg, sort, topN: effTopN, fold, xKind, sortByX: isXSorted,
    });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [type, rows, xCol, activeY.join("|"), effAgg, sort, effTopN, fold, xKind, isXSorted]);

  const colorFor = (name: string, i: number) => colorOverrides[name] || PALETTE[i % MAX_SERIES];

  // Map features. Colours come from the SAME colorOverrides the series pickers
  // write to, so "colour by category" on the map and per-series colours on a
  // chart are one mechanism with one settings UI.
  const mapData = useMemo(() => {
    if (type !== "map" || !geomCol) return null;
    const idxOf = new Map(mapCatValues.map((v, i) => [v, i]));
    const pick = (cat: string | null) => {
      if (cat === null) return colorOverrides.__map || PALETTE[1];
      const i = idxOf.get(cat) ?? idxOf.size;
      return colorOverrides[cat] || PALETTE[i % MAX_SERIES];
    };
    const built = buildFeatures(columns, rows, geomCol, mapCat || null, pick);
    if (built.fc && built.drawn > SIMPLIFY_ABOVE) {
      // No-op on point-only sets and on very large ones — safe above the bar.
      built.fc = simplifyFeatureCollection(built.fc as never) as unknown as typeof built.fc;
    }
    return built;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [type, geomCol, columns, rows, mapCat, mapCatValues, colorOverrides]);
  const setColor = (name: string, hex: string) => setColorOverrides((p) => ({ ...p, [name]: hex }));
  const resetColor = (name: string) => setColorOverrides((p) => { const n = { ...p }; delete n[name]; return n; });

  const seriesNames = prep?.names || (countMode ? ["מספר שורות"] : activeY);
  const seriesColors = seriesNames.map((n, i) => colorFor(n, i));
  const dualYAllowed = (type === "line" || (type === "bar" && mode === "group")) && seriesNames.length >= 2;
  const seriesAxes: Axis[] = seriesNames.map((n) => (dualY && dualYAllowed ? axisOf[n] || "left" : "left"));

  // Pie entries (positive slices) — shared by the chart + the color settings.
  const pieEntries = type === "pie" && prep
    ? prep.labels.map((label, i) => ({ label, value: prep.series[0][i] ?? 0 })).filter((e) => e.value > 0)
    : [];

  const doExport = (kind: "png" | "svg") => {
    const svg = chartWrapRef.current?.querySelector("svg");
    if (!svg) return;
    if (kind === "svg") exportSvg(svg, title);
    else exportPng(svg, title);
  };

  const activeReq = type ? REQS.find((r) => r.type === type)?.req : null;

  return (
    <div className="card" style={{ marginBottom: "1rem", padding: "1rem" }}>
      <div className="flex" style={{ gap: "0.6rem", alignItems: "center", flexWrap: "wrap", marginBottom: "0.5rem" }}>
        <strong style={{ fontSize: "0.95rem" }}>📊 תרשימים</strong>
        <span className="text-sm text-muted">הציגו את תוצאת השאילתה כתרשים — או לחצו "אוטומטי" ונבחר בשבילכם</span>
      </div>

      {/* Type chips: ✨ auto + every chart type (enabled when the shape fits). */}
      <div className="flex" style={{ gap: "0.4rem", flexWrap: "wrap", marginBottom: "0.35rem", alignItems: "center" }}>
        <button
          type="button"
          onClick={autoPick}
          title="בחירה אוטומטית של סוג התרשים המתאים לתוצאה"
          style={{
            padding: "0.35rem 0.8rem", borderRadius: 999, fontSize: "0.84rem", fontWeight: 700,
            border: "1px solid var(--primary, #0f766e)", background: "var(--primary, #0f766e)",
            color: "white", cursor: "pointer",
          }}
        >
          ✨ אוטומטי
        </button>
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
                padding: "0.35rem 0.7rem", borderRadius: 999, fontSize: "0.84rem",
                fontWeight: active ? 700 : 500,
                border: active ? "2px solid var(--primary, #0f766e)" : "1px solid var(--border, #d1d5db)",
                background: active ? "var(--bg-muted, #eef2f5)" : "var(--bg, #fff)",
                color: ok ? "var(--text)" : "var(--text-muted)",
                cursor: ok ? "pointer" : "not-allowed", opacity: ok ? 1 : 0.55,
              }}
            >
              {r.icon} {r.label}
            </button>
          );
        })}
        {type && (
          <button
            type="button"
            onClick={() => setType(null)}
            title="סגירת התרשים"
            style={{
              marginInlineStart: "auto", fontSize: "0.78rem", padding: "0.25rem 0.6rem", borderRadius: 4,
              border: "1px solid var(--border, #d1d5db)", background: "none", color: "var(--text-muted)", cursor: "pointer",
            }}
          >
            ✕ סגירה
          </button>
        )}
      </div>
      <div className="text-sm text-muted" style={{ fontSize: "0.75rem", marginBottom: "0.6rem", lineHeight: 1.5 }}>
        {activeReq || "כל כפתור מציג בריחוף את מבנה הנתונים שהוא צריך; כפתור אפור = התוצאה הנוכחית לא מתאימה לסוג הזה."}
      </div>

      {type && !hasSeries && type !== "stat" && type !== "map" && (
        <div className="text-sm text-muted">אין עמודה מספרית מתאימה להצגה בתרשים.</div>
      )}

      {type === "map" && mapData?.fc && (
        <>
          {/* Map settings — the same shape as the chart controls above: a row of
              inline pickers, with colours behind the ⚙ toggle. */}
          <div className="flex" style={{ gap: "0.75rem", alignItems: "center", flexWrap: "wrap", margin: "0.4rem 0 0.6rem" }}>
            <label className="text-sm text-muted">
              צבע לפי:{" "}
              <select
                value={mapCat}
                onChange={(e) => { setMapCat(e.target.value); setColorOverrides({}); }}
                style={{ padding: "0.25rem 0.4rem", border: "1px solid var(--border, #d1d5db)", borderRadius: 4, fontSize: "0.82rem" }}
              >
                <option value="">צבע אחיד</option>
                {mapCatOptions.map((c) => <option key={c} value={c}>{c}</option>)}
              </select>
            </label>
            <label className="text-sm text-muted">
              רקע:{" "}
              <select
                value={basemap}
                onChange={(e) => setBasemap(e.target.value as Basemap)}
                style={{ padding: "0.25rem 0.4rem", border: "1px solid var(--border, #d1d5db)", borderRadius: 4, fontSize: "0.82rem" }}
              >
                <option value="streets">מפת רחובות</option>
                <option value="satellite">תצלום לוויין</option>
                <option value="none">ללא רקע</option>
              </select>
            </label>
            <span className="text-sm text-muted">
              {mapData.drawn.toLocaleString()} צורות
              {mapData.total > mapData.drawn ? ` (מתוך ${mapData.total.toLocaleString()})` : ""}
            </span>
            <button
              type="button"
              onClick={() => setShowSettings((v) => !v)}
              style={{
                fontSize: "0.78rem", padding: "0.25rem 0.6rem", borderRadius: 4, cursor: "pointer",
                border: "1px solid var(--border, #d1d5db)",
                background: showSettings ? "var(--bg-muted, #eef2f5)" : "none", color: "var(--text)",
              }}
            >
              ⚙ הגדרות {showSettings ? "▲" : "▼"}
            </button>
          </div>

          {showSettings && (
            <div style={{ border: "1px solid var(--border, #e5e7eb)", borderRadius: 6, padding: "0.7rem", marginBottom: "0.7rem" }}>
              <div className="flex" style={{ gap: "1.2rem", flexWrap: "wrap", alignItems: "center" }}>
                <label className="text-sm text-muted">
                  אטימות מילוי: {Math.round(fillOpacity * 100)}%{" "}
                  <input type="range" min={0} max={0.8} step={0.05} value={fillOpacity}
                         onChange={(e) => setFillOpacity(Number(e.target.value))}
                         style={{ verticalAlign: "middle" }} />
                </label>
                <label className="text-sm text-muted">
                  גודל נקודה: {pointRadius}{" "}
                  <input type="range" min={2} max={14} step={1} value={pointRadius}
                         onChange={(e) => setPointRadius(Number(e.target.value))}
                         style={{ verticalAlign: "middle" }} />
                </label>
              </div>

              <div style={{ marginTop: "0.6rem" }}>
                <div className="text-sm text-muted" style={{ marginBottom: "0.35rem" }}>
                  {mapCat ? "צבע לכל ערך:" : "צבע הצורות:"}
                </div>
                <div className="flex" style={{ gap: "0.9rem", flexWrap: "wrap" }}>
                  {(mapCat ? mapCatValues : ["__map"]).map((name, i) => {
                    const current = colorOverrides[name] || PALETTE[(mapCat ? i : 1) % MAX_SERIES];
                    return (
                      <span key={name} className="flex text-sm" style={{ gap: "0.35rem", alignItems: "center" }}>
                        <input
                          type="color"
                          value={current}
                          onChange={(e) => setColorOverrides((p) => ({ ...p, [name]: e.target.value }))}
                          style={{ width: 30, height: 24, padding: 0, border: "1px solid var(--border,#d1d5db)", borderRadius: 4, cursor: "pointer" }}
                          aria-label={`צבע ל-${name === "__map" ? "כל הצורות" : name}`}
                        />
                        {name === "__map" ? "כל הצורות" : name}
                        {colorOverrides[name] && (
                          <button type="button" onClick={() => setColorOverrides((p) => {
                            const n = { ...p }; delete n[name]; return n;
                          })} title="חזרה לצבע ברירת המחדל"
                            style={{ border: "none", background: "none", cursor: "pointer", color: "var(--text-muted)", fontSize: "0.75rem" }}>↺</button>
                        )}
                      </span>
                    );
                  })}
                </div>
              </div>
            </div>
          )}

          {mapCat && (
            <div className="flex" style={{ gap: "0.8rem", flexWrap: "wrap", marginBottom: "0.5rem" }}>
              {mapCatValues.map((v, i) => (
                <span key={v} className="text-sm" style={{ display: "inline-flex", gap: "0.3rem", alignItems: "center" }}>
                  <span aria-hidden style={{ width: 11, height: 11, borderRadius: 3, flex: "0 0 auto",
                                             background: colorOverrides[v] || PALETTE[i % MAX_SERIES] }} />
                  {v}
                </span>
              ))}
            </div>
          )}

          <Suspense fallback={<div className="text-sm text-muted" style={{ padding: "1rem" }}>טוען מפה…</div>}>
            <SqlMapLeaflet fc={mapData.fc} basemap={basemap}
                           fillOpacity={fillOpacity} pointRadius={pointRadius} />
          </Suspense>
        </>
      )}

      {type === "map" && !mapData?.fc && (
        <div className="text-sm text-muted">לא נמצאה גיאומטריה קריאה בתוצאה.</div>
      )}

      {type === "stat" && (
        <StatTiles row={rows[0]} numCols={numCols} title={title} />
      )}

      {type && type !== "stat" && type !== "map" && (
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
                {(type === "scatter" ? numCols : type === "pie" ? (catCols.length ? catCols : columns) : columns).map((c) => (
                  <option key={c} value={c}>{c}</option>
                ))}
              </select>
            </label>

            {countMode ? (
              <span className="text-sm text-muted">הערך: מספר השורות בכל קטגוריה (אין צורך בעמודה מספרית)</span>
            ) : singleValue || type === "scatter" ? (
              <label className="text-sm text-muted">
                {type === "scatter" ? "ציר Y: " : "ערך: "}
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
                        setYCols((prev) => e.target.checked
                          ? (prev.length >= MAX_SERIES ? prev : [...prev, c])
                          : prev.filter((x) => x !== c))
                      }
                    />
                    <span style={{ color: "var(--text)" }}>{c}</span>
                  </label>
                ))}
                {yCols.length >= MAX_SERIES && <span style={{ fontSize: "0.72rem" }}>(עד {MAX_SERIES} סדרות)</span>}
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
              ⚙ הגדרות {showSettings ? "▲" : "▼"}
            </button>
          </div>

          {/* Settings drawer */}
          {showSettings && hasSeries && (
            <div style={{ border: "1px solid var(--border, #e5e7eb)", borderRadius: 6, padding: "0.7rem 0.9rem", margin: "0 0 0.8rem", background: "var(--bg-muted, #f8fafc)" }}>
              <div className="flex" style={{ gap: "0.9rem 1.4rem", flexWrap: "wrap", alignItems: "center", marginBottom: "0.6rem" }}>
                <label className="text-sm text-muted">
                  כותרת:{" "}
                  <input
                    type="text"
                    value={title}
                    onChange={(e) => setTitle(e.target.value)}
                    placeholder="כותרת לתרשים (לא חובה)"
                    style={{ padding: "0.25rem 0.45rem", border: "1px solid var(--border, #d1d5db)", borderRadius: 4, fontSize: "0.82rem", minWidth: 200 }}
                  />
                </label>

                {type !== "scatter" && (
                  <label className="text-sm text-muted">
                    כשיש כמה שורות לאותה קטגוריה:{" "}
                    <select
                      value={agg}
                      onChange={(e) => setAgg(e.target.value as AggMode)}
                      style={{ padding: "0.25rem 0.4rem", border: "1px solid var(--border, #d1d5db)", borderRadius: 4, fontSize: "0.82rem" }}
                    >
                      <option value="sum">סכום</option>
                      <option value="avg">ממוצע</option>
                      <option value="count">ספירת שורות</option>
                      <option value="min">מינימום</option>
                      <option value="max">מקסימום</option>
                      <option value="none">ללא (כל שורה נקודה)</option>
                    </select>
                  </label>
                )}

                {(isBarish || type === "pie") && (
                  <label className="text-sm text-muted">
                    מיון:{" "}
                    <select
                      value={sort}
                      onChange={(e) => setSort(e.target.value as SortMode)}
                      style={{ padding: "0.25rem 0.4rem", border: "1px solid var(--border, #d1d5db)", borderRadius: 4, fontSize: "0.82rem" }}
                    >
                      <option value="result">כסדר התוצאה</option>
                      <option value="value_desc">ערך — מהגדול לקטן</option>
                      <option value="value_asc">ערך — מהקטן לגדול</option>
                      <option value="label">לפי התווית (א-ב)</option>
                    </select>
                  </label>
                )}

                {isBarish && (
                  <>
                    <label className="text-sm text-muted">
                      עד:{" "}
                      <select
                        value={topN}
                        onChange={(e) => setTopN(Number(e.target.value))}
                        style={{ padding: "0.25rem 0.4rem", border: "1px solid var(--border, #d1d5db)", borderRadius: 4, fontSize: "0.82rem" }}
                      >
                        {[10, 20, 30, 50].map((n) => <option key={n} value={n}>{n} קטגוריות</option>)}
                        <option value={0}>הכל (עד 100)</option>
                      </select>
                    </label>
                    {(agg === "sum" || agg === "count") && prep && prep.totalCats > effTopN && effTopN > 0 && (
                      <label className="text-sm text-muted" style={{ display: "inline-flex", gap: "0.3rem", alignItems: "center" }}>
                        <input type="checkbox" checked={fold} onChange={(e) => setFold(e.target.checked)} />
                        לקבץ את היתר ל"אחר"
                      </label>
                    )}
                    {seriesNames.length >= 2 && (
                      <label className="text-sm text-muted">
                        מצב:{" "}
                        <select
                          value={mode}
                          onChange={(e) => setMode(e.target.value as BarMode)}
                          style={{ padding: "0.25rem 0.4rem", border: "1px solid var(--border, #d1d5db)", borderRadius: 4, fontSize: "0.82rem" }}
                        >
                          <option value="group">מקובץ (זו לצד זו)</option>
                          <option value="stack">מוערם</option>
                          <option value="stack100">מוערם 100%</option>
                        </select>
                      </label>
                    )}
                    <label className="text-sm text-muted" style={{ display: "inline-flex", gap: "0.3rem", alignItems: "center" }}>
                      <input type="checkbox" checked={showVals} onChange={(e) => setShowVals(e.target.checked)} />
                      תוויות ערך
                    </label>
                  </>
                )}

                {type === "pie" && (
                  <label className="text-sm text-muted" style={{ display: "inline-flex", gap: "0.3rem", alignItems: "center" }}>
                    <input type="checkbox" checked={donut} onChange={(e) => setDonut(e.target.checked)} />
                    טבעת (דונאט)
                  </label>
                )}

                {dualYAllowed && (
                  <label className="text-sm" style={{ display: "inline-flex", gap: "0.4rem", alignItems: "center" }}>
                    <input type="checkbox" checked={dualY} onChange={(e) => setDualY(e.target.checked)} />
                    <span style={{ color: "var(--text)" }}>שני צירי Y נפרדים</span>
                    <span className="text-muted" style={{ fontSize: "0.72rem" }}>
                      (כששתי סדרות בסדרי גודל שונים — עדיף להיזהר: שני צירים עלולים להטעות)
                    </span>
                  </label>
                )}
              </div>

              {/* Per-series color (+ axis when dual-Y) */}
              {type !== "pie" ? (
                <div className="flex" style={{ flexDirection: "column", gap: "0.4rem" }}>
                  {seriesNames.map((name, i) => (
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
                      {dualY && dualYAllowed && (
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
                  {pieEntries.map((s, i) => (
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

          {hasSeries && (
            <>
              <div className="flex" style={{ gap: "0.5rem", alignItems: "center", flexWrap: "wrap", marginBottom: "0.2rem" }}>
                {title.trim() && <strong style={{ fontSize: "0.95rem" }}>{title.trim()}</strong>}
                <span style={{ marginInlineStart: "auto", display: "inline-flex", gap: "0.4rem" }}>
                  <button type="button" onClick={() => doExport("png")} title="הורדת התרשים כתמונת PNG"
                    style={{ fontSize: "0.75rem", padding: "0.2rem 0.55rem", borderRadius: 4, border: "1px solid var(--border, #d1d5db)", background: "none", color: "var(--text-muted)", cursor: "pointer" }}>
                    &#8595; PNG
                  </button>
                  <button type="button" onClick={() => doExport("svg")} title="הורדת התרשים כקובץ SVG (וקטורי)"
                    style={{ fontSize: "0.75rem", padding: "0.2rem 0.55rem", borderRadius: 4, border: "1px solid var(--border, #d1d5db)", background: "none", color: "var(--text-muted)", cursor: "pointer" }}>
                    &#8595; SVG
                  </button>
                </span>
              </div>
              <div ref={chartWrapRef} style={{ direction: "ltr", overflowX: "auto" }}>
                {type === "bar" && prep && (
                  <BarChart prep={prep} colors={seriesColors} mode={mode} axes={seriesAxes} dualY={dualY && dualYAllowed} showVals={showVals} />
                )}
                {type === "barh" && prep && (
                  <BarHChart prep={prep} colors={seriesColors} mode={mode} showVals={showVals} />
                )}
                {type === "line" && prep && (
                  <LineChart prep={prep} colors={seriesColors} axes={seriesAxes} dualY={dualY && dualYAllowed} />
                )}
                {type === "area" && prep && (
                  <AreaChart prep={prep} colors={seriesColors} />
                )}
                {type === "pie" && (
                  <PieChart entries={pieEntries} colorFor={colorFor} donut={donut} />
                )}
                {type === "scatter" && (
                  <ScatterChart rows={rows} xCol={xCol} yCols={activeY} colors={seriesColors}
                    labelCol={catCols[0] || null} />
                )}
              </div>
              {prep && prep.truncated && (
                <div className="text-sm text-muted" style={{ textAlign: "center", marginTop: "0.3rem" }}>
                  מוצגות {prep.labels.length} קטגוריות מתוך {prep.totalCats.toLocaleString()} — אפשר לשנות ב"הגדרות".
                </div>
              )}
            </>
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
const SURFACE = "var(--bg, #ffffff)";
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
function makeScale(vals: number[], zeroBased = true): Scale {
  const finite = vals.filter((v) => Number.isFinite(v));
  const max = zeroBased ? Math.max(0, ...finite) : Math.max(...finite);
  const min = zeroBased ? Math.min(0, ...finite) : Math.min(...finite);
  const ticks = niceTicks(Number.isFinite(min) ? min : 0, Number.isFinite(max) ? max : 1);
  return { ticks, lo: ticks[0], hi: ticks[ticks.length - 1] };
}
// Colour for an axis's tick labels: the series colour when that axis carries
// exactly one series (makes a dual-axis readable), else muted ink.
function axisInk(colorsOnAxis: string[]): string {
  return colorsOnAxis.length === 1 ? colorsOnAxis[0] : MUTED;
}

function truncLabel(s: string, max = 16): string {
  return s.length > max ? s.slice(0, max - 1) + "…" : s;
}

// ── Bar chart (grouped / stacked / 100%; optional dual Y in grouped) ─────────

function BarChart({ prep, colors, mode, axes, dualY, showVals }: {
  prep: Prepared; colors: string[]; mode: BarMode; axes: Axis[]; dualY: boolean; showVals: boolean;
}) {
  const { labels, names } = prep;
  const stacked = names.length >= 2 && (mode === "stack" || mode === "stack100");
  const pct = stacked && mode === "stack100";
  const rightUsed = !stacked && dualY && axes.includes("right");
  const m = { top: 20, right: rightUsed ? 52 : 16, bottom: 74, left: 56 };

  // Raw values (null→0) + display values (pct when 100%).
  const raw = prep.series.map((s) => s.map((v) => v ?? 0));
  let disp = raw;
  if (pct) {
    disp = raw.map((s, si) => s.map((v, i) => {
      const tot = raw.reduce((sum, s2) => sum + Math.max(0, s2[i]), 0);
      return tot > 0 ? (Math.max(0, v) / tot) * 100 : (si === 0 ? 0 : 0);
    }));
  }

  let left: Scale, right: Scale;
  if (stacked) {
    const tops = labels.map((_, i) => disp.reduce((s, sr) => s + Math.max(0, sr[i]), 0));
    const bots = labels.map((_, i) => disp.reduce((s, sr) => s + Math.min(0, sr[i]), 0));
    left = pct ? { ticks: [0, 20, 40, 60, 80, 100], lo: 0, hi: 100 } : makeScale([...tops, ...bots]);
    right = left;
  } else {
    const leftVals = disp.filter((_, s) => axes[s] === "left").flat();
    const rightVals = disp.filter((_, s) => axes[s] === "right").flat();
    left = makeScale(leftVals.length ? leftVals : disp.flat());
    right = rightUsed ? makeScale(rightVals) : left;
  }

  const plotW = W - m.left - m.right, plotH = H - m.top - m.bottom;
  const yOfIn = (sc: Scale, v: number) => m.top + plotH - ((v - sc.lo) / (sc.hi - sc.lo)) * plotH;
  const band = plotW / Math.max(1, labels.length);
  const groupW = Math.min(band * 0.8, stacked ? 46 : 64);
  const barW = stacked ? groupW : groupW / names.length;
  const rotate = labels.length > 8;
  const leftInk = axisInk(colors.filter((_, s) => axes[s] === "left"));
  const rightInk = axisInk(colors.filter((_, s) => axes[s] === "right"));
  const valsOk = showVals && labels.length * (stacked ? 1 : names.length) <= 40;
  const fmtVal = (v: number) => pct ? v.toFixed(0) + "%" : fmtCompact(v);

  return (
    <>
      <svg viewBox={`0 0 ${W} ${H}`} width="100%" style={{ maxWidth: W, fontFamily: "system-ui, sans-serif" }} role="img" aria-label="תרשים עמודות">
        {left.ticks.map((t) => (
          <g key={t}>
            <line x1={m.left} x2={W - m.right} y1={yOfIn(left, t)} y2={yOfIn(left, t)} stroke={GRID} strokeWidth={1} />
            <text x={m.left - 8} y={yOfIn(left, t) + 4} textAnchor="end" fontSize={11} fill={leftInk} style={{ fontVariantNumeric: "tabular-nums" }}>{pct ? t + "%" : fmtCompact(t)}</text>
          </g>
        ))}
        {rightUsed && right.ticks.map((t) => (
          <text key={t} x={W - m.right + 8} y={yOfIn(right, t) + 4} textAnchor="start" fontSize={11} fill={rightInk} style={{ fontVariantNumeric: "tabular-nums" }}>{fmtCompact(t)}</text>
        ))}
        <line x1={m.left} x2={W - m.right} y1={yOfIn(left, Math.max(0, left.lo))} y2={yOfIn(left, Math.max(0, left.lo))} stroke={AXISLINE} strokeWidth={1} />
        {labels.map((lab, i) => {
          const gx = m.left + i * band + (band - groupW) / 2;
          let posTop = 0, negBot = 0; // stacked cumulative (display units)
          return (
            <g key={i}>
              {names.map((_, s) => {
                const v = disp[s][i];
                const rawV = raw[s][i];
                const sc = !stacked && axes[s] === "right" ? right : left;
                let y: number, h: number, bx: number, bw: number;
                if (stacked) {
                  bx = gx; bw = Math.max(1, barW - 2);
                  if (v >= 0) { y = yOfIn(sc, posTop + v); h = yOfIn(sc, posTop) - y; posTop += v; }
                  else { const y0 = yOfIn(sc, negBot); y = y0; h = yOfIn(sc, negBot + v) - y0; negBot += v; }
                } else {
                  const y0 = yOfIn(sc, Math.max(0, sc.lo));
                  const yv = yOfIn(sc, v);
                  y = Math.min(yv, y0); h = Math.abs(yv - y0);
                  bx = gx + s * barW; bw = Math.max(1, barW - 2);
                }
                const tip = `${lab} · ${names[s]}: ${fmtNum(rawV)}${pct ? ` (${v.toFixed(1)}%)` : ""}`;
                return stacked ? (
                  <rect key={s} x={bx + 1} y={y + 1} width={bw} height={Math.max(0, h - 2)} fill={colors[s]}>
                    <title>{tip}</title>
                  </rect>
                ) : (
                  <path key={s} d={topRoundedRect(bx + 1, y, bw, Math.max(0, h), 3)} fill={colors[s]}>
                    <title>{tip}</title>
                  </path>
                );
              })}
              {valsOk && !stacked && names.map((_, s) => {
                const v = disp[s][i];
                const sc = axes[s] === "right" ? right : left;
                return (
                  <text key={`v${s}`} x={gx + s * barW + barW / 2} y={yOfIn(sc, Math.max(0, v)) - 4}
                    textAnchor="middle" fontSize={10} fill={MUTED} style={{ fontVariantNumeric: "tabular-nums" }}>
                    {fmtVal(v)}
                  </text>
                );
              })}
              {valsOk && stacked && !pct && (
                <text x={gx + groupW / 2} y={yOfIn(left, posTop) - 4} textAnchor="middle" fontSize={10} fill={MUTED} style={{ fontVariantNumeric: "tabular-nums" }}>
                  {fmtCompact(raw.reduce((s2, sr) => s2 + Math.max(0, sr[i]), 0))}
                </text>
              )}
              <text
                x={m.left + i * band + band / 2}
                y={H - m.bottom + (rotate ? 12 : 16)}
                textAnchor={rotate ? "end" : "middle"}
                fontSize={11}
                fill={MUTED}
                transform={rotate ? `rotate(-35 ${m.left + i * band + band / 2} ${H - m.bottom + 12})` : undefined}
              >
                {truncLabel(lab)}
              </text>
            </g>
          );
        })}
      </svg>
      <Legend names={names} colors={colors} />
    </>
  );
}

// ── Horizontal bar chart (grouped / stacked / 100%) ──────────────────────────

function BarHChart({ prep, colors, mode, showVals }: {
  prep: Prepared; colors: string[]; mode: BarMode; showVals: boolean;
}) {
  const { labels, names } = prep;
  const stacked = names.length >= 2 && (mode === "stack" || mode === "stack100");
  const pct = stacked && mode === "stack100";

  const raw = prep.series.map((s) => s.map((v) => v ?? 0));
  let disp = raw;
  if (pct) {
    disp = raw.map((s) => s.map((v, i) => {
      const tot = raw.reduce((sum, s2) => sum + Math.max(0, s2[i]), 0);
      return tot > 0 ? (Math.max(0, v) / tot) * 100 : 0;
    }));
  }

  // Left margin sized to the longest (truncated) label.
  const maxLab = labels.reduce((n, l) => Math.max(n, truncLabel(l, 28).length), 4);
  const m = { top: 8, right: 40, bottom: 34, left: Math.min(240, Math.max(80, maxLab * 7.5 + 14)) };

  let scale: Scale;
  if (stacked) {
    const tops = labels.map((_, i) => disp.reduce((s, sr) => s + Math.max(0, sr[i]), 0));
    const bots = labels.map((_, i) => disp.reduce((s, sr) => s + Math.min(0, sr[i]), 0));
    scale = pct ? { ticks: [0, 20, 40, 60, 80, 100], lo: 0, hi: 100 } : makeScale([...tops, ...bots]);
  } else {
    scale = makeScale(disp.flat());
  }

  const band = stacked || names.length === 1 ? 26 : Math.max(20, names.length * 13 + 8);
  const height = m.top + m.bottom + labels.length * band;
  const plotW = W - m.left - m.right;
  const xOf = (v: number) => m.left + ((v - scale.lo) / (scale.hi - scale.lo)) * plotW;
  const barH = stacked || names.length === 1 ? band - 8 : (band - 8) / names.length;
  const x0 = xOf(Math.max(0, scale.lo));
  const valsOk = showVals && labels.length * (stacked ? 1 : names.length) <= 60;
  const fmtVal = (v: number) => pct ? v.toFixed(0) + "%" : fmtCompact(v);

  return (
    <>
      <svg viewBox={`0 0 ${W} ${height}`} width="100%" style={{ maxWidth: W, fontFamily: "system-ui, sans-serif" }} role="img" aria-label="תרשים עמודות אופקיות">
        {scale.ticks.map((t) => (
          <g key={t}>
            <line x1={xOf(t)} x2={xOf(t)} y1={m.top} y2={height - m.bottom} stroke={GRID} strokeWidth={1} />
            <text x={xOf(t)} y={height - m.bottom + 16} textAnchor="middle" fontSize={11} fill={MUTED} style={{ fontVariantNumeric: "tabular-nums" }}>{pct ? t + "%" : fmtCompact(t)}</text>
          </g>
        ))}
        <line x1={x0} x2={x0} y1={m.top} y2={height - m.bottom} stroke={AXISLINE} strokeWidth={1} />
        {labels.map((lab, i) => {
          const gy = m.top + i * band + 4;
          let posEnd = 0, negEnd = 0;
          return (
            <g key={i}>
              <text x={m.left - 8} y={m.top + i * band + band / 2 + 4} textAnchor="end" fontSize={11} fill={INK}>
                {truncLabel(lab, 28)}
              </text>
              {names.map((_, s) => {
                const v = disp[s][i];
                const rawV = raw[s][i];
                let x: number, w: number, by: number, bh: number;
                if (stacked) {
                  by = gy; bh = Math.max(1, barH);
                  if (v >= 0) { x = xOf(posEnd); w = xOf(posEnd + v) - x; posEnd += v; }
                  else { const xe = xOf(negEnd + v); x = xe; w = xOf(negEnd) - xe; negEnd += v; }
                } else {
                  const xv = xOf(v);
                  x = Math.min(xv, x0); w = Math.abs(xv - x0);
                  by = gy + s * barH; bh = Math.max(1, barH - 2);
                }
                const tip = `${lab} · ${names[s]}: ${fmtNum(rawV)}${pct ? ` (${v.toFixed(1)}%)` : ""}`;
                return (
                  <rect key={s} x={x + (stacked ? 1 : 0)} y={by} width={Math.max(0, w - (stacked ? 2 : 0))} height={bh} rx={stacked ? 0 : 2} fill={colors[s]}>
                    <title>{tip}</title>
                  </rect>
                );
              })}
              {valsOk && !stacked && names.map((_, s) => {
                const v = disp[s][i];
                const xv = xOf(v);
                return (
                  <text key={`v${s}`} x={Math.max(xv, x0) + 4} y={gy + s * barH + barH / 2 + 3}
                    textAnchor="start" fontSize={10} fill={MUTED} style={{ fontVariantNumeric: "tabular-nums" }}>
                    {fmtVal(v)}
                  </text>
                );
              })}
              {valsOk && stacked && !pct && (
                <text x={xOf(posEnd) + 4} y={gy + barH / 2 + 3} textAnchor="start" fontSize={10} fill={MUTED} style={{ fontVariantNumeric: "tabular-nums" }}>
                  {fmtCompact(raw.reduce((s2, sr) => s2 + Math.max(0, sr[i]), 0))}
                </text>
              )}
            </g>
          );
        })}
      </svg>
      <Legend names={names} colors={colors} />
    </>
  );
}

// ── Line chart (optional dual Y) ─────────────────────────────────────────────

function LineChart({ prep, colors, axes, dualY }: {
  prep: Prepared; colors: string[]; axes: Axis[]; dualY: boolean;
}) {
  const { labels, series, names } = prep;
  const rightUsed = dualY && axes.includes("right");
  const m = { top: 16, right: rightUsed ? 52 : 16, bottom: 74, left: 56 };

  const nn = (arr: (number | null)[]) => arr.filter((v): v is number => v !== null);
  const leftVals = nn(series.filter((_, s) => axes[s] === "left").flat());
  const rightVals = nn(series.filter((_, s) => axes[s] === "right").flat());
  const left = makeScale(leftVals.length ? leftVals : nn(series.flat()));
  const right = rightUsed ? makeScale(rightVals) : left;

  const plotW = W - m.left - m.right, plotH = H - m.top - m.bottom;
  const n = Math.max(1, labels.length - 1);
  const xOf = (i: number) => m.left + (labels.length === 1 ? plotW / 2 : (i / n) * plotW);
  const yOfIn = (sc: Scale, v: number) => m.top + plotH - ((v - sc.lo) / (sc.hi - sc.lo)) * plotH;
  const showMarks = labels.length <= 40;
  const hitTargets = labels.length <= 200;
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
                <circle key={i} cx={xOf(i)} cy={yOfIn(sc, v)} r={3.5} fill={color} stroke={SURFACE} strokeWidth={1} />
              ))}
              {hitTargets && vals.map((v, i) => v === null ? null : (
                <circle key={`h${i}`} cx={xOf(i)} cy={yOfIn(sc, v)} r={10} fill="transparent">
                  <title>{`${labels[i]} · ${names[s]}: ${fmtNum(v)}`}</title>
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
            {truncLabel(lab)}
          </text>
        ) : null))}
      </svg>
      <Legend names={names} colors={colors} />
    </>
  );
}

// ── Stacked area chart ───────────────────────────────────────────────────────

function AreaChart({ prep, colors }: { prep: Prepared; colors: string[] }) {
  const { labels, names } = prep;
  const m = { top: 16, right: 16, bottom: 74, left: 56 };

  // null→0; stack positives up (negatives are rare here — clamped to 0 with the
  // raw value still shown in the tooltip).
  const raw = prep.series.map((s) => s.map((v) => v ?? 0));
  const vals = raw.map((s) => s.map((v) => Math.max(0, v)));
  const cum: number[][] = []; // cum[s][i] = top boundary of series s
  labels.forEach((_, i) => {
    let acc = 0;
    vals.forEach((s, si) => { acc += s[i]; (cum[si] ||= [])[i] = acc; });
  });
  const maxTot = Math.max(1, ...labels.map((_, i) => cum[names.length - 1]?.[i] ?? 0));
  const scale = makeScale([0, maxTot]);

  const plotW = W - m.left - m.right, plotH = H - m.top - m.bottom;
  const n = Math.max(1, labels.length - 1);
  const xOf = (i: number) => m.left + (labels.length === 1 ? plotW / 2 : (i / n) * plotW);
  const yOf = (v: number) => m.top + plotH - ((v - scale.lo) / (scale.hi - scale.lo)) * plotH;
  const labelEvery = Math.ceil(labels.length / 10);
  const rotate = labels.length > 8;
  const hitTargets = labels.length <= 200;

  return (
    <>
      <svg viewBox={`0 0 ${W} ${H}`} width="100%" style={{ maxWidth: W, fontFamily: "system-ui, sans-serif" }} role="img" aria-label="תרשים שטח נערם">
        {scale.ticks.map((t) => (
          <g key={t}>
            <line x1={m.left} x2={W - m.right} y1={yOf(t)} y2={yOf(t)} stroke={GRID} strokeWidth={1} />
            <text x={m.left - 8} y={yOf(t) + 4} textAnchor="end" fontSize={11} fill={MUTED} style={{ fontVariantNumeric: "tabular-nums" }}>{fmtCompact(t)}</text>
          </g>
        ))}
        {names.map((_, s) => {
          const topPts = labels.map((_, i) => `${xOf(i)},${yOf(cum[s][i])}`);
          const basePts = labels.map((_, i) => `${xOf(i)},${yOf(s === 0 ? 0 : cum[s - 1][i])}`).reverse();
          return (
            <polygon key={s} points={[...topPts, ...basePts].join(" ")} fill={colors[s]} stroke={SURFACE} strokeWidth={1.5} />
          );
        }).reverse() /* draw largest cumulative first so lower bands sit on top */}
        {hitTargets && names.map((_, s) =>
          labels.map((lab, i) => (
            <circle key={`${s}-${i}`} cx={xOf(i)} cy={yOf(cum[s][i])} r={9} fill="transparent">
              <title>{`${lab} · ${names[s]}: ${fmtNum(raw[s][i])}`}</title>
            </circle>
          )),
        )}
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
            {truncLabel(lab)}
          </text>
        ) : null))}
      </svg>
      <Legend names={names} colors={colors} />
    </>
  );
}

// ── Pie / donut chart ────────────────────────────────────────────────────────

function PieChart({ entries, colorFor, donut }: {
  entries: { label: string; value: number }[];
  colorFor: (name: string, i: number) => string;
  donut: boolean;
}) {
  const total = entries.reduce((s, e) => s + e.value, 0);
  if (total <= 0) return <div className="text-sm text-muted" style={{ direction: "rtl" }}>אין ערכים חיוביים להצגה בעוגה.</div>;

  const cx = 200, cy = H / 2, r = 150, ir = donut ? r * 0.58 : 0;
  let a0 = -Math.PI / 2;
  const arcs = entries.map(({ label, value }, i) => {
    const frac = value / total;
    const a1 = a0 + frac * Math.PI * 2;
    const large = a1 - a0 > Math.PI ? 1 : 0;
    const x0 = cx + r * Math.cos(a0), y0 = cy + r * Math.sin(a0);
    const x1 = cx + r * Math.cos(a1), y1 = cy + r * Math.sin(a1);
    const mid = (a0 + a1) / 2;
    let path: string;
    if (donut) {
      const ix0 = cx + ir * Math.cos(a1), iy0 = cy + ir * Math.sin(a1);
      const ix1 = cx + ir * Math.cos(a0), iy1 = cy + ir * Math.sin(a0);
      path = `M${x0},${y0} A${r},${r} 0 ${large} 1 ${x1},${y1} L${ix0},${iy0} A${ir},${ir} 0 ${large} 0 ${ix1},${iy1} Z`;
    } else {
      path = `M${cx},${cy} L${x0},${y0} A${r},${r} 0 ${large} 1 ${x1},${y1} Z`;
    }
    const lx = cx + (r + 14) * Math.cos(mid), ly = cy + (r + 14) * Math.sin(mid);
    a0 = a1;
    return { label, value, frac, path, color: colorFor(label, i), mid, lx, ly };
  });

  return (
    <>
      <svg viewBox={`0 0 ${W} ${H}`} width="100%" style={{ maxWidth: W, fontFamily: "system-ui, sans-serif" }} role="img" aria-label="תרשים עוגה">
        {arcs.map((a, i) => (
          <path key={i} d={a.path} fill={a.color} stroke={SURFACE} strokeWidth={2}>
            <title>{`${a.label}: ${fmtNum(a.value)} (${(a.frac * 100).toFixed(1)}%)`}</title>
          </path>
        ))}
        {donut && (
          <>
            <text x={cx} y={cy - 4} textAnchor="middle" fontSize={22} fontWeight={700} fill={INK}>{fmtCompact(total)}</text>
            <text x={cx} y={cy + 16} textAnchor="middle" fontSize={11} fill={MUTED}>סה״כ</text>
          </>
        )}
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
            {truncLabel(a.label, 14) + ` ${(a.frac * 100).toFixed(0)}%`}
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

// ── Scatter chart ────────────────────────────────────────────────────────────

const SCATTER_CAP = 2000;

function ScatterChart({ rows, xCol, yCols, colors, labelCol }: {
  rows: Row[]; xCol: string; yCols: string[]; colors: string[]; labelCol: string | null;
}) {
  const m = { top: 16, right: 16, bottom: 56, left: 60 };
  const capped = rows.slice(0, SCATTER_CAP);
  const pts = capped
    .map((r) => ({
      x: toNum(r[xCol]),
      ys: yCols.map((y) => toNum(r[y])),
      label: labelCol ? String(r[labelCol] ?? "") : "",
    }))
    .filter((p) => p.x !== null && p.ys.some((v) => v !== null));

  const xs = pts.map((p) => p.x!) ;
  const yvals = pts.flatMap((p) => p.ys.filter((v): v is number => v !== null));
  if (!xs.length || !yvals.length) {
    return <div className="text-sm text-muted" style={{ direction: "rtl" }}>אין נקודות מספריות להצגה.</div>;
  }
  const xScale = makeScale(xs, false);
  const yScale = makeScale(yvals, false);
  const plotW = W - m.left - m.right, plotH = H - m.top - m.bottom;
  const xOf = (v: number) => m.left + ((v - xScale.lo) / (xScale.hi - xScale.lo)) * plotW;
  const yOf = (v: number) => m.top + plotH - ((v - yScale.lo) / (yScale.hi - yScale.lo)) * plotH;

  return (
    <>
      <svg viewBox={`0 0 ${W} ${H}`} width="100%" style={{ maxWidth: W, fontFamily: "system-ui, sans-serif" }} role="img" aria-label="תרשים פיזור">
        {yScale.ticks.map((t) => (
          <g key={`y${t}`}>
            <line x1={m.left} x2={W - m.right} y1={yOf(t)} y2={yOf(t)} stroke={GRID} strokeWidth={1} />
            <text x={m.left - 8} y={yOf(t) + 4} textAnchor="end" fontSize={11} fill={MUTED} style={{ fontVariantNumeric: "tabular-nums" }}>{fmtCompact(t)}</text>
          </g>
        ))}
        {xScale.ticks.map((t) => (
          <g key={`x${t}`}>
            <line x1={xOf(t)} x2={xOf(t)} y1={m.top} y2={H - m.bottom} stroke={GRID} strokeWidth={1} />
            <text x={xOf(t)} y={H - m.bottom + 18} textAnchor="middle" fontSize={11} fill={MUTED} style={{ fontVariantNumeric: "tabular-nums" }}>{fmtCompact(t)}</text>
          </g>
        ))}
        <text x={m.left + plotW / 2} y={H - 8} textAnchor="middle" fontSize={11} fill={MUTED}>{xCol}</text>
        {pts.map((p, i) =>
          p.ys.map((v, s) => v === null ? null : (
            <g key={`${i}-${s}`}>
              <circle cx={xOf(p.x!)} cy={yOf(v)} r={4} fill={colors[s]} fillOpacity={0.8} stroke={SURFACE} strokeWidth={1} />
              <circle cx={xOf(p.x!)} cy={yOf(v)} r={9} fill="transparent">
                <title>{`${p.label ? p.label + " · " : ""}${xCol}: ${fmtNum(p.x!)} · ${yCols[s]}: ${fmtNum(v)}`}</title>
              </circle>
            </g>
          )),
        )}
      </svg>
      <Legend names={yCols} colors={colors} />
      {rows.length > SCATTER_CAP && (
        <div className="text-sm text-muted" style={{ textAlign: "center", marginTop: "0.3rem", direction: "rtl" }}>
          מוצגות {SCATTER_CAP.toLocaleString()} הנקודות הראשונות מתוך {rows.length.toLocaleString()}.
        </div>
      )}
    </>
  );
}

// ── Stat tiles (single-row result) ───────────────────────────────────────────

function StatTiles({ row, numCols, title }: { row: Row; numCols: string[]; title: string }) {
  const shown = numCols.slice(0, 4);
  return (
    <div style={{ margin: "0.6rem 0 0.4rem" }}>
      {title.trim() && <div style={{ fontWeight: 600, marginBottom: "0.5rem" }}>{title.trim()}</div>}
      <div className="flex" style={{ gap: "1rem", flexWrap: "wrap" }}>
        {shown.map((c) => {
          const v = toNum(row[c]);
          return (
            <div key={c} style={{
              border: "1px solid var(--border, #e5e7eb)", borderRadius: 8,
              padding: "0.8rem 1.4rem", minWidth: 150, textAlign: "center",
            }}>
              <div className="text-sm text-muted" style={{ marginBottom: "0.25rem" }}>{c}</div>
              <div style={{ fontSize: "2rem", fontWeight: 700, color: INK, lineHeight: 1.1 }}>
                {v === null ? String(row[c] ?? "") : fmtNum(v)}
              </div>
            </div>
          );
        })}
      </div>
      {numCols.length > 4 && (
        <div className="text-sm text-muted" style={{ marginTop: "0.4rem" }}>
          מוצגות 4 העמודות המספריות הראשונות.
        </div>
      )}
    </div>
  );
}
