/**
 * Turning a SQL result into map features.
 *
 * Split out of the panel so the chart panel can own the map as just another
 * view of the same result — detection has to run before the "מפה" chip can be
 * enabled, which is well before anything decides to load Leaflet.
 */
import { wktToGeoJson, looksLikeGeometry } from "./wkt";

export type Row = Record<string, unknown>;

export interface MapFeature {
  type: "Feature";
  geometry: Record<string, unknown>;
  properties: Record<string, unknown>;
}
export interface MapFeatureCollection {
  type: "FeatureCollection";
  features: MapFeature[];
}

// Above this many features, polygon/line geometry is simplified by the caller
// (points have nothing to simplify).
export const SIMPLIFY_ABOVE = 400;
// Hard cap on drawn features. The console already caps the result at 1,000
// rows, so this only bites on a pathological result.
export const MAX_FEATURES = 2000;

/** Column holding geometry: the one whose sampled values look like WKT or
 *  GeoJSON. Name is a tiebreaker only — CONTENT decides, so this works for
 *  `ST_AsText(geom)`, a bare `geometry_wkt`, or any alias. */
export function findGeomColumn(columns: string[], rows: Row[]): string | null {
  const sample = rows.slice(0, 25);
  const byName = ["geom", "wkt", "st_astext", "st_asgeojson", "geometry", "geometry_wkt"];
  const candidates = columns.filter((c) => sample.some((r) => looksLikeGeometry(r[c])));
  if (candidates.length === 0) return null;
  candidates.sort((a, b) => {
    const ai = byName.indexOf(a.toLowerCase());
    const bi = byName.indexOf(b.toLowerCase());
    return (ai === -1 ? 99 : ai) - (bi === -1 ? 99 : bi);
  });
  return candidates[0];
}

/** Columns worth offering as "colour by": low-cardinality, with repeats.
 *
 *  This is what makes a mixed result readable — a query returning municipality
 *  polygons AND the sites inside them is one flat smear when everything shares
 *  a colour. Judged by SHAPE, not by name, so it works on any query. */
export function categoryColumns(columns: string[], rows: Row[], geomCol: string,
                                maxDistinct = 8): string[] {
  const out: { col: string; n: number; numeric: boolean }[] = [];
  for (const c of columns) {
    if (c === geomCol) continue;
    const seen = new Set<string>();
    let nonEmpty = 0;
    let bail = false;
    let numeric = true;
    for (const r of rows) {
      const v = r[c];
      if (v === null || v === undefined || v === "") continue;
      if (typeof v === "object") { bail = true; break; }
      nonEmpty++;
      seen.add(String(v));
      if (numeric && !(typeof v === "number" || /^-?[0-9]+(\.[0-9]+)?$/.test(String(v)))) {
        numeric = false;
      }
      if (seen.size > maxDistinct) { bail = true; break; }
    }
    if (bail || seen.size < 2 || nonEmpty < seen.size * 2) continue;
    out.push({ col: c, n: seen.size, numeric });
  }
  // A LABEL beats a number of the same shape. A multi-layer map query carries
  // both — `layer` with four names, and styling artefacts like fill_opacity or
  // stroke_width with three numbers — and ranking purely by "fewest distinct"
  // opened on `fill_opacity`, giving a legend that reads 0.12 / 0 / 0.05. The
  // numbers stay available in the picker; they just stop being the default,
  // because a number with a handful of values is usually a measure or a flag
  // and a text column with a handful of values is usually the thing itself.
  out.sort((a, b) => (a.numeric === b.numeric ? a.n - b.n : a.numeric ? 1 : -1));
  return out.map((o) => o.col);
}

// ── colouring by a measure (choropleth) ──────────────────────────────────────

/** One hue, light→dark — the sequential ramp for magnitude. Seven steps of the
 *  same blue: the lightest means "near the low end" and is allowed to recede
 *  toward the map, the darkest is the high end. Never a rainbow: a multi-hue
 *  ramp reads as identity, not as more-vs-less.
 *
 *  Kept on the LIGHT steps in both site themes on purpose — what these marks sit
 *  on is the basemap, not the page surface, and the basemap is light in every
 *  option that draws anything. */
export const SEQUENTIAL_RAMP = [
  "#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b",
];
/** Rows whose value is missing or not a number. Drawn, but visibly outside the
 *  scale — dropping them would silently shrink the answer. */
export const NO_VALUE_COLOR = "#9ca3af";

export type ScaleMode = "linear" | "quantile";

export interface NumericScale {
  col: string;
  min: number;
  max: number;
  /** Ascending values, for the quantile mode's breaks. */
  sorted: number[];
  mode: ScaleMode;
}

export function toNumber(v: unknown): number | null {
  if (typeof v === "number") return Number.isFinite(v) ? v : null;
  if (typeof v === "string") {
    const s = v.trim().replace(/,/g, "");
    if (s === "" || !/^-?\d+(\.\d+)?$/.test(s)) return null;
    const n = Number(s);
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

/** Columns worth offering as a colour SCALE: at least two distinct numbers, and
 *  numeric in the large majority of non-empty rows (a stray "—" must not
 *  disqualify a measure). Ordered as the result presents them, so the first
 *  offer is usually the measure the query was written to produce. */
export function numericColumns(columns: string[], rows: Row[], geomCol: string): string[] {
  const out: string[] = [];
  for (const c of columns) {
    if (c === geomCol) continue;
    let nonEmpty = 0, numeric = 0;
    const seen = new Set<number>();
    for (const r of rows) {
      const v = r[c];
      if (v === null || v === undefined || v === "") continue;
      nonEmpty++;
      const n = toNumber(v);
      if (n !== null) { numeric++; seen.add(n); }
    }
    if (nonEmpty && numeric >= nonEmpty * 0.8 && seen.size >= 2) out.push(c);
  }
  return out;
}

export function numericScale(rows: Row[], col: string, mode: ScaleMode): NumericScale | null {
  const vals: number[] = [];
  for (const r of rows) {
    const n = toNumber(r[col]);
    if (n !== null) vals.push(n);
  }
  if (vals.length < 2) return null;
  const sorted = [...vals].sort((a, b) => a - b);
  return { col, min: sorted[0], max: sorted[sorted.length - 1], sorted, mode };
}

/** Which ramp step a value lands on.
 *
 *  `linear` stretches min→max, which is what "normalised to its own extremes"
 *  means and what a reader assumes a colour scale does. `quantile` gives each
 *  step an equal SHARE OF THE ROWS instead — the honest option for counts, where
 *  one Tel Aviv puts every other settlement in the lightest bucket and the map
 *  goes blank. Both are offered because they answer different questions. */
export function scaleStep(value: number, s: NumericScale): number {
  const last = SEQUENTIAL_RAMP.length - 1;
  if (s.mode === "quantile") {
    // Rank among the values, so equal counts land in each step.
    let lo = 0, hi = s.sorted.length;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (s.sorted[mid] < value) lo = mid + 1; else hi = mid;
    }
    const frac = s.sorted.length > 1 ? lo / (s.sorted.length - 1) : 0;
    return Math.min(last, Math.floor(frac * SEQUENTIAL_RAMP.length));
  }
  if (s.max === s.min) return Math.floor(last / 2);
  const t = (value - s.min) / (s.max - s.min);
  return Math.min(last, Math.max(0, Math.floor(t * SEQUENTIAL_RAMP.length)));
}

export function scaleColor(value: unknown, s: NumericScale): string {
  const n = toNumber(value);
  if (n === null) return NO_VALUE_COLOR;
  return SEQUENTIAL_RAMP[scaleStep(n, s)];
}

/** The value at each step boundary — what the legend prints under the ramp. */
export function scaleBreaks(s: NumericScale): number[] {
  const n = SEQUENTIAL_RAMP.length;
  if (s.mode === "quantile") {
    return Array.from({ length: n + 1 }, (_, i) =>
      s.sorted[Math.min(s.sorted.length - 1, Math.round((i / n) * (s.sorted.length - 1)))]);
  }
  return Array.from({ length: n + 1 }, (_, i) => s.min + ((s.max - s.min) * i) / n);
}

/** Distinct values of a category column, in first-seen order (so colours are
 *  assigned in the order the result presents them). */
export function categoryValues(rows: Row[], col: string, max = 8): string[] {
  const seen: string[] = [];
  for (const r of rows) {
    const v = r[col];
    if (v === null || v === undefined || v === "") continue;
    const s = String(v);
    if (!seen.includes(s)) {
      seen.push(s);
      if (seen.length >= max) break;
    }
  }
  return seen;
}

/** Build the FeatureCollection.
 *
 *  `colorFor` receives the whole row and returns the colour to draw it in —
 *  which is what lets one call site serve all three colouring modes (one
 *  colour, a colour per category value, a ramp step per measure) without this
 *  function knowing which is in play. */
export function buildFeatures(
  columns: string[],
  rows: Row[],
  geomCol: string,
  colorFor: (row: Row) => string,
): { fc: MapFeatureCollection | null; drawn: number; total: number } {
  const propCols = columns.filter((c) => c !== geomCol);
  const features: MapFeature[] = [];
  let total = 0;
  for (const r of rows) {
    const g = wktToGeoJson(r[geomCol]);
    if (!g) continue;
    total++;
    if (features.length >= MAX_FEATURES) continue;
    const properties: Record<string, unknown> = {};
    for (const c of propCols) properties[c] = r[c];
    properties.__color = colorFor(r);
    features.push({ type: "Feature", geometry: g as Record<string, unknown>, properties });
  }
  if (!features.length) return { fc: null, drawn: 0, total: 0 };
  return { fc: { type: "FeatureCollection", features }, drawn: features.length, total };
}
