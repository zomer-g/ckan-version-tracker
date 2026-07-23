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
  const out: { col: string; n: number }[] = [];
  for (const c of columns) {
    if (c === geomCol) continue;
    const seen = new Set<string>();
    let nonEmpty = 0;
    let bail = false;
    for (const r of rows) {
      const v = r[c];
      if (v === null || v === undefined || v === "") continue;
      if (typeof v === "object") { bail = true; break; }
      nonEmpty++;
      seen.add(String(v));
      if (seen.size > maxDistinct) { bail = true; break; }
    }
    if (bail || seen.size < 2 || nonEmpty < seen.size * 2) continue;
    out.push({ col: c, n: seen.size });
  }
  out.sort((a, b) => a.n - b.n);          // fewest distinct values first
  return out.map((o) => o.col);
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

/** Build the FeatureCollection. `colorFor` receives the row's category value
 *  (or null when not colouring by a column) and returns the colour to draw. */
export function buildFeatures(
  columns: string[],
  rows: Row[],
  geomCol: string,
  catCol: string | null,
  colorFor: (category: string | null) => string,
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
    const cat = catCol ? String(r[catCol] ?? "—") : null;
    properties.__color = colorFor(cat);
    features.push({ type: "Feature", geometry: g as Record<string, unknown>, properties });
  }
  if (!features.length) return { fc: null, drawn: 0, total: 0 };
  return { fc: { type: "FeatureCollection", features }, drawn: features.length, total };
}
