/**
 * Map view for a /data console SQL result — the visible half of the spatial
 * analysis. When a query returns a geometry column (WKT from ST_AsText, the raw
 * geometry_wkt, or GeoJSON from ST_AsGeoJSON), this turns each row into a map
 * feature and draws them, so "which parks fall inside area X" is a shape on a
 * map rather than a wall of coordinates.
 *
 * Deliberately light: the geometry detection and parsing here are cheap and
 * pull in no map library. Leaflet itself lives in SqlMapLeaflet and is loaded
 * lazily, only once the user opens the map — a console visit that never touches
 * geometry pays nothing.
 *
 * Sits beside SqlChartPanel: charts answer "how much", the map answers "where".
 */
import { lazy, Suspense, useMemo, useState } from "react";
import { wktToGeoJson, looksLikeGeometry } from "../utils/wkt";
import { simplifyFeatureCollection } from "../utils/geoSimplify";
import type { MapFeatureCollection } from "./SqlMapLeaflet";

const SqlMapLeaflet = lazy(() => import("./SqlMapLeaflet"));

type Row = Record<string, unknown>;

// Above this many features the panel simplifies polygon/line geometry (points
// are untouched — there is nothing to simplify) so the map stays responsive.
const SIMPLIFY_ABOVE = 400;
// Hard cap on drawn features. The console already caps the result at 1,000
// rows, so this only bites on a pathological result; it keeps the DOM sane.
const MAX_FEATURES = 2000;

// Categorical colours — the same validated order the charts use, so a category
// keeps one identity across both panels.
const PALETTE = [
  "#2a78d6", "#008300", "#e87ba4", "#eda100",
  "#1baf7a", "#eb6834", "#4a3aa7", "#e34948",
];
const MAX_CATEGORIES = PALETTE.length;

/** A low-cardinality text column to colour by, if the result has one.
 *
 *  This is what makes a mixed result readable: a query returning municipality
 *  polygons AND the sites inside them is a green smear when everything shares a
 *  colour, and immediately legible when the two kinds differ. Picked by shape,
 *  not by name — any column with 2..8 distinct values over a result that has
 *  more rows than values qualifies. */
function findCategoryColumn(columns: string[], rows: Row[], geomCol: string): string | null {
  let best: { col: string; n: number } | null = null;
  for (const c of columns) {
    if (c === geomCol) continue;
    const seen = new Set<string>();
    let nonEmpty = 0;
    for (const r of rows) {
      const v = r[c];
      if (v === null || v === undefined || v === "") continue;
      if (typeof v === "object") { seen.clear(); break; }
      nonEmpty++;
      seen.add(String(v));
      if (seen.size > MAX_CATEGORIES) break;
    }
    if (seen.size < 2 || seen.size > MAX_CATEGORIES) continue;
    if (nonEmpty < seen.size * 2) continue;      // needs repeats to be a grouping
    if (!best || seen.size < best.n) best = { col: c, n: seen.size };
  }
  return best?.col ?? null;
}

/** Pick the column holding geometry: the one whose sampled values look like WKT
 *  or GeoJSON. Name is a tiebreaker only — content is what decides, so this
 *  works for `ST_AsText(geom)`, a bare `geometry_wkt`, or an aliased column. */
function findGeomColumn(columns: string[], rows: Row[]): string | null {
  const sample = rows.slice(0, 25);
  const byName = ["geom", "wkt", "st_astext", "st_asgeojson", "geometry", "geometry_wkt"];
  const candidates = columns.filter((c) =>
    sample.some((r) => looksLikeGeometry(r[c])),
  );
  if (candidates.length === 0) return null;
  candidates.sort((a, b) => {
    const ai = byName.indexOf(a.toLowerCase());
    const bi = byName.indexOf(b.toLowerCase());
    return (ai === -1 ? 99 : ai) - (bi === -1 ? 99 : bi);
  });
  return candidates[0];
}

export default function SqlMapPanel({
  columns,
  rows,
}: {
  columns: string[];
  rows: Row[];
  resultId: number;
}) {
  const [open, setOpen] = useState(false);

  const geomCol = useMemo(() => findGeomColumn(columns, rows), [columns, rows]);

  const { fc, drawn, total, legend } = useMemo(() => {
    const none = { fc: null as MapFeatureCollection | null, drawn: 0, total: 0,
                   legend: [] as Array<{ label: string; color: string }> };
    if (!geomCol) return none;
    const catCol = findCategoryColumn(columns, rows, geomCol);
    const colorOf = new Map<string, string>();
    const propCols = columns.filter((c) => c !== geomCol);
    const features: MapFeatureCollection["features"] = [];
    let total = 0;
    for (const r of rows) {
      const g = wktToGeoJson(r[geomCol]);
      if (!g) continue;
      total++;
      if (features.length >= MAX_FEATURES) continue;
      const properties: Record<string, unknown> = {};
      for (const c of propCols) properties[c] = r[c];
      if (catCol) {
        const key = String(r[catCol] ?? "—");
        if (!colorOf.has(key) && colorOf.size < MAX_CATEGORIES) {
          colorOf.set(key, PALETTE[colorOf.size]);
        }
        properties.__color = colorOf.get(key) ?? PALETTE[PALETTE.length - 1];
      }
      features.push({ type: "Feature", geometry: g as Record<string, unknown>, properties });
    }
    if (features.length === 0) return none;
    let fc: MapFeatureCollection = { type: "FeatureCollection", features };
    if (features.length > SIMPLIFY_ABOVE) {
      // simplifyFeatureCollection is a no-op on point-only sets and on very
      // large ones, so this is safe to call unconditionally above the threshold.
      fc = simplifyFeatureCollection(fc as never) as unknown as MapFeatureCollection;
    }
    const legend = [...colorOf.entries()].map(([label, color]) => ({ label, color }));
    return { fc, drawn: features.length, total, legend };
  }, [geomCol, columns, rows]);

  if (!fc) return null; // no geometry in this result → no panel

  return (
    <div className="card" style={{ padding: "1rem", marginBottom: "1rem" }}>
      <div className="flex" style={{ gap: "0.6rem", alignItems: "center", flexWrap: "wrap" }}>
        <button
          type="button"
          onClick={() => setOpen((v) => !v)}
          aria-expanded={open}
          style={{
            display: "inline-flex", gap: "0.4rem", alignItems: "center",
            background: open ? "#dcfce7" : "transparent", cursor: "pointer",
            border: `1px solid ${open ? "#15803d" : "var(--border,#cbd5e1)"}`,
            color: open ? "#15803d" : "var(--text)", fontWeight: 700,
            padding: "0.3rem 0.7rem", borderRadius: 6, fontSize: "0.9rem",
          }}
        >
          <svg width={15} height={15} viewBox="0 0 24 24" fill="none" stroke="currentColor"
               strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round" aria-hidden>
            <path d="M9 3 3 5.5v15L9 18l6 3 6-2.5v-15L15 6 9 3z" />
            <path d="M9 3v15M15 6v15" />
          </svg>
          {open ? "הסתר מפה" : "הצג על מפה"}
        </button>
        <span className="text-sm text-muted">
          {drawn.toLocaleString()} צורות על המפה
          {total > drawn ? ` (מתוך ${total.toLocaleString()} — הוגבל)` : ""}
        </span>
        {open && legend.length > 0 && (
          <span className="flex" style={{ gap: "0.6rem", flexWrap: "wrap", marginInlineStart: "auto" }}>
            {legend.map((l) => (
              <span key={l.label} className="text-sm" style={{ display: "inline-flex", gap: "0.3rem", alignItems: "center" }}>
                <span aria-hidden style={{ width: 11, height: 11, borderRadius: 3, background: l.color, flex: "0 0 auto" }} />
                {l.label}
              </span>
            ))}
          </span>
        )}
      </div>

      {open && (
        <div style={{ marginTop: "0.7rem" }}>
          <Suspense fallback={<div className="text-sm text-muted" style={{ padding: "1rem" }}>טוען מפה…</div>}>
            <SqlMapLeaflet fc={fc} />
          </Suspense>
        </div>
      )}
    </div>
  );
}
