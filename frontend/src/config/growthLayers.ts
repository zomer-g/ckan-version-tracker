/**
 * Hardcoded layer definitions for the public ``/growth`` page.
 *
 * Each entry maps a human-meaningful label ("פירות הדר", "אבוקדו")
 * onto a list of expected ``growthname`` values in the agricultural-
 * parcels GeoJSON (the source layer scraped from govmap.gov.il's
 * "חלקות חקלאיות" dataset, archived as tracked dataset
 * 9574d100-15e1-4c6f-9e84-e14e1ba574c3).
 *
 * Matching semantics: case-sensitive substring on a trimmed string.
 * We pick substring (not exact) because the source data sometimes
 * carries qualifiers like "הדרים – לימון" or "אבוקדו – הס" — keeping
 * the rule loose means a single token catches every variant.
 *
 * To add a new layer (e.g. ענבי-יין), append another entry with a
 * fresh ``id``, a chart-friendly ``color``, and the substring tokens
 * that should match. No other code changes are required — the page
 * iterates over this array to build the checkbox list and the
 * Leaflet GeoJSON layers.
 */

export interface GrowthLayer {
  /** Stable identifier — used as React key, URL query parameter, and
   *  Leaflet pane name. Keep it lowercase ASCII. */
  id: string;
  /** Display label, Hebrew. Shown in the checkbox and in the popup
   *  header so the user can tell which layer a polygon belongs to. */
  labelHe: string;
  /** Display label, English (for the i18n parallel string). */
  labelEn: string;
  /** Fill / stroke colour. Pick high-contrast hues so overlapping
   *  layers stay readable; the avocado green should not be confused
   *  with the citrus orange in fullscreen view. */
  color: string;
  /** Substring matchers for the ``growthname`` property. A feature
   *  belongs to this layer if any of these is a substring of the
   *  trimmed ``growthname`` value. */
  growthnameMatches: string[];
}

export const DATASET_ID = "9574d100-15e1-4c6f-9e84-e14e1ba574c3";

export const GROWTH_LAYERS: GrowthLayer[] = [
  {
    id: "citrus",
    labelHe: "פירות הדר",
    labelEn: "Citrus",
    // Amber — distinct from the avocado green and from the OSM /
    // imagery basemap palette.
    color: "#f59e0b",
    // Citrus species we expect to find in the agricultural-parcels
    // layer. "הדרים" catches the umbrella category itself when the
    // source rolls up varieties; the rest catch specific species
    // names. Substring match so "הדרים-לימון" and "לימון בלאדי" both
    // hit.
    growthnameMatches: [
      "הדרים",
      "לימון",
      "תפוז",
      "אשכולית",
      "קלמנטינ",
      "מנדרינ",
      "פומלית",
      "פומלה",
      "אתרוג",
      "ליים",
    ],
  },
  {
    id: "avocado",
    labelHe: "אבוקדו",
    labelEn: "Avocado",
    // Avocado green — the colour the polygons would actually be in
    // the field. Easy mnemonic for the legend.
    color: "#16a34a",
    growthnameMatches: ["אבוקדו"],
  },
];

/**
 * True iff the feature's ``growthname`` property matches any of the
 * layer's substring matchers. Pulled out as a free function so the
 * GrowthPage can use it both for the in-memory filter pass and for
 * the per-feature colour decision when both layers are visible.
 */
export function featureMatchesLayer(
  layer: GrowthLayer,
  props: Record<string, unknown> | null | undefined,
): boolean {
  if (!props) return false;
  const raw = props.growthname;
  if (typeof raw !== "string") return false;
  const v = raw.trim();
  if (!v) return false;
  for (const m of layer.growthnameMatches) {
    if (v.includes(m)) return true;
  }
  return false;
}
