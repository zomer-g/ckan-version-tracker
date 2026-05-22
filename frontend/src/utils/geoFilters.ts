/**
 * Pure helpers that drive the categorical-filter sidebar in
 * GovmapView. Lives outside the component so the logic can be reused
 * (e.g. when we eventually add a "Colour by" dropdown) and unit-
 * tested without React/Leaflet in the test runtime.
 *
 * Design notes:
 *   - Categorical detection is conservative: STRING values only, and
 *     only fields whose distinct-value cardinality lands in
 *     ``[MIN_DISTINCT, MAX_DISTINCT]``. Numeric / boolean / date
 *     fields are intentionally skipped — they show up in the per-
 *     feature popup but don't get a filter chip in v1.
 *   - Filter semantics: AND across fields, OR within a field. An
 *     empty set under a field means "this field has no active
 *     constraint" (vs. "exclude everything"). This matches how the
 *     sidebar checkboxes behave: unticking the last value of a field
 *     reverts that field to "show all", instead of hiding every row.
 */

export interface MinimalFeature {
  type: "Feature";
  properties?: Record<string, unknown> | null;
  // geometry / id etc. — we don't care about them here.
}

/** Min distinct-value count for a field to be considered a useful
 *  filter facet. Below this (i.e. all-same-value) the field is just
 *  noise — every feature passes regardless of the user's selection,
 *  so we drop it from the sidebar to save space. */
export const MIN_DISTINCT = 2;
/** Max distinct-value count. Above this a checklist is unusable
 *  (think: id fields, free-text descriptions); the user should query
 *  by other means. 50 is a soft cap roughly matching what fits in a
 *  scrollable 400px sidebar. */
export const MAX_DISTINCT = 50;

/**
 * Walks every feature's properties and returns, per qualifying field,
 * the count of features carrying each distinct value.
 *
 * Output shape:
 * ```
 * {
 *   yeshuvname: { "תקוע": 12, "אלון שבות": 7, … },
 *   moatza:     { "גוש עציון": 35 },        // dropped: 1 distinct
 *   id:         { … 35 distinct …  },       // dropped if > MAX_DISTINCT
 * }
 * ```
 *
 * Fields that fall outside the [MIN, MAX] cardinality band are
 * omitted entirely. Null / undefined values are skipped per row (they
 * never become a checkbox label).
 */
export function discoverCategoricalFields(
  features: MinimalFeature[],
): Record<string, Record<string, number>> {
  const raw: Record<string, Record<string, number>> = {};
  for (const f of features) {
    const props = f.properties || {};
    for (const [k, v] of Object.entries(props)) {
      if (typeof v !== "string") continue;
      const trimmed = v.trim();
      if (!trimmed) continue;
      const bucket = raw[k] || (raw[k] = {});
      bucket[trimmed] = (bucket[trimmed] || 0) + 1;
    }
  }
  const out: Record<string, Record<string, number>> = {};
  for (const [k, vals] of Object.entries(raw)) {
    const card = Object.keys(vals).length;
    if (card < MIN_DISTINCT || card > MAX_DISTINCT) continue;
    out[k] = vals;
  }
  return out;
}

/**
 * Filter the feature list by an AND-of-ORs predicate.
 *
 * `filters[field]` is the set of values the user has ticked for that
 * field. A feature passes the filter for that field when:
 *   - the set is empty (no constraint), OR
 *   - the feature's value for `field` is a string and the set
 *     contains it.
 * A feature passes the overall filter when it passes every field's
 * predicate (AND across fields).
 *
 * Stringification: we compare against the feature's raw string value
 * verbatim — the same path as ``discoverCategoricalFields`` — so what
 * the sidebar shows is what gets matched, even if Hebrew gershayim
 * etc. differ across rows.
 */
export function applyFilters(
  features: MinimalFeature[],
  filters: Record<string, ReadonlySet<string>>,
): MinimalFeature[] {
  const activeEntries = Object.entries(filters).filter(([, s]) => s.size > 0);
  if (activeEntries.length === 0) return features;
  return features.filter((f) => {
    const props = f.properties || {};
    for (const [field, allowed] of activeEntries) {
      const raw = (props as Record<string, unknown>)[field];
      const v = typeof raw === "string" ? raw.trim() : "";
      if (!allowed.has(v)) return false;
    }
    return true;
  });
}
