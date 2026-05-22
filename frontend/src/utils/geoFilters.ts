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
/** Max distinct-value count. Above this a checklist is genuinely
 *  unmanageable — even with "show more" pagination, browsing a
 *  thousand-option list is a worse UX than text search. 500 is a
 *  pragmatic ceiling: it covers e.g. ~250 yishuv-name values that
 *  appear on real country-scale GovMap layers, but still rejects
 *  pure ID columns (those get dropped earlier by the
 *  distinct === feature.length test anyway). Fields between
 *  ``COLLAPSED_VISIBLE_VALUES`` and this cap show the top-N most-
 *  frequent values by default with a "show all" toggle. */
export const MAX_DISTINCT = 500;
/** When a field has more than this many distinct values, the sidebar
 *  initially shows only the top-N most-frequent ones with a "הצג
 *  עוד (M)" button to expand. Keeps the panel scannable on the
 *  agricultural-parcels layer (~14 yeshuvname values is fine; 250 is
 *  not). */
export const COLLAPSED_VISIBLE_VALUES = 10;
/** Max length of an individual value to be considered "categorical".
 *  Above this a value looks like a timestamp / GUID / free-text rather
 *  than a domain code — checkboxes truncated to "2024-…" provide no
 *  selection signal to the user, so we drop the whole field. */
export const MAX_VALUE_LENGTH = 30;

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
  // Track which fields had ANY value exceeding MAX_VALUE_LENGTH so we
  // can drop them wholesale below. Even one timestamp in an otherwise-
  // short column means the field is probably free-form and not a good
  // facet (per-row populations vary).
  const tooLong = new Set<string>();
  for (const f of features) {
    const props = f.properties || {};
    for (const [k, v] of Object.entries(props)) {
      if (typeof v !== "string") continue;
      const trimmed = v.trim();
      if (!trimmed) continue;
      if (trimmed.length > MAX_VALUE_LENGTH) {
        tooLong.add(k);
        continue;
      }
      const bucket = raw[k] || (raw[k] = {});
      bucket[trimmed] = (bucket[trimmed] || 0) + 1;
    }
  }
  const out: Record<string, Record<string, number>> = {};
  for (const [k, vals] of Object.entries(raw)) {
    if (tooLong.has(k)) continue;
    const card = Object.keys(vals).length;
    if (card < MIN_DISTINCT || card > MAX_DISTINCT) continue;
    // Drop ID-like fields: every feature has a different value (e.g.
    // globalid, objectid). These pass the cardinality check on small
    // datasets but produce a useless one-checkbox-per-row sidebar.
    if (card === features.length) continue;
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
