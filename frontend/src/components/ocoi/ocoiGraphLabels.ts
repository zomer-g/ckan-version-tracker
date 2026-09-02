/**
 * Relationship vocabulary for the OCOI graph and its table.
 *
 * Ported from OCOI's own components/graph/labels.ts. Both the map and the
 * accessible table read from here, so an edge cannot be described one way in
 * the picture and another way in the text — which is the whole point of having
 * the table be an equivalent rather than an approximation.
 */
import { OcoiEntityType } from "../../api/client";

export const EDGE_LABELS: Record<string, string> = {
  restricted_from: "מוגבל מ־",
  owns: "בעלות",
  manages: "מנהל",
  employed_by: "מועסק ב־",
  related_to: "קשור ל־",
  board_member: "חבר דירקטוריון",
  operates_in: "פועל בתחום",
  family_member: "בן משפחה",
  mk_expense_payment: "תשלום מתקציב הציבור",
};

export const ORIGIN_LABELS: Record<string, string> = {
  coi_declaration: "הסדר למניעת ניגוד עניינים",
  mk_expense: "הוצאות קשר עם הציבור",
};

export const TYPE_LABELS: Record<OcoiEntityType | string, string> = {
  person: "אדם",
  company: "חברה",
  association: "עמותה",
  domain: "תחום",
};

/**
 * One colour per relationship type, as CSS custom properties.
 *
 * OCOI drew three edge classes (restriction / MK expense / everything else),
 * which collapsed six distinct relationships into one indistinguishable grey.
 * Naming each type is what lets a reader tell "בן משפחה" from "מוגבל מ־"
 * without hovering. Values are tokens, not hex, so both themes work —
 * OcoiGraphView resolves them before handing them to the canvas.
 */
export const EDGE_COLORS: Record<string, string> = {
  restricted_from: "var(--fill-danger)",
  family_member: "var(--fill-pink)",
  board_member: "var(--fill-violet)",
  manages: "var(--fill-indigo)",
  employed_by: "var(--fill-sky)",
  owns: "var(--fill-warn)",
  operates_in: "var(--fill-lime)",
  mk_expense_payment: "var(--fill-good)",
  related_to: "var(--fill-neutral)",
};

export const EDGE_FALLBACK_COLOR = "var(--fill-neutral)";

/**
 * Which relationship speaks for a merged pair.
 *
 * Two entities are often joined by several rows at once (a person is both
 * "קשור ל־" a company and "מוגבל מ־" it). They are drawn as ONE line — five
 * identical curves between the same two dots say nothing — so the line takes
 * the colour of the most consequential relationship, and its label lists them
 * all. A restriction is the finding the project exists to surface, so it wins.
 */
export const EDGE_PRIORITY = [
  "restricted_from",
  "owns",
  "manages",
  "board_member",
  "employed_by",
  "family_member",
  "operates_in",
  "mk_expense_payment",
  "related_to",
];

export function edgeLabel(relType: string): string {
  return EDGE_LABELS[relType] || relType;
}

export function originLabel(origin?: string | null): string {
  if (!origin) return ORIGIN_LABELS.coi_declaration;
  return ORIGIN_LABELS[origin] || origin;
}

export function dominantRelType(types: string[]): string {
  for (const candidate of EDGE_PRIORITY) {
    if (types.includes(candidate)) return candidate;
  }
  return types[0] || "related_to";
}
