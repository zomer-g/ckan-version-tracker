/**
 * How a מיסוי מקרקעין deal's money and share are shown, the same on every page
 * that lists deals (נדל"ן לעם's parcel card, עסקאות נדל"ן).
 *
 * The register publishes two amounts: שווי מכירה, the value finally set (and
 * the one the government's real-estate site shows), and תמורה מוצהרת, what the
 * parties declared. They agree on ~91% of rows. It also publishes the share of
 * the asset sold (portion, 0.000-1.000) against the WHOLE asset's area, so the
 * comparable price per sqm divides by area x share.
 */

/** "מלא" for the whole asset, otherwise a percentage. */
export function portionLabel(d: { portion?: string | null; portion_fraction?: number | null }): string {
  const f = d.portion_fraction ?? (d.portion != null && d.portion !== "" ? Number(d.portion) : null);
  if (f == null || Number.isNaN(f)) return "—";
  if (f >= 0.9995) return "מלא";
  if (f <= 0) return "<0.05%";
  const pct = f * 100;
  return `${pct < 10 ? pct.toLocaleString("he-IL", { maximumFractionDigits: 1 }) : Math.round(pct)}%`;
}

/** The declared amount differs from the sale value. */
export function declaredDiffers(d: { amount: number | null; declared_amount: number | null }): boolean {
  return d.amount != null && d.declared_amount != null && d.amount !== d.declared_amount;
}

export const DEAL_HEADER_TITLES: Record<string, string> = {
  "שווי מכירה": "השווי שנקבע לעסקה (כפי שמפרסם גם אתר הנדל״ן הממשלתי)",
  "תמורה מוצהרת": "מה שהצדדים הצהירו. מודגש כשהוא שונה משווי המכירה",
  "חלק נמכר": "החלק מהנכס שנמכר בעסקה. השווי משולם על החלק הזה בלבד",
  "שטח (מ״ר)": "שטח הנכס כולו, לא רק החלק שנמכר",
  "₪ למ״ר": "שווי המכירה חלקי שטח החלק שנמכר (שטח × חלק נמכר)",
};
