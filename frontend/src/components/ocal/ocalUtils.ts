// Shared helpers for the יומן לעם (Ocal) tabs.

/** "HH:MM" from an ISO timestamptz, in the stored (UTC) wall-clock. */
export function fmtTime(iso: string | null | undefined): string {
  if (!iso) return "";
  const m = String(iso).match(/[T ](\d{2}:\d{2})/);
  if (m) return m[1];
  const d = new Date(iso);
  return isNaN(d.getTime()) ? "" : d.toISOString().slice(11, 16);
}

// Nothing about this one is Ocal-specific, and the dataset cards need it too —
// it lives in utils/dates now and is re-exported here so the Ocal tabs that
// already import it from this module keep working.
export { fmtDateHe } from "../../utils/dates";

export const HE_MONTHS = [
  "ינואר", "פברואר", "מרץ", "אפריל", "מאי", "יוני",
  "יולי", "אוגוסט", "ספטמבר", "אוקטובר", "נובמבר", "דצמבר",
];

// Sunday-first, matching the API's calendar windowing.
export const HE_DOW = ["א׳", "ב׳", "ג׳", "ד׳", "ה׳", "ו׳", "ש׳"];

/** Local-calendar YYYY-MM-DD for a Date (no timezone shift). */
export function isoDate(d: Date): string {
  const p = (n: number) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}`;
}

/** Readable participant/entity truncation. */
export function truncate(s: string | null | undefined, max = 160): string {
  if (!s) return "";
  const clean = String(s).replace(/\s+/g, " ").trim();
  return clean.length > max ? clean.slice(0, max).trimEnd() + "…" : clean;
}
