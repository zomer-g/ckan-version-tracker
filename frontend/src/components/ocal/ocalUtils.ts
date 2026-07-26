// Shared helpers for the יומן לעם (Ocal) tabs.

/** "HH:MM" from an ISO timestamptz, in the stored (UTC) wall-clock. */
export function fmtTime(iso: string | null | undefined): string {
  if (!iso) return "";
  const m = String(iso).match(/[T ](\d{2}:\d{2})/);
  if (m) return m[1];
  const d = new Date(iso);
  return isNaN(d.getTime()) ? "" : d.toISOString().slice(11, 16);
}

/** "DD.MM.YYYY" from a "YYYY-MM-DD" (or ISO) date string. */
export function fmtDateHe(dateStr: string | null | undefined): string {
  if (!dateStr) return "";
  const [y, mo, da] = String(dateStr).slice(0, 10).split("-");
  if (!y || !mo || !da) return String(dateStr);
  return `${da}.${mo}.${y}`;
}

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
