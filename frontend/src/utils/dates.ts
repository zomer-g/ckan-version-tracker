// Date formatting for timestamps that arrive as strings from the API.

/**
 * "DD.MM.YYYY" from a "YYYY-MM-DD" (or ISO) date string.
 *
 * Deliberately string-slicing rather than `new Date(s).toLocaleDateString()`.
 * A CKAN `metadata_modified` has no timezone suffix, so JS reads it as local
 * wall-clock; a date-only string it reads as UTC midnight instead. Either way
 * the rendered day can be off by one for a reader west of the source. The date
 * the publisher wrote is the date we show.
 */
export function fmtDateHe(dateStr: string | null | undefined): string {
  if (!dateStr) return "";
  const [y, mo, da] = String(dateStr).slice(0, 10).split("-");
  if (!y || !mo || !da) return String(dateStr);
  return `${da}.${mo}.${y}`;
}
