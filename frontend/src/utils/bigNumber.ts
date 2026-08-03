/**
 * Big numbers written the way a person says them — "67 מיליון", not "67,431,908".
 *
 * The home hero reports site-wide totals (rows across every SQL table, archived
 * files) that run into the tens of millions. At that size the exact digits are
 * noise: nobody reads them, and they change on every poll. So the scale gets a
 * WORD and the value gets at most one decimal.
 *
 * Below the first scale word (1,000) the exact number is still readable, so it
 * is kept exact and locale-grouped.
 */

// [threshold, he word, en word]. Ordered biggest first.
const SCALES: Array<[number, string, string]> = [
  [1e9, "מיליארד", "billion"],
  [1e6, "מיליון", "million"],
  [1e3, "אלף", "thousand"],
];

/**
 * Round to one decimal only while the mantissa is small (1.4 מיליון carries
 * real information; 67.4 מיליון does not — it just looks like precision we
 * do not have, since the row totals are planner estimates).
 */
function mantissa(value: number, scale: number): string {
  const n = value / scale;
  return n < 10 ? String(Math.round(n * 10) / 10) : String(Math.round(n));
}

export function formatBigNumber(value: number, lang: string): string {
  const locale = lang.startsWith("he") ? "he-IL" : "en-US";
  const n = Math.max(0, Math.round(value));
  for (const [scale, he, en] of SCALES) {
    if (n >= scale) {
      return `${mantissa(n, scale)} ${locale === "he-IL" ? he : en}`;
    }
  }
  return n.toLocaleString(locale);
}
