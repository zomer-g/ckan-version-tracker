/**
 * Big numbers written the way a person says them — "67 מיליון", not "67,431,908".
 *
 * The home hero reports site-wide totals (rows across every SQL table, archived
 * files) that run into the tens of millions. At that size the exact digits are
 * noise: nobody reads them, and they change on every poll. So the scale gets a
 * WORD and the value gets at most one decimal.
 *
 * The value and the scale word are returned SEPARATELY so the caller can style
 * them differently. Set as one string they read as a different kind of thing
 * from the plain counts beside them ("33 מיליון" next to "12,579"), and the
 * stat row stops scanning as a row of numbers. Rendered as a big value plus a
 * small unit, the digits stay the same size across every stat and the word
 * demotes to the unit it actually is.
 *
 * Below the first scale word (1,000) the exact number is still readable, so it
 * is kept exact and locale-grouped — and has no unit.
 */

export interface BigNumberParts {
  value: string;
  /** The scale word ("מיליון"), or null when the value is exact. */
  unit: string | null;
}

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

export function splitBigNumber(value: number, lang: string): BigNumberParts {
  const he = lang.startsWith("he");
  const locale = he ? "he-IL" : "en-US";
  const n = Math.max(0, Math.round(value));
  for (const [scale, heWord, enWord] of SCALES) {
    if (n >= scale) {
      return { value: mantissa(n, scale), unit: he ? heWord : enWord };
    }
  }
  return { value: n.toLocaleString(locale), unit: null };
}
