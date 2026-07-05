// Detecting the historical yearly versions of a featured (pinned) CBS page.
//
// The admin pins one page — e.g. "קובץ הרשויות המקומיות בישראל - 2021". That
// series has a page per year ("...2020", "...2019", ...הרשויות המקומיות בישראל
// 2017"). We want to always surface those prior-year versions under the pinned
// card. There is no series key in the data, so we detect siblings heuristically
// from titles: strip the year, tokenize, and treat two pages as the same series
// when their token sets are close (Jaccard ≥ THRESHOLD). This is deliberately
// loose enough to bridge phrasing drift ("קובץ הרשויות…" vs "הרשויות…",
// "יישובים בישראל" vs "יישובים ואוכלוסייה בישראל") yet tight enough to exclude
// unrelated pages and wide-range hub pages (extra tokens sink the Jaccard).
import { CbsResult } from "../api/client";

const THRESHOLD = 0.6;

// Tokens that carry no series identity — dropped before comparison.
const STOPWORDS = new Set(["קובץ", "קבצי", "קובצי", "-", "–", "—"]);

function stemTokens(title: string): Set<string> {
  return new Set(
    title
      // Drop year ranges and single years (1900–2099).
      .replace(/\b(19|20)\d{2}\s*[-–—]\s*(19|20)\d{2}\b/g, " ")
      .replace(/\b(19|20)\d{2}\b/g, " ")
      // Punctuation → space.
      .replace(/[.,:;()"'\-–—]/g, " ")
      .split(/\s+/)
      .map((t) => t.trim())
      .filter((t) => t.length > 1 && !STOPWORDS.has(t))
  );
}

function jaccard(a: Set<string>, b: Set<string>): number {
  if (a.size === 0 || b.size === 0) return 0;
  let inter = 0;
  for (const t of a) if (b.has(t)) inter++;
  return inter / (a.size + b.size - inter);
}

export interface YearVersion {
  yearLabel: string;
  sortYear: number;
  url: string;
  title: string;
}

// The query to feed /api/cbs/search when hunting for a pinned page's siblings —
// its title with the year stripped, so the FTS matches the series broadly.
export function seriesQuery(record: CbsResult): string {
  return [...stemTokens(record.title || "")].join(" ");
}

function yearOf(r: CbsResult): { label: string; sort: number } | null {
  const end = r.year_end ?? r.year_start;
  if (!end) return null;
  const label =
    r.year_start && r.year_end && r.year_start !== r.year_end
      ? `${r.year_start}–${r.year_end}`
      : String(end);
  return { label, sort: end };
}

// From raw search candidates, keep the ones in the same series as `record` and
// return one entry per year (best/first per year), newest first, EXCLUDING the
// pinned page itself. Empty array ⇒ the item has no historical versions.
export function historicalVersions(
  record: CbsResult,
  candidates: CbsResult[]
): YearVersion[] {
  const ref = stemTokens(record.title || "");
  if (ref.size === 0) return [];

  const byYear = new Map<string, YearVersion>();
  for (const c of candidates) {
    if (c.url === record.url) continue;
    if (!c.title) continue;
    if (jaccard(ref, stemTokens(c.title)) < THRESHOLD) continue;
    const y = yearOf(c);
    if (!y) continue;
    // First candidate for a given year wins (search returns best matches first).
    if (!byYear.has(y.label)) {
      byYear.set(y.label, {
        yearLabel: y.label,
        sortYear: y.sort,
        url: c.url,
        title: c.title,
      });
    }
  }
  return [...byYear.values()].sort((a, b) => b.sortYear - a.sortYear);
}
