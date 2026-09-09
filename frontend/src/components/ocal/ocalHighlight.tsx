/**
 * Showing WHY an event matched the search.
 *
 * The server matches against a tsvector built from four fields (Ocal migration
 * 021's diary_events_search_trigger):
 *
 *     title (A) · location (B) · participants (B) · dataset_name (C)
 *
 * ...with geresh/gershayim stripped and each token searched as a PREFIX (see
 * _build_tsquery in app/api/ocal.py). Two of those four fields are easy to miss
 * on a result card — participants is truncated, and dataset_name isn't shown at
 * all — so a perfectly good hit can look like a random event with the searched
 * name nowhere in sight. These helpers re-run the same matching rules on the
 * client so the card can highlight the hit and name the field it came from.
 *
 * The rules are deliberately a mirror, not a re-implementation: they can differ
 * from Postgres at the margins (the 'hebrew' config stems, we don't), which is
 * why a card that highlights nothing says so rather than claiming a mismatch.
 */
import React from "react";

/** Geresh (׳), gershayim (״), ASCII quotes and the typographic apostrophe. */
const GERESH_CLASS = "[\u05f3\u05f4\"'\u2019]";
const GERESH_RE = /[\u05f3\u05f4"'\u2019]/g;
const BOOL_RE = /^(AND|OR|NOT)$/i;

function escapeRe(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/**
 * The words a hit is allowed to highlight.
 *
 * Boolean operators are dropped, and so is the word after NOT — that term is
 * what the row must NOT contain, so highlighting it would be a lie.
 */
export function searchTerms(q: string | null | undefined): string[] {
  if (!q) return [];
  const out: string[] = [];
  const toks = q.trim().split(/\s+/).filter(Boolean);
  for (let i = 0; i < toks.length; i++) {
    const tok = toks[i];
    if (BOOL_RE.test(tok)) {
      if (tok.toUpperCase() === "NOT") i++; // skip the negated term too
      continue;
    }
    const bare = tok.replace(GERESH_RE, "");
    if (bare) out.push(bare);
  }
  return out;
}

/**
 * One regex matching any term at the start of a word, tolerating the quote
 * marks the indexer strips (מח"ש is one token there, two chars apart here) and
 * running on to the end of the word so a prefix hit highlights the whole word.
 *
 * The leading separator is captured so it can be re-emitted outside the mark —
 * JS \b is ASCII-only and useless for Hebrew, and lookbehind isn't safe to
 * assume on every browser we serve.
 */
function buildMatcher(terms: string[]): RegExp | null {
  if (!terms.length) return null;
  const alts = terms
    .map((t) => t.split("").map(escapeRe).join(`${GERESH_CLASS}*`))
    .join("|");
  const wordTail = `(?:[\\p{L}\\p{N}]|${GERESH_CLASS})*`;
  try {
    return new RegExp(`(^|[^\\p{L}\\p{N}])((?:${alts})${wordTail})`, "giu");
  } catch {
    return null; // pathological input — fall back to no highlighting
  }
}

/** Does this field hold at least one of the searched words? */
export function fieldMatches(text: string | null | undefined, terms: string[]): boolean {
  if (!text) return false;
  const re = buildMatcher(terms);
  return re ? re.test(String(text)) : false;
}

/** Collapse whitespace the way the cards display it. */
function clean(text: string): string {
  return String(text).replace(/\s+/g, " ").trim();
}

/**
 * A window of `max` chars around the first hit, instead of the first `max`
 * chars. A participants list is often hundreds of names long and the one that
 * was searched for is rarely first.
 */
export function snippetAround(
  text: string | null | undefined,
  terms: string[],
  max = 160,
): string {
  if (!text) return "";
  const s = clean(text);
  if (s.length <= max) return s;
  const re = buildMatcher(terms);
  const m = re ? re.exec(s) : null;
  if (!m) return s.slice(0, max).trimEnd() + "…";
  const at = m.index + m[1].length;
  if (at + m[2].length <= max) return s.slice(0, max).trimEnd() + "…";
  const start = Math.max(0, at - 40);
  return "…" + s.slice(start, start + max).trim() + (start + max < s.length ? "…" : "");
}

const MARK_STYLE: React.CSSProperties = {
  background: "var(--primary-100)",
  color: "inherit",
  padding: "0 2px",
  borderRadius: 2,
  fontWeight: 600,
};

/**
 * `text` with every searched word wrapped in <mark> — an element, not a styled
 * span, so the emphasis reaches a screen reader too.
 */
export function Highlight({
  text,
  terms,
}: {
  text: string | null | undefined;
  terms: string[];
}) {
  const s = text == null ? "" : String(text);
  const re = buildMatcher(terms);
  if (!s || !re) return <>{s}</>;

  const parts: React.ReactNode[] = [];
  let last = 0;
  let m: RegExpExecArray | null;
  let i = 0;
  while ((m = re.exec(s)) !== null) {
    const at = m.index + m[1].length;
    if (at > last) parts.push(s.slice(last, at));
    parts.push(
      <mark key={i++} style={MARK_STYLE}>
        {m[2]}
      </mark>,
    );
    last = at + m[2].length;
    if (m.index === re.lastIndex) re.lastIndex++; // zero-width guard
  }
  if (!parts.length) return <>{s}</>;
  if (last < s.length) parts.push(s.slice(last));
  return <>{parts}</>;
}
