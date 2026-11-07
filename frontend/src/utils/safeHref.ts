/**
 * A link target that is safe to render: http(s), mailto, or a same-site path.
 *
 * URLs on these pages come from people (tracking requests) and from scraped
 * government pages. The server now refuses non-http(s) URLs when they are
 * submitted, and React blocks `javascript:` hrefs; this is the third layer, so
 * a value stored before the check, or a scheme React does not block (`data:`,
 * `vbscript:`), never becomes a clickable link.
 */
export function safeHref(url: string | null | undefined): string | undefined {
  const v = (url ?? "").trim();
  if (!v) return undefined;
  if (v.startsWith("/") && !v.startsWith("//")) return v;
  if (v.startsWith("#")) return v;
  try {
    const u = new URL(v);
    if (u.protocol === "http:" || u.protocol === "https:" || u.protocol === "mailto:") return v;
  } catch {
    /* not an absolute URL */
  }
  return undefined;
}
