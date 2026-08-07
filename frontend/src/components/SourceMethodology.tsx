import { useEffect, useMemo, useState } from "react";
import { sources as sourcesApi } from "../api/client";
import { registrySource, type RegistrySourceView } from "../utils/sourceBadge";

/**
 * How a source is tracked, and why — the source's own account of its method.
 *
 * A tracking decision is invisible in the data it produces. A reader looking at
 * the Jerusalem building-licensing register sees files re-read on three
 * different clocks and has no way to know that the cadence rests on a
 * measurement, that most of its "open" files have not moved in a decade, or
 * that the monthly full pass is what makes the narrower runs safe. Without
 * somewhere to say that, the reasoning lives only in a commit message.
 *
 * The text is PLAIN, deliberately: it arrives from a worker's manifest, and
 * rendering markup from there would let any worker inject into this page. Only
 * three shapes are recognised — a "## " heading, a "- " bullet, and a
 * paragraph — and everything else is printed as written.
 */
type Block =
  | { kind: "h"; text: string }
  | { kind: "ul"; items: string[] }
  | { kind: "p"; text: string };

export function parseMethodology(raw: string): Block[] {
  const blocks: Block[] = [];
  for (const chunk of raw.split(/\n\s*\n/)) {
    const lines = chunk.split("\n").map((l) => l.trim()).filter(Boolean);
    if (!lines.length) continue;
    if (lines[0].startsWith("## ")) {
      blocks.push({ kind: "h", text: lines[0].slice(3).trim() });
      const rest = lines.slice(1);
      if (rest.length) blocks.push(...parseMethodology(rest.join("\n")));
      continue;
    }
    if (lines[0].startsWith("- ")) {
      // A wrapped bullet continues on an unmarked line. Requiring every line to
      // carry the marker looked equivalent and was not: one wrapped bullet made
      // the whole list fall through and render as a single run-on paragraph.
      const items: string[] = [];
      for (const line of lines) {
        if (line.startsWith("- ")) items.push(line.slice(2).trim());
        else if (items.length) items[items.length - 1] += " " + line;
        else items.push(line);
      }
      blocks.push({ kind: "ul", items });
      continue;
    }
    blocks.push({ kind: "p", text: lines.join(" ") });
  }
  return blocks;
}

export default function SourceMethodology({
  sourceId,
  collapsed = false,
}: {
  sourceId: string | undefined;
  /** Start folded — used on a dataset page, where the versions are the subject
   *  and the method is background. The source's own page shows it open. */
  collapsed?: boolean;
}) {
  const [open, setOpen] = useState(!collapsed);
  // The registry map is primed once at boot (App.tsx), but that fetch and this
  // page's own fetch race, and the map is not React state — so a page that
  // renders first would show nothing and never re-render. Read the map, and
  // fetch for ourselves when it hasn't landed yet; the response is
  // cached for five minutes, so the second call costs nothing.
  const [fetched, setFetched] = useState<RegistrySourceView | null>(null);
  const primed = registrySource(sourceId);
  useEffect(() => {
    if (primed || !sourceId) return;
    let alive = true;
    sourcesApi
      .registry()
      .then((data) => {
        if (!alive) return;
        setFetched((data.sources || []).find((s) => s.id === sourceId) ?? null);
      })
      .catch(() => {
        /* the method note is context, never a reason to break the page */
      });
    return () => {
      alive = false;
    };
  }, [sourceId, primed]);

  const source = primed ?? fetched;
  const blocks = useMemo(
    () => (source?.methodology_he ? parseMethodology(source.methodology_he) : []),
    [source?.methodology_he],
  );
  if (!blocks.length) return null;

  return (
    <section
      className="card"
      style={{ padding: "1rem 1.15rem", marginTop: "1rem", display: "grid", gap: "0.6rem" }}
      aria-labelledby={`methodology-${sourceId}`}
    >
      <button
        id={`methodology-${sourceId}`}
        onClick={() => setOpen((v) => !v)}
        aria-expanded={open}
        style={{
          all: "unset",
          cursor: "pointer",
          display: "flex",
          alignItems: "baseline",
          gap: "0.5rem",
          fontWeight: 700,
          fontSize: "0.95rem",
        }}
      >
        <span aria-hidden="true" style={{ fontSize: "0.7rem" }}>{open ? "▼" : "◀"}</span>
        שיטת המעקב — מה נמדד וכל כמה זמן נקרא
      </button>

      {open && (
        <div style={{ display: "grid", gap: "0.7rem", maxWidth: "72ch" }}>
          {blocks.map((b, i) =>
            b.kind === "h" ? (
              <h3
                key={i}
                style={{
                  fontSize: "0.88rem",
                  margin: "0.4rem 0 0",
                  color: "var(--text-muted)",
                  letterSpacing: "0.02em",
                }}
              >
                {b.text}
              </h3>
            ) : b.kind === "ul" ? (
              <ul key={i} style={{ margin: 0, paddingInlineStart: "1.2rem", display: "grid", gap: "0.3rem" }}>
                {b.items.map((it, j) => (
                  <li key={j} style={{ fontSize: "0.87rem", lineHeight: 1.65 }}>{it}</li>
                ))}
              </ul>
            ) : (
              <p key={i} style={{ margin: 0, fontSize: "0.87rem", lineHeight: 1.7 }}>
                {b.text}
              </p>
            ),
          )}
        </div>
      )}
    </section>
  );
}
