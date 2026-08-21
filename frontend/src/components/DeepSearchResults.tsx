/**
 * Presentational pieces of "שאלות לעם" — the cross-source deep search.
 *
 * The page (QuestionsPage.tsx) owns all orchestration and state; everything
 * here is a pure render of what it hands over.
 *
 * Two display modes, both driven by the same column data:
 *   • לרוחב  — one horizontally scrolling column per source.
 *   • לאורך  — a single merged list, round-robin across the sources.
 *
 * The merge is deliberately NOT a ranking: there is no score comparable across
 * corpora, so we interleave each source's own ordering and say so in the UI
 * rather than implying a relevance we cannot compute.
 */
import React from "react";

import type { DeepCard, DeepColumn, DeepFilter, DeepSource } from "../api/client";

export type ColStatus = "queued" | "loading" | "done" | "error";

export interface ColState {
  status: ColStatus;
  column?: DeepColumn;
  error?: string;
}

/**
 * What a source chip shows where the ✓ used to be.
 *
 * Once a run finishes the useful thing is HOW MANY that source found, so the
 * count replaces the tick. Before a run there is nothing to count, so the tick
 * still marks the selection.
 *
 * A failed source shows ⚠ and NEVER a number. Rendering "0" for a source that
 * could not answer is the same lie this whole feature has been hunting all
 * week — TAG-IT's 25s timeout arriving as an empty 200, a tool error digging
 * out to an empty list — and it is worse in a chip than anywhere else, because
 * a chip is the one place a reader takes in every source at a glance and
 * concludes "nothing there".
 *
 * The count is the number of results actually IN the column, not the corpus
 * total: the total is null whenever we ask a full-text backend to skip
 * counting, and a badge that is sometimes a page size and sometimes a corpus
 * total would mean nothing. When a bigger total IS known it goes in the
 * tooltip, where there is room to say which is which.
 */
export function chipOutcome(st: ColState | undefined): {
  kind: "idle" | "pending" | "count" | "error";
  text: string;
  title?: string;
} {
  if (!st) return { kind: "idle", text: "✓" };
  // The visible glyph is a narrow "⋯" so the fixed slot need not be sized for
  // the widest word; the word itself lives in the tooltip and the aria-label,
  // which is where it can be read without moving anything.
  if (st.status === "queued") {
    return { kind: "pending", text: "⋯", title: "ממתין בתור" };
  }
  if (st.status === "loading") return { kind: "pending", text: "⋯", title: "מחפש…" };
  if (st.status === "error") {
    return { kind: "error", text: "⚠", title: st.error || "שגיאה בשליפה מהמקור" };
  }
  const col = st.column;
  if (col?.error) return { kind: "error", text: "⚠", title: col.error };
  const n = col?.results?.length ?? 0;
  const total = typeof col?.total === "number" ? col.total : null;
  return {
    kind: "count",
    text: String(n),
    title:
      total !== null && total > n
        ? `${n} מוצגות מתוך ${total.toLocaleString("he-IL")} תוצאות`
        : `${n} תוצאות`,
  };
}

// Radius tokens follow the rule documented in index.css :root — buttons and
// inputs get --radius (8px), chips and badges get --radius-full. Nothing here
// hardcodes a radius, so the feature can never drift from the site.
/**
 * Make a server-supplied chip colour carry readable text (WCAG 1.4.6).
 *
 * Each deep-search source ships its own fill from the backend manifest, and the
 * chip painted white on it unconditionally — 1.99:1 on the light end of the
 * ramp, which fails even 1.4.3. Choosing the ink is not enough on its own: a
 * mid-tone like #0A7A9A gives 4.93:1 against white and 4.2:1 against black, so
 * NO ink clears 7:1 on it.
 *
 * So the fill itself is walked toward whichever end already suits it until the
 * pair reaches 7:1. The hue survives — which is the whole point of the chip —
 * and the result holds no matter what a future manifest declares.
 */
function readablePair(fill: string | null | undefined): { bg: string; fg: string } {
  const m = /^#([0-9a-f]{3}|[0-9a-f]{6})$/i.exec((fill || "").trim());
  if (!m) return { bg: "var(--fill-brand)", fg: "var(--on-fill)" };
  let h = m[1];
  if (h.length === 3) h = h.split("").map((c) => c + c).join("");
  const rgb = [0, 2, 4].map((i) => parseInt(h.slice(i, i + 2), 16));

  const lum = (c: number[]) => {
    const l = c.map((v) => {
      const x = v / 255;
      return x <= 0.03928 ? x / 12.92 : Math.pow((x + 0.055) / 1.055, 2.4);
    });
    return 0.2126 * l[0] + 0.7152 * l[1] + 0.0722 * l[2];
  };
  const against = (c: number[], other: number) => {
    const a = lum(c);
    const [hi, lo] = a > other ? [a, other] : [other, a];
    return (hi + 0.05) / (lo + 0.05);
  };
  const WHITE_L = 1, INK_L = lum([11, 21, 25]);

  // Darken for white ink, lighten for dark ink — whichever the colour is
  // already closer to, so the chip keeps looking like itself.
  const goDark = lum(rgb) < 0.35;
  const target = goDark ? [0, 0, 0] : [255, 255, 255];
  let out = rgb;
  for (let i = 0; i <= 100; i++) {
    const t = i / 100;
    out = rgb.map((v, k) => Math.round(v + (target[k] - v) * t));
    if (against(out, goDark ? WHITE_L : INK_L) >= 7) break;
  }
  const hex = "#" + out.map((v) => v.toString(16).padStart(2, "0")).join("");
  return { bg: hex, fg: goDark ? "#ffffff" : "#0B1519" };
}

const chip = (bg: string, fg: string): React.CSSProperties => ({
  display: "inline-block",
  padding: "0.125rem 0.5rem",
  borderRadius: "var(--radius-full, 9999px)",
  background: bg,
  color: fg,
  fontSize: "0.72rem",
  fontWeight: 600,
  whiteSpace: "nowrap",
});

const MUTED_CHIP = chip("var(--surface-2)", "var(--text-muted)");
const EXTERNAL_CHIP = chip("var(--warning)", "#fff");

/**
 * Snippets arrive with the matched text wrapped in «…» — TAG-IT's own
 * convention, which the gateway also applies to locally-built snippets so
 * there is exactly one thing to render here regardless of which corpus
 * answered. Rendered as <mark> rather than styled spans so the highlight
 * carries meaning for screen readers too.
 */
function Highlighted({ text }: { text: string }) {
  if (!text.includes("«")) return <>{text}</>;
  return (
    <>
      {text.split(/(«[^»]*»)/g).map((part, i) =>
        part.startsWith("«") && part.endsWith("»") ? (
          <mark
            key={i}
            style={{
              background: "var(--primary-100)",
              color: "inherit",
              padding: "0 2px",
              borderRadius: 2,
              fontWeight: 600,
            }}
          >
            {part.slice(1, -1)}
          </mark>
        ) : (
          <React.Fragment key={i}>{part}</React.Fragment>
        ),
      )}
    </>
  );
}

/** Round-robin across sources: every source's top hit, then every source's 2nd… */
export function mergeInterleaved(
  sources: DeepSource[],
  results: Record<string, ColState>,
): { card: DeepCard; source: DeepSource }[] {
  const lists = sources.map((s) => ({
    source: s,
    items: results[s.id]?.column?.results ?? [],
  }));
  const out: { card: DeepCard; source: DeepSource }[] = [];
  const max = lists.reduce((m, l) => Math.max(m, l.items.length), 0);
  for (let i = 0; i < max; i++) {
    for (const l of lists) {
      if (l.items[i]) out.push({ card: l.items[i], source: l.source });
    }
  }
  return out;
}

// ── attribution ─────────────────────────────────────────────────────────────
// Not decoration: OVER's MCP servers describe processed data, and their own
// instructions require it to be labelled and linked back for verification.

export function Attribution({ source }: { source: DeepSource | DeepColumn }) {
  return (
    <div
      className="text-sm text-muted"
      style={{ marginTop: "0.5rem", fontSize: "0.72rem", lineHeight: 1.5 }}
    >
      {source.attribution.text}{" "}
      <a
        href={source.attribution.href}
        target="_blank"
        rel="noreferrer"
        style={{ color: "var(--primary)", whiteSpace: "nowrap" }}
      >
        אימות מקור ↗
      <span className="sr-only"> (נפתח בחלון חדש)</span></a>
    </div>
  );
}

// ── one result ──────────────────────────────────────────────────────────────

export function ResultCard({
  card,
  source,
  showSource,
}: {
  card: DeepCard;
  source: DeepSource;
  showSource?: boolean;
}) {
  return (
    <article
      className="card"
      style={{
        padding: "0.7rem 0.85rem",
        // In the merged list the source is not implied by position, so each
        // card carries its colour on the inline-start edge.
        borderInlineStart: showSource ? `4px solid ${source.color}` : undefined,
      }}
    >
      <div
        style={{
          display: "flex",
          gap: "0.5rem",
          alignItems: "baseline",
          justifyContent: "space-between",
          flexWrap: "wrap",
        }}
      >
        <div style={{ fontWeight: 600, fontSize: "0.92rem", lineHeight: 1.45 }}>
          {card.url ? (
            <a
              href={card.url}
              target="_blank"
              rel="noreferrer"
              style={{ color: "var(--primary)" }}
            >
              <Highlighted text={card.title} />
            <span className="sr-only"> (נפתח בחלון חדש)</span></a>
          ) : (
            <Highlighted text={card.title} />
          )}
        </div>
        {card.date && (
          <span className="text-sm text-muted" style={{ whiteSpace: "nowrap" }}>
            {card.date}
          </span>
        )}
      </div>

      {card.snippet && (
        <div
          className="text-sm text-muted"
          style={{ marginTop: "0.3rem", lineHeight: 1.6 }}
        >
          <Highlighted text={card.snippet} />
        </div>
      )}

      {(showSource || card.badges.length > 0) && (
        <div
          style={{
            display: "flex",
            gap: "0.3rem",
            flexWrap: "wrap",
            marginTop: "0.45rem",
          }}
        >
          {showSource && <span style={chip(source.color, "#fff")}>{source.name}</span>}
          {card.badges.map((b, i) => (
            <span key={`${b}-${i}`} style={MUTED_CHIP}>
              {b}
            </span>
          ))}
        </div>
      )}
    </article>
  );
}

// ── one column ──────────────────────────────────────────────────────────────

function ColumnBody({ source, state }: { source: DeepSource; state?: ColState }) {
  if (!state || state.status === "queued") {
    return (
      <div className="text-sm text-muted" style={{ padding: "0.6rem 0" }}>
        ⏳ ממתין בתור…
      </div>
    );
  }
  if (state.status === "loading") {
    return (
      <div className="text-sm text-muted" role="status" style={{ padding: "0.6rem 0" }}>
        מחפש…
      </div>
    );
  }
  if (!source.configured) {
    return (
      <div className="text-sm text-muted" style={{ padding: "0.6rem 0" }}>
        המקור אינו מוגדר בשרת.
      </div>
    );
  }
  const err = state.error || state.column?.error;
  if (err) {
    return (
      <div
        className="text-sm"
        style={{ padding: "0.6rem 0", color: "var(--danger)" }}
      >
        שגיאה בשליפה מהמקור — {err}
      </div>
    );
  }
  const items = state.column?.results ?? [];
  if (items.length === 0) {
    return (
      <div className="text-sm text-muted" style={{ padding: "0.6rem 0" }}>
        אין תוצאות לשאילתה זו.
      </div>
    );
  }
  return (
    // Not `.flex` — that utility centers items; cards must stretch full-width.
    <div style={{ display: "flex", flexDirection: "column", gap: "0.55rem" }}>
      {items.map((c, i) => (
        <ResultCard key={`${c.title}-${i}`} card={c} source={source} />
      ))}
    </div>
  );
}

export function SourceColumn({
  source,
  state,
  onHide,
}: {
  source: DeepSource;
  state?: ColState;
  onHide: (id: string) => void;
}) {
  const count = state?.status === "done" ? state.column?.results.length ?? 0 : null;
  const total = state?.column?.total;
  return (
    <section
      style={{
        flex: "1 0 300px",
        maxWidth: 400,
        minWidth: 0,
        borderTop: `3px solid ${source.color}`,
        paddingTop: "0.55rem",
      }}
    >
      <header
        style={{
          display: "flex",
          alignItems: "baseline",
          justifyContent: "space-between",
          gap: "0.4rem",
        }}
      >
        <h3 style={{ margin: 0, fontSize: "0.95rem", fontWeight: 700 }}>
          {source.name}
          {source.external && (
            <span style={{ ...EXTERNAL_CHIP, marginInlineStart: "0.4rem", fontWeight: 600 }}>
              מקור חיצוני
            </span>
          )}
        </h3>
        <div style={{ display: "flex", alignItems: "baseline", gap: "0.4rem" }}>
          <span className="text-sm text-muted" style={{ whiteSpace: "nowrap" }}>
            {count === null
              ? "…"
              : total && total > count
                ? `${count} מתוך ${total.toLocaleString("he-IL")}`
                : `${count}`}
          </span>
          <button
            type="button"
            onClick={() => onHide(source.id)}
            aria-label={`הסתר את ${source.name}`}
            title="הסתר מקור זה"
            style={{
              border: "none",
              background: "none",
              cursor: "pointer",
              color: "var(--text-muted)",
              fontSize: "0.9rem",
              lineHeight: 1,
              padding: "0 0.15rem",
            }}
          >
            ✕
          </button>
        </div>
      </header>

      {source.hint && (
        <div
          className="text-sm text-muted"
          style={{ fontSize: "0.75rem", marginTop: "0.1rem" }}
        >
          {source.hint}
        </div>
      )}

      <div style={{ marginTop: "0.55rem" }}>
        <ColumnBody source={source} state={state} />
      </div>

      <Attribution source={source} />
    </section>
  );
}

// ── source chips ────────────────────────────────────────────────────────────

export function SourceChips({
  sources,
  hidden,
  states,
  openFilter,
  onToggle,
  onOpenFilter,
  onSelectAll,
  onClearAll,
}: {
  sources: DeepSource[];
  hidden: Set<string>;
  states: Record<string, ColState>;
  openFilter: string | null;
  onToggle: (id: string) => void;
  onOpenFilter: (id: string | null) => void;
  onSelectAll: () => void;
  onClearAll: () => void;
}) {
  const shown = sources.filter((s) => !hidden.has(s.id)).length;
  return (
    <div style={{ marginBottom: "0.75rem" }}>
      <div
        style={{
          display: "flex",
          alignItems: "baseline",
          gap: "0.6rem",
          flexWrap: "wrap",
          marginBottom: "0.4rem",
        }}
      >
        <span className="text-sm text-muted">
          מקורות לחיפוש — {shown}/{sources.length} נבחרו
        </span>
        <button
          type="button"
          onClick={onSelectAll}
          className="btn-secondary"
          style={{ fontSize: "0.75rem", padding: "0.15rem 0.55rem" }}
        >
          בחר הכול
        </button>
        <button
          type="button"
          onClick={onClearAll}
          className="btn-secondary"
          style={{ fontSize: "0.75rem", padding: "0.15rem 0.55rem" }}
        >
          נקה
        </button>
      </div>

      {/* A GRID, not a wrapping flex row.
          The chips used to be inline-flex in flex-wrap, so every chip's width
          followed its own content — and the content changes throughout a run
          (✓ → "מחפש…" → a count → ⚠). Each change re-measured that chip, which
          re-flowed the whole row, which re-packed the rows below it. The result
          was a source list that shuffled under the cursor exactly while someone
          was trying to click it.
          Fixed tracks mean a chip's cell is decided by the container, never by
          its contents, so a source cannot move because its own status changed —
          nor because a NEIGHBOUR's did. */}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fill, minmax(15.5rem, 1fr))",
          gap: "0.4rem",
          alignItems: "stretch",
        }}
      >
        {sources.map((s) => {
          const on = !hidden.has(s.id);
          const st = states[s.id];
          // Only a selected source has an outcome worth showing — an excluded
          // one was never asked, and a stale count next to it would read as
          // "this source found nothing".
          const outcome = on ? chipOutcome(st) : null;
          const hasFilters = !!(s.filters && s.filters.length > 0);
          return (
            <span
              key={s.id}
              style={{
                display: "grid",
                // slot | name | filters — the outer two are fixed, so only the
                // name column ever absorbs a difference, and it never changes.
                gridTemplateColumns: `1.6rem minmax(0, 1fr) ${hasFilters ? "auto" : "0px"}`,
                alignItems: "center",
                gap: "0.35rem",
                borderRadius: "var(--radius-full, 9999px)",
                border: `1px solid ${on ? readablePair(s.color).bg : "var(--border)"}`,
                background: on ? readablePair(s.color).bg : "transparent",
                color: on ? readablePair(s.color).fg : "var(--text-muted)",
                padding: "0.25rem 0.7rem",
                minHeight: "2rem",
                fontSize: "0.8rem",
                opacity: s.configured ? 1 : 0.5,
              }}
              title={
                !s.configured
                  ? "המקור אינו מוגדר בשרת"
                  : (s.external ? "מקור חיצוני — " : "") + s.hint
              }
            >
              {/* The status slot is a FIXED-WIDTH cell of the chip grid. Its
                  contents swap between a tick, a count, a spinner and ⚠ during
                  one run; without a reserved cell each swap would shove the
                  name sideways. */}
              <span
                aria-hidden="true"
                title={outcome?.title}
                style={{
                  display: "inline-flex",
                  alignItems: "center",
                  justifyContent: "center",
                  width: "1.6rem",
                  height: "1.3rem",
                  borderRadius: "var(--radius-full, 9999px)",
                  background:
                    outcome?.kind === "count" ? "rgba(255,255,255,0.28)" : "transparent",
                  fontWeight: outcome?.kind === "count" ? 700 : 400,
                  fontVariantNumeric: "tabular-nums",
                  fontSize: outcome?.kind === "pending" ? "0.9rem" : "inherit",
                  lineHeight: 1,
                }}
              >
                {outcome?.text ?? ""}
              </span>

              <button
                type="button"
                role="checkbox"
                aria-checked={on}
                aria-label={
                  outcome && outcome.kind !== "idle"
                    ? `${s.name} — ${outcome.title || outcome.text}`
                    : s.name
                }
                disabled={!s.configured}
                onClick={() => onToggle(s.id)}
                style={{
                  border: "none",
                  background: "none",
                  padding: 0,
                  cursor: s.configured ? "pointer" : "not-allowed",
                  color: "inherit",
                  font: "inherit",
                  textAlign: "start",
                  // One line, clipped: a name that wrapped would change the
                  // chip's height and take the whole grid row with it.
                  whiteSpace: "nowrap",
                  overflow: "hidden",
                  textOverflow: "ellipsis",
                  minWidth: 0,
                }}
              >
                {s.name}
                {s.external && (
                  <span aria-label="מקור חיצוני" style={{ opacity: 0.85 }}>
                    {" ↗"}
                  </span>
                )}
              </button>

              {hasFilters && (
                <button
                  type="button"
                  onClick={() => onOpenFilter(openFilter === s.id ? null : s.id)}
                  aria-expanded={openFilter === s.id}
                  title={`מסננים ל${s.name}`}
                  style={{
                    border: "none",
                    background: "none",
                    padding: 0,
                    cursor: "pointer",
                    color: "inherit",
                    fontSize: "0.72rem",
                    textDecoration: "underline",
                    whiteSpace: "nowrap",
                  }}
                >
                  מסננים
                </button>
              )}
            </span>
          );
        })}
      </div>
    </div>
  );
}

// ── per-source filter box ───────────────────────────────────────────────────

const inputStyle: React.CSSProperties = {
  padding: "0.3rem 0.5rem",
  border: "1px solid var(--border)",
  borderRadius: "var(--radius, 8px)",
  fontSize: "0.85rem",
  width: "auto",
};

function FilterControl({
  filter,
  value,
  onChange,
}: {
  filter: DeepFilter;
  value: string;
  onChange: (v: string) => void;
}) {
  if (filter.type === "select") {
    return (
      <select
        value={value}
        onChange={(e) => onChange(e.target.value)}
        aria-label={filter.label}
        style={{ ...inputStyle, minWidth: 140 }}
      >
        {(filter.options ?? []).map((o) => (
          <option key={o.value} value={o.value}>
            {o.label}
          </option>
        ))}
      </select>
    );
  }
  return (
    <input
      type={filter.type === "date" ? "date" : filter.type === "number" ? "number" : "search"}
      value={value}
      onChange={(e) => onChange(e.target.value)}
      aria-label={filter.label}
      style={{ ...inputStyle, minWidth: 130 }}
    />
  );
}

export function SourceFilterBox({
  source,
  values,
  onChange,
  onClose,
}: {
  source: DeepSource;
  values: Record<string, string>;
  onChange: (filterId: string, value: string) => void;
  onClose: () => void;
}) {
  return (
    <div
      className="card"
      style={{
        padding: "0.65rem 0.85rem",
        marginBottom: "0.75rem",
        borderInlineStart: `4px solid ${source.color}`,
      }}
    >
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "baseline",
          marginBottom: "0.45rem",
        }}
      >
        <strong style={{ fontSize: "0.88rem" }}>מסננים — {source.name}</strong>
        <button
          type="button"
          onClick={onClose}
          className="btn-secondary"
          style={{ fontSize: "0.75rem", padding: "0.15rem 0.55rem" }}
        >
          סגור
        </button>
      </div>
      <div style={{ display: "flex", gap: "0.75rem", flexWrap: "wrap", alignItems: "flex-end" }}>
        {(source.filters ?? []).map((f) => (
          <label
            key={f.id}
            style={{
              display: "flex",
              flexDirection: "column",
              gap: "0.2rem",
              fontSize: "0.78rem",
            }}
          >
            <span className="text-muted">{f.label}</span>
            <FilterControl
              filter={f}
              value={values[f.id] ?? ""}
              onChange={(v) => onChange(f.id, v)}
            />
          </label>
        ))}
      </div>
      <div className="text-sm text-muted" style={{ marginTop: "0.4rem", fontSize: "0.72rem" }}>
        שינוי מסנן מריץ מחדש את העמודה הזו בלבד.
      </div>
    </div>
  );
}
