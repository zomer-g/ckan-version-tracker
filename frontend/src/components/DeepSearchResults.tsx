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

// Radius tokens follow the rule documented in index.css :root — buttons and
// inputs get --radius (8px), chips and badges get --radius-full. Nothing here
// hardcodes a radius, so the feature can never drift from the site.
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

const MUTED_CHIP = chip("var(--bg-muted, #eef2f5)", "var(--text-muted)");
const EXTERNAL_CHIP = chip("var(--warning, #f59e0b)", "#fff");

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
              background: "var(--primary-100, #CCEBF3)",
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
      </a>
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
            </a>
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
        style={{ padding: "0.6rem 0", color: "var(--danger, #dc2626)" }}
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

      <div style={{ display: "flex", gap: "0.4rem", flexWrap: "wrap" }}>
        {sources.map((s) => {
          const on = !hidden.has(s.id);
          const st = states[s.id];
          const status =
            st?.status === "queued" ? "ממתין" : st?.status === "loading" ? "מחפש…" : "";
          return (
            <span
              key={s.id}
              style={{
                display: "inline-flex",
                alignItems: "center",
                gap: "0.35rem",
                borderRadius: "var(--radius-full, 9999px)",
                border: `1px solid ${on ? s.color : "var(--border, #d1d5db)"}`,
                background: on ? s.color : "transparent",
                color: on ? "#fff" : "var(--text-muted)",
                padding: "0.2rem 0.6rem",
                fontSize: "0.8rem",
                opacity: s.configured ? 1 : 0.5,
              }}
              title={
                !s.configured
                  ? "המקור אינו מוגדר בשרת"
                  : (s.external ? "מקור חיצוני — " : "") + s.hint
              }
            >
              <button
                type="button"
                role="checkbox"
                aria-checked={on}
                aria-label={s.name}
                disabled={!s.configured}
                onClick={() => onToggle(s.id)}
                style={{
                  border: "none",
                  background: "none",
                  padding: 0,
                  cursor: s.configured ? "pointer" : "not-allowed",
                  color: "inherit",
                  font: "inherit",
                }}
              >
                {on ? "✓ " : ""}
                {s.name}
                {s.external && (
                  <span aria-label="מקור חיצוני" style={{ opacity: 0.85 }}>
                    {" ↗"}
                  </span>
                )}
              </button>
              {status && (
                <span style={{ fontSize: "0.68rem", opacity: 0.85 }}>{status}</span>
              )}
              {s.filters && s.filters.length > 0 && (
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
  border: "1px solid var(--border, #d1d5db)",
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
