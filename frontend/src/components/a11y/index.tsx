import { useEffect, useId, useRef, useState } from "react";
import { Link, useLocation } from "react-router-dom";

/* ═══════════════════════════════════════════════════════════════════════════
   Shared accessibility primitives.

   Each export below exists to satisfy a named WCAG criterion across many call
   sites at once. Reach for these rather than re-solving the same problem
   locally — the point is that there is one implementation to get right.
   ═══════════════════════════════════════════════════════════════════════════ */

/** Visually hidden, still announced. */
export function SrOnly(props: { children: React.ReactNode }) {
  return <span className="sr-only">{props.children}</span>;
}

/**
 * A link that leaves the site.
 *
 * 83 links across the app opened a new window with nothing but a "↗" glyph to
 * say so — a visual cue only, and an unannounced context change (WCAG 3.2.5).
 * The arrow stays for sighted users but is now hidden from the accessibility
 * tree, with the fact spelled out beside it.
 */
export function ExternalLink(props: {
  href: string;
  children: React.ReactNode;
  className?: string;
  style?: React.CSSProperties;
  title?: string;
  /** Label prefix for screen readers when the visible text is generic. */
  describe?: string;
}) {
  const { href, children, describe, ...rest } = props;
  return (
    <a {...rest} href={href} target="_blank" rel="noopener noreferrer">
      {children}
      <span aria-hidden="true"> ↗</span>
      <span className="sr-only">
        {describe ? ` — ${describe}` : ""} (נפתח בחלון חדש)
      </span>
    </a>
  );
}

/**
 * A horizontally scrolling wrapper that a keyboard can actually scroll.
 *
 * A plain `overflow-x: auto` div takes no focus, so arrow keys never reach it
 * and the columns past the fold are unreachable without a mouse (WCAG 2.1.1).
 * tabIndex={0} plus a role and a name make it a labelled scrollable region.
 */
export function ScrollRegion(props: {
  label: string;
  children: React.ReactNode;
  style?: React.CSSProperties;
  className?: string;
  maxHeight?: string | number;
}) {
  const { label, children, style, className, maxHeight } = props;
  return (
    <div
      tabIndex={0}
      role="region"
      aria-label={label}
      className={`scroll-region${className ? " " + className : ""}`}
      style={{ overflowX: "auto", overflowY: maxHeight ? "auto" : undefined, maxHeight, ...style }}
    >
      {children}
    </div>
  );
}

/**
 * The error message for one field, wired to it.
 *
 * The app had zero uses of aria-invalid / aria-describedby: errors appeared in
 * a page-level alert and nothing tied them to the input that failed, so a
 * screen-reader user heard "error" and had to guess where (WCAG 3.3.1).
 * Use with fieldErrorProps() on the control itself.
 */
export function FieldError(props: { id: string; children?: React.ReactNode }) {
  if (!props.children) return null;
  return (
    <p className="field-error" id={props.id}>
      <span aria-hidden="true">⚠</span>
      <span>{props.children}</span>
    </p>
  );
}

/** Spread onto the input/select/textarea that FieldError describes. */
export function fieldErrorProps(id: string, error?: string | null) {
  return {
    "aria-invalid": error ? (true as const) : undefined,
    "aria-describedby": error ? id : undefined,
  };
}

/**
 * A determinate or indeterminate progress bar with a value screen readers can
 * read. The app previously drew progress with a styled div and nothing else
 * (WCAG 4.1.2). Omitting `value` reports an indeterminate run, which is the
 * honest answer for a walk that paginates to exhaustion.
 */
export function ProgressBar(props: {
  label: string;
  value?: number | null;
  max?: number;
  height?: number;
  tone?: "brand" | "good" | "warn";
}) {
  const { label, value, max = 100, height = 6, tone = "brand" } = props;
  const pct = value == null ? null : Math.max(0, Math.min(100, (value / max) * 100));
  const fill =
    tone === "good" ? "var(--fill-good)" : tone === "warn" ? "var(--fill-warn)" : "var(--fill-brand)";
  return (
    <div
      role="progressbar"
      aria-label={label}
      aria-valuenow={pct == null ? undefined : Math.round(pct)}
      aria-valuemin={pct == null ? undefined : 0}
      aria-valuemax={pct == null ? undefined : 100}
      aria-valuetext={pct == null ? "מתבצע — ההיקף הכולל אינו ידוע" : `${Math.round(pct)}%`}
      style={{
        height,
        background: "var(--surface-2)",
        borderRadius: 999,
        overflow: "hidden",
      }}
    >
      <div
        className={pct == null ? "queue-indeterminate" : undefined}
        style={{
          height: "100%",
          width: pct == null ? "100%" : `${pct}%`,
          background:
            pct == null
              ? `repeating-linear-gradient(90deg, ${fill} 0 8px, transparent 8px 16px)`
              : fill,
          transition: "width var(--motion-base)",
        }}
      />
    </div>
  );
}

/**
 * Where the reader is in the site (WCAG 2.4.8, Location). Deep routes —
 * /versions/:id, /organizations/:orgId, /sources/:sourceId — gave no clue what
 * they sat under.
 */
export function Breadcrumbs(props: {
  items: { label: string; to?: string }[];
}) {
  return (
    <nav className="breadcrumbs" aria-label="מיקום בעמוד">
      <ol>
        <li>
          <Link to="/">גרסאות לעם</Link>
        </li>
        {props.items.map((it, i) => {
          const last = i === props.items.length - 1;
          return (
            <li key={it.label + i}>
              {it.to && !last ? (
                <Link to={it.to}>{it.label}</Link>
              ) : (
                <span aria-current={last ? "page" : undefined}>{it.label}</span>
              )}
            </li>
          );
        })}
      </ol>
    </nav>
  );
}

/**
 * A plain-language opener for a dense page (WCAG 3.1.5, Reading Level).
 * The criterion asks for a simplified version where the prose reads above
 * lower-secondary level, which the SQL, API and methodology pages all do.
 */
export function PlainSummary(props: { children: React.ReactNode }) {
  return (
    <aside className="plain-summary">
      <h2>בקצרה</h2>
      <p>{props.children}</p>
      <p style={{ marginTop: "0.5rem", fontSize: "0.85rem" }}>
        <Link to="/about#glossary">מילון המונחים של האתר</Link>
      </p>
    </aside>
  );
}

/**
 * A tab strip that behaves like one (WCAG 4.1.2, 2.1.1).
 *
 * The app declared role="tablist"/"tab" but shipped none of the pattern: no
 * tabpanel, no aria-controls, no roving tabindex, no arrow keys — and in one
 * case a plain link parked inside the tablist, which is not a valid child.
 */
export function Tabs<T extends string>(props: {
  label: string;
  value: T;
  onChange: (v: T) => void;
  tabs: { id: T; label: React.ReactNode }[];
  /** Rendered beside the strip but OUTSIDE the tablist, where it belongs. */
  aside?: React.ReactNode;
  idPrefix?: string;
}) {
  const auto = useId();
  const prefix = props.idPrefix || auto;
  const refs = useRef<Record<string, HTMLButtonElement | null>>({});

  const move = (dir: 1 | -1) => {
    const i = props.tabs.findIndex((t) => t.id === props.value);
    const next = props.tabs[(i + dir + props.tabs.length) % props.tabs.length];
    props.onChange(next.id);
    requestAnimationFrame(() => refs.current[next.id]?.focus());
  };

  return (
    <div style={{ display: "flex", alignItems: "center", gap: "0.4rem", flexWrap: "wrap" }}>
      <div className="flex" role="tablist" aria-label={props.label} style={{ gap: "0.4rem", flexWrap: "wrap" }}>
        {props.tabs.map((t) => {
          const on = t.id === props.value;
          return (
            <button
              key={t.id}
              ref={(el) => {
                refs.current[t.id] = el;
              }}
              type="button"
              role="tab"
              id={`${prefix}-tab-${t.id}`}
              aria-selected={on}
              aria-controls={`${prefix}-panel-${t.id}`}
              tabIndex={on ? 0 : -1}
              className={on ? "btn-primary" : "btn-secondary"}
              onClick={() => props.onChange(t.id)}
              onKeyDown={(e) => {
                // RTL: ArrowLeft advances, because "next" is to the left.
                if (e.key === "ArrowLeft") { e.preventDefault(); move(1); }
                else if (e.key === "ArrowRight") { e.preventDefault(); move(-1); }
                else if (e.key === "Home") { e.preventDefault(); props.onChange(props.tabs[0].id); }
                else if (e.key === "End") { e.preventDefault(); props.onChange(props.tabs[props.tabs.length - 1].id); }
              }}
              style={{ fontSize: "0.85rem", padding: "0.35rem 0.8rem" }}
            >
              {t.label}
            </button>
          );
        })}
      </div>
      {props.aside}
    </div>
  );
}

/** The panel a Tabs strip controls. `idPrefix` must match. */
export function TabPanel(props: {
  id: string;
  idPrefix: string;
  children: React.ReactNode;
}) {
  return (
    <div
      role="tabpanel"
      id={`${props.idPrefix}-panel-${props.id}`}
      aria-labelledby={`${props.idPrefix}-tab-${props.id}`}
      tabIndex={0}
    >
      {props.children}
    </div>
  );
}

/**
 * Announce SPA route changes to a screen reader (WCAG 4.1.3, Status Messages).
 * Moving focus to <main> tells the user something happened but not what; this
 * says which page they landed on.
 */
export function RouteAnnouncer() {
  const location = useLocation();
  const [msg, setMsg] = useState("");
  useEffect(() => {
    // Wait a beat: the new page sets document.title in its own effect, and
    // announcing before that reads out the previous page's name.
    const id = window.setTimeout(() => setMsg(document.title), 250);
    return () => window.clearTimeout(id);
    // pathname only: a ?page= or ?q= change keeps the same title, and
    // re-reading it would talk over the region that DID change.
  }, [location.pathname]);
  return (
    <div className="sr-only" role="status" aria-live="polite" aria-atomic="true">
      {msg}
    </div>
  );
}
