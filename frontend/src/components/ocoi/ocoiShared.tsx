/**
 * Shared presentation bits for ניגוד עניינים לעם (OCOI).
 *
 * One colour per entity kind, used by every surface — search hits, entity
 * cards, the graph. Keeping it here (rather than per-component) is what makes
 * a person the same green in a result list and in the graph, which is the
 * whole reason the colour carries meaning.
 */
import { OcoiEntityType, OCOI_TYPE_LABELS } from "../../api/client";

export const TYPE_COLORS: Record<OcoiEntityType, string> = {
  person: "var(--fill-teal)",       // teal — the officials
  company: "var(--fill-warn)",      // amber — commercial entities
  association: "var(--fill-violet)", // violet — עמותות
  domain: "var(--fill-sky)",        // blue — subject areas
};

export const TYPE_ICONS: Record<OcoiEntityType, string> = {
  person: "👤",
  company: "🏢",
  association: "🤝",
  domain: "🏷️",
};

export function TypeChip({ type, small }: { type: OcoiEntityType; small?: boolean }) {
  return (
    <span
      style={{
        display: "inline-block",
        padding: small ? "0.05rem 0.4rem" : "0.15rem 0.55rem",
        borderRadius: 999,
        fontSize: small ? "0.7rem" : "0.75rem",
        fontWeight: 600,
        color: "var(--on-fill)",
        background: TYPE_COLORS[type],
        whiteSpace: "nowrap",
      }}
    >
      {TYPE_ICONS[type]} {OCOI_TYPE_LABELS[type]}
    </span>
  );
}

/** "10,000+" when the server capped the count, an exact figure otherwise. */
export function formatTotal(total: number, capped?: boolean): string {
  return capped ? `${total.toLocaleString()}+` : total.toLocaleString();
}

export function Empty({ children }: { children: React.ReactNode }) {
  return (
    <div className="text-muted" style={{ padding: "1.5rem 0", textAlign: "center" }}>
      {children}
    </div>
  );
}

export function Spinner({ label = "טוען…" }: { label?: string }) {
  return (
    <div className="text-muted" style={{ padding: "1.5rem 0", textAlign: "center" }}>
      {label}
    </div>
  );
}

export function ErrorNote({ error }: { error: string }) {
  return (
    <div
      role="alert"
      style={{
        padding: "0.7rem 0.9rem",
        borderRadius: 8,
        background: "var(--tint-bad-bg)",
        color: "var(--danger, #992C2C)",
        fontSize: "0.9rem",
        margin: "0.75rem 0",
      }}
    >
      {error}
    </div>
  );
}

/** Shared pager. Kept dumb — every tab owns its own page state. */
export function Pager({
  page,
  pages,
  onPage,
}: {
  page: number;
  pages: number;
  onPage: (p: number) => void;
}) {
  if (pages <= 1) return null;
  const btn = (label: string, target: number, disabled: boolean) => (
    <button
      type="button"
      className="btn btn-sm"
      disabled={disabled}
      onClick={() => onPage(target)}
      style={{ opacity: disabled ? 0.45 : 1 }}
    >
      {label}
    </button>
  );
  return (
    <div className="flex" style={{ gap: "0.5rem", alignItems: "center", justifyContent: "center", marginTop: "1rem" }}>
      {btn("→ הקודם", page - 1, page <= 1)}
      <span className="text-sm text-muted">
        עמוד {page.toLocaleString()} מתוך {pages.toLocaleString()}
      </span>
      {btn("הבא ←", page + 1, page >= pages)}
    </div>
  );
}
