/**
 * One tile on a property's card — the same shape as the dataset cards on the
 * home page: a title with a count badge, the source in a muted line, a short
 * summary, and the actions at the foot. A tile with more to show has an
 * "הרחבה" button; opening it stretches the tile across the whole row and shows
 * the full rows underneath, as a table.
 *
 * Every source and every information cube on the card is one of these, in one
 * grid, so the card reads as a set of equal parts rather than a stack of boxes
 * of different widths.
 */
import type { ReactNode } from "react";

export function Ltr({ children }: { children: ReactNode }) {
  return <span style={{ unicodeBidi: "isolate", direction: "ltr", display: "inline-block" }}>{children}</span>;
}

export interface TileLink {
  href: string;
  label: string;
  title?: string;
  /** An in-site link (no new window). */
  internal?: boolean;
}

export default function NadlanTile({
  title, badge, badgeTone = "info", label, children, open = false, onToggle,
  expandLabel = "הרחבה", detail, links = [], muted = false,
}: {
  title: string;
  badge?: ReactNode;
  badgeTone?: "info" | "muted";
  /** The source or group, in the muted line under the title. */
  label?: ReactNode;
  /** The summary: two or three short lines. */
  children?: ReactNode;
  open?: boolean;
  /** Absent: the tile has nothing more to show and gets no button. */
  onToggle?: () => void;
  expandLabel?: string;
  /** The full rows, rendered only while open. */
  detail?: ReactNode;
  links?: TileLink[];
  /** Nothing recorded here (a cube with no hits). */
  muted?: boolean;
}) {
  return (
    <article className={`card nadlan-tile${open ? " is-open" : ""}${muted ? " is-muted" : ""}`}>
      <div className="flex-between nadlan-tile-head">
        <h4 className="nadlan-tile-title">{title}</h4>
        {badge != null && (
          <span className={`badge ${badgeTone === "info" ? "badge-info" : "nadlan-badge-muted"}`}>
            {badge}
          </span>
        )}
      </div>
      {label && <p className="text-sm text-muted nadlan-tile-label">{label}</p>}
      {children && <div className="text-sm nadlan-tile-body">{children}</div>}

      {(onToggle || links.length > 0) && (
        <div className="flex nadlan-tile-foot">
          {onToggle && (
            <button type="button" className="btn-primary nadlan-tile-btn" onClick={onToggle}
                    aria-expanded={open} aria-label={`${open ? "סגירה" : expandLabel} — ${title}`}>
              {open ? "סגירה" : expandLabel}
            </button>
          )}
          {links.map((l) => l.internal ? (
            <a key={l.href} href={l.href} className="text-sm card-source-link nadlan-tile-link"
               title={l.title}>
              {l.label}
            </a>
          ) : (
            <a key={l.href} href={l.href} target="_blank" rel="noopener noreferrer"
               className="text-sm card-source-link nadlan-tile-link" title={l.title}>
              {l.label}<span aria-hidden="true"> &#8599;</span>
              <span className="sr-only"> (נפתח בחלון חדש)</span>
            </a>
          ))}
        </div>
      )}

      {open && detail && <div className="nadlan-tile-detail">{detail}</div>}
    </article>
  );
}

/** A plain data table for a tile's detail: header row, zebra-free, scrolls
 *  sideways on a phone instead of squeezing the columns. */
export function TileTable({ label, columns, rows }: {
  label: string;
  columns: { key: string; label: ReactNode; title?: string }[];
  rows: ReactNode[][];
}) {
  return (
    <div tabIndex={0} role="region" aria-label={label} className="scroll-region nadlan-table-wrap">
      <table className="nadlan-table">
        <thead>
          <tr>
            {columns.map((c) => (
              <th key={c.key} scope="col" title={c.title}>{c.label}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={i}>
              {r.map((v, j) => <td key={j}>{v ?? "—"}</td>)}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
