/**
 * "The latest version may not have imported faithfully."
 *
 * Distinct from SourceGoneNotice, and the distinction is the whole point:
 * a removed source means no NEW data is coming, while this means the data that
 * IS here may be wrong. A reader who downloads without seeing this gets a file
 * that looks complete and is not.
 *
 * The case that produced it: קווי גובה 50 ס"מ published 93,866 features against
 * 93,866 declared by the source — a perfect completeness score — having turned
 * 93,436 contour LINES into points. Every count agreed. The layer was gone.
 *
 * Deliberately louder than the archive chips and quieter than an error: the
 * archive is still worth having, and the reader is the one who can judge
 * whether the defect matters for their use. So it states what is suspected and
 * why, and never hides the download.
 */

function formatDate(iso: string | null | undefined): string | null {
  if (!iso) return null;
  const d = new Date(iso);
  if (isNaN(d.getTime())) return null;
  const p = (n: number) => String(n).padStart(2, "0");
  return `${p(d.getDate())}.${p(d.getMonth() + 1)}.${d.getFullYear()}`;
}

export default function ImportWarningNotice({
  warning,
  at,
  variant = "banner",
}: {
  warning: string | null | undefined;
  at?: string | null;
  variant?: "banner" | "chip";
}) {
  if (!warning) return null;
  const since = formatDate(at);

  if (variant === "chip") {
    return (
      <span
        title={warning}
        style={{
          display: "inline-flex",
          alignItems: "center",
          gap: "0.25rem",
          fontSize: "0.7rem",
          fontWeight: 600,
          padding: "0.1rem 0.45rem",
          borderRadius: "999px",
          background: "var(--tint-warn-bg)",
          color: "var(--warning)",
          whiteSpace: "nowrap",
        }}
      >
        <span aria-hidden="true">⚠</span> חשש לייבוא פגום
      </span>
    );
  }

  return (
    <div
      role="alert"
      style={{
        marginTop: "0.6rem",
        padding: "0.6rem 0.85rem",
        borderRadius: 8,
        background: "var(--tint-warn-bg)",
        border: "1px solid var(--tint-warn-bd)",
        color: "var(--warning)",
        fontSize: "0.85rem",
        lineHeight: 1.6,
        maxWidth: "46rem",
      }}
    >
      <strong style={{ display: "block", marginBottom: "0.15rem" }}>
        <span aria-hidden="true">⚠</span> יש חשש שהגרסה האחרונה לא יובאה כראוי{since ? ` — נבדק ב-${since}` : ""}
      </strong>
      {warning}
      <div style={{ marginTop: "0.35rem", opacity: 0.9 }}>
        הקבצים נשארים זמינים להורדה — כדאי לבדוק אותם לפני הסתמכות, ולהשוות
        לגרסה קודמת אם יש.
      </div>
    </div>
  );
}
