/**
 * "The publisher removed this source."
 *
 * Set server-side (TrackedDataset.source_gone_at) only on a verdict the scraper
 * reached with certainty — for GovMap, the catalog was fetched successfully and
 * the layer id was absent from it. A catalog timeout, or a layer that IS listed,
 * produces a transient error instead and never lands here. So this is a
 * statement of fact, not a guess from a failed poll, and it is worded that way.
 *
 * Why it is a notice and not a "removed" status: a source that no longer exists
 * is the case where the archive matters MOST — what is on this page may be the
 * last public copy. So the dataset stays listed, readable and downloadable, and
 * the only thing that changes is that the page says outright why no new versions
 * are appearing. Without it the dataset just looks neglected.
 *
 *   • `variant="banner"` — the dataset page. Explains the consequence.
 *   • `variant="chip"`   — dense lists (admin cards), where it must be
 *     scannable next to a dozen other chips.
 */

function formatDate(iso: string | null | undefined): string | null {
  if (!iso) return null;
  const d = new Date(iso);
  if (isNaN(d.getTime())) return null;
  const p = (n: number) => String(n).padStart(2, "0");
  return `${p(d.getDate())}.${p(d.getMonth() + 1)}.${d.getFullYear()}`;
}

export default function SourceGoneNotice({
  goneAt,
  variant = "banner",
}: {
  goneAt: string | null | undefined;
  variant?: "banner" | "chip";
}) {
  if (!goneAt) return null;
  const since = formatDate(goneAt);

  if (variant === "chip") {
    return (
      <span
        title={
          since
            ? `המקור הוסר מאתר המפרסם — זוהה ב-${since}. הארכיון כאן נשמר.`
            : "המקור הוסר מאתר המפרסם. הארכיון כאן נשמר."
        }
        style={{
          display: "inline-flex",
          alignItems: "center",
          gap: "0.25rem",
          fontSize: "0.7rem",
          fontWeight: 600,
          padding: "0.1rem 0.45rem",
          borderRadius: "999px",
          background: "#fee2e2",
          color: "#991b1b",
          whiteSpace: "nowrap",
        }}
      >
        ⃠ הוסר מהמקור
      </span>
    );
  }

  return (
    <div
      role="status"
      style={{
        marginTop: "0.6rem",
        padding: "0.6rem 0.85rem",
        borderRadius: 8,
        background: "#fef2f2",
        border: "1px solid #fecaca",
        color: "#7f1d1d",
        fontSize: "0.85rem",
        lineHeight: 1.6,
        maxWidth: "46rem",
      }}
    >
      <strong style={{ display: "block", marginBottom: "0.15rem" }}>
        ⃠ המקור הוסר מאתר המפרסם{since ? ` — זוהה ב-${since}` : ""}
      </strong>
      המאגר אינו קיים יותר אצל המפרסם, ולכן לא ייווצרו לו גרסאות חדשות.
      הגרסאות שכבר נשמרו כאן נותרות זמינות להורדה ולתשאול — וייתכן שהן העותק
      הציבורי האחרון של הנתונים האלה.
    </div>
  );
}
