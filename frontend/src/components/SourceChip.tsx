/**
 * Clickable source-affiliation chip. Renders the per-dataset source badge
 * (GOV.IL / IDF.IL / כנסת / …) as a Link to that source's page (/sources/:id),
 * so a dataset can be navigated back to the collection of datasets from the
 * same source. Mirrors TagChips (clickable tags → /tags/:id) and the org link.
 *
 * The palette + stable source id both come from sourceBadgeFor — the single
 * source of truth in utils/sourceBadge.ts — so a new source added there shows
 * up here (and links correctly) automatically.
 */
import { Link } from "react-router-dom";
import { sourceBadgeFor } from "../utils/sourceBadge";

export default function SourceChip({
  sourceType,
  organization,
  ckanId,
  size = "sm",
}: {
  sourceType: string | null | undefined;
  organization?: string | null;
  ckanId?: string | null;
  /** "sm" = the inline card pill; "md" = the larger page-header pill. */
  size?: "sm" | "md";
}) {
  const badge = sourceBadgeFor(sourceType, organization, ckanId);
  const pad = size === "md" ? "0.3rem 0.7rem" : "0.15rem 0.45rem";
  const fontSize = size === "md" ? "0.8rem" : "0.65rem";
  return (
    <Link
      to={`/sources/${badge.id}`}
      title={`כל המאגרים מ־${badge.label}`}
      // A standalone chip, not a link inside a sentence, so WCAG 2.5.5 gives it
      // no exemption — .source-chip carries the 44px minimum. The pill still
      // LOOKS small: the extra size is transparent padding around it.
      className="source-chip"
      style={{
        display: "inline-flex",
        alignItems: "center",
        justifyContent: "center",
        padding: pad,
        borderRadius: "9999px",
        fontSize,
        fontWeight: size === "md" ? 700 : 600,
        background: badge.bg,
        color: badge.fg,
        textDecoration: "none",
        lineHeight: 1.4,
      }}
    >
      {badge.label}
    </Link>
  );
}
