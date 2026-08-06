import { useTranslation } from "react-i18next";
import { Link } from "react-router-dom";
import { ResolveMatch } from "../api/client";
import SourceChip from "../components/SourceChip";

/**
 * "This source is already tracked here" — the answer to a pasted source URL
 * that resolves to one or more existing datasets.
 *
 * Rendered only when the URL is AMBIGUOUS (several datasets are cut from the
 * same page, e.g. jeden.co.il's two corpora). A single unambiguous match never
 * reaches this component: the caller navigates straight to its versions page,
 * which is the whole point of the deep link.
 */
export default function ResolvedMatches({
  matches,
  sourceUrl,
}: {
  matches: ResolveMatch[];
  sourceUrl?: string;
}) {
  const { t } = useTranslation();
  if (matches.length === 0) return null;

  return (
    <section aria-labelledby="resolved-heading" className="mb-2">
      <h2
        id="resolved-heading"
        style={{ fontSize: "1.25rem", fontWeight: 700, marginBottom: "0.5rem" }}
      >
        {t("lookup.already_tracked_title")}
      </h2>
      <p className="text-sm text-muted mb-1">
        {t("lookup.already_tracked_hint", { count: matches.length })}
        {sourceUrl && (
          <>
            {" "}
            <span dir="ltr" style={{ wordBreak: "break-all", opacity: 0.8 }}>
              {sourceUrl}
            </span>
          </>
        )}
      </p>

      <div className="grid grid-2">
        {matches.map((m) => (
          <article key={m.id} className="card">
            <div className="flex-between mb-1">
              <h3 style={{ fontSize: "1rem", fontWeight: 600, margin: 0 }}>
                <Link to={`/versions/${m.id}`}>{m.title}</Link>
              </h3>
              <div className="flex" style={{ gap: "0.4rem", alignItems: "center" }}>
                <SourceChip
                  sourceType={m.source_type || undefined}
                  organization={m.organization || undefined}
                  ckanId={m.ckan_id || undefined}
                />
                <span className="badge badge-info">
                  {m.version_count} {t("home.versions_count")}
                </span>
              </div>
            </div>

            {m.source_url && (
              <p
                className="text-sm text-muted mb-1"
                dir="ltr"
                style={{ wordBreak: "break-all" }}
              >
                {m.source_url}
              </p>
            )}

            {m.status === "pending" && (
              <p className="text-sm mb-1" style={{ color: "var(--primary)" }}>
                {t("lookup.status_pending")}
              </p>
            )}

            <Link
              to={`/versions/${m.id}`}
              className="btn-primary"
              style={{ textDecoration: "none", fontSize: "0.85rem", padding: "0.35rem 0.85rem" }}
            >
              {t("lookup.open_versions")} ←
            </Link>
          </article>
        ))}
      </div>
    </section>
  );
}
