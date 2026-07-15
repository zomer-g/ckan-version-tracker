/**
 * One source's datasets — the drill-down from SourcesPage. Filters the
 * tracked-dataset list to those whose sourceBadgeFor().id matches the URL
 * slug, and renders them with the same card style as the organization page.
 */
import { useEffect, useMemo, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { useTranslation } from "react-i18next";
import { publicApi, TrackedDataset } from "../api/client";
import { sourceBadgeFor } from "../utils/sourceBadge";
import TagChips from "../components/TagChips";
import SourceChip from "../components/SourceChip";
import AdminDatasetActions from "../components/AdminDatasetActions";

export default function SourceDetailPage() {
  const { t } = useTranslation();
  const { sourceId } = useParams<{ sourceId: string }>();
  const [datasets, setDatasets] = useState<TrackedDataset[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    publicApi
      .datasets()
      .then(setDatasets)
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  // Datasets belonging to this source + the source's badge (for the header /
  // colours). Both derive from the same sourceBadgeFor call per dataset.
  const { mine, badge } = useMemo(() => {
    const list = datasets
      .map((d) => ({ d, b: sourceBadgeFor(d.source_type, d.organization, d.ckan_id) }))
      .filter((x) => x.b.id === sourceId);
    return {
      mine: list.map((x) => x.d),
      badge: list[0]?.b ?? null,
    };
  }, [datasets, sourceId]);

  if (loading) {
    return (
      <div className="container mt-3">
        <div className="loading" role="status">{t("common.loading")}</div>
      </div>
    );
  }

  return (
    <div className="container mt-3">
      <div style={{ marginBottom: "0.75rem", fontSize: "0.85rem" }}>
        <Link to="/sources" className="text-muted" style={{ textDecoration: "none" }}>
          ← {t("sources.back", "חזרה למקורות")}
        </Link>
      </div>

      <div className="page-header" style={{ display: "flex", alignItems: "center", gap: "0.75rem" }}>
        {badge && (
          <span
            style={{
              display: "inline-flex",
              alignItems: "center",
              justifyContent: "center",
              padding: "0.3rem 0.7rem",
              borderRadius: 8,
              background: badge.bg,
              color: badge.fg,
              fontWeight: 700,
              fontSize: "0.9rem",
            }}
          >
            {badge.label}
          </span>
        )}
        <h1 style={{ margin: 0 }}>{badge?.label ?? sourceId}</h1>
      </div>
      <p className="text-muted text-sm" style={{ marginBottom: "1rem" }}>
        {mine.length} {t("sources.datasets_count", "מאגרים במעקב")}
      </p>

      {mine.length === 0 ? (
        <div className="empty-state">{t("sources.no_datasets", "אין מאגרים למקור זה.")}</div>
      ) : (
        <div className="grid grid-2">
          {mine.map((d) => {
            return (
              <article key={d.id} className="card">
                <div className="flex-between mb-1">
                  <h3 style={{ fontSize: "1rem", fontWeight: 600, margin: 0 }}>
                    <Link to={`/versions/${d.id}`}>{d.title}</Link>
                  </h3>
                  <SourceChip
                    sourceType={d.source_type}
                    organization={d.organization}
                    ckanId={d.ckan_id}
                  />
                </div>
                <div className="text-sm text-muted">
                  {d.version_count} {t("home.versions_count")}
                  {d.organization_title && (
                    <>
                      {" · "}
                      {d.organization_id ? (
                        <Link
                          to={`/organizations/${d.organization_id}`}
                          style={{ color: "var(--text-muted)", textDecoration: "none" }}
                        >
                          {d.organization_title}
                        </Link>
                      ) : (
                        d.organization_title
                      )}
                    </>
                  )}
                </div>
                <TagChips tags={d.tags} />
                <div
                  className="flex"
                  style={{ marginTop: "0.5rem", gap: "0.4rem", flexWrap: "wrap", alignItems: "center" }}
                >
                  <Link
                    to={`/versions/${d.id}`}
                    className="btn-primary"
                    style={{ textDecoration: "none", fontSize: "0.85rem", padding: "0.3rem 0.75rem" }}
                  >
                    {t("tracked.versions")}
                  </Link>
                  <AdminDatasetActions
                    datasetId={d.id}
                    title={d.title}
                    onDeleted={(id) =>
                      setDatasets((prev) => prev.filter((x) => x.id !== id))
                    }
                  />
                </div>
              </article>
            );
          })}
        </div>
      )}
    </div>
  );
}
