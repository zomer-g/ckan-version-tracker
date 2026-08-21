import { useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { useTranslation } from "react-i18next";
import { tagsApi, type TagDetail } from "../api/client";
import TagChips from "../components/TagChips";
import SourceChip from "../components/SourceChip";
import AdminDatasetActions from "../components/AdminDatasetActions";
import { fmtDateHe } from "../utils/dates";

import { useDocumentTitle } from "../hooks/useDocumentTitle";
import { Breadcrumbs } from "../components/a11y";
export default function TagDetailPage() {
  const { t } = useTranslation();
  const { tagId } = useParams<{ tagId: string }>();
  const [tag, setTag] = useState<TagDetail | null>(null);
  useDocumentTitle(tag?.name || "תגית");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    if (!tagId) return;
    setLoading(true);
    tagsApi
      .get(tagId)
      .then(setTag)
      .catch((e) => setError(e.message || String(e)))
      .finally(() => setLoading(false));
  }, [tagId]);

  if (loading) {
    return (
      <div className="container mt-3">
        <div className="loading" role="status">
          {t("common.loading")}
        </div>
      </div>
    );
  }
  if (error || !tag) {
    return (
      <div className="container mt-3">
        <div className="empty-state" role="alert">
          {error || t("tags.tag_not_found", "תגית לא נמצאה")}
        </div>
        <div style={{ marginTop: "1rem" }}>
          <Link to="/tags" className="btn-secondary">
            ← {t("tags.back_to_tags", "חזרה לכל התגיות")}
          </Link>
        </div>
      </div>
    );
  }

  return (
    <div className="container mt-3">
      <div style={{ marginBottom: "0.75rem", fontSize: "0.85rem" }}>
        <Link to="/tags" className="text-muted" style={{ textDecoration: "none" }}>
          ← {t("tags.back_to_tags", "חזרה לכל התגיות")}
        </Link>
      </div>

      <div
        style={{
          display: "flex",
          gap: "1rem",
          alignItems: "center",
          marginBottom: "1.5rem",
          padding: "1rem",
          background: "var(--surface)",
          borderRadius: "var(--radius)",
          boxShadow: "var(--shadow-sm)",
          border: "1px solid var(--border)",
        }}
      >
        <div
          style={{
            width: 64,
            height: 64,
            borderRadius: 8,
            background: "var(--primary-50, #e0e7ff)",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            fontSize: "1.6rem",
            color: "var(--primary)",
            flexShrink: 0,
          }}
        >
          🏷
        </div>
        <div style={{ flex: 1, minWidth: 0 }}>
          <Breadcrumbs items={[{ label: t("nav.tags", "תגיות"), to: "/tags" }, { label: tag.name }]} />
          <h1 style={{ margin: "0 0 0.25rem 0", fontSize: "1.5rem" }}>{tag.name}</h1>
          <div className="text-sm text-muted">
            {tag.dataset_count} {t("tags.datasets_count", "מאגרים")}
          </div>
          {tag.description && (
            <p className="text-sm" style={{ margin: "0.5rem 0 0 0", whiteSpace: "pre-wrap" }}>
              {tag.description}
            </p>
          )}
        </div>
      </div>

      {tag.datasets.length === 0 ? (
        <div className="empty-state">
          {t("organizations.no_datasets", "אין מאגרים תחת תגית זו.")}
        </div>
      ) : (
        <div className="grid grid-2">
          {tag.datasets.map((d) => (
            <article key={d.id} className="card">
              <div className="flex-between mb-1">
                <h2 style={{ fontSize: "1rem", fontWeight: 600, margin: 0 }}>
                  <Link to={`/versions/${d.id}`}>{d.title}</Link>
                </h2>
                {/* ckanId is not optional decoration: its prefix is what tells
                    the scraper sources apart. Omitting it collapsed every
                    non-CKAN dataset on this page into one "GOV.IL" chip. */}
                <SourceChip
                  sourceType={d.source_type}
                  organization={d.organization}
                  ckanId={d.ckan_id}
                />
              </div>
              {d.organization_title && (
                <div className="text-sm text-muted" style={{ marginBottom: "0.25rem" }}>
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
                </div>
              )}
              {/* See OrganizationDetailPage: last_modified is the data's own
                  timestamp, last_polled_at is ours. */}
              <div className="text-sm text-muted">
                {d.version_count} {t("home.versions_count")}
                {d.last_modified && (
                  <>
                    {" "}· {t("tracked.last_modified")}: {fmtDateHe(d.last_modified)}
                  </>
                )}
                {d.last_polled_at && (
                  <>
                    {" "}· {t("tracked.last_poll")}: {fmtDateHe(d.last_polled_at)}
                  </>
                )}
              </div>
              <TagChips tags={d.tags} excludeId={tag.id} />
              <div className="flex" style={{ marginTop: "0.5rem", gap: "0.4rem", flexWrap: "wrap", alignItems: "center" }}>
                <Link
                  to={`/versions/${d.id}`}
                  className="btn-primary"
                  style={{
                    textDecoration: "none",
                    fontSize: "0.85rem",
                    padding: "0.3rem 0.75rem",
                  }}
                >
                  {t("tracked.versions")}
                </Link>
                <AdminDatasetActions
                  datasetId={d.id}
                  title={d.title}
                  onDeleted={(id) =>
                    setTag((prev) =>
                      prev
                        ? { ...prev, datasets: prev.datasets.filter((x) => x.id !== id) }
                        : prev
                    )
                  }
                />
              </div>
            </article>
          ))}
        </div>
      )}
    </div>
  );
}
