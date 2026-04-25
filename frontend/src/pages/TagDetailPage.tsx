import { useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { useTranslation } from "react-i18next";
import { tagsApi, type TagDetail } from "../api/client";
import TagChips from "../components/TagChips";

export default function TagDetailPage() {
  const { t } = useTranslation();
  const { tagId } = useParams<{ tagId: string }>();
  const [tag, setTag] = useState<TagDetail | null>(null);
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
                <h3 style={{ fontSize: "1rem", fontWeight: 600, margin: 0 }}>
                  <Link to={`/versions/${d.id}`}>{d.title}</Link>
                </h3>
                <span
                  style={{
                    display: "inline-block",
                    padding: "0.15rem 0.45rem",
                    borderRadius: "9999px",
                    fontSize: "0.65rem",
                    fontWeight: 600,
                    background: d.source_type === "scraper" ? "#fef3c7" : "#ccfbf1",
                    color: d.source_type === "scraper" ? "#92400e" : "#0f766e",
                  }}
                >
                  {d.source_type === "scraper" ? "GOV.IL" : "DATA.GOV.IL"}
                </span>
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
              <div className="text-sm text-muted">
                {d.version_count} {t("home.versions_count")}
                {d.last_polled_at && (
                  <>
                    {" "}· {t("tracked.last_poll")}:{" "}
                    {new Date(d.last_polled_at).toLocaleDateString()}
                  </>
                )}
              </div>
              <TagChips tags={d.tags} excludeId={tag.id} />
              <div style={{ marginTop: "0.5rem" }}>
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
              </div>
            </article>
          ))}
        </div>
      )}
    </div>
  );
}
