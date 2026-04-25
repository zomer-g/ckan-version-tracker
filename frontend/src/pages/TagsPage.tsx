import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { useTranslation } from "react-i18next";
import { tagsApi, admin as adminApi, type TagWithCount } from "../api/client";
import { useAuth } from "../auth/AuthContext";

export default function TagsPage() {
  const { t } = useTranslation();
  const { user } = useAuth();
  const [tags, setTags] = useState<TagWithCount[]>([]);
  const [loading, setLoading] = useState(true);
  const [query, setQuery] = useState("");

  useEffect(() => {
    tagsApi
      .list()
      .then(setTags)
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  const filtered = query.trim()
    ? tags.filter((tag) =>
        tag.name.toLowerCase().includes(query.trim().toLowerCase())
      )
    : tags;

  const handleDelete = async (tag: TagWithCount) => {
    if (!user?.is_admin) return;
    const ok = window.confirm(
      t("tags.delete_confirm", 'למחוק את התגית "{{name}}"? המאגרים יישארו, רק השיוך יוסר.', {
        name: tag.name,
      })
    );
    if (!ok) return;
    try {
      await adminApi.deleteTag(tag.id);
      setTags((prev) => prev.filter((t2) => t2.id !== tag.id));
    } catch (e: any) {
      alert(`${t("tags.delete_failed", "מחיקת התגית נכשלה")}: ${e?.message || e}`);
    }
  };

  return (
    <div className="container mt-3">
      <div className="page-header">
        <h1>{t("tags.page_title", "תגיות")}</h1>
        <p className="text-muted text-sm">
          {t("tags.page_subtitle", "מאגרים מקובצים לפי תחום")}
        </p>
      </div>

      <div style={{ marginBottom: "1rem" }}>
        <input
          type="search"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder={t("tags.search_placeholder", "חפש תגית...")}
          style={{
            width: "100%",
            padding: "0.6rem 0.9rem",
            fontSize: "1rem",
            border: "1px solid var(--border)",
            borderRadius: "var(--radius)",
            background: "var(--surface)",
          }}
        />
      </div>

      {loading ? (
        <div className="loading" role="status">
          {t("common.loading")}
        </div>
      ) : filtered.length === 0 ? (
        <div className="empty-state">
          {t("tags.no_tags", "עדיין לא נוצרו תגיות.")}
        </div>
      ) : (
        <div className="grid grid-2">
          {filtered.map((tag) => (
            <div
              key={tag.id}
              className="card"
              style={{
                display: "flex",
                gap: "0.75rem",
                alignItems: "center",
                position: "relative",
              }}
            >
              <Link
                to={`/tags/${tag.id}`}
                style={{
                  flex: 1,
                  minWidth: 0,
                  textDecoration: "none",
                  color: "inherit",
                  display: "flex",
                  gap: "0.75rem",
                  alignItems: "center",
                }}
              >
                <div
                  style={{
                    width: 48,
                    height: 48,
                    borderRadius: 8,
                    background: "var(--primary-50, #e0e7ff)",
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "center",
                    fontSize: "1.25rem",
                    color: "var(--primary)",
                    flexShrink: 0,
                  }}
                >
                  🏷
                </div>
                <div style={{ flex: 1, minWidth: 0 }}>
                  <div style={{ fontWeight: 600, fontSize: "1rem", marginBottom: "0.2rem" }}>
                    {tag.name}
                  </div>
                  <div className="text-sm text-muted">
                    {tag.dataset_count}{" "}
                    {t("tags.datasets_count", "מאגרים")}
                  </div>
                </div>
              </Link>
              {user?.is_admin && (
                <button
                  type="button"
                  onClick={() => handleDelete(tag)}
                  aria-label={`Delete ${tag.name}`}
                  style={{
                    background: "transparent",
                    border: "1px solid var(--border)",
                    borderRadius: 6,
                    padding: "0.25rem 0.45rem",
                    fontSize: "0.85rem",
                    cursor: "pointer",
                    color: "var(--danger, #dc2626)",
                  }}
                >
                  🗑
                </button>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
