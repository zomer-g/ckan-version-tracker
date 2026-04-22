import { useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { useTranslation } from "react-i18next";
import { organizations as orgsApi, OrganizationDetail } from "../api/client";

export default function OrganizationDetailPage() {
  const { t } = useTranslation();
  const { orgId } = useParams<{ orgId: string }>();
  const [org, setOrg] = useState<OrganizationDetail | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    if (!orgId) return;
    setLoading(true);
    orgsApi.get(orgId)
      .then(setOrg)
      .catch((e) => setError(e.message || String(e)))
      .finally(() => setLoading(false));
  }, [orgId]);

  if (loading) return <div className="container mt-3"><div className="loading" role="status">{t("common.loading")}</div></div>;
  if (error || !org) {
    return (
      <div className="container mt-3">
        <div className="empty-state" role="alert">{error || t("organizations.not_found", "ארגון לא נמצא")}</div>
        <div style={{ marginTop: "1rem" }}>
          <Link to="/organizations" className="btn-secondary">← {t("organizations.back", "חזרה לרשימה")}</Link>
        </div>
      </div>
    );
  }

  return (
    <div className="container mt-3">
      <div style={{ marginBottom: "0.75rem", fontSize: "0.85rem" }}>
        <Link to="/organizations" className="text-muted" style={{ textDecoration: "none" }}>
          {t("organizations.back", "חזרה לרשימה")}
        </Link>
        {org.parent && (
          <>
            {" / "}
            <Link to={`/organizations/${org.parent.id}`} style={{ color: "var(--primary)", textDecoration: "none" }}>
              {org.parent.title}
            </Link>
          </>
        )}
      </div>

      <div style={{
        display: "flex",
        gap: "1rem",
        alignItems: "flex-start",
        marginBottom: "1.5rem",
        padding: "1rem",
        background: "var(--surface)",
        borderRadius: "var(--radius)",
        boxShadow: "var(--shadow-sm)",
        border: "1px solid var(--border)",
      }}>
        {(org.image_url || org.gov_il_logo_url) && (
          <img
            src={(org.image_url || org.gov_il_logo_url) as string}
            alt=""
            style={{
              width: 80,
              height: 80,
              borderRadius: 8,
              objectFit: "contain",
              background: "#fff",
              border: "1px solid var(--border)",
              flexShrink: 0,
            }}
            onError={(e) => { (e.target as HTMLImageElement).style.visibility = "hidden"; }}
          />
        )}
        <div style={{ flex: 1, minWidth: 0 }}>
          <h1 style={{ margin: "0 0 0.25rem 0", fontSize: "1.5rem" }}>{org.title}</h1>
          <div className="text-sm text-muted" style={{ marginBottom: "0.5rem" }}>
            {org.dataset_count} {t("organizations.datasets_count", "מאגרים במעקב")}
          </div>
          {org.description && (
            <p className="text-sm" style={{ margin: 0, whiteSpace: "pre-wrap", marginBottom: "0.5rem" }}>{org.description}</p>
          )}
          <div className="flex" style={{ gap: "0.75rem", fontSize: "0.85rem", flexWrap: "wrap" }}>
            {org.gov_il_url_name && (
              <a
                href={`https://www.gov.il/he/departments/${org.gov_il_url_name}`}
                target="_blank"
                rel="noopener noreferrer"
                style={{ color: "var(--primary)", textDecoration: "none" }}
              >
                gov.il &#8599;
              </a>
            )}
            {org.data_gov_il_slug && (
              <a
                href={`https://data.gov.il/he/organizations/${org.data_gov_il_slug}`}
                target="_blank"
                rel="noopener noreferrer"
                style={{ color: "var(--primary)", textDecoration: "none" }}
              >
                data.gov.il &#8599;
              </a>
            )}
            {org.external_website && (
              <a
                href={org.external_website}
                target="_blank"
                rel="noopener noreferrer"
                style={{ color: "var(--text-muted)", textDecoration: "none" }}
              >
                {t("organizations.external_website", "אתר רשמי")} &#8599;
              </a>
            )}
          </div>
        </div>
      </div>

      {org.children.length > 0 && (
        <section style={{ marginBottom: "1.5rem" }}>
          <h2 style={{ fontSize: "1.1rem", fontWeight: 700, marginBottom: "0.75rem" }}>
            {t("organizations.children_heading", "יחידות תחת")} {org.title} ({org.children.length})
          </h2>
          <div className="grid grid-2">
            {org.children.map((c) => (
              <Link
                key={c.id}
                to={`/organizations/${c.id}`}
                className="card"
                style={{
                  textDecoration: "none",
                  color: "inherit",
                  display: "flex",
                  gap: "0.6rem",
                  alignItems: "center",
                }}
              >
                {c.gov_il_logo_url ? (
                  <img
                    src={c.gov_il_logo_url}
                    alt=""
                    style={{
                      width: 40, height: 40, borderRadius: 6, objectFit: "contain",
                      background: "#fff", border: "1px solid var(--border)", flexShrink: 0,
                    }}
                    onError={(e) => { (e.target as HTMLImageElement).style.visibility = "hidden"; }}
                  />
                ) : (
                  <div style={{
                    width: 40, height: 40, borderRadius: 6,
                    background: "var(--primary-50, #e0e7ff)",
                    display: "flex", alignItems: "center", justifyContent: "center",
                    fontSize: "1.1rem", color: "var(--primary)", flexShrink: 0,
                  }}>🏛</div>
                )}
                <div style={{ flex: 1, minWidth: 0 }}>
                  <div style={{ fontWeight: 600, fontSize: "0.95rem" }}>{c.title}</div>
                  <div className="text-sm text-muted">
                    {c.dataset_count} {t("organizations.datasets_count", "מאגרים במעקב")}
                  </div>
                </div>
              </Link>
            ))}
          </div>
        </section>
      )}

      <h2 style={{ fontSize: "1.1rem", fontWeight: 700, marginBottom: "0.75rem" }}>
        {t("organizations.datasets_heading", "מאגרים")}
      </h2>

      {org.datasets.length === 0 ? (
        <div className="empty-state">{t("organizations.no_datasets", "אין מאגרים במעקב עבור ארגון זה.")}</div>
      ) : (
        <div className="grid grid-2">
          {org.datasets.map((d) => (
            <article key={d.id} className="card">
              <div className="flex-between mb-1">
                <h3 style={{ fontSize: "1rem", fontWeight: 600, margin: 0 }}>
                  <Link to={`/versions/${d.id}`}>{d.title}</Link>
                </h3>
                <span style={{
                  display: "inline-block",
                  padding: "0.15rem 0.45rem",
                  borderRadius: "9999px",
                  fontSize: "0.65rem",
                  fontWeight: 600,
                  background: d.source_type === "scraper" ? "#fef3c7" : "#ccfbf1",
                  color: d.source_type === "scraper" ? "#92400e" : "#0f766e",
                }}>
                  {d.source_type === "scraper" ? "GOV.IL" : "DATA.GOV.IL"}
                </span>
              </div>
              <div className="text-sm text-muted">
                {d.version_count} {t("home.versions_count")}
                {d.last_polled_at && (
                  <> · {t("tracked.last_poll")}: {new Date(d.last_polled_at).toLocaleDateString()}</>
                )}
              </div>
              <div style={{ marginTop: "0.5rem" }}>
                <Link
                  to={`/versions/${d.id}`}
                  className="btn-primary"
                  style={{ textDecoration: "none", fontSize: "0.85rem", padding: "0.3rem 0.75rem" }}
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
