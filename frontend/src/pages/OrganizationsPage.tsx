import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { useTranslation } from "react-i18next";
import { organizations as orgsApi, Organization } from "../api/client";

export default function OrganizationsPage() {
  const { t } = useTranslation();
  const [orgs, setOrgs] = useState<Organization[]>([]);
  const [loading, setLoading] = useState(true);
  const [query, setQuery] = useState("");

  useEffect(() => {
    orgsApi.list()
      .then(setOrgs)
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  const filtered = query.trim()
    ? orgs.filter((o) =>
        o.title.toLowerCase().includes(query.toLowerCase()) ||
        o.name.toLowerCase().includes(query.toLowerCase())
      )
    : orgs;

  const withDatasets = filtered.filter((o) => o.dataset_count > 0);
  const withoutDatasets = filtered.filter((o) => o.dataset_count === 0);

  return (
    <div className="container mt-3">
      <div className="page-header">
        <h1>{t("organizations.title", "ארגונים")}</h1>
        <p className="text-muted text-sm">
          {t("organizations.subtitle", "רשימת הארגונים שמפרסמים מאגרי מידע ממשלתיים")}
        </p>
      </div>

      <div style={{ marginBottom: "1rem" }}>
        <input
          type="search"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder={t("organizations.search_placeholder", "חפש ארגון...")}
          aria-label={t("organizations.search_placeholder", "חפש ארגון")}
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
        <div className="loading" role="status">{t("common.loading")}</div>
      ) : filtered.length === 0 ? (
        <div className="empty-state">
          {t("organizations.empty", "אין ארגונים. מנהל המערכת יכול לסנכרן מ-data.gov.il.")}
        </div>
      ) : (
        <>
          {withDatasets.length > 0 && (
            <div className="grid grid-2" style={{ marginBottom: "1.5rem" }}>
              {withDatasets.map((o) => <OrgCard key={o.id} org={o} />)}
            </div>
          )}
          {withoutDatasets.length > 0 && (
            <>
              <h2 style={{ fontSize: "1rem", fontWeight: 600, marginTop: "1.5rem", marginBottom: "0.75rem", color: "var(--text-muted)" }}>
                {t("organizations.no_datasets_heading", "ארגונים ללא מאגרים במעקב")}
              </h2>
              <div className="grid grid-2">
                {withoutDatasets.map((o) => <OrgCard key={o.id} org={o} muted />)}
              </div>
            </>
          )}
        </>
      )}
    </div>
  );
}

function OrgCard({ org, muted = false }: { org: Organization; muted?: boolean }) {
  const { t } = useTranslation();
  return (
    <Link
      to={`/organizations/${org.id}`}
      className="card"
      style={{
        textDecoration: "none",
        color: "inherit",
        display: "flex",
        gap: "0.75rem",
        alignItems: "center",
        opacity: muted ? 0.65 : 1,
      }}
    >
      {(org.image_url || org.gov_il_logo_url) ? (
        <img
          src={(org.image_url || org.gov_il_logo_url) as string}
          alt=""
          style={{
            width: 56,
            height: 56,
            borderRadius: 8,
            objectFit: "contain",
            background: "#fff",
            border: "1px solid var(--border)",
            flexShrink: 0,
          }}
          onError={(e) => { (e.target as HTMLImageElement).style.visibility = "hidden"; }}
        />
      ) : (
        <div style={{
          width: 56,
          height: 56,
          borderRadius: 8,
          background: "var(--primary-50, #e0e7ff)",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          fontSize: "1.5rem",
          color: "var(--primary)",
          flexShrink: 0,
        }}>
          🏛
        </div>
      )}
      <div style={{ flex: 1, minWidth: 0 }}>
        <div style={{ fontWeight: 600, fontSize: "1rem", marginBottom: "0.2rem" }}>
          {org.title}
        </div>
        <div className="text-sm text-muted">
          {org.dataset_count} {t("organizations.datasets_count", "מאגרים במעקב")}
        </div>
      </div>
    </Link>
  );
}
