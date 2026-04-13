import { useState, useEffect, FormEvent } from "react";
import { useTranslation } from "react-i18next";
import { Link } from "react-router-dom";
import { ckan, publicApi, TrackedDataset } from "../api/client";
import RequestForm from "../components/RequestForm";

const ODATA_BASE = "https://www.odata.org.il";

interface CkanResource {
  id: string;
  name: string;
  description?: string;
  format?: string;
  url?: string;
}

interface SearchResult {
  id: string;
  name: string;
  title: string;
  notes: string;
  organization?: { title: string; name: string };
  metadata_modified: string;
  num_resources: number;
  resources?: CkanResource[];
}

function formatInterval(seconds: number, t: (k: string) => string): string {
  if (seconds < 60) return `${seconds} ${t("tracked.seconds")}`;
  const minutes = Math.round(seconds / 60);
  if (minutes < 60) return `${minutes} ${t("tracked.minutes")}`;
  const hours = Math.round(minutes / 60);
  if (hours < 24) return `${hours} ${t("tracked.hours")}`;
  const days = Math.round(hours / 24);
  if (days < 7) return `${days} ${t("tracked.days")}`;
  if (days < 28) { const weeks = Math.round(days / 7); return `${weeks} ${t("tracked.weeks")}`; }
  if (days < 90) { const months = Math.round(days / 30); return `${months} ${t("tracked.months")}`; }
  const quarters = Math.round(days / 90);
  return `${quarters} ${t("tracked.quarters")}`;
}

export default function HomePage() {
  const { t } = useTranslation();
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<SearchResult[]>([]);
  const [count, setCount] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [targetResourceId, setTargetResourceId] = useState<string | null>(null);

  // Tracked datasets
  const [trackedDatasets, setTrackedDatasets] = useState<TrackedDataset[]>([]);
  const [trackedLoading, setTrackedLoading] = useState(true);

  // Request form state — which dataset has form open
  const [requestFormFor, setRequestFormFor] = useState<string | null>(null);

  useEffect(() => {
    publicApi.datasets()
      .then(setTrackedDatasets)
      .catch(() => {})
      .finally(() => setTrackedLoading(false));
  }, []);

  const extractDatasetName = (input: string): string | null => {
    const trimmed = input.trim();
    if (!trimmed.includes("data.gov.il") && !trimmed.includes("gov.il/he/dataset")) return null;
    const fullMatch = trimmed.match(/\/datasets\/([^/]+)\/([^/?#]+)/);
    if (fullMatch) return fullMatch[2];
    const simpleMatch = trimmed.match(/\/dataset\/([^/?#]+)/);
    if (simpleMatch) return simpleMatch[1];
    return null;
  };

  const extractResourceId = (input: string): string | null => {
    const trimmed = input.trim();
    const match = trimmed.match(/\/datasets\/[^/]+\/[^/]+\/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})/i);
    return match ? match[1] : null;
  };

  const stripHtml = (html: string) => {
    const doc = new DOMParser().parseFromString(html, "text/html");
    return doc.body.textContent || "";
  };

  const search = async (e?: FormEvent) => {
    e?.preventDefault();
    if (!query.trim()) return;
    setLoading(true);
    setError("");
    setTargetResourceId(null);
    setRequestFormFor(null);
    try {
      const datasetName = extractDatasetName(query);
      const resourceId = extractResourceId(query);
      if (datasetName) {
        const pkg = await ckan.dataset(datasetName);
        if (resourceId) setTargetResourceId(resourceId);
        setResults([pkg]);
        setCount(1);
      } else {
        const data = await ckan.search(query);
        setResults(data.results);
        setCount(data.count);
      }
    } catch (err: any) {
      setError(err.message);
    }
    setLoading(false);
  };

  const resultKey = (datasetId: string, resourceId?: string) =>
    resourceId ? `${datasetId}::${resourceId}` : datasetId;

  return (
    <div>
      {/* Hero Section */}
      <section className="home-hero" aria-labelledby="hero-title">
        <div className="container" style={{ textAlign: "center" }}>
          <h1 id="hero-title" style={{ fontSize: "2.25rem", fontWeight: 700, color: "white", marginBottom: "0.5rem" }}>
            {t("home.hero_title")}
          </h1>
          <p style={{ color: "rgba(255,255,255,0.9)", fontSize: "1.1rem", marginBottom: "1.5rem" }}>
            {t("home.hero_subtitle")}
          </p>

          <form onSubmit={search} className="home-search-form" role="search">
            <label htmlFor="home-search" className="sr-only" style={{ position: "absolute", width: 1, height: 1, overflow: "hidden", clip: "rect(0,0,0,0)" }}>
              {t("home.search_placeholder")}
            </label>
            <input
              id="home-search"
              type="search"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder={t("home.search_placeholder")}
              style={{
                flex: 1,
                padding: "0.75rem 1rem",
                fontSize: "1rem",
                border: "none",
                borderRadius: "var(--radius) 0 0 var(--radius)",
                outline: "none",
              }}
            />
            <button
              type="submit"
              disabled={loading}
              style={{
                padding: "0.75rem 1.5rem",
                fontSize: "1rem",
                fontWeight: 600,
                background: "var(--primary-dark)",
                color: "white",
                border: "none",
                borderRadius: "0 var(--radius) var(--radius) 0",
                cursor: "pointer",
              }}
            >
              {t("search.search_btn")}
            </button>
          </form>
        </div>
      </section>

      <div className="container mt-3" role="main">
        {/* Search Results */}
        {error && <div role="alert" className="badge badge-danger mb-2" style={{ display: "block", textAlign: "center" }}>{error}</div>}

        <div aria-live="polite" aria-atomic="true">
          {loading && <div className="loading" role="status">{t("common.loading")}</div>}
        </div>

        {!loading && results.length > 0 && (
          <section aria-label={t("search.title")} style={{ marginBottom: "2rem" }}>
            <div className="grid grid-2">
              {results.map((r) => {
                const targetResource = targetResourceId
                  ? r.resources?.find((res) => res.id === targetResourceId)
                  : null;

                const formKey = resultKey(r.id, targetResource?.id);

                return (
                  <article key={r.id} className="card">
                    <div className="flex-between mb-1">
                      <h2 style={{ fontSize: "1rem", fontWeight: 600, margin: 0 }}>{r.title}</h2>
                    </div>
                    {r.notes && (
                      <p className="text-sm text-muted mb-1" style={{ maxHeight: "3em", overflow: "hidden" }}>
                        {stripHtml(r.notes).slice(0, 200)}
                      </p>
                    )}
                    <div className="flex text-sm text-muted">
                      {r.organization && (
                        <span>{t("search.organization")}: {r.organization.title}</span>
                      )}
                      <span>{t("search.resources")}: {r.num_resources}</span>
                      <span>{t("search.last_modified")}: {r.metadata_modified?.slice(0, 10)}</span>
                    </div>

                    {/* Targeted resource */}
                    {targetResource && (
                      <div style={{
                        marginTop: "0.75rem",
                        padding: "0.75rem",
                        background: "var(--bg-secondary, #f8f9fa)",
                        borderRadius: "6px",
                        border: "1px solid var(--border)",
                      }}>
                        <div className="flex-between">
                          <div>
                            <span style={{ fontWeight: 600, fontSize: "0.9rem" }}>
                              {targetResource.name || targetResource.id}
                            </span>
                            {targetResource.format && (
                              <span className="badge" style={{ marginInlineStart: "0.5rem", fontSize: "0.7rem" }}>
                                {targetResource.format}
                              </span>
                            )}
                          </div>
                        </div>
                      </div>
                    )}

                    {/* Resources list when URL-based search */}
                    {!targetResource && r.resources && r.resources.length > 0 && extractDatasetName(query) && (
                      <div style={{ marginTop: "0.75rem" }}>
                        <div style={{ fontSize: "0.85rem", fontWeight: 600, marginBottom: "0.4rem" }}>
                          {t("search.resources")}:
                        </div>
                        {r.resources.map((res) => (
                          <div
                            key={res.id}
                            style={{
                              padding: "0.5rem 0.75rem",
                              marginBottom: "0.3rem",
                              background: "var(--bg-secondary, #f8f9fa)",
                              borderRadius: "4px",
                              border: "1px solid var(--border)",
                              display: "flex",
                              justifyContent: "space-between",
                              alignItems: "center",
                            }}
                          >
                            <div>
                              <span style={{ fontSize: "0.85rem" }}>{res.name || res.id}</span>
                              {res.format && (
                                <span className="badge" style={{ marginInlineStart: "0.5rem", fontSize: "0.7rem" }}>
                                  {res.format}
                                </span>
                              )}
                            </div>
                          </div>
                        ))}
                      </div>
                    )}

                    {/* Request Tracking button */}
                    <div style={{ marginTop: "0.75rem" }}>
                      {requestFormFor === formKey ? (
                        <RequestForm
                          ckanId={r.id}
                          resourceId={targetResource?.id}
                          datasetTitle={r.title}
                          onClose={() => setRequestFormFor(null)}
                        />
                      ) : (
                        <button
                          className="btn-primary"
                          onClick={() => setRequestFormFor(formKey)}
                          style={{ fontSize: "0.85rem" }}
                        >
                          {t("home.request_btn")}
                        </button>
                      )}
                    </div>
                  </article>
                );
              })}
            </div>

            {count > results.length && (
              <p className="text-sm text-muted mt-2" style={{ textAlign: "center" }}>
                {t("search.results_count", { count })}
              </p>
            )}
          </section>
        )}

        {!loading && results.length === 0 && query && !error && (
          <div className="empty-state mb-2">{t("search.no_results")}</div>
        )}

        {/* Tracked Datasets Section */}
        <section aria-labelledby="tracked-heading" style={{ marginTop: "1rem" }}>
          <h2 id="tracked-heading" style={{ fontSize: "1.5rem", fontWeight: 700, marginBottom: "1rem" }}>
            {t("home.tracked_title")}
          </h2>

          {trackedLoading ? (
            <div className="loading" role="status" aria-live="polite">{t("common.loading")}</div>
          ) : trackedDatasets.length === 0 ? (
            <div className="empty-state">{t("home.no_tracked")}</div>
          ) : (
            <div className="grid grid-2">
              {trackedDatasets.map((ds) => (
                <article key={ds.id} className="card">
                  <div className="flex-between mb-1">
                    <h3 style={{ fontSize: "1rem", fontWeight: 600, margin: 0 }}>
                      <Link to={`/versions/${ds.id}`}>{ds.title}</Link>
                    </h3>
                    <span className="badge badge-info">
                      {ds.version_count} {t("home.versions_count")}
                    </span>
                  </div>

                  {ds.resource_name && (
                    <p className="text-sm mb-1" style={{ color: "var(--primary)", fontWeight: 500 }}>
                      {ds.resource_name}
                    </p>
                  )}

                  <p className="text-sm text-muted mb-1">
                    {ds.organization}
                    {" · "}
                    {t("tracked.poll_interval")}: {formatInterval(ds.poll_interval, t)}
                  </p>

                  <div className="flex mt-1" style={{ gap: "0.75rem", flexWrap: "wrap" }}>
                    <Link
                      to={`/versions/${ds.id}`}
                      className="btn-primary"
                      style={{ textDecoration: "none", fontSize: "0.85rem", padding: "0.35rem 0.85rem" }}
                    >
                      {t("tracked.versions")}
                    </Link>

                    {ds.odata_dataset_id && (
                      <a
                        href={`${ODATA_BASE}/dataset/${ds.odata_dataset_id}`}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="text-sm"
                        style={{ color: "var(--primary)", textDecoration: "none" }}
                      >
                        ODATA &#8599;
                      </a>
                    )}

                    <a
                      href={ds.source_url || `https://data.gov.il/he/datasets/${ds.organization}/${ds.ckan_name}`}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="text-sm"
                      style={{ color: "var(--text-muted)", textDecoration: "none" }}
                    >
                      {t("home.source_link")} &#8599;
                    </a>
                  </div>
                </article>
              ))}
            </div>
          )}
        </section>
      </div>
    </div>
  );
}
