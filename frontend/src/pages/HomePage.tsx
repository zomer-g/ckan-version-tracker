import { useState, useEffect, FormEvent } from "react";
import { useTranslation } from "react-i18next";
import { Link } from "react-router-dom";
import { ckan, publicApi, govil, govmap, idf, TrackedDataset, GovIlValidation, GovMapValidation } from "../api/client";
import TagChips from "../components/TagChips";
import RequestForm from "../components/RequestForm";
import GovmapRequestForm from "../components/GovmapRequestForm";
import { sourceBadgeFor } from "../utils/sourceBadge";
// idf.il section pattern lives in utils/idfPattern.ts so the
// HomePage and SearchPage versions can never drift when we add new
// sections.
import { IDF_PATTERN } from "../utils/idfPattern";

const ODATA_BASE = "https://www.odata.org.il";

/** Detect gov.il collector URLs */
const GOV_IL_PATTERN = /^https?:\/\/(www\.)?gov\.il\/he\/(departments?\/dynamiccollectors?|collectors?|pages)\/([^/?#]+)/i;
/** Detect govmap.gov.il layer URLs (requires lay=<id>) */
const GOVMAP_PATTERN = /^https?:\/\/(www\.)?govmap\.gov\.il\/?\?.*[?&]lay(?:er|ers)?=\d+/i;

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

/**
 * Three-stat row under the search bar.
 *
 * Mirrors the Ocal/Ocoi hero canonical: 3 columns, big white number,
 * small primary-200 label. Numbers are derived from the dataset list
 * we already fetch — no second roundtrip, no flash of zero.
 *
 * If the list is still loading we render placeholders ("—") instead
 * of "0" so the user doesn't read a zero for a few hundred ms.
 */
function HomeStats({
  datasets,
  loading,
  t,
}: {
  datasets: TrackedDataset[];
  loading: boolean;
  t: (k: string) => string;
}) {
  const datasetCount = datasets.length;
  const versionCount = datasets.reduce((sum, d) => sum + (d.version_count || 0), 0);
  const orgCount = new Set(
    datasets
      .map((d) => d.organization_id || d.organization)
      .filter((x): x is string => Boolean(x)),
  ).size;

  const fmt = (n: number) => n.toLocaleString("he-IL");

  return (
    <div className="home-stats" aria-label="סטטיסטיקות">
      <div className="home-stat">
        <div className="home-stat-number">{loading ? "—" : fmt(datasetCount)}</div>
        <div className="home-stat-label">{t("home.stats_datasets")}</div>
      </div>
      <div className="home-stat">
        <div className="home-stat-number">{loading ? "—" : fmt(versionCount)}</div>
        <div className="home-stat-label">{t("home.stats_versions")}</div>
      </div>
      <div className="home-stat">
        <div className="home-stat-number">{loading ? "—" : fmt(orgCount)}</div>
        <div className="home-stat-label">{t("home.stats_organizations")}</div>
      </div>
    </div>
  );
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

  // Gov.il scraper result
  const [govIlResult, setGovIlResult] = useState<GovIlValidation | null>(null);
  // GovMap layer result (single seed URL — the form lets the user add more)
  const [govMapResult, setGovMapResult] = useState<GovMapValidation | null>(null);
  // IDF (idf.il) Military-Prosecution scraper result — same shape as
  // GovIlValidation (validator returns page_type/collector_name/title/url).
  const [idfResult, setIdfResult] = useState<GovIlValidation | null>(null);

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

  const detectGovIlUrl = (input: string): boolean => {
    return GOV_IL_PATTERN.test(input.trim());
  };

  const detectGovMapUrl = (input: string): boolean => {
    return GOVMAP_PATTERN.test(input.trim());
  };

  const detectIdfUrl = (input: string): boolean => {
    return IDF_PATTERN.test(input.trim());
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
    setGovIlResult(null);
    setGovMapResult(null);
    setIdfResult(null);
    try {
      // 1. Check for govmap.gov.il layer URL
      if (detectGovMapUrl(query)) {
        const validation = await govmap.validate(query.trim());
        if (validation.valid) {
          setGovMapResult(validation);
          setResults([]);
          setCount(0);
        } else {
          setError(validation.error || "Invalid govmap URL");
        }
        setLoading(false);
        return;
      }

      // 2. Check for gov.il collector URL
      if (detectGovIlUrl(query)) {
        const validation = await govil.validate(query.trim());
        if (validation.valid) {
          setGovIlResult(validation);
          setResults([]);
          setCount(0);
        } else {
          setError(validation.error || "Invalid gov.il URL");
        }
        setLoading(false);
        return;
      }

      // 2b. Check for idf.il Military-Prosecution URL
      if (detectIdfUrl(query)) {
        const validation = await idf.validate(query.trim());
        if (validation.valid) {
          setIdfResult(validation);
          setResults([]);
          setCount(0);
        } else {
          setError(validation.error || "Invalid idf.il URL");
        }
        setLoading(false);
        return;
      }

      // 3. Check for data.gov.il URL
      const datasetName = extractDatasetName(query);
      const resourceId = extractResourceId(query);
      if (datasetName) {
        const pkg = await ckan.dataset(datasetName);
        if (resourceId) setTargetResourceId(resourceId);
        setResults([pkg]);
        setCount(1);
      } else {
        // 3. Keyword search
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
      {/* Hero Section — לעם family canonical (single white search pill + 3 stats) */}
      <section className="home-hero" aria-labelledby="hero-title">
        <div className="container home-hero-inner">
          <h1 id="hero-title" className="home-hero-title">
            {t("home.hero_title")}
          </h1>
          <p className="home-hero-subtitle">
            {t("home.hero_subtitle")}
          </p>

          <form onSubmit={search} className="home-search-form" role="search">
            <label htmlFor="home-search" className="sr-only">
              {t("home.search_placeholder")}
            </label>
            <svg
              className="home-search-icon"
              viewBox="0 0 24 24"
              width="20"
              height="20"
              fill="none"
              stroke="currentColor"
              strokeWidth="2"
              strokeLinecap="round"
              strokeLinejoin="round"
              aria-hidden="true"
            >
              <circle cx="11" cy="11" r="7" />
              <path d="m21 21-4.3-4.3" />
            </svg>
            <input
              id="home-search"
              type="search"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder={t("home.search_placeholder")}
              className="home-search-input"
              disabled={loading}
            />
            {/* Submit happens on Enter — no visible button, matching Ocal/Ocoi. */}
            <button type="submit" className="sr-only" disabled={loading} aria-label={t("search.search_btn")}>
              {t("search.search_btn")}
            </button>
          </form>

          <HomeStats datasets={trackedDatasets} loading={trackedLoading} t={t} />
        </div>
      </section>

      <div className="container mt-3" role="main">
        {/* Search Results */}
        {error && <div role="alert" className="badge badge-danger mb-2" style={{ display: "block", textAlign: "center" }}>{error}</div>}

        <div aria-live="polite" aria-atomic="true">
          {loading && <div className="loading" role="status">{t("common.loading")}</div>}
        </div>

        {/* GovMap layer result */}
        {!loading && govMapResult && (
          <section aria-label="govmap result" style={{ marginBottom: "2rem" }}>
            <div className="grid grid-2">
              <article className="card" style={{ borderRight: "4px solid #0ea5e9" }}>
                <div className="flex-between mb-1">
                  <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
                    <h2 style={{ fontSize: "1rem", fontWeight: 600, margin: 0 }}>
                      {govMapResult.title}
                    </h2>
                    <span style={{
                      display: "inline-block",
                      padding: "0.15rem 0.5rem",
                      borderRadius: "9999px",
                      fontSize: "0.65rem",
                      fontWeight: 600,
                      background: "#e0f2fe",
                      color: "#075985",
                    }}>
                      GOVMAP
                    </span>
                  </div>
                </div>
                <div className="flex text-sm text-muted" style={{ gap: "0.75rem" }}>
                  <span>lay={govMapResult.layer_id}</span>
                  <span>govmap.gov.il</span>
                </div>
                <p className="text-sm text-muted mt-1" style={{ wordBreak: "break-all", direction: "ltr" }}>
                  <a href={govMapResult.url} target="_blank" rel="noopener noreferrer" style={{ color: "var(--primary)" }}>
                    {govMapResult.url}
                  </a>
                </p>

                <div style={{ marginTop: "0.75rem" }}>
                  {requestFormFor === "govmap" ? (
                    <GovmapRequestForm
                      initialUrl={govMapResult.url || ""}
                      onClose={() => setRequestFormFor(null)}
                    />
                  ) : (
                    <button
                      className="btn-primary"
                      onClick={() => setRequestFormFor("govmap")}
                      style={{ fontSize: "0.85rem" }}
                    >
                      {t("home.govmap_request_btn")}
                    </button>
                  )}
                </div>
              </article>
            </div>
          </section>
        )}

        {/* Gov.il scraper result */}
        {!loading && govIlResult && (
          <section aria-label="gov.il result" style={{ marginBottom: "2rem" }}>
            <div className="grid grid-2">
              <article className="card" style={{ borderRight: "4px solid #f59e0b" }}>
                <div className="flex-between mb-1">
                  <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
                    <h2 style={{ fontSize: "1rem", fontWeight: 600, margin: 0 }}>{govIlResult.title}</h2>
                    <span style={{
                      display: "inline-block",
                      padding: "0.15rem 0.5rem",
                      borderRadius: "9999px",
                      fontSize: "0.65rem",
                      fontWeight: 600,
                      background: "#fef3c7",
                      color: "#92400e",
                    }}>
                      GOV.IL
                    </span>
                  </div>
                </div>
                <div className="flex text-sm text-muted" style={{ gap: "0.75rem" }}>
                  <span>
                    {govIlResult.page_type === "dynamic_collector" ? "Dynamic Collector" : "Traditional Collector"}
                  </span>
                  <span>gov.il</span>
                </div>
                <p className="text-sm text-muted mt-1" style={{ wordBreak: "break-all" }}>
                  <a href={govIlResult.url} target="_blank" rel="noopener noreferrer" style={{ color: "var(--primary)" }}>
                    {govIlResult.url}
                  </a>
                </p>

                {/* Request form for scraper dataset */}
                <div style={{ marginTop: "0.75rem" }}>
                  {requestFormFor === "govil" ? (
                    <RequestForm
                      datasetTitle={govIlResult.title || ""}
                      onClose={() => setRequestFormFor(null)}
                      sourceType="scraper"
                      sourceUrl={govIlResult.url}
                    />
                  ) : (
                    <button
                      className="btn-primary"
                      onClick={() => setRequestFormFor("govil")}
                      style={{ fontSize: "0.85rem" }}
                    >
                      {t("home.request_btn")}
                    </button>
                  )}
                </div>
              </article>
            </div>
          </section>
        )}

        {/* IDF scraper result — same card layout as gov.il, different
            badge colour so it's obviously a separate source. */}
        {!loading && idfResult && (
          <section aria-label="idf.il result" style={{ marginBottom: "2rem" }}>
            <div className="grid grid-2">
              <article className="card" style={{ borderRight: "4px solid #0f766e" }}>
                <div className="flex-between mb-1">
                  <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
                    <h2 style={{ fontSize: "1rem", fontWeight: 600, margin: 0 }}>{idfResult.title}</h2>
                    <span style={{
                      display: "inline-block",
                      padding: "0.15rem 0.5rem",
                      borderRadius: "9999px",
                      fontSize: "0.65rem",
                      fontWeight: 600,
                      background: "#ccfbf1",
                      color: "#115e59",
                    }}>
                      IDF.IL
                    </span>
                  </div>
                </div>
                <div className="flex text-sm text-muted" style={{ gap: "0.75rem" }}>
                  <span>הפרקליטות הצבאית</span>
                  <span>idf.il</span>
                </div>
                <p className="text-sm text-muted mt-1" style={{ wordBreak: "break-all" }}>
                  <a href={idfResult.url} target="_blank" rel="noopener noreferrer" style={{ color: "var(--primary)" }}>
                    {idfResult.url}
                  </a>
                </p>

                <div style={{ marginTop: "0.75rem" }}>
                  {requestFormFor === "idf" ? (
                    <RequestForm
                      datasetTitle={idfResult.title || ""}
                      onClose={() => setRequestFormFor(null)}
                      sourceType="scraper"
                      sourceUrl={idfResult.url}
                    />
                  ) : (
                    <button
                      className="btn-primary"
                      onClick={() => setRequestFormFor("idf")}
                      style={{ fontSize: "0.85rem" }}
                    >
                      {t("home.request_btn")}
                    </button>
                  )}
                </div>
              </article>
            </div>
          </section>
        )}

        {/* CKAN results */}
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

                    {/* Resources list when URL-based search.
                        When the request form is open for THIS dataset we
                        hide the read-only list — the form has its own
                        interactive picker for the same resources, and
                        showing both is just visual noise that confused
                        the admin into thinking nothing was clickable. */}
                    {!targetResource &&
                      r.resources &&
                      r.resources.length > 0 &&
                      extractDatasetName(query) &&
                      requestFormFor !== formKey && (
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
                          availableResources={
                            r.resources?.map((res) => ({
                              id: res.id,
                              name: res.name,
                              format: res.format,
                            }))
                          }
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

        {!loading && results.length === 0 && !govIlResult && !govMapResult && !idfResult && query && !error && (
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
                    <div className="flex" style={{ gap: "0.4rem", alignItems: "center" }}>
                      {(() => {
                        const palette = sourceBadgeFor(ds.source_type, ds.organization, ds.ckan_id);
                        return (
                          <span style={{
                            display: "inline-block",
                            padding: "0.15rem 0.45rem",
                            borderRadius: "9999px",
                            fontSize: "0.65rem",
                            fontWeight: 600,
                            background: palette.bg,
                            color: palette.fg,
                          }}>
                            {palette.label}
                          </span>
                        );
                      })()}
                      <span className="badge badge-info">
                        {ds.version_count} {t("home.versions_count")}
                      </span>
                    </div>
                  </div>

                  {ds.resource_name && (
                    <p className="text-sm mb-1" style={{ color: "var(--primary)", fontWeight: 500 }}>
                      {ds.resource_name}
                    </p>
                  )}

                  <p className="text-sm text-muted mb-1">
                    {ds.organization_id ? (
                      <Link
                        to={`/organizations/${ds.organization_id}`}
                        style={{ color: "var(--primary)", textDecoration: "none" }}
                      >
                        {ds.organization_title || ds.organization}
                      </Link>
                    ) : (
                      ds.organization
                    )}
                    {" · "}
                    {t("tracked.poll_interval")}: {formatInterval(ds.poll_interval, t)}
                  </p>

                  <TagChips tags={ds.tags} />

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
                        // Promoted from "small underlined ODATA text"
                        // to a visible outlined button so casual users
                        // notice the archived files exist. Outlined
                        // (not filled) so it stays clearly secondary
                        // to the primary "גרסאות" button next to it.
                        style={{
                          fontSize: "0.85rem",
                          padding: "0.35rem 0.85rem",
                          background: "white",
                          color: "var(--primary, #0f766e)",
                          border: "1px solid var(--primary, #0f766e)",
                          borderRadius: 4,
                          textDecoration: "none",
                          fontWeight: 500,
                        }}
                      >
                        {t("tracked.open_archive_short")} &#8599;
                      </a>
                    )}

                    {(() => {
                      const sourceHref =
                        ds.source_type === "scraper" || ds.source_type === "govmap"
                          ? ds.source_url
                          : (ds.source_url || `https://data.gov.il/he/datasets/${ds.organization}/${ds.ckan_name}`);
                      if (!sourceHref) return null;
                      const linkLabel = t(sourceBadgeFor(ds.source_type, ds.organization, ds.ckan_id).sourceLinkKey);
                      return (
                        <a
                          href={sourceHref}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="text-sm"
                          style={{ color: "var(--text-muted)", textDecoration: "none" }}
                        >
                          {linkLabel} &#8599;
                        </a>
                      );
                    })()}

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
