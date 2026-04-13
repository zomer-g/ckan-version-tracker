import { useState, FormEvent } from "react";
import { useTranslation } from "react-i18next";
import { ckan, datasets as datasetsApi } from "../api/client";
import { useAuth } from "../auth/AuthContext";

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

export default function SearchPage() {
  const { t } = useTranslation();
  const { user } = useAuth();
  const isAdmin = !!user?.is_admin;
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<SearchResult[]>([]);
  const [count, setCount] = useState(0);
  const [loading, setLoading] = useState(false);
  const [tracking, setTracking] = useState<Set<string>>(new Set());
  const [tracked, setTracked] = useState<Map<string, "tracked" | "pending">>(new Map());
  const [showIntervalFor, setShowIntervalFor] = useState<string | null>(null);
  const [error, setError] = useState("");
  // Track which resource_id was extracted from URL
  const [targetResourceId, setTargetResourceId] = useState<string | null>(null);

  /**
   * Extract dataset name from a data.gov.il URL, or return null if not a URL.
   * Supports:
   *   https://data.gov.il/he/datasets/org_name/dataset_name
   *   https://data.gov.il/he/datasets/org_name/dataset_name/resource_id
   *   https://data.gov.il/dataset/dataset_name
   */
  const extractDatasetName = (input: string): string | null => {
    const trimmed = input.trim();
    if (!trimmed.includes("data.gov.il") && !trimmed.includes("gov.il/he/dataset")) return null;

    // /datasets/org/name or /datasets/org/name/resource_id — always grab 2nd segment (dataset name)
    const fullMatch = trimmed.match(/\/datasets\/([^/]+)\/([^/?#]+)/);
    if (fullMatch) return fullMatch[2]; // org/dataset_name — return dataset_name

    // /dataset/name (without org)
    const simpleMatch = trimmed.match(/\/dataset\/([^/?#]+)/);
    if (simpleMatch) return simpleMatch[1];

    return null;
  };

  /**
   * Extract resource_id (UUID) from a data.gov.il resource URL.
   * Resource URLs look like: /datasets/org/name/c4a8e209-c4a4-4482-b094-defc5bf4588e
   */
  const extractResourceId = (input: string): string | null => {
    const trimmed = input.trim();
    // Match UUID at the end of a datasets URL path
    const match = trimmed.match(/\/datasets\/[^/]+\/[^/]+\/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})/i);
    return match ? match[1] : null;
  };

  const search = async (e?: FormEvent) => {
    e?.preventDefault();
    setLoading(true);
    setError("");
    setTargetResourceId(null);
    try {
      const datasetName = extractDatasetName(query);
      const resourceId = extractResourceId(query);
      if (datasetName) {
        // Direct dataset lookup by name extracted from URL
        const pkg = await ckan.dataset(datasetName);
        if (resourceId) {
          setTargetResourceId(resourceId);
        }
        setResults([pkg]);
        setCount(1);
      } else {
        // Regular keyword search
        const data = await ckan.search(query);
        setResults(data.results);
        setCount(data.count);
      }
    } catch (err: any) {
      setError(err.message);
    }
    setLoading(false);
  };

  /** Unique key for tracking state: combines dataset id + optional resource id */
  const trackKey = (datasetId: string, resourceId?: string) =>
    resourceId ? `${datasetId}::${resourceId}` : datasetId;

  const trackDataset = async (ckanId: string, interval: number, resourceId?: string) => {
    const key = trackKey(ckanId, resourceId);
    setShowIntervalFor(null);
    setTracking((prev) => new Set(prev).add(key));
    try {
      await datasetsApi.track(ckanId, interval, resourceId);
      setTracked((prev) => new Map(prev).set(key, isAdmin ? "tracked" : "pending"));
    } catch (err: any) {
      if (err.message?.includes("already tracked")) {
        setTracked((prev) => new Map(prev).set(key, "tracked"));
      } else {
        setError(err.message);
      }
    }
    setTracking((prev) => {
      const next = new Set(prev);
      next.delete(key);
      return next;
    });
  };

  const INTERVAL_OPTIONS = [
    { value: 900, label: "כל 15 דקות" },
    { value: 3600, label: "כל שעה" },
    { value: 43200, label: "כל 12 שעות" },
    { value: 86400, label: "כל יום" },
    { value: 604800, label: "כל שבוע" },
    { value: 2592000, label: "כל חודש" },
    { value: 7776000, label: "כל רבעון" },
  ];

  const stripHtml = (html: string) => {
    const doc = new DOMParser().parseFromString(html, "text/html");
    return doc.body.textContent || "";
  };

  /** Render the track button / status badge for a given dataset+resource combo */
  const renderTrackButton = (datasetId: string, label: string, resourceId?: string) => {
    const key = trackKey(datasetId, resourceId);
    const status = tracked.get(key);

    if (status === "tracked") {
      return <span className="badge badge-success" role="status">{t("search.tracking")}</span>;
    }
    if (status === "pending") {
      return (
        <span className="badge badge-success" role="status" style={{ background: "#22c55e", color: "#fff" }}>
          {t("search.request_sent", "\u05D4\u05D1\u05E7\u05E9\u05D4 \u05E0\u05E9\u05DC\u05D7\u05D4 \u2014 \u05DE\u05DE\u05EA\u05D9\u05DF \u05DC\u05D0\u05D9\u05E9\u05D5\u05E8")}
        </span>
      );
    }

    return (
      <div style={{ display: "flex", gap: "0.3rem", alignItems: "center" }}>
        {showIntervalFor === key && (
          <select
            defaultValue={604800}
            onChange={(e) => trackDataset(datasetId, Number(e.target.value), resourceId)}
            style={{ width: "auto", padding: "0.2rem 0.4rem", fontSize: "0.8rem" }}
            aria-label={t("tracked.poll_interval")}
            autoFocus
          >
            <option value="" disabled>{t("tracked.poll_interval")}</option>
            {INTERVAL_OPTIONS.map((opt) => (
              <option key={opt.value} value={opt.value}>{opt.label}</option>
            ))}
          </select>
        )}
        <button
          className="btn-primary"
          onClick={() => showIntervalFor === key ? setShowIntervalFor(null) : setShowIntervalFor(key)}
          disabled={tracking.has(key)}
          aria-label={tracking.has(key) ? t("common.loading") : `${t("search.track_btn")} ${label}`}
          style={{ fontSize: "0.8rem", padding: "0.25rem 0.6rem" }}
        >
          {tracking.has(key) ? t("common.loading") : t("search.track_btn")}
        </button>
      </div>
    );
  };

  return (
    <div>
      <div className="page-header">
        <h1>{t("search.title")}</h1>
      </div>

      <form onSubmit={search} className="flex mb-2" role="search">
        <label htmlFor="search-input" className="sr-only" style={{ position: "absolute", width: 1, height: 1, overflow: "hidden", clip: "rect(0,0,0,0)" }}>
          {t("search.placeholder")}
        </label>
        <input
          id="search-input"
          type="search"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder={t("search.placeholder")}
          style={{ flex: 1 }}
        />
        <button type="submit" className="btn-primary" disabled={loading}>
          {t("search.search_btn")}
        </button>
      </form>

      {error && <div role="alert" className="badge badge-danger mb-2">{error}</div>}

      <div aria-live="polite" aria-atomic="true">
        {loading && <div className="loading" role="status">{t("common.loading")}</div>}

        {!loading && results.length === 0 && query && (
          <div className="empty-state">{t("search.no_results")}</div>
        )}

        {!loading && count > 0 && (
          <p className="sr-only" style={{ position: "absolute", width: 1, height: 1, overflow: "hidden", clip: "rect(0,0,0,0)" }}>
            {t("search.results_count", { count, defaultValue: `${count} results found` })}
          </p>
        )}
      </div>

      <div className="grid grid-2">
        {results.map((r) => {
          // If a specific resource was targeted via URL, show only that resource
          const targetResource = targetResourceId
            ? r.resources?.find((res) => res.id === targetResourceId)
            : null;

          return (
            <article key={r.id} className="card">
              <div className="flex-between mb-1">
                <h2 style={{ fontSize: "1rem", fontWeight: 600 }}>{r.title}</h2>
                {/* Dataset-level track button (only if no specific resource targeted) */}
                {!targetResource && renderTrackButton(r.id, r.title)}
              </div>
              {r.notes && (
                <p className="text-sm text-muted mb-1" style={{ maxHeight: "3em", overflow: "hidden" }}>
                  {stripHtml(r.notes).slice(0, 200)}
                </p>
              )}
              <div className="flex text-sm text-muted">
                {r.organization && (
                  <span>
                    {t("search.organization")}: {r.organization.title}
                  </span>
                )}
                <span>
                  {t("search.resources")}: {r.num_resources}
                </span>
                <span>
                  {t("search.last_modified")}: {r.metadata_modified?.slice(0, 10)}
                </span>
              </div>

              {/* Show targeted resource for tracking */}
              {targetResource && (
                <div
                  style={{
                    marginTop: "0.75rem",
                    padding: "0.75rem",
                    background: "var(--bg-secondary, #f8f9fa)",
                    borderRadius: "6px",
                    border: "1px solid var(--border, #e2e8f0)",
                  }}
                >
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
                    {renderTrackButton(r.id, `${r.title} - ${targetResource.name}`, targetResource.id)}
                  </div>
                </div>
              )}

              {/* Show all resources when dataset fetched via URL (but no specific resource targeted) */}
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
                        border: "1px solid var(--border, #e2e8f0)",
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
                      {renderTrackButton(r.id, `${r.title} - ${res.name}`, res.id)}
                    </div>
                  ))}
                </div>
              )}
            </article>
          );
        })}
      </div>

      {count > results.length && (
        <p className="text-sm text-muted mt-2" style={{ textAlign: "center" }}>
          {t("search.results_count", { count, defaultValue: `${count} total results` })}
        </p>
      )}
    </div>
  );
}
