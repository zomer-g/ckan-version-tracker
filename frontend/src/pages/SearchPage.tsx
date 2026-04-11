import { useState, FormEvent } from "react";
import { useTranslation } from "react-i18next";
import { ckan, datasets as datasetsApi } from "../api/client";

interface SearchResult {
  id: string;
  name: string;
  title: string;
  notes: string;
  organization?: { title: string; name: string };
  metadata_modified: string;
  num_resources: number;
}

export default function SearchPage() {
  const { t } = useTranslation();
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<SearchResult[]>([]);
  const [count, setCount] = useState(0);
  const [loading, setLoading] = useState(false);
  const [tracking, setTracking] = useState<Set<string>>(new Set());
  const [tracked, setTracked] = useState<Set<string>>(new Set());
  const [showIntervalFor, setShowIntervalFor] = useState<string | null>(null);
  const [error, setError] = useState("");

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

  const search = async (e?: FormEvent) => {
    e?.preventDefault();
    setLoading(true);
    setError("");
    try {
      const datasetName = extractDatasetName(query);
      if (datasetName) {
        // Direct dataset lookup by name extracted from URL
        const pkg = await ckan.dataset(datasetName);
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

  const trackDataset = async (ckanId: string, interval: number) => {
    setShowIntervalFor(null);
    setTracking((prev) => new Set(prev).add(ckanId));
    try {
      await datasetsApi.track(ckanId, interval);
      setTracked((prev) => new Set(prev).add(ckanId));
    } catch (err: any) {
      if (err.message?.includes("already tracked")) {
        setTracked((prev) => new Set(prev).add(ckanId));
      } else {
        setError(err.message);
      }
    }
    setTracking((prev) => {
      const next = new Set(prev);
      next.delete(ckanId);
      return next;
    });
  };

  const INTERVAL_OPTIONS = [
    { value: 900, label: "\u05DB\u05DC 15 \u05D3\u05E7\u05D5\u05EA" },
    { value: 3600, label: "\u05DB\u05DC \u05E9\u05E2\u05D4" },
    { value: 43200, label: "\u05DB\u05DC 12 \u05E9\u05E2\u05D5\u05EA" },
    { value: 86400, label: "\u05DB\u05DC \u05D9\u05D5\u05DD" },
    { value: 604800, label: "\u05DB\u05DC \u05E9\u05D1\u05D5\u05E2" },
  ];

  const stripHtml = (html: string) => {
    const doc = new DOMParser().parseFromString(html, "text/html");
    return doc.body.textContent || "";
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
        {results.map((r) => (
          <article key={r.id} className="card">
            <div className="flex-between mb-1">
              <h2 style={{ fontSize: "1rem", fontWeight: 600 }}>{r.title}</h2>
              {tracked.has(r.id) ? (
                <span className="badge badge-success" role="status">{t("search.tracking")}</span>
              ) : (
                <div style={{ display: "flex", gap: "0.3rem", alignItems: "center" }}>
                  {showIntervalFor === r.id && (
                    <select
                      defaultValue={604800}
                      onChange={(e) => trackDataset(r.id, Number(e.target.value))}
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
                    onClick={() => showIntervalFor === r.id ? setShowIntervalFor(null) : setShowIntervalFor(r.id)}
                    disabled={tracking.has(r.id)}
                    aria-label={tracking.has(r.id) ? t("common.loading") : `${t("search.track_btn")} ${r.title}`}
                  >
                    {tracking.has(r.id) ? t("common.loading") : t("search.track_btn")}
                  </button>
                </div>
              )}
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
          </article>
        ))}
      </div>

      {count > results.length && (
        <p className="text-sm text-muted mt-2" style={{ textAlign: "center" }}>
          {t("search.results_count", { count, defaultValue: `${count} total results` })}
        </p>
      )}
    </div>
  );
}
