import { useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";

/**
 * Live search over מידע לעם (odata.org.il).
 *
 * odata.org.il runs CKAN, whose action API is CORS-open
 * (access-control-allow-origin: *), so we query it DIRECTLY from the
 * browser — no backend proxy. Results link back to the dataset page on
 * odata; nothing is stored here. The surrounding page carries the
 * "processed data" banner, so this component stays purely functional.
 */
const ODATA_BASE = "https://www.odata.org.il";
const PAGE_ROWS = 20;

type OdataOrg = { title?: string; name?: string };
type OdataTag = { name?: string; display_name?: string };
type OdataPackage = {
  name: string;
  title?: string;
  notes?: string;
  num_resources?: number;
  organization?: OdataOrg | null;
  tags?: OdataTag[];
};

function datasetUrl(name: string, lang: string) {
  const prefix = lang === "en" ? "/en" : "";
  return `${ODATA_BASE}${prefix}/dataset/${encodeURIComponent(name)}`;
}

function searchUrl(q: string, lang: string) {
  const prefix = lang === "en" ? "/en" : "";
  return `${ODATA_BASE}${prefix}/dataset?q=${encodeURIComponent(q)}`;
}

function snippet(text: string | undefined, max = 220) {
  if (!text) return "";
  const clean = text.replace(/\s+/g, " ").trim();
  return clean.length > max ? clean.slice(0, max).trimEnd() + "…" : clean;
}

export default function OdataSearch() {
  const { t, i18n } = useTranslation();
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<OdataPackage[]>([]);
  const [count, setCount] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(false);
  const [searched, setSearched] = useState(false);
  const [lastQuery, setLastQuery] = useState("");
  const abortRef = useRef<AbortController | null>(null);

  const runSearch = async (q: string) => {
    const trimmed = q.trim();
    abortRef.current?.abort();
    const ctrl = new AbortController();
    abortRef.current = ctrl;

    setLoading(true);
    setError(false);
    setSearched(true);
    setLastQuery(trimmed);
    try {
      const url =
        `${ODATA_BASE}/api/3/action/package_search` +
        `?q=${encodeURIComponent(trimmed)}&rows=${PAGE_ROWS}`;
      const res = await fetch(url, { signal: ctrl.signal });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      if (!data?.success) throw new Error("CKAN returned success=false");
      const result = data.result || {};
      setResults(result.results || []);
      setCount(result.count || 0);
    } catch (err) {
      if ((err as Error).name === "AbortError") return; // superseded — ignore
      setError(true);
      setResults([]);
      setCount(0);
    } finally {
      // Only the latest request clears the spinner.
      if (abortRef.current === ctrl) setLoading(false);
    }
  };

  // Cancel any in-flight request on unmount.
  useEffect(() => () => abortRef.current?.abort(), []);

  const onSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    runSearch(query);
  };

  return (
    <div className="odata-search">
      <p className="odata-search-intro">{t("projects.odata_search.intro")}</p>

      <form className="odata-search-form" onSubmit={onSubmit} role="search">
        <input
          type="search"
          className="odata-search-input"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder={t("projects.odata_search.placeholder")}
          aria-label={t("projects.odata_search.placeholder")}
        />
        <button type="submit" className="btn-primary" disabled={loading}>
          {loading
            ? t("projects.odata_search.searching")
            : t("projects.odata_search.button")}
        </button>
      </form>

      {error && (
        <div className="odata-search-msg odata-search-error" role="alert">
          {t("projects.odata_search.error")}
        </div>
      )}

      {!error && searched && !loading && results.length === 0 && (
        <div className="odata-search-msg" role="status">
          {t("projects.odata_search.no_results")}
        </div>
      )}

      {!error && results.length > 0 && (
        <>
          <div className="odata-search-count" role="status">
            {t("projects.odata_search.results_count", { count })}
          </div>

          <ul className="odata-results">
            {results.map((pkg) => {
              const org = pkg.organization?.title || pkg.organization?.name;
              const title = pkg.title?.trim() || pkg.name;
              return (
                <li key={pkg.name} className="odata-result">
                  <a
                    className="odata-result-title"
                    href={datasetUrl(pkg.name, i18n.language)}
                    target="_blank"
                    rel="noopener noreferrer"
                  >
                    {title}
                  </a>
                  <div className="odata-result-meta">
                    {org && <span className="odata-result-org">{org}</span>}
                    <span className="odata-result-resources">
                      {t("projects.odata_search.resources", {
                        count: pkg.num_resources || 0,
                      })}
                    </span>
                  </div>
                  {pkg.notes && (
                    <p className="odata-result-notes">{snippet(pkg.notes)}</p>
                  )}
                </li>
              );
            })}
          </ul>

          {count > results.length && (
            <a
              className="odata-search-more"
              href={searchUrl(lastQuery, i18n.language)}
              target="_blank"
              rel="noopener noreferrer"
            >
              {t("projects.odata_search.view_all", { count })}
            </a>
          )}
        </>
      )}
    </div>
  );
}
