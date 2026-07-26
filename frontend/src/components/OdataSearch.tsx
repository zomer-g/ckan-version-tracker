import { useCallback, useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";

/**
 * Live faceted search over מידע לעם (odata.org.il).
 *
 * odata.org.il runs CKAN, whose action API is CORS-open
 * (access-control-allow-origin: *), so we query it DIRECTLY from the
 * browser — no backend proxy. Results link back to the dataset page on
 * odata; nothing is stored here.
 *
 * Faceting is deliberately limited to ORGANIZATIONS and FORMATS (res_format)
 * per product spec — no tags, no groups/entities. Facet lists are computed
 * from the query alone (rows=0 request) so they stay stable and support
 * multi-select; the results request carries the full filter (fq).
 */
const ODATA_BASE = "https://www.odata.org.il";
const PAGE_ROWS = 20;
const FACET_LIMIT = 50;
const FACET_VISIBLE = 10;

type OdataOrg = { title?: string; name?: string };
type OdataResource = {
  id?: string;
  name?: string;
  format?: string;
  url?: string;
};
type OdataPackage = {
  name: string;
  title?: string;
  notes?: string;
  num_resources?: number;
  organization?: OdataOrg | null;
  resources?: OdataResource[];
};
type FacetItem = { name: string; count: number };

function localePrefix(lang: string) {
  return lang === "en" ? "/en" : "";
}
function datasetUrl(name: string, lang: string) {
  return `${ODATA_BASE}${localePrefix(lang)}/dataset/${encodeURIComponent(name)}`;
}
function searchUrl(q: string, lang: string) {
  return `${ODATA_BASE}${localePrefix(lang)}/dataset?q=${encodeURIComponent(q)}`;
}
function snippet(text: string | undefined, max = 220) {
  if (!text) return "";
  const clean = text.replace(/\s+/g, " ").trim();
  return clean.length > max ? clean.slice(0, max).trimEnd() + "…" : clean;
}

function resourceName(r: OdataResource, fallback: string) {
  const n = r.name?.trim();
  if (n) return n;
  try {
    const tail = decodeURIComponent((r.url || "").split("/").pop() || "");
    if (tail) return tail;
  } catch {
    /* malformed URL — use the fallback */
  }
  return fallback;
}

/** Solr filter-query: OR within a field, AND across fields. */
function buildFq(orgs: string[], formats: string[]) {
  const parts: string[] = [];
  if (orgs.length)
    parts.push(`organization:(${orgs.map((v) => `"${v}"`).join(" OR ")})`);
  if (formats.length)
    parts.push(`res_format:(${formats.map((v) => `"${v}"`).join(" OR ")})`);
  return parts.join(" AND ");
}

function toggle(set: string[], value: string) {
  return set.includes(value)
    ? set.filter((v) => v !== value)
    : [...set, value];
}

export default function OdataSearch() {
  const { t, i18n } = useTranslation();
  const lang = i18n.language;

  const [query, setQuery] = useState("");
  const [activeQuery, setActiveQuery] = useState("");
  const [selectedOrgs, setSelectedOrgs] = useState<string[]>([]);
  const [selectedFormats, setSelectedFormats] = useState<string[]>([]);

  const [results, setResults] = useState<OdataPackage[]>([]);
  const [count, setCount] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);

  const [orgFacets, setOrgFacets] = useState<FacetItem[]>([]);
  const [formatFacets, setFormatFacets] = useState<FacetItem[]>([]);
  const [orgTitles, setOrgTitles] = useState<Record<string, string>>({});
  const [showAllOrgs, setShowAllOrgs] = useState(false);
  const [showAllFormats, setShowAllFormats] = useState(false);

  const resultsAbort = useRef<AbortController | null>(null);
  const facetsAbort = useRef<AbortController | null>(null);

  // One-time: slug → Hebrew/English org title (facets only carry the slug).
  useEffect(() => {
    let alive = true;
    fetch(
      `${ODATA_BASE}/api/3/action/organization_list` +
        `?all_fields=true&include_dataset_count=false&limit=1000`
    )
      .then((r) => r.json())
      .then((d) => {
        if (!alive || !d?.success) return;
        const map: Record<string, string> = {};
        for (const o of d.result || [])
          map[o.name] = o.title || o.display_name || o.name;
        setOrgTitles(map);
      })
      .catch(() => {
        /* fall back to slugs — non-fatal */
      });
    return () => {
      alive = false;
    };
  }, []);

  // Facet lists depend only on the query text (kept stable across filter
  // toggles so multi-select works and the full breakdown stays visible).
  const loadFacets = useCallback(async (q: string) => {
    facetsAbort.current?.abort();
    const ctrl = new AbortController();
    facetsAbort.current = ctrl;
    try {
      const url =
        `${ODATA_BASE}/api/3/action/package_search` +
        `?q=${encodeURIComponent(q)}&rows=0` +
        `&facet.field=${encodeURIComponent('["organization","res_format"]')}` +
        `&facet.limit=${FACET_LIMIT}`;
      const res = await fetch(url, { signal: ctrl.signal });
      const data = await res.json();
      if (!data?.success) return;
      const sf = data.result?.search_facets || {};
      setOrgFacets(sf.organization?.items || []);
      setFormatFacets(sf.res_format?.items || []);
    } catch {
      /* leave prior facets in place on failure */
    }
  }, []);

  const loadResults = useCallback(
    async (q: string, orgs: string[], formats: string[]) => {
      resultsAbort.current?.abort();
      const ctrl = new AbortController();
      resultsAbort.current = ctrl;
      setLoading(true);
      setError(false);
      try {
        const fq = buildFq(orgs, formats);
        const url =
          `${ODATA_BASE}/api/3/action/package_search` +
          `?q=${encodeURIComponent(q)}&rows=${PAGE_ROWS}` +
          (fq ? `&fq=${encodeURIComponent(fq)}` : "");
        const res = await fetch(url, { signal: ctrl.signal });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        if (!data?.success) throw new Error("CKAN success=false");
        setResults(data.result?.results || []);
        setCount(data.result?.count || 0);
      } catch (err) {
        if ((err as Error).name === "AbortError") return;
        setError(true);
        setResults([]);
        setCount(0);
      } finally {
        if (resultsAbort.current === ctrl) setLoading(false);
      }
    },
    []
  );

  // Facets re-run on query change only.
  useEffect(() => {
    loadFacets(activeQuery);
  }, [activeQuery, loadFacets]);

  // Results re-run on query OR any filter change.
  useEffect(() => {
    loadResults(activeQuery, selectedOrgs, selectedFormats);
  }, [activeQuery, selectedOrgs, selectedFormats, loadResults]);

  useEffect(
    () => () => {
      resultsAbort.current?.abort();
      facetsAbort.current?.abort();
    },
    []
  );

  const onSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    setActiveQuery(query.trim());
  };

  const clearFilters = () => {
    setSelectedOrgs([]);
    setSelectedFormats([]);
  };
  const hasFilters = selectedOrgs.length > 0 || selectedFormats.length > 0;

  const renderFacet = (
    title: string,
    items: FacetItem[],
    selected: string[],
    onToggle: (v: string) => void,
    label: (name: string) => string,
    showAll: boolean,
    setShowAll: (v: boolean) => void
  ) => {
    if (items.length === 0) return null;
    const visible = showAll ? items : items.slice(0, FACET_VISIBLE);
    return (
      <div className="odata-facet">
        <div className="odata-facet-title">{title}</div>
        <ul className="odata-facet-list">
          {visible.map((it) => {
            const active = selected.includes(it.name);
            return (
              <li key={it.name}>
                <button
                  type="button"
                  className={`odata-facet-item${active ? " is-active" : ""}`}
                  aria-pressed={active}
                  onClick={() => onToggle(it.name)}
                >
                  <span className="odata-facet-label">{label(it.name)}</span>
                  <span className="odata-facet-count">{it.count}</span>
                </button>
              </li>
            );
          })}
        </ul>
        {items.length > FACET_VISIBLE && (
          <button
            type="button"
            className="odata-facet-more"
            onClick={() => setShowAll(!showAll)}
          >
            {showAll
              ? t("projects.odata_search.show_less")
              : t("projects.odata_search.show_more", {
                  count: items.length - FACET_VISIBLE,
                })}
          </button>
        )}
      </div>
    );
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

      {error ? (
        <div className="odata-search-msg odata-search-error" role="alert">
          {t("projects.odata_search.error")}
        </div>
      ) : (
        <div className="odata-layout">
          <aside className="odata-facets" aria-label={t("projects.odata_search.filters")}>
            <div className="odata-facets-head">
              <span>{t("projects.odata_search.filters")}</span>
              {hasFilters && (
                <button
                  type="button"
                  className="odata-clear"
                  onClick={clearFilters}
                >
                  {t("projects.odata_search.clear_filters")}
                </button>
              )}
            </div>
            {renderFacet(
              t("projects.odata_search.filters_orgs"),
              orgFacets,
              selectedOrgs,
              (v) => setSelectedOrgs((s) => toggle(s, v)),
              (name) => orgTitles[name] || name,
              showAllOrgs,
              setShowAllOrgs
            )}
            {renderFacet(
              t("projects.odata_search.filters_formats"),
              formatFacets,
              selectedFormats,
              (v) => setSelectedFormats((s) => toggle(s, v)),
              (name) => name,
              showAllFormats,
              setShowAllFormats
            )}
          </aside>

          <div className="odata-main">
            <div className="odata-search-count" role="status">
              {loading
                ? t("projects.odata_search.searching")
                : t("projects.odata_search.results_count", { count })}
            </div>

            {!loading && results.length === 0 && (
              <div className="odata-search-msg" role="status">
                {t("projects.odata_search.no_results")}
              </div>
            )}

            <ul className="odata-results">
              {results.map((pkg) => {
                const org = pkg.organization?.title || pkg.organization?.name;
                const title = pkg.title?.trim() || pkg.name;
                return (
                  <li key={pkg.name} className="odata-result">
                    <a
                      className="odata-result-title"
                      href={datasetUrl(pkg.name, lang)}
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
                    {pkg.resources && pkg.resources.length > 0 && (
                      <details className="odata-files">
                        <summary className="odata-files-summary">
                          {t("projects.odata_search.files", {
                            count: pkg.resources.length,
                          })}
                        </summary>
                        <ul className="odata-file-list">
                          {pkg.resources.map((r, idx) => (
                            <li key={r.id || `${pkg.name}-${idx}`}>
                              {r.url ? (
                                <a
                                  className="odata-file"
                                  href={r.url}
                                  target="_blank"
                                  rel="noopener noreferrer"
                                >
                                  <span className="odata-file-format">
                                    {(r.format || "").toUpperCase() || "—"}
                                  </span>
                                  <span className="odata-file-name">
                                    {resourceName(
                                      r,
                                      t("projects.odata_search.file_unnamed")
                                    )}
                                  </span>
                                </a>
                              ) : (
                                <span className="odata-file odata-file-disabled">
                                  <span className="odata-file-format">
                                    {(r.format || "").toUpperCase() || "—"}
                                  </span>
                                  <span className="odata-file-name">
                                    {resourceName(
                                      r,
                                      t("projects.odata_search.file_unnamed")
                                    )}
                                  </span>
                                </span>
                              )}
                            </li>
                          ))}
                        </ul>
                      </details>
                    )}
                  </li>
                );
              })}
            </ul>

            {!loading && count > results.length && (
              <a
                className="odata-search-more"
                href={searchUrl(activeQuery, lang)}
                target="_blank"
                rel="noopener noreferrer"
              >
                {t("projects.odata_search.view_all", { count })}
              </a>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
