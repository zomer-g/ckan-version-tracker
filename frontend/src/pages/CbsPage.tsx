import { useState, useEffect, useCallback, FormEvent } from "react";
import { useSearchParams } from "react-router-dom";
import { useTranslation } from "react-i18next";
import {
  cbs,
  formatBytes,
  CbsResult,
  CbsFacets,
  CbsSearchParams,
} from "../api/client";

// Human labels for the geographic-granularity codes the crawler emits.
const GEO_LABELS: Record<string, string> = {
  national: "ארצי",
  district: "מחוז",
  subdistrict: "נפה",
  municipality: "רשות מקומית",
  locality: "יישוב",
};

const PAGE_SIZE = 30;

export default function CbsPage() {
  const { t, i18n } = useTranslation();
  const he = i18n.language === "he";

  // Search + filter state is mirrored into the URL query string so a specific
  // search is shareable / deep-linkable. On first render we seed state FROM the
  // URL; runSearch() writes state back TO the URL. We read the params once for
  // the initial state — later URL writes go through setSearchParams, and we
  // don't re-seed from `searchParams` on every change (that would fight the
  // controlled inputs).
  const [searchParams, setSearchParams] = useSearchParams();
  const initial = new URLSearchParams(searchParams);

  const [query, setQuery] = useState(() => initial.get("q") || "");
  const [facets, setFacets] = useState<CbsFacets | null>(null);
  const [results, setResults] = useState<CbsResult[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [offset, setOffset] = useState(0);

  // Active facet filters.
  const [subject, setSubject] = useState(() => initial.get("subject") || "");
  const [geo, setGeo] = useState(() => initial.get("geo") || "");
  const [fileType, setFileType] = useState(() => initial.get("file_type") || "");
  const [yearFrom, setYearFrom] = useState(() => initial.get("year_from") || "");
  const [yearTo, setYearTo] = useState(() => initial.get("year_to") || "");

  useEffect(() => {
    cbs.facets().then(setFacets).catch(() => {
      // Non-fatal — the page still works as free-text search without facets.
    });
  }, []);

  const runSearch = useCallback(
    async (nextOffset = 0) => {
      setLoading(true);
      setError("");
      // On a fresh search (page 1) reflect the active query + filters into the
      // URL so the address bar is a shareable link. `replace` keeps filter
      // tweaks out of the browser history. Paging ("load more") doesn't touch
      // the URL — a shared link always opens on the first page.
      if (nextOffset === 0) {
        const sp = new URLSearchParams();
        if (query.trim()) sp.set("q", query.trim());
        if (subject) sp.set("subject", subject);
        if (geo) sp.set("geo", geo);
        if (fileType) sp.set("file_type", fileType);
        if (yearFrom) sp.set("year_from", yearFrom);
        if (yearTo) sp.set("year_to", yearTo);
        setSearchParams(sp, { replace: true });
      }
      try {
        const params: CbsSearchParams = {
          q: query.trim() || undefined,
          subject: subject || undefined,
          geo: geo || undefined,
          file_type: fileType || undefined,
          year_from: yearFrom ? Number(yearFrom) : undefined,
          year_to: yearTo ? Number(yearTo) : undefined,
          limit: PAGE_SIZE,
          offset: nextOffset,
        };
        const data = await cbs.search(params);
        setTotal(data.total);
        // offset 0 = fresh search (replace); >0 = "load more" (append).
        setResults((prev) =>
          nextOffset === 0 ? data.results : prev.concat(data.results)
        );
        setOffset(nextOffset);
      } catch (err: any) {
        setError(err.message);
      }
      setLoading(false);
    },
    [query, subject, geo, fileType, yearFrom, yearTo, setSearchParams]
  );

  // Load an initial (unfiltered, newest-first) page on mount, and re-run
  // whenever a facet filter changes so the results stay in sync with the UI.
  useEffect(() => {
    runSearch(0);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [subject, geo, fileType, yearFrom, yearTo]);

  const onSubmit = (e: FormEvent) => {
    e.preventDefault();
    runSearch(0);
  };

  const clearFilters = () => {
    setSubject("");
    setGeo("");
    setFileType("");
    setYearFrom("");
    setYearTo("");
  };

  const hasFilters = !!(subject || geo || fileType || yearFrom || yearTo);

  const yearSpan = (r: CbsResult) => {
    if (r.year_start && r.year_end) {
      return r.year_start === r.year_end
        ? String(r.year_start)
        : `${r.year_start}–${r.year_end}`;
    }
    return r.year_start || r.year_end || null;
  };

  return (
    <div className="container mt-3">
      <div className="page-header">
        <h1>{t("cbs.title", 'הלשכה המרכזית לסטטיסטיקה (למ"ס)')}</h1>
        <p className="text-muted" style={{ marginTop: "0.35rem", maxWidth: "48rem" }}>
          {t(
            "cbs.subtitle",
            'אינדקס חיפוש של אתר הלמ"ס (cbs.gov.il): פרסומים, הודעות, לוחות וקבצים להורדה. החיפוש הוא מעל תוכן העמודים; הקבצים עצמם נשמרים באתר הלמ"ס ונפתחים ישירות משם.'
          )}
        </p>
      </div>

      <form onSubmit={onSubmit} className="flex mb-2" role="search">
        <label
          htmlFor="cbs-search"
          className="sr-only"
          style={{ position: "absolute", width: 1, height: 1, overflow: "hidden", clip: "rect(0,0,0,0)" }}
        >
          {t("cbs.placeholder", 'חיפוש בתוכן הלמ"ס')}
        </label>
        <input
          id="cbs-search"
          type="search"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder={t("cbs.placeholder", 'חיפוש בתוכן הלמ"ס — למשל "מדד המחירים לצרכן"')}
          style={{ flex: 1 }}
        />
        <button type="submit" className="btn-primary" disabled={loading}>
          {t("search.search_btn", "חיפוש")}
        </button>
      </form>

      {/* Facet filters */}
      {facets && (
        <div
          className="flex mb-2"
          style={{ flexWrap: "wrap", gap: "0.5rem", alignItems: "center" }}
        >
          <select
            value={subject}
            onChange={(e) => setSubject(e.target.value)}
            aria-label={t("cbs.subject", "נושא")}
            style={{ width: "auto", padding: "0.3rem 0.5rem", fontSize: "0.85rem" }}
          >
            <option value="">{t("cbs.all_subjects", "כל הנושאים")}</option>
            {facets.subjects.map((s) => (
              <option key={s} value={s}>{s}</option>
            ))}
          </select>

          <select
            value={geo}
            onChange={(e) => setGeo(e.target.value)}
            aria-label={t("cbs.geo", "רזולוציה גאוגרפית")}
            style={{ width: "auto", padding: "0.3rem 0.5rem", fontSize: "0.85rem" }}
          >
            <option value="">{t("cbs.all_geo", "כל הרמות הגאוגרפיות")}</option>
            {facets.geo_levels.map((g) => (
              <option key={g} value={g}>{GEO_LABELS[g] || g}</option>
            ))}
          </select>

          <select
            value={fileType}
            onChange={(e) => setFileType(e.target.value)}
            aria-label={t("cbs.file_type", "סוג קובץ")}
            style={{ width: "auto", padding: "0.3rem 0.5rem", fontSize: "0.85rem" }}
          >
            <option value="">{t("cbs.all_file_types", "כל סוגי הקבצים")}</option>
            {facets.file_types.map((f) => (
              <option key={f} value={f}>{f.toUpperCase()}</option>
            ))}
          </select>

          <input
            type="number"
            value={yearFrom}
            onChange={(e) => setYearFrom(e.target.value)}
            placeholder={t("cbs.year_from", "משנה")}
            aria-label={t("cbs.year_from", "משנה")}
            min={facets.year_min ?? undefined}
            max={facets.year_max ?? undefined}
            style={{ width: "6rem", padding: "0.3rem 0.5rem", fontSize: "0.85rem" }}
          />
          <input
            type="number"
            value={yearTo}
            onChange={(e) => setYearTo(e.target.value)}
            placeholder={t("cbs.year_to", "עד שנה")}
            aria-label={t("cbs.year_to", "עד שנה")}
            min={facets.year_min ?? undefined}
            max={facets.year_max ?? undefined}
            style={{ width: "6rem", padding: "0.3rem 0.5rem", fontSize: "0.85rem" }}
          />

          {hasFilters && (
            <button
              type="button"
              className="btn-secondary"
              onClick={clearFilters}
              style={{ fontSize: "0.8rem", padding: "0.3rem 0.6rem" }}
            >
              {t("cbs.clear_filters", "נקה סינון")}
            </button>
          )}
        </div>
      )}

      {error && <div role="alert" className="badge badge-danger mb-2">{error}</div>}

      <div aria-live="polite" aria-atomic="true">
        {loading && offset === 0 && (
          <div className="loading" role="status">{t("common.loading", "טוען…")}</div>
        )}
        {!loading && total === 0 && (
          <div className="empty-state">
            {t("cbs.no_results", "לא נמצאו תוצאות באינדקס הלמ\"ס.")}
          </div>
        )}
        {!loading && total > 0 && (
          <p className="text-sm text-muted mb-2">
            {t("cbs.results_count", { count: total, defaultValue: `${total} תוצאות` })}
          </p>
        )}
      </div>

      <div className="grid grid-2">
        {results.map((r) => {
          const span = yearSpan(r);
          return (
            <article
              key={r.url}
              className="card"
              style={{ borderRight: "4px solid #0ea5e9" }}
            >
              <div className="flex-between mb-1">
                <h2 style={{ fontSize: "1rem", fontWeight: 600, margin: 0 }}>
                  <a
                    href={r.url}
                    target="_blank"
                    rel="noopener noreferrer"
                    style={{ color: "var(--text, inherit)" }}
                  >
                    {(he ? r.title : r.title_en) || r.title || r.title_en || r.url}
                  </a>
                </h2>
                <span
                  style={{
                    display: "inline-block",
                    padding: "0.15rem 0.5rem",
                    borderRadius: "9999px",
                    fontSize: "0.65rem",
                    fontWeight: 600,
                    background: "#e0f2fe",
                    color: "#075985",
                    whiteSpace: "nowrap",
                  }}
                >
                  למ"ס
                </span>
              </div>

              {r.summary && (
                <p
                  className="text-sm text-muted mb-1"
                  style={{ maxHeight: "3.2em", overflow: "hidden" }}
                >
                  {r.summary}
                </p>
              )}

              <div
                className="flex text-sm text-muted"
                style={{ gap: "0.75rem", flexWrap: "wrap" }}
              >
                {r.section && <span>{r.section}</span>}
                {span && <span>{span}</span>}
                {r.geo_levels && r.geo_levels.length > 0 && (
                  <span>{r.geo_levels.map((g) => GEO_LABELS[g] || g).join(", ")}</span>
                )}
              </div>

              {r.subject_tags && r.subject_tags.length > 0 && (
                <div
                  style={{ display: "flex", flexWrap: "wrap", gap: "0.3rem", marginTop: "0.5rem" }}
                >
                  {r.subject_tags.map((s) => (
                    <span
                      key={s}
                      className="badge"
                      style={{ fontSize: "0.7rem", background: "#f1f5f9", color: "#334155" }}
                    >
                      {s}
                    </span>
                  ))}
                </div>
              )}

              {r.file_links && r.file_links.length > 0 && (
                <div style={{ marginTop: "0.6rem" }}>
                  <div style={{ fontSize: "0.8rem", fontWeight: 600, marginBottom: "0.35rem" }}>
                    {t("cbs.files", "קבצים")}:
                  </div>
                  {r.file_links.map((f, idx) => (
                    <div
                      key={`${f.href}-${idx}`}
                      style={{
                        display: "flex",
                        justifyContent: "space-between",
                        alignItems: "center",
                        gap: "0.5rem",
                        padding: "0.35rem 0.55rem",
                        marginBottom: "0.25rem",
                        background: "var(--bg-secondary, #f8f9fa)",
                        borderRadius: "4px",
                        border: "1px solid var(--border, #e2e8f0)",
                      }}
                    >
                      <a
                        href={f.href}
                        target="_blank"
                        rel="noopener noreferrer"
                        style={{ fontSize: "0.82rem", color: "var(--primary)", wordBreak: "break-word" }}
                      >
                        {f.label || f.href.split("/").pop()}
                        {f.ext && (
                          <span
                            className="badge"
                            style={{ marginInlineStart: "0.4rem", fontSize: "0.65rem" }}
                          >
                            {f.ext.toUpperCase()}
                          </span>
                        )}
                      </a>
                      {f.size != null && (
                        <span className="text-sm text-muted" style={{ whiteSpace: "nowrap" }}>
                          {formatBytes(f.size)}
                        </span>
                      )}
                    </div>
                  ))}
                </div>
              )}
            </article>
          );
        })}
      </div>

      {results.length < total && (
        <div style={{ textAlign: "center", marginTop: "1rem" }}>
          <button
            className="btn-secondary"
            onClick={() => runSearch(offset + PAGE_SIZE)}
            disabled={loading}
          >
            {loading ? t("common.loading", "טוען…") : t("cbs.load_more", "טען עוד")}
          </button>
        </div>
      )}
    </div>
  );
}
