import { useState, useEffect, useCallback, FormEvent } from "react";
import { useSearchParams } from "react-router-dom";
import { useTranslation } from "react-i18next";
import {
  cbs,
  CbsResult,
  CbsFacets,
  CbsSearchParams,
} from "../api/client";
import { useAuth } from "../auth/AuthContext";
import CbsFeatured from "../components/CbsFeatured";
import CbsResultCard from "../components/CbsResultCard";
import { geoLabel, sectionLabel } from "../utils/cbsLabels";
import {
  seriesQuery,
  historicalVersions,
  YearVersion,
} from "../utils/cbsSeries";

const PAGE_SIZE = 30;

export default function CbsPage() {
  const { t } = useTranslation();
  const { user } = useAuth();
  const canPin = !!user?.is_admin;

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

  // Admin-pinned quick-access pages. `featured` is the pinned records shown at
  // the top of the default view; `pinnedUrls` is the same set as URLs, used to
  // light up the pin star on any matching search-result card. `pinBusy` holds
  // the URL of an in-flight pin/unpin so its star can show a wait state.
  const [featured, setFeatured] = useState<CbsResult[]>([]);
  const [pinBusy, setPinBusy] = useState<string | null>(null);
  const pinnedUrls = new Set(featured.map((r) => r.url));

  // Historical yearly versions of each pinned page (keyed by its url), detected
  // heuristically from titles — always shown under the featured card.
  const [featuredHistory, setFeaturedHistory] = useState<
    Record<string, YearVersion[]>
  >({});

  useEffect(() => {
    cbs.featured().then((res) => setFeatured(res.results)).catch(() => {
      // Non-fatal — the page still works without the pinned strip.
    });
  }, []);

  // For each pinned page, search its series (title minus year) and derive the
  // prior-year versions. Runs whenever the pinned set changes.
  useEffect(() => {
    let cancelled = false;
    if (featured.length === 0) {
      setFeaturedHistory({});
      return;
    }
    Promise.all(
      featured.map(async (rec) => {
        try {
          const res = await cbs.search({
            q: seriesQuery(rec),
            sort: "chrono",
            limit: 100,
          });
          return [rec.url, historicalVersions(rec, res.results)] as const;
        } catch {
          return [rec.url, [] as YearVersion[]] as const;
        }
      })
    ).then((entries) => {
      if (!cancelled) setFeaturedHistory(Object.fromEntries(entries));
    });
    return () => {
      cancelled = true;
    };
  }, [featured]);

  const togglePin = useCallback(
    async (record: CbsResult) => {
      setPinBusy(record.url);
      const isPinned = featured.some((r) => r.url === record.url);
      try {
        const res = isPinned
          ? await cbs.unpin(record.url)
          : await cbs.pin(record.url);
        setFeatured(res.results);
      } catch (e: any) {
        setError(e?.message || "שגיאה בעדכון המועדפים");
      } finally {
        setPinBusy(null);
      }
    },
    [featured]
  );

  // Active facet filters.
  const [subject, setSubject] = useState(() => initial.get("subject") || "");
  const [geo, setGeo] = useState(() => initial.get("geo") || "");
  const [section, setSection] = useState(() => initial.get("section") || "");
  const [fileType, setFileType] = useState(() => initial.get("file_type") || "");
  const [yearFrom, setYearFrom] = useState(() => initial.get("year_from") || "");
  const [yearTo, setYearTo] = useState(() => initial.get("year_to") || "");
  // Result ordering: relevance (default) or chronological (newest data year).
  const [sort, setSort] = useState<"relevance" | "chrono">(
    () => (initial.get("sort") === "chrono" ? "chrono" : "relevance")
  );

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
        if (section) sp.set("section", section);
        if (fileType) sp.set("file_type", fileType);
        if (yearFrom) sp.set("year_from", yearFrom);
        if (yearTo) sp.set("year_to", yearTo);
        if (sort !== "relevance") sp.set("sort", sort);
        setSearchParams(sp, { replace: true });
      }
      try {
        const params: CbsSearchParams = {
          q: query.trim() || undefined,
          subject: subject || undefined,
          geo: geo || undefined,
          section: section || undefined,
          file_type: fileType || undefined,
          year_from: yearFrom ? Number(yearFrom) : undefined,
          year_to: yearTo ? Number(yearTo) : undefined,
          sort,
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
    [query, subject, geo, section, fileType, yearFrom, yearTo, sort, setSearchParams]
  );

  // Load an initial (unfiltered, newest-first) page on mount, and re-run
  // whenever a facet filter or the sort order changes so the results stay in
  // sync with the UI.
  useEffect(() => {
    runSearch(0);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [subject, geo, section, fileType, yearFrom, yearTo, sort]);

  const onSubmit = (e: FormEvent) => {
    e.preventDefault();
    runSearch(0);
  };

  const clearFilters = () => {
    setSubject("");
    setGeo("");
    setSection("");
    setFileType("");
    setYearFrom("");
    setYearTo("");
  };

  const hasFilters = !!(subject || geo || section || fileType || yearFrom || yearTo);

  // Featured quick-access cards are shown only on the default (unsearched,
  // unfiltered) view — the URL reflects the last SUBMITTED search, so typing
  // in the box doesn't hide them, submitting does.
  const showFeatured = !hasFilters && !searchParams.get("q");

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
              <option key={g} value={g}>{geoLabel(g)}</option>
            ))}
          </select>

          <select
            value={section}
            onChange={(e) => setSection(e.target.value)}
            aria-label={t("cbs.section", "סוג עמוד")}
            style={{ width: "auto", padding: "0.3rem 0.5rem", fontSize: "0.85rem" }}
          >
            <option value="">{t("cbs.all_sections", "כל סוגי העמודים")}</option>
            {facets.sections.map((s) => (
              <option key={s} value={s}>{sectionLabel(s)}</option>
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

      {showFeatured && (
        <CbsFeatured
          records={featured}
          canPin={canPin}
          pinBusy={pinBusy}
          onTogglePin={togglePin}
          history={featuredHistory}
        />
      )}

      {error && <div role="alert" className="badge badge-danger mb-2">{error}</div>}

      <div
        className="flex-between mb-2"
        style={{ gap: "0.75rem", flexWrap: "wrap" }}
      >
        <div aria-live="polite" aria-atomic="true">
          {loading && offset === 0 && (
            <span className="loading" role="status">{t("common.loading", "טוען…")}</span>
          )}
          {!loading && total === 0 && (
            <span className="text-sm text-muted">
              {t("cbs.no_results", "לא נמצאו תוצאות באינדקס הלמ\"ס.")}
            </span>
          )}
          {!loading && total > 0 && (
            <span className="text-sm text-muted">
              {t("cbs.results_count", { count: total, defaultValue: `${total} תוצאות` })}
            </span>
          )}
        </div>

        <label className="flex" style={{ gap: "0.35rem", fontSize: "0.82rem", margin: 0 }}>
          <span className="text-muted" style={{ fontWeight: 400 }}>
            {t("cbs.sort_by", "מיון")}:
          </span>
          <select
            value={sort}
            onChange={(e) => setSort(e.target.value as "relevance" | "chrono")}
            aria-label={t("cbs.sort_by", "מיון")}
            style={{ width: "auto", padding: "0.25rem 0.5rem", fontSize: "0.82rem" }}
          >
            <option value="relevance">{t("cbs.sort_relevance", "רלוונטיות")}</option>
            <option value="chrono">{t("cbs.sort_chrono", "כרונולוגי (שנה יורדת)")}</option>
          </select>
        </label>
      </div>

      <div className="grid grid-2">
        {results.map((r) => (
          <CbsResultCard
            key={r.url}
            record={r}
            canPin={canPin}
            pinned={pinnedUrls.has(r.url)}
            busy={pinBusy === r.url}
            onTogglePin={togglePin}
          />
        ))}
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
