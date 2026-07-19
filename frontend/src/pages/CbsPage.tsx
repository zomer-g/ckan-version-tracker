import { useState, useEffect, useCallback, FormEvent } from "react";
import { useSearchParams } from "react-router-dom";
import { useTranslation } from "react-i18next";
import {
  cbs,
  CbsResult,
  CbsFacets,
  CbsSearchParams,
  CbsResolveResponse,
  CbsGazetteerEntry,
} from "../api/client";
import { useAuth } from "../auth/AuthContext";
import CbsAbout from "../components/CbsAbout";
import CbsAnswerCard from "../components/CbsAnswerCard";
import CbsFeedbackButtons from "../components/CbsFeedbackButtons";
import CbsFeatured from "../components/CbsFeatured";
import CbsResultCard from "../components/CbsResultCard";
import { geoLabel, productFormLabel, sectionLabel } from "../utils/cbsLabels";
import {
  seriesQuery,
  historicalVersions,
  YearVersion,
} from "../utils/cbsSeries";

const PAGE_SIZE = 30;

// The two search surfaces + the explainer. "ask" is the natural-language mode
// (a question → POST /api/cbs/resolve → one actionable answer card +
// supporting results); "advanced" is the original keyword+facets interface,
// kept verbatim; "about" is a static how-it-works page (CbsAbout).
type Mode = "ask" | "advanced" | "about";

// Params that only the advanced (keyword/facet) interface produces. Any link
// carrying one of them predates the NL mode — or was shared from the advanced
// tab — so it must open in advanced mode and behave exactly as it always did.
const ADVANCED_PARAMS = [
  "q", "subject", "geo", "section", "file_type", "year_from", "year_to", "sort",
  // Enrichment-layer filters (the ultimate-search-interface plan).
  "product_form", "freq", "source_op", "latest_only", "locality",
];

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

  // Mode selection, in priority order:
  //   1. an explicit ?mode= (shared link from either tab),
  //   2. any legacy keyword/facet param ⇒ advanced, so every link that worked
  //      before the NL mode existed still opens on the exact same interface,
  //   3. otherwise the new NL mode for a fresh visitor.
  const [mode, setMode] = useState<Mode>(() => {
    const m = initial.get("mode");
    if (m === "ask" || m === "advanced" || m === "about") return m;
    return ADVANCED_PARAMS.some((p) => initial.get(p)) ? "advanced" : "ask";
  });

  // ── natural-language mode state ──
  const [askQuery, setAskQuery] = useState(() => initial.get("ask") || "");
  const [answer, setAnswer] = useState<CbsResolveResponse | null>(null);
  const [askLoading, setAskLoading] = useState(false);
  const [askError, setAskError] = useState("");

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
  // Enrichment-layer filters — the user-vocabulary dimensions the benchmark
  // shows people actually express: product form ("שכבה להורדה"), frequency,
  // named source operation, latest-edition-only, and a locality entity.
  const [productForm, setProductForm] = useState(() => initial.get("product_form") || "");
  const [freq, setFreq] = useState(() => initial.get("freq") || "");
  const [sourceOp, setSourceOp] = useState(() => initial.get("source_op") || "");
  const [latestOnly, setLatestOnly] = useState(() => initial.get("latest_only") === "1");
  // Locality (יישוב/רשות) — resolved via the gazetteer autocomplete. There is
  // no per-locality tag on index rows, so the chosen name joins the free-text
  // query (that is how the community itself finds locality data).
  const [locality, setLocality] = useState(() => initial.get("locality") || "");
  const [localitySuggestions, setLocalitySuggestions] = useState<CbsGazetteerEntry[]>([]);

  // Populate the locality datalist as the user types (2+ chars).
  useEffect(() => {
    let cancelled = false;
    const v = locality.trim();
    if (v.length < 2) {
      setLocalitySuggestions([]);
      return;
    }
    cbs.gazetteer(v).then((res) => {
      if (!cancelled) setLocalitySuggestions(res.results);
    }).catch(() => {
      // Non-fatal — the field still works as plain text.
    });
    return () => {
      cancelled = true;
    };
  }, [locality]);
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
        if (productForm) sp.set("product_form", productForm);
        if (freq) sp.set("freq", freq);
        if (sourceOp) sp.set("source_op", sourceOp);
        if (latestOnly) sp.set("latest_only", "1");
        if (locality.trim()) sp.set("locality", locality.trim());
        if (sort !== "relevance") sp.set("sort", sort);
        setSearchParams(sp, { replace: true });
      }
      try {
        // The locality entity joins the free text — index rows carry no
        // per-locality tag, and lexical match on the name is how locality data
        // is actually found.
        const qText = [query.trim(), locality.trim()].filter(Boolean).join(" ");
        const params: CbsSearchParams = {
          q: qText || undefined,
          subject: subject || undefined,
          geo: geo || undefined,
          section: section || undefined,
          file_type: fileType || undefined,
          year_from: yearFrom ? Number(yearFrom) : undefined,
          year_to: yearTo ? Number(yearTo) : undefined,
          product_form: productForm || undefined,
          freq: freq || undefined,
          source_op: sourceOp || undefined,
          latest_only: latestOnly || undefined,
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
    [query, subject, geo, section, fileType, yearFrom, yearTo, productForm,
     freq, sourceOp, latestOnly, locality, sort, setSearchParams]
  );

  // Load an initial (unfiltered, newest-first) page on mount, and re-run
  // whenever a facet filter or the sort order changes so the results stay in
  // sync with the UI. Only in advanced mode — the NL tab drives its own fetch,
  // and firing a blank keyword search behind it would be wasted work.
  useEffect(() => {
    if (mode !== "advanced") return;
    runSearch(0);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [mode, subject, geo, section, fileType, yearFrom, yearTo, productForm,
      freq, sourceOp, latestOnly, sort]);

  const onSubmit = (e: FormEvent) => {
    e.preventDefault();
    runSearch(0);
  };

  // ── natural-language mode ──
  // POST /api/cbs/resolve returns the answer card + its supporting results in
  // one shot. Retrieval there runs on the raw question (no LLM) — measured
  // better than LLM-cleaned keywords on the WhatsApp benchmark — so this is a
  // plain, fast index call.
  const runAsk = useCallback(
    async (question: string) => {
      const qq = question.trim();
      if (!qq) return;
      setAskLoading(true);
      setAskError("");
      setAnswer(null);
      const sp = new URLSearchParams();
      sp.set("mode", "ask");
      sp.set("ask", qq);
      setSearchParams(sp, { replace: true });
      try {
        setAnswer(await cbs.resolve(qq, 10));
      } catch (err: any) {
        setAskError(err?.message || "שגיאה בפתרון השאלה");
      }
      setAskLoading(false);
    },
    [setSearchParams]
  );

  // Answer a question that arrived in the URL (a shared /cbs?mode=ask&ask=… link).
  useEffect(() => {
    if (mode === "ask" && askQuery.trim() && !answer && !askLoading && !askError) {
      runAsk(askQuery);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [mode]);

  const onAskSubmit = (e: FormEvent) => {
    e.preventDefault();
    runAsk(askQuery);
  };

  const switchMode = (next: Mode) => {
    setMode(next);
    const sp = new URLSearchParams();
    sp.set("mode", next);
    // Carry the typed text across tabs so switching doesn't lose the user's work.
    if (next === "advanced" && askQuery.trim() && !query.trim()) setQuery(askQuery);
    if (next === "ask" && query.trim() && !askQuery.trim()) setAskQuery(query);
    setSearchParams(sp, { replace: true });
  };

  const clearFilters = () => {
    setSubject("");
    setGeo("");
    setSection("");
    setFileType("");
    setYearFrom("");
    setYearTo("");
    setProductForm("");
    setFreq("");
    setSourceOp("");
    setLatestOnly(false);
    setLocality("");
  };

  const hasFilters = !!(subject || geo || section || fileType || yearFrom ||
    yearTo || productForm || freq || sourceOp || latestOnly || locality);

  // "עריכה בחיפוש מתקדם" from the NL answer card: map the deterministic parse
  // of the question (the chips) onto the advanced filters, then switch tabs —
  // the bridge that makes the two surfaces one interface.
  const editInAdvanced = useCallback(() => {
    const u = answer?.understood;
    if (u) {
      if (u.geo_level) setGeo(u.geo_level);
      if (u.product_form) setProductForm(u.product_form);
      if (u.source_op) setSourceOp(u.source_op);
      if (u.latest) setLatestOnly(true);
      if (u.years.length > 0) {
        setYearFrom(String(u.years[0]));
        setYearTo(String(u.years[u.years.length - 1]));
      }
      if (u.geo_entity) setLocality(u.geo_entity.name);
    }
    if (askQuery.trim()) setQuery(askQuery);
    setMode("advanced");
    const sp = new URLSearchParams();
    sp.set("mode", "advanced");
    setSearchParams(sp, { replace: true });
  }, [answer, askQuery, setSearchParams]);

  // Featured quick-access cards are shown only on the default (unsearched,
  // unfiltered) view — the URL reflects the last SUBMITTED search, so typing
  // in the box doesn't hide them, submitting does.
  const showFeatured = !hasFilters && !searchParams.get("q");

  // Quick feedback by email: a mailto prefilled with the current question /
  // filters + the shareable URL, so a report arrives with its full context.
  const feedbackHref = (() => {
    const lines = [
      mode === "ask"
        ? `שאלה: ${askQuery.trim() || "—"}`
        : `חיפוש: ${query.trim() || "—"}${locality ? ` | יישוב: ${locality}` : ""}`,
      answer && mode === "ask" ? `סוג תשובה: ${answer.answer_type}` : "",
      `קישור: ${typeof window !== "undefined" ? window.location.href : ""}`,
      "",
      "הפידבק שלי:",
      "",
    ].filter((l) => l !== "");
    const subject = encodeURIComponent('פידבק על חיפוש הלמ"ס ב-OVER');
    const body = encodeURIComponent(lines.join("\n"));
    return `mailto:zomerg@gmail.com?subject=${subject}&body=${body}`;
  })();

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

      {/* Mode tabs. The advanced tab is the original interface, untouched. */}
      <div
        className="flex mb-2"
        style={{ gap: "0.4rem" }}
        role="tablist"
        aria-label={t("cbs.mode", "מצב חיפוש")}
      >
        {([
          ["ask", t("cbs.mode_ask", "שאלה בשפה טבעית")],
          ["advanced", t("cbs.mode_advanced", "חיפוש מתקדם")],
          ["about", t("cbs.mode_about", "איך זה עובד")],
        ] as [Mode, string][]).map(([m, label]) => (
          <button
            key={m}
            type="button"
            role="tab"
            aria-selected={mode === m}
            className={mode === m ? "btn-primary" : "btn-secondary"}
            onClick={() => switchMode(m)}
            style={{ fontSize: "0.85rem", padding: "0.35rem 0.8rem" }}
          >
            {label}
          </button>
        ))}
        {/* Quick feedback — opens the user's mail client with the current
            question/filters + shareable URL prefilled. */}
        <a
          href={feedbackHref}
          className="btn-secondary"
          style={{
            fontSize: "0.8rem",
            padding: "0.35rem 0.7rem",
            marginInlineStart: "auto",
            textDecoration: "none",
          }}
          title={t("cbs.feedback_title", "שלחו לנו במייל מה עבד ומה לא — הקשר החיפוש יצורף אוטומטית")}
        >
          📧 {t("cbs.feedback", "פידבק")}
        </a>
      </div>

      {mode === "ask" && (
        <>
          <form onSubmit={onAskSubmit} className="flex mb-2" role="search">
            <label
              htmlFor="cbs-ask"
              className="sr-only"
              style={{ position: "absolute", width: 1, height: 1, overflow: "hidden", clip: "rect(0,0,0,0)" }}
            >
              {t("cbs.ask_placeholder", 'שאלה על נתוני הלמ"ס')}
            </label>
            <input
              id="cbs-ask"
              type="search"
              value={askQuery}
              onChange={(e) => setAskQuery(e.target.value)}
              placeholder={t(
                "cbs.ask_placeholder",
                'שאלו בשפה חופשית — למשל "איפה אפשר למצוא אחוז חרדים לפי אזור סטטיסטי?"'
              )}
              style={{ flex: 1 }}
            />
            <button type="submit" className="btn-primary" disabled={askLoading}>
              {askLoading ? t("common.loading", "טוען…") : t("cbs.ask_btn", "מצא לי")}
            </button>
          </form>

          {askError && <div role="alert" className="badge badge-danger mb-2">{askError}</div>}

          <div aria-live="polite">
            {answer && <CbsAnswerCard data={answer} onEditInAdvanced={editInAdvanced} />}
          </div>

          {answer && (
            <div className="mb-2" style={{ display: "flex", justifyContent: "flex-end" }}>
              <CbsFeedbackButtons
                query={askQuery}
                mode="ask"
                answerType={answer.answer_type}
                topUrl={answer.primary?.link || answer.primary?.url || null}
              />
            </div>
          )}

          {answer && answer.results.length > 0 && (
            <>
              <p className="text-sm text-muted" style={{ margin: "0 0 0.5rem" }}>
                {t("cbs.supporting", "מקורות נוספים שנמצאו")}:
              </p>
              <div className="grid grid-2">
                {answer.results.map((r) => (
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
            </>
          )}

          {!answer && !askLoading && (
            <CbsFeatured
              records={featured}
              canPin={canPin}
              pinBusy={pinBusy}
              onTogglePin={togglePin}
              history={featuredHistory}
            />
          )}
        </>
      )}

      {mode === "about" && <CbsAbout />}

      {mode === "advanced" && (
        <>
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

          {/* Enrichment-layer filters — rendered only once the backfill has
              populated the facet values, so an empty select never shows. */}
          {facets.product_forms.length > 0 && (
            <select
              value={productForm}
              onChange={(e) => setProductForm(e.target.value)}
              aria-label={t("cbs.product_form", "צורת התוצר")}
              style={{ width: "auto", padding: "0.3rem 0.5rem", fontSize: "0.85rem" }}
            >
              <option value="">{t("cbs.all_product_forms", "כל צורות התוצר")}</option>
              {facets.product_forms.map((p) => (
                <option key={p} value={p}>{productFormLabel(p)}</option>
              ))}
            </select>
          )}

          {facets.freqs.length > 0 && (
            <select
              value={freq}
              onChange={(e) => setFreq(e.target.value)}
              aria-label={t("cbs.freq", "תדירות")}
              style={{ width: "auto", padding: "0.3rem 0.5rem", fontSize: "0.85rem" }}
            >
              <option value="">{t("cbs.all_freqs", "כל התדירויות")}</option>
              {facets.freqs.map((f) => (
                <option key={f} value={f}>{f}</option>
              ))}
            </select>
          )}

          {facets.source_ops.length > 0 && (
            <select
              value={sourceOp}
              onChange={(e) => setSourceOp(e.target.value)}
              aria-label={t("cbs.source_op", "מקור האיסוף")}
              style={{ width: "auto", padding: "0.3rem 0.5rem", fontSize: "0.85rem" }}
            >
              <option value="">{t("cbs.all_sources", "כל מקורות האיסוף")}</option>
              {facets.source_ops.map((s) => (
                <option key={s} value={s}>{s}</option>
              ))}
            </select>
          )}

          {/* Locality entity (gazetteer autocomplete) — joins the free text. */}
          <input
            type="text"
            value={locality}
            onChange={(e) => setLocality(e.target.value)}
            list="cbs-locality-list"
            placeholder={t("cbs.locality", "יישוב / רשות")}
            aria-label={t("cbs.locality", "יישוב / רשות")}
            style={{ width: "9rem", padding: "0.3rem 0.5rem", fontSize: "0.85rem" }}
          />
          <datalist id="cbs-locality-list">
            {localitySuggestions.map((s) => (
              <option key={s.code} value={s.name}>
                {[s.municipal_status, s.subdistrict && `נפת ${s.subdistrict}`]
                  .filter(Boolean)
                  .join(" · ")}
              </option>
            ))}
          </datalist>

          <label
            className="flex"
            style={{ gap: "0.3rem", alignItems: "center", fontSize: "0.82rem", margin: 0 }}
          >
            <input
              type="checkbox"
              checked={latestOnly}
              onChange={(e) => setLatestOnly(e.target.checked)}
              style={{ width: "auto", margin: 0 }}
            />
            {t("cbs.latest_only", "רק המהדורה העדכנית")}
          </label>

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

        {/* Feedback on this keyword search (shown once there are results). */}
        {!loading && total > 0 && (query.trim() || locality.trim()) && (
          <CbsFeedbackButtons
            query={[query.trim(), locality.trim()].filter(Boolean).join(" ")}
            mode="advanced"
            topUrl={results[0]?.url || null}
          />
        )}

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
        </>
      )}
    </div>
  );
}
