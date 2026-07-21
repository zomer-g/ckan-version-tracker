import { useState, useEffect, FormEvent } from "react";
import { useTranslation } from "react-i18next";
import { ckan, datasets as datasetsApi, govil, idf, health, registries, avodata, munidata, mevaker, hatzav, mankal, jda, eden, knesset, GovIlValidation } from "../api/client";
import { useAuth } from "../auth/AuthContext";
import AdminDatasetActions from "../components/AdminDatasetActions";
// idf.il section pattern lives in utils/idfPattern.ts — single
// source of truth shared with HomePage so we never let one accept
// URLs the other rejects.
import { IDF_PATTERN } from "../utils/idfPattern";
// practitioners.health.gov.il per-registry URL pattern. Mirror of
// HEALTH_PRACTITIONERS_RE in app/api/health.py.
import { HEALTH_PRACTITIONERS_PATTERN } from "../utils/healthPattern";
// registries.health.gov.il per-registry URL pattern. Mirror of the
// catalog in app/api/registries.py.
import { REGISTRIES_PATTERN } from "../utils/registriesPattern";
// avodata.labor.gov.il occupations-index URL pattern. Mirror of
// AVODATA_OCCUPATIONS_RE in app/api/avodata.py.
import { AVODATA_OCCUPATIONS_PATTERN } from "../utils/avodataPattern";
// municipal-data.org per-metric URL pattern. Mirror of _parse_munidata_url
// in app/api/munidata.py.
import { MUNIDATA_METRIC_PATTERN } from "../utils/munidataPattern";
// mevaker.gov.il reports-index URL pattern. Mirror of MEVAKER_SUBJECTS_RE
// in app/api/mevaker.py.
import { MEVAKER_SUBJECTS_PATTERN } from "../utils/mevakerPattern";
// geo.mot.gov.il (חצב) portal URL pattern. Mirror of HATZAV_ROOT_RE in
// app/api/hatzav.py.
import { HATZAV_PATTERN } from "../utils/hatzavPattern";
// apps.education.gov.il/Mankal (חוזרי מנכ"ל) portal URL pattern. Mirror of
// MANKAL_INDEX_PATHS in app/api/mankal.py.
import { MANKAL_PATTERN } from "../utils/mankalPattern";
// jda.gov.il (הרשות לפיתוח ירושלים) tenders-portal URL pattern. Mirror of
// corpus_of in app/api/jda.py.
import { JDA_PATTERN } from "../utils/jdaPattern";
// jeden.co.il (חברת עדן / Eden) tenders + committee-decisions portal URL
// pattern. Host-only; the corpus comes from a ?category= marker and the
// backend /api/eden/validate is authoritative.
import { EDEN_PATTERN } from "../utils/edenPattern";
// knesset.gov.il committee-protocols URL pattern (KNS_Committee ODATA query);
// backend /api/knesset/validate is authoritative on the committee scope.
import { KNESSET_PATTERN } from "../utils/knessetPattern";

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

/** Detect gov.il collector URLs */
const GOV_IL_PATTERN = /^https?:\/\/(www\.)?gov\.il\/he\/(departments?\/dynamiccollectors?|collectors?|pages)\/([^/?#]+)/i;

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
  const [targetResourceId, setTargetResourceId] = useState<string | null>(null);

  // Gov.il scraper result
  const [govIlResult, setGovIlResult] = useState<GovIlValidation | null>(null);
  const [govIlTracked, setGovIlTracked] = useState<"tracked" | "pending" | null>(null);
  const [govIlTracking, setGovIlTracking] = useState(false);
  const [showGovIlInterval, setShowGovIlInterval] = useState(false);

  // IDF scraper result — shares the gov.il flow because the backend
  // accepts both URL families on the same /datasets endpoint (which
  // parser wins is decided server-side).
  const [idfResult, setIdfResult] = useState<GovIlValidation | null>(null);
  const [idfTracked, setIdfTracked] = useState<"tracked" | "pending" | null>(null);
  const [idfTracking, setIdfTracking] = useState(false);
  const [showIdfInterval, setShowIdfInterval] = useState(false);

  // practitioners.health.gov.il scraper result — same flow as IDF.
  const [healthResult, setHealthResult] = useState<GovIlValidation | null>(null);
  const [healthTracked, setHealthTracked] = useState<"tracked" | "pending" | null>(null);
  const [healthTracking, setHealthTracking] = useState(false);
  const [showHealthInterval, setShowHealthInterval] = useState(false);
  // registries.health.gov.il scraper result — same flow.
  const [registriesResult, setRegistriesResult] = useState<GovIlValidation | null>(null);
  const [registriesTracked, setRegistriesTracked] = useState<"tracked" | "pending" | null>(null);
  const [registriesTracking, setRegistriesTracking] = useState(false);
  const [showRegistriesInterval, setShowRegistriesInterval] = useState(false);

  // avodata.labor.gov.il scraper result — same flow.
  const [avodataResult, setAvodataResult] = useState<GovIlValidation | null>(null);
  const [avodataTracked, setAvodataTracked] = useState<"tracked" | "pending" | null>(null);
  const [avodataTracking, setAvodataTracking] = useState(false);
  const [showAvodataInterval, setShowAvodataInterval] = useState(false);
  // municipal-data.org scraper result — same flow.
  const [munidataResult, setMunidataResult] = useState<GovIlValidation | null>(null);
  const [munidataTracked, setMunidataTracked] = useState<"tracked" | "pending" | null>(null);
  const [munidataTracking, setMunidataTracking] = useState(false);
  const [showMunidataInterval, setShowMunidataInterval] = useState(false);
  // mevaker.gov.il scraper result — same flow.
  const [mevakerResult, setMevakerResult] = useState<GovIlValidation | null>(null);
  const [mevakerTracked, setMevakerTracked] = useState<"tracked" | "pending" | null>(null);
  const [mevakerTracking, setMevakerTracking] = useState(false);
  const [showMevakerInterval, setShowMevakerInterval] = useState(false);
  // geo.mot.gov.il (חצב) scraper result — same flow.
  const [hatzavResult, setHatzavResult] = useState<GovIlValidation | null>(null);
  const [hatzavTracked, setHatzavTracked] = useState<"tracked" | "pending" | null>(null);
  const [hatzavTracking, setHatzavTracking] = useState(false);
  const [showHatzavInterval, setShowHatzavInterval] = useState(false);
  // apps.education.gov.il/Mankal (חוזרי מנכ"ל) scraper result — same flow.
  const [mankalResult, setMankalResult] = useState<GovIlValidation | null>(null);
  const [mankalTracked, setMankalTracked] = useState<"tracked" | "pending" | null>(null);
  const [mankalTracking, setMankalTracking] = useState(false);
  const [showMankalInterval, setShowMankalInterval] = useState(false);
  // jda.gov.il (הרשות לפיתוח ירושלים) scraper result — same flow.
  const [jdaResult, setJdaResult] = useState<GovIlValidation | null>(null);
  const [jdaTracked, setJdaTracked] = useState<"tracked" | "pending" | null>(null);
  const [jdaTracking, setJdaTracking] = useState(false);
  const [showJdaInterval, setShowJdaInterval] = useState(false);
  // jeden.co.il (חברת עדן / Eden) scraper results. The two corpora
  // (מכרזים / החלטות ועדת מכרזים) share ONE page, so a bare URL can't be
  // validated — we probe both ?category= variants and show one card per
  // valid corpus, each trackable independently. The track state is keyed
  // by page_type so the two cards don't share a button.
  const [edenResults, setEdenResults] = useState<GovIlValidation[]>([]);
  const [edenTracked, setEdenTracked] = useState<Record<string, "tracked" | "pending">>({});
  const [edenTracking, setEdenTracking] = useState<Record<string, boolean>>({});
  const [showEdenInterval, setShowEdenInterval] = useState<Record<string, boolean>>({});
  // knesset.gov.il committee-protocols scraper result — same flow as avodata.
  // One committee (CategoryID / Id scope) per pasted URL.
  const [knessetResult, setKnessetResult] = useState<GovIlValidation | null>(null);
  const [knessetTracked, setKnessetTracked] = useState<"tracked" | "pending" | null>(null);
  const [knessetTracking, setKnessetTracking] = useState(false);
  const [showKnessetInterval, setShowKnessetInterval] = useState(false);

  // Admin-only: ckan_id → tracked dataset id (local UUID), so admin actions
  // (poll/delete) can be rendered inline on results that are already tracked.
  const [trackedByCkanId, setTrackedByCkanId] = useState<Map<string, { id: string; title: string }>>(new Map());

  useEffect(() => {
    if (!isAdmin) return;
    datasetsApi
      .list()
      .then((rows) => {
        const m = new Map<string, { id: string; title: string }>();
        for (const d of rows) {
          if (d.ckan_id) m.set(d.ckan_id, { id: d.id, title: d.title });
        }
        setTrackedByCkanId(m);
      })
      .catch(() => {
        // Non-fatal — admin actions just won't render on this session.
      });
  }, [isAdmin]);

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

  const detectIdfUrl = (input: string): boolean => {
    return IDF_PATTERN.test(input.trim());
  };

  const detectHealthUrl = (input: string): boolean => {
    return HEALTH_PRACTITIONERS_PATTERN.test(input.trim());
  };

  const detectRegistriesUrl = (input: string): boolean => {
    return REGISTRIES_PATTERN.test(input.trim());
  };

  const detectMevakerUrl = (input: string): boolean => {
    return MEVAKER_SUBJECTS_PATTERN.test(input.trim());
  };

  const detectHatzavUrl = (input: string): boolean => {
    return HATZAV_PATTERN.test(input.trim());
  };

  const detectAvodataUrl = (input: string): boolean => {
    return AVODATA_OCCUPATIONS_PATTERN.test(input.trim());
  };

  const detectMunidataUrl = (input: string): boolean => {
    return MUNIDATA_METRIC_PATTERN.test(input.trim());
  };

  const detectMankalUrl = (input: string): boolean => {
    return MANKAL_PATTERN.test(input.trim());
  };

  const detectJdaUrl = (input: string): boolean => {
    return JDA_PATTERN.test(input.trim());
  };

  const detectEdenUrl = (input: string): boolean => {
    return EDEN_PATTERN.test(input.trim());
  };

  const detectKnessetUrl = (input: string): boolean => {
    return KNESSET_PATTERN.test(input.trim());
  };

  const search = async (e?: FormEvent) => {
    e?.preventDefault();
    setLoading(true);
    setError("");
    setTargetResourceId(null);
    setGovIlResult(null);
    setGovIlTracked(null);
    setShowGovIlInterval(false);
    setIdfResult(null);
    setIdfTracked(null);
    setShowIdfInterval(false);
    setHealthResult(null);
    setHealthTracked(null);
    setShowHealthInterval(false);
    setRegistriesResult(null);
    setRegistriesTracked(null);
    setShowRegistriesInterval(false);
    setAvodataResult(null);
    setAvodataTracked(null);
    setShowAvodataInterval(false);
    setMunidataResult(null);
    setMunidataTracked(null);
    setShowMunidataInterval(false);
    setMevakerResult(null);
    setMevakerTracked(null);
    setShowMevakerInterval(false);
    setHatzavResult(null);
    setHatzavTracked(null);
    setShowHatzavInterval(false);
    setMankalResult(null);
    setMankalTracked(null);
    setShowMankalInterval(false);
    setJdaResult(null);
    setJdaTracked(null);
    setShowJdaInterval(false);
    setEdenResults([]);
    setEdenTracked({});
    setShowEdenInterval({});
    setKnessetResult(null);
    setKnessetTracked(null);
    setShowKnessetInterval(false);
    try {
      // 1. Check for gov.il collector URL
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

      // 1b. Check for idf.il Military-Prosecution URL
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

      // 1c. Check for practitioners.health.gov.il per-registry URL.
      if (detectHealthUrl(query)) {
        const validation = await health.validate(query.trim());
        if (validation.valid) {
          setHealthResult(validation);
          setResults([]);
          setCount(0);
        } else {
          setError(validation.error || "Invalid practitioners.health.gov.il URL");
        }
        setLoading(false);
        return;
      }

      // 1c-registries. Check for registries.health.gov.il per-registry URL.
      if (detectRegistriesUrl(query)) {
        const validation = await registries.validate(query.trim());
        if (validation.valid) {
          setRegistriesResult(validation);
          setResults([]);
          setCount(0);
        } else {
          setError(validation.error || "Invalid registries.health.gov.il URL");
        }
        setLoading(false);
        return;
      }

      // 1d. Check for avodata.labor.gov.il per-scope URL.
      if (detectAvodataUrl(query)) {
        const validation = await avodata.validate(query.trim());
        if (validation.valid) {
          setAvodataResult(validation);
          setResults([]);
          setCount(0);
        } else {
          setError(validation.error || "Invalid avodata.labor.gov.il URL");
        }
        setLoading(false);
        return;
      }

      // 1d-2. Check for municipal-data.org per-metric URL.
      if (detectMunidataUrl(query)) {
        const validation = await munidata.validate(query.trim());
        if (validation.valid) {
          setMunidataResult(validation);
          setResults([]);
          setCount(0);
        } else {
          setError(validation.error || "Invalid municipal-data.org URL");
        }
        setLoading(false);
        return;
      }

      // 1e. Check for mevaker.gov.il reports-index URL.
      if (detectMevakerUrl(query)) {
        const validation = await mevaker.validate(query.trim());
        if (validation.valid) {
          setMevakerResult(validation);
          setResults([]);
          setCount(0);
        } else {
          setError(validation.error || "Invalid mevaker.gov.il URL");
        }
        setLoading(false);
        return;
      }

      // 1f. Check for geo.mot.gov.il (חצב) portal URL.
      if (detectHatzavUrl(query)) {
        const validation = await hatzav.validate(query.trim());
        if (validation.valid) {
          setHatzavResult(validation);
          setResults([]);
          setCount(0);
        } else {
          setError(validation.error || "Invalid geo.mot.gov.il URL");
        }
        setLoading(false);
        return;
      }

      // 1g. Check for apps.education.gov.il/Mankal (חוזרי מנכ"ל) portal URL.
      if (detectMankalUrl(query)) {
        const validation = await mankal.validate(query.trim());
        if (validation.valid) {
          setMankalResult(validation);
          setResults([]);
          setCount(0);
        } else {
          setError(validation.error || "Invalid apps.education.gov.il URL");
        }
        setLoading(false);
        return;
      }

      // 1h. Check for jda.gov.il (הרשות לפיתוח ירושלים) tenders-portal URL.
      if (detectJdaUrl(query)) {
        const validation = await jda.validate(query.trim());
        if (validation.valid) {
          setJdaResult(validation);
          setResults([]);
          setCount(0);
        } else {
          setError(validation.error || "Invalid jda.gov.il URL");
        }
        setLoading(false);
        return;
      }

      // 1i. Check for jeden.co.il (חברת עדן / Eden) URL. The two corpora
      // (מכרזים / החלטות ועדת מכרזים) share ONE page, so we can't validate
      // the raw pasted URL — instead probe BOTH ?category= variants off the
      // site origin and show one card per valid corpus.
      if (detectEdenUrl(query)) {
        let origin = "https://jeden.co.il";
        try { origin = new URL(query.trim()).origin; } catch {}
        const [tenders, decisions] = await Promise.all([
          eden.validate(`${origin}/?category=tenders`),
          eden.validate(`${origin}/?category=decisions`),
        ]);
        const valid = [tenders, decisions].filter((r) => r.valid);
        if (valid.length > 0) {
          setEdenResults(valid);
          setResults([]);
          setCount(0);
        } else {
          setError("כתובת jeden.co.il לא תקינה");
        }
        setLoading(false);
        return;
      }

      // 1j. Check for a knesset.gov.il committee (KNS_Committee ODATA) URL.
      if (detectKnessetUrl(query)) {
        const validation = await knesset.validate(query.trim());
        if (validation.valid) {
          setKnessetResult(validation);
          setResults([]);
          setCount(0);
        } else {
          setError(validation.error || "כתובת knesset.gov.il לא תקינה");
        }
        setLoading(false);
        return;
      }

      // 2. Check for data.gov.il URL
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

  const trackGovIlDataset = async (interval: number) => {
    if (!govIlResult?.url || !govIlResult?.title) return;
    setShowGovIlInterval(false);
    setGovIlTracking(true);
    try {
      await datasetsApi.trackScraper(govIlResult.url, govIlResult.title, interval);
      setGovIlTracked(isAdmin ? "tracked" : "pending");
    } catch (err: any) {
      if (err.message?.includes("already tracked")) {
        setGovIlTracked("tracked");
      } else {
        setError(err.message);
      }
    }
    setGovIlTracking(false);
  };

  // trackScraper on the backend accepts both gov.il and idf.il URLs —
  // which parser wins is decided server-side. No frontend split needed
  // beyond pointing the call at the IDF result.
  const trackIdfDataset = async (interval: number) => {
    if (!idfResult?.url || !idfResult?.title) return;
    setShowIdfInterval(false);
    setIdfTracking(true);
    try {
      await datasetsApi.trackScraper(idfResult.url, idfResult.title, interval);
      setIdfTracked(isAdmin ? "tracked" : "pending");
    } catch (err: any) {
      if (err.message?.includes("already tracked")) {
        setIdfTracked("tracked");
      } else {
        setError(err.message);
      }
    }
    setIdfTracking(false);
  };

  // Same pattern as IDF — backend's /datasets endpoint dispatches by
  // URL host (parser order: gov.il → idf.il → practitioners.health.gov.il).
  const trackHealthDataset = async (interval: number) => {
    if (!healthResult?.url || !healthResult?.title) return;
    setShowHealthInterval(false);
    setHealthTracking(true);
    try {
      await datasetsApi.trackScraper(healthResult.url, healthResult.title, interval);
      setHealthTracked(isAdmin ? "tracked" : "pending");
    } catch (err: any) {
      if (err.message?.includes("already tracked")) {
        setHealthTracked("tracked");
      } else {
        setError(err.message);
      }
    }
    setHealthTracking(false);
  };

  // Same pattern — backend's /datasets endpoint dispatches by URL host.
  const trackRegistriesDataset = async (interval: number) => {
    if (!registriesResult?.url || !registriesResult?.title) return;
    setShowRegistriesInterval(false);
    setRegistriesTracking(true);
    try {
      await datasetsApi.trackScraper(registriesResult.url, registriesResult.title, interval);
      setRegistriesTracked(isAdmin ? "tracked" : "pending");
    } catch (err: any) {
      if (err.message?.includes("already tracked")) {
        setRegistriesTracked("tracked");
      } else {
        setError(err.message);
      }
    }
    setRegistriesTracking(false);
  };

  const trackAvodataDataset = async (interval: number) => {
    if (!avodataResult?.url || !avodataResult?.title) return;
    setShowAvodataInterval(false);
    setAvodataTracking(true);
    try {
      await datasetsApi.trackScraper(avodataResult.url, avodataResult.title, interval);
      setAvodataTracked(isAdmin ? "tracked" : "pending");
    } catch (err: any) {
      if (err.message?.includes("already tracked")) {
        setAvodataTracked("tracked");
      } else {
        setError(err.message);
      }
    }
    setAvodataTracking(false);
  };

  const trackMunidataDataset = async (interval: number) => {
    if (!munidataResult?.url || !munidataResult?.title) return;
    setShowMunidataInterval(false);
    setMunidataTracking(true);
    try {
      await datasetsApi.trackScraper(munidataResult.url, munidataResult.title, interval);
      setMunidataTracked(isAdmin ? "tracked" : "pending");
    } catch (err: any) {
      if (err.message?.includes("already tracked")) {
        setMunidataTracked("tracked");
      } else {
        setError(err.message);
      }
    }
    setMunidataTracking(false);
  };

  const trackKnessetDataset = async (interval: number) => {
    if (!knessetResult?.url || !knessetResult?.title) return;
    setShowKnessetInterval(false);
    setKnessetTracking(true);
    try {
      await datasetsApi.trackScraper(knessetResult.url, knessetResult.title, interval);
      setKnessetTracked(isAdmin ? "tracked" : "pending");
    } catch (err: any) {
      if (err.message?.includes("already tracked")) {
        setKnessetTracked("tracked");
      } else {
        setError(err.message);
      }
    }
    setKnessetTracking(false);
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

  /** Render track button for gov.il scraper dataset */
  const renderGovIlTrackButton = () => {
    if (govIlTracked === "tracked") {
      return <span className="badge badge-success" role="status">{t("search.tracking")}</span>;
    }
    if (govIlTracked === "pending") {
      return (
        <span className="badge badge-success" role="status" style={{ background: "#22c55e", color: "#fff" }}>
          {t("search.request_sent", "\u05D4\u05D1\u05E7\u05E9\u05D4 \u05E0\u05E9\u05DC\u05D7\u05D4 \u2014 \u05DE\u05DE\u05EA\u05D9\u05DF \u05DC\u05D0\u05D9\u05E9\u05D5\u05E8")}
        </span>
      );
    }

    return (
      <div style={{ display: "flex", gap: "0.3rem", alignItems: "center" }}>
        {showGovIlInterval && (
          <select
            defaultValue={604800}
            onChange={(e) => trackGovIlDataset(Number(e.target.value))}
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
          onClick={() => setShowGovIlInterval(!showGovIlInterval)}
          disabled={govIlTracking}
          style={{ fontSize: "0.8rem", padding: "0.25rem 0.6rem" }}
        >
          {govIlTracking ? t("common.loading") : t("search.track_btn")}
        </button>
      </div>
    );
  };

  // Same shape as renderGovIlTrackButton, bound to the avodata state.
  const renderAvodataTrackButton = () => {
    if (avodataTracked === "tracked") {
      return <span className="badge badge-success" role="status">{t("search.tracking")}</span>;
    }
    if (avodataTracked === "pending") {
      return (
        <span className="badge badge-success" role="status" style={{ background: "#22c55e", color: "#fff" }}>
          {t("search.request_sent", "הבקשה נשלחה — ממתין לאישור")}
        </span>
      );
    }
    return (
      <div style={{ display: "flex", gap: "0.3rem", alignItems: "center" }}>
        {showAvodataInterval && (
          <select
            defaultValue={604800}
            onChange={(e) => trackAvodataDataset(Number(e.target.value))}
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
          onClick={() => setShowAvodataInterval(!showAvodataInterval)}
          disabled={avodataTracking}
          style={{ fontSize: "0.8rem", padding: "0.25rem 0.6rem" }}
        >
          {avodataTracking ? t("common.loading") : t("search.track_btn")}
        </button>
      </div>
    );
  };

  // Same shape as renderAvodataTrackButton, bound to the munidata state.
  // Defaults the interval to monthly (2592000s) — municipal-data.org
  // refreshes its dashboard infrequently.
  const renderMunidataTrackButton = () => {
    if (munidataTracked === "tracked") {
      return <span className="badge badge-success" role="status">{t("search.tracking")}</span>;
    }
    if (munidataTracked === "pending") {
      return (
        <span className="badge badge-success" role="status" style={{ background: "#22c55e", color: "#fff" }}>
          {t("search.request_sent", "הבקשה נשלחה — ממתין לאישור")}
        </span>
      );
    }
    return (
      <div style={{ display: "flex", gap: "0.3rem", alignItems: "center" }}>
        {showMunidataInterval && (
          <select
            defaultValue={2592000}
            onChange={(e) => trackMunidataDataset(Number(e.target.value))}
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
          onClick={() => setShowMunidataInterval(!showMunidataInterval)}
          disabled={munidataTracking}
          style={{ fontSize: "0.8rem", padding: "0.25rem 0.6rem" }}
        >
          {munidataTracking ? t("common.loading") : t("search.track_btn")}
        </button>
      </div>
    );
  };

  // Same shape as renderAvodataTrackButton, bound to the knesset state.
  const renderKnessetTrackButton = () => {
    if (knessetTracked === "tracked") {
      return <span className="badge badge-success" role="status">{t("search.tracking")}</span>;
    }
    if (knessetTracked === "pending") {
      return (
        <span className="badge badge-success" role="status" style={{ background: "#22c55e", color: "#fff" }}>
          {t("search.request_sent", "הבקשה נשלחה — ממתין לאישור")}
        </span>
      );
    }
    return (
      <div style={{ display: "flex", gap: "0.3rem", alignItems: "center" }}>
        {showKnessetInterval && (
          <select
            defaultValue={604800}
            onChange={(e) => trackKnessetDataset(Number(e.target.value))}
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
          onClick={() => setShowKnessetInterval(!showKnessetInterval)}
          disabled={knessetTracking}
          style={{ fontSize: "0.8rem", padding: "0.25rem 0.6rem" }}
        >
          {knessetTracking ? t("common.loading") : t("search.track_btn")}
        </button>
      </div>
    );
  };

  const trackMevakerDataset = async (interval: number) => {
    if (!mevakerResult?.url || !mevakerResult?.title) return;
    setShowMevakerInterval(false);
    setMevakerTracking(true);
    try {
      await datasetsApi.trackScraper(mevakerResult.url, mevakerResult.title, interval);
      setMevakerTracked(isAdmin ? "tracked" : "pending");
    } catch (err: any) {
      if (err.message?.includes("already tracked")) {
        setMevakerTracked("tracked");
      } else {
        setError(err.message);
      }
    }
    setMevakerTracking(false);
  };

  // Same shape as renderAvodataTrackButton, bound to the mevaker state.
  const renderMevakerTrackButton = () => {
    if (mevakerTracked === "tracked") {
      return <span className="badge badge-success" role="status">{t("search.tracking")}</span>;
    }
    if (mevakerTracked === "pending") {
      return (
        <span className="badge badge-success" role="status" style={{ background: "#22c55e", color: "#fff" }}>
          {t("search.request_sent", "הבקשה נשלחה — ממתין לאישור")}
        </span>
      );
    }
    return (
      <div style={{ display: "flex", gap: "0.3rem", alignItems: "center" }}>
        {showMevakerInterval && (
          <select
            defaultValue={604800}
            onChange={(e) => trackMevakerDataset(Number(e.target.value))}
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
          onClick={() => setShowMevakerInterval(!showMevakerInterval)}
          disabled={mevakerTracking}
          style={{ fontSize: "0.8rem", padding: "0.25rem 0.6rem" }}
        >
          {mevakerTracking ? t("common.loading") : t("search.track_btn")}
        </button>
      </div>
    );
  };

  const trackHatzavDataset = async (interval: number) => {
    if (!hatzavResult?.url || !hatzavResult?.title) return;
    setShowHatzavInterval(false);
    setHatzavTracking(true);
    try {
      await datasetsApi.trackScraper(hatzavResult.url, hatzavResult.title, interval);
      setHatzavTracked(isAdmin ? "tracked" : "pending");
    } catch (err: any) {
      if (err.message?.includes("already tracked")) {
        setHatzavTracked("tracked");
      } else {
        setError(err.message);
      }
    }
    setHatzavTracking(false);
  };

  // Same shape as renderMevakerTrackButton, bound to the hatzav state.
  const renderHatzavTrackButton = () => {
    if (hatzavTracked === "tracked") {
      return <span className="badge badge-success" role="status">{t("search.tracking")}</span>;
    }
    if (hatzavTracked === "pending") {
      return (
        <span className="badge badge-success" role="status" style={{ background: "#22c55e", color: "#fff" }}>
          {t("search.request_sent", "הבקשה נשלחה — ממתין לאישור")}
        </span>
      );
    }
    return (
      <div style={{ display: "flex", gap: "0.3rem", alignItems: "center" }}>
        {showHatzavInterval && (
          <select
            defaultValue={604800}
            onChange={(e) => trackHatzavDataset(Number(e.target.value))}
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
          onClick={() => setShowHatzavInterval(!showHatzavInterval)}
          disabled={hatzavTracking}
          style={{ fontSize: "0.8rem", padding: "0.25rem 0.6rem" }}
        >
          {hatzavTracking ? t("common.loading") : t("search.track_btn")}
        </button>
      </div>
    );
  };

  const trackMankalDataset = async (interval: number) => {
    if (!mankalResult?.url || !mankalResult?.title) return;
    setShowMankalInterval(false);
    setMankalTracking(true);
    try {
      await datasetsApi.trackScraper(mankalResult.url, mankalResult.title, interval);
      setMankalTracked(isAdmin ? "tracked" : "pending");
    } catch (err: any) {
      if (err.message?.includes("already tracked")) {
        setMankalTracked("tracked");
      } else {
        setError(err.message);
      }
    }
    setMankalTracking(false);
  };

  // Same shape as renderHatzavTrackButton, bound to the mankal state.
  const renderMankalTrackButton = () => {
    if (mankalTracked === "tracked") {
      return <span className="badge badge-success" role="status">{t("search.tracking")}</span>;
    }
    if (mankalTracked === "pending") {
      return (
        <span className="badge badge-success" role="status" style={{ background: "#22c55e", color: "#fff" }}>
          {t("search.request_sent", "הבקשה נשלחה — ממתין לאישור")}
        </span>
      );
    }
    return (
      <div style={{ display: "flex", gap: "0.3rem", alignItems: "center" }}>
        {showMankalInterval && (
          <select
            defaultValue={604800}
            onChange={(e) => trackMankalDataset(Number(e.target.value))}
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
          onClick={() => setShowMankalInterval(!showMankalInterval)}
          disabled={mankalTracking}
          style={{ fontSize: "0.8rem", padding: "0.25rem 0.6rem" }}
        >
          {mankalTracking ? t("common.loading") : t("search.track_btn")}
        </button>
      </div>
    );
  };

  const trackJdaDataset = async (interval: number) => {
    if (!jdaResult?.url || !jdaResult?.title) return;
    setShowJdaInterval(false);
    setJdaTracking(true);
    try {
      await datasetsApi.trackScraper(jdaResult.url, jdaResult.title, interval);
      setJdaTracked(isAdmin ? "tracked" : "pending");
    } catch (err: any) {
      if (err.message?.includes("already tracked")) {
        setJdaTracked("tracked");
      } else {
        setError(err.message);
      }
    }
    setJdaTracking(false);
  };

  // Same shape as renderMankalTrackButton, bound to the jda state.
  const renderJdaTrackButton = () => {
    if (jdaTracked === "tracked") {
      return <span className="badge badge-success" role="status">{t("search.tracking")}</span>;
    }
    if (jdaTracked === "pending") {
      return (
        <span className="badge badge-success" role="status" style={{ background: "#22c55e", color: "#fff" }}>
          {t("search.request_sent", "הבקשה נשלחה — ממתין לאישור")}
        </span>
      );
    }
    return (
      <div style={{ display: "flex", gap: "0.3rem", alignItems: "center" }}>
        {showJdaInterval && (
          <select
            defaultValue={604800}
            onChange={(e) => trackJdaDataset(Number(e.target.value))}
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
          onClick={() => setShowJdaInterval(!showJdaInterval)}
          disabled={jdaTracking}
          style={{ fontSize: "0.8rem", padding: "0.25rem 0.6rem" }}
        >
          {jdaTracking ? t("common.loading") : t("search.track_btn")}
        </button>
      </div>
    );
  };

  // Track one eden corpus. Keyed by page_type so the tenders and decisions
  // cards each carry their own button/interval/tracked state.
  const trackEdenDataset = async (r: GovIlValidation, interval: number) => {
    if (!r.url || !r.title) return;
    const key = r.page_type || r.url;
    setShowEdenInterval((s) => ({ ...s, [key]: false }));
    setEdenTracking((s) => ({ ...s, [key]: true }));
    try {
      await datasetsApi.trackScraper(r.url, r.title, interval);
      setEdenTracked((s) => ({ ...s, [key]: isAdmin ? "tracked" : "pending" }));
    } catch (err: any) {
      if (err.message?.includes("already tracked")) {
        setEdenTracked((s) => ({ ...s, [key]: "tracked" }));
      } else {
        setError(err.message);
      }
    }
    setEdenTracking((s) => ({ ...s, [key]: false }));
  };

  // Same shape as renderJdaTrackButton, bound to the per-corpus eden state.
  const renderEdenTrackButton = (r: GovIlValidation) => {
    const key = r.page_type || r.url || "";
    if (edenTracked[key] === "tracked") {
      return <span className="badge badge-success" role="status">{t("search.tracking")}</span>;
    }
    if (edenTracked[key] === "pending") {
      return (
        <span className="badge badge-success" role="status" style={{ background: "#22c55e", color: "#fff" }}>
          {t("search.request_sent", "הבקשה נשלחה — ממתין לאישור")}
        </span>
      );
    }
    return (
      <div style={{ display: "flex", gap: "0.3rem", alignItems: "center" }}>
        {showEdenInterval[key] && (
          <select
            defaultValue={604800}
            onChange={(e) => trackEdenDataset(r, Number(e.target.value))}
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
          onClick={() => setShowEdenInterval((s) => ({ ...s, [key]: !s[key] }))}
          disabled={edenTracking[key]}
          style={{ fontSize: "0.8rem", padding: "0.25rem 0.6rem" }}
        >
          {edenTracking[key] ? t("common.loading") : t("search.track_btn")}
        </button>
      </div>
    );
  };

  // Same shape as renderGovIlTrackButton, bound to the health state.
  const renderHealthTrackButton = () => {
    if (healthTracked === "tracked") {
      return <span className="badge badge-success" role="status">{t("search.tracking")}</span>;
    }
    if (healthTracked === "pending") {
      return (
        <span className="badge badge-success" role="status" style={{ background: "#22c55e", color: "#fff" }}>
          {t("search.request_sent", "הבקשה נשלחה — ממתין לאישור")}
        </span>
      );
    }
    return (
      <div style={{ display: "flex", gap: "0.3rem", alignItems: "center" }}>
        {showHealthInterval && (
          <select
            defaultValue={604800}
            onChange={(e) => trackHealthDataset(Number(e.target.value))}
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
          onClick={() => setShowHealthInterval(!showHealthInterval)}
          disabled={healthTracking}
          style={{ fontSize: "0.8rem", padding: "0.25rem 0.6rem" }}
        >
          {healthTracking ? t("common.loading") : t("search.track_btn")}
        </button>
      </div>
    );
  };

  // Same shape as renderHealthTrackButton, bound to the registries state.
  const renderRegistriesTrackButton = () => {
    if (registriesTracked === "tracked") {
      return <span className="badge badge-success" role="status">{t("search.tracking")}</span>;
    }
    if (registriesTracked === "pending") {
      return (
        <span className="badge badge-success" role="status" style={{ background: "#22c55e", color: "#fff" }}>
          {t("search.request_sent", "הבקשה נשלחה — ממתין לאישור")}
        </span>
      );
    }
    return (
      <div style={{ display: "flex", gap: "0.3rem", alignItems: "center" }}>
        {showRegistriesInterval && (
          <select
            defaultValue={604800}
            onChange={(e) => trackRegistriesDataset(Number(e.target.value))}
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
          onClick={() => setShowRegistriesInterval(!showRegistriesInterval)}
          disabled={registriesTracking}
          style={{ fontSize: "0.8rem", padding: "0.25rem 0.6rem" }}
        >
          {registriesTracking ? t("common.loading") : t("search.track_btn")}
        </button>
      </div>
    );
  };

  // Same shape as renderGovIlTrackButton, just bound to the IDF state.
  // Could be DRY'd up with a factory, but keeping the two parallel is
  // easier to read while we have only two scraper origins.
  const renderIdfTrackButton = () => {
    if (idfTracked === "tracked") {
      return <span className="badge badge-success" role="status">{t("search.tracking")}</span>;
    }
    if (idfTracked === "pending") {
      return (
        <span className="badge badge-success" role="status" style={{ background: "#22c55e", color: "#fff" }}>
          {t("search.request_sent", "\u05D4\u05D1\u05E7\u05E9\u05D4 \u05E0\u05E9\u05DC\u05D7\u05D4 \u2014 \u05DE\u05DE\u05EA\u05D9\u05DF \u05DC\u05D0\u05D9\u05E9\u05D5\u05E8")}
        </span>
      );
    }
    return (
      <div style={{ display: "flex", gap: "0.3rem", alignItems: "center" }}>
        {showIdfInterval && (
          <select
            defaultValue={604800}
            onChange={(e) => trackIdfDataset(Number(e.target.value))}
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
          onClick={() => setShowIdfInterval(!showIdfInterval)}
          disabled={idfTracking}
          style={{ fontSize: "0.8rem", padding: "0.25rem 0.6rem" }}
        >
          {idfTracking ? t("common.loading") : t("search.track_btn")}
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

        {!loading && results.length === 0 && !govIlResult && !idfResult && !healthResult && !registriesResult && !avodataResult && !munidataResult && !mevakerResult && !hatzavResult && !mankalResult && !jdaResult && edenResults.length === 0 && !knessetResult && query && (
          <div className="empty-state">{t("search.no_results")}</div>
        )}

        {!loading && count > 0 && (
          <p className="sr-only" style={{ position: "absolute", width: 1, height: 1, overflow: "hidden", clip: "rect(0,0,0,0)" }}>
            {t("search.results_count", { count, defaultValue: `${count} results found` })}
          </p>
        )}
      </div>

      {/* Gov.il scraper result */}
      {govIlResult && (
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
              {renderGovIlTrackButton()}
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
          </article>
        </div>
      )}

      {/* IDF scraper result */}
      {idfResult && (
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
              {renderIdfTrackButton()}
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
          </article>
        </div>
      )}

      {/* avodata.labor.gov.il scraper result */}
      {avodataResult && (
        <div className="grid grid-2">
          <article className="card" style={{ borderRight: "4px solid #2563eb" }}>
            <div className="flex-between mb-1">
              <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
                <h2 style={{ fontSize: "1rem", fontWeight: 600, margin: 0 }}>{avodataResult.title}</h2>
                <span style={{
                  display: "inline-block",
                  padding: "0.15rem 0.5rem",
                  borderRadius: "9999px",
                  fontSize: "0.65rem",
                  fontWeight: 600,
                  background: "#dbeafe",
                  color: "#1e40af",
                }}>
                  AVODATA
                </span>
              </div>
              {renderAvodataTrackButton()}
            </div>
            <div className="flex text-sm text-muted" style={{ gap: "0.75rem" }}>
              <span>עולמות תעסוקה — משרד העבודה</span>
              <span>avodata.labor.gov.il</span>
            </div>
            <p className="text-sm text-muted mt-1" style={{ wordBreak: "break-all" }}>
              <a href={avodataResult.url} target="_blank" rel="noopener noreferrer" style={{ color: "var(--primary)" }}>
                {avodataResult.url}
              </a>
            </p>
          </article>
        </div>
      )}

      {/* municipal-data.org scraper result */}
      {munidataResult && (
        <div className="grid grid-2">
          <article className="card" style={{ borderRight: "4px solid #65a30d" }}>
            <div className="flex-between mb-1">
              <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
                <h2 style={{ fontSize: "1rem", fontWeight: 600, margin: 0 }}>{munidataResult.title}</h2>
                <span style={{
                  display: "inline-block",
                  padding: "0.15rem 0.5rem",
                  borderRadius: "9999px",
                  fontSize: "0.65rem",
                  fontWeight: 600,
                  background: "#ecfccb",
                  color: "#3f6212",
                }}>
                  מצב השלטון המקומי
                </span>
              </div>
              {renderMunidataTrackButton()}
            </div>
            <div className="flex text-sm text-muted" style={{ gap: "0.75rem" }}>
              <span>מצב השלטון המקומי — משרד הפנים</span>
              <span>municipal-data.org</span>
            </div>
            <p className="text-sm text-muted mt-1" style={{ wordBreak: "break-all" }}>
              <a href={munidataResult.url} target="_blank" rel="noopener noreferrer" style={{ color: "var(--primary)" }}>
                {munidataResult.url}
              </a>
            </p>
          </article>
        </div>
      )}

      {/* mevaker.gov.il scraper result */}
      {mevakerResult && (
        <div className="grid grid-2">
          <article className="card" style={{ borderRight: "4px solid #dc2626" }}>
            <div className="flex-between mb-1">
              <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
                <h2 style={{ fontSize: "1rem", fontWeight: 600, margin: 0 }}>{mevakerResult.title}</h2>
                <span style={{
                  display: "inline-block",
                  padding: "0.15rem 0.5rem",
                  borderRadius: "9999px",
                  fontSize: "0.65rem",
                  fontWeight: 600,
                  background: "#fee2e2",
                  color: "#991b1b",
                }}>
                  MEVAKER
                </span>
              </div>
              {renderMevakerTrackButton()}
            </div>
            <div className="flex text-sm text-muted" style={{ gap: "0.75rem" }}>
              <span>מבקר המדינה ונציב תלונות הציבור</span>
              <span>mevaker.gov.il</span>
            </div>
            <p className="text-sm text-muted mt-1" style={{ wordBreak: "break-all" }}>
              <a href={mevakerResult.url} target="_blank" rel="noopener noreferrer" style={{ color: "var(--primary)" }}>
                {mevakerResult.url}
              </a>
            </p>
          </article>
        </div>
      )}

      {/* geo.mot.gov.il (חצב) scraper result */}
      {hatzavResult && (
        <div className="grid grid-2">
          <article className="card" style={{ borderRight: "4px solid #4f46e5" }}>
            <div className="flex-between mb-1">
              <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
                <h2 style={{ fontSize: "1rem", fontWeight: 600, margin: 0 }}>{hatzavResult.title}</h2>
                <span style={{
                  display: "inline-block",
                  padding: "0.15rem 0.5rem",
                  borderRadius: "9999px",
                  fontSize: "0.65rem",
                  fontWeight: 600,
                  background: "#e0e7ff",
                  color: "#3730a3",
                }}>
                  חצב
                </span>
              </div>
              {renderHatzavTrackButton()}
            </div>
            <div className="flex text-sm text-muted" style={{ gap: "0.75rem" }}>
              <span>משרד התחבורה — מערכת חצב</span>
              <span>geo.mot.gov.il</span>
            </div>
            <p className="text-sm text-muted mt-1" style={{ wordBreak: "break-all" }}>
              <a href={hatzavResult.url} target="_blank" rel="noopener noreferrer" style={{ color: "var(--primary)" }}>
                {hatzavResult.url}
              </a>
            </p>
          </article>
        </div>
      )}

      {/* apps.education.gov.il/Mankal (חוזרי מנכ"ל) scraper result */}
      {mankalResult && (
        <div className="grid grid-2">
          <article className="card" style={{ borderRight: "4px solid #059669" }}>
            <div className="flex-between mb-1">
              <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
                <h2 style={{ fontSize: "1rem", fontWeight: 600, margin: 0 }}>{mankalResult.title}</h2>
                <span style={{
                  display: "inline-block",
                  padding: "0.15rem 0.5rem",
                  borderRadius: "9999px",
                  fontSize: "0.65rem",
                  fontWeight: 600,
                  background: "#d1fae5",
                  color: "#065f46",
                }}>
                  חוזרי מנכ"ל
                </span>
              </div>
              {renderMankalTrackButton()}
            </div>
            <div className="flex text-sm text-muted" style={{ gap: "0.75rem" }}>
              <span>משרד החינוך — חוזרי מנכ"ל</span>
              <span>apps.education.gov.il</span>
            </div>
            <p className="text-sm text-muted mt-1" style={{ wordBreak: "break-all" }}>
              <a href={mankalResult.url} target="_blank" rel="noopener noreferrer" style={{ color: "var(--primary)" }}>
                {mankalResult.url}
              </a>
            </p>
          </article>
        </div>
      )}

      {/* jda.gov.il (הרשות לפיתוח ירושלים) scraper result */}
      {jdaResult && (
        <div className="grid grid-2">
          <article className="card" style={{ borderRight: "4px solid #db2777" }}>
            <div className="flex-between mb-1">
              <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
                <h2 style={{ fontSize: "1rem", fontWeight: 600, margin: 0 }}>{jdaResult.title}</h2>
                <span style={{
                  display: "inline-block",
                  padding: "0.15rem 0.5rem",
                  borderRadius: "9999px",
                  fontSize: "0.65rem",
                  fontWeight: 600,
                  background: "#fce7f3",
                  color: "#9d174d",
                }}>
                  JDA
                </span>
              </div>
              {renderJdaTrackButton()}
            </div>
            <div className="flex text-sm text-muted" style={{ gap: "0.75rem" }}>
              <span>הרשות לפיתוח ירושלים</span>
              <span>jda.gov.il</span>
            </div>
            <p className="text-sm text-muted mt-1" style={{ wordBreak: "break-all" }}>
              <a href={jdaResult.url} target="_blank" rel="noopener noreferrer" style={{ color: "var(--primary)" }}>
                {jdaResult.url}
              </a>
            </p>
          </article>
        </div>
      )}

      {/* jeden.co.il (חברת עדן / Eden) scraper results — one card per valid
          corpus (מכרזים / החלטות ועדת מכרזים), each trackable independently. */}
      {edenResults.length > 0 && (
        <div className="grid grid-2">
          {edenResults.map((r) => (
            <article key={r.page_type || r.url} className="card" style={{ borderRight: "4px solid #ea580c" }}>
              <div className="flex-between mb-1">
                <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
                  <h2 style={{ fontSize: "1rem", fontWeight: 600, margin: 0 }}>{r.title}</h2>
                  <span style={{
                    display: "inline-block",
                    padding: "0.15rem 0.5rem",
                    borderRadius: "9999px",
                    fontSize: "0.65rem",
                    fontWeight: 600,
                    background: "#ffedd5",
                    color: "#9a3412",
                  }}>
                    EDEN
                  </span>
                </div>
                {renderEdenTrackButton(r)}
              </div>
              <div className="flex text-sm text-muted" style={{ gap: "0.75rem" }}>
                <span>חברת עדן</span>
                <span>jeden.co.il</span>
              </div>
              <p className="text-sm text-muted mt-1" style={{ wordBreak: "break-all" }}>
                <a href={r.url} target="_blank" rel="noopener noreferrer" style={{ color: "var(--primary)" }}>
                  {r.url}
                </a>
              </p>
            </article>
          ))}
        </div>
      )}

      {/* knesset.gov.il committee-protocols scraper result — indigo "כנסת" chip */}
      {knessetResult && (
        <div className="grid grid-2">
          <article className="card" style={{ borderRight: "4px solid #4f46e5" }}>
            <div className="flex-between mb-1">
              <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
                <h2 style={{ fontSize: "1rem", fontWeight: 600, margin: 0 }}>{knessetResult.title}</h2>
                <span style={{
                  display: "inline-block",
                  padding: "0.15rem 0.5rem",
                  borderRadius: "9999px",
                  fontSize: "0.65rem",
                  fontWeight: 600,
                  background: "#e0e7ff",
                  color: "#3730a3",
                }}>
                  כנסת
                </span>
              </div>
              {renderKnessetTrackButton()}
            </div>
            <div className="flex text-sm text-muted" style={{ gap: "0.75rem" }}>
              <span>פרוטוקולי ועדות הכנסת</span>
              <span>knesset.gov.il</span>
            </div>
            <p className="text-sm text-muted mt-1" style={{ wordBreak: "break-all" }}>
              <a href={knessetResult.url} target="_blank" rel="noopener noreferrer" style={{ color: "var(--primary)" }}>
                {knessetResult.url}
              </a>
            </p>
          </article>
        </div>
      )}

      {/* practitioners.health.gov.il scraper result */}
      {healthResult && (
        <div className="grid grid-2">
          <article className="card" style={{ borderRight: "4px solid #7c3aed" }}>
            <div className="flex-between mb-1">
              <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
                <h2 style={{ fontSize: "1rem", fontWeight: 600, margin: 0 }}>{healthResult.title}</h2>
                <span style={{
                  display: "inline-block",
                  padding: "0.15rem 0.5rem",
                  borderRadius: "9999px",
                  fontSize: "0.65rem",
                  fontWeight: 600,
                  background: "#ede9fe",
                  color: "#5b21b6",
                }}>
                  PRACTITIONERS
                </span>
              </div>
              {renderHealthTrackButton()}
            </div>
            <div className="flex text-sm text-muted" style={{ gap: "0.75rem" }}>
              <span>בעלי מקצועות בריאות</span>
              <span>practitioners.health.gov.il</span>
            </div>
            <p className="text-sm text-muted mt-1" style={{ wordBreak: "break-all" }}>
              <a href={healthResult.url} target="_blank" rel="noopener noreferrer" style={{ color: "var(--primary)" }}>
                {healthResult.url}
              </a>
            </p>
          </article>
        </div>
      )}

      {/* registries.health.gov.il scraper result */}
      {registriesResult && (
        <div className="grid grid-2">
          <article className="card" style={{ borderRight: "4px solid #14b8a6" }}>
            <div className="flex-between mb-1">
              <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
                <h2 style={{ fontSize: "1rem", fontWeight: 600, margin: 0 }}>{registriesResult.title}</h2>
                <span style={{
                  display: "inline-block",
                  padding: "0.15rem 0.5rem",
                  borderRadius: "9999px",
                  fontSize: "0.65rem",
                  fontWeight: 600,
                  background: "#ccfbf1",
                  color: "#115e59",
                }}>
                  בריאות
                </span>
              </div>
              {renderRegistriesTrackButton()}
            </div>
            <div className="flex text-sm text-muted" style={{ gap: "0.75rem" }}>
              <span>מאגרי מידע — משרד הבריאות</span>
              <span>registries.health.gov.il</span>
            </div>
            <p className="text-sm text-muted mt-1" style={{ wordBreak: "break-all" }}>
              <a href={registriesResult.url} target="_blank" rel="noopener noreferrer" style={{ color: "var(--primary)" }}>
                {registriesResult.url}
              </a>
            </p>
          </article>
        </div>
      )}

      {/* CKAN results */}
      <div className="grid grid-2">
        {results.map((r) => {
          const targetResource = targetResourceId
            ? r.resources?.find((res) => res.id === targetResourceId)
            : null;

          const trackedLocal = trackedByCkanId.get(r.id);
          return (
            <article key={r.id} className="card">
              <div className="flex-between mb-1">
                <h2 style={{ fontSize: "1rem", fontWeight: 600 }}>{r.title}</h2>
                {!targetResource && renderTrackButton(r.id, r.title)}
              </div>
              {trackedLocal && (
                <div style={{ marginBottom: "0.5rem" }}>
                  <AdminDatasetActions
                    datasetId={trackedLocal.id}
                    title={trackedLocal.title}
                    onDeleted={(id) => {
                      setTrackedByCkanId((prev) => {
                        const next = new Map(prev);
                        for (const [ckanId, v] of prev.entries()) {
                          if (v.id === id) next.delete(ckanId);
                        }
                        return next;
                      });
                      // Also let the existing track-state machine forget this
                      // dataset, so the user sees the "track" button again
                      // instead of a stale "tracking" badge.
                      setTracked((prev) => {
                        const next = new Map(prev);
                        for (const key of Array.from(next.keys())) {
                          if (key === r.id || key.startsWith(`${r.id}::`)) {
                            next.delete(key);
                          }
                        }
                        return next;
                      });
                    }}
                  />
                </div>
              )}
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
