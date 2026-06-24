import { useState, useEffect, FormEvent } from "react";
import { useTranslation } from "react-i18next";
import { ckan, datasets as datasetsApi, govil, idf, health, avodata, mevaker, hatzav, GovIlValidation } from "../api/client";
import { useAuth } from "../auth/AuthContext";
import AdminDatasetActions from "../components/AdminDatasetActions";
// idf.il section pattern lives in utils/idfPattern.ts — single
// source of truth shared with HomePage so we never let one accept
// URLs the other rejects.
import { IDF_PATTERN } from "../utils/idfPattern";
// practitioners.health.gov.il per-registry URL pattern. Mirror of
// HEALTH_PRACTITIONERS_RE in app/api/health.py.
import { HEALTH_PRACTITIONERS_PATTERN } from "../utils/healthPattern";
// avodata.labor.gov.il occupations-index URL pattern. Mirror of
// AVODATA_OCCUPATIONS_RE in app/api/avodata.py.
import { AVODATA_OCCUPATIONS_PATTERN } from "../utils/avodataPattern";
// mevaker.gov.il reports-index URL pattern. Mirror of MEVAKER_SUBJECTS_RE
// in app/api/mevaker.py.
import { MEVAKER_SUBJECTS_PATTERN } from "../utils/mevakerPattern";
// geo.mot.gov.il (חצב) portal URL pattern. Mirror of HATZAV_ROOT_RE in
// app/api/hatzav.py.
import { HATZAV_PATTERN } from "../utils/hatzavPattern";

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

  // avodata.labor.gov.il scraper result — same flow.
  const [avodataResult, setAvodataResult] = useState<GovIlValidation | null>(null);
  const [avodataTracked, setAvodataTracked] = useState<"tracked" | "pending" | null>(null);
  const [avodataTracking, setAvodataTracking] = useState(false);
  const [showAvodataInterval, setShowAvodataInterval] = useState(false);
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

  const detectMevakerUrl = (input: string): boolean => {
    return MEVAKER_SUBJECTS_PATTERN.test(input.trim());
  };

  const detectHatzavUrl = (input: string): boolean => {
    return HATZAV_PATTERN.test(input.trim());
  };

  const detectAvodataUrl = (input: string): boolean => {
    return AVODATA_OCCUPATIONS_PATTERN.test(input.trim());
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
    setAvodataResult(null);
    setAvodataTracked(null);
    setShowAvodataInterval(false);
    setMevakerResult(null);
    setMevakerTracked(null);
    setShowMevakerInterval(false);
    setHatzavResult(null);
    setHatzavTracked(null);
    setShowHatzavInterval(false);
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

        {!loading && results.length === 0 && !govIlResult && !idfResult && !healthResult && !avodataResult && !mevakerResult && !hatzavResult && query && (
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
