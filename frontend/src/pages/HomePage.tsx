import { useState, useEffect, useMemo, FormEvent } from "react";
import { useTranslation } from "react-i18next";
import { Link, useSearchParams } from "react-router-dom";
import { ckan, publicApi, govil, govmap, idf, health, registries, avodata, munidata, emun, servicescompass, mevaker, hatzav, mankal, jda, eden, knesset, sources, TrackedDataset, GovIlValidation, GovMapValidation, RegistrySourceValidation } from "../api/client";
import TagChips from "../components/TagChips";
import SourceChip from "../components/SourceChip";
import RequestForm from "../components/RequestForm";
import RegistrySourceCard from "../components/RegistrySourceCard";
import GovmapRequestForm from "../components/GovmapRequestForm";
import { sourceBadgeFor } from "../utils/sourceBadge";
// idf.il section pattern lives in utils/idfPattern.ts so the
// HomePage and SearchPage versions can never drift when we add new
// sections.
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
// gov.il/apps/servicescompass URL pattern. Mirror of
// SERVICESCOMPASS_PATH_RE in app/api/servicescompass.py.
import { SERVICESCOMPASS_PATTERN } from "../utils/servicescompassPattern";
// municipal-data.org per-metric URL pattern. Mirror of _parse_munidata_url
// in app/api/munidata.py.
import { MUNIDATA_METRIC_PATTERN } from "../utils/munidataPattern";
// govextra.gov.il/pmo/emun dashboard URL pattern. Mirror of _parse_emun_url
// in app/api/emun.py.
import { EMUN_DASHBOARD_PATTERN } from "../utils/emunPattern";
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
// knesset.gov.il committee-protocols URL pattern (KNS_Committee ODATA query).
// The committee scope (CategoryID / Id) is validated by the backend
// /api/knesset/validate, which is authoritative.
import { KNESSET_PATTERN } from "../utils/knessetPattern";

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

// Lower-cased "searchable text" for a tracked dataset — its title, org,
// tags, ckan id, source URL. Powers the in-site keyword search so a query
// matches datasets ALREADY tracked here, not just data.gov.il.
function trackedHaystack(ds: TrackedDataset): string {
  const tags = Array.isArray((ds as any).tags)
    ? (ds as any).tags
        .map((tg: any) => (typeof tg === "string" ? tg : tg?.name || ""))
        .join(" ")
    : "";
  return [
    ds.title,
    ds.organization_title,
    ds.organization,
    ds.ckan_id,
    ds.source_url,
    ds.resource_name,
    tags,
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();
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
  const [searchParams, setSearchParams] = useSearchParams();
  const [query, setQuery] = useState(() => searchParams.get("q") || "");
  const [results, setResults] = useState<SearchResult[]>([]);
  const [count, setCount] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [targetResourceId, setTargetResourceId] = useState<string | null>(null);
  // The last SUBMITTED keyword query (empty for URL-detect searches / no
  // search yet). Drives the in-site filter over already-tracked datasets.
  const [submittedQuery, setSubmittedQuery] = useState("");

  // Tracked datasets
  const [trackedDatasets, setTrackedDatasets] = useState<TrackedDataset[]>([]);
  const [trackedLoading, setTrackedLoading] = useState(true);

  // Tracked datasets that match the submitted keyword search. Reactive so
  // it fills in once the dataset list finishes loading (e.g. on a deep
  // link like /?q=...). Empty when there's no active keyword search.
  const matchedTracked = useMemo(() => {
    const tokens = submittedQuery.trim().toLowerCase().split(/\s+/).filter(Boolean);
    if (!tokens.length) return [];
    // Match datasets where EVERY query word appears (order-independent),
    // so "ממשלה החלטות" still finds "החלטות ממשלה".
    return trackedDatasets.filter((ds) => {
      const hay = trackedHaystack(ds);
      return tokens.every((tok) => hay.includes(tok));
    });
  }, [submittedQuery, trackedDatasets]);

  // Collapse the hundreds of Knesset-committee datasets (tag "ועדות כנסת")
  // into ONE card on the default list, so they don't drown the main screen.
  // The card links to /knesset (the protocol-search page). When the user is
  // searching, individual committees still surface via matchedTracked.
  const KNESSET_COMMITTEE_TAG = "ועדות כנסת";
  const isCommitteeDataset = (ds: TrackedDataset) =>
    (Array.isArray((ds as any).tags) ? (ds as any).tags : []).some(
      (tg: any) => (typeof tg === "string" ? tg : tg?.name || "") === KNESSET_COMMITTEE_TAG,
    );
  const restTracked = useMemo(
    () => trackedDatasets.filter((ds) => !isCommitteeDataset(ds)),
    [trackedDatasets],
  );
  const committeeGroup = useMemo(() => {
    const list = trackedDatasets.filter(isCommitteeDataset);
    if (list.length === 0) return null;
    return {
      count: list.length,
      versions: list.reduce((s, d) => s + (d.version_count || 0), 0),
    };
  }, [trackedDatasets]);

  // Request form state — which dataset has form open
  const [requestFormFor, setRequestFormFor] = useState<string | null>(null);

  // Gov.il scraper result
  const [govIlResult, setGovIlResult] = useState<GovIlValidation | null>(null);
  // GovMap layer result (single seed URL — the form lets the user add more)
  const [govMapResult, setGovMapResult] = useState<GovMapValidation | null>(null);
  // IDF (idf.il) Military-Prosecution scraper result — same shape as
  // GovIlValidation (validator returns page_type/collector_name/title/url).
  const [idfResult, setIdfResult] = useState<GovIlValidation | null>(null);
  // practitioners.health.gov.il per-registry scraper result — same
  // shape as GovIlValidation.
  const [healthResult, setHealthResult] = useState<GovIlValidation | null>(null);
  // registries.health.gov.il per-registry scraper result — same shape.
  const [registriesResult, setRegistriesResult] = useState<GovIlValidation | null>(null);
  // avodata.labor.gov.il per-scope scraper result — same shape.
  const [avodataResult, setAvodataResult] = useState<GovIlValidation | null>(null);
  // gov.il/apps/servicescompass scraper result — same shape.
  const [servicescompassResult, setServicescompassResult] = useState<GovIlValidation | null>(null);
  // municipal-data.org per-metric scraper result — same shape.
  const [munidataResult, setMunidataResult] = useState<GovIlValidation | null>(null);
  const [emunResult, setEmunResult] = useState<GovIlValidation | null>(null);
  // mevaker.gov.il reports scraper result — same shape.
  const [mevakerResult, setMevakerResult] = useState<GovIlValidation | null>(null);
  // geo.mot.gov.il (חצב) catalog scraper result — same shape.
  const [hatzavResult, setHatzavResult] = useState<GovIlValidation | null>(null);
  // apps.education.gov.il/Mankal (חוזרי מנכ"ל) scraper result — same shape.
  const [mankalResult, setMankalResult] = useState<GovIlValidation | null>(null);
  // jda.gov.il (הרשות לפיתוח ירושלים) scraper result — same shape.
  const [jdaResult, setJdaResult] = useState<GovIlValidation | null>(null);
  // jeden.co.il (חברת עדן / Eden) scraper results. The two corpora
  // (מכרזים / החלטות ועדת מכרזים) share ONE page, so a bare URL can't be
  // validated — instead we probe both ?category= variants and show one
  // card per valid corpus.
  const [edenResults, setEdenResults] = useState<GovIlValidation[]>([]);
  // knesset.gov.il committee-protocols scraper result — same shape. One
  // committee (CategoryID / Id scope) per pasted URL.
  const [knessetResult, setKnessetResult] = useState<GovIlValidation | null>(null);
  // A source declared by the scraper worker's manifest instead of hardcoded
  // here. One state and one card cover every such source — the chip colours,
  // the site name and the poll cadence all arrive with the validation.
  const [registryResult, setRegistryResult] =
    useState<RegistrySourceValidation | null>(null);

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

  // Worth asking the server whether a manifest claims this URL. Deliberately
  // shape-only — the manifests' regexes are Python-flavoured and live on the
  // server, so the browser can't match them itself. data.gov.il is excluded
  // because it has its own dedicated handling further down.
  const looksLikeTrackableUrl = (input: string): boolean => {
    const trimmed = input.trim();
    if (!/^https?:\/\//i.test(trimmed)) return false;
    try {
      return !new URL(trimmed).hostname.endsWith("data.gov.il");
    } catch {
      return false;
    }
  };

  const detectGovMapUrl = (input: string): boolean => {
    return GOVMAP_PATTERN.test(input.trim());
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

  const detectAvodataUrl = (input: string): boolean => {
    return AVODATA_OCCUPATIONS_PATTERN.test(input.trim());
  };

  const detectServicescompassUrl = (input: string): boolean => {
    return SERVICESCOMPASS_PATTERN.test(input.trim());
  };

  const detectMunidataUrl = (input: string): boolean => {
    return MUNIDATA_METRIC_PATTERN.test(input.trim());
  };

  const detectEmunUrl = (input: string): boolean => {
    return EMUN_DASHBOARD_PATTERN.test(input.trim());
  };

  const detectMevakerUrl = (input: string): boolean => {
    return MEVAKER_SUBJECTS_PATTERN.test(input.trim());
  };

  const detectHatzavUrl = (input: string): boolean => {
    return HATZAV_PATTERN.test(input.trim());
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

  const stripHtml = (html: string) => {
    const doc = new DOMParser().parseFromString(html, "text/html");
    return doc.body.textContent || "";
  };

  const search = async (e?: FormEvent) => {
    e?.preventDefault();
    const trimmed = query.trim();
    if (!trimmed) return;
    // Reflect the query in the URL so a search is shareable / bookmarkable.
    setSearchParams({ q: trimmed }, { replace: true });
    setLoading(true);
    setError("");
    setTargetResourceId(null);
    setRequestFormFor(null);
    setGovIlResult(null);
    setGovMapResult(null);
    setIdfResult(null);
    setHealthResult(null);
    setRegistriesResult(null);
    setAvodataResult(null);
    setServicescompassResult(null);
    setMunidataResult(null);
    setEmunResult(null);
    setMevakerResult(null);
    setHatzavResult(null);
    setMankalResult(null);
    setJdaResult(null);
    setEdenResults([]);
    setKnessetResult(null);
    setRegistryResult(null);
    setSubmittedQuery("");
    try {
      // 1. Check for govmap.gov.il layer URL
      if (detectGovMapUrl(query)) {
        const validation = await govmap.validate(query.trim());
        if (validation.valid) {
          setGovMapResult(validation);
          setRequestFormFor("govmap");
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
          setRequestFormFor("govil");
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
          setRequestFormFor("idf");
          setResults([]);
          setCount(0);
        } else {
          setError(validation.error || "Invalid idf.il URL");
        }
        setLoading(false);
        return;
      }

      // 2c. Check for practitioners.health.gov.il per-registry URL.
      // Same auto-treat-as-scraper flow as idf.il — the backend's
      // /api/health/validate returns the same shape so we render
      // through the existing scraper card.
      if (detectHealthUrl(query)) {
        const validation = await health.validate(query.trim());
        if (validation.valid) {
          setHealthResult(validation);
          setRequestFormFor("health");
          setResults([]);
          setCount(0);
        } else {
          setError(validation.error || "Invalid practitioners.health.gov.il URL");
        }
        setLoading(false);
        return;
      }

      // 2c-registries. Check for registries.health.gov.il per-registry URL.
      // Same auto-treat-as-scraper flow — /api/registries/validate returns
      // the same shape so we render through the existing scraper card.
      if (detectRegistriesUrl(query)) {
        const validation = await registries.validate(query.trim());
        if (validation.valid) {
          setRegistriesResult(validation);
          setRequestFormFor("registries");
          setResults([]);
          setCount(0);
        } else {
          setError(validation.error || "Invalid registries.health.gov.il URL");
        }
        setLoading(false);
        return;
      }

      // 2d. Check for avodata.labor.gov.il per-scope URL.
      if (detectAvodataUrl(query)) {
        const validation = await avodata.validate(query.trim());
        if (validation.valid) {
          setAvodataResult(validation);
          setRequestFormFor("avodata");
          setResults([]);
          setCount(0);
        } else {
          setError(validation.error || "Invalid avodata.labor.gov.il URL");
        }
        setLoading(false);
        return;
      }

      // 2d-1. Check for gov.il/apps/servicescompass URL.
      if (detectServicescompassUrl(query)) {
        const validation = await servicescompass.validate(query.trim());
        if (validation.valid) {
          setServicescompassResult(validation);
          setRequestFormFor("servicescompass");
          setResults([]);
          setCount(0);
        } else {
          setError(validation.error || "Invalid gov.il/apps/servicescompass URL");
        }
        setLoading(false);
        return;
      }

      // 2d-2. Check for municipal-data.org per-metric URL.
      if (detectMunidataUrl(query)) {
        const validation = await munidata.validate(query.trim());
        if (validation.valid) {
          setMunidataResult(validation);
          setRequestFormFor("munidata");
          setResults([]);
          setCount(0);
        } else {
          setError(validation.error || "Invalid municipal-data.org URL");
        }
        setLoading(false);
        return;
      }

      // 2d-3. Check for the govextra.gov.il/pmo/emun dashboard URL.
      if (detectEmunUrl(query)) {
        const validation = await emun.validate(query.trim());
        if (validation.valid) {
          setEmunResult(validation);
          setRequestFormFor("emun");
          setResults([]);
          setCount(0);
        } else {
          setError(validation.error || "Invalid govextra.gov.il/pmo/emun URL");
        }
        setLoading(false);
        return;
      }

      // 2e. Check for mevaker.gov.il reports-index URL.
      if (detectMevakerUrl(query)) {
        const validation = await mevaker.validate(query.trim());
        if (validation.valid) {
          setMevakerResult(validation);
          setRequestFormFor("mevaker");
          setResults([]);
          setCount(0);
        } else {
          setError(validation.error || "Invalid mevaker.gov.il URL");
        }
        setLoading(false);
        return;
      }

      // 2f. Check for geo.mot.gov.il (חצב) portal URL.
      if (detectHatzavUrl(query)) {
        const validation = await hatzav.validate(query.trim());
        if (validation.valid) {
          setHatzavResult(validation);
          setRequestFormFor("hatzav");
          setResults([]);
          setCount(0);
        } else {
          setError(validation.error || "Invalid geo.mot.gov.il URL");
        }
        setLoading(false);
        return;
      }

      // 2g. Check for apps.education.gov.il/Mankal (חוזרי מנכ"ל) portal URL.
      if (detectMankalUrl(query)) {
        const validation = await mankal.validate(query.trim());
        if (validation.valid) {
          setMankalResult(validation);
          setRequestFormFor("mankal");
          setResults([]);
          setCount(0);
        } else {
          setError(validation.error || "Invalid apps.education.gov.il URL");
        }
        setLoading(false);
        return;
      }

      // 2h. Check for jda.gov.il (הרשות לפיתוח ירושלים) tenders-portal URL.
      if (detectJdaUrl(query)) {
        const validation = await jda.validate(query.trim());
        if (validation.valid) {
          setJdaResult(validation);
          setRequestFormFor("jda");
          setResults([]);
          setCount(0);
        } else {
          setError(validation.error || "Invalid jda.gov.il URL");
        }
        setLoading(false);
        return;
      }

      // 2i. Check for jeden.co.il (חברת עדן / Eden) URL. The two corpora
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

      // 2j. Check for a knesset.gov.il committee (KNS_Committee ODATA) URL —
      // one committee (CategoryID / Id scope) per pasted URL. The backend
      // probes the ODATA feed for the committee's real name.
      if (detectKnessetUrl(query)) {
        const validation = await knesset.validate(query.trim());
        if (validation.valid) {
          setKnessetResult(validation);
          setRequestFormFor("knesset");
          setResults([]);
          setCount(0);
        } else {
          setError(validation.error || "כתובת knesset.gov.il לא תקינה");
        }
        setLoading(false);
        return;
      }

      // 2k. Sources the scraper worker declared (no code for them here). Runs
      // after every hardcoded detector so it can never intercept one of them,
      // and only for an absolute non-data.gov.il URL so a keyword search
      // doesn't pay a round-trip. An unrecognised URL falls through silently
      // to the search below, exactly as before this path existed.
      if (looksLikeTrackableUrl(query)) {
        const validation = await sources.validate(query.trim());
        if (validation.valid) {
          setRegistryResult(validation);
          setRequestFormFor(validation.source_id || "registry");
          setResults([]);
          setCount(0);
          setLoading(false);
          return;
        }
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
        // 3. Keyword search — data.gov.il AND, via submittedQuery, the
        // datasets already tracked here (matchedTracked memo + section).
        const data = await ckan.search(query);
        setResults(data.results);
        setCount(data.count);
        setSubmittedQuery(trimmed);
      }
    } catch (err: any) {
      setError(err.message);
    }
    setLoading(false);
  };

  const resultKey = (datasetId: string, resourceId?: string) =>
    resourceId ? `${datasetId}::${resourceId}` : datasetId;

  // Deep link: a page opened at /?q=... runs that search on mount (query
  // was seeded from the URL in useState). Runs once.
  useEffect(() => {
    if (query.trim()) search();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

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
              onChange={(e) => {
                const v = e.target.value;
                setQuery(v);
                // Emptying the box returns to the full browse view.
                if (!v.trim()) {
                  setSubmittedQuery("");
                  setResults([]);
                  setCount(0);
                  setSearchParams({}, { replace: true });
                }
              }}
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

        {/* practitioners.health.gov.il scraper result — same card
            layout, distinct badge so it's obvious this is a health
            ministry registry. */}
        {!loading && healthResult && (
          <section aria-label="practitioners.health.gov.il result" style={{ marginBottom: "2rem" }}>
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

                <div style={{ marginTop: "0.75rem" }}>
                  {requestFormFor === "health" ? (
                    <RequestForm
                      datasetTitle={healthResult.title || ""}
                      onClose={() => setRequestFormFor(null)}
                      sourceType="scraper"
                      sourceUrl={healthResult.url}
                    />
                  ) : (
                    <button
                      className="btn-primary"
                      onClick={() => setRequestFormFor("health")}
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

        {/* registries.health.gov.il scraper result — teal בריאות chip,
            distinct from the purple practitioners.health.gov.il card. */}
        {!loading && registriesResult && (
          <section aria-label="registries.health.gov.il result" style={{ marginBottom: "2rem" }}>
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

                <div style={{ marginTop: "0.75rem" }}>
                  {requestFormFor === "registries" ? (
                    <RequestForm
                      datasetTitle={registriesResult.title || ""}
                      onClose={() => setRequestFormFor(null)}
                      sourceType="scraper"
                      sourceUrl={registriesResult.url}
                    />
                  ) : (
                    <button
                      className="btn-primary"
                      onClick={() => setRequestFormFor("registries")}
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

        {/* avodata.labor.gov.il scraper result — sky-blue AVODATA chip
            to keep it visually distinct from the purple practitioners
            and green idf badges. */}
        {!loading && avodataResult && (
          <section aria-label="avodata.labor.gov.il result" style={{ marginBottom: "2rem" }}>
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

                <div style={{ marginTop: "0.75rem" }}>
                  {requestFormFor === "avodata" ? (
                    <RequestForm
                      datasetTitle={avodataResult.title || ""}
                      onClose={() => setRequestFormFor(null)}
                      sourceType="scraper"
                      sourceUrl={avodataResult.url}
                    />
                  ) : (
                    <button
                      className="btn-primary"
                      onClick={() => setRequestFormFor("avodata")}
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

        {/* gov.il/apps/servicescompass scraper result — amber "מצפן
            השירותים" chip, distinct from the sky-blue AVODATA and lime
            municipal-data badges. */}
        {!loading && servicescompassResult && (
          <section aria-label="gov.il services compass result" style={{ marginBottom: "2rem" }}>
            <div className="grid grid-2">
              <article className="card" style={{ borderRight: "4px solid #ea580c" }}>
                <div className="flex-between mb-1">
                  <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
                    <h2 style={{ fontSize: "1rem", fontWeight: 600, margin: 0 }}>{servicescompassResult.title}</h2>
                    <span style={{
                      display: "inline-block",
                      padding: "0.15rem 0.5rem",
                      borderRadius: "9999px",
                      fontSize: "0.65rem",
                      fontWeight: 600,
                      background: "#ffedd5",
                      color: "#9a3412",
                    }}>
                      מצפן השירותים
                    </span>
                  </div>
                </div>
                <div className="flex text-sm text-muted" style={{ gap: "0.75rem" }}>
                  <span>מערך הדיגיטל הלאומי</span>
                  <span>gov.il/apps/servicescompass</span>
                </div>
                <p className="text-sm text-muted mt-1" style={{ wordBreak: "break-all" }}>
                  <a href={servicescompassResult.url} target="_blank" rel="noopener noreferrer" style={{ color: "var(--primary)" }}>
                    {servicescompassResult.url}
                  </a>
                </p>

                <div style={{ marginTop: "0.75rem" }}>
                  {requestFormFor === "servicescompass" ? (
                    <RequestForm
                      datasetTitle={servicescompassResult.title || ""}
                      onClose={() => setRequestFormFor(null)}
                      sourceType="scraper"
                      sourceUrl={servicescompassResult.url}
                    />
                  ) : (
                    <button
                      className="btn-primary"
                      onClick={() => setRequestFormFor("servicescompass")}
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

        {/* municipal-data.org scraper result — lime "מצב השלטון המקומי" chip. */}
        {!loading && munidataResult && (
          <section aria-label="municipal-data.org result" style={{ marginBottom: "2rem" }}>
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

                <div style={{ marginTop: "0.75rem" }}>
                  {requestFormFor === "munidata" ? (
                    <RequestForm
                      datasetTitle={munidataResult.title || ""}
                      onClose={() => setRequestFormFor(null)}
                      sourceType="scraper"
                      sourceUrl={munidataResult.url}
                    />
                  ) : (
                    <button
                      className="btn-primary"
                      onClick={() => setRequestFormFor("munidata")}
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

{/* govextra.gov.il/pmo/emun scraper result — indigo אמו"ן chip. */}
        {!loading && emunResult && (
          <section aria-label="emun result" style={{ marginBottom: "2rem" }}>
            <div className="grid grid-2">
              <article className="card" style={{ borderRight: "4px solid #4f46e5" }}>
                <div className="flex-between mb-1">
                  <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
                    <h2 style={{ fontSize: "1rem", fontWeight: 600, margin: 0 }}>{emunResult.title}</h2>
                    <span style={{
                      display: "inline-block",
                      padding: "0.15rem 0.5rem",
                      borderRadius: "9999px",
                      fontSize: "0.65rem",
                      fontWeight: 600,
                      background: "#e0e7ff",
                      color: "#3730a3",
                    }}>
                      מערכת אמו"ן
                    </span>
                  </div>
                </div>
                <div className="flex text-sm text-muted" style={{ gap: "0.75rem" }}>
                  <span>מעקב יישום החלטות הממשלה — משרד ראש הממשלה</span>
                  <span>govextra.gov.il</span>
                </div>
                <p className="text-sm text-muted mt-1" style={{ wordBreak: "break-all" }}>
                  <a href={emunResult.url} target="_blank" rel="noopener noreferrer" style={{ color: "var(--primary)" }}>
                    {emunResult.url}
                  </a>
                </p>

                <div style={{ marginTop: "0.75rem" }}>
                  {requestFormFor === "emun" ? (
                    <RequestForm
                      datasetTitle={emunResult.title || ""}
                      onClose={() => setRequestFormFor(null)}
                      sourceType="scraper"
                      sourceUrl={emunResult.url}
                    />
                  ) : (
                    <button
                      className="btn-primary"
                      onClick={() => setRequestFormFor("emun")}
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

                {/* mevaker.gov.il scraper result — deep-red MEVAKER chip. */}
        {!loading && mevakerResult && (
          <section aria-label="mevaker.gov.il result" style={{ marginBottom: "2rem" }}>
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

                <div style={{ marginTop: "0.75rem" }}>
                  {requestFormFor === "mevaker" ? (
                    <RequestForm
                      datasetTitle={mevakerResult.title || ""}
                      onClose={() => setRequestFormFor(null)}
                      sourceType="scraper"
                      sourceUrl={mevakerResult.url}
                    />
                  ) : (
                    <button
                      className="btn-primary"
                      onClick={() => setRequestFormFor("mevaker")}
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

        {/* geo.mot.gov.il (חצב) scraper result — indigo חצב chip. */}
        {!loading && hatzavResult && (
          <section aria-label="geo.mot.gov.il result" style={{ marginBottom: "2rem" }}>
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

                <div style={{ marginTop: "0.75rem" }}>
                  {requestFormFor === "hatzav" ? (
                    <RequestForm
                      datasetTitle={hatzavResult.title || ""}
                      onClose={() => setRequestFormFor(null)}
                      sourceType="scraper"
                      sourceUrl={hatzavResult.url}
                    />
                  ) : (
                    <button
                      className="btn-primary"
                      onClick={() => setRequestFormFor("hatzav")}
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

        {/* apps.education.gov.il/Mankal (חוזרי מנכ"ל) scraper result —
            emerald חוזרי מנכ"ל chip. */}
        {!loading && mankalResult && (
          <section aria-label="apps.education.gov.il result" style={{ marginBottom: "2rem" }}>
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

                <div style={{ marginTop: "0.75rem" }}>
                  {requestFormFor === "mankal" ? (
                    <RequestForm
                      datasetTitle={mankalResult.title || ""}
                      onClose={() => setRequestFormFor(null)}
                      sourceType="scraper"
                      sourceUrl={mankalResult.url}
                    />
                  ) : (
                    <button
                      className="btn-primary"
                      onClick={() => setRequestFormFor("mankal")}
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

        {/* jda.gov.il (הרשות לפיתוח ירושלים) scraper result — rose/pink
            JDA chip. */}
        {!loading && jdaResult && (
          <section aria-label="jda.gov.il result" style={{ marginBottom: "2rem" }}>
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

                <div style={{ marginTop: "0.75rem" }}>
                  {requestFormFor === "jda" ? (
                    <RequestForm
                      datasetTitle={jdaResult.title || ""}
                      onClose={() => setRequestFormFor(null)}
                      sourceType="scraper"
                      sourceUrl={jdaResult.url}
                    />
                  ) : (
                    <button
                      className="btn-primary"
                      onClick={() => setRequestFormFor("jda")}
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

        {/* jeden.co.il (חברת עדן / Eden) scraper results — orange EDEN
            chip. One card per valid corpus (מכרזים / החלטות ועדת מכרזים),
            each trackable independently. */}
        {!loading && edenResults.length > 0 && (
          <section aria-label="jeden.co.il result" style={{ marginBottom: "2rem" }}>
            <div className="grid grid-2">
              {edenResults.map((r) => {
                const formKey = `eden:${r.page_type}`;
                return (
                  <article key={formKey} className="card" style={{ borderRight: "4px solid #ea580c" }}>
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

                    <div style={{ marginTop: "0.75rem" }}>
                      {requestFormFor === formKey ? (
                        <RequestForm
                          datasetTitle={r.title || ""}
                          onClose={() => setRequestFormFor(null)}
                          sourceType="scraper"
                          sourceUrl={r.url}
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
          </section>
        )}

        {/* knesset.gov.il committee-protocols scraper result — indigo "כנסת"
            chip, distinct from the orange EDEN / rose JDA / sky AVODATA. */}
        {!loading && knessetResult && (
          <section aria-label="knesset.gov.il result" style={{ marginBottom: "2rem" }}>
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

                <div style={{ marginTop: "0.75rem" }}>
                  {requestFormFor === "knesset" ? (
                    <RequestForm
                      datasetTitle={knessetResult.title || ""}
                      onClose={() => setRequestFormFor(null)}
                      sourceType="scraper"
                      sourceUrl={knessetResult.url}
                    />
                  ) : (
                    <button
                      className="btn-primary"
                      onClick={() => setRequestFormFor("knesset")}
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

        {/* Worker-declared source — one card serves all of them. */}
        {!loading && registryResult && (
          <RegistrySourceCard
            result={registryResult}
            formOpen={requestFormFor === (registryResult.source_id || "registry")}
            onOpenForm={() => setRequestFormFor(registryResult.source_id || "registry")}
            onCloseForm={() => setRequestFormFor(null)}
          />
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

        {!loading && results.length === 0 && matchedTracked.length === 0 && !govIlResult && !govMapResult && !idfResult && query && !error && (
          <div className="empty-state mb-2">{t("search.no_results")}</div>
        )}

        {/* Tracked Datasets Section. During an in-site keyword search this
            is filtered to the datasets that match (matchedTracked) so the
            search covers "my own site", not just data.gov.il; otherwise it
            lists everything tracked. */}
        <section aria-labelledby="tracked-heading" style={{ marginTop: "1rem" }}>
          <h2 id="tracked-heading" style={{ fontSize: "1.5rem", fontWeight: 700, marginBottom: "1rem" }}>
            {submittedQuery
              ? `${t("home.tracked_matches_title")} (${matchedTracked.length})`
              : t("home.tracked_title")}
          </h2>

          {trackedLoading ? (
            <div className="loading" role="status" aria-live="polite">{t("common.loading")}</div>
          ) : (submittedQuery ? matchedTracked.length === 0 : restTracked.length === 0 && !committeeGroup) ? (
            <div className="empty-state">
              {submittedQuery ? t("home.tracked_matches_empty") : t("home.no_tracked")}
            </div>
          ) : (
            <div className="grid grid-2">
              {!submittedQuery && committeeGroup && (
                <article key="knesset-committees" className="card" style={{ borderInlineStart: "3px solid #4f46e5" }}>
                  <div className="flex-between mb-1">
                    <h3 style={{ fontSize: "1rem", fontWeight: 600, margin: 0 }}>
                      <Link to="/knesset">🏛️ פרוטוקולי ועדות הכנסת</Link>
                    </h3>
                    <div className="flex" style={{ gap: "0.4rem", alignItems: "center" }}>
                      <span style={{ display: "inline-block", padding: "0.15rem 0.45rem", borderRadius: "9999px", fontSize: "0.65rem", fontWeight: 600, background: "#4f46e5", color: "white" }}>
                        כנסת
                      </span>
                      <span className="badge badge-info">{committeeGroup.count.toLocaleString()} ועדות</span>
                    </div>
                  </div>
                  <p className="text-sm text-muted mb-1" style={{ lineHeight: 1.6 }}>
                    כל ועדות הכנסת במקום אחד — חיפוש פרוטוקולים לפי שם ועדה, מספר כנסת וטקסט חופשי.
                    {committeeGroup.versions > 0 && <> · {committeeGroup.versions.toLocaleString()} גרסאות במעקב.</>}
                  </p>
                  <div className="flex mt-1" style={{ gap: "0.75rem", flexWrap: "wrap" }}>
                    <Link to="/knesset" className="btn-primary" style={{ textDecoration: "none", fontSize: "0.85rem", padding: "0.35rem 0.85rem" }}>
                      חיפוש פרוטוקולים ←
                    </Link>
                  </div>
                </article>
              )}
              {(submittedQuery ? matchedTracked : restTracked).map((ds) => (
                <article key={ds.id} className="card">
                  <div className="flex-between mb-1">
                    <h3 style={{ fontSize: "1rem", fontWeight: 600, margin: 0 }}>
                      <Link to={`/versions/${ds.id}`}>{ds.title}</Link>
                    </h3>
                    <div className="flex" style={{ gap: "0.4rem", alignItems: "center" }}>
                      <SourceChip
                        sourceType={ds.source_type}
                        organization={ds.organization}
                        ckanId={ds.ckan_id}
                      />
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
                      const dsBadge = sourceBadgeFor(ds.source_type, ds.organization, ds.ckan_id);
                      // Worker-declared sources label themselves from their manifest.
                      const linkLabel = dsBadge.sourceLinkLabel ?? t(dsBadge.sourceLinkKey);
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
