/**
 * Single source of truth for the per-dataset "where does this come from"
 * chip + source-link label.
 *
 * Without this helper, every page that renders a dataset card duplicated
 * the same `source_type === "scraper" ? "GOV.IL" : ...` branch, which is
 * why IDF datasets initially showed up wearing a "GOV.IL" badge — the
 * scraper branch couldn't tell the two origins apart.
 *
 * For the IDF-vs-gov.il split we look at multiple signals because the
 * obvious one — TrackedDataset.organization — can drift: admins
 * routinely reassign datasets to a real Organization entity (e.g.
 * "israel_defense_forces") after the request is approved, overwriting
 * the "idf.il" / "gov.il" string the backend stamped at create time.
 * The ckan_id prefix (set at create time and never changed) is the
 * most reliable marker.
 */

export interface SourceBadge {
  /** Stable, URL-safe source id — the grouping key for the Sources page
   *  (/sources/:id). Unlike `label` (Hebrew/quotes/spaces on some sources)
   *  this is a clean ASCII slug and never changes. */
  id: string;
  /** background colour of the chip */
  bg: string;
  /** text colour of the chip */
  fg: string;
  /** chip label shown to the user (e.g. "GOV.IL", "IDF.IL", "PRACTITIONERS") */
  label: string;
  /** accent colour for borders / left-rails on the dataset card */
  accent: string;
  /** i18n key for the "source link" anchor under the card */
  sourceLinkKey:
    | "home.source_link"
    | "home.source_link_govil"
    | "home.source_link_govmap"
    | "home.source_link_idf"
    | "home.source_link_health"
    | "home.source_link_registries"
    | "home.source_link_avodata"
    | "home.source_link_munidata"
    | "home.source_link_servicescompass"
    | "home.source_link_mevaker"
    | "home.source_link_hatzav"
    | "home.source_link_mankal"
    | "home.source_link_cbs"
    | "home.source_link_jda"
    | "home.source_link_eden"
    | "home.source_link_knesset";
}

const IDF_ORG_HINTS = ["idf.il", "israel_defense_forces", "idf"];

// Hints we accept for the practitioners.health.gov.il scraper. ONLY
// values unique to this source belong here — generic Ministry-of-Health
// slugs like "ministry-health" or "health" must NOT be added because
// they are shared with regular gov.il collectors that happen to belong
// to the health ministry (e.g. /he/collectors/publications/...), and
// any such ambiguity would mislabel them as PRACTITIONERS. The string
// "practitioners.health.gov.il" is the exact organization slug the
// backend stamps at create time in app/api/datasets.py for this
// source and nothing else uses it.
const HEALTH_ORG_HINTS = ["practitioners.health.gov.il"];

// Same drift-immune rule for registries.health.gov.il (משרד הבריאות
// "מאגרי מידע") — only the exact "registries.health.gov.il" stamp the
// backend writes at create time, NOT a generic Ministry-of-Health slug
// shared with regular gov.il collectors or the practitioners source. The
// ckan_id prefix "registries-scraper-" is the primary signal.
const REGISTRIES_ORG_HINTS = ["registries.health.gov.il"];

// Same drift-immune rule for avodata. Only the exact "avodata.labor.gov.il"
// stamp the backend writes at create time — NOT "ministry-labor" or
// "labor", which are shared with regular gov.il collectors owned by
// the Ministry of Labor. The ckan_id prefix "avodata-scraper-" is the
// primary signal; this org hint is only a safety net for the same source.
const AVODATA_ORG_HINTS = ["avodata.labor.gov.il"];

// Same drift-immune rule for municipal-data.org ("מצב השלטון המקומי", the
// Ministry of Interior local-government dashboard) — only the exact
// "municipal-data.org" stamp the backend writes at create time. The ckan_id
// prefix "munidata-scraper-" is the primary signal.
const MUNIDATA_ORG_HINTS = ["municipal-data.org"];

// gov.il/apps/servicescompass ("מצפן השירותים הממשלתיים", National Digital
// Agency). Detection keys ONLY on the drift-immune ckan_id prefix
// "servicescompass-scraper-": the host it lives on (www.gov.il) is shared
// with the generic gov.il scraper, so using it as an org hint would leak
// this badge onto every gov.il dataset (lessons #2). Hence no org hints.
const SERVICESCOMPASS_ORG_HINTS: string[] = [];

// Same drift-immune rule for mevaker — only the exact "mevaker.gov.il"
// stamp the backend writes at create time. The ckan_id prefix
// "mevaker-scraper-" is the primary signal.
const MEVAKER_ORG_HINTS = ["mevaker.gov.il"];

// Same drift-immune rule for hatzav (חצב, geo.mot.gov.il) — only the
// exact "geo.mot.gov.il" stamp the backend writes at create time, NOT a
// generic Ministry-of-Transport slug shared with regular gov.il
// collectors. The ckan_id prefix "hatzav-scraper-" is the primary signal.
const HATZAV_ORG_HINTS = ["geo.mot.gov.il"];

// Same drift-immune rule for חוזרי מנכ"ל (apps.education.gov.il/Mankal) —
// only the exact "apps.education.gov.il" stamp the backend writes at
// create time, NOT a generic Ministry-of-Education slug shared with
// regular gov.il collectors. The ckan_id prefix "mankal-scraper-" is the
// primary signal.
const MANKAL_ORG_HINTS = ["apps.education.gov.il"];

// Same drift-immune rule for jda.gov.il (הרשות לפיתוח ירושלים / Jerusalem
// Development Authority tenders portal) — only the exact "jda.gov.il"
// stamp the backend writes at create time, NOT a generic ministry slug
// shared with regular gov.il collectors. The ckan_id prefix
// "jda-scraper-" is the primary signal.
const JDA_ORG_HINTS = ["jda.gov.il"];

// Same drift-immune rule for jeden.co.il (חברת עדן / Eden, the Jerusalem
// municipal development company — tenders + committee decisions) — only
// the exact "jeden.co.il" stamp the backend writes at create time, NOT a
// generic municipal slug shared with other sources. The ckan_id prefix
// "eden-scraper-" is the primary signal.
const EDEN_ORG_HINTS = ["jeden.co.il"];

function looksLikeIdf(
  organization: string | null | undefined,
  ckan_id: string | null | undefined,
): boolean {
  if (ckan_id && ckan_id.startsWith("idf-scraper-")) return true;
  if (organization && IDF_ORG_HINTS.includes(organization.toLowerCase())) return true;
  return false;
}

function looksLikeHealth(
  organization: string | null | undefined,
  ckan_id: string | null | undefined,
): boolean {
  // ckan_id is set at create time and never changes (mirror of the
  // IDF check above). For datasets created by the health.gov.il
  // parser in app/api/datasets.py this prefix is the authoritative
  // signal that the dataset came from the practitioners portal.
  if (ckan_id && ckan_id.startsWith("health-scraper-")) return true;
  if (organization && HEALTH_ORG_HINTS.includes(organization.toLowerCase())) return true;
  return false;
}

function looksLikeRegistries(
  organization: string | null | undefined,
  ckan_id: string | null | undefined,
): boolean {
  // Same drift-immune scheme as health: the ckan_id prefix is stamped at
  // create time and never changes, so it's the authoritative signal that
  // the dataset came from registries.health.gov.il.
  if (ckan_id && ckan_id.startsWith("registries-scraper-")) return true;
  if (organization && REGISTRIES_ORG_HINTS.includes(organization.toLowerCase())) return true;
  return false;
}

function looksLikeAvodata(
  organization: string | null | undefined,
  ckan_id: string | null | undefined,
): boolean {
  // Same drift-immune scheme as IDF + health: ckan_id is the
  // primary signal and never changes after creation.
  if (ckan_id && ckan_id.startsWith("avodata-scraper-")) return true;
  if (organization && AVODATA_ORG_HINTS.includes(organization.toLowerCase())) return true;
  return false;
}

function looksLikeMunidata(
  organization: string | null | undefined,
  ckan_id: string | null | undefined,
): boolean {
  if (ckan_id && ckan_id.startsWith("munidata-scraper-")) return true;
  if (organization && MUNIDATA_ORG_HINTS.includes(organization.toLowerCase())) return true;
  return false;
}

function looksLikeServicescompass(
  organization: string | null | undefined,
  ckan_id: string | null | undefined,
): boolean {
  // ckan_id prefix only — the host is shared with the gov.il scraper, so
  // there is no org hint safe to match on (SERVICESCOMPASS_ORG_HINTS is
  // intentionally empty; kept for symmetry with the other detectors).
  if (ckan_id && ckan_id.startsWith("servicescompass-scraper-")) return true;
  if (organization && SERVICESCOMPASS_ORG_HINTS.includes(organization.toLowerCase())) return true;
  return false;
}

function looksLikeMevaker(
  organization: string | null | undefined,
  ckan_id: string | null | undefined,
): boolean {
  if (ckan_id && ckan_id.startsWith("mevaker-scraper-")) return true;
  if (organization && MEVAKER_ORG_HINTS.includes(organization.toLowerCase())) return true;
  return false;
}

function looksLikeHatzav(
  organization: string | null | undefined,
  ckan_id: string | null | undefined,
): boolean {
  if (ckan_id && ckan_id.startsWith("hatzav-scraper-")) return true;
  if (organization && HATZAV_ORG_HINTS.includes(organization.toLowerCase())) return true;
  return false;
}

function looksLikeMankal(
  organization: string | null | undefined,
  ckan_id: string | null | undefined,
): boolean {
  if (ckan_id && ckan_id.startsWith("mankal-scraper-")) return true;
  if (organization && MANKAL_ORG_HINTS.includes(organization.toLowerCase())) return true;
  return false;
}

function looksLikeJda(
  organization: string | null | undefined,
  ckan_id: string | null | undefined,
): boolean {
  if (ckan_id && ckan_id.startsWith("jda-scraper-")) return true;
  if (organization && JDA_ORG_HINTS.includes(organization.toLowerCase())) return true;
  return false;
}

function looksLikeEden(
  organization: string | null | undefined,
  ckan_id: string | null | undefined,
): boolean {
  if (ckan_id && ckan_id.startsWith("eden-scraper-")) return true;
  if (organization && EDEN_ORG_HINTS.includes(organization.toLowerCase())) return true;
  return false;
}

// Same drift-immune rule for knesset.gov.il committee protocols. The org
// hint is ONLY the exact "knesset.gov.il" stamp the backend writes at create
// time; "knesset-scraper-" is the primary signal.
const KNESSET_ORG_HINTS = ["knesset.gov.il"];

function looksLikeKnesset(
  organization: string | null | undefined,
  ckan_id: string | null | undefined,
): boolean {
  if (ckan_id && ckan_id.startsWith("knesset-scraper-")) return true;
  if (organization && KNESSET_ORG_HINTS.includes(organization.toLowerCase())) return true;
  return false;
}

/**
 * @param source_type — TrackedDataset.source_type ("ckan" | "scraper" | "govmap")
 * @param organization — TrackedDataset.organization slug. May be the
 *   raw "idf.il"/"gov.il" the backend stamped at create time, OR a
 *   real Organization entity slug an admin reassigned to (e.g.
 *   "israel_defense_forces"). Both are recognised; falsy values fall
 *   back to ckan_id detection.
 * @param ckan_id — TrackedDataset.ckan_id (stable since create). The
 *   most reliable signal for the IDF-vs-gov.il split because it
 *   doesn't drift when admins reassign organizations.
 */
export function sourceBadgeFor(
  source_type: string | null | undefined,
  organization: string | null | undefined = null,
  ckan_id: string | null | undefined = null,
): SourceBadge {
  if (source_type === "govmap") {
    return {
      bg: "#e0f2fe",
      fg: "#075985",
      id: "govmap",
      label: "GOVMAP",
      accent: "#0ea5e9",
      sourceLinkKey: "home.source_link_govmap",
    };
  }
  if (source_type === "cbs") {
    // Cyan pill for the CBS (למ"ס) content index — distinct from the sky-blue
    // GOVMAP chip so the two aren't confused.
    return {
      bg: "#cffafe",
      fg: "#155e75",
      id: "cbs",
      label: 'למ"ס',
      accent: "#06b6d4",
      sourceLinkKey: "home.source_link_cbs",
    };
  }
  if (source_type === "scraper") {
    if (looksLikeIdf(organization, ckan_id)) {
      return {
        // Saturated green per user request (#5d936c). The dark fg
        // colour we used on the prior light-mint background fails WCAG
        // AA contrast on this darker bg, so switch the chip text to
        // white — the chip is now distinctly readable as a filled
        // green pill instead of a tinted outline.
        bg: "#5d936c",
        fg: "#ffffff",
        id: "idf",
        label: "IDF.IL",
        accent: "#0f766e",
        sourceLinkKey: "home.source_link_idf",
      };
    }
    if (looksLikeHealth(organization, ckan_id)) {
      // Purple pill per user request, for practitioners.health.gov.il.
      // bg/fg combo lands on WCAG AA (~7.5:1) so the label stays
      // readable on both light and dark page themes; accent matches
      // the left-rail colour used on the result card.
      return {
        bg: "#ede9fe",
        fg: "#5b21b6",
        id: "health",
        label: "PRACTITIONERS",
        accent: "#7c3aed",
        sourceLinkKey: "home.source_link_health",
      };
    }
    if (looksLikeRegistries(organization, ckan_id)) {
      // Teal pill for registries.health.gov.il, distinct from the purple
      // practitioners.health.gov.il chip. bg/fg lands on WCAG AA on both
      // light and dark themes; accent matches the card left-rail.
      return {
        bg: "#ccfbf1",
        fg: "#115e59",
        id: "registries",
        label: "בריאות",
        accent: "#14b8a6",
        sourceLinkKey: "home.source_link_registries",
      };
    }
    if (looksLikeAvodata(organization, ckan_id)) {
      // Sky-blue pill for avodata.labor.gov.il, distinct from the
      // PRACTITIONERS purple and IDF green so the source family is
      // obvious at a glance.
      return {
        bg: "#dbeafe",
        fg: "#1e40af",
        id: "avodata",
        label: "AVODATA",
        accent: "#2563eb",
        sourceLinkKey: "home.source_link_avodata",
      };
    }
    if (looksLikeMunidata(organization, ckan_id)) {
      // Lime/olive pill for municipal-data.org ("מצב השלטון המקומי", Ministry
      // of Interior local-government dashboard), distinct from the emerald
      // mankal (#059669), teal registries (#14b8a6) and avodata sky-blue.
      return {
        bg: "#ecfccb",
        fg: "#3f6212",
        id: "munidata",
        label: "מצב השלטון המקומי",
        accent: "#65a30d",
        sourceLinkKey: "home.source_link_munidata",
      };
    }
    if (looksLikeServicescompass(organization, ckan_id)) {
      // Amber pill for gov.il/apps/servicescompass ("מצפן השירותים
      // הממשלתיים", National Digital Agency), distinct from avodata sky-blue,
      // munidata lime and mankal emerald.
      return {
        bg: "#ffedd5",
        fg: "#9a3412",
        id: "servicescompass",
        label: "מצפן השירותים",
        accent: "#ea580c",
        sourceLinkKey: "home.source_link_servicescompass",
      };
    }
    if (looksLikeMevaker(organization, ckan_id)) {
      // Deep-red pill for mevaker.gov.il (State Comptroller), distinct
      // from the other source families.
      return {
        bg: "#fee2e2",
        fg: "#991b1b",
        id: "mevaker",
        label: "MEVAKER",
        accent: "#dc2626",
        sourceLinkKey: "home.source_link_mevaker",
      };
    }
    if (looksLikeHatzav(organization, ckan_id)) {
      // Indigo pill for חצב (geo.mot.gov.il, Ministry of Transport map
      // viewer), distinct from the avodata sky-blue and the other
      // source families.
      return {
        bg: "#e0e7ff",
        fg: "#3730a3",
        id: "hatzav",
        label: "חצב",
        accent: "#4f46e5",
        sourceLinkKey: "home.source_link_hatzav",
      };
    }
    if (looksLikeMankal(organization, ckan_id)) {
      // Emerald pill for חוזרי מנכ"ל (apps.education.gov.il, Ministry of
      // Education Director-General circulars), distinct from the other
      // source families.
      return {
        bg: "#d1fae5",
        fg: "#065f46",
        id: "mankal",
        label: "חוזרי מנכ\"ל",
        accent: "#059669",
        sourceLinkKey: "home.source_link_mankal",
      };
    }
    if (looksLikeJda(organization, ckan_id)) {
      // Rose/pink pill for jda.gov.il (הרשות לפיתוח ירושלים, Jerusalem
      // Development Authority tenders portal), distinct from the other
      // source families.
      return {
        bg: "#fce7f3",
        fg: "#9d174d",
        id: "jda",
        label: "JDA",
        accent: "#db2777",
        sourceLinkKey: "home.source_link_jda",
      };
    }
    if (looksLikeEden(organization, ckan_id)) {
      // Orange pill for jeden.co.il (חברת עדן, Eden — Jerusalem municipal
      // development company; tenders + committee decisions), distinct from
      // the jda rose (#db2777) and the govil amber (#f59e0b).
      return {
        bg: "#ffedd5",
        fg: "#9a3412",
        id: "eden",
        label: "EDEN",
        accent: "#ea580c",
        sourceLinkKey: "home.source_link_eden",
      };
    }
    if (looksLikeKnesset(organization, ckan_id)) {
      // Indigo/blue pill for knesset.gov.il committee protocols, distinct
      // from the govmap sky (#0ea5e9), jda rose and govil amber.
      return {
        bg: "#e0e7ff",
        fg: "#3730a3",
        id: "knesset",
        label: "כנסת",
        accent: "#4f46e5",
        sourceLinkKey: "home.source_link_knesset",
      };
    }
    return {
      bg: "#fef3c7",
      fg: "#92400e",
      id: "govil",
      label: "GOV.IL",
      accent: "#f59e0b",
      sourceLinkKey: "home.source_link_govil",
    };
  }
  // ckan (the default)
  return {
    bg: "#ccfbf1",
    fg: "#0f766e",
    id: "datagovil",
    label: "DATA.GOV.IL",
    accent: "var(--warning)",
    sourceLinkKey: "home.source_link",
  };
}
