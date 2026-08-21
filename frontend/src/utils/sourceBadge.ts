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
    | "home.source_link_emun"
    | "home.source_link_servicescompass"
    | "home.source_link_mevaker"
    | "home.source_link_hatzav"
    | "home.source_link_mankal"
    | "home.source_link_cbs"
    | "home.source_link_jda"
    | "home.source_link_eden"
    | "home.source_link_knesset";
  /** Ready-made source-link label for a worker-declared source, whose text
   *  comes from its manifest rather than the bundle. When set, render this
   *  instead of translating `sourceLinkKey`. */
  sourceLinkLabel?: string;
}

/**
 * Badges for sources registered at runtime (GET /api/sources/registry),
 * keyed by ckan_id prefix ("<id>-scraper-").
 *
 * These sources have no code in this bundle — they're declared by a manifest
 * in the scraper worker — so their chip can't be a branch in the ladder
 * below. `primeRegistryBadges` fills this map once at boot; until it resolves
 * (or if the request fails) such a dataset falls through to the generic
 * GOV.IL scraper chip, which is a cosmetic miss, not a broken card.
 */
const runtimeBadges = new Map<string, SourceBadge>();

export interface RegistrySourceView {
  id: string;
  label_he: string;
  label_en: string;
  site_url: string;
  origin: string;
  ckan_id_prefix: string;
  badge: { bg: string; fg: string; accent: string; label: string };
  source_link_he: string;
  source_link_en: string;
  default_poll_interval: number;
  neon_eligible: boolean;
  spatial: boolean;
  /** Long-form Hebrew prose on how this source is tracked and why. Plain text
   *  with "## " headings and "- " bullets — never HTML, because it arrives from
   *  a worker's manifest. See SourceMethodology. */
  methodology_he?: string | null;
  /** The same account cut to a dataset page's narrower question — how current
   *  is this, and what is missing. Falls back to methodology_he when a source
   *  declares no short form. */
  methodology_short_he?: string | null;
}

/** Every registry source by id, for the pages that need more than its badge. */
const runtimeSources = new Map<string, RegistrySourceView>();

export function registrySource(id: string | undefined): RegistrySourceView | null {
  return (id && runtimeSources.get(id)) || null;
}

/**
 * Snap a worker-supplied badge colour onto the site's tint palette.
 *
 * A scraper source ships its own chip colours in its manifest, and those were
 * picked to look right on a white page — several land between 4.9:1 and 6.7:1,
 * and none of them follow the theme, so in dark mode they stayed pale-on-pale.
 * The manifest still chooses the HUE (that is how a reader tells sources
 * apart); the exact values come from the palette, where each pair is verified
 * at 7:1 or better in both themes (WCAG 1.4.6).
 *
 * An unrecognised colour falls back to the neutral tint rather than being
 * passed through — an unknown value is exactly the case we cannot vouch for.
 */
const TINT_BY_HUE: Record<string, string> = {
  // blues / skies
  "#e0f2fe": "sky", "#dbeafe": "sky", "#eff6ff": "sky", "#f0f9ff": "sky",
  // teals / cyans
  "#cffafe": "teal", "#ccfbf1": "teal", "#f0fdfa": "teal",
  // greens
  "#d1fae5": "good", "var(--tint-good-bg)": "good", "#ecfdf5": "good", "#f0fdf4": "good",
  // limes
  "#ecfccb": "lime", "#f7fee7": "lime",
  // violets / indigos
  "#ede9fe": "violet", "#f5f3ff": "violet", "#e0e7ff": "indigo", "#eef2ff": "indigo",
  // ambers / oranges
  "#ffedd5": "warn", "#fef3c7": "warn", "#fffbeb": "warn", "#fff7ed": "warn",
  "var(--tint-note-bg)": "note", "#fefce8": "note",
  // reds
  "var(--tint-bad-bg)": "bad", "#fef2f2": "bad", "#fff1f2": "bad",
  // pinks
  "#fce7f3": "pink", "#fdf2f8": "pink",
  // neutrals
  "#f1f5f9": "neutral", "#f8fafc": "neutral", "#e2e8f0": "neutral", "#f9fafb": "neutral",
};

function tintFor(bg: string): string {
  return TINT_BY_HUE[(bg || "").trim().toLowerCase()] || "neutral";
}

export function primeRegistryBadges(
  sources: RegistrySourceView[],
  lang: string,
): void {
  runtimeBadges.clear();
  runtimeSources.clear();
  for (const source of sources) {
    runtimeSources.set(source.id, source);
    runtimeBadges.set(source.ckan_id_prefix, {
      id: source.id,
      bg: `var(--tint-${tintFor(source.badge.bg)}-bg)`,
      fg: `var(--tint-${tintFor(source.badge.bg)}-fg)`,
      label: source.badge.label,
      accent: `var(--tint-${tintFor(source.badge.bg)}-bd)`,
      // Unused for these sources — sourceLinkLabel wins — but the field is
      // required, and the generic key is the honest fallback.
      sourceLinkKey: "home.source_link",
      sourceLinkLabel:
        lang === "en" ? source.source_link_en : source.source_link_he,
    });
  }
}

function registryBadgeFor(ckan_id: string | null | undefined): SourceBadge | null {
  if (!ckan_id) return null;
  for (const [prefix, badge] of runtimeBadges) {
    if (ckan_id.startsWith(prefix)) return badge;
  }
  return null;
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

// Same drift-immune rule for מערכת אמו"ן (govextra.gov.il/pmo/emun, the PMO
// government-decision follow-up dashboard). The ckan_id prefix
// "emun-scraper-" is the primary signal; "govextra.gov.il" is a safe org
// hint because that host serves only PMO mini-sites and no other OVER source
// is stamped with it — unlike a ministry slug, which would leak (lessons #2).
const EMUN_ORG_HINTS = ["govextra.gov.il"];

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

function looksLikeEmun(
  organization: string | null | undefined,
  ckan_id: string | null | undefined,
): boolean {
  if (ckan_id && ckan_id.startsWith("emun-scraper-")) return true;
  if (organization && EMUN_ORG_HINTS.includes(organization.toLowerCase())) return true;
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
      bg: "var(--tint-sky-bg)",
      fg: "var(--tint-sky-fg)",
      id: "govmap",
      label: "GOVMAP",
      accent: "var(--tint-sky-bd)",
      sourceLinkKey: "home.source_link_govmap",
    };
  }
  if (source_type === "cbs") {
    // Cyan pill for the CBS (למ"ס) content index — distinct from the sky-blue
    // GOVMAP chip so the two aren't confused.
    return {
      bg: "var(--tint-teal-bg)",
      fg: "var(--tint-teal-fg)",
      id: "cbs",
      label: 'למ"ס',
      accent: "var(--tint-teal-bd)",
      sourceLinkKey: "home.source_link_cbs",
    };
  }
  if (source_type === "scraper") {
    // Worker-declared sources first: their ckan_id prefixes are unique per
    // source and can't collide with the hardcoded ones below (OVER rejects a
    // manifest that claims a built-in source's id).
    const registered = registryBadgeFor(ckan_id);
    if (registered) return registered;
    if (looksLikeIdf(organization, ckan_id)) {
      return {
        // Saturated green per user request (#5d936c). The dark fg
        // colour we used on the prior light-mint background fails WCAG
        // AA contrast on this darker bg, so switch the chip text to
        // white — the chip is now distinctly readable as a filled
        // green pill instead of a tinted outline.
        bg: "var(--fill-good)",
        fg: "var(--on-fill)",
        id: "idf",
        label: "IDF.IL",
        accent: "var(--tint-teal-fg)",
        sourceLinkKey: "home.source_link_idf",
      };
    }
    if (looksLikeHealth(organization, ckan_id)) {
      // Purple pill per user request, for practitioners.health.gov.il.
      // bg/fg combo lands on WCAG AA (~7.5:1) so the label stays
      // readable on both light and dark page themes; accent matches
      // the left-rail colour used on the result card.
      return {
        bg: "var(--tint-violet-bg)",
        fg: "var(--tint-violet-fg)",
        id: "health",
        label: "PRACTITIONERS",
        accent: "var(--tint-violet-bd)",
        sourceLinkKey: "home.source_link_health",
      };
    }
    if (looksLikeRegistries(organization, ckan_id)) {
      // Teal pill for registries.health.gov.il, distinct from the purple
      // practitioners.health.gov.il chip. bg/fg lands on WCAG AA on both
      // light and dark themes; accent matches the card left-rail.
      return {
        bg: "var(--tint-teal-bg)",
        fg: "var(--tint-teal-fg)",
        id: "registries",
        label: "בריאות",
        accent: "var(--tint-teal-bd)",
        sourceLinkKey: "home.source_link_registries",
      };
    }
    if (looksLikeAvodata(organization, ckan_id)) {
      // Sky-blue pill for avodata.labor.gov.il, distinct from the
      // PRACTITIONERS purple and IDF green so the source family is
      // obvious at a glance.
      return {
        bg: "var(--tint-sky-bg)",
        fg: "var(--tint-sky-fg)",
        id: "avodata",
        label: "AVODATA",
        accent: "var(--tint-sky-bd)",
        sourceLinkKey: "home.source_link_avodata",
      };
    }
    if (looksLikeMunidata(organization, ckan_id)) {
      // Lime/olive pill for municipal-data.org ("מצב השלטון המקומי", Ministry
      // of Interior local-government dashboard), distinct from the emerald
      // mankal (#059669), teal registries (#14b8a6) and avodata sky-blue.
      return {
        bg: "var(--tint-lime-bg)",
        fg: "var(--tint-lime-fg)",
        id: "munidata",
        label: "מצב השלטון המקומי",
        accent: "var(--tint-lime-bd)",
        sourceLinkKey: "home.source_link_munidata",
      };
    }
    if (looksLikeEmun(organization, ckan_id)) {
      // Indigo pill for מערכת אמו"ן (govextra.gov.il/pmo/emun, PMO
      // government-decision follow-up), distinct from the munidata lime,
      // servicescompass amber, registries teal and avodata sky-blue.
      return {
        bg: "var(--tint-indigo-bg)",
        fg: "var(--tint-indigo-fg)",
        id: "emun",
        label: 'מערכת אמו"ן',
        accent: "var(--tint-indigo-bd)",
        sourceLinkKey: "home.source_link_emun",
      };
    }
    if (looksLikeServicescompass(organization, ckan_id)) {
      // Amber pill for gov.il/apps/servicescompass ("מצפן השירותים
      // הממשלתיים", National Digital Agency), distinct from avodata sky-blue,
      // munidata lime and mankal emerald.
      return {
        bg: "var(--tint-warn-bg)",
        fg: "var(--tint-warn-fg)",
        id: "servicescompass",
        label: "מצפן השירותים",
        accent: "var(--tint-warn-bd)",
        sourceLinkKey: "home.source_link_servicescompass",
      };
    }
    if (looksLikeMevaker(organization, ckan_id)) {
      // Deep-red pill for mevaker.gov.il (State Comptroller), distinct
      // from the other source families.
      return {
        bg: "var(--tint-bad-bg)",
        fg: "var(--tint-bad-fg)",
        id: "mevaker",
        label: "MEVAKER",
        accent: "var(--tint-bad-bd)",
        sourceLinkKey: "home.source_link_mevaker",
      };
    }
    if (looksLikeHatzav(organization, ckan_id)) {
      // Indigo pill for חצב (geo.mot.gov.il, Ministry of Transport map
      // viewer), distinct from the avodata sky-blue and the other
      // source families.
      return {
        bg: "var(--tint-indigo-bg)",
        fg: "var(--tint-indigo-fg)",
        id: "hatzav",
        label: "חצב",
        accent: "var(--tint-indigo-bd)",
        sourceLinkKey: "home.source_link_hatzav",
      };
    }
    if (looksLikeMankal(organization, ckan_id)) {
      // Emerald pill for חוזרי מנכ"ל (apps.education.gov.il, Ministry of
      // Education Director-General circulars), distinct from the other
      // source families.
      return {
        bg: "var(--tint-good-bg)",
        fg: "var(--tint-good-fg)",
        id: "mankal",
        label: "חוזרי מנכ\"ל",
        accent: "var(--tint-good-bd)",
        sourceLinkKey: "home.source_link_mankal",
      };
    }
    if (looksLikeJda(organization, ckan_id)) {
      // Rose/pink pill for jda.gov.il (הרשות לפיתוח ירושלים, Jerusalem
      // Development Authority tenders portal), distinct from the other
      // source families.
      return {
        bg: "var(--tint-pink-bg)",
        fg: "var(--tint-pink-fg)",
        id: "jda",
        label: "JDA",
        accent: "var(--tint-pink-bd)",
        sourceLinkKey: "home.source_link_jda",
      };
    }
    if (looksLikeEden(organization, ckan_id)) {
      // Orange pill for jeden.co.il (חברת עדן, Eden — Jerusalem municipal
      // development company; tenders + committee decisions), distinct from
      // the jda rose (#db2777) and the govil amber (#f59e0b).
      return {
        bg: "var(--tint-warn-bg)",
        fg: "var(--tint-warn-fg)",
        id: "eden",
        label: "EDEN",
        accent: "var(--tint-warn-bd)",
        sourceLinkKey: "home.source_link_eden",
      };
    }
    if (looksLikeKnesset(organization, ckan_id)) {
      // Indigo/blue pill for knesset.gov.il committee protocols, distinct
      // from the govmap sky (#0ea5e9), jda rose and govil amber.
      return {
        bg: "var(--tint-indigo-bg)",
        fg: "var(--tint-indigo-fg)",
        id: "knesset",
        label: "כנסת",
        accent: "var(--tint-indigo-bd)",
        sourceLinkKey: "home.source_link_knesset",
      };
    }
    return {
      bg: "var(--tint-warn-bg)",
      fg: "var(--tint-warn-fg)",
      id: "govil",
      label: "GOV.IL",
      accent: "var(--tint-warn-bd)",
      sourceLinkKey: "home.source_link_govil",
    };
  }
  // ckan (the default)
  return {
    bg: "var(--tint-teal-bg)",
    fg: "var(--tint-teal-fg)",
    id: "datagovil",
    label: "DATA.GOV.IL",
    accent: "var(--warning)",
    sourceLinkKey: "home.source_link",
  };
}

/**
 * The badge for a SOURCE KEY — the upstream-site id the server groups by
 * (app/services/source_load.source_key): "govmap", "ckan", "cbs", or a
 * scraper's ckan_id prefix ("munidata", "mankal", "idf", …).
 *
 * The server can enumerate the sources present in the catalog but has no
 * business knowing their colours or Hebrew names, which live here and in the
 * worker manifests. Feeding the key back through `sourceBadgeFor` as a
 * synthetic ckan_id is what lets a source added tomorrow — built-in ladder
 * entry OR runtime manifest — pick up its own chip with no code change here.
 */
export function sourceBadgeForKey(key: string): SourceBadge {
  if (key === "govmap" || key === "cbs") return sourceBadgeFor(key);
  if (key === "ckan" || key === "datagovil") return sourceBadgeFor("ckan");
  const badge = sourceBadgeFor("scraper", null, `${key}-scraper-x`);
  // A key we can't place — an unclassifiable row ("unknown"), or a manifest
  // source whose registry fetch hasn't landed — falls through the ladder to
  // the generic GOV.IL chip. In a LIST of sources that lie is worse than no
  // chip at all: three unrelated sources would all read "GOV.IL" and look
  // like duplicates. Wear the raw key instead, which is at least distinct.
  if (badge.id !== key) {
    return {
      id: key,
      bg: "var(--tint-neutral-bg)",
      fg: "var(--tint-neutral-fg)",
      label: key,
      accent: "var(--tint-neutral-bd)",
      sourceLinkKey: "home.source_link",
    };
  }
  return badge;
}
