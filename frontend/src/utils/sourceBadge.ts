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
    | "home.source_link_avodata";
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

// Same drift-immune rule for avodata. Only the exact "avodata.labor.gov.il"
// stamp the backend writes at create time — NOT "ministry-labor" or
// "labor", which are shared with regular gov.il collectors owned by
// the Ministry of Labor. The ckan_id prefix "avodata-scraper-" is the
// primary signal; this org hint is only a safety net for the same source.
const AVODATA_ORG_HINTS = ["avodata.labor.gov.il"];

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
      label: "GOVMAP",
      accent: "#0ea5e9",
      sourceLinkKey: "home.source_link_govmap",
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
        label: "PRACTITIONERS",
        accent: "#7c3aed",
        sourceLinkKey: "home.source_link_health",
      };
    }
    if (looksLikeAvodata(organization, ckan_id)) {
      // Sky-blue pill for avodata.labor.gov.il, distinct from the
      // PRACTITIONERS purple and IDF green so the source family is
      // obvious at a glance.
      return {
        bg: "#dbeafe",
        fg: "#1e40af",
        label: "AVODATA",
        accent: "#2563eb",
        sourceLinkKey: "home.source_link_avodata",
      };
    }
    return {
      bg: "#fef3c7",
      fg: "#92400e",
      label: "GOV.IL",
      accent: "#f59e0b",
      sourceLinkKey: "home.source_link_govil",
    };
  }
  // ckan (the default)
  return {
    bg: "#ccfbf1",
    fg: "#0f766e",
    label: "DATA.GOV.IL",
    accent: "var(--warning)",
    sourceLinkKey: "home.source_link",
  };
}
