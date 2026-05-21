/**
 * Single source of truth for the per-dataset "where does this come from"
 * chip + source-link label.
 *
 * Without this helper, every page that renders a dataset card duplicated
 * the same `source_type === "scraper" ? "GOV.IL" : ...` branch, which is
 * why IDF datasets initially showed up wearing a "GOV.IL" badge — the
 * scraper branch couldn't tell the two origins apart. Branching on
 * `organization` here (which the backend sets to "gov.il" or "idf.il" at
 * dataset-create time) is the cheapest reliable signal.
 */

export interface SourceBadge {
  /** background colour of the chip */
  bg: string;
  /** text colour of the chip */
  fg: string;
  /** chip label shown to the user (e.g. "GOV.IL", "IDF.IL") */
  label: string;
  /** accent colour for borders / left-rails on the dataset card */
  accent: string;
  /** i18n key for the "source link" anchor under the card */
  sourceLinkKey: "home.source_link" | "home.source_link_govil" | "home.source_link_govmap" | "home.source_link_idf";
}

/**
 * @param source_type — TrackedDataset.source_type ("ckan" | "scraper" | "govmap")
 * @param organization — TrackedDataset.organization slug; only used to
 *   split the "scraper" bucket into gov.il vs idf.il. Falsy values fall
 *   back to the legacy gov.il palette so existing scraper datasets
 *   (created before the IDF code path) keep their old appearance.
 */
export function sourceBadgeFor(
  source_type: string | null | undefined,
  organization: string | null | undefined = null,
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
    if (organization === "idf.il") {
      return {
        bg: "#ccfbf1",
        fg: "#115e59",
        label: "IDF.IL",
        accent: "#0f766e",
        sourceLinkKey: "home.source_link_idf",
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
