import { useState } from "react";
import { useTranslation } from "react-i18next";
import { lookupLinkFor, TrackedDataset } from "../api/client";

/**
 * Copies this dataset's SOURCE-URL deep link (/lookup?url=…) rather than its
 * /versions/<uuid> address.
 *
 * The two differ in what they survive. A UUID link is this row in this
 * database; the source-URL link is the thing itself, so it keeps working if
 * the dataset is re-created, and it degrades into the collection request form
 * instead of a dead page. It's also the link you can hand to someone who only
 * has the government URL — they don't need to know OVER's id for it.
 */
export default function CopyLookupLinkButton({
  dataset,
}: {
  dataset: TrackedDataset;
}) {
  const { t } = useTranslation();
  const [copied, setCopied] = useState(false);

  // Same fallback as the "source" link in the page header: CKAN datasets are
  // tracked per resource and carry no source_url, so rebuild the package
  // permalink — the resolver matches it back by package name.
  const sourceUrl =
    dataset.source_url ||
    (dataset.organization && dataset.ckan_name
      ? `https://data.gov.il/he/datasets/${dataset.organization}/${dataset.ckan_name}`
      : "");
  if (!sourceUrl) return null;

  const copy = async () => {
    try {
      await navigator.clipboard.writeText(lookupLinkFor(sourceUrl));
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch {
      /* clipboard blocked (insecure context / permission) — no-op */
    }
  };

  return (
    <button
      type="button"
      onClick={copy}
      title={t("lookup.copy_link_title")}
      style={{
        fontSize: "0.85rem",
        padding: "0.3rem 0.7rem",
        background: "none",
        border: "1px solid var(--border, var(--border))",
        color: "var(--text-muted)",
        borderRadius: 4,
        cursor: "pointer",
      }}
    >
      {copied ? t("lookup.copied") : `🔗 ${t("lookup.copy_link")}`}
    </button>
  );
}
