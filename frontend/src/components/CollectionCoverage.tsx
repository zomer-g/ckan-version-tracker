import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { publicApi, CkanCoverage } from "../api/client";
import RequestForm from "./RequestForm";

interface CollectionCoverageProps {
  /** The CKAN package this dataset was cut from. */
  ckanId: string;
  /** Shown as the prefilled name in the request form. */
  collectionTitle: string;
}

/**
 * "This collection has more files — here they are."
 *
 * A data.gov.il package is a folder, and each of its files gets its own
 * dataset here. So arriving at one file's versions page used to be a dead
 * end for the obvious next question: what ELSE is in this collection, and
 * can I have it? The answer was reachable only by going back to search and
 * knowing to look, or by an admin editing the tracked-resource set — which
 * merges files INTO this dataset, the opposite of what the split model means.
 *
 * Renders nothing when the collection is fully collected, and nothing while
 * the answer is unknown: an empty card that might mean "nothing to add" and
 * might mean "the source did not answer" is worse than no card.
 */
export default function CollectionCoverage({
  ckanId,
  collectionTitle,
}: CollectionCoverageProps) {
  const { t } = useTranslation();
  const [coverage, setCoverage] = useState<CkanCoverage | null>(null);
  const [open, setOpen] = useState(false);

  useEffect(() => {
    let cancelled = false;
    publicApi
      .ckanCoverage(ckanId)
      .then((c) => {
        if (!cancelled) setCoverage(c);
      })
      // Best-effort: the source being unreachable must not put an error on a
      // page whose actual subject (the archive) loaded fine.
      .catch(() => {});
    return () => {
      cancelled = true;
    };
  }, [ckanId]);

  if (!coverage || coverage.free === 0) return null;

  const held = coverage.collected + coverage.pending;

  return (
    <div className="card" style={{ marginBottom: "1rem" }}>
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          gap: "0.75rem",
          flexWrap: "wrap",
        }}
      >
        <div>
          <div style={{ fontWeight: 600, fontSize: "0.95rem" }}>
            {t("versions.collection_more_title", { n: coverage.free })}
          </div>
          <div className="text-sm text-muted" style={{ marginTop: "0.2rem" }}>
            {t("versions.collection_more_body", {
              total: coverage.total,
              held,
              free: coverage.free,
            })}
          </div>
        </div>
        {!open && (
          <button className="btn-primary" onClick={() => setOpen(true)} style={{ fontSize: "0.85rem" }}>
            {t("versions.collection_more_btn")}
          </button>
        )}
      </div>

      {open && (
        <div style={{ marginTop: "0.75rem" }}>
          <RequestForm
            ckanId={coverage.ckan_id}
            datasetTitle={coverage.title || collectionTitle}
            availableResources={coverage.resources.map((r) => ({
              id: r.id,
              name: r.name,
              format: r.format || undefined,
            }))}
            onClose={() => setOpen(false)}
          />
        </div>
      )}
    </div>
  );
}
