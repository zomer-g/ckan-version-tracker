import { useTranslation } from "react-i18next";
import { CbsResult } from "../api/client";
import CbsResultCard from "./CbsResultCard";

// The admin-pinned quick-access strip shown at the top of the CBS page on the
// default (unsearched) view. Purely presentational: CbsPage owns the pinned
// records + pin/unpin handler and hides this strip the moment a search or
// filter is active. Each pinned page renders as a normal result card with an
// amber accent and (for admins) a filled star to unpin it.
interface Props {
  records: CbsResult[];
  canPin: boolean;
  pinBusy: string | null;
  onTogglePin: (record: CbsResult) => void;
}

export default function CbsFeatured({ records, canPin, pinBusy, onTogglePin }: Props) {
  const { t } = useTranslation();

  // Nothing pinned yet: admins still get a hint about how to pin; visitors see
  // nothing (the strip collapses).
  if (records.length === 0) {
    if (!canPin) return null;
    return (
      <section className="mb-2">
        <h2 style={{ fontSize: "1.05rem", fontWeight: 700, margin: "0 0 0.4rem" }}>
          {t("cbs.featured_title", "מועדפים")}
        </h2>
        <p className="text-sm text-muted">
          {t(
            "cbs.featured_admin_hint",
            'עדיין לא נעצת עמודים. חפש עמוד ולחץ על הכוכב (☆) כדי לנעוץ אותו כאן.'
          )}
        </p>
      </section>
    );
  }

  return (
    <section
      className="mb-2"
      aria-label={t("cbs.featured_title", "מועדפים")}
    >
      <h2 style={{ fontSize: "1.05rem", fontWeight: 700, margin: "0 0 0.6rem" }}>
        {t("cbs.featured_title", "מועדפים")}
      </h2>
      <div className="grid grid-2">
        {records.map((r) => (
          <CbsResultCard
            key={r.url}
            record={r}
            featured
            canPin={canPin}
            pinned
            busy={pinBusy === r.url}
            onTogglePin={onTogglePin}
          />
        ))}
      </div>
    </section>
  );
}
