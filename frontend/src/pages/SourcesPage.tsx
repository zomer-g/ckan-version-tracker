/**
 * Sources overview — like Organizations and Tags, but grouped by the DATA
 * SOURCE a dataset was collected from (GovMap, data.gov.il, GOV.IL, למ"ס,
 * חוזרי מנכ"ל, …) rather than by publishing organization.
 *
 * The source classification is the single source of truth in
 * utils/sourceBadge.ts (sourceBadgeFor) — the same helper that paints the
 * per-dataset chip everywhere. We group the tracked-dataset list by its
 * stable `id` slug, so a new source added there shows up here automatically.
 */
import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { useTranslation } from "react-i18next";
import { publicApi, TrackedDataset } from "../api/client";
import { sourceBadgeFor, type SourceBadge } from "../utils/sourceBadge";

interface SourceGroup {
  badge: SourceBadge;
  count: number;
}

export default function SourcesPage() {
  const { t } = useTranslation();
  const [datasets, setDatasets] = useState<TrackedDataset[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    publicApi
      .datasets()
      .then(setDatasets)
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  const groups = useMemo<SourceGroup[]>(() => {
    const map = new Map<string, SourceGroup>();
    for (const d of datasets) {
      const badge = sourceBadgeFor(d.source_type, d.organization, d.ckan_id);
      const g = map.get(badge.id);
      if (g) g.count += 1;
      else map.set(badge.id, { badge, count: 1 });
    }
    return [...map.values()].sort((a, b) => b.count - a.count);
  }, [datasets]);

  return (
    <div className="container mt-3">
      <div className="page-header">
        <h1>{t("sources.title", "מקורות")}</h1>
        <p className="text-muted text-sm">
          {t(
            "sources.subtitle",
            "המאגרים במעקב, מקובצים לפי מקור המידע שממנו נאספו",
          )}
        </p>
      </div>

      {loading ? (
        <div className="loading" role="status">
          {t("common.loading")}
        </div>
      ) : groups.length === 0 ? (
        <div className="empty-state">{t("sources.empty", "אין מקורות להצגה.")}</div>
      ) : (
        <div className="grid grid-2">
          {groups.map((g) => (
            <SourceCard key={g.badge.id} badge={g.badge} count={g.count} />
          ))}
        </div>
      )}
    </div>
  );
}

function SourceCard({ badge, count }: { badge: SourceBadge; count: number }) {
  const { t } = useTranslation();
  return (
    <Link
      to={`/sources/${badge.id}`}
      className="card"
      style={{
        textDecoration: "none",
        color: "inherit",
        display: "flex",
        gap: "0.75rem",
        alignItems: "center",
        borderInlineStart: `4px solid ${badge.accent}`,
      }}
    >
      {/* Source chip, same palette as the per-dataset badge, sized up as the
          card's "logo" so the sources read as a coherent family. */}
      <span
        style={{
          display: "inline-flex",
          alignItems: "center",
          justifyContent: "center",
          minWidth: 64,
          height: 40,
          padding: "0 0.6rem",
          borderRadius: 8,
          background: badge.bg,
          color: badge.fg,
          fontWeight: 700,
          fontSize: "0.8rem",
          flexShrink: 0,
        }}
      >
        {badge.label}
      </span>
      <div style={{ flex: 1, minWidth: 0 }}>
        <div className="text-sm text-muted">
          {count} {t("sources.datasets_count", "מאגרים במעקב")}
        </div>
      </div>
    </Link>
  );
}
