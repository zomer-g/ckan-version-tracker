import { useState, useEffect } from "react";
import { useTranslation } from "react-i18next";
import { useParams, Link, useNavigate } from "react-router-dom";
import { versions as versionsApi, datasets as datasetsApi, Version, TrackedDataset } from "../api/client";

const ODATA_BASE = "https://www.odata.org.il";

export default function VersionsPage() {
  const { t } = useTranslation();
  const { datasetId } = useParams<{ datasetId: string }>();
  const navigate = useNavigate();
  const [versionsList, setVersionsList] = useState<Version[]>([]);
  const [dataset, setDataset] = useState<TrackedDataset | null>(null);
  const [loading, setLoading] = useState(true);
  const [selected, setSelected] = useState<string[]>([]);

  useEffect(() => {
    if (!datasetId) return;
    Promise.all([
      versionsApi.list(datasetId),
      datasetsApi.list().then((all) => all.find((d) => d.id === datasetId) || null),
    ])
      .then(([versions, ds]) => {
        setVersionsList(versions);
        setDataset(ds);
      })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, [datasetId]);

  const toggleSelect = (id: string) => {
    setSelected((prev) => {
      if (prev.includes(id)) return prev.filter((v) => v !== id);
      if (prev.length >= 2) return [prev[1], id];
      return [...prev, id];
    });
  };

  const handleKeyDown = (e: React.KeyboardEvent, id: string) => {
    if (e.key === "Enter" || e.key === " ") {
      e.preventDefault();
      toggleSelect(id);
    }
  };

  const compare = () => {
    if (selected.length === 2) {
      navigate(`/diff/${datasetId}?from=${selected[0]}&to=${selected[1]}`);
    }
  };

  // ODATA links are now a single dataset-level link, not per-resource

  if (loading) return <div className="loading" role="status" aria-live="polite">{t("common.loading")}</div>;

  return (
    <div>
      <div className="page-header flex-between">
        <h1>{t("versions.title")}</h1>
        <div className="flex" style={{ alignItems: "center", gap: "1rem" }}>
          {dataset?.odata_dataset_id && (
            <a
              href={`${ODATA_BASE}/dataset/${dataset.odata_dataset_id}`}
              target="_blank"
              rel="noopener noreferrer"
              style={{
                textDecoration: "none",
                fontSize: "0.85rem",
                color: "var(--primary)",
                background: "none",
                border: "none",
                padding: 0,
                cursor: "pointer",
              }}
            >
              {t("tracked.view_on_odata")} ↗
            </a>
          )}
          <Link
            to="/tracked"
            style={{
              textDecoration: "none",
              fontSize: "0.85rem",
              color: "var(--text-muted, #64748b)",
              background: "none",
              border: "none",
              padding: 0,
            }}
          >
            ← {t("common.back")}
          </Link>
          {selected.length === 2 && (
            <button className="btn-primary" onClick={compare}>
              {t("versions.compare")}
            </button>
          )}
        </div>
      </div>

      <p className="text-sm text-muted mb-2">
        {t("versions.select_hint", "Select two versions to compare")}
      </p>

      {versionsList.length === 0 ? (
        <div className="empty-state">{t("versions.no_versions")}</div>
      ) : (
        <div style={{ display: "flex", flexDirection: "column", gap: "0.75rem" }} role="listbox" aria-multiselectable="true" aria-label={t("versions.title")}>
          {versionsList.map((v) => {
            const summary = v.change_summary;
            const isSelected = selected.includes(v.id);

            return (
              <div
                key={v.id}
                className="card"
                role="option"
                aria-selected={isSelected}
                tabIndex={0}
                onClick={() => toggleSelect(v.id)}
                onKeyDown={(e) => handleKeyDown(e, v.id)}
                style={{
                  borderColor: isSelected ? "var(--primary)" : undefined,
                  borderWidth: isSelected ? 2 : 1,
                  cursor: "pointer",
                }}
              >
                <div className="flex-between">
                  <div className="flex">
                    <h2 style={{ fontSize: "1rem", fontWeight: 600, margin: 0 }}>
                      {t("versions.version")} {v.version_number}
                    </h2>
                    <span className="text-sm text-muted">
                      {t("versions.detected")}: {new Date(v.detected_at).toLocaleString()}
                    </span>
                  </div>
                  <span className="text-sm text-muted">
                    {v.metadata_modified?.slice(0, 19)}
                  </span>
                </div>

                {summary && summary.type === "large_dataset" ? (
                  <div className="flex mt-1" style={{ gap: "0.5rem", flexWrap: "wrap" }}>
                    <span className="badge badge-info">{t("versions.large_dataset")}</span>
                    <span className="text-sm">
                      {summary.record_count?.toLocaleString()} {t("versions.rows")}
                      {summary.delta != null && summary.delta !== summary.record_count && (
                        <span style={{ color: summary.delta >= 0 ? "var(--success)" : "var(--danger)", marginInlineStart: "0.3rem" }}>
                          ({summary.delta >= 0 ? "+" : ""}{summary.delta.toLocaleString()})
                        </span>
                      )}
                    </span>
                  </div>
                ) : summary && (
                  <div className="flex mt-1">
                    {(summary.resources_modified?.length ?? 0) > 0 && (
                      <span className="badge badge-warning">
                        {summary.resources_modified!.length} {t("versions.resources_modified")}
                      </span>
                    )}
                    {(summary.resources_added?.length ?? 0) > 0 && (
                      <span className="badge badge-success">
                        {summary.resources_added!.length} {t("versions.resources_added")}
                      </span>
                    )}
                    {(summary.resources_removed?.length ?? 0) > 0 && (
                      <span className="badge badge-danger">
                        {summary.resources_removed!.length} {t("versions.resources_removed")}
                      </span>
                    )}
                  </div>
                )}

                {/* Single ODATA link per version */}
                {dataset?.odata_dataset_id && (
                  <div className="mt-1">
                    <a
                      href={`${ODATA_BASE}/dataset/${dataset.odata_dataset_id}`}
                      target="_blank"
                      rel="noopener noreferrer"
                      onClick={(e) => e.stopPropagation()}
                      className="text-sm"
                      style={{
                        color: "var(--primary)",
                        textDecoration: "none",
                      }}
                    >
                      {t("versions.view_on_odata")} ↗
                    </a>
                  </div>
                )}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
