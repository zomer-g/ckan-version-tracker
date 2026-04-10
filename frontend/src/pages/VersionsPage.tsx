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

  /** Get ODATA resource links from version's resource_mappings */
  const getOdataLinks = (v: Version): { name: string; url: string }[] => {
    const mappings = v.resource_mappings || {};
    const links: { name: string; url: string }[] = [];

    // Metadata resource
    if (v.odata_metadata_resource_id) {
      links.push({
        name: `v${v.version_number} metadata`,
        url: `${ODATA_BASE}/dataset/${dataset?.odata_dataset_id}/resource/${v.odata_metadata_resource_id}`,
      });
    }

    // Data resources (skip internal keys starting with _)
    for (const [key, value] of Object.entries(mappings)) {
      if (key.startsWith("_") || typeof value !== "string") continue;
      links.push({
        name: key.slice(0, 12) + "...",
        url: `${ODATA_BASE}/dataset/${dataset?.odata_dataset_id}/resource/${value}`,
      });
    }

    return links;
  };

  if (loading) return <div className="loading" role="status" aria-live="polite">{t("common.loading")}</div>;

  return (
    <div>
      <div className="page-header flex-between">
        <h1>{t("versions.title")}</h1>
        <div className="flex">
          {dataset?.odata_dataset_id && (
            <a
              href={`${ODATA_BASE}/dataset/${dataset.odata_dataset_id}`}
              target="_blank"
              rel="noopener noreferrer"
              className="btn-secondary"
              style={{ textDecoration: "none", fontSize: "0.8rem" }}
            >
              {t("tracked.view_on_odata")} ↗
            </a>
          )}
          <Link to="/tracked" className="btn-secondary" style={{ textDecoration: "none" }}>
            {t("common.back")}
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
            const odataLinks = dataset?.odata_dataset_id ? getOdataLinks(v) : [];

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

                {summary && (
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

                {/* ODATA direct links */}
                {odataLinks.length > 0 && (
                  <div className="mt-1" style={{ display: "flex", gap: "0.5rem", flexWrap: "wrap" }}>
                    {odataLinks.map((link, i) => (
                      <a
                        key={i}
                        href={link.url}
                        target="_blank"
                        rel="noopener noreferrer"
                        onClick={(e) => e.stopPropagation()}
                        style={{
                          fontSize: "0.75rem",
                          padding: "0.15rem 0.5rem",
                          background: "#dbeafe",
                          color: "#1e40af",
                          borderRadius: "4px",
                          textDecoration: "none",
                        }}
                      >
                        {t("versions.view_on_odata")} ↗
                      </a>
                    ))}
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
