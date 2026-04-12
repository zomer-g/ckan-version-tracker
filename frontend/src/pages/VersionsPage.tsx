import { useState, useEffect } from "react";
import { useTranslation } from "react-i18next";
import { useParams, Link } from "react-router-dom";
import { versions as versionsApi, publicApi, Version, TrackedDataset } from "../api/client";

const ODATA_BASE = "https://www.odata.org.il";

export default function VersionsPage() {
  const { t } = useTranslation();
  const { datasetId } = useParams<{ datasetId: string }>();
  const [versionsList, setVersionsList] = useState<Version[]>([]);
  const [dataset, setDataset] = useState<TrackedDataset | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!datasetId) return;
    Promise.all([
      versionsApi.list(datasetId),
      publicApi.datasets().then((all) => all.find((d) => d.id === datasetId) || null),
    ])
      .then(([versions, ds]) => {
        setVersionsList(versions);
        setDataset(ds);
      })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, [datasetId]);

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
              }}
            >
              {t("tracked.view_on_odata")} &#8599;
            </a>
          )}
          {dataset && (
            <a
              href={dataset.source_url || `https://data.gov.il/he/datasets/${dataset.organization}/${dataset.ckan_name}`}
              target="_blank"
              rel="noopener noreferrer"
              style={{
                textDecoration: "none",
                fontSize: "0.85rem",
                color: "var(--text-muted)",
              }}
            >
              {t("home.source_link")} &#8599;
            </a>
          )}
          <Link
            to="/"
            style={{
              textDecoration: "none",
              fontSize: "0.85rem",
              color: "var(--text-muted)",
            }}
          >
            &larr; {t("common.back")}
          </Link>
        </div>
      </div>

      {versionsList.length === 0 ? (
        <div className="empty-state">{t("versions.no_versions")}</div>
      ) : (
        <div style={{ display: "flex", flexDirection: "column", gap: "0.75rem" }} role="list" aria-label={t("versions.title")}>
          {versionsList.map((v) => {
            const summary = v.change_summary;

            return (
              <div
                key={v.id}
                className="card"
                role="listitem"
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

                {/* ODATA link — direct to resource if available */}
                {dataset?.odata_dataset_id && (() => {
                  // Find the first real odata resource_id from mappings
                  // Skip internal keys (_hashes, _resource_ids, _large_dataset_info)
                  const mappings = v.resource_mappings || {};
                  let odataResourceId: string | null = null;
                  for (const [key, val] of Object.entries(mappings)) {
                    if (key.startsWith("_")) continue;
                    if (typeof val === "string" && val.length > 10) {
                      odataResourceId = val;
                      break;
                    }
                  }
                  const href = odataResourceId
                    ? `${ODATA_BASE}/dataset/${dataset.odata_dataset_id}/resource/${odataResourceId}`
                    : `${ODATA_BASE}/dataset/${dataset.odata_dataset_id}`;
                  return (
                    <div className="mt-1">
                      <a
                        href={href}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="text-sm"
                        style={{ color: "var(--primary)", textDecoration: "none" }}
                      >
                        {t("versions.view_on_odata")} &#8599;
                      </a>
                    </div>
                  );
                })()}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
