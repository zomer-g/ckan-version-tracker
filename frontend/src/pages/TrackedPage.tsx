import { useState, useEffect } from "react";
import { useTranslation } from "react-i18next";
import { Link } from "react-router-dom";
import {
  datasets as datasetsApi,
  TrackedDataset,
} from "../api/client";
import { useAuth } from "../auth/AuthContext";

function formatInterval(seconds: number, t: (k: string) => string): string {
  if (seconds < 60) return `${seconds} ${t("tracked.seconds")}`;
  const minutes = Math.round(seconds / 60);
  if (minutes < 60) return `${minutes} ${t("tracked.minutes")}`;
  const hours = Math.round(minutes / 60);
  if (hours < 24) return `${hours} ${t("tracked.hours")}`;
  const days = Math.round(hours / 24);
  if (days < 7) return `${days} ${t("tracked.days")}`;
  const weeks = Math.round(days / 7);
  return `${weeks} ${t("tracked.weeks")}`;
}

const INTERVAL_OPTIONS = [
  { value: 900, labelKey: "tracked.interval_15min" },
  { value: 3600, labelKey: "tracked.interval_1hour" },
  { value: 43200, labelKey: "tracked.interval_12hour" },
  { value: 86400, labelKey: "tracked.interval_1day" },
  { value: 604800, labelKey: "tracked.interval_1week" },
];

export default function TrackedPage() {
  const { t } = useTranslation();
  const { user } = useAuth();
  const isAdmin = !!user?.is_admin;
  const [datasets, setDatasets] = useState<TrackedDataset[]>([]);
  const [loading, setLoading] = useState(true);
  const [polling, setPolling] = useState<Set<string>>(new Set());
  const [editingInterval, setEditingInterval] = useState<string | null>(null);

  useEffect(() => {
    loadDatasets();
  }, []);

  const loadDatasets = async () => {
    setLoading(true);
    try {
      const data = await datasetsApi.list();
      setDatasets(data);
    } catch {}
    setLoading(false);
  };

  const pollNow = async (id: string) => {
    setPolling((prev) => new Set(prev).add(id));
    try {
      await datasetsApi.poll(id);
    } catch {}
    setPolling((prev) => {
      const next = new Set(prev);
      next.delete(id);
      return next;
    });
    // Reload after a short delay to show updated last_polled_at
    setTimeout(loadDatasets, 2000);
  };

  const updateInterval = async (id: string, interval: number) => {
    try {
      const updated = await datasetsApi.update(id, { poll_interval: interval });
      setDatasets((prev) => prev.map((d) => (d.id === id ? { ...d, poll_interval: updated.poll_interval } : d)));
    } catch {}
    setEditingInterval(null);
  };

  const untrack = async (id: string) => {
    try {
      await datasetsApi.untrack(id);
      setDatasets((prev) => prev.filter((d) => d.id !== id));
    } catch {}
  };

  if (loading) return <div className="loading" role="status" aria-live="polite">{t("common.loading")}</div>;

  if (datasets.length === 0) {
    return (
      <div>
        <div className="page-header">
          <h1>{t("tracked.title")}</h1>
        </div>
        <div className="empty-state">{t("tracked.empty")}</div>
      </div>
    );
  }

  return (
    <div>
      <div className="page-header">
        <h1>{t("tracked.title")}</h1>
      </div>

      <div className="grid grid-2">
        {datasets.map((ds) => (
          <article key={ds.id} className="card">
            <div className="flex-between mb-1">
              <h2 style={{ fontSize: "1rem", fontWeight: 600, margin: 0 }}>
                <Link to={`/versions/${ds.id}`}>{ds.title}</Link>
              </h2>
              <span
                className={`badge ${
                  ds.status === "pending"
                    ? "badge-warning"
                    : ds.status === "rejected"
                    ? "badge-danger"
                    : ds.is_active
                    ? "badge-success"
                    : "badge-warning"
                }`}
                role="status"
              >
                {ds.status === "pending"
                  ? t("tracked.pending")
                  : ds.status === "rejected"
                  ? t("tracked.rejected")
                  : ds.is_active
                  ? t("tracked.active")
                  : t("tracked.paused")}
              </span>
            </div>

            {ds.resource_name && (
              <p className="text-sm mb-1" style={{ color: "var(--primary)", fontWeight: 500 }}>
                {t("tracked.resource", "\u05DE\u05E9\u05D0\u05D1")}: {ds.resource_name}
              </p>
            )}

            <p className="text-sm text-muted mb-1">
              {ds.organization}
              {ds.requester_name && (
                <span> · {t("admin.requester")}: {ds.requester_name}</span>
              )}
            </p>

            <div className="text-sm mb-1">
              <div className="flex" style={{ gap: "0.5rem" }}>
                {t("tracked.poll_interval")}:{" "}
                {isAdmin ? (
                  editingInterval === ds.id ? (
                    <select
                      value={ds.poll_interval}
                      onChange={(e) => updateInterval(ds.id, Number(e.target.value))}
                      onBlur={() => setEditingInterval(null)}
                      autoFocus
                      style={{ width: "auto", padding: "0.2rem 0.4rem", fontSize: "0.8rem" }}
                      aria-label={t("tracked.poll_interval")}
                    >
                      {INTERVAL_OPTIONS.map((opt) => (
                        <option key={opt.value} value={opt.value}>
                          {t(opt.labelKey)}
                        </option>
                      ))}
                    </select>
                  ) : (
                    <button
                      onClick={() => setEditingInterval(ds.id)}
                      style={{
                        background: "none",
                        border: "1px dashed var(--border)",
                        padding: "0.1rem 0.4rem",
                        fontSize: "0.8rem",
                        borderRadius: "4px",
                        cursor: "pointer",
                      }}
                      title={t("tracked.change_interval")}
                    >
                      {formatInterval(ds.poll_interval, t)} ✎
                    </button>
                  )
                ) : (
                  <span style={{ fontSize: "0.8rem" }}>{formatInterval(ds.poll_interval, t)}</span>
                )}
              </div>
              <div>
                {t("tracked.last_poll")}:{" "}
                {ds.last_polled_at
                  ? new Date(ds.last_polled_at).toLocaleString()
                  : t("tracked.never")}
              </div>
              {ds.last_modified && (
                <div>
                  {t("search.last_modified")}: {ds.last_modified.slice(0, 19)}
                </div>
              )}
            </div>

            {ds.status === "active" && ds.odata_dataset_id && (
              <div className="text-sm mb-1">
                <a
                  href={`https://www.odata.org.il/dataset/${ds.odata_dataset_id}`}
                  target="_blank"
                  rel="noopener noreferrer"
                  style={{ fontSize: "0.8rem" }}
                >
                  {t("tracked.view_on_odata")} ↗
                </a>
              </div>
            )}

            <div className="flex mt-1">
              <Link to={`/versions/${ds.id}`} className="btn-primary" style={{ textDecoration: "none" }}>
                {t("tracked.versions")}
              </Link>
              {isAdmin && ds.status === "active" && (
                <button
                  className="btn-secondary"
                  onClick={() => pollNow(ds.id)}
                  disabled={polling.has(ds.id)}
                  aria-busy={polling.has(ds.id)}
                  aria-label={polling.has(ds.id) ? t("common.loading") : t("tracked.poll_now")}
                >
                  {polling.has(ds.id) ? t("common.loading") : t("tracked.poll_now")}
                </button>
              )}
              {isAdmin && (
                <button className="btn-danger" onClick={() => untrack(ds.id)}>
                  {t("tracked.untrack")}
                </button>
              )}
            </div>
          </article>
        ))}
      </div>
    </div>
  );
}
