import { useState, useEffect } from "react";
import { useTranslation } from "react-i18next";
import { Link } from "react-router-dom";
import {
  datasets as datasetsApi,
  TrackedDataset,
} from "../api/client";

export default function TrackedPage() {
  const { t } = useTranslation();
  const [datasets, setDatasets] = useState<TrackedDataset[]>([]);
  const [loading, setLoading] = useState(true);
  const [polling, setPolling] = useState<Set<string>>(new Set());

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
                className={`badge ${ds.is_active ? "badge-success" : "badge-warning"}`}
                role="status"
              >
                {ds.is_active ? t("tracked.active") : t("tracked.paused")}
              </span>
            </div>

            {ds.organization && (
              <p className="text-sm text-muted mb-1">{ds.organization}</p>
            )}

            <div className="text-sm mb-1">
              <div>
                {t("tracked.poll_interval")}: {ds.poll_interval} {t("tracked.seconds")}
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

            <div className="flex mt-1">
              <Link to={`/versions/${ds.id}`} className="btn-primary" style={{ textDecoration: "none" }}>
                {t("tracked.versions")}
              </Link>
              <button
                className="btn-secondary"
                onClick={() => pollNow(ds.id)}
                disabled={polling.has(ds.id)}
                aria-busy={polling.has(ds.id)}
                aria-label={polling.has(ds.id) ? t("common.loading") : t("tracked.poll_now")}
              >
                {polling.has(ds.id) ? t("common.loading") : t("tracked.poll_now")}
              </button>
              <button className="btn-danger" onClick={() => untrack(ds.id)}>
                {t("tracked.untrack")}
              </button>
            </div>
          </article>
        ))}
      </div>
    </div>
  );
}
