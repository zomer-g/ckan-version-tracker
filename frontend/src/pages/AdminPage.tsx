import { useState, useEffect } from "react";
import { useTranslation } from "react-i18next";
import { admin as adminApi, PendingRequest } from "../api/client";

export default function AdminPage() {
  const { t } = useTranslation();
  const [requests, setRequests] = useState<PendingRequest[]>([]);
  const [loading, setLoading] = useState(true);
  const [processing, setProcessing] = useState<Set<string>>(new Set());

  useEffect(() => {
    loadPending();
  }, []);

  const loadPending = async () => {
    setLoading(true);
    try {
      const data = await adminApi.pending();
      setRequests(data);
    } catch {
      // ignore
    }
    setLoading(false);
  };

  const handleApprove = async (id: string) => {
    setProcessing((prev) => new Set(prev).add(id));
    try {
      await adminApi.approve(id);
      setRequests((prev) => prev.filter((r) => r.id !== id));
    } catch {
      // ignore
    }
    setProcessing((prev) => {
      const next = new Set(prev);
      next.delete(id);
      return next;
    });
  };

  const handleReject = async (id: string) => {
    setProcessing((prev) => new Set(prev).add(id));
    try {
      await adminApi.reject(id);
      setRequests((prev) => prev.filter((r) => r.id !== id));
    } catch {
      // ignore
    }
    setProcessing((prev) => {
      const next = new Set(prev);
      next.delete(id);
      return next;
    });
  };

  if (loading) {
    return (
      <div className="loading" role="status" aria-live="polite">
        {t("common.loading")}
      </div>
    );
  }

  return (
    <div>
      <div className="page-header">
        <h1>{t("admin.title")}</h1>
      </div>

      {requests.length === 0 ? (
        <div className="empty-state">{t("admin.empty")}</div>
      ) : (
        <div className="grid grid-2">
          {requests.map((req) => (
            <article key={req.id} className="card" aria-label={req.title}>
              <h2 style={{ fontSize: "1rem", fontWeight: 600, margin: "0 0 0.5rem 0" }}>
                {req.title}
              </h2>

              {req.organization && (
                <p className="text-sm text-muted mb-1">{req.organization}</p>
              )}

              <div className="text-sm mb-1">
                <div>
                  {t("admin.requester")}: {req.requester_name} ({req.requester_email})
                </div>
                <div>
                  {t("admin.requested_at")}:{" "}
                  {new Date(req.created_at).toLocaleString()}
                </div>
              </div>

              <div className="flex mt-1">
                <button
                  className="btn-primary"
                  onClick={() => handleApprove(req.id)}
                  disabled={processing.has(req.id)}
                  aria-label={`${t("admin.approve")} ${req.title}`}
                >
                  {processing.has(req.id) ? t("common.loading") : t("admin.approve")}
                </button>
                <button
                  className="btn-danger"
                  onClick={() => handleReject(req.id)}
                  disabled={processing.has(req.id)}
                  aria-label={`${t("admin.reject")} ${req.title}`}
                >
                  {processing.has(req.id) ? t("common.loading") : t("admin.reject")}
                </button>
              </div>
            </article>
          ))}
        </div>
      )}
    </div>
  );
}
