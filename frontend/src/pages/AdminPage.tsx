import { useState, useEffect } from "react";
import { useTranslation } from "react-i18next";
import { admin as adminApi, PendingRequest } from "../api/client";

const INTERVAL_OPTIONS = [
  { value: 900, label: "\u05DB\u05DC 15 \u05D3\u05E7\u05D5\u05EA" },
  { value: 3600, label: "\u05DB\u05DC \u05E9\u05E2\u05D4" },
  { value: 43200, label: "\u05DB\u05DC 12 \u05E9\u05E2\u05D5\u05EA" },
  { value: 86400, label: "\u05DB\u05DC \u05D9\u05D5\u05DD" },
  { value: 604800, label: "\u05DB\u05DC \u05E9\u05D1\u05D5\u05E2" },
];

function formatIntervalLabel(seconds: number): string {
  const match = INTERVAL_OPTIONS.find((o) => o.value === seconds);
  if (match) return match.label;
  if (seconds < 3600) return `\u05DB\u05DC ${Math.round(seconds / 60)} \u05D3\u05E7\u05D5\u05EA`;
  if (seconds < 86400) return `\u05DB\u05DC ${Math.round(seconds / 3600)} \u05E9\u05E2\u05D5\u05EA`;
  return `\u05DB\u05DC ${Math.round(seconds / 86400)} \u05D9\u05DE\u05D9\u05DD`;
}

export default function AdminPage() {
  const { t } = useTranslation();
  const [requests, setRequests] = useState<PendingRequest[]>([]);
  const [loading, setLoading] = useState(true);
  const [processing, setProcessing] = useState<Set<string>>(new Set());
  const [intervalOverrides, setIntervalOverrides] = useState<Record<string, number>>({});

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
      const override = intervalOverrides[id];
      await adminApi.approve(id, override);
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
                <div>
                  {t("tracked.poll_interval")}: {formatIntervalLabel(req.poll_interval)}
                </div>
              </div>

              <div className="text-sm mb-1">
                <label style={{ fontSize: "0.85rem" }}>
                  {t("admin.override_interval", "\u05E9\u05E0\u05D4 \u05EA\u05D3\u05D9\u05E8\u05D5\u05EA")}:{" "}
                  <select
                    value={intervalOverrides[req.id] ?? ""}
                    onChange={(e) =>
                      setIntervalOverrides((prev) => ({
                        ...prev,
                        [req.id]: e.target.value ? Number(e.target.value) : undefined!,
                      }))
                    }
                    style={{ width: "auto", padding: "0.2rem 0.4rem", fontSize: "0.8rem" }}
                  >
                    <option value="">{t("admin.keep_requested", "\u05DC\u05E4\u05D9 \u05D4\u05D1\u05E7\u05E9\u05D4")}</option>
                    {INTERVAL_OPTIONS.map((opt) => (
                      <option key={opt.value} value={opt.value}>{opt.label}</option>
                    ))}
                  </select>
                </label>
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
