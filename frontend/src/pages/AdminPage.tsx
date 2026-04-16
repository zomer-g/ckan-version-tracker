import { useState, useEffect } from "react";
import { useTranslation } from "react-i18next";
import { Link } from "react-router-dom";
import {
  admin as adminApi,
  datasets as datasetsApi,
  publicApi,
  PendingRequest,
  TrackedDataset,
} from "../api/client";

const INTERVAL_OPTIONS = [
  { value: 900, label: "כל 15 דקות" },
  { value: 3600, label: "כל שעה" },
  { value: 43200, label: "כל 12 שעות" },
  { value: 86400, label: "כל יום" },
  { value: 604800, label: "כל שבוע" },
  { value: 2592000, label: "כל חודש" },
  { value: 7776000, label: "כל רבעון" },
];

function formatIntervalLabel(seconds: number): string {
  const match = INTERVAL_OPTIONS.find((o) => o.value === seconds);
  if (match) return match.label;
  if (seconds < 3600) return `כל ${Math.round(seconds / 60)} דקות`;
  if (seconds < 86400) return `כל ${Math.round(seconds / 3600)} שעות`;
  return `כל ${Math.round(seconds / 86400)} ימים`;
}

export default function AdminPage() {
  const { t } = useTranslation();
  const [requests, setRequests] = useState<PendingRequest[]>([]);
  const [allDatasets, setAllDatasets] = useState<TrackedDataset[]>([]);
  const [loading, setLoading] = useState(true);
  const [processing, setProcessing] = useState<Set<string>>(new Set());
  const [intervalOverrides, setIntervalOverrides] = useState<Record<string, number>>({});
  const [titleOverrides, setTitleOverrides] = useState<Record<string, string>>({});
  const [pollToast, setPollToast] = useState<{ id: string; ok: boolean; msg: string } | null>(null);
  // Active-dataset rename state
  const [editingTitleFor, setEditingTitleFor] = useState<string | null>(null);
  const [editingTitleValue, setEditingTitleValue] = useState("");
  const [savingTitle, setSavingTitle] = useState(false);

  useEffect(() => {
    loadAll();
  }, []);

  const loadAll = async () => {
    setLoading(true);
    try {
      const [pending, all] = await Promise.all([
        adminApi.pending(),
        publicApi.datasets(),
      ]);
      setRequests(pending);
      setAllDatasets(all);
    } catch (e) {
      console.error(e);
    }
    setLoading(false);
  };

  const handleApprove = async (id: string) => {
    setProcessing((prev) => new Set(prev).add(id));
    try {
      const intervalOverride = intervalOverrides[id];
      const titleOverride = titleOverrides[id]?.trim();
      // Only send title if it was actually edited (different from original)
      const req = requests.find((r) => r.id === id);
      const titleToSend = titleOverride && titleOverride !== req?.title ? titleOverride : undefined;
      await adminApi.approve(id, intervalOverride, titleToSend);
      await loadAll();
    } catch (e) { console.error(e); }
    setProcessing((prev) => { const n = new Set(prev); n.delete(id); return n; });
  };

  const startEditTitle = (id: string, currentTitle: string) => {
    setEditingTitleFor(id);
    setEditingTitleValue(currentTitle);
  };

  const cancelEditTitle = () => {
    setEditingTitleFor(null);
    setEditingTitleValue("");
  };

  const saveEditTitle = async (id: string) => {
    const newTitle = editingTitleValue.trim();
    if (!newTitle) {
      cancelEditTitle();
      return;
    }
    setSavingTitle(true);
    try {
      await datasetsApi.update(id, { title: newTitle });
      setAllDatasets((prev) =>
        prev.map((d) => (d.id === id ? { ...d, title: newTitle } : d))
      );
      cancelEditTitle();
    } catch (e) {
      console.error(e);
      alert("שמירת השם נכשלה");
    }
    setSavingTitle(false);
  };

  const handleReject = async (id: string) => {
    setProcessing((prev) => new Set(prev).add(id));
    try {
      await adminApi.reject(id);
      await loadAll();
    } catch (e) { console.error(e); }
    setProcessing((prev) => { const n = new Set(prev); n.delete(id); return n; });
  };

  const handlePoll = async (id: string) => {
    setProcessing((prev) => new Set(prev).add(id));
    setPollToast(null);
    try {
      await datasetsApi.poll(id);
      setPollToast({ id, ok: true, msg: "נשלח לדגום ✓" });
      setTimeout(() => setPollToast(null), 3500);
    } catch (e: any) {
      setPollToast({ id, ok: false, msg: e?.message || "שגיאה בדגום" });
      setTimeout(() => setPollToast(null), 4000);
    }
    setProcessing((prev) => { const n = new Set(prev); n.delete(id); return n; });
  };

  const handleDelete = async (id: string, title: string) => {
    if (!confirm(`למחוק את "${title}"?`)) return;
    setProcessing((prev) => new Set(prev).add(id));
    try {
      await datasetsApi.untrack(id);
      setAllDatasets((prev) => prev.filter((d) => d.id !== id));
    } catch (e) { console.error(e); }
    setProcessing((prev) => { const n = new Set(prev); n.delete(id); return n; });
  };

  const handleUpdateInterval = async (id: string, interval: number) => {
    try {
      await datasetsApi.update(id, { poll_interval: interval });
      setAllDatasets((prev) =>
        prev.map((d) => (d.id === id ? { ...d, poll_interval: interval } : d))
      );
    } catch (e) { console.error(e); }
  };

  if (loading) return <div className="loading" role="status">{t("common.loading")}</div>;

  const activeDatasets = allDatasets.filter((d) => d.status === "active");

  return (
    <div>
      {/* Section 1: Pending Requests */}
      <div className="page-header">
        <h1>{t("admin.title")}</h1>
      </div>

      {requests.length === 0 ? (
        <div className="empty-state" style={{ padding: "1.5rem" }}>{t("admin.empty")}</div>
      ) : (
        <div className="grid grid-2 mb-2">
          {requests.map((req) => (
            <article key={req.id} className="card" style={{ borderRight: `4px solid ${req.source_type === "scraper" ? "#f59e0b" : "var(--warning)"}` }}>
              <div className="flex-between" style={{ marginBottom: "0.5rem", gap: "0.5rem" }}>
                <input
                  type="text"
                  value={titleOverrides[req.id] ?? req.title}
                  onChange={(e) => setTitleOverrides((prev) => ({
                    ...prev, [req.id]: e.target.value,
                  }))}
                  aria-label="שם המאגר"
                  style={{
                    flex: 1,
                    fontSize: "1rem",
                    fontWeight: 600,
                    padding: "0.3rem 0.5rem",
                    border: "1px solid var(--border)",
                    borderRadius: "4px",
                    background: "var(--surface)",
                  }}
                />
                <span style={{
                  display: "inline-block",
                  padding: "0.15rem 0.5rem",
                  borderRadius: "9999px",
                  fontSize: "0.65rem",
                  fontWeight: 600,
                  background: req.source_type === "scraper" ? "#fef3c7" : "#ccfbf1",
                  color: req.source_type === "scraper" ? "#92400e" : "#0f766e",
                  flexShrink: 0,
                }}>
                  {req.source_type === "scraper" ? "GOV.IL" : "DATA.GOV.IL"}
                </span>
              </div>
              {req.source_type === "scraper" && req.source_url && (
                <p className="text-sm text-muted" style={{ wordBreak: "break-all" }}>
                  <a href={req.source_url} target="_blank" rel="noopener noreferrer" style={{ color: "var(--primary)" }}>
                    {req.source_url}
                  </a>
                </p>
              )}
              {req.organization && req.source_type !== "scraper" && <p className="text-sm text-muted">{req.organization}</p>}
              <div className="text-sm mb-1">
                <div>{t("admin.requester")}: {req.requester_name} ({req.requester_email})</div>
                <div>{t("admin.requested_at")}: {new Date(req.created_at).toLocaleString()}</div>
                <div>{t("tracked.poll_interval")}: {formatIntervalLabel(req.poll_interval)}</div>
              </div>
              <div className="text-sm mb-1">
                <label style={{ fontSize: "0.85rem" }}>
                  שנה תדירות:{" "}
                  <select
                    value={intervalOverrides[req.id] ?? ""}
                    onChange={(e) => setIntervalOverrides((prev) => ({
                      ...prev, [req.id]: e.target.value ? Number(e.target.value) : undefined!,
                    }))}
                    style={{ width: "auto", padding: "0.2rem 0.4rem", fontSize: "0.8rem" }}
                  >
                    <option value="">לפי הבקשה</option>
                    {INTERVAL_OPTIONS.map((o) => <option key={o.value} value={o.value}>{o.label}</option>)}
                  </select>
                </label>
              </div>
              <div className="flex mt-1">
                <button className="btn-primary" onClick={() => handleApprove(req.id)} disabled={processing.has(req.id)}>
                  {processing.has(req.id) ? "..." : t("admin.approve")}
                </button>
                <button className="btn-danger" onClick={() => handleReject(req.id)} disabled={processing.has(req.id)}>
                  {processing.has(req.id) ? "..." : t("admin.reject")}
                </button>
              </div>
            </article>
          ))}
        </div>
      )}

      {/* Section 2: Active Datasets Management */}
      <div className="page-header mt-3">
        <h2 style={{ fontSize: "1.25rem", fontWeight: 700 }}>ניהול מאגרים פעילים ({activeDatasets.length})</h2>
      </div>

      {activeDatasets.length === 0 ? (
        <div className="empty-state">אין מאגרים פעילים</div>
      ) : (
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", background: "var(--surface)", borderRadius: "var(--radius)", overflow: "hidden", boxShadow: "var(--shadow-sm)" }}>
            <thead>
              <tr style={{ background: "var(--primary-50)", borderBottom: "2px solid var(--border)" }}>
                <th style={thStyle}>שם מאגר</th>
                <th style={thStyle}>מקור</th>
                <th style={thStyle}>ארגון</th>
                <th style={thStyle}>תדירות</th>
                <th style={thStyle}>גרסאות</th>
                <th style={thStyle}>בדיקה אחרונה</th>
                <th style={thStyle}>פעולות</th>
              </tr>
            </thead>
            <tbody>
              {activeDatasets.map((ds) => (
                <tr key={ds.id} style={{ borderBottom: "1px solid var(--border)" }}>
                  <td style={tdStyle}>
                    {editingTitleFor === ds.id ? (
                      <div style={{ display: "flex", gap: "0.3rem", alignItems: "center" }}>
                        <input
                          type="text"
                          value={editingTitleValue}
                          onChange={(e) => setEditingTitleValue(e.target.value)}
                          onKeyDown={(e) => {
                            if (e.key === "Enter") saveEditTitle(ds.id);
                            if (e.key === "Escape") cancelEditTitle();
                          }}
                          autoFocus
                          aria-label="שם המאגר"
                          style={{
                            flex: 1,
                            padding: "0.25rem 0.4rem",
                            fontSize: "0.85rem",
                            border: "1px solid var(--primary)",
                            borderRadius: "4px",
                          }}
                        />
                        <button
                          className="btn-primary"
                          style={{ padding: "0.2rem 0.5rem", fontSize: "0.7rem" }}
                          onClick={() => saveEditTitle(ds.id)}
                          disabled={savingTitle}
                          title="שמור"
                        >
                          ✓
                        </button>
                        <button
                          className="btn-secondary"
                          style={{ padding: "0.2rem 0.5rem", fontSize: "0.7rem" }}
                          onClick={cancelEditTitle}
                          disabled={savingTitle}
                          title="בטל"
                        >
                          ✕
                        </button>
                      </div>
                    ) : (
                      <div style={{ display: "flex", gap: "0.3rem", alignItems: "center" }}>
                        <Link to={`/versions/${ds.id}`} style={{ fontWeight: 500 }}>
                          {ds.title}
                        </Link>
                        <button
                          onClick={() => startEditTitle(ds.id, ds.title)}
                          aria-label="ערוך שם"
                          title="ערוך שם"
                          style={{
                            background: "none",
                            border: "none",
                            cursor: "pointer",
                            fontSize: "0.85rem",
                            padding: "0.1rem 0.3rem",
                            color: "var(--text-muted)",
                            lineHeight: 1,
                          }}
                        >
                          ✏
                        </button>
                      </div>
                    )}
                    {ds.source_type === "scraper" && ds.source_url && (
                      <div style={{ fontSize: "0.75rem", marginTop: "0.2rem" }}>
                        <a href={ds.source_url} target="_blank" rel="noopener noreferrer" style={{ color: "var(--primary)" }}>
                          {ds.source_url}
                        </a>
                      </div>
                    )}
                  </td>
                  <td style={tdStyle}>
                    <span style={{
                      display: "inline-block",
                      padding: "0.15rem 0.5rem",
                      borderRadius: "9999px",
                      fontSize: "0.7rem",
                      fontWeight: 600,
                      background: ds.source_type === "scraper" ? "#fef3c7" : "#ccfbf1",
                      color: ds.source_type === "scraper" ? "#92400e" : "#0f766e",
                    }}>
                      {ds.source_type === "scraper" ? "GOV.IL" : "DATA.GOV.IL"}
                    </span>
                  </td>
                  <td style={tdStyle} className="text-sm text-muted">{ds.organization}</td>
                  <td style={tdStyle}>
                    <select
                      value={ds.poll_interval}
                      onChange={(e) => handleUpdateInterval(ds.id, Number(e.target.value))}
                      style={{ width: "auto", padding: "0.2rem 0.4rem", fontSize: "0.8rem", border: "1px solid var(--border)", borderRadius: "4px" }}
                    >
                      {INTERVAL_OPTIONS.map((o) => <option key={o.value} value={o.value}>{o.label}</option>)}
                    </select>
                  </td>
                  <td style={tdStyle} className="text-sm">
                    <Link to={`/versions/${ds.id}`}>{ds.version_count}</Link>
                  </td>
                  <td style={tdStyle} className="text-sm text-muted">
                    {ds.last_polled_at ? new Date(ds.last_polled_at).toLocaleString() : "—"}
                  </td>
                  <td style={tdStyle}>
                    <div className="flex" style={{ gap: "0.4rem" }}>
                      <div style={{ display: "flex", flexDirection: "column", gap: "0.2rem" }}>
                        <button
                          className="btn-secondary"
                          style={{ padding: "0.25rem 0.75rem", fontSize: "0.75rem" }}
                          onClick={() => handlePoll(ds.id)}
                          disabled={processing.has(ds.id)}
                        >
                          {processing.has(ds.id) ? "..." : "דגום"}
                        </button>
                        {pollToast?.id === ds.id && (
                          <span style={{
                            fontSize: "0.7rem",
                            padding: "0.15rem 0.4rem",
                            borderRadius: "4px",
                            background: pollToast.ok ? "#dcfce7" : "#fee2e2",
                            color: pollToast.ok ? "#166534" : "#991b1b",
                            whiteSpace: "nowrap",
                          }}>
                            {pollToast.msg}
                          </span>
                        )}
                      </div>
                      <button
                        className="btn-danger"
                        style={{ padding: "0.25rem 0.75rem", fontSize: "0.75rem" }}
                        onClick={() => handleDelete(ds.id, ds.title)}
                        disabled={processing.has(ds.id)}
                      >
                        מחק
                      </button>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

const thStyle: React.CSSProperties = {
  textAlign: "start",
  padding: "0.75rem",
  fontSize: "0.8rem",
  fontWeight: 600,
  color: "var(--text)",
};

const tdStyle: React.CSSProperties = {
  padding: "0.6rem 0.75rem",
  verticalAlign: "middle",
  fontSize: "0.85rem",
};
