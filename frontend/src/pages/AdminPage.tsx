import { useState, useEffect } from "react";
import { useTranslation } from "react-i18next";
import { Link } from "react-router-dom";
import {
  admin as adminApi,
  datasets as datasetsApi,
  publicApi,
  organizations as orgsApi,
  PendingRequest,
  TrackedDataset,
  ScrapeQueueResponse,
  Organization,
} from "../api/client";

function formatRelative(iso: string | null): string {
  if (!iso) return "";
  const ms = Date.now() - new Date(iso).getTime();
  const sec = Math.round(ms / 1000);
  if (sec < 60) return `לפני ${sec} שניות`;
  const min = Math.round(sec / 60);
  if (min < 60) return `לפני ${min} דקות`;
  const hr = Math.round(min / 60);
  if (hr < 24) return `לפני ${hr} שעות`;
  const days = Math.round(hr / 24);
  return `לפני ${days} ימים`;
}

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
  // Scrape queue state
  const [queue, setQueue] = useState<ScrapeQueueResponse | null>(null);
  // Organizations
  const [orgs, setOrgs] = useState<Organization[]>([]);
  const [orgOverrides, setOrgOverrides] = useState<Record<string, string>>({});
  const [syncingOrgs, setSyncingOrgs] = useState(false);
  const [syncToast, setSyncToast] = useState<string | null>(null);

  useEffect(() => {
    loadAll();
    loadOrgs();
    loadQueue();
    const id = setInterval(loadQueue, 5000);
    return () => clearInterval(id);
  }, []);

  const loadOrgs = async () => {
    try {
      const list = await orgsApi.list();
      setOrgs(list);
    } catch (e) {
      console.error("Failed to load organizations", e);
    }
  };

  const handleSyncOrgs = async () => {
    setSyncingOrgs(true);
    setSyncToast(null);
    try {
      const res = await adminApi.syncOrganizations();
      setSyncToast(`נוספו ${res.created}, עודכנו ${res.updated}, שויכו ${res.linked_datasets} מאגרים`);
      await loadOrgs();
      await loadAll();
      setTimeout(() => setSyncToast(null), 6000);
    } catch (e: any) {
      setSyncToast(`שגיאה: ${e?.message || e}`);
      setTimeout(() => setSyncToast(null), 6000);
    }
    setSyncingOrgs(false);
  };

  const handleChangeOrg = async (datasetId: string, newOrgId: string) => {
    try {
      await datasetsApi.update(datasetId, { organization_id: newOrgId || "" });
      const matched = orgs.find((o) => o.id === newOrgId);
      setAllDatasets((prev) =>
        prev.map((d) =>
          d.id === datasetId
            ? {
                ...d,
                organization_id: newOrgId || null,
                organization_title: matched?.title || null,
                organization: matched?.name || d.organization,
              }
            : d
        )
      );
    } catch (e: any) {
      alert(`שיוך ארגון נכשל: ${e?.message || e}`);
    }
  };

  const loadQueue = async () => {
    try {
      const q = await adminApi.scrapeTasks();
      setQueue(q);
    } catch (e) {
      console.error("Failed to load queue", e);
    }
  };

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
      const orgIdOverride = orgOverrides[id] || undefined;
      await adminApi.approve(id, intervalOverride, titleToSend, orgIdOverride);
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

  const handleCancelTask = async (taskId: string, title: string, kind: "running" | "pending") => {
    const verb = kind === "running" ? "לאפס" : "להסיר מהתור";
    if (!confirm(`${verb} את המשימה של "${title}"?`)) return;
    try {
      await adminApi.cancelScrapeTask(taskId);
      await loadQueue();
    } catch (e: any) {
      alert(`שגיאה: ${e?.message || e}`);
    }
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

  // Build per-dataset status map for inline indicators
  const datasetStatusMap = new Map<string, { kind: "running" | "pending" | "failed"; tooltip?: string }>();
  if (queue) {
    queue.failed.forEach((t) =>
      datasetStatusMap.set(t.dataset_id, { kind: "failed", tooltip: t.error || undefined })
    );
    queue.pending.forEach((t) =>
      datasetStatusMap.set(t.dataset_id, { kind: "pending" })
    );
    // Running takes priority — set last so it overrides pending/failed for same dataset
    queue.running.forEach((t) =>
      datasetStatusMap.set(t.dataset_id, {
        kind: "running",
        tooltip: `${t.phase || ""} ${t.progress || 0}% — ${t.message || ""}`.trim(),
      })
    );
  }

  return (
    <div>
      {/* Section 0: Scrape Queue */}
      <div className="page-header flex-between" style={{ flexWrap: "wrap", gap: "0.75rem" }}>
        <h1 style={{ margin: 0 }}>{t("admin.title")}</h1>
        <div style={{ display: "flex", gap: "0.5rem", alignItems: "center", flexWrap: "wrap" }}>
          <button
            className="btn-secondary"
            onClick={handleSyncOrgs}
            disabled={syncingOrgs}
            style={{ fontSize: "0.8rem", padding: "0.35rem 0.75rem" }}
          >
            {syncingOrgs ? "..." : t("organizations.admin_sync")}
          </button>
          {syncToast && (
            <span style={{
              fontSize: "0.75rem",
              padding: "0.25rem 0.5rem",
              borderRadius: "4px",
              background: syncToast.startsWith("שגיאה") ? "#fee2e2" : "#dcfce7",
              color: syncToast.startsWith("שגיאה") ? "#991b1b" : "#166534",
            }}>
              {syncToast}
            </span>
          )}
        </div>
      </div>

      <section style={{
        marginBottom: "1.5rem",
        padding: "1rem 1.25rem",
        background: "var(--surface)",
        borderRadius: "var(--radius)",
        boxShadow: "var(--shadow-sm)",
        border: "1px solid var(--border)",
      }} aria-labelledby="queue-heading">
        <div className="flex-between" style={{ marginBottom: "0.75rem" }}>
          <h2 id="queue-heading" style={{ fontSize: "1.1rem", fontWeight: 700, margin: 0 }}>
            תור גירוד
          </h2>
          <button onClick={loadQueue} className="btn-secondary" style={{ fontSize: "0.75rem", padding: "0.25rem 0.6rem" }}>
            רענן ↻
          </button>
        </div>

        {!queue ? (
          <div className="text-sm text-muted">טוען...</div>
        ) : queue.running.length === 0 && queue.pending.length === 0 && queue.failed.length === 0 ? (
          <div className="text-sm text-muted">התור ריק — אין משימות גירוד פעילות</div>
        ) : (
          <div style={{ display: "flex", flexDirection: "column", gap: "0.75rem" }}>
            {/* Running */}
            {queue.running.length > 0 && (
              <div>
                <div style={{ fontSize: "0.85rem", fontWeight: 600, marginBottom: "0.4rem", color: "#16a34a" }}>
                  🔄 בעבודה כרגע ({queue.running.length})
                </div>
                {queue.running.map((t) => {
                  const ageMs = t.created_at ? Date.now() - new Date(t.created_at).getTime() : 0;
                  const isStuck = ageMs > 30 * 60 * 1000;  // 30 minutes
                  return (
                    <div key={t.task_id} style={{
                      padding: "0.6rem 0.75rem",
                      marginBottom: "0.4rem",
                      background: isStuck ? "#fef2f2" : "#f0fdf4",
                      border: isStuck ? "1px solid #fca5a5" : "1px solid #bbf7d0",
                      borderRadius: "6px",
                    }}>
                      <div className="flex-between" style={{ gap: "0.5rem" }}>
                        <div style={{ fontWeight: 600, fontSize: "0.9rem", flex: 1 }}>
                          <Link to={`/versions/${t.dataset_id}`}>{t.dataset_title}</Link>
                          {isStuck && (
                            <span style={{
                              marginInlineStart: "0.5rem",
                              fontSize: "0.7rem",
                              padding: "0.1rem 0.4rem",
                              borderRadius: "9999px",
                              background: "#fee2e2",
                              color: "#991b1b",
                              fontWeight: 600,
                            }}>
                              ⚠ ייתכן שתקוע
                            </span>
                          )}
                        </div>
                        <button
                          onClick={() => handleCancelTask(t.task_id, t.dataset_title, "running")}
                          title="אפס משימה"
                          style={{
                            background: "none",
                            border: "1px solid #dc2626",
                            color: "#dc2626",
                            cursor: "pointer",
                            fontSize: "0.7rem",
                            padding: "0.2rem 0.5rem",
                            borderRadius: "4px",
                            whiteSpace: "nowrap",
                          }}
                        >
                          ✕ אפס
                        </button>
                      </div>
                      <div className="text-sm text-muted" style={{ marginTop: "0.2rem" }}>
                        שלב: <strong>{t.phase || "—"}</strong> · {t.progress}% · התחיל {formatRelative(t.created_at)}
                      </div>
                      {t.message && (
                        <div className="text-sm" style={{ marginTop: "0.2rem", color: "#166534" }}>
                          {t.message}
                        </div>
                      )}
                      {/* Progress bar */}
                      <div style={{
                        marginTop: "0.4rem",
                        height: "6px",
                        background: isStuck ? "#fecaca" : "#dcfce7",
                        borderRadius: "3px",
                        overflow: "hidden",
                      }}>
                        <div style={{
                          width: `${Math.max(2, t.progress)}%`,
                          height: "100%",
                          background: isStuck ? "#dc2626" : "#16a34a",
                          transition: "width 0.5s",
                        }} />
                      </div>
                    </div>
                  );
                })}
              </div>
            )}

            {/* Pending */}
            {queue.pending.length > 0 && (
              <div>
                <div style={{ fontSize: "0.85rem", fontWeight: 600, marginBottom: "0.4rem", color: "#92400e" }}>
                  🕐 ממתין בתור ({queue.pending.length})
                  {queue.running.length === 0 && (
                    <span style={{
                      marginInlineStart: "0.5rem",
                      fontSize: "0.7rem",
                      padding: "0.1rem 0.4rem",
                      borderRadius: "9999px",
                      background: "#e0e7ff",
                      color: "#3730a3",
                      fontWeight: 500,
                    }}>
                      אין worker פעיל — המשימות יחכו עד שיעלה אחד
                    </span>
                  )}
                </div>
                <ul style={{ listStyle: "none", padding: 0, margin: 0 }}>
                  {queue.pending.map((t) => {
                    const ageMs = t.created_at ? Date.now() - new Date(t.created_at).getTime() : 0;
                    const isOld = ageMs > 60 * 60 * 1000;  // 1 hour
                    return (
                      <li key={t.task_id} style={{
                        padding: "0.4rem 0.6rem",
                        marginBottom: "0.2rem",
                        background: isOld ? "#fef3c7" : "#fffbeb",
                        border: isOld ? "1px solid #fbbf24" : "1px solid #fde68a",
                        borderRadius: "4px",
                        fontSize: "0.85rem",
                        display: "flex",
                        justifyContent: "space-between",
                        gap: "0.5rem",
                        alignItems: "center",
                      }}>
                        <Link to={`/versions/${t.dataset_id}`} style={{ flex: 1 }}>{t.dataset_title}</Link>
                        <span className="text-muted" style={{ fontSize: "0.8rem", whiteSpace: "nowrap" }}>
                          נוסף {formatRelative(t.created_at)}
                        </span>
                        <button
                          onClick={() => handleCancelTask(t.task_id, t.dataset_title, "pending")}
                          title="הסר מהתור"
                          style={{
                            background: "none",
                            border: "1px solid #92400e",
                            color: "#92400e",
                            cursor: "pointer",
                            fontSize: "0.7rem",
                            padding: "0.15rem 0.4rem",
                            borderRadius: "4px",
                            whiteSpace: "nowrap",
                          }}
                        >
                          ✕ הסר
                        </button>
                      </li>
                    );
                  })}
                </ul>
              </div>
            )}

            {/* Failed */}
            {queue.failed.length > 0 && (
              <div>
                <div style={{ fontSize: "0.85rem", fontWeight: 600, marginBottom: "0.4rem", color: "#991b1b" }}>
                  ⚠ כשלים אחרונים — 24 שעות ({queue.failed.length})
                </div>
                <ul style={{ listStyle: "none", padding: 0, margin: 0 }}>
                  {queue.failed.map((t) => (
                    <li key={t.task_id} style={{
                      padding: "0.4rem 0.6rem",
                      marginBottom: "0.2rem",
                      background: "#fef2f2",
                      border: "1px solid #fecaca",
                      borderRadius: "4px",
                      fontSize: "0.85rem",
                    }}>
                      <div className="flex-between" style={{ gap: "0.5rem" }}>
                        <Link to={`/versions/${t.dataset_id}`}>{t.dataset_title}</Link>
                        <span className="text-muted" style={{ fontSize: "0.8rem" }}>
                          {formatRelative(t.completed_at)}
                        </span>
                      </div>
                      {t.error && (
                        <div style={{
                          marginTop: "0.2rem",
                          fontSize: "0.75rem",
                          color: "#991b1b",
                          wordBreak: "break-word",
                          fontFamily: "monospace",
                        }}>
                          {t.error.length > 200 ? t.error.slice(0, 200) + "..." : t.error}
                        </div>
                      )}
                    </li>
                  ))}
                </ul>
              </div>
            )}
          </div>
        )}
      </section>

      {/* Section 1: Pending Requests */}
      <div className="page-header">
        <h2 style={{ fontSize: "1.25rem", fontWeight: 700 }}>{t("admin.title")}</h2>
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
              <div className="text-sm mb-1">
                <label style={{ fontSize: "0.85rem" }}>
                  {t("organizations.admin_column")}:{" "}
                  <select
                    value={orgOverrides[req.id] ?? req.organization_id ?? ""}
                    onChange={(e) => setOrgOverrides((prev) => ({
                      ...prev, [req.id]: e.target.value,
                    }))}
                    style={{ width: "auto", maxWidth: "100%", padding: "0.2rem 0.4rem", fontSize: "0.8rem" }}
                  >
                    <option value="">{t("organizations.select_placeholder")}</option>
                    {orgs.map((o) => <option key={o.id} value={o.id}>{o.title}</option>)}
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
                <th style={thStyle}>{t("organizations.admin_column")}</th>
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
                        {(() => {
                          const s = datasetStatusMap.get(ds.id);
                          if (!s) return null;
                          const config = {
                            running: { icon: "🔄", color: "#16a34a", label: "בעבודה" },
                            pending: { icon: "🕐", color: "#92400e", label: "בתור" },
                            failed:  { icon: "⚠️", color: "#991b1b", label: "נכשל" },
                          }[s.kind];
                          return (
                            <span
                              title={s.tooltip ? `${config.label} — ${s.tooltip}` : config.label}
                              style={{ fontSize: "0.9rem", color: config.color }}
                            >
                              {config.icon}
                            </span>
                          );
                        })()}
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
                  <td style={tdStyle}>
                    <select
                      value={ds.organization_id ?? ""}
                      onChange={(e) => handleChangeOrg(ds.id, e.target.value)}
                      style={{
                        width: "100%",
                        maxWidth: 220,
                        padding: "0.2rem 0.4rem",
                        fontSize: "0.8rem",
                        border: "1px solid var(--border)",
                        borderRadius: "4px",
                      }}
                    >
                      <option value="">{t("organizations.select_placeholder")}</option>
                      {orgs.map((o) => <option key={o.id} value={o.id}>{o.title}</option>)}
                    </select>
                    {ds.organization && !ds.organization_id && (
                      <div className="text-muted" style={{ fontSize: "0.7rem", marginTop: "0.2rem" }}>
                        {ds.organization}
                      </div>
                    )}
                  </td>
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
