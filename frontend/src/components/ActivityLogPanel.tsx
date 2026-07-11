import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { admin as adminApi, ActivityLogEntry } from "../api/client";
import CopyListButton from "./CopyListButton";

// Append-only event stream of the dataset/scrape lifecycle: a row per
// requested / approved / rejected / queued / started / completed / failed
// event, with the error message expandable on failed or rejected steps.

const EVENT_META: Record<string, { label: string; emoji: string; color: string; bg: string }> = {
  requested: { label: "בקשה", emoji: "📨", color: "#3730a3", bg: "#eef2ff" },
  approved: { label: "אושר", emoji: "✅", color: "#065f46", bg: "#ecfdf5" },
  rejected: { label: "נדחה", emoji: "🚫", color: "#991b1b", bg: "#fef2f2" },
  queued: { label: "בתור", emoji: "⏳", color: "#92400e", bg: "#fffbeb" },
  started: { label: "התחיל", emoji: "▶️", color: "#1e40af", bg: "#eff6ff" },
  completed: { label: "הסתיים", emoji: "🏁", color: "#065f46", bg: "#ecfdf5" },
  failed: { label: "נכשל", emoji: "❌", color: "#991b1b", bg: "#fef2f2" },
};

const EVENT_FILTERS: { id: string; label: string }[] = [
  { id: "", label: "הכל" },
  { id: "requested", label: "בקשות" },
  { id: "approved", label: "אישורים" },
  { id: "queued", label: "כניסה לתור" },
  { id: "started", label: "תחילת גירוד" },
  { id: "completed", label: "סיום גירוד" },
  { id: "failed", label: "כשלים" },
  { id: "rejected", label: "דחיות" },
];

function fmtTimestamp(iso: string): string {
  if (!iso) return "—";
  return new Date(iso).toLocaleString("he-IL", {
    day: "2-digit", month: "2-digit", year: "numeric",
    hour: "2-digit", minute: "2-digit", second: "2-digit",
  });
}

export default function ActivityLogPanel() {
  const [entries, setEntries] = useState<ActivityLogEntry[]>([]);
  const [total, setTotal] = useState(0);
  const [event, setEvent] = useState<string>("");
  const [q, setQ] = useState<string>("");
  const [qInput, setQInput] = useState<string>("");
  const [offset, setOffset] = useState(0);
  const [loading, setLoading] = useState(true);
  const [errorMsg, setErrorMsg] = useState<string | null>(null);
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const LIMIT = 100;

  const load = async () => {
    setLoading(true);
    setErrorMsg(null);
    try {
      const page = await adminApi.activityLog({ event: event || undefined, q: q || undefined, limit: LIMIT, offset });
      setEntries(page.entries);
      setTotal(page.total);
    } catch (e) {
      setErrorMsg((e as Error)?.message ?? String(e));
    } finally {
      setLoading(false);
    }
  };
  useEffect(() => {
    load();
    const t = setInterval(load, 20000);
    return () => clearInterval(t);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [event, q, offset]);

  const toggle = (id: string) =>
    setExpanded((s) => {
      const n = new Set(s);
      if (n.has(id)) n.delete(id);
      else n.add(id);
      return n;
    });

  const pageFrom = total === 0 ? 0 : offset + 1;
  const pageTo = Math.min(offset + LIMIT, total);

  return (
    <section className="card mb-2" style={{ padding: "1rem 1.25rem" }}>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", flexWrap: "wrap", gap: "0.75rem", marginBottom: "0.75rem" }}>
        <h2 style={{ fontSize: "1.25rem", fontWeight: 700, margin: 0 }}>📜 לוג משימות</h2>
        <span style={{ display: "inline-flex", alignItems: "center", gap: "0.6rem" }}>
          {/* Copies the CURRENT view — the active event filter + search are
              respected, so "כשלים" + a query yields exactly that error list,
              ready to paste into a debugging chat. Full detail included. */}
          <CopyListButton
            label="העתק רשימה"
            getText={() => {
              const filt = [
                event ? `אירוע=${(EVENT_FILTERS.find((f) => f.id === event) || { label: event }).label}` : "",
                q ? `חיפוש="${q}"` : "",
              ].filter(Boolean).join(", ");
              return [
                `לוג משימות${filt ? ` (סינון: ${filt})` : ""} — ${entries.length} מתוך ${total}, הועתק ${new Date().toLocaleString("he-IL")}`,
                ...entries.map((e, i) => {
                  const meta = EVENT_META[e.event] || { label: e.event };
                  let line = `${i + 1}. ${fmtTimestamp(e.created_at)} [${meta.label}${e.status === "error" ? "/שגיאה" : ""}] ${e.dataset_title || "—"}`;
                  if (e.message) line += ` — ${e.message}`;
                  if (e.detail) line += `\n   ${e.detail.replace(/\n/g, "\n   ")}`;
                  return line;
                }),
              ].join("\n\n");
            }}
          />
          <span className="text-muted" style={{ fontSize: "0.85rem" }}>
            {total.toLocaleString()} אירועים{total > 0 ? ` · מציג ${pageFrom}–${pageTo}` : ""}
          </span>
        </span>
      </div>

      <div style={{ display: "flex", alignItems: "center", gap: "0.4rem", flexWrap: "wrap", marginBottom: "0.75rem" }}>
        {EVENT_FILTERS.map((f) => (
          <button
            key={f.id || "all"}
            onClick={() => { setOffset(0); setEvent(f.id); }}
            style={{
              fontSize: "0.78rem", padding: "0.25rem 0.7rem", borderRadius: "999px",
              border: "1px solid var(--border)", cursor: "pointer",
              background: event === f.id ? "var(--primary, #0369a1)" : "var(--surface)",
              color: event === f.id ? "#fff" : "var(--text)",
              fontWeight: event === f.id ? 600 : 400,
            }}
          >
            {f.label}
          </button>
        ))}
        <form
          onSubmit={(e) => { e.preventDefault(); setOffset(0); setQ(qInput.trim()); }}
          style={{ marginInlineStart: "auto", display: "flex", gap: "0.3rem" }}
        >
          <input
            value={qInput}
            onChange={(e) => setQInput(e.target.value)}
            placeholder="חיפוש לפי מאגר / הודעה / שגיאה…"
            style={{ fontSize: "0.8rem", padding: "0.3rem 0.6rem", border: "1px solid var(--border)", borderRadius: "6px", width: "16rem", maxWidth: "60vw" }}
          />
          <button type="submit" className="btn-secondary" style={{ fontSize: "0.78rem", padding: "0.3rem 0.7rem" }}>חפש</button>
          {q && (
            <button type="button" className="btn-secondary" onClick={() => { setQInput(""); setQ(""); setOffset(0); }} style={{ fontSize: "0.78rem", padding: "0.3rem 0.6rem" }}>×</button>
          )}
        </form>
      </div>

      {errorMsg && <div className="text-sm" style={{ color: "#b91c1c", marginBottom: "0.5rem" }}>{errorMsg}</div>}
      {loading && entries.length === 0 ? (
        <div className="empty-state" style={{ padding: "1.5rem" }}>טוען…</div>
      ) : entries.length === 0 ? (
        <div className="empty-state" style={{ padding: "1.5rem" }}>אין אירועים להצגה.</div>
      ) : (
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.85rem" }}>
            <thead>
              <tr style={{ textAlign: "right", color: "var(--text-muted)", borderBottom: "2px solid var(--border)" }}>
                <th style={{ padding: "0.5rem 0.6rem", whiteSpace: "nowrap" }}>מתי</th>
                <th style={{ padding: "0.5rem 0.6rem" }}>אירוע</th>
                <th style={{ padding: "0.5rem 0.6rem", width: "30%" }}>מאגר</th>
                <th style={{ padding: "0.5rem 0.6rem" }}>פירוט</th>
                <th style={{ padding: "0.5rem 0.6rem", whiteSpace: "nowrap" }}>מקור</th>
              </tr>
            </thead>
            <tbody>
              {entries.map((e) => {
                const meta = EVENT_META[e.event] || { label: e.event, emoji: "•", color: "var(--text)", bg: "var(--surface)" };
                const isErr = e.status === "error";
                const open = expanded.has(e.id);
                return (
                  <tr key={e.id} style={{ borderBottom: "1px solid var(--border)", background: isErr ? "#fef2f2" : undefined }}>
                    <td style={{ padding: "0.5rem 0.6rem", whiteSpace: "nowrap", color: "var(--text-muted)", fontVariantNumeric: "tabular-nums" }}>
                      {fmtTimestamp(e.created_at)}
                    </td>
                    <td style={{ padding: "0.5rem 0.6rem" }}>
                      <span style={{ display: "inline-flex", alignItems: "center", gap: "0.3rem", padding: "0.15rem 0.55rem", borderRadius: "999px", background: meta.bg, color: meta.color, fontWeight: 600, fontSize: "0.78rem", whiteSpace: "nowrap" }}>
                        <span>{meta.emoji}</span>{meta.label}
                      </span>
                    </td>
                    <td style={{ padding: "0.5rem 0.6rem", maxWidth: 0 }}>
                      {e.tracked_dataset_id ? (
                        <Link to={`/versions/${e.tracked_dataset_id}`} style={{ display: "block", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }} title={e.dataset_title || ""}>
                          {e.dataset_title || e.tracked_dataset_id.slice(0, 8)}
                        </Link>
                      ) : (
                        <span className="text-muted" title={e.dataset_title || ""}>{e.dataset_title || "—"}</span>
                      )}
                      {e.source_type && (
                        <div className="text-muted" style={{ fontSize: "0.7rem" }}>{e.source_type}</div>
                      )}
                    </td>
                    <td style={{ padding: "0.5rem 0.6rem" }}>
                      <div style={{ color: isErr ? "#b91c1c" : "var(--text)" }}>{e.message || "—"}</div>
                      {e.detail && (
                        <div style={{ marginTop: "0.2rem" }}>
                          <button
                            onClick={() => toggle(e.id)}
                            style={{ fontSize: "0.72rem", color: "#b91c1c", background: "none", border: "none", cursor: "pointer", padding: 0, textDecoration: "underline" }}
                          >
                            {open ? "הסתר שגיאה" : "הצג שגיאה"}
                          </button>
                          {open && (
                            <pre style={{ marginTop: "0.3rem", padding: "0.5rem", background: "#fff", border: "1px solid #fecaca", borderRadius: "6px", fontSize: "0.72rem", whiteSpace: "pre-wrap", wordBreak: "break-word", maxHeight: "12rem", overflow: "auto", direction: "ltr", textAlign: "left" }}>
                              {e.detail}
                            </pre>
                          )}
                        </div>
                      )}
                    </td>
                    <td style={{ padding: "0.5rem 0.6rem", whiteSpace: "nowrap", color: "var(--text-muted)", fontSize: "0.78rem" }}>
                      {e.actor || "—"}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      {total > LIMIT && (
        <div style={{ display: "flex", alignItems: "center", justifyContent: "center", gap: "0.75rem", marginTop: "0.75rem" }}>
          <button className="btn-secondary" disabled={offset === 0} onClick={() => setOffset(Math.max(0, offset - LIMIT))} style={{ fontSize: "0.8rem", padding: "0.3rem 0.8rem" }}>‹ הקודם</button>
          <span className="text-muted" style={{ fontSize: "0.8rem" }}>{pageFrom}–{pageTo} מתוך {total.toLocaleString()}</span>
          <button className="btn-secondary" disabled={pageTo >= total} onClick={() => setOffset(offset + LIMIT)} style={{ fontSize: "0.8rem", padding: "0.3rem 0.8rem" }}>הבא ›</button>
        </div>
      )}
    </section>
  );
}
