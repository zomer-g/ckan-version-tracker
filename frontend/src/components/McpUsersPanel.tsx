import { useEffect, useState } from "react";
import { admin as adminApi, McpUser } from "../api/client";

// Closed-beta access management for the MCP server: invite by email, toggle
// active, change tier. An invited (active) email can connect Claude.ai/etc to
// https://www.over.org.il/mcp and complete the Google OAuth flow.

const MCP_URL =
  typeof window !== "undefined" ? `${window.location.origin}/mcp` : "https://www.over.org.il/mcp";

function fmt(iso: string | null): string {
  if (!iso) return "—";
  return new Date(iso).toLocaleString("he-IL", {
    day: "2-digit", month: "2-digit", year: "numeric", hour: "2-digit", minute: "2-digit",
  });
}

export default function McpUsersPanel() {
  const [users, setUsers] = useState<McpUser[]>([]);
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState<string | null>(null);
  const [email, setEmail] = useState("");
  const [name, setName] = useState("");
  const [busy, setBusy] = useState(false);

  const load = async () => {
    setLoading(true);
    setErr(null);
    try {
      setUsers(await adminApi.mcpUsers());
    } catch (e) {
      setErr((e as Error)?.message ?? String(e));
    } finally {
      setLoading(false);
    }
  };
  useEffect(() => { load(); }, []);

  const invite = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!email.trim()) return;
    setBusy(true);
    setErr(null);
    try {
      await adminApi.mcpInvite(email.trim(), name.trim() || undefined);
      setEmail("");
      setName("");
      await load();
    } catch (e) {
      setErr((e as Error)?.message ?? String(e));
    } finally {
      setBusy(false);
    }
  };

  const toggle = async (u: McpUser) => {
    try {
      await adminApi.mcpUpdateUser(u.id, { is_active: !u.is_active });
      await load();
    } catch (e) { alert((e as Error)?.message ?? String(e)); }
  };

  const setTier = async (u: McpUser, tier: string) => {
    try {
      await adminApi.mcpUpdateUser(u.id, { tier });
      await load();
    } catch (e) { alert((e as Error)?.message ?? String(e)); }
  };

  return (
    <section className="card mb-2" style={{ padding: "1rem 1.25rem" }}>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", flexWrap: "wrap", gap: "0.5rem" }}>
        <h2 style={{ fontSize: "1.25rem", fontWeight: 700, margin: 0 }}>🔌 גישת MCP (בטא סגורה)</h2>
        <span className="text-muted" style={{ fontSize: "0.85rem" }}>{users.length} מוזמנים</span>
      </div>

      <div className="text-sm" style={{ marginTop: "0.5rem", color: "var(--text-muted)", lineHeight: 1.6 }}>
        רק כתובות מייל מוזמנות (ופעילות) יכולות לחבר את שרת ה-MCP ב-Claude / ChatGPT / Cursor.
        כתובת השרת: <code dir="ltr" style={{ background: "var(--surface)", padding: "0.1rem 0.4rem", borderRadius: "4px" }}>{MCP_URL}</code>
        {" · "}המשתמש מתחבר עם אותה כתובת Google שהוזמנה.
      </div>

      <form onSubmit={invite} style={{ display: "flex", gap: "0.5rem", flexWrap: "wrap", margin: "0.85rem 0" }}>
        <input
          type="email" required value={email} onChange={(e) => setEmail(e.target.value)}
          placeholder="כתובת Google להזמנה" dir="ltr"
          style={{ flex: "1 1 16rem", padding: "0.4rem 0.6rem", border: "1px solid var(--border)", borderRadius: "6px", fontSize: "0.85rem" }}
        />
        <input
          type="text" value={name} onChange={(e) => setName(e.target.value)}
          placeholder="שם (אופציונלי)"
          style={{ flex: "0 1 12rem", padding: "0.4rem 0.6rem", border: "1px solid var(--border)", borderRadius: "6px", fontSize: "0.85rem" }}
        />
        <button className="btn-primary" type="submit" disabled={busy} style={{ padding: "0.4rem 1rem", fontSize: "0.85rem" }}>
          {busy ? "מזמין…" : "הזמן"}
        </button>
      </form>

      {err && <div className="text-sm" style={{ color: "#b91c1c", marginBottom: "0.5rem" }}>{err}</div>}

      {loading ? (
        <div className="empty-state" style={{ padding: "1.5rem" }}>טוען…</div>
      ) : users.length === 0 ? (
        <div className="empty-state" style={{ padding: "1.5rem" }}>אין עדיין משתמשים מוזמנים. הזמן את עצמך כדי להתחבר.</div>
      ) : (
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.85rem" }}>
            <thead>
              <tr style={{ textAlign: "right", color: "var(--text-muted)", borderBottom: "2px solid var(--border)" }}>
                <th style={{ padding: "0.5rem 0.6rem" }}>אימייל</th>
                <th style={{ padding: "0.5rem 0.6rem" }}>שם</th>
                <th style={{ padding: "0.5rem 0.6rem" }}>סטטוס</th>
                <th style={{ padding: "0.5rem 0.6rem" }}>רמה</th>
                <th style={{ padding: "0.5rem 0.6rem" }}>קריאות (30 יום)</th>
                <th style={{ padding: "0.5rem 0.6rem" }}>נראה לאחרונה</th>
                <th style={{ padding: "0.5rem 0.6rem" }}>פעולות</th>
              </tr>
            </thead>
            <tbody>
              {users.map((u) => (
                <tr key={u.id} style={{ borderBottom: "1px solid var(--border)", opacity: u.is_active ? 1 : 0.55 }}>
                  <td style={{ padding: "0.5rem 0.6rem" }} dir="ltr">{u.email}</td>
                  <td style={{ padding: "0.5rem 0.6rem" }}>{u.name || "—"}</td>
                  <td style={{ padding: "0.5rem 0.6rem" }}>
                    <span style={{
                      padding: "0.1rem 0.5rem", borderRadius: "999px", fontSize: "0.75rem", fontWeight: 600,
                      background: u.is_active ? "#ecfdf5" : "#fef2f2", color: u.is_active ? "#065f46" : "#991b1b",
                    }}>{u.is_active ? "פעיל" : "מושבת"}</span>
                  </td>
                  <td style={{ padding: "0.5rem 0.6rem" }}>
                    <select value={u.tier} onChange={(e) => setTier(u, e.target.value)}
                      style={{ padding: "0.15rem 0.4rem", fontSize: "0.78rem", border: "1px solid var(--border)", borderRadius: "4px" }}>
                      <option value="beta">beta</option>
                      <option value="free">free</option>
                      <option value="pro">pro</option>
                    </select>
                  </td>
                  <td style={{ padding: "0.5rem 0.6rem", fontVariantNumeric: "tabular-nums" }}>{u.calls_30d.toLocaleString()}</td>
                  <td style={{ padding: "0.5rem 0.6rem", color: "var(--text-muted)" }}>{fmt(u.last_seen_at)}</td>
                  <td style={{ padding: "0.5rem 0.6rem" }}>
                    <button onClick={() => toggle(u)} className={u.is_active ? "btn-danger" : "btn-secondary"}
                      style={{ padding: "0.2rem 0.6rem", fontSize: "0.75rem" }}>
                      {u.is_active ? "השבת" : "הפעל"}
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}
