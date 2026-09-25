import { useEffect, useMemo, useState } from "react";
import {
  admin as adminApi, ApiAccessFilters, ApiAccessGroup, ApiAccessRow, ApiAccessStats,
} from "../api/client";

// The API access log (app.api_access_log): who calls /api/* and the MCP servers,
// how, when and for what. Every breakdown row is clickable and narrows all the
// others to it; our own frontend's calls are hidden by default ("site").

const CHANNEL_LABELS: Record<string, string> = {
  site: "האתר עצמו", mcp: "MCP", script: "סקריפט / קוד", browser: "דפדפן (מחוץ לאתר)",
  bot: "בוט / זחלן", apps_script: "Looker / Apps Script", other: "אחר",
};
const ACTOR_LABELS: Record<string, string> = {
  anonymous: "אנונימי", user: "משתמש מחובר", mcp_user: "משתמש MCP", mcp_service: "שירות MCP",
  sql_service: "שירות SQL", connector: "מחבר Looker", bearer_invalid: "טוקן לא תקף", unknown: "לא ידוע",
};
const DAYS_OPTIONS = [1, 3, 7, 30, 90, 180];
const DOW = ["", "ב׳", "ג׳", "ד׳", "ה׳", "ו׳", "ש׳", "א׳"]; // isodow 1=Mon … 7=Sun

type FilterKey = Exclude<keyof ApiAccessFilters, "days" | "exclude_site">;
const FILTER_LABELS: Record<FilterKey, string> = {
  area: "API", channel: "ערוץ", actor_kind: "זהות", ip: "IP", actor_id: "משתמש",
  client: "לקוח", status: "סטטוס", route: "נתיב", target: "יעד",
};

const nf = new Intl.NumberFormat("he-IL");
const n = (v: number | null | undefined) => (v == null ? "—" : nf.format(Math.round(Number(v))));

function bytes(v: number | null | undefined): string {
  const b = Number(v || 0);
  if (b < 1024) return `${b} B`;
  const u = ["KB", "MB", "GB", "TB"];
  let x = b / 1024, i = 0;
  while (x >= 1024 && i < u.length - 1) { x /= 1024; i++; }
  return `${x.toFixed(x < 10 ? 1 : 0)} ${u[i]}`;
}

function when(iso: string | null | undefined): string {
  if (!iso) return "—";
  return new Date(iso).toLocaleString("he-IL", {
    day: "2-digit", month: "2-digit", hour: "2-digit", minute: "2-digit",
  });
}

const cell: React.CSSProperties = { padding: "0.35rem 0.5rem", whiteSpace: "nowrap" };
const th: React.CSSProperties = { ...cell, textAlign: "right", color: "var(--text-muted)", fontWeight: 600 };

function Tile({ label, value, sub }: { label: string; value: string; sub?: string }) {
  return (
    <div style={{ flex: "1 1 9rem", background: "var(--surface-2)", borderRadius: "8px", padding: "0.6rem 0.8rem" }}>
      <div className="text-muted" style={{ fontSize: "0.75rem" }}>{label}</div>
      <div style={{ fontSize: "1.35rem", fontWeight: 700 }}>{value}</div>
      {sub && <div className="text-muted" style={{ fontSize: "0.72rem" }}>{sub}</div>}
    </div>
  );
}

function Breakdown({ title, rows, label, onPick }: {
  title: string; rows: ApiAccessGroup[]; label?: (k: string) => string; onPick?: (k: string) => void;
}) {
  const max = Math.max(1, ...rows.map((r) => r.requests));
  return (
    <div className="card" style={{ padding: "0.7rem 0.8rem", flex: "1 1 20rem", minWidth: 0 }}>
      <h3 style={{ fontSize: "0.95rem", margin: "0 0 0.4rem" }}>{title}</h3>
      {rows.length === 0 ? <div className="text-muted text-sm">אין נתונים</div> : (
        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.8rem" }}>
          <thead><tr>
            <th scope="col" style={th}></th><th scope="col" style={th}>קריאות</th>
            <th scope="col" style={th}>IPs</th><th scope="col" style={th}>נפח</th>
            <th scope="col" style={th}>שגיאות</th>
          </tr></thead>
          <tbody>
            {rows.map((r) => {
              const k = r.key ?? "";
              return (
                <tr key={k} style={{ borderTop: "1px solid var(--border)" }}>
                  <td style={{ ...cell, whiteSpace: "normal", width: "45%" }}>
                    <button type="button" onClick={() => onPick?.(k)} disabled={!onPick}
                      title={onPick ? "סנן לפי ערך זה" : undefined}
                      style={{ all: "unset", cursor: onPick ? "pointer" : "default", display: "block", width: "100%" }}>
                      <span dir="auto" style={{ wordBreak: "break-all" }}>{label ? label(k) : k || "—"}</span>
                      <span aria-hidden="true" style={{ display: "block", height: "4px", marginTop: "2px", borderRadius: "2px",
                        background: "var(--primary-400)", width: `${(r.requests / max) * 100}%` }} />
                    </button>
                  </td>
                  <td style={cell}>{n(r.requests)}</td>
                  <td style={cell}>{n(r.ips)}</td>
                  <td style={cell}>{bytes(r.bytes)}</td>
                  <td style={{ ...cell, color: r.errors ? "var(--tint-bad-fg)" : undefined }}>{n(r.errors)}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      )}
    </div>
  );
}

function Series({ stats }: { stats: ApiAccessStats }) {
  const s = stats.series;
  if (!s.length) return null;
  const max = Math.max(1, ...s.map((p) => p.requests));
  const fmt = (t: string) => {
    const d = new Date(t);
    return stats.bucket === "hour"
      ? d.toLocaleString("he-IL", { day: "2-digit", month: "2-digit", hour: "2-digit" })
      : d.toLocaleDateString("he-IL", { day: "2-digit", month: "2-digit" });
  };
  return (
    <div className="card" style={{ padding: "0.7rem 0.8rem", marginBottom: "0.8rem" }}>
      <h3 style={{ fontSize: "0.95rem", margin: "0 0 0.5rem" }}>
        קריאות לפי {stats.bucket === "hour" ? "שעה" : "יום"} (שעון ישראל)
      </h3>
      <div dir="ltr" role="img" aria-label="גרף קריאות לאורך זמן"
        style={{ display: "flex", alignItems: "flex-end", gap: "2px", height: "120px" }}>
        {s.map((p) => (
          <div key={p.t} title={`${fmt(p.t)} · ${n(p.requests)} קריאות · ${n(p.ips)} IPs · ${bytes(p.bytes)} · ${n(p.errors)} שגיאות`}
            style={{ flex: "1 1 0", minWidth: "2px", display: "flex", flexDirection: "column", justifyContent: "flex-end", height: "100%" }}>
            <div style={{ height: `${((p.requests - p.errors) / max) * 100}%`, background: "var(--primary-500)" }} />
            <div style={{ height: `${(p.errors / max) * 100}%`, background: "var(--tint-bad-bd)" }} />
          </div>
        ))}
      </div>
      <div dir="ltr" className="text-muted" style={{ display: "flex", justifyContent: "space-between", fontSize: "0.7rem", marginTop: "0.2rem" }}>
        <span>{fmt(s[0].t)}</span><span>{fmt(s[s.length - 1].t)}</span>
      </div>
    </div>
  );
}

function Heatmap({ stats }: { stats: ApiAccessStats }) {
  const grid = useMemo(() => {
    const m = new Map<string, number>();
    for (const c of stats.heatmap) m.set(`${c.dow}-${c.hour}`, c.requests);
    return m;
  }, [stats]);
  const max = Math.max(1, ...stats.heatmap.map((c) => c.requests));
  const order = [7, 1, 2, 3, 4, 5, 6]; // Sunday first
  return (
    <div className="card" style={{ padding: "0.7rem 0.8rem", flex: "1 1 26rem", minWidth: 0 }}>
      <h3 style={{ fontSize: "0.95rem", margin: "0 0 0.4rem" }}>מתי: יום בשבוע × שעה</h3>
      <div style={{ overflowX: "auto" }}>
        <table dir="ltr" style={{ borderCollapse: "collapse", fontSize: "0.65rem" }}>
          <thead><tr><th />{Array.from({ length: 24 }, (_, h) => (
            <th key={h} scope="col" style={{ padding: "0 1px", color: "var(--text-muted)", fontWeight: 400 }}>{h}</th>))}</tr></thead>
          <tbody>
            {order.map((d) => (
              <tr key={d}>
                <th scope="row" style={{ paddingInlineEnd: "4px", color: "var(--text-muted)", fontWeight: 400 }}>{DOW[d]}</th>
                {Array.from({ length: 24 }, (_, h) => {
                  const v = grid.get(`${d}-${h}`) || 0;
                  return <td key={h} title={`${DOW[d]} ${h}:00 · ${n(v)}`}
                    style={{ width: "14px", height: "14px", background: v ? "var(--primary-600)" : "var(--surface-2)",
                      opacity: v ? 0.15 + 0.85 * (v / max) : 1, border: "1px solid var(--surface)" }} />;
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export default function ApiAccessPanel() {
  const [filters, setFilters] = useState<ApiAccessFilters>({ days: 7, exclude_site: true });
  const [stats, setStats] = useState<ApiAccessStats | null>(null);
  const [rows, setRows] = useState<ApiAccessRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState<string | null>(null);
  const [showRaw, setShowRaw] = useState(false);

  const load = async (f: ApiAccessFilters) => {
    setLoading(true);
    setErr(null);
    try {
      const [s, r] = await Promise.all([adminApi.apiAccessStats(f), adminApi.apiAccessRecent(f, 200)]);
      setStats(s);
      setRows(r.rows);
    } catch (e) {
      setErr((e as Error)?.message ?? String(e));
    } finally {
      setLoading(false);
    }
  };
  useEffect(() => { load(filters); }, [filters]);

  const pick = (k: FilterKey) => (v: string) => setFilters((f) => ({ ...f, [k]: v }));
  const active = (Object.keys(FILTER_LABELS) as FilterKey[]).filter((k) => filters[k]);
  const t = stats?.totals;

  return (
    <section className="card mb-2" style={{ padding: "1rem 1.25rem" }}>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", flexWrap: "wrap", gap: "0.5rem" }}>
        <h2 style={{ fontSize: "1.25rem", fontWeight: 700, margin: 0 }}><span aria-hidden="true">📈</span> גישה ל-API</h2>
        <div style={{ display: "flex", gap: "0.6rem", alignItems: "center", flexWrap: "wrap", fontSize: "0.85rem" }}>
          <label>טווח:{" "}
            <select value={filters.days} onChange={(e) => setFilters((f) => ({ ...f, days: Number(e.target.value) }))}
              style={{ padding: "0.2rem 0.4rem", border: "1px solid var(--border)", borderRadius: "4px" }}>
              {DAYS_OPTIONS.map((d) => <option key={d} value={d}>{d === 1 ? "יממה" : `${d} ימים`}</option>)}
            </select>
          </label>
          <label style={{ display: "flex", gap: "0.3rem", alignItems: "center" }}>
            <input type="checkbox" checked={filters.exclude_site}
              onChange={(e) => setFilters((f) => ({ ...f, exclude_site: e.target.checked }))} />
            בלי קריאות של האתר עצמו
          </label>
          <button className="btn-secondary" type="button" onClick={() => load(filters)} style={{ padding: "0.25rem 0.7rem" }}>רענון</button>
        </div>
      </div>

      <div className="text-sm" style={{ margin: "0.5rem 0", color: "var(--text-muted)", lineHeight: 1.6 }}>
        שורה לכל קריאה ל-<code>/api/*</code> ולשרתי ה-MCP (בלי worker ובלי admin). טוקנים ומפתחות לא נשמרים.
        לחיצה על שורה בכל פילוח מסננת את כל המסך לפיה.{stats ? ` הנתונים נשמרים ${stats.retention_days} ימים.` : ""}
      </div>

      {active.length > 0 && (
        <div style={{ display: "flex", gap: "0.4rem", flexWrap: "wrap", marginBottom: "0.6rem" }}>
          {active.map((k) => (
            <button key={k} type="button" onClick={() => setFilters((f) => ({ ...f, [k]: undefined }))}
              style={{ border: "1px solid var(--border)", background: "var(--primary-50)", borderRadius: "999px",
                padding: "0.15rem 0.6rem", fontSize: "0.8rem", cursor: "pointer" }}
              aria-label={`הסר סינון ${FILTER_LABELS[k]}`}>
              {FILTER_LABELS[k]}: <span dir="auto">{String(filters[k])}</span> ✕
            </button>
          ))}
        </div>
      )}

      {err && <div className="text-sm" style={{ color: "var(--danger)", marginBottom: "0.5rem" }}>{err}</div>}
      {loading && !stats && <div className="empty-state" style={{ padding: "1.5rem" }}>טוען…</div>}

      {stats && t && (
        <div style={{ opacity: loading ? 0.6 : 1 }}>
          <div style={{ display: "flex", gap: "0.6rem", flexWrap: "wrap", marginBottom: "0.8rem" }}>
            <Tile label="קריאות" value={n(t.requests)} sub={t.first_ts ? `${when(t.first_ts)} – ${when(t.last_ts)}` : undefined} />
            <Tile label="כתובות IP שונות" value={n(t.unique_ips)} />
            <Tile label="משתמשים מזוהים" value={n(t.unique_actors)} />
            <Tile label="נפח שנשלח" value={bytes(t.bytes)} />
            <Tile label="שגיאות" value={n(t.errors)}
              sub={t.requests ? `${((t.errors / t.requests) * 100).toFixed(1)}% · ${n(t.throttled)} נחסמו (429)` : undefined} />
            <Tile label="זמן תגובה" value={`${n(t.p50_ms)} ms`} sub={`p95: ${n(t.p95_ms)} ms`} />
          </div>

          <Series stats={stats} />

          <div style={{ display: "flex", gap: "0.8rem", flexWrap: "wrap", marginBottom: "0.8rem" }}>
            <Breakdown title="איך: ערוץ" rows={stats.by_channel} label={(k) => CHANNEL_LABELS[k] ?? k} onPick={pick("channel")} />
            <Breakdown title="לאיזה API" rows={stats.by_area} onPick={pick("area")} />
          </div>
          <div style={{ display: "flex", gap: "0.8rem", flexWrap: "wrap", marginBottom: "0.8rem" }}>
            <Breakdown title="מי: סוג זהות" rows={stats.by_actor_kind} label={(k) => ACTOR_LABELS[k] ?? k} onPick={pick("actor_kind")} />
            <Breakdown title="באיזה כלי (User-Agent)" rows={stats.by_client} onPick={pick("client")} />
          </div>
          <div style={{ display: "flex", gap: "0.8rem", flexWrap: "wrap", marginBottom: "0.8rem" }}>
            <Breakdown title="למה: על איזה מאגר / יעד" rows={stats.by_target}
              onPick={(k) => setFilters((f) => ({ ...f, target: k.split(" · ").slice(1).join(" · ") }))} />
            <Breakdown title="מאיפה הפנו (Referer)" rows={stats.by_referer} />
          </div>
          <div style={{ display: "flex", gap: "0.8rem", flexWrap: "wrap", marginBottom: "0.8rem" }}>
            <Breakdown title="מדינה" rows={stats.by_country} />
            <Breakdown title="סטטוס" rows={stats.by_status} />
            <Heatmap stats={stats} />
          </div>

          <div className="card" style={{ padding: "0.7rem 0.8rem", marginBottom: "0.8rem" }}>
            <h3 style={{ fontSize: "0.95rem", margin: "0 0 0.4rem" }}>נקודות קצה</h3>
            <div tabIndex={0} role="region" aria-label="נקודות קצה" className="scroll-region" style={{ overflowX: "auto" }}>
              <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.8rem" }}>
                <thead><tr>
                  {["נתיב", "קריאות", "IPs", "נפח", "שגיאות", "ממוצע ms"].map((h) => <th key={h} scope="col" style={th}>{h}</th>)}
                </tr></thead>
                <tbody>
                  {stats.routes.map((r) => (
                    <tr key={`${r.method} ${r.route}`} style={{ borderTop: "1px solid var(--border)" }}>
                      <td style={cell} dir="ltr">
                        <button type="button" onClick={() => setFilters((f) => ({ ...f, route: r.route }))}
                          style={{ all: "unset", cursor: "pointer" }} title="סנן לפי נתיב זה">
                          <b>{r.method}</b> {r.route}
                        </button>
                      </td>
                      <td style={cell}>{n(r.requests)}</td><td style={cell}>{n(r.ips)}</td>
                      <td style={cell}>{bytes(r.bytes)}</td><td style={cell}>{n(r.errors)}</td><td style={cell}>{n(r.avg_ms)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          <div style={{ display: "flex", gap: "0.8rem", flexWrap: "wrap", marginBottom: "0.8rem" }}>
            <div className="card" style={{ padding: "0.7rem 0.8rem", flex: "2 1 34rem", minWidth: 0 }}>
              <h3 style={{ fontSize: "0.95rem", margin: "0 0 0.4rem" }}>מי: כתובות IP מובילות</h3>
              <div tabIndex={0} role="region" aria-label="כתובות IP מובילות" className="scroll-region" style={{ overflowX: "auto" }}>
                <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.8rem" }}>
                  <thead><tr>
                    {["IP", "קריאות", "נפח", "שגיאות", "נתיבים", "כלי", "ערוץ", "מדינה", "API עיקרי", "מזוהה כ-", "לאחרונה"].map((h) =>
                      <th key={h} scope="col" style={th}>{h}</th>)}
                  </tr></thead>
                  <tbody>
                    {stats.top_ips.map((r) => (
                      <tr key={r.ip} style={{ borderTop: "1px solid var(--border)" }}>
                        <td style={cell} dir="ltr">
                          <button type="button" onClick={() => setFilters((f) => ({ ...f, ip: r.ip }))}
                            style={{ all: "unset", cursor: "pointer", textDecoration: "underline" }}>{r.ip}</button>
                        </td>
                        <td style={cell}>{n(r.requests)}</td><td style={cell}>{bytes(r.bytes)}</td>
                        <td style={cell}>{n(r.errors)}</td><td style={cell}>{n(r.routes)}</td>
                        <td style={cell}>{r.client ?? "—"}</td>
                        <td style={cell}>{CHANNEL_LABELS[r.channel ?? ""] ?? r.channel ?? "—"}</td>
                        <td style={cell}>{r.country ?? "—"}</td><td style={cell}>{r.top_area ?? "—"}</td>
                        <td style={cell} dir="ltr">{r.actor_label ?? ""}</td>
                        <td style={cell}>{when(r.last_ts)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
            <div className="card" style={{ padding: "0.7rem 0.8rem", flex: "1 1 22rem", minWidth: 0 }}>
              <h3 style={{ fontSize: "0.95rem", margin: "0 0 0.4rem" }}>מי: משתמשים מזוהים</h3>
              {stats.top_actors.length === 0 ? <div className="text-muted text-sm">אין קריאות מזוהות בטווח</div> : (
                <div tabIndex={0} role="region" aria-label="משתמשים מזוהים" className="scroll-region" style={{ overflowX: "auto" }}>
                  <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.8rem" }}>
                    <thead><tr>
                      {["מי", "סוג", "קריאות", "נפח", "IPs", "API עיקרי", "לאחרונה"].map((h) => <th key={h} scope="col" style={th}>{h}</th>)}
                    </tr></thead>
                    <tbody>
                      {stats.top_actors.map((r) => (
                        <tr key={`${r.actor_kind}-${r.actor_id}`} style={{ borderTop: "1px solid var(--border)" }}>
                          <td style={cell} dir="ltr">
                            {r.actor_id ? (
                              <button type="button" onClick={() => setFilters((f) => ({ ...f, actor_id: r.actor_id! }))}
                                style={{ all: "unset", cursor: "pointer", textDecoration: "underline" }}>{r.label ?? r.actor_id}</button>
                            ) : (r.label ?? "—")}
                          </td>
                          <td style={cell}>{ACTOR_LABELS[r.actor_kind] ?? r.actor_kind}</td>
                          <td style={cell}>{n(r.requests)}</td><td style={cell}>{bytes(r.bytes)}</td>
                          <td style={cell}>{n(r.ips)}</td><td style={cell}>{r.top_area ?? "—"}</td>
                          <td style={cell}>{when(r.last_ts)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          </div>

          <div className="card" style={{ padding: "0.7rem 0.8rem" }}>
            <button type="button" className="btn-secondary" onClick={() => setShowRaw((v) => !v)} style={{ padding: "0.25rem 0.7rem" }}>
              {showRaw ? "הסתר" : "הצג"} קריאות אחרונות ({rows.length})
            </button>
            {showRaw && (
              <div tabIndex={0} role="region" aria-label="קריאות אחרונות" className="scroll-region" style={{ overflowX: "auto", marginTop: "0.5rem" }}>
                <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.75rem" }}>
                  <thead><tr>
                    {["זמן", "סטטוס", "קריאה", "IP", "מי", "כלי", "ערוץ", "ms", "נפח", "Referer"].map((h) => <th key={h} scope="col" style={th}>{h}</th>)}
                  </tr></thead>
                  <tbody>
                    {rows.map((r) => (
                      <tr key={r.id} style={{ borderTop: "1px solid var(--border)" }}>
                        <td style={cell}>{when(r.ts)}</td>
                        <td style={{ ...cell, color: r.status >= 400 ? "var(--tint-bad-fg)" : undefined }}>{r.status}</td>
                        <td style={{ ...cell, whiteSpace: "normal", wordBreak: "break-all", minWidth: "16rem" }} dir="ltr" title={r.user_agent ?? ""}>
                          <b>{r.method}</b> {r.path}{r.query ? `?${r.query}` : ""}
                        </td>
                        <td style={cell} dir="ltr">{r.ip ?? "—"}{r.country ? ` · ${r.country}` : ""}</td>
                        <td style={cell} dir="ltr">{r.actor_label ?? ACTOR_LABELS[r.actor_kind] ?? r.actor_kind}</td>
                        <td style={cell}>{r.client ?? "—"}</td>
                        <td style={cell}>{CHANNEL_LABELS[r.channel] ?? r.channel}</td>
                        <td style={cell}>{n(r.duration_ms)}</td>
                        <td style={cell}>{bytes(r.bytes_out)}</td>
                        <td style={cell} dir="ltr">{r.referer_host ?? ""}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </div>
      )}
    </section>
  );
}
