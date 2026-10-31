/**
 * בדיקות מערכת — the system health checks (app/services/health_checks.py).
 *
 * Three things on one tab:
 *   1. the current state: the newest verdict of every check, broken ones first,
 *      each with "since when" and its sub-results (one row per scraper source,
 *      per upstream, per worker);
 *   2. the buttons: run the daily pass, the full weekly pass, or one check;
 *   3. the log: every run, what it found new and what it found resolved.
 */
import { Fragment, useCallback, useEffect, useRef, useState } from "react";
import { Link } from "react-router-dom";
import {
  admin as adminApi,
  HealthChecksInfo,
  HealthItem,
  HealthOverview,
  HealthResult,
  HealthRunRow,
  HealthStatus,
} from "../api/client";
import { useAutoRefresh } from "../hooks/useAutoRefresh";
import { AutoRefreshToggle } from "./AutoRefreshToggle";

const STATUS: Record<HealthStatus, { label: string; emoji: string; fg: string; bg: string }> = {
  ok: { label: "תקין", emoji: "✅", fg: "var(--tint-good-fg)", bg: "var(--tint-good-bg)" },
  warn: { label: "אזהרה", emoji: "⚠️", fg: "var(--tint-warn-fg)", bg: "var(--tint-warn-bg)" },
  fail: { label: "תקלה", emoji: "❌", fg: "var(--tint-bad-fg)", bg: "var(--tint-bad-bg)" },
  error: { label: "הבדיקה קרסה", emoji: "💥", fg: "var(--tint-bad-fg)", bg: "var(--tint-bad-bg)" },
  skip: { label: "דולג", emoji: "⏭", fg: "var(--tint-neutral-fg)", bg: "var(--tint-neutral-bg)" },
  running: { label: "רץ", emoji: "⏳", fg: "var(--tint-info-fg)", bg: "var(--tint-info-bg)" },
};
const RANK: Record<string, number> = { fail: 0, error: 0, warn: 1, running: 2, ok: 3, skip: 4 };
const TIER_LABEL: Record<string, string> = { daily: "יומית", weekly: "שבועית", manual: "ידנית" };

function fmt(iso?: string | null): string {
  if (!iso) return "—";
  return new Date(iso).toLocaleString("he-IL", {
    day: "2-digit", month: "2-digit", hour: "2-digit", minute: "2-digit",
  });
}

function since(iso?: string): string | null {
  if (!iso) return null;
  const h = (Date.now() - new Date(iso).getTime()) / 3.6e6;
  if (h < 1) return "מהשעה האחרונה";
  if (h < 48) return `כבר ${Math.round(h)} שעות`;
  return `כבר ${Math.round(h / 24)} ימים (מאז ${fmt(iso)})`;
}

function dur(ms?: number | null): string {
  if (ms == null) return "—";
  return ms < 1000 ? `${ms} ms` : ms < 120000 ? `${(ms / 1000).toFixed(1)} שנ׳` : `${Math.round(ms / 60000)} דק׳`;
}

function Badge({ status }: { status: HealthStatus }) {
  const m = STATUS[status] ?? STATUS.skip;
  return (
    <span style={{
      display: "inline-block", whiteSpace: "nowrap", fontSize: "0.75rem", fontWeight: 600,
      color: m.fg, background: m.bg, borderRadius: 999, padding: "0.05rem 0.5rem",
    }}>
      <span aria-hidden="true">{m.emoji}</span> {m.label}
    </span>
  );
}

const cell = { padding: "0.3rem 0.45rem", verticalAlign: "top" } as const;

function DatasetList({ title, rows }: {
  title: string;
  rows: { dataset_id: string; title: string; note: string | null }[];
}) {
  if (!rows.length) return null;
  return (
    <div style={{ marginTop: "0.3rem" }}>
      <div className="text-muted" style={{ fontSize: "0.75rem" }}>{title}</div>
      <ul style={{ margin: "0.15rem 0 0", paddingInlineStart: "1.1rem", fontSize: "0.8rem" }}>
        {rows.map((r) => (
          <li key={r.dataset_id}>
            <Link to={`/versions/${r.dataset_id}`}>{r.title}</Link>
            {r.note && <span className="text-muted" style={{ overflowWrap: "anywhere" }}> — {r.note}</span>}
          </li>
        ))}
      </ul>
    </div>
  );
}

function ItemDetails({ it }: { it: HealthItem }) {
  return (
    <>
      <DatasetList title="דוגמאות לכשלים"
        rows={(it.samples ?? []).map((s) => ({ dataset_id: s.dataset_id, title: s.title, note: s.error }))} />
      <DatasetList title="באיחור"
        rows={(it.late ?? []).map((s) => ({ dataset_id: s.dataset_id, title: s.title, note: `poll אחרון ${s.last_polled}` }))} />
      <DatasetList title="בלי איסוף מוצלח"
        rows={(it.stale ?? []).map((s) => ({ dataset_id: s.dataset_id, title: s.title, note: `הצלחה אחרונה ${s.last_success}${s.error ? ` · ${s.error}` : ""}` }))} />
    </>
  );
}

function ResultBody({ r }: { r: HealthResult }) {
  const items = [...(r.items ?? [])].sort((a, b) => (RANK[a.status] ?? 9) - (RANK[b.status] ?? 9));
  const details = r.details && Object.keys(r.details).length ? r.details : null;
  return (
    <div style={{ padding: "0.3rem 0.2rem 0.6rem" }}>
      {items.length > 0 && (
        <table style={{ width: "100%", fontSize: "0.8rem", borderCollapse: "collapse" }}>
          <tbody>
            {items.map((it) => (
              <tr key={it.key} style={{ borderTop: "1px solid var(--border)" }}>
                <td style={{ ...cell, width: "7rem" }}><Badge status={it.status} /></td>
                <td style={{ ...cell, fontWeight: 600, whiteSpace: "nowrap" }}>{it.label}</td>
                <td style={cell}>
                  {it.message}
                  {it.since && it.status !== "ok" && (
                    <span className="text-muted"> · {since(it.since)}</span>
                  )}
                  <ItemDetails it={it} />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      {details && (
        <details style={{ marginTop: "0.4rem" }}>
          <summary className="text-sm text-muted" style={{ cursor: "pointer" }}>פרטים גולמיים</summary>
          <pre dir="ltr" style={{
            fontSize: "0.72rem", background: "var(--code-bg)", padding: "0.5rem",
            borderRadius: 4, overflowX: "auto", maxHeight: 320, whiteSpace: "pre-wrap",
          }}>{JSON.stringify(details, null, 2)}</pre>
        </details>
      )}
    </div>
  );
}

function ResultsTable({ results, onRerun, busy, showWhen }: {
  results: HealthResult[];
  onRerun?: (key: string) => void;
  busy?: boolean;
  showWhen?: boolean;
}) {
  const [open, setOpen] = useState<Set<string>>(new Set());
  const toggle = (k: string) => setOpen((s) => {
    const n = new Set(s);
    if (n.has(k)) n.delete(k); else n.add(k);
    return n;
  });
  const sorted = [...results].sort((a, b) => (RANK[a.status] ?? 9) - (RANK[b.status] ?? 9));
  return (
    <div tabIndex={0} role="region" aria-label="תוצאות בדיקות" className="scroll-region" style={{ overflowX: "auto" }}>
      <table style={{ width: "100%", fontSize: "0.85rem", borderCollapse: "collapse" }}>
        <thead>
          <tr style={{ color: "var(--text-muted)" }}>
            <th scope="col" style={{ ...cell, textAlign: "start" }}>מצב</th>
            <th scope="col" style={{ ...cell, textAlign: "start" }}>רכיב</th>
            <th scope="col" style={{ ...cell, textAlign: "start" }}>בדיקה</th>
            <th scope="col" style={{ ...cell, textAlign: "start" }}>ממצא</th>
            {showWhen && <th scope="col" style={{ ...cell, textAlign: "start" }}>נבדק</th>}
            {onRerun && <th scope="col" style={cell} />}
          </tr>
        </thead>
        <tbody>
          {sorted.map((r) => {
            const expandable = (r.items && r.items.length > 0) || (r.details && Object.keys(r.details).length > 0);
            const isOpen = open.has(r.key);
            return (
              <Fragment key={r.key}>
                <tr style={{ borderTop: "1px solid var(--border)" }}>
                  <td style={cell}><Badge status={r.status} /></td>
                  <td style={{ ...cell, whiteSpace: "nowrap" }} className="text-muted">{r.component}</td>
                  <td style={cell}>
                    {expandable ? (
                      <button type="button" onClick={() => toggle(r.key)} aria-expanded={isOpen}
                        style={{ background: "none", border: "none", padding: 0, cursor: "pointer", color: "var(--primary)", fontWeight: 600, textAlign: "start" }}>
                        {isOpen ? "▾" : "▸"} {r.title}
                      </button>
                    ) : <span style={{ fontWeight: 600 }}>{r.title}</span>}
                    <div className="text-muted" dir="ltr" style={{ fontSize: "0.7rem", textAlign: "end" }}>{r.key}</div>
                  </td>
                  <td style={cell}>
                    {r.message}
                    {r.since && r.status !== "ok" && r.status !== "skip" && (
                      <div className="text-muted" style={{ fontSize: "0.75rem" }}>{since(r.since)}</div>
                    )}
                  </td>
                  {showWhen && (
                    <td style={{ ...cell, whiteSpace: "nowrap" }} className="text-muted">
                      {fmt(r.checked_at)}
                      <div style={{ fontSize: "0.72rem" }}>{TIER_LABEL[r.run_tier ?? ""] ?? r.run_tier} · {dur(r.duration_ms)}</div>
                    </td>
                  )}
                  {onRerun && (
                    <td style={cell}>
                      <button type="button" disabled={busy} onClick={() => onRerun(r.key)} className="text-sm"
                        style={{ background: "none", border: "1px solid var(--border)", borderRadius: 4, padding: "0.1rem 0.5rem", cursor: "pointer", whiteSpace: "nowrap" }}>
                        הרץ שוב
                      </button>
                    </td>
                  )}
                </tr>
                {isOpen && (
                  <tr>
                    <td colSpan={4 + (showWhen ? 1 : 0) + (onRerun ? 1 : 0)}><ResultBody r={r} /></td>
                  </tr>
                )}
              </Fragment>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function RunLog({ refreshKey }: { refreshKey: number }) {
  const [runs, setRuns] = useState<HealthRunRow[]>([]);
  const [total, setTotal] = useState(0);
  const [tier, setTier] = useState("");
  const [status, setStatus] = useState("");
  const [offset, setOffset] = useState(0);
  const [openId, setOpenId] = useState<string | null>(null);
  const [detail, setDetail] = useState<HealthResult[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const LIMIT = 30;

  useEffect(() => {
    adminApi.healthRuns({ tier: tier || undefined, status: status || undefined, limit: LIMIT, offset })
      .then((d) => { setRuns(d.runs); setTotal(d.total); setError(null); })
      .catch((e) => setError(e instanceof Error ? e.message : String(e)));
  }, [tier, status, offset, refreshKey]);

  const openRun = async (id: string) => {
    if (openId === id) { setOpenId(null); return; }
    setOpenId(id);
    setDetail(null);
    try {
      setDetail((await adminApi.healthRun(id)).results);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  const sel = { fontSize: "0.85rem", padding: "0.2rem 0.4rem" } as const;
  return (
    <section className="card mb-2" style={{ padding: "1rem 1.25rem" }}>
      <div className="flex" style={{ justifyContent: "space-between", flexWrap: "wrap", gap: "0.6rem", alignItems: "center" }}>
        <h3 style={{ margin: 0, fontSize: "1.1rem" }}><span aria-hidden="true">📜</span> לוג ריצות</h3>
        <span className="flex" style={{ gap: "0.5rem", flexWrap: "wrap" }}>
          <label className="text-sm">סוג{" "}
            <select style={sel} value={tier} onChange={(e) => { setTier(e.target.value); setOffset(0); }}>
              <option value="">הכל</option>
              <option value="daily">יומית</option>
              <option value="weekly">שבועית</option>
              <option value="manual">ידנית</option>
            </select>
          </label>
          <label className="text-sm">תוצאה{" "}
            <select style={sel} value={status} onChange={(e) => { setStatus(e.target.value); setOffset(0); }}>
              <option value="">הכל</option>
              <option value="fail">תקלה</option>
              <option value="warn">אזהרה</option>
              <option value="ok">תקין</option>
              <option value="error">נקטעה</option>
            </select>
          </label>
        </span>
      </div>
      {error && <div className="text-sm" style={{ color: "var(--danger)" }}>{error}</div>}
      <div tabIndex={0} role="region" aria-label="לוג ריצות" className="scroll-region" style={{ overflowX: "auto", marginTop: "0.6rem" }}>
        <table style={{ width: "100%", fontSize: "0.85rem", borderCollapse: "collapse" }}>
          <thead>
            <tr style={{ color: "var(--text-muted)" }}>
              {["מתי", "סוג", "הופעל ע״י", "תוצאה", "סיכום", "חדש / נפתר", "משך"].map((h) => (
                <th key={h} scope="col" style={{ ...cell, textAlign: "start" }}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {runs.map((r) => {
              const s = r.summary || {};
              const isOpen = openId === r.id;
              return (
                <Fragment key={r.id}>
                  <tr style={{ borderTop: "1px solid var(--border)", cursor: "pointer", background: isOpen ? "var(--tint-neutral-bg)" : undefined }}
                      onClick={() => void openRun(r.id)}>
                    <td style={{ ...cell, whiteSpace: "nowrap" }}>
                      <button type="button" aria-expanded={isOpen} onClick={(e) => { e.stopPropagation(); void openRun(r.id); }}
                        style={{ background: "none", border: "none", padding: 0, cursor: "pointer", color: "var(--primary)" }}>
                        {isOpen ? "▾" : "▸"} {fmt(r.started_at)}
                      </button>
                    </td>
                    <td style={cell}>{TIER_LABEL[r.tier] ?? r.tier}</td>
                    <td style={{ ...cell, overflowWrap: "anywhere" }} className="text-muted">{r.trigger === "scheduler" ? "מתוזמן" : r.trigger}</td>
                    <td style={cell}><Badge status={r.status} /></td>
                    <td style={{ ...cell, whiteSpace: "nowrap" }}>
                      {s.note ? <span className="text-muted">{s.note}</span> : (
                        <>✅ {s.ok ?? 0} · ⚠️ {s.warn ?? 0} · ❌ {(s.fail ?? 0) + (s.error ?? 0)}</>
                      )}
                    </td>
                    <td style={{ ...cell, fontSize: "0.78rem" }} dir="ltr">
                      {(s.new_problems ?? []).length > 0 && (
                        <div style={{ color: "var(--tint-bad-fg)" }}>+ {(s.new_problems ?? []).join(", ")}</div>
                      )}
                      {(s.resolved ?? []).length > 0 && (
                        <div style={{ color: "var(--tint-good-fg)" }}>✓ {(s.resolved ?? []).join(", ")}</div>
                      )}
                    </td>
                    <td style={{ ...cell, whiteSpace: "nowrap" }} className="text-muted">{dur(r.duration_ms)}</td>
                  </tr>
                  {isOpen && (
                    <tr>
                      <td colSpan={7} style={{ padding: "0.4rem 0.2rem 0.8rem" }}>
                        {detail ? <ResultsTable results={detail} /> : <span className="text-muted text-sm">טוען…</span>}
                      </td>
                    </tr>
                  )}
                </Fragment>
              );
            })}
            {runs.length === 0 && (
              <tr><td colSpan={7} className="text-muted" style={{ padding: "0.6rem" }}>אין עדיין ריצות.</td></tr>
            )}
          </tbody>
        </table>
      </div>
      {total > LIMIT && (
        <div className="flex mt-1" style={{ gap: "0.5rem", alignItems: "center" }}>
          <button type="button" className="btn-secondary" disabled={offset === 0} onClick={() => setOffset(Math.max(0, offset - LIMIT))}>הקודם</button>
          <span className="text-sm text-muted">{offset + 1}–{Math.min(offset + LIMIT, total)} מתוך {total}</span>
          <button type="button" className="btn-secondary" disabled={offset + LIMIT >= total} onClick={() => setOffset(offset + LIMIT)}>הבא</button>
        </div>
      )}
    </section>
  );
}

export default function HealthChecksPanel() {
  const [info, setInfo] = useState<HealthChecksInfo | null>(null);
  const [ov, setOv] = useState<HealthOverview | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [note, setNote] = useState<string | null>(null);
  const [logKey, setLogKey] = useState(0);
  const wasBusy = useRef(false);

  const load = useCallback(async () => {
    try {
      const [i, o] = await Promise.all([adminApi.healthChecks(), adminApi.healthOverview()]);
      setInfo(i);
      setOv(o);
      setError(null);
      // A run just finished: refresh the log too.
      if (wasBusy.current && !o.busy) setLogKey((k) => k + 1);
      wasBusy.current = o.busy;
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, []);

  useEffect(() => { void load(); }, [load]);
  const regionRef = useRef<HTMLDivElement>(null);
  const { paused, toggle } = useAutoRefresh(load, ov?.busy ? 4000 : 30000, regionRef);

  const start = async (body: { tier?: string; checks?: string[] }, label: string) => {
    setNote(null);
    setError(null);
    try {
      await adminApi.healthStart(body);
      setNote(`${label} התחילה. התוצאות יופיעו כאן כשהיא תסתיים.`);
      wasBusy.current = true;
      setTimeout(() => void load(), 800);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  const busy = !!ov?.busy;
  const checks = ov?.checks ?? [];
  const bad = checks.filter((c) => ["fail", "error", "warn"].includes(c.status));
  const neverRan = (info?.checks ?? []).filter((d) => !checks.some((c) => c.key === d.key));

  const btn = (primary: boolean) => ({
    background: primary ? "var(--primary)" : "none",
    color: primary ? "#fff" : undefined,
    border: primary ? "none" : "1px solid var(--border)",
    borderRadius: 4, padding: "0.35rem 0.9rem", cursor: "pointer", fontWeight: 600,
  });

  return (
    <div ref={regionRef}>
      <section className="card mb-2" style={{ padding: "1rem 1.25rem" }}>
        <div className="flex" style={{ justifyContent: "space-between", flexWrap: "wrap", gap: "0.75rem", alignItems: "flex-start" }}>
        <p className="text-sm text-muted" style={{ lineHeight: 1.7, margin: "0 0 0.4rem", flex: "1 1 320px" }}>
          בדיקה יומית קלה (מסדי נתונים, R2, מתזמן, ורקרים, תורים, כל מקור גירוד, האתר והמקורות הממשלתיים)
          ובדיקה שבועית מקיפה שמוסיפה שבירה שקטה של סקרייפרים, מדגם קבצים ב-R2 ובדיקות קצה-לקצה מול האתר החי.
          {" "}בעיה שמופיעה כאן לראשונה מסומנת כ"חדשה"{info?.alerts_configured ? " ונשלחת להתראה" : ""}; בעיה שנמשכת מציגה ממתי.
        </p>
          <AutoRefreshToggle paused={paused} onToggle={toggle} seconds={busy ? 4 : 30} />
        </div>
        <div className="text-sm text-muted" style={{ display: "flex", gap: "1.2rem", flexWrap: "wrap" }}>
          <span>יומית הבאה: {fmt(info?.next_runs.daily)}</span>
          <span>שבועית הבאה: {fmt(info?.next_runs.weekly)}</span>
          {Object.entries(ov?.latest ?? {}).map(([t, r]) => (
            <span key={t}>{TIER_LABEL[t] ?? t} אחרונה: {fmt(r.started_at)} <Badge status={r.status} /></span>
          ))}
          {info && !info.alerts_configured && <span>התראות: לא מוגדרות (HEALTH_ALERT_WEBHOOK_URL)</span>}
          {info && !info.enabled && <span style={{ color: "var(--warning)" }}>הריצות המתוזמנות כבויות (HEALTH_CHECKS_ENABLED)</span>}
        </div>
        <div className="flex mt-1" style={{ gap: "0.6rem", flexWrap: "wrap", alignItems: "center" }}>
          <button type="button" className="text-sm" style={btn(true)} disabled={busy}
            onClick={() => void start({ tier: "daily" }, "הבדיקה היומית")}>הרץ בדיקה יומית</button>
          <button type="button" className="text-sm" style={btn(false)} disabled={busy}
            onClick={() => void start({ tier: "weekly" }, "הבדיקה השבועית המלאה")}>הרץ בדיקה שבועית מלאה</button>
          {busy && (
            <span className="text-sm" role="status"><Badge status="running" />{" "}
              {ov?.running ? `${TIER_LABEL[ov.running.tier] ?? ov.running.tier} — התחילה ${fmt(ov.running.started_at)}` : "בדיקה רצה…"}
            </span>
          )}
        </div>
        {note && <div className="text-sm" style={{ color: "var(--success)", marginTop: "0.4rem" }}>{note}</div>}
        {error && <div className="text-sm" style={{ color: "var(--danger)", marginTop: "0.4rem" }}>{error}</div>}
      </section>

      <section className="card mb-2" style={{ padding: "1rem 1.25rem" }}>
        <h3 style={{ margin: "0 0 0.5rem", fontSize: "1.1rem" }}>
          מצב נוכחי{" "}
          <span className="text-sm text-muted" style={{ fontWeight: 400 }}>
            {checks.length ? (bad.length ? `${bad.length} בדיקות דורשות תשומת לב` : "הכל תקין") : ""}
          </span>
        </h3>
        {checks.length > 0
          ? <ResultsTable results={checks} showWhen busy={busy}
              onRerun={(key) => void start({ checks: [key] }, `הבדיקה ${key}`)} />
          : <p className="text-muted text-sm">עוד לא רצה אף בדיקה. לחצו על "הרץ בדיקה יומית".</p>}
        {neverRan.length > 0 && checks.length > 0 && (
          <p className="text-sm text-muted" style={{ marginTop: "0.5rem" }}>
            עוד לא רצו: {neverRan.map((d) => d.title).join(" · ")} (רצות בבדיקה השבועית)
          </p>
        )}
      </section>

      <RunLog refreshKey={logKey} />

      {info && (
        <details className="card mb-2" style={{ padding: "0.8rem 1.25rem" }}>
          <summary style={{ cursor: "pointer", fontWeight: 600 }}>מה כל בדיקה עושה</summary>
          <table style={{ width: "100%", fontSize: "0.82rem", borderCollapse: "collapse", marginTop: "0.5rem" }}>
            <tbody>
              {info.checks.map((c) => (
                <tr key={c.key} style={{ borderTop: "1px solid var(--border)" }}>
                  <td style={{ ...cell, fontWeight: 600 }}>{c.title}
                    <div className="text-muted" dir="ltr" style={{ fontSize: "0.7rem", textAlign: "end" }}>{c.key}</div>
                  </td>
                  <td style={{ ...cell, whiteSpace: "nowrap" }}>{c.tiers.includes("daily") ? "יומית + שבועית" : "שבועית"}</td>
                  <td style={cell}>{c.description}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </details>
      )}
    </div>
  );
}
