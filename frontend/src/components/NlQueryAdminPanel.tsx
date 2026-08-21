import { useCallback, useEffect, useState } from "react";
import { adminNlQuery } from "../api/client";
import type { NlAdminConfig, NlAdminLogRow, NlAdminStats, NlSuggestLogRow } from "../api/client";

/**
 * Admin panel for the free-text query feature: what people asked, which stage
 * answered, what it cost, and the switches to turn any of it off.
 *
 * The panel is built around one number — the share of questions that reached a
 * PAID stage. Everything else on the page exists to explain that number or to
 * change it. Per-stage counts show where the questions go; the log shows which
 * questions in particular went to the expensive tier; the switches let an admin
 * cut a tier off without waiting for a deploy.
 */

const STAGE_LABEL: Record<string, string> = {
  cache: "מטמון", template: "תבנית", deepseek: "DeepSeek",
  anthropic: "Claude", refused: "סירוב", invalid: "פלט לא תקין", error: "שגיאה",
};
// Free stages green, cheap amber, expensive red — the colour IS the cost.
const STAGE_TONE: Record<string, [string, string]> = {
  cache: ["var(--tint-good-bg)", "var(--success)"], template: ["var(--tint-good-bg)", "var(--success)"],
  deepseek: ["var(--tint-note-bg)", "var(--tint-note-fg)"], anthropic: ["var(--tint-bad-bg)", "var(--danger)"],
  refused: ["#f1f5f9", "#475569"], invalid: ["#ffedd5", "#c2410c"],
  error: ["var(--tint-bad-bg)", "var(--danger)"],
};

const chip = (stage: string) => {
  const [bg, fg] = STAGE_TONE[stage] || ["#f1f5f9", "#475569"];
  return { display: "inline-block", padding: "0.05rem 0.45rem", borderRadius: 4,
           background: bg, color: fg, fontSize: "0.72rem", whiteSpace: "nowrap" } as const;
};
const box: React.CSSProperties = {
  fontSize: "0.82rem", padding: "0.25rem 0.45rem", borderRadius: 4,
  border: "1px solid var(--border)", background: "var(--bg)",
};
const fmt = (n: number | null | undefined) =>
  n == null ? "—" : Number(n).toLocaleString("he-IL");

export default function NlQueryAdminPanel() {
  const [cfg, setCfg] = useState<NlAdminConfig | null>(null);
  const [stats, setStats] = useState<NlAdminStats | null>(null);
  const [rows, setRows] = useState<NlAdminLogRow[]>([]);
  const [total, setTotal] = useState(0);
  const [stage, setStage] = useState("");
  const [days, setDays] = useState(7);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState("");
  const [openSql, setOpenSql] = useState<number | null>(null);
  const [sug, setSug] = useState<{
    rows: NlSuggestLogRow[];
    totals: { searches: number; picked: number; picked_at_1: number; empty: number };
    synonym_candidates: Array<{ query: string; picked_table: string; n: number }>;
  } | null>(null);
  const [adopting, setAdopting] = useState("");

  const load = useCallback(async () => {
    setErr("");
    try {
      const [c, s, l] = await Promise.all([
        adminNlQuery.config(),
        adminNlQuery.stats(days),
        adminNlQuery.log({ limit: 100, stage: stage || undefined }),
      ]);
      setCfg(c); setStats(s); setRows(l.rows); setTotal(l.total);
      // The explorer's log is a different question from the answer box's
      // ("was the right dataset offered?" vs "what did the model cost?"), so a
      // failure here must not blank the cost panel beside it.
      adminNlQuery.suggestLog(100).then(setSug).catch(() => setSug(null));
    } catch (e) {
      setErr(e instanceof Error ? e.message : "שגיאה בטעינה");
    }
  }, [days, stage]);

  useEffect(() => { load(); }, [load]);

  async function save(patch: Record<string, unknown>) {
    setBusy(true);
    try { setCfg(await adminNlQuery.setConfig(patch)); }
    catch (e) { setErr(e instanceof Error ? e.message : "שגיאה בשמירה"); }
    finally { setBusy(false); }
  }


  return (
    <div dir="rtl">
      {err && <div style={{ color: "var(--danger)", marginBottom: "0.6rem" }}>{err}</div>}

      {/* ── switches ───────────────────────────────────────────────── */}
      <div style={{ border: "1px solid var(--border)", borderRadius: 6, padding: "0.7rem 0.85rem", marginBottom: "0.9rem" }}>
        <div style={{ fontWeight: 600, marginBottom: "0.5rem" }}>מתגי הפעלה</div>
        {cfg && (
          <>
            <div className="flex" style={{ gap: "1.1rem", flexWrap: "wrap", alignItems: "center" }}>
              <label style={{ fontSize: "0.85rem" }}>
                <input type="checkbox" checked={cfg.config.enabled} disabled={busy}
                  onChange={(e) => save({ enabled: e.target.checked })} /> התכונה פעילה
              </label>
              <label style={{ fontSize: "0.85rem", opacity: cfg.keys.deepseek ? 1 : 0.45 }}
                     title={cfg.keys.deepseek ? "" : "אין מפתח DeepSeek בשרת — השלב כבוי בכל מקרה"}>
                <input type="checkbox" checked={cfg.config.allow_deepseek}
                  disabled={busy || !cfg.keys.deepseek}
                  onChange={(e) => save({ allow_deepseek: e.target.checked })} /> DeepSeek (זול)
              </label>
              <label style={{ fontSize: "0.85rem", opacity: cfg.keys.anthropic ? 1 : 0.45 }}
                     title={cfg.keys.anthropic ? "" : "אין מפתח Anthropic בשרת — השלב כבוי בכל מקרה"}>
                <input type="checkbox" checked={cfg.config.allow_anthropic}
                  disabled={busy || !cfg.keys.anthropic}
                  onChange={(e) => save({ allow_anthropic: e.target.checked })} /> Claude (יקר)
              </label>
              <label style={{ fontSize: "0.85rem" }}
                     title="כשמודל זול משיב 'אין לי תשובה' — לשאול מודל יקר יותר. זה המתג היקר: מחוץ-לתחום הוא תרחיש שכיח.">
                <input type="checkbox" checked={cfg.config.escalate_on_unanswerable} disabled={busy}
                  onChange={(e) => save({ escalate_on_unanswerable: e.target.checked })} />
                {" "}הסלמה גם על "אין לי תשובה"
              </label>
            </div>

            <div className="flex" style={{ gap: "0.9rem", flexWrap: "wrap", alignItems: "center", marginTop: "0.6rem", fontSize: "0.82rem" }}>
              <span className="text-muted">תקרות יומיות (ריק = ברירת המחדל של השרת):</span>
              <label>
                קריאות:{" "}
                <input type="number" min={0} style={{ ...box, width: 100 }}
                  defaultValue={cfg.config.daily_call_budget ?? ""}
                  placeholder={String(cfg.defaults.daily_call_budget)}
                  onBlur={(e) => {
                    const v = e.target.value.trim();
                    save(v === "" ? { clear_call_budget: true } : { daily_call_budget: Number(v) });
                  }} />
              </label>
              <label>
                טוקני פלט:{" "}
                <input type="number" min={0} style={{ ...box, width: 140 }}
                  defaultValue={cfg.config.daily_output_token_budget ?? ""}
                  placeholder={String(cfg.defaults.daily_output_token_budget)}
                  onBlur={(e) => {
                    const v = e.target.value.trim();
                    save(v === "" ? { clear_token_budget: true } : { daily_output_token_budget: Number(v) });
                  }} />
              </label>
            </div>

            <div className="text-muted" style={{ fontSize: "0.75rem", marginTop: "0.5rem" }}>
              סולם פעיל כרגע:{" "}
              {cfg.active_tiers.length
                ? cfg.active_tiers.map((t, i) => (
                    <span key={t.provider}>{i > 0 && " ← "}<b>{t.model}</b></span>
                  ))
                : <span style={{ color: "var(--danger)" }}>אין — רק תבניות ומטמון עובדים</span>}
              {" · "}שינוי נכנס לתוקף מיידית.
            </div>
          </>
        )}
      </div>

      {/* ── cost ───────────────────────────────────────────────────── */}
      <div style={{ border: "1px solid var(--border)", borderRadius: 6, padding: "0.7rem 0.85rem", marginBottom: "0.9rem" }}>
        <div className="flex" style={{ alignItems: "center", gap: 10, marginBottom: "0.5rem" }}>
          <span style={{ fontWeight: 600 }}>עלות ושימוש</span>
          <select value={days} onChange={(e) => setDays(Number(e.target.value))} aria-label="טווח ימים לחישוב העלות" style={box}>
            {[1, 7, 30, 90].map((d) => <option key={d} value={d}>{d} ימים</option>)}
          </select>
          <button onClick={load} style={{ ...box, cursor: "pointer" }}>רענן</button>
        </div>
        {stats && (
          <>
            <div className="flex" style={{ gap: "1.5rem", flexWrap: "wrap", marginBottom: "0.6rem" }}>
              <div>
                <div className="text-muted" style={{ fontSize: "0.75rem" }}>שאלות</div>
                <div style={{ fontSize: "1.3rem", fontWeight: 600 }}>{fmt(stats.total)}</div>
              </div>
              <div>
                <div className="text-muted" style={{ fontSize: "0.75rem" }}>מהן ללא עלות</div>
                <div style={{ fontSize: "1.3rem", fontWeight: 600, color: "var(--success)" }}>
                  {stats.free_share == null ? "—" : `${Math.round(stats.free_share * 100)}%`}
                </div>
              </div>
              <div>
                <div className="text-muted" style={{ fontSize: "0.75rem" }}>נענו</div>
                <div style={{ fontSize: "1.3rem", fontWeight: 600 }}>
                  {stats.answered_share == null ? "—" : `${Math.round(stats.answered_share * 100)}%`}
                </div>
              </div>
              <div>
                <div className="text-muted" style={{ fontSize: "0.75rem" }}>הגיעו למודל בתשלום</div>
                <div style={{ fontSize: "1.3rem", fontWeight: 600 }}>{fmt(stats.paid)}</div>
              </div>
              <div title="קריאות ששילמנו עליהן ולא יצאה מהן תשובה — סירוב או פלט לא תקין. זה המדד שאומר אם המודל הזול מספיק טוב.">
                <div className="text-muted" style={{ fontSize: "0.75rem" }}>מהן בזבוז</div>
                <div style={{ fontSize: "1.3rem", fontWeight: 600,
                              color: (stats.wasted_share ?? 0) > 0.4 ? "var(--danger)" : "var(--tint-note-fg)" }}>
                  {stats.wasted_share == null ? "—" : `${Math.round(stats.wasted_share * 100)}%`}
                </div>
              </div>
              <div>
                <div className="text-muted" style={{ fontSize: "0.75rem" }}>מכסת היום (קריאות)</div>
                <div style={{ fontSize: "1.3rem", fontWeight: 600 }}>
                  {fmt(stats.budget_today.calls)} / {fmt(stats.budget_today.call_budget)}
                </div>
              </div>
              <div>
                <div className="text-muted" style={{ fontSize: "0.75rem" }}>טוקני פלט היום</div>
                <div style={{ fontSize: "1.3rem", fontWeight: 600 }}>
                  {fmt(stats.budget_today.output_tokens)} / {fmt(stats.budget_today.output_token_budget)}
                </div>
              </div>
            </div>
            <table style={{ width: "100%", fontSize: "0.8rem", borderCollapse: "collapse" }}>
              <thead>
                <tr style={{ textAlign: "start", color: "var(--text-muted)" }}>
                  <th scope="col" style={{ textAlign: "start", padding: "0.2rem 0" }}>שלב</th>
                  <th scope="col" style={{ textAlign: "start" }}>שאלות</th>
                  <th scope="col" style={{ textAlign: "start" }}>נענו</th>
                  <th scope="col" style={{ textAlign: "start" }}>הוסלמו</th>
                  <th scope="col" style={{ textAlign: "start" }}>טוקני קלט</th>
                  <th scope="col" style={{ textAlign: "start" }}>טוקני פלט</th>
                  <th scope="col" style={{ textAlign: "start" }}>חציון זמן</th>
                </tr>
              </thead>
              <tbody>
                {stats.by_stage.map((s) => (
                  <tr key={s.stage} style={{ borderTop: "1px solid var(--border)" }}>
                    <td style={{ padding: "0.25rem 0" }}><span style={chip(s.stage)}>{STAGE_LABEL[s.stage] || s.stage}</span></td>
                    <td>{fmt(s.n)}</td><td>{fmt(s.answered)}</td><td>{fmt(s.escalated)}</td>
                    <td>{fmt(s.input_tokens)}</td><td>{fmt(s.output_tokens)}</td>
                    <td>{s.median_ms == null ? "—" : `${fmt(s.median_ms)} ms`}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </>
        )}
      </div>

      {/* ── explorer: searches, picks, synonym candidates ──────────── */}
      {sug && (
        <div style={{ border: "1px solid var(--border)", borderRadius: 6, padding: "0.7rem 0.85rem", marginBottom: "0.9rem" }}>
          <div style={{ fontWeight: 600, marginBottom: "0.5rem" }}>
            מצא נתונים — חיפושים ובחירות
          </div>
          <div className="flex" style={{ gap: "1.5rem", flexWrap: "wrap", marginBottom: "0.6rem" }}>
            <div>
              <div className="text-muted" style={{ fontSize: "0.75rem" }}>חיפושים</div>
              <div style={{ fontSize: "1.3rem", fontWeight: 600 }}>{fmt(sug.totals.searches)}</div>
            </div>
            <div title="באיזה חלק מהחיפושים המשתמש בחר מאגר מהרשימה. זהו recall בשטח — המדד שהמסך תלוי בו, על שאלות אמיתיות ולא על סט שנכתב ביד.">
              <div className="text-muted" style={{ fontSize: "0.75rem" }}>נבחר מאגר</div>
              <div style={{ fontSize: "1.3rem", fontWeight: 600, color: "var(--success)" }}>
                {sug.totals.searches
                  ? `${Math.round((sug.totals.picked / sug.totals.searches) * 100)}%` : "—"}
              </div>
            </div>
            <div title="מתוך הבחירות — כמה היו בהצעה הראשונה.">
              <div className="text-muted" style={{ fontSize: "0.75rem" }}>מהן במקום 1</div>
              <div style={{ fontSize: "1.3rem", fontWeight: 600 }}>
                {sug.totals.picked
                  ? `${Math.round((sug.totals.picked_at_1 / sug.totals.picked) * 100)}%` : "—"}
              </div>
            </div>
            <div title="חיפושים שלא החזירו כלום — הפער האמיתי בכיסוי הקטלוג.">
              <div className="text-muted" style={{ fontSize: "0.75rem" }}>ללא הצעות</div>
              <div style={{ fontSize: "1.3rem", fontWeight: 600,
                            color: sug.totals.empty ? "var(--tint-note-fg)" : undefined }}>
                {fmt(sug.totals.empty)}
              </div>
            </div>
          </div>

          {!!sug.synonym_candidates.length && (
            <div style={{ marginBottom: "0.6rem" }}>
              <div style={{ fontSize: "0.85rem", fontWeight: 600, marginBottom: 4 }}>
                מועמדים למילים נרדפות
              </div>
              <div className="text-muted" style={{ fontSize: "0.75rem", marginBottom: 6 }}>
                המשתמש חיפש מילה, המערכת רק ניחשה לפי דמיון כתיב, והוא בחר בכל זאת.
                אימוץ הופך את הצמד להתאמה מדויקת — זה התיקון השיטתי למורפולוגיה העברית.
              </div>
              {sug.synonym_candidates.map((c) => (
                <div key={`${c.query}|${c.picked_table}`} className="flex"
                     style={{ gap: 8, alignItems: "center", flexWrap: "wrap", padding: "0.25rem 0",
                              borderTop: "1px solid var(--border)", fontSize: "0.82rem" }}>
                  <b>{c.query}</b>
                  <span className="text-muted">← {c.picked_table}</span>
                  <span style={chip("cache")}>{c.n}</span>
                  <button disabled={!!adopting}
                    onClick={async () => {
                      setAdopting(c.query);
                      try { await adminNlQuery.adoptSynonym(c.query, c.picked_table); await load(); }
                      catch (e) { setErr(e instanceof Error ? e.message : "שגיאה באימוץ"); }
                      finally { setAdopting(""); }
                    }}
                    style={{ ...box, cursor: "pointer", fontWeight: 600 }}>
                    {adopting === c.query ? "מאמץ…" : "אמץ"}
                  </button>
                </div>
              ))}
            </div>
          )}

          <details>
            <summary style={{ cursor: "pointer", fontSize: "0.85rem", fontWeight: 600 }}>
              חיפושים אחרונים ({sug.rows.length})
            </summary>
            <table style={{ width: "100%", fontSize: "0.78rem", borderCollapse: "collapse", marginTop: 6 }}>
              <thead>
                <tr style={{ color: "var(--text-muted)" }}>
                  <th scope="col" style={{ textAlign: "start", padding: "0.2rem 0" }}>מתי</th>
                  <th scope="col" style={{ textAlign: "start" }}>חיפוש</th>
                  <th scope="col" style={{ textAlign: "start" }}>הצעות</th>
                  <th scope="col" style={{ textAlign: "start" }}>נבחר</th>
                  <th scope="col" style={{ textAlign: "start" }}>מיקום</th>
                </tr>
              </thead>
              <tbody>
                {sug.rows.map((r) => (
                  <tr key={r.id} style={{ borderTop: "1px solid var(--border)" }}>
                    <td className="text-muted" style={{ padding: "0.22rem 0", whiteSpace: "nowrap" }}>
                      {new Date(r.created_at).toLocaleString("he-IL", { dateStyle: "short", timeStyle: "short" })}
                    </td>
                    <td>{r.query}</td>
                    <td className="text-muted">
                      {r.suggestions_count}{r.approximate_count ? ` (${r.approximate_count} משוער)` : ""}
                    </td>
                    <td className="text-muted">{r.picked_table || "—"}</td>
                    <td className="text-muted">{r.picked_rank ?? "—"}</td>
                  </tr>
                ))}
                {!sug.rows.length && (
                  <tr><td colSpan={5} className="text-muted" style={{ padding: "0.6rem 0" }}>
                    אין חיפושים עדיין
                  </td></tr>
                )}
              </tbody>
            </table>
          </details>
        </div>
      )}

      {/* ── log ────────────────────────────────────────────────────── */}
      <div style={{ border: "1px solid var(--border)", borderRadius: 6, padding: "0.7rem 0.85rem" }}>
        <div className="flex" style={{ alignItems: "center", gap: 10, marginBottom: "0.5rem", flexWrap: "wrap" }}>
          <span style={{ fontWeight: 600 }}>לוג שאלות</span>
          <select value={stage} onChange={(e) => setStage(e.target.value)} aria-label="סינון הלוג לפי שלב" style={box}>
            <option value="">כל השלבים</option>
            {Object.entries(STAGE_LABEL).map(([k, v]) => <option key={k} value={k}>{v}</option>)}
          </select>
          <span className="text-muted" style={{ fontSize: "0.78rem" }}>{fmt(total)} רשומות</span>
          <span className="text-muted" style={{ fontSize: "0.72rem" }}>
            הלוג מכיל טקסט שכתבו משתמשים — מוצג לאדמין בלבד ואינו חשוף בקטלוג הציבורי.
          </span>
        </div>
        <table style={{ width: "100%", fontSize: "0.8rem", borderCollapse: "collapse" }}>
          <thead>
            <tr style={{ color: "var(--text-muted)" }}>
              <th scope="col" style={{ textAlign: "start", padding: "0.2rem 0" }}>מתי</th>
              <th scope="col" style={{ textAlign: "start" }}>שאלה</th>
              <th scope="col" style={{ textAlign: "start" }}>שלב</th>
              <th scope="col" style={{ textAlign: "start" }}>ניסיונות</th>
              <th scope="col" style={{ textAlign: "start" }}>תוצאה</th>
              <th scope="col" style={{ textAlign: "start" }}>טוקנים</th>
              <th scope="col" style={{ textAlign: "start" }}>זמן</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => (
              <tr key={r.id} style={{ borderTop: "1px solid var(--border)", verticalAlign: "top" }}>
                <td style={{ padding: "0.3rem 0", whiteSpace: "nowrap" }} className="text-muted">
                  {new Date(r.created_at).toLocaleString("he-IL", { dateStyle: "short", timeStyle: "short" })}
                </td>
                <td style={{ maxWidth: 260 }}>
                  {r.question}
                  {r.sql && (
                    <>
                      {" "}
                      <button onClick={() => setOpenSql(openSql === r.id ? null : r.id)}
                        style={{ background: "none", border: "none", padding: 0, cursor: "pointer", color: "var(--primary)", fontSize: "0.72rem", textDecoration: "underline" }}>
                        SQL
                      </button>
                      {openSql === r.id && (
                        <pre style={{ margin: "0.3rem 0 0", padding: "0.4rem", background: "var(--surface-2)", borderRadius: 4, fontSize: "0.7rem", whiteSpace: "pre-wrap", direction: "ltr", textAlign: "start" }}>{r.sql}</pre>
                      )}
                    </>
                  )}
                </td>
                <td><span style={chip(r.stage)}>{STAGE_LABEL[r.stage] || r.stage}</span></td>
                <td className="text-muted" style={{ fontSize: "0.72rem", whiteSpace: "nowrap" }}>
                  {r.attempts || "—"}{r.escalated ? " ↑" : ""}
                </td>
                <td style={{ maxWidth: 220 }}>
                  {r.answered
                    ? <span className="text-muted" style={{ fontSize: "0.75rem" }}>{r.entity}</span>
                    : <span style={{ fontSize: "0.75rem", color: "var(--warning)" }}>{r.reason}</span>}
                </td>
                <td className="text-muted" style={{ fontSize: "0.72rem", whiteSpace: "nowrap" }}>
                  {r.input_tokens + r.output_tokens ? `${fmt(r.input_tokens)}/${fmt(r.output_tokens)}` : "—"}
                </td>
                <td className="text-muted" style={{ fontSize: "0.72rem" }}>
                  {r.duration_ms == null ? "—" : `${fmt(r.duration_ms)} ms`}
                </td>
              </tr>
            ))}
            {!rows.length && (
              <tr><td colSpan={7} className="text-muted" style={{ padding: "0.8rem 0" }}>אין רשומות</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
