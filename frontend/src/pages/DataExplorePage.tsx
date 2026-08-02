import { useCallback, useEffect, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { dataCatalog, nlExplore } from "../api/client";
import type {
  CatalogTable, CatalogTableDetail, NlJoinable, NlSuggestion, TableProfile,
} from "../api/client";
import DataTabs from "../components/DataTabs";
import FilterBuilder from "../components/FilterBuilder";

/**
 * "מצא נתונים" — describe what you're looking for, in four steps.
 *
 * WHY THIS REPLACED THE FREE-TEXT ANSWER BOX. The autopilot version took a
 * question and produced a number. Measured in production: it found the right
 * dataset 87% of the time, but correctly refused only 56% of the time when no
 * dataset fit — so nearly half of out-of-scope questions came back as a
 * confident answer from an unrelated dataset, with valid SQL and a Hebrew
 * explanation and nothing to suggest anything was wrong. On a transparency site
 * that is the one unacceptable outcome, and it was shelved.
 *
 * Same retrieval, different job. As a SHORTLIST the right dataset is in the top
 * five 100% of the time on the gold set, because the requirement drops from "be
 * right" to "include the right one". The person disambiguates — which is the
 * step machines were failing and humans do instantly, given the reason each
 * candidate matched.
 *
 * No language model runs anywhere in this page. Retrieval is lexical scoring
 * over a cached model; field summaries come from the stored profiler output;
 * filters offer the column's real values; the join list is one deterministic
 * rule (both sides carry a locality). Nothing here can hallucinate, and nothing
 * here costs money.
 */

const card: React.CSSProperties = {
  border: "1px solid var(--border, #e5e7eb)", borderRadius: 8,
  padding: "0.85rem 1rem", marginBottom: "0.9rem", background: "var(--bg, #fff)",
};
const chip = (bg: string, fg: string): React.CSSProperties => ({
  display: "inline-block", padding: "0.05rem 0.45rem", borderRadius: 4,
  background: bg, color: fg, fontSize: "0.72rem", whiteSpace: "nowrap",
});
const fmt = (n: number | null | undefined) =>
  n == null ? "—" : Number(n).toLocaleString("he-IL");

function StepHead({ n, title, done, children }: {
  n: number; title: string; done?: boolean; children?: React.ReactNode;
}) {
  return (
    <div className="flex" style={{ alignItems: "center", gap: 9, marginBottom: "0.6rem", flexWrap: "wrap" }}>
      <span style={{
        width: 24, height: 24, borderRadius: "50%", display: "inline-flex",
        alignItems: "center", justifyContent: "center", fontSize: "0.8rem", fontWeight: 700,
        background: done ? "var(--primary, #0f766e)" : "var(--bg-muted, #e2e8f0)",
        color: done ? "#fff" : "var(--text-muted, #475569)",
      }}>{done ? "✓" : n}</span>
      <span style={{ fontWeight: 600 }}>{title}</span>
      {children}
    </div>
  );
}

export default function DataExplorePage() {
  const [params, setParams] = useSearchParams();

  // ── step 1: describe ──
  const [text, setText] = useState(params.get("q") || "");
  const [suggestions, setSuggestions] = useState<NlSuggestion[] | null>(null);
  const [suggestId, setSuggestId] = useState<number | null>(null);
  const [searching, setSearching] = useState(false);
  const [err, setErr] = useState("");

  // ── step 2: the chosen dataset ──
  const [picked, setPicked] = useState<string>(params.get("t") || "");
  const [detail, setDetail] = useState<CatalogTableDetail | null>(null);
  const [profile, setProfile] = useState<TableProfile | null>(null);
  const [loadingDetail, setLoadingDetail] = useState(false);

  // ── step 4: cross with another dataset ──
  const [joinable, setJoinable] = useState<NlJoinable[] | null>(null);
  const [joinReason, setJoinReason] = useState("");
  const [joinFilter, setJoinFilter] = useState("");
  const [showJoin, setShowJoin] = useState(false);
  const [crossBusy, setCrossBusy] = useState("");
  const [crossErr, setCrossErr] = useState("");

  const search = useCallback(async (q: string) => {
    const s = q.trim();
    if (!s) return;
    setSearching(true); setErr(""); setSuggestions(null);
    try {
      const r = await nlExplore.suggest(s, 8);
      setSuggestions(r.suggestions);
      setSuggestId(r.suggest_id);
    } catch (e) {
      setErr(e instanceof Error ? e.message : "שגיאה בחיפוש");
    } finally { setSearching(false); }
  }, []);

  // Run on load when the URL carries a query, so a shared link reproduces the
  // whole flow rather than an empty box.
  useEffect(() => {
    const q = params.get("q");
    if (q && suggestions === null && !searching) search(q);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    if (!picked) { setDetail(null); setProfile(null); return; }
    let alive = true;
    setLoadingDetail(true);
    Promise.allSettled([dataCatalog.tableDetail(picked), dataCatalog.tableProfile(picked)])
      .then(([d, p]) => {
        if (!alive) return;
        setDetail(d.status === "fulfilled" ? d.value : null);
        setProfile(p.status === "fulfilled" ? p.value : null);
      })
      .finally(() => { if (alive) setLoadingDetail(false); });
    return () => { alive = false; };
  }, [picked]);

  useEffect(() => {
    if (!showJoin || !picked) return;
    let alive = true;
    nlExplore.joinable(picked, joinFilter)
      .then((r) => { if (alive) { setJoinable(r.joinable); setJoinReason(r.reason || ""); } })
      .catch(() => { if (alive) { setJoinable([]); setJoinReason("שגיאה בטעינת הרשימה"); } });
    return () => { alive = false; };
  }, [showJoin, picked, joinFilter]);

  function choose(table: string) {
    // Report the pick before anything else: this row is the wild-recall metric
    // and the synonym-candidate stream. Fire-and-forget — a logging failure
    // must never interrupt the user's flow.
    if (suggestId != null && suggestions) {
      const idx = suggestions.findIndex((s) => s.table === table);
      if (idx >= 0) {
        void nlExplore.picked(suggestId, table, idx + 1,
                              !!suggestions[idx].approximate);
      }
    }
    setPicked(table);
    setShowJoin(false); setJoinable(null);
    setParams((prev) => {
      const n = new URLSearchParams(prev);
      n.set("t", table);
      if (text.trim()) n.set("q", text.trim());
      return n;
    }, { replace: true });
  }

  // The builders generate SQL; the console runs it. Handing the query to /data
  // rather than executing it here keeps ONE place where results, charts and CSV
  // export live, and keeps the generated SQL visible and editable.
  const runInConsole = (sql: string) => {
    window.location.href = `/data?sql=${encodeURIComponent(sql)}`;
  };

  const pickedTable: CatalogTable | null = detail;

  return (
    <div className="container mt-3" dir="rtl">
      <DataTabs active="explore" />
      <div className="page-header" style={{ marginBottom: "0.75rem" }}>
        <h1 style={{ margin: 0 }}>מצא נתונים</h1>
        <div className="text-sm text-muted" style={{ marginTop: "0.35rem", lineHeight: 1.7 }}>
          תארו במילים שלכם מה אתם מחפשים. נציע מאגרים מתאימים ונסביר למה כל אחד עלה,
          ואתם בוחרים — בלי לכתוב SQL ובלי שהמערכת מחליטה במקומכם.
        </div>
      </div>

      {/* ── 1 · describe ─────────────────────────────────────────── */}
      <div style={card}>
        <StepHead n={1} title="מה אתם מחפשים?" done={!!suggestions?.length} />
        <div className="flex" style={{ gap: 6, flexWrap: "wrap" }}>
          <input
            value={text}
            onChange={(e) => setText(e.target.value)}
            onKeyDown={(e) => { if (e.key === "Enter") search(text); }}
            maxLength={400}
            placeholder="למשל: מעיינות, עמותות רשומות, החלטות ממשלה, רישיונות עסק"
            style={{ flex: "1 1 340px", minWidth: 240, padding: "0.45rem 0.65rem",
                     fontSize: "0.95rem", borderRadius: 4,
                     border: "1px solid var(--border, #d1d5db)" }}
          />
          <button type="button" onClick={() => search(text)} disabled={searching || !text.trim()}
            style={{ padding: "0.45rem 1.3rem", borderRadius: 4, border: "none", fontWeight: 600,
                     background: "var(--primary, #0f766e)", color: "white",
                     cursor: searching ? "wait" : "pointer",
                     opacity: searching || !text.trim() ? 0.7 : 1 }}>
            {searching ? "מחפש…" : "חפש"}
          </button>
        </div>
        {err && <div style={{ color: "#b91c1c", marginTop: "0.5rem", fontSize: "0.85rem" }}>{err}</div>}
      </div>

      {/* ── 2 · pick ─────────────────────────────────────────────── */}
      {suggestions && (
        <div style={card}>
          <StepHead n={2} title="בחרו מאגר" done={!!picked}>
            <span className="text-muted" style={{ fontSize: "0.78rem" }}>
              {suggestions.length ? `${suggestions.length} הצעות` : "לא נמצאו הצעות"}
            </span>
          </StepHead>

          {!suggestions.length && (
            <div style={{ fontSize: "0.85rem", lineHeight: 1.8 }}>
              לא מצאנו מאגר שמתאים לניסוח הזה. אפשר לנסות מילה אחרת (למשל שם התחום במקום
              שם הפעולה), או <Link to="/data" style={{ color: "var(--primary)" }}>לעיין בקטלוג המלא</Link>.
            </div>
          )}

          {suggestions.map((s) => {
            const on = s.table === picked;
            return (
              <button key={s.table} type="button" onClick={() => choose(s.table)}
                style={{
                  display: "block", width: "100%", textAlign: "start", cursor: "pointer",
                  border: on ? "2px solid var(--primary, #0f766e)" : "1px solid var(--border, #e5e7eb)",
                  background: on ? "var(--bg-muted, #f0fdfa)" : "transparent",
                  borderRadius: 6, padding: "0.55rem 0.7rem", marginBottom: 6,
                }}>
                <div className="flex" style={{ gap: 8, alignItems: "center", flexWrap: "wrap" }}>
                  <span style={{ fontWeight: 600, fontSize: "0.92rem" }}>{s.title}</span>
                  {s.rows != null && (
                    <span style={chip("#f1f5f9", "#475569")}>{fmt(s.rows)} שורות</span>
                  )}
                  {s.can_join && <span style={chip("#e0f2fe", "#0369a1")}>ניתן להצלבה</span>}
                  {s.approximate && (
                    <span style={chip("#fef9c3", "#a16207")}
                          title="התאמה על בסיס דמיון בכתיב בלבד — ייתכן שאינה קשורה.">
                      התאמה משוערת
                    </span>
                  )}
                  {!s.official && (
                    <span style={chip("#fef3c7", "#92400e")}
                          title={`מקור מעובד או תרומת ציבור${s.organization ? ` (${s.organization})` : ""} — לא פרסום ממשלתי רשמי. מאגרים רשמיים מדורגים לפניו.`}>
                      לא רשמי
                    </span>
                  )}
                  {s.schema === "idx" && (
                    <span style={chip("#f1f5f9", "#64748b")}
                          title="שכבת אינדקס/מיפוי — כותרת שנגזרה אוטומטית, בלי תיאור מוגה.">
                      אינדקס
                    </span>
                  )}
                </div>
                {/* Why it matched. This is what turns a ranked list into a
                    decision the reader can make at a glance. */}
                <div className="text-muted" style={{ fontSize: "0.75rem", marginTop: 3 }}>
                  התאמה {s.why}
                </div>
                {s.summary && (
                  <div style={{ fontSize: "0.8rem", marginTop: 4, lineHeight: 1.6 }}>
                    {s.summary.slice(0, 220)}{s.summary.length > 220 ? "…" : ""}
                  </div>
                )}
              </button>
            );
          })}
        </div>
      )}

      {/* ── 3 · fields + filters ─────────────────────────────────── */}
      {picked && (
        <div style={card}>
          <StepHead n={3} title="מה להציג ומה לסנן" />
          {loadingDetail && <div className="text-muted">טוען…</div>}

          {pickedTable && (
            <>
              <div className="flex" style={{ gap: 10, flexWrap: "wrap", alignItems: "center", marginBottom: "0.5rem" }}>
                <b>{pickedTable.title}</b>
                <span className="text-muted" style={{ fontSize: "0.8rem" }}>
                  {pickedTable.columns.length} עמודות
                  {detail?.row_count != null && ` · ${fmt(detail.row_count)} שורות`}
                </span>
                {pickedTable.page_url && (
                  <Link to={pickedTable.page_url} style={{ fontSize: "0.8rem", color: "var(--primary)" }}>
                    עמוד המאגר →
                  </Link>
                )}
              </div>

              {profile?.summary_he && (
                <div style={{ fontSize: "0.85rem", lineHeight: 1.7, marginBottom: "0.6rem",
                              padding: "0.5rem 0.65rem", background: "var(--bg-muted, #f8fafc)",
                              borderRadius: 4 }}>
                  {profile.summary_he}
                </div>
              )}

              {/* Field overview from the stored profile: what each column holds,
                  how full it is, and its real range or top values. */}
              <details open style={{ marginBottom: "0.6rem" }}>
                <summary style={{ cursor: "pointer", fontSize: "0.85rem", fontWeight: 600 }}>
                  סקירת השדות
                </summary>
                <div style={{ maxHeight: 260, overflowY: "auto", marginTop: 6 }}>
                  <table style={{ width: "100%", fontSize: "0.78rem", borderCollapse: "collapse" }}>
                    <thead>
                      <tr style={{ color: "var(--text-muted, #64748b)", textAlign: "start" }}>
                        <th style={{ textAlign: "start", padding: "0.2rem 0" }}>עמודה</th>
                        <th style={{ textAlign: "start" }}>סוג</th>
                        <th style={{ textAlign: "start" }}>מלא</th>
                        <th style={{ textAlign: "start" }}>ערכים / טווח</th>
                      </tr>
                    </thead>
                    <tbody>
                      {pickedTable.columns.map((c) => {
                        const p = profile?.sql_profile?.columns?.[c.name];
                        const vals = (p?.top_values || []).slice(0, 4)
                          .map((v) => `${v.value} (${fmt(v.count)})`).join(" · ");
                        const range = p?.min != null ? `${p.min} … ${p.max}` : "";
                        return (
                          <tr key={c.name} style={{ borderTop: "1px solid var(--border, #f1f5f9)" }}>
                            <td style={{ padding: "0.22rem 0", fontWeight: 500 }}>{c.name}</td>
                            <td className="text-muted">{p?.detected_kind || c.type}</td>
                            <td className="text-muted">
                              {p?.fill_rate != null ? `${Math.round(p.fill_rate * 100)}%` : "—"}
                            </td>
                            <td className="text-muted" style={{ maxWidth: 340 }}>{vals || range || "—"}</td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              </details>

              <FilterBuilder key={picked} table={pickedTable} profile={profile}
                             onUseSql={(sql) => runInConsole(sql)} />
            </>
          )}
        </div>
      )}

      {/* ── 4 · cross with another dataset ───────────────────────── */}
      {picked && pickedTable && (
        <div style={card}>
          <StepHead n={4} title="להצליב עם מאגר נוסף?" />
          {!showJoin ? (
            <div className="flex" style={{ gap: 10, alignItems: "center", flexWrap: "wrap" }}>
              <button type="button" onClick={() => setShowJoin(true)}
                style={{ padding: "0.35rem 1rem", borderRadius: 4, fontWeight: 600, cursor: "pointer",
                         border: "1px solid var(--primary, #0f766e)", background: "transparent",
                         color: "var(--primary, #0f766e)" }}>
                הצג מאגרים להצלבה
              </button>
              <span className="text-muted" style={{ fontSize: "0.78rem" }}>
                ההצלבה נעשית לפי סמל היישוב הקנוני — כל צד מסוכם לפי יישוב לפני החיבור,
                כך שספירה לא מוכפלת.
              </span>
            </div>
          ) : (
            <>
              {joinReason && <div style={{ fontSize: "0.85rem" }}>{joinReason}</div>}
              {joinable && joinable.length > 0 && (
                <>
                  <input value={joinFilter} onChange={(e) => setJoinFilter(e.target.value)}
                    placeholder="סננו את הרשימה…"
                    style={{ padding: "0.3rem 0.5rem", fontSize: "0.85rem", borderRadius: 4,
                             border: "1px solid var(--border, #d1d5db)", marginBottom: 8,
                             minWidth: 240 }} />
                  <div style={{ maxHeight: 300, overflowY: "auto" }}>
                    {joinable.map((j) => (
                      <div key={j.table} className="flex"
                        style={{ gap: 8, alignItems: "center", flexWrap: "wrap",
                                 padding: "0.35rem 0", borderTop: "1px solid var(--border, #f1f5f9)" }}>
                        <span style={{ fontSize: "0.85rem", flex: "1 1 240px" }}>{j.title}</span>
                        <span style={chip("#f1f5f9", "#475569")}>{fmt(j.rows)} שורות</span>
                        {!j.official && (
                          <span style={chip("#fef3c7", "#92400e")} title="מקור מעובד או תרומת ציבור">לא רשמי</span>
                        )}
                        <span className="text-muted" style={{ fontSize: "0.72rem" }}>לפי {j.via}</span>
                        {/* THE button this step exists for. The first version
                            only navigated to the other dataset — dropping the
                            one already chosen — because the join compiler was
                            never invoked from the UI. */}
                        <button type="button" disabled={!!crossBusy}
                          onClick={async () => {
                            setCrossBusy(j.table); setCrossErr("");
                            try {
                              const r = await nlExplore.cross(picked, j.table);
                              if (r.ok && r.sql) runInConsole(r.sql);
                              else setCrossErr(r.reason || "לא ניתן להצליב את הזוג הזה");
                            } catch (e) {
                              setCrossErr(e instanceof Error ? e.message : "שגיאה בהצלבה");
                            } finally { setCrossBusy(""); }
                          }}
                          style={{ fontSize: "0.78rem", padding: "0.2rem 0.7rem", borderRadius: 4,
                                   border: "none", fontWeight: 600, cursor: "pointer",
                                   background: "var(--primary, #0f766e)", color: "#fff",
                                   opacity: crossBusy === j.table ? 0.6 : 1 }}>
                          {crossBusy === j.table ? "מרכיב…" : "הצלב ↔"}
                        </button>
                        <Link to={`/data/explore?t=${encodeURIComponent(j.table)}&q=${encodeURIComponent(text)}`}
                              style={{ fontSize: "0.75rem", color: "var(--primary)" }}
                              title="לעבור לחקור את המאגר הזה במקום הנוכחי">
                          פתח →
                        </Link>
                      </div>
                    ))}
                  </div>
                </>
              )}
              {crossErr && (
                <div style={{ color: "#b91c1c", fontSize: "0.8rem", margin: "0.4rem 0" }}>{crossErr}</div>
              )}
              {joinable && !joinable.length && !joinReason && (
                <div className="text-muted" style={{ fontSize: "0.85rem" }}>
                  אין מאגרים תואמים לסינון הזה.
                </div>
              )}
            </>
          )}
        </div>
      )}
    </div>
  );
}
