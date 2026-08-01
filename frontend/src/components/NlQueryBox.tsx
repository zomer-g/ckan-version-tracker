import { useEffect, useRef, useState } from "react";
import { nlQuery } from "../api/client";
import type { NlQueryResponse, NlExample } from "../api/client";

/**
 * "שאלה בשפה חופשית" — ask in Hebrew, get a query.
 *
 * Sits above the SQL editor on /data and is the entry point for people who
 * can't write SQL. It does NOT hide the SQL: whatever comes back is loaded into
 * the editor and shown, because on a transparency site an answer nobody can
 * audit is worth less than no answer at all.
 *
 * Three things this UI has to communicate honestly, in order of importance:
 *
 *  1. WHICH TIER ANSWERED. A deterministic template match and a model-derived
 *     query carry genuinely different confidence, so they are badged
 *     differently. Hiding that would be the interface telling a comfortable lie.
 *  2. WHAT WAS ACTUALLY RUN. The one-line Hebrew explanation is derived
 *     server-side from the validated query — not written by the model — so it
 *     cannot describe a filter that was never applied.
 *  3. WHEN IT DOESN'T KNOW. The expected failure of a semantic layer is "out of
 *     scope", not a wrong number. That path gets a real panel with the nearby
 *     datasets, not a red error toast.
 */

const badge: React.CSSProperties = {
  display: "inline-block", padding: "0.05rem 0.45rem", borderRadius: 4,
  fontSize: "0.7rem", fontWeight: 600, whiteSpace: "nowrap",
};

export default function NlQueryBox({ onUseSql }: {
  onUseSql: (sql: string, run?: boolean) => void;
}) {
  const [q, setQ] = useState("");
  const [busy, setBusy] = useState(false);
  const [res, setRes] = useState<NlQueryResponse | null>(null);
  const [err, setErr] = useState("");
  const [examples, setExamples] = useState<NlExample[]>([]);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    // Free (no model call) and grounded in the live catalog — every suggestion
    // names a real table and a real column. A free-text box with no examples
    // reads as a search box and gets search queries.
    nlQuery.examples().then((r) => setExamples(r.examples)).catch(() => setExamples([]));
  }, []);

  async function ask(question?: string) {
    const text = (question ?? q).trim();
    if (!text || busy) return;
    setQ(text);
    setBusy(true);
    setErr("");
    setRes(null);
    try {
      // run=false: compile only. The console runs it a beat later through the
      // normal path, which reuses the existing result table, chart panel, CSV
      // export and URL state — and avoids executing the same query twice.
      const r = await nlQuery.query(text, false);
      setRes(r);
      if (r.answered && r.sql) onUseSql(r.sql, true);
    } catch (e) {
      setErr(e instanceof Error ? e.message : "שגיאה בשירות");
    } finally {
      setBusy(false);
    }
  }

  const src = res?.answered ? res.source : null;
  const srcBadge = src === "template"
    ? { text: "התאמה מדויקת", bg: "#dcfce7", fg: "#15803d",
        title: "השאלה נענתה על ידי התאמת תבנית — בלי מודל שפה, ובלי אפשרות להזיה." }
    : { text: "נוצר במודל שפה", bg: "#fef9c3", fg: "#a16207",
        title: `מודל שפה (${res?.model || src}) בחר את המאגר והשדות מתוך מודל מוצהר. `
             + "השאילתה אומתה מול המודל לפני ההרצה — אבל כדאי לוודא שהיא אכן עונה "
             + "על מה ששאלתם." };

  return (
    <div dir="rtl" style={{ border: "1px solid var(--border, #e5e7eb)", borderRadius: 6, padding: "0.7rem 0.8rem", marginBottom: "0.7rem", background: "var(--bg-muted, #f8fafc)" }}>
      <div className="flex" style={{ alignItems: "center", gap: 8, marginBottom: "0.45rem", flexWrap: "wrap" }}>
        <span style={{ fontWeight: 600, fontSize: "0.9rem" }}>💬 שאלו בשפה חופשית</span>
        {/* Stated up front, not buried in a tooltip. The feature can pick the
            wrong dataset or the wrong filter and still look confident, and the
            reader has to know that before they trust a number off it. */}
        <span style={{ ...badge, background: "#fee2e2", color: "#b91c1c" }}>ניסיוני</span>
        <span className="text-muted" style={{ fontSize: "0.72rem" }}>
          התכונה בבדיקה — השאילתה שנוצרת מוצגת תמיד, וכדאי לוודא אותה לפני שמסתמכים על התוצאה.
        </span>
      </div>

      <div className="flex" style={{ gap: 6, flexWrap: "wrap" }}>
        <input
          ref={inputRef}
          value={q}
          onChange={(e) => setQ(e.target.value)}
          onKeyDown={(e) => { if (e.key === "Enter") ask(); }}
          maxLength={400}
          placeholder="למשל: כמה רישיונות עסק לפי יישוב"
          style={{ flex: "1 1 320px", minWidth: 240, padding: "0.4rem 0.6rem", fontSize: "0.9rem", borderRadius: 4, border: "1px solid var(--border, #d1d5db)" }}
        />
        <button
          type="button" onClick={() => ask()} disabled={busy || !q.trim()}
          style={{ padding: "0.4rem 1.1rem", borderRadius: 4, border: "none", fontWeight: 600,
                   background: "var(--primary, #0f766e)", color: "white",
                   cursor: busy ? "wait" : "pointer", opacity: busy || !q.trim() ? 0.7 : 1 }}
        >
          {busy ? "מנסח…" : "שאל"}
        </button>
      </div>

      {!res && !busy && examples.length > 0 && (
        <div style={{ marginTop: "0.45rem", fontSize: "0.75rem" }}>
          <span className="text-muted">נסו: </span>
          {examples.slice(0, 4).map((ex, i) => (
            <span key={ex.question}>
              {i > 0 && " · "}
              <button type="button" onClick={() => ask(ex.question)}
                style={{ background: "none", border: "none", padding: 0, cursor: "pointer", color: "var(--primary, #0f766e)", textDecoration: "underline", fontSize: "0.75rem" }}>
                {ex.question}
              </button>
            </span>
          ))}
        </div>
      )}

      {err && (
        <div style={{ marginTop: "0.5rem", fontSize: "0.8rem", color: "#b91c1c" }}>{err}</div>
      )}

      {res && !res.answered && (
        // The semantic layer's designed failure mode. A refusal that names the
        // nearby datasets is navigation; a bare "לא נמצא" is a dead end.
        <div style={{ marginTop: "0.55rem", padding: "0.5rem 0.65rem", borderRadius: 4, background: "#fff7ed", border: "1px solid #fed7aa", fontSize: "0.82rem" }}>
          <b>אין לי תשובה לשאלה הזו.</b>
          <div style={{ marginTop: "0.25rem", lineHeight: 1.6 }}>{res.reason}</div>
          {!!res.candidates?.length && (
            <div style={{ marginTop: "0.4rem", fontSize: "0.78rem" }}>
              <span className="text-muted">מאגרים קרובים בנושא: </span>
              {res.candidates.map((c, i) => (
                <span key={c.table}>
                  {i > 0 && " · "}
                  {c.page_url
                    ? <a href={c.page_url} style={{ color: "var(--primary)" }}>{c.title}</a>
                    : <span>{c.title}</span>}
                </span>
              ))}
            </div>
          )}
        </div>
      )}

      {res?.answered && (
        <div style={{ marginTop: "0.55rem", fontSize: "0.82rem" }}>
          <div className="flex" style={{ gap: 6, alignItems: "center", flexWrap: "wrap" }}>
            <span style={{ ...badge, background: srcBadge.bg, color: srcBadge.fg }} title={srcBadge.title}>
              {srcBadge.text}
            </span>
            {res.cached && (
              <span style={{ ...badge, background: "#e0f2fe", color: "#0369a1" }}
                    title="נענה מתוך מטמון של שאלה זהה שנשאלה קודם — לא נעשתה קריאה למודל.">
                מהמטמון
              </span>
            )}
            {res.escalated && (
              <span style={{ ...badge, background: "#ede9fe", color: "#6d28d9" }}
                    title="המודל הזול לא הצליח לבנות שאילתה תקינה, והשאלה הועברה למודל חזק יותר.">
                הוסלם
              </span>
            )}
            <span style={{ lineHeight: 1.6 }}>{res.explanation}</span>
          </div>

          <div className="text-muted" style={{ fontSize: "0.72rem", marginTop: "0.4rem" }}>
            השאילתה נטענה לעורך והורצה. כדאי לקרוא אותה ולוודא שהיא אכן עונה על מה ששאלתם —
            בעיקר את הפילטרים ואת המאגר שנבחר.
          </div>
        </div>
      )}
    </div>
  );
}
