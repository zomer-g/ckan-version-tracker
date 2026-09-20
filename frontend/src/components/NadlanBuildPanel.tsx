/**
 * Admin control for the נדל"ן לעם crosswalk build.
 *
 * The build has been API-only since it shipped: eight stages reachable through
 * `POST /api/admin/nadlan/build?stages=…` and nowhere else, which means the two
 * opt-in stages — `source_indexes` and `pip` — are in practice unreachable
 * without hand-writing a request with an admin token. `source_indexes` is what
 * creates the indexes the עסקאות נדל"ן browse rides on, so "run it when you
 * need it" needs somewhere to run it from.
 *
 * Two things the UI states rather than assumes:
 *
 * 1. **Which stages the default run skips, and why.** `source_indexes` is a
 *    one-off over source tables; `pip` is the 384k point-in-polygon pass and the
 *    only stage with material Neon compute cost. Both are unticked by default
 *    here for the same reason the API omits them.
 * 2. **A build is a background task.** The POST returns immediately, so the
 *    panel polls the state rather than pretending the response means "done" —
 *    and it keeps polling while any stage is still `running`.
 */
import { useCallback, useEffect, useState } from "react";

import { admin, NadlanBuildState, NadlanStageRow } from "../api/client";

/** The stage list, in dependency order, with what each one actually does.
 *  Mirrors nadlan_index.STAGES; a stage the server does not know is refused
 *  with a 422 that names the valid ones, so drift shows up loudly. */
const STAGES: { id: string; label: string; note: string; optIn?: boolean }[] = [
  { id: "source_indexes", label: "אינדקסים על טבלאות המקור",
    note: "חד-פעמי לכל טבלה. כאן נוצרים גם שלושת האינדקסים של מאגר עסקאות הנדל\"ן — בלעדיהם הדפדוף סורק 3.84 מיליון שורות בכל שאילתה.",
    optIn: true },
  { id: "parcels", label: "שדרת החלקות", note: "1.1 מיליון חלקות משכבת החלקות." },
  { id: "gazetteer", label: "גזטיר הנכסים", note: "3.68 מיליון שורות, מגולגלות לרמת חלקה." },
  { id: "postal_localities", label: "יישובי הדואר", note: "גישור קוד דואר ישראל לקוד הלמ\"ס." },
  { id: "streets", label: "אינדקס הרחובות", note: "רחובות והטיות שלהם." },
  { id: "addresses", label: "שדרת הכתובות", note: "622 אלף כתובות, כולל המיקוד." },
  { id: "zip5", label: "גלגול מיקוד", note: "מיקוד 5 ליישוב." },
  { id: "pip", label: "שיוך כתובת לחלקה (PIP)",
    note: "נקודה בתוך פוליגון. חייב לרוץ אחרי שדרת הכתובות: ה-TRUNCATE שם מוחק כל parcel_key, ורק השלב הזה כותב אותם בחזרה." },
];

const DEFAULT_SELECTION = STAGES.filter((s) => !s.optIn).map((s) => s.id);

function fmtMs(ms: number | null): string {
  if (ms == null) return "—";
  if (ms < 1000) return `${ms} ms`;
  const s = ms / 1000;
  return s < 90 ? `${s.toFixed(1)} שנ׳` : `${(s / 60).toFixed(1)} דק׳`;
}

function fmtTime(iso: string | null): string {
  if (!iso) return "—";
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? iso : d.toLocaleString("he-IL");
}

function statusColor(status: string | null): string {
  if (status === "ok") return "var(--success)";
  if (status === "running") return "var(--primary)";
  if (status === "partial") return "var(--warning)";
  return "var(--danger)";
}

export default function NadlanBuildPanel() {
  const [state, setState] = useState<NadlanBuildState | null>(null);
  const [selected, setSelected] = useState<string[]>(DEFAULT_SELECTION);
  const [busy, setBusy] = useState(false);
  const [merging, setMerging] = useState(false);
  const [mergeResult, setMergeResult] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [message, setMessage] = useState<string | null>(null);

  const load = useCallback(async () => {
    try {
      setState(await admin.nadlanState());
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : "טעינת המצב נכשלה");
    }
  }, []);

  useEffect(() => { void load(); }, [load]);

  // A build is a background task, so the response says "started", not "done".
  // Poll while any stage is still running, and stop as soon as none is.
  const running = (state?.stages ?? []).some((s) => s.status === "running");
  useEffect(() => {
    if (!running) return;
    const t = setInterval(() => { void load(); }, 5000);
    return () => clearInterval(t);
  }, [running, load]);

  const toggle = (id: string) =>
    setSelected((prev) => prev.includes(id) ? prev.filter((x) => x !== id) : [...prev, id]);

  const run = async () => {
    if (!selected.length) return;
    setBusy(true); setError(null); setMessage(null);
    try {
      const res = await admin.nadlanBuild(selected.join(","));
      setMessage(res.message ?? "הבנייה התחילה ברקע.");
      await load();
    } catch (e) {
      setError(e instanceof Error ? e.message : "ההפעלה נכשלה");
    } finally {
      setBusy(false);
    }
  };

  const merge = async () => {
    setMerging(true); setError(null); setMergeResult(null);
    try {
      const r = await admin.nadlanGeocodeMerge();
      setMergeResult(
        `מוזגו ${r.merged.toLocaleString("he-IL")} נקודות` +
        (r.rejected_outside_locality
          ? `, ${r.rejected_outside_locality.toLocaleString("he-IL")} נדחו כמחוץ ליישוב`
          : ""));
      await load();
    } catch (e) {
      setError(e instanceof Error ? e.message : "המיזוג נכשל");
    } finally {
      setMerging(false);
    }
  };

  const byStage = new Map<string, NadlanStageRow>(
    (state?.stages ?? []).map((s) => [s.stage, s]));

  return (
    <div>
      <h2 style={{ marginTop: 0 }}>נדל"ן לעם — בניית ההצלבה</h2>
      <p className="text-sm text-muted" style={{ lineHeight: 1.8, maxWidth: "60rem" }}>
        כל שלב אידמפוטנטי (TRUNCATE + INSERT), ואפשר להריץ אותם בנפרד.
        <b>אינדקסים על טבלאות המקור</b> הוא היחיד שאינו בברירת המחדל: הוא חד-פעמי
        לכל טבלה ואינו צריך לחזור. <b>PIP</b> כן בברירת המחדל, כי שדרת הכתובות
        מוחקת כל <code>parcel_key</code> ורק הוא כותב אותם בחזרה, והרצה בלעדיו
        משאירה את חיפוש הכתובות בלי אף חלקה. הבנייה רצה ברקע; הטבלה כאן מתרעננת
        מעצמה כל עוד יש שלב פעיל.
      </p>

      {state?.stats && Object.keys(state.stats).length > 0 && (
        <div className="text-sm" style={{
          margin: "0.6rem 0 1rem", padding: "0.6rem 0.9rem", borderRadius: 8,
          background: "var(--surface-2)", border: "1px solid var(--border)",
        }}>
          {(state.stats.parcels ?? 0).toLocaleString("he-IL")} חלקות ·{" "}
          {(state.stats.addresses ?? 0).toLocaleString("he-IL")} כתובות ·{" "}
          {(state.stats.streets ?? 0).toLocaleString("he-IL")} רחובות ·{" "}
          {(state.stats.zip5_codes ?? 0).toLocaleString("he-IL")} מיקודים
        </div>
      )}

      <div style={{ display: "grid", gap: "0.4rem", marginBottom: "0.9rem" }}>
        {STAGES.map((s) => (
          <label key={s.id} className="text-sm" style={{ display: "flex", gap: "0.5rem", alignItems: "flex-start" }}>
            <input
              type="checkbox"
              checked={selected.includes(s.id)}
              onChange={() => toggle(s.id)}
              style={{ marginTop: "0.3rem" }}
            />
            <span>
              <b>{s.label}</b>
              {s.optIn && (
                <span style={{
                  marginInlineStart: "0.4rem", fontSize: "0.72rem", padding: "0.05rem 0.4rem",
                  borderRadius: 999, background: "#fef3c7", color: "#833909",
                }}>
                  לא בברירת המחדל
                </span>
              )}
              <span className="text-muted" style={{ fontSize: "0.8rem" }}> — {s.note}</span>
              <code className="text-muted" style={{ fontSize: "0.72rem", marginInlineStart: "0.35rem" }}>{s.id}</code>
            </span>
          </label>
        ))}
      </div>

      <div className="flex" style={{ gap: "0.5rem", alignItems: "center", flexWrap: "wrap" }}>
        <button
          type="button"
          className="btn"
          disabled={busy || !selected.length}
          onClick={() => void run()}
        >
          {busy ? "מפעיל…"
                : selected.length === 1 ? "הרצת שלב אחד"
                : `הרצת ${selected.length} שלבים`}
        </button>
        <button
          type="button"
          onClick={() => setSelected(DEFAULT_SELECTION)}
          style={{
            padding: "0.35rem 0.8rem", fontSize: "0.85rem", cursor: "pointer",
            border: "1px solid var(--border)", borderRadius: 6, background: "none",
          }}
        >
          חזרה לברירת המחדל
        </button>
        <button
          type="button"
          onClick={() => void load()}
          style={{
            padding: "0.35rem 0.8rem", fontSize: "0.85rem", cursor: "pointer",
            border: "1px solid var(--border)", borderRadius: 6, background: "none",
          }}
        >
          רענון מצב
        </button>
        {running && <span className="text-sm" style={{ color: "var(--primary)" }}>בנייה פעילה…</span>}
      </div>

      {message && <div className="text-sm" style={{ marginTop: "0.6rem", color: "var(--success)" }}>{message}</div>}
      {error && <div className="text-sm" style={{ marginTop: "0.6rem", color: "var(--danger)" }}>{error}</div>}

      <div tabIndex={0} role="region" aria-label="מצב שלבי הבנייה" className="scroll-region"
           style={{ marginTop: "1rem", overflowX: "auto" }}>
        <table style={{ width: "100%", fontSize: "0.85rem", borderCollapse: "collapse" }}>
          <thead>
            <tr style={{ textAlign: "start", color: "var(--text-muted)" }}>
              {["שלב", "סטטוס", "שורות", "משך", "הסתיים", "הערה"].map((h) => (
                <th key={h} scope="col" style={{ textAlign: "start", padding: "0.3rem 0.45rem", whiteSpace: "nowrap" }}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {STAGES.map((s) => {
              const row = byStage.get(s.id);
              return (
                <tr key={s.id} style={{ borderTop: "1px solid var(--border)" }}>
                  <td style={{ padding: "0.3rem 0.45rem", whiteSpace: "nowrap" }}>{s.label}</td>
                  <td style={{ padding: "0.3rem 0.45rem", color: row ? statusColor(row.status) : "var(--text-muted)" }}>
                    {row?.status ?? "טרם רץ"}
                  </td>
                  <td style={{ padding: "0.3rem 0.45rem" }}>
                    {row?.rows_out != null ? row.rows_out.toLocaleString("he-IL") : "—"}
                  </td>
                  <td style={{ padding: "0.3rem 0.45rem", whiteSpace: "nowrap" }}>{fmtMs(row?.duration_ms ?? null)}</td>
                  <td style={{ padding: "0.3rem 0.45rem", whiteSpace: "nowrap" }}>{fmtTime(row?.finished_at ?? null)}</td>
                  <td style={{ padding: "0.3rem 0.45rem", maxWidth: "26rem" }}>{row?.note ?? "—"}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      <div style={{
        marginTop: "1.2rem", padding: "0.8rem 1rem", borderRadius: 8,
        background: "var(--surface-2)", border: "1px solid var(--border)",
      }}>
        <div style={{ fontWeight: 700, fontSize: "0.9rem", marginBottom: "0.35rem" }}>
          נקודות מ-GovMap
        </div>
        <p className="text-sm text-muted" style={{ margin: "0 0 0.6rem", lineHeight: 1.8 }}>
          הכתובות שקיבלו קואורדינטה מהג׳אוקודר נשמרות בתור נפרד ומתמזגות לשדרת
          הכתובות. בניית שדרת הכתובות מוחקת גם את הנקודות שכבר מוזגו, אבל{" "}
          <b>אין צורך ללחוץ כאן אחריה</b>: המיזוג רץ ממילא כל 15 דקות בתוך משימת
          הג׳אוקודינג המתוזמנת. הכפתור כאן רק מקדים אותו, למשל מיד אחרי בנייה
          במקום להמתין לטיק הבא. נקודה קיימת לעולם לא נדרסת, ונקודה שנפלה מחוץ
          ליישוב של הכתובת נדחית.
        </p>
        <button
          type="button"
          disabled={merging}
          onClick={() => void merge()}
          style={{
            padding: "0.35rem 0.9rem", fontSize: "0.85rem", cursor: "pointer",
            border: "1px solid var(--border)", borderRadius: 6, background: "none",
          }}
        >
          {merging ? "ממזג…" : "מיזוג נקודות לשדרת הכתובות"}
        </button>
        {mergeResult && (
          <span className="text-sm" style={{ marginInlineStart: "0.6rem", color: "var(--success)" }}>
            {mergeResult}
          </span>
        )}
      </div>

      <p className="text-sm text-muted" style={{ marginTop: "0.8rem", lineHeight: 1.7 }}>
        תיעוד מלא: <code>docs/nadlan.md</code>. מאגר העסקאות עצמו מתועד ב-<code>docs/deals.md</code>,
        והוא מאותר לפי חתימת העמודות ולא לפי שם — ולכן האינדקסים שלו נוצרים בשלב
        הזה ולא ברשימה סטטית.
      </p>
    </div>
  );
}
