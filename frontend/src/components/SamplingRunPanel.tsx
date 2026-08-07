import { useEffect, useState } from "react";
import { datasets as datasetsApi, type SamplingOptions } from "../api/client";
import { useAuth } from "../auth/AuthContext";

/**
 * Admin panel: sample a register in a targeted way instead of re-reading it whole.
 *
 * A source like the Jerusalem building-licensing register is too large and too
 * slow to read in full for most questions — a complete pass is hours of paced
 * requests. The useful questions are narrower, and each is the same scrape with
 * a different target list:
 *
 *   כל הישויות     the honest full pass
 *   רק חדשים       what the site has and OVER does not
 *   לפי סטטוס      the files sitting at one status — where change actually is
 *   תיק בודד       one file, now
 *
 * Renders nothing unless the dataset's source declares it supports this, so it
 * is invisible on every other dataset rather than being a dead control.
 */
/** "604800" → "שבוע". A cadence is read, not counted in seconds. */
function everyLabel(seconds: number): string {
  const days = Math.round(seconds / 86400);
  if (days >= 28 && days <= 31) return "חודש";
  if (days === 7) return "שבוע";
  if (days >= 1) return `${days} ימים`;
  const hours = Math.max(1, Math.round(seconds / 3600));
  return `${hours} שעות`;
}

export default function SamplingRunPanel({ datasetId }: { datasetId: string }) {
  const { user } = useAuth();
  const [opts, setOpts] = useState<SamplingOptions | null>(null);
  const [status, setStatus] = useState("");
  const [item, setItem] = useState("");
  // Take the item list from another dataset of the same source instead of
  // re-discovering it — see sampling_runs. Empty = discover as usual.
  const [targetsFrom, setTargetsFrom] = useState("");
  const [busy, setBusy] = useState(false);
  const [toast, setToast] = useState<{ ok: boolean; msg: string } | null>(null);

  useEffect(() => {
    if (!user?.is_admin) return;
    let alive = true;
    datasetsApi
      .sampling(datasetId)
      .then((o) => alive && setOpts(o))
      .catch(() => alive && setOpts(null));
    return () => {
      alive = false;
    };
  }, [datasetId, user?.is_admin]);

  if (!user?.is_admin || !opts?.enabled) return null;

  const labels = opts.mode_labels || {};
  const statuses = opts.statuses || [];
  // Modes OVER runs on its own cadence. Without this the panel reads the same
  // whether a mode runs weekly by itself or only ever when someone clicks —
  // and that difference is the whole question of whether the dataset is
  // keeping up with its source.
  const scheduled = Object.entries(opts.schedule || {});

  const run = async (mode: string) => {
    setBusy(true);
    setToast(null);
    try {
      const r = await datasetsApi.sample(datasetId, {
        mode,
        status: mode === "status" ? status || undefined : undefined,
        item: mode === "item" ? item.trim() : undefined,
        targets_from_dataset:
          mode === "status" && targetsFrom.trim() ? targetsFrom.trim() : undefined,
      });
      setToast({ ok: true, msg: `נוסף לתור: ${r.summary} ✓` });
    } catch (e: any) {
      setToast({ ok: false, msg: e?.message || "שגיאה בהפעלת הדגימה" });
    } finally {
      setBusy(false);
      setTimeout(() => setToast(null), 6000);
    }
  };

  const btn = {
    fontSize: "0.75rem",
    padding: "0.3rem 0.7rem",
    whiteSpace: "nowrap" as const,
  };

  return (
    <div
      className="card"
      style={{ padding: "0.9rem", marginTop: "1rem", display: "grid", gap: "0.7rem" }}
    >
      <div style={{ display: "flex", alignItems: "baseline", gap: "0.5rem", flexWrap: "wrap" }}>
        <strong style={{ fontSize: "0.9rem" }}>דגימה ממוקדת</strong>
        <span style={{ fontSize: "0.72rem", color: "var(--text-muted)" }}>
          כל דגימה נשמרת כשורה נוספת עם חותמת זמן — ההיסטוריה של כל ישות נשמרת במלואה
          {opts.item_key ? ` (מפתח: ${opts.item_key})` : ""}
        </span>
      </div>

      <div style={{ display: "flex", gap: "0.5rem", flexWrap: "wrap", alignItems: "center" }}>
        {opts.modes.includes("all") && (
          <button className="btn-secondary" style={btn} disabled={busy} onClick={() => run("all")}>
            {labels.all || "כל הישויות"}
          </button>
        )}
        {opts.modes.includes("new") && (
          <button className="btn-secondary" style={btn} disabled={busy} onClick={() => run("new")}>
            {labels.new || "רק חדשות"}
          </button>
        )}
        {scheduled.map(([mode, s]) => (
          <span
            key={mode}
            title={
              s.last_run_at
                ? `הריצה האוטומטית האחרונה: ${new Date(s.last_run_at).toLocaleString("he-IL")}`
                : "טרם רצה אוטומטית — תיכנס לתור בבדיקה הקרובה"
            }
            style={{
              fontSize: "0.7rem",
              color: "var(--text-muted)",
              border: "1px solid var(--border)",
              borderRadius: "999px",
              padding: "0.15rem 0.55rem",
            }}
          >
            ⏱ {labels[mode] || mode} — אוטומטי כל {everyLabel(s.interval_seconds)}
            {s.last_run_at
              ? `, אחרונה ${new Date(s.last_run_at).toLocaleDateString("he-IL")}`
              : ", טרם רצה"}
          </span>
        ))}
      </div>

      {opts.modes.includes("status") && (
        <div style={{ display: "flex", gap: "0.4rem", flexWrap: "wrap", alignItems: "center" }}>
          <select
            value={status}
            onChange={(e) => setStatus(e.target.value)}
            style={{ fontSize: "0.75rem", padding: "0.25rem", maxWidth: "22rem" }}
          >
            <option value="">— בחר סטטוס —</option>
            {statuses.map((s) => (
              <option key={s.value} value={s.value}>
                {s.value || "(ריק)"} — {s.items.toLocaleString("he-IL")}
              </option>
            ))}
          </select>
          <button
            className="btn-secondary"
            style={btn}
            disabled={busy || !status}
            onClick={() => run("status")}
          >
            {labels.status || "לפי סטטוס"}
          </button>
          {!statuses.length && (
            <span style={{ fontSize: "0.7rem", color: "var(--text-muted)" }}>
              אין עדיין סטטוסים בארכיון — הרץ דגימה מלאה אחת קודם
            </span>
          )}
        </div>
      )}

      {opts.modes.includes("status") && (
        <div style={{ display: "grid", gap: "0.3rem" }}>
          <label style={{ fontSize: "0.72rem", color: "var(--text-muted)" }}>
            רשימת הישויות ממאגר אחר (אופציונלי) — מדלג על גילוי מחדש
          </label>
          <div style={{ display: "flex", gap: "0.4rem", flexWrap: "wrap", alignItems: "center" }}>
            <input
              value={targetsFrom}
              onChange={(e) => setTargetsFrom(e.target.value)}
              placeholder="מזהה מאגר המקור (UUID)"
              style={{ fontSize: "0.75rem", padding: "0.25rem 0.4rem", width: "22rem" }}
            />
            <button
              className="btn-secondary"
              style={btn}
              disabled={busy || !targetsFrom.trim()}
              onClick={() => run("status")}
            >
              דגום לפי הרשימה
            </button>
          </div>
        </div>
      )}

      {opts.modes.includes("item") && (
        <div style={{ display: "flex", gap: "0.4rem", flexWrap: "wrap", alignItems: "center" }}>
          <input
            value={item}
            onChange={(e) => setItem(e.target.value)}
            placeholder={opts.item_key || "מזהה ישות"}
            style={{ fontSize: "0.75rem", padding: "0.25rem 0.4rem", width: "12rem" }}
          />
          <button
            className="btn-secondary"
            style={btn}
            disabled={busy || !item.trim()}
            onClick={() => run("item")}
          >
            {labels.item || "ישות בודדת"}
          </button>
        </div>
      )}

      {toast && (
        <span
          style={{
            fontSize: "0.72rem",
            padding: "0.25rem 0.5rem",
            borderRadius: "4px",
            background: toast.ok ? "#dcfce7" : "#fee2e2",
            color: toast.ok ? "#166534" : "#991b1b",
          }}
        >
          {toast.msg}
        </span>
      )}
    </div>
  );
}
