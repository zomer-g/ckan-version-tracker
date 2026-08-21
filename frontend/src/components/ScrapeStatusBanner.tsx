import { useEffect, useState } from "react";
import { datasets as datasetsApi, type ScrapeStatus } from "../api/client";

import { autoRefreshPaused } from "../hooks/useAutoRefresh";
/**
 * "This collection is running right now, and here is how it's doing."
 *
 * A collection that takes days publishes nothing until it finishes, so a
 * dataset page with no versions looks identical whether the run is healthy,
 * stalled, or long dead. This says which — to anyone, with no login.
 *
 * The heartbeat age is the honest health signal: the worker reports every 30
 * seconds, so anything past a few minutes means trouble regardless of what the
 * message says.
 */
function human(seconds: number): string {
  if (seconds < 90) return `${Math.round(seconds)} שניות`;
  if (seconds < 5400) return `${Math.round(seconds / 60)} דקות`;
  return `${(seconds / 3600).toFixed(1)} שעות`;
}

export default function ScrapeStatusBanner({ datasetId }: { datasetId: string }) {
  const [status, setStatus] = useState<ScrapeStatus | null>(null);

  useEffect(() => {
    let alive = true;
    const load = () =>
      datasetsApi
        .scrapeStatus(datasetId)
        .then((s) => alive && setStatus(s))
        .catch(() => alive && setStatus(null));
    load();
    // While a run is in flight the page is worth refreshing; the endpoint is
    // a single indexed row, and 30s matches the worker's own heartbeat. The
    // user can switch that off globally (WCAG 2.2.4).
    if (autoRefreshPaused()) return () => { alive = false; };
    const timer = window.setInterval(load, 30000);
    return () => {
      alive = false;
      window.clearInterval(timer);
    };
  }, [datasetId]);

  if (!status?.running) return null;

  const stale = (status.seconds_since_heartbeat ?? 0) > 600;
  return (
    <div
      className="card"
      style={{
        padding: "0.75rem 0.9rem",
        marginTop: "1rem",
        borderInlineStart: `3px solid ${stale ? "var(--danger)" : "#0C5E58"}`,
        display: "grid",
        gap: "0.35rem",
      }}
    >
      <div style={{ display: "flex", gap: "0.5rem", alignItems: "baseline", flexWrap: "wrap" }}>
        <strong style={{ fontSize: "0.9rem" }}>
          {stale ? "⚠ איסוף שנתקע?" : "● איסוף מתבצע כעת"}
        </strong>
        {status.elapsed_seconds != null && (
          <span style={{ fontSize: "0.78rem", color: "var(--text-muted)" }}>
            רץ {human(status.elapsed_seconds)}
          </span>
        )}
      </div>
      {status.message && (
        <div style={{ fontSize: "0.8rem" }} dir="auto">
          {status.message}
        </div>
      )}
      <div style={{ fontSize: "0.72rem", color: stale ? "var(--danger)" : "var(--text-muted)" }}>
        {stale
          ? `אין דיווח מהמכונה כבר ${human(status.seconds_since_heartbeat ?? 0)} — ייתכן שהריצה נפלה`
          : `דיווח אחרון לפני ${human(status.seconds_since_heartbeat ?? 0)}`}
        {" · "}
        גרסה תיווצר רק בסיום האיסוף
      </div>
    </div>
  );
}
