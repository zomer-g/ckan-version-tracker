/**
 * לוג הסרות — what publishers took down: files that left a dataset between two
 * versions, whole sources that disappeared, and GovMap layers that left the
 * catalog. Shown on the system checks tab (app/services/removals_log.py).
 */
import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { admin as adminApi, RemovalEvent, RemovalsLog } from "../api/client";

const KIND: Record<string, string> = {
  file_removed: "קובץ הוסר",
  file_replaced: "קובץ הוחלף",
  dataset_gone: "מאגר הוסר מהמקור",
  layer_removed: "שכבת GovMap הוסרה",
};

const kindOf = (e: RemovalEvent) => (e.replaced ? "file_replaced" : e.kind);

function fmt(iso?: string | null): string {
  if (!iso) return "—";
  return new Date(iso).toLocaleString("he-IL", {
    day: "2-digit", month: "2-digit", year: "2-digit", hour: "2-digit", minute: "2-digit",
  });
}

function toCsv(events: RemovalEvent[]): string {
  const head = ["מתי", "סוג", "מה הוסר", "מאגר", "מזהה מאגר", "גרסה קודמת", "גרסה", "מזהה קובץ/שכבה", "קישור למקור"];
  const q = (v: unknown) => {
    const s = v == null ? "" : String(v);
    return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };
  const rows = events.map((e) => [
    e.at, KIND[kindOf(e)], e.name ?? e.resource_id ?? e.layer_id, e.dataset_title, e.dataset_id,
    e.previous_version, e.version_number, e.resource_id ?? e.layer_id, e.source_url,
  ].map(q).join(","));
  return "﻿" + [head.join(","), ...rows].join("\n");
}

const cell = { padding: "0.3rem 0.45rem", verticalAlign: "top" } as const;

export default function RemovalsLogPanel() {
  const [days, setDays] = useState(30);
  const [kind, setKind] = useState("");
  const [data, setData] = useState<RemovalsLog | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    let live = true;
    setLoading(true);
    setError(null);
    adminApi.healthRemovals(days)
      .then((d) => { if (live) setData(d); })
      .catch((e) => { if (live) setError(e instanceof Error ? e.message : String(e)); })
      .finally(() => { if (live) setLoading(false); });
    return () => { live = false; };
  }, [days]);

  const events = useMemo(
    () => (data?.events ?? []).filter((e) => !kind || kindOf(e) === kind),
    [data, kind],
  );

  const download = () => {
    const blob = new Blob([toCsv(events)], { type: "text/csv;charset=utf-8" });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = `removals-${days}d-${new Date().toISOString().slice(0, 10)}.csv`;
    a.click();
    URL.revokeObjectURL(a.href);
  };

  const sel = { fontSize: "0.85rem", padding: "0.2rem 0.4rem" } as const;
  const counts = data?.counts ?? {};
  return (
    <section className="card mb-2" style={{ padding: "1rem 1.25rem" }}>
      <div className="flex" style={{ justifyContent: "space-between", flexWrap: "wrap", gap: "0.6rem", alignItems: "center" }}>
        <h3 style={{ margin: 0, fontSize: "1.1rem" }}><span aria-hidden="true">🗑️</span> לוג הסרות</h3>
        <span className="flex" style={{ gap: "0.5rem", flexWrap: "wrap", alignItems: "center" }}>
          <label className="text-sm">תקופה{" "}
            <select style={sel} value={days} onChange={(e) => setDays(Number(e.target.value))}>
              <option value={7}>שבוע</option>
              <option value={30}>30 יום</option>
              <option value={90}>90 יום</option>
              <option value={365}>שנה</option>
            </select>
          </label>
          <label className="text-sm">סוג{" "}
            <select style={sel} value={kind} onChange={(e) => setKind(e.target.value)}>
              <option value="">הכל</option>
              {Object.entries(KIND).map(([k, l]) => <option key={k} value={k}>{l}</option>)}
            </select>
          </label>
          <button type="button" className="text-sm" disabled={!events.length} onClick={download}
            style={{ background: "none", border: "1px solid var(--border)", borderRadius: 4, padding: "0.25rem 0.7rem", cursor: "pointer" }}>
            הורדה כ-CSV
          </button>
        </span>
      </div>
      <p className="text-sm text-muted" style={{ margin: "0.4rem 0 0", lineHeight: 1.6 }}>
        קבצים שהיו בגרסה אחת של מאגר ואינם בגרסה הבאה, מאגרים שהמפרסם הסיר, ושכבות שירדו מהקטלוג של GovMap.
        {" "}קבצים מושווים רק בין גרסאות מאותו סוג (משיכת data.gov.il מול משיכת data.gov.il), ולא בריצות חלקיות — אחרת כל
        מעבר בין סוגי גרסאות נראה כמו הסרה. "הוחלף" = הגיע באותה גרסה קובץ חדש באותו שם.
      </p>
      {data && (
        <div className="text-sm" style={{ display: "flex", gap: "1rem", flexWrap: "wrap", marginTop: "0.4rem" }}>
          {Object.keys(KIND).map((k) => <span key={k}>{KIND[k]}: <b>{counts[k] ?? 0}</b></span>)}
        </div>
      )}
      {data?.errors.map((e) => <div key={e} className="text-sm" style={{ color: "var(--warning)" }}>{e}</div>)}
      {error && <div className="text-sm" style={{ color: "var(--danger)" }}>{error}</div>}
      {loading && <div className="text-sm text-muted" role="status">טוען…</div>}
      {!loading && data && !events.length && (
        <p className="text-sm text-muted">לא נרשמו הסרות בתקופה הזו.</p>
      )}
      {events.length > 0 && (
        <div tabIndex={0} role="region" aria-label="לוג הסרות" className="scroll-region"
          style={{ overflow: "auto", maxHeight: 520, marginTop: "0.6rem" }}>
          <table style={{ width: "100%", fontSize: "0.85rem", borderCollapse: "collapse" }}>
            <thead>
              <tr style={{ color: "var(--text-muted)" }}>
                {["מתי", "סוג", "מה הוסר", "מאגר", "גרסה"].map((h) => (
                  <th key={h} scope="col" style={{ ...cell, textAlign: "start" }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {events.map((e, i) => (
                <tr key={`${e.kind}-${e.dataset_id ?? e.layer_id}-${e.resource_id ?? ""}-${i}`}
                  style={{ borderTop: "1px solid var(--border)" }}>
                  <td style={{ ...cell, whiteSpace: "nowrap" }}>{fmt(e.at)}</td>
                  <td style={{ ...cell, whiteSpace: "nowrap" }}>{KIND[kindOf(e)]}</td>
                  <td style={{ ...cell, overflowWrap: "anywhere" }}>
                    {e.name ?? <span dir="ltr" className="text-muted">{e.resource_id ?? e.layer_id}</span>}
                  </td>
                  <td style={{ ...cell, overflowWrap: "anywhere" }}>
                    {e.dataset_id
                      ? <Link to={`/versions/${e.dataset_id}`}>{e.dataset_title ?? e.dataset_id}</Link>
                      : e.source_url
                        ? <a href={e.source_url} target="_blank" rel="noopener noreferrer">GovMap ↗</a>
                        : "—"}
                  </td>
                  <td style={{ ...cell, whiteSpace: "nowrap" }} className="text-muted">
                    {e.version_number != null ? `${e.previous_version} → ${e.version_number}` : "—"}
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
