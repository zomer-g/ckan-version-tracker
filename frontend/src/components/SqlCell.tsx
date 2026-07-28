import { useState } from "react";
import { dataCatalog, type KnessetDbSqlResult } from "../api/client";

// A minimal, Jupyter-like SQL cell for the guide: shows ONE fixed example query
// with a run button, and renders its result inline — so the demo runs in place,
// without navigating away. The query is fixed (read-only), not a free editor.
export default function SqlCell({ sql, maxRows = 12 }: { sql: string; maxRows?: number }) {
  const [result, setResult] = useState<KnessetDbSqlResult | null>(null);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  const run = async () => {
    if (busy) return;
    setBusy(true); setErr(null);
    try { setResult(await dataCatalog.sql(sql)); }
    catch (e) { setErr((e as Error)?.message || "שגיאה"); setResult(null); }
    finally { setBusy(false); }
  };

  const rows = result?.rows ?? [];
  const shown = rows.slice(0, maxRows);

  return (
    <div className="cell" dir="rtl">
      <div className="cell-head">
        <button className="cell-run" onClick={run} disabled={busy}>{busy ? "מריץ…" : "▸ הרץ"}</button>
        <span className="cell-label">דוגמה חיה</span>
      </div>
      <pre className="cell-sql">{sql}</pre>

      {err && <div className="cell-err">{err}</div>}
      {result && (
        <div className="cell-out">
          <div className="cell-count">{result.row_count.toLocaleString()} שורות{result.truncated ? " (נחתך)" : ""}{rows.length > maxRows ? ` · מוצגות ${maxRows}` : ""}</div>
          {result.columns.length > 0 && rows.length > 0 ? (
            <div className="cell-tblwrap">
              <table className="cell-tbl">
                <thead><tr>{result.columns.map((c) => <th key={c}>{c}</th>)}</tr></thead>
                <tbody>
                  {shown.map((r, i) => (
                    <tr key={i}>{result.columns.map((c) => {
                      const v = r[c];
                      return <td key={c}>{v === null || v === undefined ? "" : String(v)}</td>;
                    })}</tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : <div className="cell-empty">אין שורות.</div>}
        </div>
      )}

      <style>{`
        .cell { border: 1px solid var(--border, #e5e7eb); border-radius: 10px; overflow: hidden; margin: 1rem 0; background: var(--bg, #fff); }
        .cell-head { display: flex; align-items: center; gap: 10px; padding: 0.5rem 0.7rem; background: var(--bg-muted, #f8fafc); border-bottom: 1px solid var(--border, #e5e7eb); }
        .cell-run { padding: 0.28rem 0.9rem; border-radius: 5px; border: 1px solid var(--primary, #0f766e); background: var(--primary, #0f766e); color: #fff; font-weight: 700; font-size: 0.82rem; cursor: pointer; }
        .cell-run:disabled { opacity: 0.6; cursor: wait; }
        .cell-label { font-size: 0.75rem; color: var(--text-muted, #94a3b8); font-weight: 600; }
        .cell-sql { margin: 0; padding: 0.8rem 1rem; direction: ltr; text-align: left; background: #0e1a18; color: #d7e6e2; overflow-x: auto; font-family: ui-monospace, Consolas, monospace; font-size: 0.8rem; line-height: 1.6; white-space: pre; }
        .cell-err { color: #b91c1c; background: #fef2f2; border-top: 1px solid #fecaca; padding: 0.6rem 0.9rem; font-size: 0.85rem; }
        .cell-out { padding: 0.7rem 0.9rem; }
        .cell-count { font-size: 0.78rem; color: var(--text-muted, #64748b); margin-bottom: 0.4rem; }
        .cell-tblwrap { overflow: auto; max-height: 320px; border: 1px solid var(--border, #eef2f5); border-radius: 8px; }
        .cell-tbl { width: 100%; border-collapse: collapse; font-size: 0.85rem; white-space: nowrap; }
        .cell-tbl th, .cell-tbl td { text-align: right; padding: 6px 12px; border-bottom: 1px solid var(--border, #eef2f5); }
        .cell-tbl thead th { position: sticky; top: 0; background: var(--bg-muted, #eef2f5); color: var(--primary, #0f766e); font-weight: 700; font-size: 0.78rem; }
        .cell-tbl tbody tr:last-child td { border-bottom: none; }
        .cell-empty { color: var(--text-muted, #94a3b8); font-size: 0.85rem; }
      `}</style>
    </div>
  );
}
