/**
 * The Parquet mirrors of the large tables, and the button that builds them.
 *
 * It exists because the alternative was a token. Every maintenance job here is
 * an admin endpoint, and reaching one from a phone meant reading a JWT out of
 * localStorage — which needs devtools, which a phone does not have. The token
 * also expires in two hours, so "get the token" is not a thing you do once.
 * A logged-in admin already carries the credential; the button just uses it.
 */
import { useCallback, useEffect, useState } from "react";
import { admin as adminApi, ParquetTable } from "../api/client";

function mb(bytes: number | null | undefined): string {
  if (bytes == null) return "—";
  if (bytes < 1024 * 1024) return `${Math.round(bytes / 1024)} KB`;
  return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
}

export default function ParquetPanel() {
  const [rows, setRows] = useState<ParquetTable[]>([]);
  const [available, setAvailable] = useState<boolean | null>(null);
  const [reason, setReason] = useState<string | null>(null);
  const [minRows, setMinRows] = useState<number | null>(null);
  const [loading, setLoading] = useState(false);
  const [busy, setBusy] = useState<string | null>(null);
  const [note, setNote] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const d = await adminApi.parquetTables();
      setRows(d.tables ?? []);
      setAvailable(d.available);
      setReason(d.unavailable_reason ?? null);
      setMinRows(d.min_rows ?? null);
    } catch (e) {
      setError(e instanceof Error ? e.message : "טעינה נכשלה");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { void load(); }, [load]);

  const build = async (table?: string) => {
    setBusy(table ?? "__all__");
    setNote(null);
    setError(null);
    try {
      const d = await adminApi.parquetBuild(table);
      setNote(
        table
          ? `הבנייה של ${table} התחילה ברקע.`
          : `הבנייה של ${d.tables} טבלאות התחילה ברקע. זה לוקח זמן — רעננו כדי לראות קבצים מופיעים.`,
      );
    } catch (e) {
      setError(e instanceof Error ? e.message : "הבנייה נכשלה");
    } finally {
      setBusy(null);
    }
  };

  const built = rows.filter((r) => r.built).length;

  return (
    <section className="card mb-2">
      <div className="flex" style={{ justifyContent: "space-between", alignItems: "baseline", gap: "0.6rem", flexWrap: "wrap" }}>
        <h2 style={{ fontSize: "1.1rem", fontWeight: 700, margin: 0 }}>
          עותקי Parquet
        </h2>
        <span className="text-sm text-muted">
          {minRows != null ? `טבלאות מעל ${minRows.toLocaleString("he-IL")} שורות` : ""}
        </span>
      </div>

      <p className="text-sm text-muted" style={{ marginTop: "0.4rem", lineHeight: 1.7 }}>
        אותן שורות בדיוק כמו ה-CSV, שמורות עמודה-אחר-עמודה: הקובץ קטן בהרבה,
        וקורא שצריך שתי עמודות מתוך ארבע-עשרה קורא רק אותן. כל העמודות נשמרות
        כטקסט, כמו בארכיון עצמו — הקובץ הזה הוא תוספת ל-CSV ולא תחליף לו.
      </p>

      {available === false && (
        <div className="text-sm" style={{ color: "var(--warning)", marginBottom: "0.5rem" }}>
          ⚠ לא ניתן לייצר כרגע: {reason}
        </div>
      )}
      {error && (
        <div className="text-sm" style={{ color: "var(--danger)", marginBottom: "0.5rem" }}>{error}</div>
      )}
      {note && (
        <div className="text-sm" style={{ color: "var(--success)", marginBottom: "0.5rem" }}>{note}</div>
      )}

      <div className="flex mt-1" style={{ gap: "0.6rem", flexWrap: "wrap", alignItems: "center" }}>
        <button
          type="button"
          onClick={() => void build()}
          disabled={available === false || busy !== null || rows.length === 0}
          className="text-sm"
          style={{
            background: "var(--primary)", color: "#fff", border: "none",
            borderRadius: 4, padding: "0.3rem 0.8rem", cursor: "pointer", fontWeight: 600,
          }}
        >
          {busy === "__all__" ? "מתחיל…" : `בנה את כל ${rows.length} הטבלאות`}
        </button>
        <button
          type="button"
          onClick={() => void load()}
          disabled={loading}
          className="text-sm"
          style={{
            background: "none", border: "1px solid var(--border)",
            borderRadius: 4, padding: "0.3rem 0.8rem", cursor: "pointer",
          }}
        >
          {loading ? "טוען…" : "רענן"}
        </button>
        <span className="text-sm text-muted">{built} מתוך {rows.length} נבנו</span>
      </div>

      <div tabIndex={0} role="region" aria-label="טבלאות Parquet" className="scroll-region"
           style={{ marginTop: "0.8rem", overflowX: "auto" }}>
        <table style={{ width: "100%", fontSize: "0.83rem", borderCollapse: "collapse" }}>
          <thead>
            <tr style={{ textAlign: "start", color: "var(--text-muted)" }}>
              <th scope="col" style={{ textAlign: "start", padding: "0.25rem 0.4rem" }}>מאגר</th>
              <th scope="col" style={{ textAlign: "start", padding: "0.25rem 0.4rem" }}>שורות</th>
              <th scope="col" style={{ textAlign: "start", padding: "0.25rem 0.4rem" }}>Parquet</th>
              <th scope="col" style={{ textAlign: "start", padding: "0.25rem 0.4rem" }} />
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => (
              <tr key={r.table} style={{ borderTop: "1px solid var(--border)" }}>
                <td style={{ padding: "0.25rem 0.4rem" }}>
                  <div>{r.dataset_title ?? r.table}</div>
                  {r.resource_name && (
                    <div className="text-muted" style={{ fontSize: "0.75rem" }}>{r.resource_name}</div>
                  )}
                  <div className="text-muted" style={{ fontSize: "0.72rem", overflowWrap: "anywhere" }}>
                    {r.table}
                  </div>
                </td>
                <td style={{ padding: "0.25rem 0.4rem", whiteSpace: "nowrap" }}>
                  {r.est_rows.toLocaleString("he-IL")}
                </td>
                <td style={{ padding: "0.25rem 0.4rem", whiteSpace: "nowrap" }}>
                  {r.built ? mb(r.parquet_bytes) : <span className="text-muted">טרם נבנה</span>}
                </td>
                <td style={{ padding: "0.25rem 0.4rem" }}>
                  <button
                    type="button"
                    onClick={() => void build(r.table)}
                    disabled={available === false || busy !== null}
                    className="text-sm"
                    style={{
                      background: "none", border: "1px solid var(--border)",
                      borderRadius: 4, padding: "0.15rem 0.6rem", cursor: "pointer",
                    }}
                  >
                    {busy === r.table ? "…" : r.built ? "בנה מחדש" : "בנה"}
                  </button>
                </td>
              </tr>
            ))}
            {!loading && rows.length === 0 && (
              <tr><td colSpan={4} className="text-muted" style={{ padding: "0.6rem 0.4rem" }}>
                אין טבלאות שעוברות את הסף.
              </td></tr>
            )}
          </tbody>
        </table>
      </div>
    </section>
  );
}
