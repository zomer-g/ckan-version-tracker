// The "כולל היסטוריה" switch of the SQL consoles and the line that says which
// engine answered (docs/history-phase3-plan.md). With the switch on, the server
// runs the query on DuckDB over the SQL table plus the Parquet files whose rows
// are no longer in SQL; without such files it stays on Postgres and says so.
import type { HistoryEngineInfo } from "../api/client";

export function HistoryToggle({ checked, onChange, rowsOnlyInR2, general = false }: {
  checked: boolean;
  onChange: (v: boolean) => void;
  rowsOnlyInR2?: number;
  general?: boolean;
}) {
  return (
    <label
      style={{ display: "inline-flex", alignItems: "center", gap: 6, fontSize: 13, cursor: "pointer" }}
      title="השאילתה רצה על מנוע ההיסטוריה (DuckDB) ומאחדת את טבלת ה-SQL עם קבצי ההיסטוריה ב-R2. איטית יותר; הדיאלקט קרוב ל-Postgres אך לא זהה (ללא PostGIS)."
    >
      <input type="checkbox" checked={checked} onChange={(e) => onChange(e.target.checked)} />
      כולל היסטוריה
      <span style={{ color: "var(--text-secondary)", fontSize: 12 }}>
        {general
          ? "(לטבלאות שחלק מהיסטוריה שלהן הועבר לקבצים; איטי יותר, DuckDB)"
          : `(${(rowsOnlyInR2 ?? 0).toLocaleString()} שורות נמצאות רק בקבצי ההיסטוריה; איטי יותר, DuckDB)`}
      </span>
    </label>
  );
}

export function EngineBadge({ result }: { result: HistoryEngineInfo | null | undefined }) {
  if (!result?.engine) return null;
  const notes = [result.history_note, result.dialect_note].filter(Boolean) as string[];
  return (
    <div style={{ fontSize: 12, color: "var(--text-secondary)", margin: "4px 0" }}>
      {result.engine === "duckdb" ? (
        <span style={{
          display: "inline-block", padding: "1px 8px", borderRadius: 10,
          background: "var(--primary)", color: "white",
        }}>
          DuckDB · {(result.cold_parts ?? 0).toLocaleString()} חלקים · {(result.elapsed_ms ?? 0).toLocaleString()} ms
        </span>
      ) : (
        <span>Postgres</span>
      )}
      {notes.map((n) => (
        <div key={n} style={{ marginTop: 2 }}>{n}</div>
      ))}
    </div>
  );
}
