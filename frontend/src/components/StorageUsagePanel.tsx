/**
 * Where the SQL database's bytes go: every table, attributed to a project, a
 * source and the dataset that owns it (GET /api/admin/storage).
 *
 * The server sends ONE flat table list; every grouping here is computed from
 * it, so switching between "by project" and "by source" never re-queries and
 * every view adds up to the same total. Clicking a group narrows to it and
 * steps one level deeper (project → source → dataset → tables).
 */
import { useCallback, useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { formatBytes } from "../api/client";

/** A size, isolated LTR so "13.88 GB" does not flip to "GB 13.88" in RTL. */
function Bytes({ n }: { n: number | null | undefined }) {
  return <bdi dir="ltr">{formatBytes(n)}</bdi>;
}

type Row = [
  schema: string, table: string, total: number, heap: number, indexes: number,
  toast: number, estRows: number | null, project: string, datasetId: string | null,
];

interface DatasetInfo { id: string; title: string; source: string; status: string; org: string | null }

interface StorageReport {
  generated_at: string;
  database_bytes: number | null;
  tables: Row[];
  datasets: Record<string, DatasetInfo>;
  projects: Record<string, string>;
  cached: boolean;
}

type GroupBy = "project" | "source" | "dataset" | "schema" | "status";

const GROUP_LABELS: { id: GroupBy; label: string }[] = [
  { id: "project", label: "פרויקט" },
  { id: "source", label: "מקור" },
  { id: "dataset", label: "מאגר" },
  { id: "schema", label: "סכימה" },
  { id: "status", label: "סטטוס מאגר" },
];

// After narrowing to a group, the next view that makes sense.
const NEXT: Record<GroupBy, GroupBy | null> = {
  project: "source", source: "dataset", schema: "dataset", status: "dataset", dataset: null,
};

const STATUS_LABELS: Record<string, string> = {
  active: "פעיל", pending: "ממתין", paused: "מושהה", hidden: "מוסתר",
  rejected: "נדחה", deleted: "נמחק",
};

const NO_DATASET = "__none__";

async function call<T>(path: string, method: "GET" | "POST" | "PATCH" = "GET", body?: unknown): Promise<T> {
  const token = localStorage.getItem("token");
  const headers: Record<string, string> = token ? { Authorization: `Bearer ${token}` } : {};
  if (body !== undefined) headers["Content-Type"] = "application/json";
  const resp = await fetch(`/api/admin/storage${path}`, {
    method,
    headers,
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
  if (!resp.ok) {
    let detail = `שגיאת שרת (${resp.status})`;
    try {
      const d = (await resp.json())?.detail;
      if (typeof d === "string") detail = d;
      else if (d) detail = d.message || JSON.stringify(d);
    } catch { /* not JSON */ }
    throw new Error(detail);
  }
  return resp.json();
}

const fetchReport = (refresh: boolean) => call<StorageReport>(refresh ? "?refresh=1" : "");

interface HashCompaction {
  pending_tables: number;
  pending_bytes: number;
  running: boolean;
  current: string | null;
  done: number;
  done_bytes_before: number;
  done_bytes_after: number;
  failed: { table: string; error: string }[];
  skipped_busy: string[];
  finished_at: string | null;
}

/** The dedup hash stored as 64-char text costs ~23 GB; as uuid about a third.
 *  The job converts table by table, smallest first (app/services/hash_compaction.py). */
function HashCompactionCard() {
  const [st, setSt] = useState<HashCompaction | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const load = useCallback(async () => {
    try { setSt(await call<HashCompaction>("/hash-compaction")); setErr(null); }
    catch (e) { setErr(e instanceof Error ? e.message : "טעינה נכשלה"); }
  }, []);
  useEffect(() => { void load(); }, [load]);
  useEffect(() => {
    if (!st?.running) return;
    const t = setInterval(() => void load(), 5000);
    return () => clearInterval(t);
  }, [st?.running, load]);

  const act = async (what: "start" | "stop") => {
    if (what === "start" && !window.confirm(
      `להמיר ${st?.pending_tables.toLocaleString("he-IL")} טבלאות? כל טבלה ננעלת לכתיבה ולקריאה ` +
      "למשך ההמרה שלה (שניות, ובגדולות עד דקה-שתיים). אפשר לעצור בכל רגע.")) return;
    try { await call(`/hash-compaction/${what}`, "POST"); await load(); }
    catch (e) { setErr(e instanceof Error ? e.message : "הפעולה נכשלה"); }
  };

  const saved = st ? st.done_bytes_before - st.done_bytes_after : 0;
  return (
    <div style={{ border: "1px solid var(--border)", borderRadius: 6, padding: "0.6rem 0.8rem", marginBottom: "0.9rem", background: "var(--bg-secondary)" }}>
      <div className="flex" style={{ gap: "0.6rem", alignItems: "baseline", flexWrap: "wrap" }}>
        <strong>דחיסת row_hash</strong>
        <span className="text-sm text-muted">
          מזהה הכפילויות נשמר כטקסט של 64 תווים; כ-uuid הוא תופס 16 בתים.
        </span>
      </div>
      {err && <div className="text-sm" style={{ color: "var(--danger)" }}>{err}</div>}
      {st && (
        <div className="flex text-sm" style={{ gap: "0.8rem", alignItems: "center", flexWrap: "wrap", marginTop: "0.4rem" }}>
          <span>נותרו {st.pending_tables.toLocaleString("he-IL")} טבלאות (<Bytes n={st.pending_bytes} />)</span>
          {(st.running || st.done > 0) && (
            <span>
              הומרו {st.done.toLocaleString("he-IL")} · נחסכו <Bytes n={saved} />
              {st.running && st.current ? <> · עכשיו: <bdi dir="ltr">{st.current}</bdi></> : null}
            </span>
          )}
          {st.failed.length > 0 && (
            <span style={{ color: "var(--danger)" }} title={st.failed.map((f) => `${f.table}: ${f.error}`).join("\n")}>
              {st.failed.length} נכשלו
            </span>
          )}
          {st.skipped_busy.length > 0 && (
            <span className="text-muted" title={st.skipped_busy.join("\n")}>
              {st.skipped_busy.length} היו תפוסות (הריצו שוב)
            </span>
          )}
          <span style={{ flex: 1 }} />
          {st.running ? (
            <button type="button" className="text-sm" onClick={() => void act("stop")}
                    style={{ background: "none", border: "1px solid var(--border)", borderRadius: 4, padding: "0.2rem 0.7rem", cursor: "pointer" }}>
              עצור
            </button>
          ) : st.pending_tables > 0 ? (
            <button type="button" className="text-sm" onClick={() => void act("start")}
                    style={{ background: "var(--primary)", color: "#fff", border: "none", borderRadius: 4, padding: "0.25rem 0.8rem", cursor: "pointer", fontWeight: 600 }}>
              המר עכשיו
            </button>
          ) : null}
        </div>
      )}
    </div>
  );
}

interface IndexMigration {
  pending_tables: number;
  pending_bytes: number;
  running: boolean;
  current: string | null;
  done: number;
  failed: { table: string; error: string }[];
  skipped_busy: string[];
  finished_at: string | null;
}

/** Row validity (last_seen): every append table's dedup index becomes partial,
 *  UNIQUE … WHERE last_seen IS NULL, so a closed row that comes back opens a new
 *  interval. Built CONCURRENTLY, smallest first (app/services/index_migration.py).
 *  Until a table is converted, nothing closes rows in it. */
function IndexMigrationCard() {
  const [st, setSt] = useState<IndexMigration | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const load = useCallback(async () => {
    try { setSt(await call<IndexMigration>("/index-migration")); setErr(null); }
    catch (e) { setErr(e instanceof Error ? e.message : "טעינה נכשלה"); }
  }, []);
  useEffect(() => { void load(); }, [load]);
  useEffect(() => {
    if (!st?.running) return;
    const t = setInterval(() => void load(), 5000);
    return () => clearInterval(t);
  }, [st?.running, load]);

  const act = async (what: "start" | "stop") => {
    if (what === "start" && !window.confirm(
      `להמיר את אינדקס הכפילויות של ${st?.pending_tables.toLocaleString("he-IL")} טבלאות לאינדקס חלקי? ` +
      "האינדקס החדש נבנה במקביל (CONCURRENTLY) בלי לנעול כתיבה, ואז הישן נמחק. אפשר לעצור בכל רגע.")) return;
    try { await call(`/index-migration/${what}`, "POST"); await load(); }
    catch (e) { setErr(e instanceof Error ? e.message : "הפעולה נכשלה"); }
  };

  return (
    <div style={{ border: "1px solid var(--border)", borderRadius: 6, padding: "0.6rem 0.8rem", marginBottom: "0.9rem", background: "var(--bg-secondary)" }}>
      <div className="flex" style={{ gap: "0.6rem", alignItems: "baseline", flexWrap: "wrap" }}>
        <strong>אינדקס חלקי לתוקף שורה (last_seen)</strong>
        <span className="text-sm text-muted">
          כל עוד טבלה לא הומרה, שום poll לא סוגר בה שורות.
        </span>
      </div>
      {err && <div className="text-sm" style={{ color: "var(--danger)" }}>{err}</div>}
      {st && (
        <div className="flex text-sm" style={{ gap: "0.8rem", alignItems: "center", flexWrap: "wrap", marginTop: "0.4rem" }}>
          <span>ממתינות {st.pending_tables.toLocaleString("he-IL")} טבלאות (<Bytes n={st.pending_bytes} />)</span>
          {(st.running || st.done > 0) && (
            <span>
              הומרו {st.done.toLocaleString("he-IL")}
              {st.running && st.current ? <> · עכשיו: <bdi dir="ltr">{st.current}</bdi></> : null}
            </span>
          )}
          {st.failed.length > 0 && (
            <span style={{ color: "var(--danger)" }} title={st.failed.map((f) => `${f.table}: ${f.error}`).join("\n")}>
              {st.failed.length} נכשלו
            </span>
          )}
          {st.skipped_busy.length > 0 && (
            <span className="text-muted" title={st.skipped_busy.join("\n")}>
              {st.skipped_busy.length} היו תפוסות (הריצו שוב)
            </span>
          )}
          <span style={{ flex: 1 }} />
          {st.running ? (
            <button type="button" className="text-sm" onClick={() => void act("stop")}
                    style={{ background: "none", border: "1px solid var(--border)", borderRadius: 4, padding: "0.2rem 0.7rem", cursor: "pointer" }}>
              עצור
            </button>
          ) : st.pending_tables > 0 ? (
            <button type="button" className="text-sm" onClick={() => void act("start")}
                    style={{ background: "var(--primary)", color: "#fff", border: "none", borderRadius: 4, padding: "0.25rem 0.8rem", cursor: "pointer", fontWeight: 600 }}>
              המר עכשיו
            </button>
          ) : null}
        </div>
      )}
    </div>
  );
}

interface HistoryDataset {
  dataset_id: string;
  title: string | null;
  history_window_days: number | null;
  history_tier?: "hot" | "cold";
  parts: number;
  rows_in_r2: number;
  rows_deleted: number;
  bytes: number;
  tables: string[];
}

interface HistoryExport {
  enabled: boolean;
  available: boolean;
  min_rows: number;
  part_rows: number;
  hour_utc: number;
  job: {
    running: boolean;
    current: string | null;
    planned: number;
    done: number;
    exported_parts: number;
    exported_rows: number;
    deleted_rows: number;
    failed: { table: string | null; error: string }[];
    finished_at: string | null;
  };
  audit: { ran_at: string | null; checked: number; missing_count: number };
  totals: { parts: number; rows_in_r2: number; rows_only_in_r2: number; bytes: number };
  datasets: HistoryDataset[];
}

interface HistoryQueryStatus {
  enabled: boolean;
  available: boolean;
  reason: string | null;
  duckdb_version: string | null;
  extensions: { httpfs: string | null; postgres: string | null; checked_at: string | null };
  hot_mode: "attach" | "arrow" | null;
  active: number;
  waiting: number;
  queue: number;
  memory_mb: number;
  timeout_s: number;
  stats: { queries: number; ok: number; errors: number; timeouts: number; busy: number;
           last_error: string | null; last_elapsed_ms: number | null };
}

interface SmokeResult {
  table: string; parquet_rows?: number; hot_rows?: number; equal: boolean;
  parquet_ms?: number; hot_ms?: number; rss_mb_before: number | null; rss_mb_after: number | null;
}

/** The DuckDB history engine (app/services/history_query.py): is it up, how
 *  the hot tables are read, the gate and the counters, and an end-to-end smoke
 *  test that counts an existing Parquet mirror against its SQL table. */
function HistoryQueryCard() {
  const [st, setSt] = useState<HistoryQueryStatus | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [table, setTable] = useState("append_taxes_nadlan_full_f41fb496_fd06f5ae");
  const [smoke, setSmoke] = useState<SmokeResult | null>(null);
  const [busy, setBusy] = useState(false);
  const load = useCallback(async () => {
    try { setSt(await call<HistoryQueryStatus>("/history/query-status")); setErr(null); }
    catch (e) { setErr(e instanceof Error ? e.message : "טעינה נכשלה"); }
  }, []);
  useEffect(() => { void load(); }, [load]);

  const runSmoke = async () => {
    setBusy(true); setSmoke(null);
    try {
      setSmoke(await call<SmokeResult>(`/history/query-smoke?table=${encodeURIComponent(table)}`, "POST"));
      setErr(null);
    } catch (e) { setErr(e instanceof Error ? e.message : "הבדיקה נכשלה"); }
    finally { setBusy(false); void load(); }
  };

  const n = (v: number) => v.toLocaleString("he-IL");
  const hot = st?.hot_mode === "attach" ? "Postgres ישירות (postgres extension)"
    : st?.hot_mode === "arrow" ? "hot via arrow (טעינה לזיכרון, עם תקרה)" : "לא זמין";
  return (
    <div style={{ border: "1px solid var(--border)", borderRadius: 6, padding: "0.6rem 0.8rem", marginBottom: "0.9rem", background: "var(--bg-secondary)" }}>
      <div className="flex" style={{ gap: "0.6rem", alignItems: "baseline", flexWrap: "wrap" }}>
        <strong>מנוע ההיסטוריה (DuckDB)</strong>
        <span className="text-sm text-muted">
          המתג "כולל היסטוריה" ו-as_of ישן קוראים דרכו את טבלת ה-SQL יחד עם הקבצים שיש להם שורות רק ב-R2.
        </span>
      </div>
      {err && <div className="text-sm" style={{ color: "var(--danger)", whiteSpace: "pre-wrap" }}>{err}</div>}
      {st && (
        <div className="flex text-sm" style={{ gap: "0.8rem", alignItems: "center", flexWrap: "wrap", marginTop: "0.4rem" }}>
          <span style={{ color: st.available ? "var(--tint-info-fg)" : undefined }}>
            {st.available ? "זמין" : `לא זמין: ${st.reason === "disabled" ? "כבוי (HISTORY_QUERY_ENABLED)" : st.reason}`}
          </span>
          {st.enabled && (
            <>
              <span>httpfs: {st.extensions.httpfs ?? "—"} · postgres: {st.extensions.postgres ?? "—"}</span>
              <span>שורות חמות: {hot}</span>
            </>
          )}
          <span>רץ {st.active} · ממתינות {st.waiting}/{st.queue}</span>
          <span>
            {n(st.stats.queries)} שאילתות · {n(st.stats.ok)} הצליחו · {n(st.stats.timeouts)} חרגו מ-{st.timeout_s} ש' · {n(st.stats.busy)} נדחו (429)
          </span>
          {st.stats.last_error && (
            <span style={{ color: "var(--danger)" }} title={st.stats.last_error}>שגיאה אחרונה</span>
          )}
          <span style={{ flex: 1 }} />
          <input value={table} onChange={(e) => setTable(e.target.value)} dir="ltr"
                 aria-label="טבלה לבדיקת קצה-לקצה" style={{ fontSize: "0.8rem", width: 300 }} />
          <button type="button" className="text-sm" onClick={() => void runSmoke()} disabled={busy || !st.available}
                  style={{ background: "none", border: "1px solid var(--border)", borderRadius: 4, padding: "0.2rem 0.7rem", cursor: "pointer" }}>
            {busy ? "בודק…" : "בדיקת קצה-לקצה"}
          </button>
        </div>
      )}
      {smoke && (
        <div className="text-sm" style={{ marginTop: "0.4rem", color: smoke.equal ? undefined : "var(--danger)" }}>
          Parquet: {smoke.parquet_rows != null ? n(smoke.parquet_rows) : "—"} ({smoke.parquet_ms ?? "—"} ms) ·
          SQL: {smoke.hot_rows != null ? n(smoke.hot_rows) : "—"} ({smoke.hot_ms ?? "—"} ms) ·
          {smoke.equal ? " שווים" : " שונים"} · RSS {smoke.rss_mb_before ?? "—"} → {smoke.rss_mb_after ?? "—"} MB
        </div>
      )}
    </div>
  );
}

/** Closed rows exported to Parquet on R2 by closing month, verified against SQL
 *  (app/services/history_export.py). Rows are deleted from SQL only for a
 *  dataset with a history window; without one, the files are a second copy. */
function HistoryExportCard() {
  const [st, setSt] = useState<HistoryExport | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [open, setOpen] = useState(false);
  const load = useCallback(async () => {
    try { setSt(await call<HistoryExport>("/history")); setErr(null); }
    catch (e) { setErr(e instanceof Error ? e.message : "טעינה נכשלה"); }
  }, []);
  useEffect(() => { void load(); }, [load]);
  useEffect(() => {
    if (!st?.job.running) return;
    const t = setInterval(() => void load(), 5000);
    return () => clearInterval(t);
  }, [st?.job.running, load]);

  const act = async (what: "run" | "stop" | "audit") => {
    if (what === "run" && !window.confirm(
      "לייצא עכשיו שורות סגורות של חודשים שהסתיימו ל-Parquet ב-R2? " +
      "שורות יימחקו מ-SQL רק במאגרים שהוגדר להם חלון שמירה, ורק אחרי אימות הקובץ.")) return;
    try {
      await call(what === "audit" ? "/history/audit" : `/history/${what}`, what === "audit" ? "GET" : "POST");
      await load();
    } catch (e) { setErr(e instanceof Error ? e.message : "הפעולה נכשלה"); }
  };

  const n = (v: number) => v.toLocaleString("he-IL");
  return (
    <div style={{ border: "1px solid var(--border)", borderRadius: 6, padding: "0.6rem 0.8rem", marginBottom: "0.9rem", background: "var(--bg-secondary)" }}>
      <div className="flex" style={{ gap: "0.6rem", alignItems: "baseline", flexWrap: "wrap" }}>
        <strong>היסטוריה ב-R2</strong>
        <span className="text-sm text-muted">
          שורות סגורות של חודשים שהסתיימו נכתבות ל-Parquet ב-R2 ומאומתות מול SQL.
          מחיקה מ-SQL רק במאגר עם חלון שמירה.
        </span>
      </div>
      {err && <div className="text-sm" style={{ color: "var(--danger)" }}>{err}</div>}
      {st && (
        <>
          <div className="flex text-sm" style={{ gap: "0.8rem", alignItems: "center", flexWrap: "wrap", marginTop: "0.4rem" }}>
            <span>
              {st.enabled ? `ריצה יומית ב-${String(st.hour_utc).padStart(2, "0")}:30 UTC` : "הריצה היומית כבויה (HISTORY_EXPORT_ENABLED)"}
            </span>
            <span>
              {n(st.totals.parts)} קבצים · {n(st.totals.rows_in_r2)} שורות (<Bytes n={st.totals.bytes} />)
            </span>
            <span style={{ color: st.totals.rows_only_in_r2 > 0 ? "var(--tint-info-fg)" : undefined }}>
              {n(st.totals.rows_only_in_r2)} שורות רק ב-R2
            </span>
            {st.audit.ran_at && (
              <span style={{ color: st.audit.missing_count > 0 ? "var(--danger)" : undefined }}>
                בדיקה אחרונה: {n(st.audit.checked)} קבצים, {n(st.audit.missing_count)} חסרים
              </span>
            )}
            {(st.job.running || st.job.done > 0) && (
              <span>
                {n(st.job.done)}/{n(st.job.planned)} עבודות · יוצאו {n(st.job.exported_rows)} · נמחקו {n(st.job.deleted_rows)}
                {st.job.running && st.job.current ? <> · עכשיו: <bdi dir="ltr">{st.job.current}</bdi></> : null}
              </span>
            )}
            {st.job.failed.length > 0 && (
              <span style={{ color: "var(--danger)" }} title={st.job.failed.map((f) => `${f.table ?? ""}: ${f.error}`).join("\n")}>
                {st.job.failed.length} נכשלו
              </span>
            )}
            <span style={{ flex: 1 }} />
            {st.datasets.length > 0 && (
              <button type="button" className="text-sm" onClick={() => setOpen((o) => !o)}
                      style={{ background: "none", border: "1px solid var(--border)", borderRadius: 4, padding: "0.2rem 0.7rem", cursor: "pointer" }}>
                {open ? "הסתר מאגרים" : `לפי מאגר (${st.datasets.length})`}
              </button>
            )}
            <button type="button" className="text-sm" onClick={() => void act("audit")}
                    style={{ background: "none", border: "1px solid var(--border)", borderRadius: 4, padding: "0.2rem 0.7rem", cursor: "pointer" }}>
              בדוק קבצים
            </button>
            {st.job.running ? (
              <button type="button" className="text-sm" onClick={() => void act("stop")}
                      style={{ background: "none", border: "1px solid var(--border)", borderRadius: 4, padding: "0.2rem 0.7rem", cursor: "pointer" }}>
                עצור
              </button>
            ) : st.available ? (
              <button type="button" className="text-sm" onClick={() => void act("run")}
                      style={{ background: "var(--primary)", color: "#fff", border: "none", borderRadius: 4, padding: "0.25rem 0.8rem", cursor: "pointer", fontWeight: 600 }}>
                ייצא עכשיו
              </button>
            ) : (
              <span className="text-muted">לא זמין (pyarrow או R2 חסרים)</span>
            )}
          </div>
          {open && st.datasets.length > 0 && (
            <table className="text-sm" style={{ width: "100%", borderCollapse: "collapse", marginTop: "0.5rem" }}>
              <thead>
                <tr style={{ textAlign: "start", borderBottom: "1px solid var(--border)" }}>
                  <th style={cell}>מאגר</th>
                  <th style={num}>חלון</th>
                  <th style={num}>קבצים</th>
                  <th style={num}>שורות ב-R2</th>
                  <th style={num}>רק ב-R2</th>
                  <th style={num}>נפח</th>
                </tr>
              </thead>
              <tbody>
                {st.datasets.map((d) => (
                  <tr key={d.dataset_id} style={{ borderBottom: "1px solid var(--border)" }}>
                    <td style={cell}>
                      <Link to={`/versions/${d.dataset_id}`}>{d.title || d.dataset_id.slice(0, 8)}</Link>
                    </td>
                    <td style={num}>
                      {d.history_tier === "cold" ? "קר (R2)" : d.history_window_days ? `${d.history_window_days} ימים` : "ייצוא בלבד"}
                    </td>
                    <td style={num}>{n(d.parts)}</td>
                    <td style={num}>{n(d.rows_in_r2)}</td>
                    <td style={num}>{n(d.rows_deleted)}</td>
                    <td style={num}><Bytes n={d.bytes} /></td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </>
      )}
    </div>
  );
}

interface ReopenTable {
  table: string; dataset_id: string; closed: number; duplicates: number;
  deleted: number; reopened: number; remaining: number | null; dry_run: boolean;
}

interface ReopenState {
  running: boolean;
  dry_run: boolean | null;
  started_at: string | null;
  finished_at: string | null;
  current: string | null;
  target: { source: string | null; dataset_id: string | null } | null;
  tables: ReopenTable[];
  refused: { dataset_id: string; title: string | null; reason: string }[];
  failed: { table: string | null; error: string }[];
  totals: { closed?: number; duplicates?: number; deleted?: number; reopened?: number; tables?: number };
}

const btn: React.CSSProperties = { background: "none", border: "1px solid var(--border)", borderRadius: 4, padding: "0.2rem 0.7rem", cursor: "pointer" };
const primaryBtn: React.CSSProperties = { background: "var(--primary)", color: "#fff", border: "none", borderRadius: 4, padding: "0.25rem 0.8rem", cursor: "pointer", fontWeight: 600 };
const box: React.CSSProperties = { border: "1px solid var(--border)", borderRadius: 6, padding: "0.6rem 0.8rem", marginBottom: "0.9rem", background: "var(--bg-secondary)" };

/** Wrong closures (docs/history-phase4-plan.md section 0): a source that does
 *  not close rows by its effective append_closes_rows (the prices feeds) got
 *  closed rows anyway. A dry run counts; a real run reopens them. */
function ClosuresReopenCard() {
  const [st, setSt] = useState<ReopenState | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [source, setSource] = useState("prices");
  const load = useCallback(async () => {
    try { setSt(await call<ReopenState>("/closures/reopen")); setErr(null); }
    catch (e) { setErr(e instanceof Error ? e.message : "טעינה נכשלה"); }
  }, []);
  useEffect(() => { void load(); }, [load]);
  useEffect(() => {
    if (!st?.running) return;
    const t = setInterval(() => void load(), 4000);
    return () => clearInterval(t);
  }, [st?.running, load]);

  const run = async (dry: boolean) => {
    if (!dry && !window.confirm(
      `לפתוח מחדש את כל השורות הסגורות של המקור "${source}"? עותקים כפולים (אותו row_hash) ` +
      "יימחקו, והשורה עם first_seen המוקדם תישאר. הריצו קודם בדיקה יבשה.")) return;
    try {
      await call(`/closures/reopen?source=${encodeURIComponent(source)}&dry_run=${dry ? 1 : 0}`, "POST");
      await load();
    } catch (e) { setErr(e instanceof Error ? e.message : "הפעולה נכשלה"); }
  };

  const n = (v: number | undefined | null) => (v ?? 0).toLocaleString("he-IL");
  const t = st?.totals ?? {};
  return (
    <div style={box}>
      <div className="flex" style={{ gap: "0.6rem", alignItems: "baseline", flexWrap: "wrap" }}>
        <strong>סגירות שגויות</strong>
        <span className="text-sm text-muted">
          מקור שמפרסם רק שינויים (append_closes_rows: false) לא אמור לסגור שורות. כאן סופרים ופותחים מחדש.
        </span>
      </div>
      {err && <div className="text-sm" style={{ color: "var(--danger)", whiteSpace: "pre-wrap" }}>{err}</div>}
      <div className="flex text-sm" style={{ gap: "0.8rem", alignItems: "center", flexWrap: "wrap", marginTop: "0.4rem" }}>
        <label>מקור: <input value={source} onChange={(e) => setSource(e.target.value)} dir="ltr" style={{ width: 120 }} /></label>
        <button type="button" className="text-sm" style={btn} disabled={st?.running} onClick={() => void run(true)}>בדיקה יבשה</button>
        <button type="button" className="text-sm" style={primaryBtn} disabled={st?.running} onClick={() => void run(false)}>פתח מחדש</button>
        {st && st.started_at && (
          <span>
            {st.running ? "רץ" : "הסתיים"} ({st.dry_run ? "יבשה" : "אמיתית"}) · {n(t.tables)} טבלאות ·
            {" "}{n(t.closed)} סגורות · {n(t.duplicates)} כפולות
            {!st.dry_run && <> · נפתחו {n(t.reopened)} · נמחקו {n(t.deleted)}</>}
            {st.running && st.current ? <> · עכשיו: <bdi dir="ltr">{st.current}</bdi></> : null}
          </span>
        )}
        {st && st.refused.length > 0 && (
          <span className="text-muted" title={st.refused.map((r) => `${r.title ?? r.dataset_id}: ${r.reason}`).join("\n")}>
            {st.refused.length} מאגרים סורבו
          </span>
        )}
        {st && st.failed.length > 0 && (
          <span style={{ color: "var(--danger)" }} title={st.failed.map((f) => `${f.table ?? ""}: ${f.error}`).join("\n")}>
            {st.failed.length} נכשלו
          </span>
        )}
      </div>
    </div>
  );
}

interface ColdCandidate {
  dataset_id: string; title: string; status: string; tables: number; rows: number; bytes: number;
  days_since_version: number | null; days_since_access: number | null; may_move_now: boolean;
}

interface WindowRec {
  dataset_id: string; title: string; history_window_days: number | null;
  closed_over_90_days: number; est_bytes: number;
}

interface TierJob {
  running: boolean; op: string | null; dataset_id: string | null; current: string | null;
  planned: number; done: number; rows: number; failed: { table: string | null; error: string }[];
}

interface Candidates {
  stale_days: number; quiet_days: number; window_hint_days: number;
  candidates: ColdCandidate[]; windows: WindowRec[]; job: TierJob;
}

/** Cold-tier candidates and window recommendations (phase 4). Shows and
 *  applies on a click; never decides on its own. */
function ColdTierCard() {
  const [st, setSt] = useState<Candidates | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [picked, setPicked] = useState<Record<string, boolean>>({});
  const [days, setDays] = useState(30);
  const load = useCallback(async () => {
    setLoading(true);
    try { setSt(await call<Candidates>("/history/candidates")); setErr(null); }
    catch (e) { setErr(e instanceof Error ? e.message : "טעינה נכשלה"); }
    finally { setLoading(false); }
  }, []);
  useEffect(() => {
    if (!st?.job.running) return;
    const t = setInterval(() => void load(), 5000);
    return () => clearInterval(t);
  }, [st?.job.running, load]);

  const moveCold = async (c: ColdCandidate) => {
    let confirmRehydrate = false;
    if (!c.may_move_now) {
      if (!window.confirm(`"${c.title}" פעיל ונדגם לפחות פעם בחודש. להעביר לקר ולהחזיר ל-SQL אוטומטית לפני ה-poll הבא?`)) return;
      confirmRehydrate = true;
    } else if (!window.confirm(`להעביר את "${c.title}" לשכבה הקרה? כל השורות ייכתבו ל-Parquet ב-R2, יאומתו, וייִמחקו מ-SQL.`)) return;
    try {
      await call(`/history/cold/${c.dataset_id}${confirmRehydrate ? "?confirm_rehydrate=true" : ""}`, "POST");
      await load();
    } catch (e) { setErr(e instanceof Error ? e.message : "הפעולה נכשלה"); }
  };

  const applyWindows = async (ids: string[]) => {
    if (!ids.length) return;
    if (!window.confirm(`להחיל חלון של ${days} ימים על ${ids.length} מאגרים?`)) return;
    try {
      await call("/history/windows", "PATCH", { items: ids.map((id) => ({ dataset_id: id, days })) });
      setPicked({});
      await load();
    } catch (e) { setErr(e instanceof Error ? e.message : "הפעולה נכשלה"); }
  };

  const n = (v: number) => v.toLocaleString("he-IL");
  const chosen = Object.keys(picked).filter((k) => picked[k]);
  return (
    <div style={box}>
      <div className="flex" style={{ gap: "0.6rem", alignItems: "baseline", flexWrap: "wrap" }}>
        <strong>מועמדים לקר והמלצות חלון</strong>
        <span className="text-sm text-muted">
          מאגר קר עובר כולו ל-Parquet ב-R2 ונקרא דרך מנוע ההיסטוריה; חלון מוציא שורות סגורות ישנות.
        </span>
        <span style={{ flex: 1 }} />
        <button type="button" className="text-sm" style={btn} onClick={() => void load()} disabled={loading}>
          {loading ? "מחשב…" : st ? "רענן" : "חשב"}
        </button>
      </div>
      {err && <div className="text-sm" style={{ color: "var(--danger)", whiteSpace: "pre-wrap" }}>{err}</div>}
      {st && (st.job.running || st.job.done > 0 || st.job.failed.length > 0) && (
        <div className="text-sm" style={{ marginTop: "0.4rem" }}>
          {st.job.op === "rehydrate" ? "החזרה ל-SQL" : "העברה לקר"}: {n(st.job.done)}/{n(st.job.planned)} טבלאות · {n(st.job.rows)} שורות
          {st.job.running && st.job.current ? <> · עכשיו: <bdi dir="ltr">{st.job.current}</bdi></> : null}
          {st.job.failed.length > 0 && (
            <span style={{ color: "var(--danger)" }} title={st.job.failed.map((f) => `${f.table ?? ""}: ${f.error}`).join("\n")}>
              {" "}· {st.job.failed.length} נכשלו
            </span>
          )}
        </div>
      )}
      {st && (
        <>
          <div className="text-sm" style={{ marginTop: "0.5rem", fontWeight: 600 }}>
            מועמדים לקר (לא התעדכנו {st.stale_days} יום ולא נשאלו ב-API {st.quiet_days} יום): {st.candidates.length}
          </div>
          {st.candidates.length > 0 && (
            <table className="text-sm" style={{ width: "100%", borderCollapse: "collapse", marginTop: "0.3rem" }}>
              <thead>
                <tr style={{ textAlign: "start", borderBottom: "1px solid var(--border)" }}>
                  <th style={cell}>מאגר</th><th style={num}>טבלאות</th><th style={num}>שורות</th>
                  <th style={num}>נפח</th><th style={num}>גרסה אחרונה</th><th style={num}>גישה אחרונה</th><th style={cell} />
                </tr>
              </thead>
              <tbody>
                {st.candidates.map((c) => (
                  <tr key={c.dataset_id} style={{ borderBottom: "1px solid var(--border)" }}>
                    <td style={cell}><Link to={`/versions/${c.dataset_id}`}>{c.title}</Link> <span className="text-muted">({c.status})</span></td>
                    <td style={num}>{n(c.tables)}</td>
                    <td style={num}>{n(c.rows)}</td>
                    <td style={num}><Bytes n={c.bytes} /></td>
                    <td style={num}>{c.days_since_version == null ? "—" : `לפני ${n(c.days_since_version)} ימים`}</td>
                    <td style={num}>{c.days_since_access == null ? "אף פעם" : `לפני ${n(c.days_since_access)} ימים`}</td>
                    <td style={cell}>
                      <button type="button" className="text-sm" style={btn} disabled={st.job.running} onClick={() => void moveCold(c)}>
                        העבר לקר
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
          <div className="flex text-sm" style={{ gap: "0.6rem", alignItems: "center", flexWrap: "wrap", marginTop: "0.7rem" }}>
            <strong>המלצות חלון (שורות סגורות מעל {st.window_hint_days} יום)</strong>
            <label>חלון: <input type="number" min={30} value={days} onChange={(e) => setDays(Number(e.target.value) || 30)} style={{ width: 70 }} /> ימים</label>
            <button type="button" className="text-sm" style={primaryBtn} disabled={!chosen.length || days < 30} onClick={() => void applyWindows(chosen)}>
              החל על המסומנים ({chosen.length})
            </button>
          </div>
          {st.windows.length > 0 && (
            <table className="text-sm" style={{ width: "100%", borderCollapse: "collapse", marginTop: "0.3rem" }}>
              <thead>
                <tr style={{ textAlign: "start", borderBottom: "1px solid var(--border)" }}>
                  <th style={cell} /><th style={cell}>מאגר</th><th style={num}>חלון היום</th>
                  <th style={num}>סגורות מעל {st.window_hint_days} יום</th><th style={num}>חיסכון משוער</th><th style={cell} />
                </tr>
              </thead>
              <tbody>
                {st.windows.map((w) => (
                  <tr key={w.dataset_id} style={{ borderBottom: "1px solid var(--border)" }}>
                    <td style={cell}>
                      <input type="checkbox" checked={!!picked[w.dataset_id]} aria-label="סמן"
                             onChange={(e) => setPicked((p) => ({ ...p, [w.dataset_id]: e.target.checked }))} />
                    </td>
                    <td style={cell}><Link to={`/versions/${w.dataset_id}`}>{w.title}</Link></td>
                    <td style={num}>{w.history_window_days ? `${w.history_window_days} ימים` : "ללא"}</td>
                    <td style={num}>{n(w.closed_over_90_days)}</td>
                    <td style={num}><Bytes n={w.est_bytes} /></td>
                    <td style={cell}>
                      <button type="button" className="text-sm" style={btn} onClick={() => void applyWindows([w.dataset_id])}>החל</button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </>
      )}
    </div>
  );
}

function pct(part: number, whole: number): string {
  if (!whole) return "0%";
  const p = (part / whole) * 100;
  return p >= 10 ? `${p.toFixed(0)}%` : p >= 0.1 ? `${p.toFixed(1)}%` : "<0.1%";
}

const cell: React.CSSProperties = { padding: "0.3rem 0.45rem", verticalAlign: "top" };
const num: React.CSSProperties = { ...cell, whiteSpace: "nowrap", fontVariantNumeric: "tabular-nums" };

export default function StorageUsagePanel() {
  const [data, setData] = useState<StorageReport | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [groupBy, setGroupBy] = useState<GroupBy>("project");
  const [filters, setFilters] = useState<{ by: GroupBy; key: string; label: string }[]>([]);
  const [q, setQ] = useState("");
  const [shownGroups, setShownGroups] = useState(25);
  const [shownTables, setShownTables] = useState(50);

  const load = useCallback(async (refresh = false) => {
    setLoading(true);
    setError(null);
    try {
      setData(await fetchReport(refresh));
    } catch (e) {
      setError(e instanceof Error ? e.message : "טעינה נכשלה");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { void load(); }, [load]);

  const [dropping, setDropping] = useState(false);
  const [dropNote, setDropNote] = useState<string | null>(null);
  // The server re-derives the orphan list when the POST lands, so the tile's
  // number is a preview; a dataset re-added meanwhile keeps its table.
  const dropOrphans = async (bytes: number, count: number) => {
    if (!window.confirm(
      `למחוק ${count.toLocaleString("he-IL")} טבלאות יתומות (${(bytes / 1e6).toFixed(0)} MB)? ` +
      "אלה טבלאות של מאגרים שנמחקו; שום מאגר לא קורא אותן. הפעולה בלתי הפיכה.")) return;
    setDropping(true);
    setDropNote(null);
    try {
      const r = await call<{ dropped: string[]; failed: string[]; count: number; freed_bytes: number }>("/orphans/drop", "POST");
      setDropNote(`נמחקו ${r.dropped.length.toLocaleString("he-IL")} מתוך ${r.count.toLocaleString("he-IL")} טבלאות, ` +
        `${(r.freed_bytes / 1e6).toFixed(0)} MB${r.failed.length ? `; נכשלו ${r.failed.length}: ${r.failed[0]}` : ""}`);
      await load(true);
    } catch (e) {
      setDropNote(e instanceof Error ? e.message : "המחיקה נכשלה");
    } finally {
      setDropping(false);
    }
  };

  const keyOf = useCallback((r: Row, by: GroupBy): string => {
    const ds = r[8] ? data?.datasets[r[8]] : undefined;
    switch (by) {
      case "project": return r[7];
      case "schema": return r[0];
      case "dataset": return r[8] ?? NO_DATASET;
      case "status": return ds?.status ?? NO_DATASET;
      case "source": return ds?.source ?? `${NO_DATASET}:${r[7]}`;
    }
  }, [data]);

  const labelOf = useCallback((by: GroupBy, key: string): string => {
    if (!data) return key;
    if (by === "project") return data.projects[key] ?? key;
    if (by === "dataset") return key === NO_DATASET ? "ללא מאגר (טבלאות פרויקט ומערכת)" : (data.datasets[key]?.title ?? key);
    if (by === "status") return key === NO_DATASET ? "ללא מאגר" : (STATUS_LABELS[key] ?? key);
    if (by === "source" && key.startsWith(NO_DATASET)) {
      const p = key.slice(NO_DATASET.length + 1);
      return `ללא מאגר · ${data.projects[p] ?? p}`;
    }
    return key;
  }, [data]);

  // Rows left after the drill-down filters (not the free-text search).
  const scoped = useMemo(() => {
    if (!data) return [];
    return data.tables.filter((r) => filters.every((f) => keyOf(r, f.by) === f.key));
  }, [data, filters, keyOf]);

  const scopedTotal = useMemo(() => scoped.reduce((s, r) => s + r[2], 0), [scoped]);
  const allTotal = useMemo(() => (data ? data.tables.reduce((s, r) => s + r[2], 0) : 0), [data]);
  const orphanBytes = useMemo(
    () => (data ? data.tables.filter((r) => r[7] === "orphans").reduce((s, r) => s + r[2], 0) : 0),
    [data],
  );
  const orphanCount = useMemo(() => (data ? data.tables.filter((r) => r[7] === "orphans").length : 0), [data]);

  const groups = useMemo(() => {
    const m = new Map<string, { key: string; total: number; heap: number; indexes: number; toast: number; tables: number; rows: number }>();
    for (const r of scoped) {
      const k = keyOf(r, groupBy);
      const g = m.get(k) ?? { key: k, total: 0, heap: 0, indexes: 0, toast: 0, tables: 0, rows: 0 };
      g.total += r[2]; g.heap += r[3]; g.indexes += r[4]; g.toast += r[5];
      g.tables += 1; g.rows += r[6] ?? 0;
      m.set(k, g);
    }
    return [...m.values()].sort((a, b) => b.total - a.total);
  }, [scoped, groupBy, keyOf]);

  const tables = useMemo(() => {
    const needle = q.trim().toLowerCase();
    if (!needle || !data) return scoped;
    return scoped.filter((r) => {
      const ds = r[8] ? data.datasets[r[8]] : undefined;
      return `${r[0]}.${r[1]} ${ds?.title ?? ""} ${ds?.source ?? ""} ${ds?.org ?? ""}`
        .toLowerCase().includes(needle);
    });
  }, [scoped, q, data]);

  const drill = (key: string) => {
    const label = labelOf(groupBy, key);
    setFilters((f) => [...f, { by: groupBy, key, label }]);
    const next = NEXT[groupBy];
    if (next && !filters.some((f) => f.by === next)) setGroupBy(next);
    setShownGroups(25);
    setShownTables(50);
  };

  const maxGroup = groups[0]?.total || 1;
  const usedGroupBys = new Set(filters.map((f) => f.by));

  return (
    <section className="card mb-2">
      <p className="text-sm text-muted" style={{ marginTop: 0, lineHeight: 1.7 }}>
        כל טבלה במסד, בגודלה המלא (נתונים + אינדקסים + TOAST), משויכת לפרויקט, למקור
        ולמאגר שהיא שייכת לו. לחיצה על שורה מצמצמת אליה ויורדת רמה אחת
        (פרויקט ← מקור ← מאגר ← טבלאות). מספרי השורות הם הערכת Postgres (reltuples).
      </p>

      {error && <div className="text-sm" style={{ color: "var(--danger)", marginBottom: "0.5rem" }}>{error}</div>}

      <HashCompactionCard />
      <IndexMigrationCard />
      <ClosuresReopenCard />
      <HistoryExportCard />
      <ColdTierCard />
      <HistoryQueryCard />

      {data && (
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(150px, 1fr))", gap: "0.6rem", marginBottom: "0.9rem" }}>
          <Stat label="גודל המסד" value={<Bytes n={data.database_bytes} />} />
          <Stat label="סך הטבלאות" value={<Bytes n={allTotal} />} sub={`${data.tables.length.toLocaleString("he-IL")} טבלאות`} />
          <Stat label="מאגרים עם טבלאות" value={Object.keys(data.datasets).length.toLocaleString("he-IL")} />
          <Stat
            label="טבלאות יתומות"
            value={<Bytes n={orphanBytes} />}
            sub="מאגר שנמחק, אף אחד לא קורא"
            warn={orphanBytes > 0}
            onClick={orphanBytes > 0 ? () => { setFilters([{ by: "project", key: "orphans", label: data.projects.orphans }]); setGroupBy("dataset"); } : undefined}
          />
        </div>
      )}
      {data && (orphanCount > 0 || dropNote) && (
        <div className="flex text-sm" style={{ gap: "0.6rem", alignItems: "center", flexWrap: "wrap", marginBottom: "0.9rem" }}>
          {orphanCount > 0 && (
            <button type="button" className="text-sm" disabled={dropping} onClick={() => void dropOrphans(orphanBytes, orphanCount)}>
              {dropping ? "מוחק…" : `מחק ${orphanCount.toLocaleString("he-IL")} טבלאות יתומות`}
            </button>
          )}
          {dropNote && <span className="text-muted">{dropNote}</span>}
        </div>
      )}

      <div className="flex" style={{ gap: "0.5rem", flexWrap: "wrap", alignItems: "center", marginBottom: "0.6rem" }}>
        <span className="text-sm text-muted">פילוח לפי:</span>
        <div role="group" aria-label="פילוח לפי" className="flex" style={{ gap: 0, flexWrap: "wrap" }}>
          {GROUP_LABELS.map((g) => (
            <button
              key={g.id}
              type="button"
              aria-pressed={groupBy === g.id}
              disabled={usedGroupBys.has(g.id)}
              onClick={() => { setGroupBy(g.id); setShownGroups(25); }}
              className="text-sm"
              style={{
                padding: "0.25rem 0.7rem", cursor: "pointer",
                border: "1px solid var(--border)", marginInlineStart: -1,
                background: groupBy === g.id ? "var(--primary)" : "var(--bg)",
                color: groupBy === g.id ? "#fff" : "inherit",
                opacity: usedGroupBys.has(g.id) ? 0.45 : 1,
              }}
            >
              {g.label}
            </button>
          ))}
        </div>
        <span style={{ flex: 1 }} />
        {data && (
          <span className="text-sm text-muted">
            נכון ל-{new Date(data.generated_at).toLocaleString("he-IL")}
            {data.cached ? " (מטמון)" : ""}
          </span>
        )}
        <button
          type="button"
          onClick={() => void load(true)}
          disabled={loading}
          className="text-sm"
          style={{ background: "none", border: "1px solid var(--border)", borderRadius: 4, padding: "0.25rem 0.8rem", cursor: "pointer" }}
        >
          {loading ? "טוען…" : "רענן"}
        </button>
      </div>

      {filters.length > 0 && (
        <div className="flex" style={{ gap: "0.4rem", flexWrap: "wrap", alignItems: "center", marginBottom: "0.6rem" }}>
          <button type="button" className="text-sm" onClick={() => { setFilters([]); setGroupBy("project"); }}
                  style={{ background: "none", border: "none", color: "var(--primary)", cursor: "pointer", padding: 0 }}>
            הכול
          </button>
          {filters.map((f, i) => (
            <span key={`${f.by}:${f.key}`} className="text-sm" style={{
              border: "1px solid var(--border)", borderRadius: 999, padding: "0.1rem 0.3rem 0.1rem 0.6rem",
              background: "var(--bg-muted)",
            }}>
              ‹ {GROUP_LABELS.find((g) => g.id === f.by)?.label}: {f.label}
              <button type="button" aria-label={`הסר סינון ${f.label}`}
                      onClick={() => setFilters((all) => all.slice(0, i))}
                      style={{ background: "none", border: "none", cursor: "pointer", marginInlineStart: 4 }}>
                ×
              </button>
            </span>
          ))}
          <span className="text-sm text-muted">
            <Bytes n={scopedTotal} /> · <bdi dir="ltr">{pct(scopedTotal, allTotal)}</bdi> מהטבלאות
          </span>
        </div>
      )}

      {loading && !data && <div className="text-sm text-muted">מחשב את גודל כל הטבלאות…</div>}

      {data && (
        <div tabIndex={0} role="region" aria-label="פילוח הנפח" className="scroll-region" style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", fontSize: "0.85rem", borderCollapse: "collapse" }}>
            <thead>
              <tr style={{ color: "var(--text-muted)" }}>
                <th scope="col" style={{ ...cell, textAlign: "start" }}>{GROUP_LABELS.find((g) => g.id === groupBy)?.label}</th>
                <th scope="col" style={{ ...cell, textAlign: "start", minWidth: 180 }}>נפח</th>
                <th scope="col" style={{ ...cell, textAlign: "start" }}>נתונים</th>
                <th scope="col" style={{ ...cell, textAlign: "start" }}>אינדקסים</th>
                <th scope="col" style={{ ...cell, textAlign: "start" }}>TOAST</th>
                <th scope="col" style={{ ...cell, textAlign: "start" }}>טבלאות</th>
                <th scope="col" style={{ ...cell, textAlign: "start" }}>שורות (הערכה)</th>
              </tr>
            </thead>
            <tbody>
              {groups.slice(0, shownGroups).map((g) => {
                const ds = groupBy === "dataset" ? data.datasets[g.key] : undefined;
                return (
                  <tr key={g.key} style={{ borderTop: "1px solid var(--border)" }}>
                    <td style={cell}>
                      <button type="button" onClick={() => drill(g.key)}
                              style={{ background: "none", border: "none", padding: 0, cursor: "pointer", color: "var(--primary)", textAlign: "start", font: "inherit" }}>
                        {labelOf(groupBy, g.key)}
                      </button>
                      {ds && (
                        <div className="text-muted" style={{ fontSize: "0.75rem" }}>
                          {ds.source} · {STATUS_LABELS[ds.status] ?? ds.status}
                          {ds.org ? ` · ${ds.org}` : ""} · <Link to={`/versions/${ds.id}`}>דף המאגר</Link>
                        </div>
                      )}
                    </td>
                    <td style={cell}>
                      <div style={{ display: "flex", alignItems: "center", gap: "0.45rem" }}>
                        <div aria-hidden style={{ flex: 1, minWidth: 60, height: 10, background: "var(--bg-muted)", borderRadius: 3 }}>
                          <div style={{ width: `${Math.max((g.total / maxGroup) * 100, 0.5)}%`, height: "100%", background: "var(--fill-brand)", borderRadius: 3 }} />
                        </div>
                        <span style={{ whiteSpace: "nowrap", fontVariantNumeric: "tabular-nums" }}>
                          <Bytes n={g.total} /> <span className="text-muted">(<bdi dir="ltr">{pct(g.total, scopedTotal)}</bdi>)</span>
                        </span>
                      </div>
                    </td>
                    <td style={num}><Bytes n={g.heap} /></td>
                    <td style={num}><Bytes n={g.indexes} /></td>
                    <td style={num}><Bytes n={g.toast} /></td>
                    <td style={num}>{g.tables.toLocaleString("he-IL")}</td>
                    <td style={num}>{g.rows ? g.rows.toLocaleString("he-IL") : "—"}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
          {groups.length > shownGroups && (
            <button type="button" className="text-sm mt-1" onClick={() => setShownGroups((n) => n + 50)}
                    style={{ background: "none", border: "1px solid var(--border)", borderRadius: 4, padding: "0.2rem 0.7rem", cursor: "pointer" }}>
              הצג עוד ({(groups.length - shownGroups).toLocaleString("he-IL")} נוספים)
            </button>
          )}
        </div>
      )}

      {data && (
        <>
          <div className="flex" style={{ gap: "0.6rem", alignItems: "center", flexWrap: "wrap", marginTop: "1.2rem" }}>
            <h3 style={{ fontSize: "1rem", margin: 0 }}>
              הטבלאות {filters.length ? "בסינון" : "הגדולות"} ({tables.length.toLocaleString("he-IL")})
            </h3>
            <input
              type="search"
              value={q}
              onChange={(e) => { setQ(e.target.value); setShownTables(50); }}
              placeholder="חיפוש לפי טבלה, מאגר, מקור או ארגון"
              aria-label="חיפוש טבלאות"
              className="text-sm"
              style={{ flex: "1 1 220px", maxWidth: 360, padding: "0.25rem 0.5rem", border: "1px solid var(--border)", borderRadius: 4 }}
            />
          </div>
          <div tabIndex={0} role="region" aria-label="טבלאות" className="scroll-region" style={{ overflowX: "auto", marginTop: "0.5rem" }}>
            <table style={{ width: "100%", fontSize: "0.82rem", borderCollapse: "collapse" }}>
              <thead>
                <tr style={{ color: "var(--text-muted)" }}>
                  <th scope="col" style={{ ...cell, textAlign: "start" }}>טבלה</th>
                  <th scope="col" style={{ ...cell, textAlign: "start" }}>פרויקט / מקור</th>
                  <th scope="col" style={{ ...cell, textAlign: "start" }}>סה"כ</th>
                  <th scope="col" style={{ ...cell, textAlign: "start" }}>נתונים</th>
                  <th scope="col" style={{ ...cell, textAlign: "start" }}>אינדקסים</th>
                  <th scope="col" style={{ ...cell, textAlign: "start" }}>TOAST</th>
                  <th scope="col" style={{ ...cell, textAlign: "start" }}>שורות</th>
                </tr>
              </thead>
              <tbody>
                {tables.slice(0, shownTables).map((r) => {
                  const ds = r[8] ? data.datasets[r[8]] : undefined;
                  return (
                    <tr key={`${r[0]}.${r[1]}`} style={{ borderTop: "1px solid var(--border)" }}>
                      <td style={cell}>
                        {ds && <div><Link to={`/versions/${ds.id}`}>{ds.title}</Link></div>}
                        <div className={ds ? "text-muted" : undefined} dir="ltr"
                             style={{ fontSize: ds ? "0.72rem" : undefined, overflowWrap: "anywhere", textAlign: "end" }}>
                          {r[0]}.{r[1]}
                        </div>
                      </td>
                      <td style={cell}>
                        <div>{data.projects[r[7]] ?? r[7]}</div>
                        {ds && <div className="text-muted" style={{ fontSize: "0.72rem" }}>{ds.source}</div>}
                      </td>
                      <td style={num}><Bytes n={r[2]} /></td>
                      <td style={num}><Bytes n={r[3]} /></td>
                      <td style={num}><Bytes n={r[4]} /></td>
                      <td style={num}><Bytes n={r[5]} /></td>
                      <td style={num}>{r[6] != null ? r[6].toLocaleString("he-IL") : "—"}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
          {tables.length > shownTables && (
            <button type="button" className="text-sm mt-1" onClick={() => setShownTables((n) => n + 100)}
                    style={{ background: "none", border: "1px solid var(--border)", borderRadius: 4, padding: "0.2rem 0.7rem", cursor: "pointer" }}>
              הצג עוד ({(tables.length - shownTables).toLocaleString("he-IL")} נוספות)
            </button>
          )}
        </>
      )}
    </section>
  );
}

function Stat({ label, value, sub, warn, onClick }: {
  label: string; value: React.ReactNode; sub?: string; warn?: boolean; onClick?: () => void;
}) {
  const body = (
    <>
      <div className="text-sm text-muted">{label}</div>
      <div style={{ fontSize: "1.35rem", fontWeight: 700, fontVariantNumeric: "tabular-nums", color: warn ? "var(--warning)" : undefined }}>
        {value}
      </div>
      {sub && <div className="text-muted" style={{ fontSize: "0.75rem" }}>{sub}</div>}
    </>
  );
  const style: React.CSSProperties = {
    border: "1px solid var(--border)", borderRadius: 6, padding: "0.5rem 0.7rem",
    background: "var(--bg-secondary)", textAlign: "start",
  };
  return onClick
    ? <button type="button" onClick={onClick} style={{ ...style, cursor: "pointer", font: "inherit", color: "inherit" }}>{body}</button>
    : <div style={style}>{body}</div>;
}
