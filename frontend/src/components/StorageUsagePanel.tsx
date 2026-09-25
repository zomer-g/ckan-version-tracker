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

async function call<T>(path: string, method: "GET" | "POST" = "GET"): Promise<T> {
  const token = localStorage.getItem("token");
  const resp = await fetch(`/api/admin/storage${path}`, {
    method,
    headers: token ? { Authorization: `Bearer ${token}` } : {},
  });
  if (!resp.ok) {
    let detail = `שגיאת שרת (${resp.status})`;
    try { detail = (await resp.json())?.detail || detail; } catch { /* not JSON */ }
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

interface OffloadRun { state: "running" | "done" | "failed"; done: number; total: number | null; current: string | null; error: string | null }

/** Move one dataset's SQL tables to CSV files on R2 (app/services/sql_offload.py). */
function OffloadButton({ datasetId, title }: { datasetId: string; title: string }) {
  const [run, setRun] = useState<OffloadRun | null>(null);
  const [busy, setBusy] = useState(false);
  useEffect(() => {
    if (run?.state !== "running") return;
    const t = setInterval(async () => {
      try { setRun((await call<{ run: OffloadRun | null }>(`/offload/${datasetId}`)).run); } catch { /* keep polling */ }
    }, 4000);
    return () => clearInterval(t);
  }, [run?.state, datasetId]);

  const start = async () => {
    setBusy(true);
    try {
      const plan = await call<{ tables: unknown[]; bytes: number; est_rows: number }>(`/offload/${datasetId}`);
      const ok = window.confirm(
        `להעביר את "${title}" מ-SQL לקבצים?\n\n` +
        `${plan.tables.length} טבלאות, כ-${plan.est_rows.toLocaleString("he-IL")} שורות, ${formatBytes(plan.bytes)}.\n` +
        "כל טבלה תיוצא ל-CSV ב-R2 ותיבדק, הגרסאות יפנו לקבצים, תוכנית האחסון תעבור ל-R2, " +
        "ורק אז הטבלאות יימחקו. המאגר לא יהיה עוד שאילתי ב-/data.");
      if (!ok) return;
      await call(`/offload/${datasetId}`, "POST");
      setRun({ state: "running", done: 0, total: plan.tables.length, current: null, error: null });
    } catch (e) {
      window.alert(e instanceof Error ? e.message : "הפעולה נכשלה");
    } finally {
      setBusy(false);
    }
  };

  if (run?.state === "running") {
    return <span className="text-muted"> · מעביר לקבצים {run.done}/{run.total ?? "?"}…</span>;
  }
  if (run?.state === "done") return <span style={{ color: "var(--success)" }}> · הועבר לקבצים ✓</span>;
  return (
    <>
      {run?.state === "failed" && <span style={{ color: "var(--danger)" }} title={run.error ?? ""}> · ההעברה נכשלה</span>}
      {" · "}
      <button type="button" onClick={() => void start()} disabled={busy}
              style={{ background: "none", border: "none", padding: 0, cursor: "pointer", color: "var(--primary)", font: "inherit", textDecoration: "underline" }}>
        העבר לקבצים
      </button>
    </>
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
                          <OffloadButton datasetId={ds.id} title={ds.title} />
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
