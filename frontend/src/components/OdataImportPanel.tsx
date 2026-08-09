import { useCallback, useEffect, useRef, useState } from "react";
import { admin, OdataImport } from "../api/client";
import {
  OdataDataset,
  OdataResource,
  ODATA_BASE,
  odataDatasetUrl,
  odataPackageSearch,
  isImportableResource,
  resourceFormat,
} from "../api/odata";

/**
 * Admin: curate which מידע לעם (odata) resources become queryable SQL tables.
 *
 * Top — browse the odata catalog (search → items → files) and "import" a
 * datastore file, which POSTs to /admin/odata/import: the backend pulls its
 * datastore dump into a real ``odata.<table>`` that then appears in /data under
 * the "מידע לעם" source. Bottom — the tables already imported, with delete.
 * These are PROCESSED data, always badged as such.
 */
function fmtDate(v?: string | null): string {
  if (!v) return "";
  const d = new Date(v);
  if (isNaN(d.getTime())) return String(v).slice(0, 16);
  const p = (n: number) => String(n).padStart(2, "0");
  return `${p(d.getDate())}.${p(d.getMonth() + 1)}.${d.getFullYear()} ${p(d.getHours())}:${p(d.getMinutes())}`;
}

export default function OdataImportPanel() {
  const [query, setQuery] = useState("");
  const [activeQuery, setActiveQuery] = useState("");
  const [items, setItems] = useState<OdataDataset[]>([]);
  const [count, setCount] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);
  const [openItem, setOpenItem] = useState<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  const [imports, setImports] = useState<OdataImport[]>([]);
  const [importsLoading, setImportsLoading] = useState(true);
  const [busy, setBusy] = useState<string | null>(null); // resource_id in flight
  const [msg, setMsg] = useState<{ ok: boolean; text: string } | null>(null);
  // Rows loaded so far by the running import job — a big file takes minutes.
  const [progress, setProgress] = useState<{ rows: number; label: string } | null>(null);

  const search = useCallback(async (q: string) => {
    abortRef.current?.abort();
    const ctrl = new AbortController();
    abortRef.current = ctrl;
    setLoading(true);
    setError(false);
    try {
      const { count, results } = await odataPackageSearch(q, 20, ctrl.signal);
      setItems(results);
      setCount(count);
    } catch (e) {
      if ((e as Error).name === "AbortError") return;
      setError(true);
      setItems([]);
      setCount(0);
    } finally {
      if (abortRef.current === ctrl) setLoading(false);
    }
  }, []);

  const loadImports = useCallback(async () => {
    setImportsLoading(true);
    try {
      const r = await admin.odataImports();
      setImports(r.imports || []);
    } catch {
      setImports([]);
    } finally {
      setImportsLoading(false);
    }
  }, []);

  useEffect(() => { search(activeQuery); }, [activeQuery, search]);
  useEffect(() => { loadImports(); }, [loadImports]);
  useEffect(() => () => abortRef.current?.abort(), []);

  const importedIds = new Set(imports.map((i) => i.resource_id));

  // The backend works as a background JOB (a 548k-row spreadsheet takes
  // minutes — longer than the gateway holds a request), so we poll it here.
  //
  // Both routes below end at this function: by the time we are here the bytes
  // are in hand, whether the browser fetched them from odata or the admin
  // picked them off disk.
  const sendImport = async (
    r: OdataResource, d: OdataDataset, blob: Blob, filename: string, fmt: string,
  ) => {
    const label = d.title?.trim() || d.name;
    // The server keys the table on it, so there is nothing to import without
    // one. The guard used to live in the fetch path only; both routes need it.
    if (!r.id) {
      setBusy(null);
      setMsg({ ok: false, text: "למשאב אין מזהה — אי אפשר לייבא." });
      return;
    }
    try {
      const fd = new FormData();
      fd.append("file", blob, filename);
      fd.append("resource_id", r.id);
      fd.append("format", fmt);
      fd.append("dataset_name", d.name || "");
      fd.append("title", label || "");
      fd.append("organization", d.organization?.title || d.organization?.name || "");
      fd.append("source_url", `${ODATA_BASE}/dataset/${d.name}/resource/${r.id}`);
      // Provenance only — and on the manual route it is the Drive link that
      // could not be fetched, which is exactly what a reader wants recorded.
      fd.append("file_url", r.url || "");
      let job = await admin.odataImportFile(fd);
      setProgress({ rows: 0, label: label || "" });
      while (job.state === "running") {
        await new Promise((res) => setTimeout(res, 2000));
        job = await admin.odataImportJob(job.id);
        setProgress({ rows: job.rows ?? 0, label: label || "" });
      }
      if (job.state === "error") throw new Error(job.error || "שגיאה לא ידועה");
      setMsg({ ok: true, text: `יובא: ${job.title || label} → odata.${job.table} (${(job.rows ?? 0).toLocaleString()} שורות)` });
      await loadImports();
    } catch (e) {
      setMsg({ ok: false, text: `ייבוא נכשל: ${(e as Error).message}` });
    } finally {
      setBusy(null);
      setProgress(null);
    }
  };

  // The file is fetched CLIENT-SIDE: Cloudflare 403s our datacenter IP, but the
  // browser can read it (CORS is open).
  const doImport = async (r: OdataResource, d: OdataDataset) => {
    if (!r.id || !r.url) {
      setMsg({ ok: false, text: "למשאב אין קובץ להורדה." });
      return;
    }
    setBusy(r.id);
    setMsg(null);
    let blob: Blob;
    try {
      const resp = await fetch(r.url);
      if (!resp.ok) throw new Error(`הורדת הקובץ מ-odata נכשלה (${resp.status})`);
      blob = await resp.blob();
    } catch (e) {
      // A resource whose url points at a Drive/Dropbox VIEWER rather than at
      // bytes lands here — the fetch is refused by CORS, or it succeeds and
      // hands back an HTML page. Neither is a file, and neither is something
      // this side can fix, so say what to do instead of just failing.
      setBusy(null);
      setMsg({
        ok: false,
        text: `לא ניתן להוריד את הקובץ מהדפדפן (${(e as Error).message}). ` +
              `אם המשאב מקשר לדרייב — הורד אותו ידנית והשתמש ב"⬆ העלה קובץ".`,
      });
      return;
    }
    // Falls back to the file extension when odata declares no format.
    const fmt = resourceFormat(r) || (r.datastore_active ? "CSV" : "");
    await sendImport(r, d, blob, r.name?.trim() || "file", fmt);
  };

  // Manual route: the admin downloaded the file themselves. Needed because a
  // מידע לעם resource can point at a Google Drive link — `open?id=…&usp=drive_fs`
  // is a viewer page, not a file — so there is nothing for the automatic route
  // to fetch, and the dataset was simply unreachable from SQL.
  // At or above this a single POST is rejected by Cloudflare's ~100MB body
  // ceiling before it ever reaches the server, so there is no error to show —
  // the button just appears dead. Such a file is sliced and sent piece by piece.
  const EDGE_BODY_LIMIT = 80 * 1024 * 1024;

  const doUploadFile = async (r: OdataResource, d: OdataDataset, file: File) => {
    if (!r.id) {
      setMsg({ ok: false, text: "למשאב אין מזהה — אי אפשר לייבא." });
      return;
    }
    setBusy(r.id);
    setMsg(null);
    // Deliberately NOT the resource's declared format: the file the admin chose
    // is the truth here, and a resource labelled CSV that turns out to be an
    // .xlsx would otherwise be parsed as the wrong thing. Empty lets the server
    // infer from the filename it is actually given.
    if (file.size < EDGE_BODY_LIMIT) {
      await sendImport(r, d, file, file.name, "");
      return;
    }
    const label = d.title?.trim() || d.name;
    const mb = (n: number) => Math.round(n / (1024 * 1024));
    try {
      const begun = await admin.odataUploadBegin();
      const size = begun.chunk_size || 32 * 1024 * 1024;
      const total = Math.ceil(file.size / size);
      // Sequential on purpose: the server APPENDS each slice, so order is the
      // file. Parallel uploads would interleave and produce a corrupt CSV that
      // parses far enough to look plausible.
      for (let i = 0; i < total; i++) {
        const fd = new FormData();
        fd.append("upload_id", begun.upload_id);
        fd.append("chunk", file.slice(i * size, Math.min((i + 1) * size, file.size)));
        await admin.odataUploadChunk(fd);
        setProgress({
          rows: 0,
          label: `מעלה ${file.name}: ${mb(Math.min((i + 1) * size, file.size))}/${mb(file.size)}MB`,
        });
      }
      const fd = new FormData();
      fd.append("upload_id", begun.upload_id);
      fd.append("resource_id", r.id);
      fd.append("filename", file.name);
      fd.append("format", "");
      fd.append("dataset_name", d.name || "");
      fd.append("title", label || "");
      fd.append("organization", d.organization?.title || d.organization?.name || "");
      fd.append("source_url", `${ODATA_BASE}/dataset/${d.name}/resource/${r.id}`);
      fd.append("file_url", r.url || "");
      let job = await admin.odataImportStaged(fd);
      setProgress({ rows: 0, label: label || "" });
      while (job.state === "running") {
        await new Promise((res) => setTimeout(res, 2000));
        job = await admin.odataImportJob(job.id);
        setProgress({ rows: job.rows ?? 0, label: label || "" });
      }
      if (job.state === "error") throw new Error(job.error || "שגיאה לא ידועה");
      setMsg({ ok: true, text: `יובא: ${job.title || label} → odata.${job.table} (${(job.rows ?? 0).toLocaleString()} שורות)` });
      await loadImports();
    } catch (e) {
      setMsg({ ok: false, text: `ייבוא נכשל: ${(e as Error).message}` });
    } finally {
      setBusy(null);
      setProgress(null);
    }
  };

  const doDelete = async (imp: OdataImport) => {
    if (!window.confirm(`למחוק את הטבלה odata.${imp.table}? (${imp.title || imp.resource_id})`)) return;
    setBusy(imp.resource_id);
    setMsg(null);
    try {
      await admin.odataDeleteImport(imp.resource_id);
      setMsg({ ok: true, text: `נמחק: odata.${imp.table}` });
      await loadImports();
    } catch (e) {
      setMsg({ ok: false, text: `מחיקה נכשלה: ${(e as Error).message}` });
    } finally {
      setBusy(null);
    }
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "1.25rem" }}>
      <div style={{ fontSize: "0.88rem", lineHeight: 1.6, color: "#7c2d12", background: "#fffdf9", border: "1px solid #f59e0b", borderInlineStart: "4px solid #b45309", borderRadius: 6, padding: "0.75rem 0.9rem" }}>
        <strong>מידע מעובד.</strong> כאן בוחרים קבצים ספציפיים מ־מידע לעם (odata.org.il)
        ודוחפים אותם לטבלת SQL אמיתית. הטבלאות מופיעות בקונסולת ה-SQL תחת המקור
        <strong> "מידע לעם"</strong> וניתנות לתשאול ול-JOIN — אך הן מידע מעובד, לא מקור
        ציבורי מקורי. נתמכים הפורמטים <strong>CSV, XLS, XLSX, ICS/ICAL</strong> (וגם כל
        משאב עם datastore).
      </div>

      {msg && (
        <div role="status" style={{ padding: "0.6rem 0.85rem", borderRadius: 6, fontSize: "0.88rem", background: msg.ok ? "#ecfdf5" : "#fef2f2", color: msg.ok ? "#065f46" : "#991b1b", border: `1px solid ${msg.ok ? "#a7f3d0" : "#fecaca"}` }}>
          {msg.text}
        </div>
      )}

      {progress && (
        <div role="status" style={{ padding: "0.6rem 0.85rem", borderRadius: 6, fontSize: "0.88rem", background: "#eff6ff", color: "#1e3a8a", border: "1px solid #bfdbfe" }}>
          מייבא את "{progress.label}"… {progress.rows > 0
            ? `${progress.rows.toLocaleString()} שורות נטענו`
            : "מנתח את הקובץ"}. קובץ גדול יכול לקחת כמה דקות — אפשר להשאיר את הדף פתוח.
        </div>
      )}

      {/* ── Imported tables ── */}
      <section>
        <h3 style={{ margin: "0 0 0.5rem", fontSize: "1.05rem" }}>
          טבלאות מיובאות ({imports.length})
        </h3>
        {importsLoading ? (
          <div className="text-sm text-muted">טוען…</div>
        ) : imports.length === 0 ? (
          <div className="text-sm text-muted">עדיין לא יובאו טבלאות. חפשו למטה ובחרו קובץ לייבוא.</div>
        ) : (
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.85rem" }}>
              <thead>
                <tr style={{ textAlign: "start", borderBottom: "2px solid var(--border, #e5e7eb)" }}>
                  <th style={{ textAlign: "start", padding: "0.4rem 0.5rem" }}>כותרת</th>
                  <th style={{ textAlign: "start", padding: "0.4rem 0.5rem" }}>טבלה</th>
                  <th style={{ textAlign: "start", padding: "0.4rem 0.5rem" }}>שורות</th>
                  <th style={{ textAlign: "start", padding: "0.4rem 0.5rem" }}>יובא</th>
                  <th style={{ padding: "0.4rem 0.5rem" }}></th>
                </tr>
              </thead>
              <tbody>
                {imports.map((imp) => (
                  <tr key={imp.resource_id} style={{ borderBottom: "1px solid var(--border, #f1f5f9)" }}>
                    <td style={{ padding: "0.4rem 0.5rem" }}>
                      {imp.source_url ? (
                        <a href={imp.source_url} target="_blank" rel="noopener noreferrer">{imp.title || imp.resource_id}</a>
                      ) : (imp.title || imp.resource_id)}
                      {imp.organization && <div className="text-muted" style={{ fontSize: "0.75rem" }}>{imp.organization}</div>}
                    </td>
                    <td style={{ padding: "0.4rem 0.5rem" }}>
                      <code dir="ltr" style={{ unicodeBidi: "isolate", fontSize: "0.78rem" }}>odata.{imp.table}</code>
                    </td>
                    <td style={{ padding: "0.4rem 0.5rem", fontVariantNumeric: "tabular-nums" }}>{(imp.rows ?? 0).toLocaleString()}</td>
                    <td style={{ padding: "0.4rem 0.5rem", whiteSpace: "nowrap" }}>{fmtDate(imp.imported_at)}</td>
                    <td style={{ padding: "0.4rem 0.5rem", whiteSpace: "nowrap" }}>
                      <button type="button" onClick={() => doDelete(imp)} disabled={busy === imp.resource_id}
                        style={{ fontSize: "0.78rem", padding: "0.2rem 0.6rem", border: "1px solid #dc2626", background: "none", color: "#dc2626", borderRadius: 4, cursor: "pointer" }}>
                        מחק
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      {/* ── Browse + import ── */}
      <section>
        <h3 style={{ margin: "0 0 0.5rem", fontSize: "1.05rem" }}>ייבוא קובץ חדש ממידע לעם</h3>
        <form onSubmit={(e) => { e.preventDefault(); setActiveQuery(query.trim()); }} role="search"
          style={{ display: "flex", gap: "0.5rem", marginBottom: "0.75rem", maxWidth: 520 }}>
          <input type="search" value={query} onChange={(e) => setQuery(e.target.value)}
            placeholder="חיפוש פריט במידע לעם…" aria-label="חיפוש פריט במידע לעם"
            style={{ flex: 1, minWidth: 0, padding: "0.45rem 0.6rem", border: "1px solid var(--border, #d1d5db)", borderRadius: 4 }} />
          <button type="submit" disabled={loading}
            style={{ padding: "0.45rem 1rem", border: "none", borderRadius: 4, background: "#b45309", color: "#fff", fontWeight: 600, cursor: "pointer" }}>
            {loading ? "מחפש…" : "חפש"}
          </button>
        </form>

        {error && <div className="text-sm" style={{ color: "#b91c1c" }}>החיפוש נכשל. נסו שוב.</div>}
        {!error && (
          <>
            <div className="text-sm text-muted" style={{ marginBottom: "0.5rem" }}>
              {loading ? "טוען…" : `${count.toLocaleString()} פריטים במידע לעם`}
            </div>
            <div className="odata-group-list">
              {items.map((d) => {
                const org = d.organization?.title || d.organization?.name;
                const open = openItem === d.name;
                const resources = d.resources || [];
                return (
                  <div key={d.name} className="odata-item">
                    <button type="button" className="odata-item-head" aria-expanded={open}
                      onClick={() => setOpenItem(open ? null : d.name)}>
                      <span className="odata-item-caret" aria-hidden>{open ? "▾" : "▸"}</span>
                      <span className="odata-item-title">{d.title?.trim() || d.name}</span>
                      <span className="odata-item-count">{resources.length}</span>
                    </button>
                    {open && (
                      <div className="odata-item-files">
                        {org && <div className="odata-item-org">{org}</div>}
                        {resources.length === 0 && <div className="odata-item-empty">אין קבצים.</div>}
                        {resources.map((r, i) => {
                          const already = r.id ? importedIds.has(r.id) : false;
                          const queryable = isImportableResource(r);
                          return (
                            <div key={r.id || i} className="odata-file-row">
                              <span className="odata-file-format">{resourceFormat(r) || "—"}</span>
                              {r.url ? (
                                <a className="odata-file-link" href={r.url} target="_blank" rel="noopener noreferrer" title={r.name || ""}>{r.name?.trim() || "קובץ"}</a>
                              ) : (
                                <span className="odata-file-link">{r.name?.trim() || "קובץ"}</span>
                              )}
                              {queryable ? (
                                <button type="button" className="odata-sql-query-btn" disabled={busy === r.id}
                                  onClick={() => doImport(r, d)}
                                  title="ייבוא הקובץ לטבלת SQL תחת המקור מידע לעם">
                                  {busy === r.id ? "מייבא…" : already ? "↻ ייבא מחדש" : "⭳ ייבא ל-SQL"}
                                </button>
                              ) : (
                                <span className="text-muted" style={{ fontSize: "0.72rem", flex: "0 0 auto" }} title="פורמט לא נתמך לייבוא אוטומטי (נתמכים: CSV/XLS/XLSX/ICAL או datastore) — אפשר להעלות את הקובץ ידנית">אין ייבוא אוטומטי</span>
                              )}
                              {/* Always offered, including where the automatic
                                  route is impossible: a resource that links to
                                  Google Drive has no fetchable bytes, and
                                  before this it could not reach SQL at all. */}
                              <label className="odata-sql-query-btn" style={{ cursor: busy === r.id ? "default" : "pointer", opacity: busy === r.id ? 0.5 : 1, flex: "0 0 auto" }}
                                title="בחר קובץ מהמחשב (CSV/XLS/XLSX/ICAL) וטען אותו לטבלת SQL תחת המשאב הזה">
                                ⬆ העלה קובץ
                                <input type="file" accept=".csv,.xls,.xlsx,.xlsm,.ics,.ical,.ica" disabled={busy === r.id}
                                  style={{ display: "none" }}
                                  onChange={(e) => {
                                    const f = e.target.files?.[0];
                                    // Reset first: picking the SAME file twice
                                    // after a failed run fires no change event
                                    // otherwise, and the button looks dead.
                                    e.target.value = "";
                                    if (f) void doUploadFile(r, d, f);
                                  }} />
                              </label>
                            </div>
                          );
                        })}
                        <a className="odata-item-open" href={odataDatasetUrl(d.name, "he")} target="_blank" rel="noopener noreferrer">פתיחה במידע לעם ↗</a>
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          </>
        )}
      </section>
    </div>
  );
}
