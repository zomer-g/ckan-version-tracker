/**
 * The source documents — the conflict-of-interest declarations themselves.
 *
 * 854 of the 2,971 have their bytes stored with us (moved to R2 during the
 * migration) and open inline; the rest are metadata-only BY DESIGN — OCOI's
 * CKAN import deliberately keeps only a link for re-fetchable files. The UI
 * says which is which instead of offering a button that 404s.
 */
import { lazy, Suspense, useCallback, useEffect, useState } from "react";
import { ocoi, OcoiDocument, OcoiEntityType, OcoiGraph, OcoiMeta } from "../../api/client";
import { Empty, ErrorNote, formatTotal, Pager, Spinner } from "./ocoiShared";

const OcoiGraphView = lazy(() => import("./OcoiGraphView"));

const PER_PAGE = 20;

export default function OcoiDocuments({
  onOpenEntity,
}: {
  onOpenEntity: (type: OcoiEntityType, id: string, name: string) => void;
}) {
  // The relationships extracted from ONE document, shown inline. A document is
  // where a claim comes from, so seeing its own web next to it is the natural
  // check — and it reuses the same renderer as the graph tab.
  const [openDoc, setOpenDoc] = useState<{ id: string; title: string } | null>(null);
  const [docGraph, setDocGraph] = useState<OcoiGraph | null>(null);
  const [graphLoading, setGraphLoading] = useState(false);

  const showGraph = async (id: string, title: string) => {
    if (openDoc?.id === id) {
      setOpenDoc(null);
      setDocGraph(null);
      return;
    }
    setOpenDoc({ id, title });
    setDocGraph(null);
    setGraphLoading(true);
    try {
      const res = await ocoi.documentGraph(id);
      setDocGraph(res.data);
    } catch {
      setDocGraph(null);
    } finally {
      setGraphLoading(false);
    }
  };

  const [q, setQ] = useState("");
  const [applied, setApplied] = useState("");
  const [verifiedOnly, setVerifiedOnly] = useState(false);
  const [page, setPage] = useState(1);
  const [rows, setRows] = useState<OcoiDocument[]>([]);
  const [meta, setMeta] = useState<OcoiMeta | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  const load = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const res = await ocoi.documents({
        page,
        limit: PER_PAGE,
        ...(applied ? { q: applied } : {}),
        ...(verifiedOnly ? { verified: "true" } : {}),
      });
      setRows(res.data || []);
      setMeta(res.meta || null);
    } catch (e) {
      setError(e instanceof Error ? e.message : "טעינת המסמכים נכשלה");
      setRows([]);
    } finally {
      setLoading(false);
    }
  }, [page, applied, verifiedOnly]);

  useEffect(() => {
    load();
  }, [load]);

  return (
    <div>
      <form
        onSubmit={(e) => {
          e.preventDefault();
          setPage(1);
          setApplied(q.trim());
        }}
        className="flex"
        style={{ gap: "0.5rem", flexWrap: "wrap", marginBottom: "0.9rem" }}
      >
        <input
          type="search"
          value={q}
          onChange={(e) => setQ(e.target.value)}
          placeholder="חיפוש בכותרות המסמכים…"
          style={{ flex: "1 1 300px", minWidth: 0, padding: "0.55rem 0.8rem" }}
          aria-label="חיפוש מסמכים"
        />
        <label className="text-sm flex" style={{ gap: "0.35rem", alignItems: "center" }}>
          <input
            type="checkbox"
            checked={verifiedOnly}
            onChange={(e) => {
              setPage(1);
              setVerifiedOnly(e.target.checked);
            }}
          />
          מאומתים בלבד
        </label>
        <button type="submit" className="btn btn-primary">
          חיפוש
        </button>
      </form>

      {error && <ErrorNote error={error} />}
      {meta && !loading && (
        <div className="text-sm text-muted" style={{ marginBottom: "0.6rem" }}>
          {formatTotal(meta.total, meta.total_capped)} מסמכים
        </div>
      )}
      {loading && <Spinner />}
      {!loading && rows.length === 0 && !error && <Empty>לא נמצאו מסמכים.</Empty>}

      {!loading && rows.length > 0 && (
        <div tabIndex={0} role="region" aria-label="מסמכי ניגוד עניינים" className="scroll-region" style={{ overflowX: "auto" }}>
          <table className="table" style={{ width: "100%", fontSize: "0.9rem" }}>
            <thead>
              <tr>
                <th scope="col" style={{ textAlign: "start" }}>כותרת</th>
                <th scope="col" style={{ textAlign: "start" }}>מקור</th>
                <th scope="col" style={{ textAlign: "center" }}>קשרים</th>
                <th scope="col" style={{ textAlign: "center" }}>סטטוס</th>
                <th scope="col" style={{ textAlign: "center" }}>פעולות</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((d) => (
                <tr key={d.id}>
                  <td style={{ maxWidth: 380 }}>
                    <span style={{ fontWeight: 500 }}>{d.title || "(ללא כותרת)"}</span>
                    {d.verified && (
                      <span
                        title="אומת על ידי עורך תוכן"
                        style={{ marginInlineStart: "0.4rem", color: "var(--success)" }}
                      >
                        ✔
                      </span>
                    )}
                  </td>
                  <td className="text-muted" style={{ fontSize: "0.85rem" }}>
                    {d.source_title || d.source_type || "—"}
                  </td>
                  <td style={{ textAlign: "center" }}>
                    {(d.relationships_count ?? 0).toLocaleString()}
                  </td>
                  <td style={{ textAlign: "center", fontSize: "0.8rem" }} className="text-muted">
                    {d.conversion_status === "converted" ? "מומר" : d.conversion_status || "—"}
                  </td>
                  <td style={{ textAlign: "center", whiteSpace: "nowrap" }}>
                    <button
                      type="button"
                      className="btn btn-sm"
                      onClick={() => showGraph(d.id, d.title)}
                      title="הצג את הקשרים שחולצו מהמסמך"
                      disabled={!d.relationships_count}
                    >
                      {openDoc?.id === d.id ? "סגור" : "קשרים"}
                    </button>{" "}
                    <a
                      className="btn btn-sm"
                      href={ocoi.fileUrl(d.id)}
                      target="_blank"
                      rel="noopener noreferrer"
                    >
                      קובץ
                    <span className="sr-only"> (נפתח בחלון חדש)</span></a>{" "}
                    {d.file_url && !d.file_url.startsWith("upload://") && (
                      <a
                        className="btn btn-sm"
                        href={d.file_url}
                        target="_blank"
                        rel="noopener noreferrer"
                        title="המקור באתר המפרסם"
                      >
                        מקור
                      <span className="sr-only"> (נפתח בחלון חדש)</span></a>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {openDoc && (
        <div style={{ marginTop: "1rem" }}>
          <div className="flex" style={{ gap: "0.5rem", alignItems: "center", marginBottom: "0.5rem" }}>
            <strong style={{ fontSize: "0.95rem" }}>קשרים מתוך: {openDoc.title}</strong>
            <button
              type="button"
              className="btn btn-sm"
              onClick={() => {
                setOpenDoc(null);
                setDocGraph(null);
              }}
            >
              סגור
            </button>
          </div>
          {graphLoading && <Spinner label="טוען קשרים…" />}
          {!graphLoading && docGraph && docGraph.nodes.length > 0 && (
            <Suspense fallback={<Spinner label="טוען את רכיב הגרף…" />}>
              <OcoiGraphView graph={docGraph} height={420} onSelect={onOpenEntity} />
            </Suspense>
          )}
          {!graphLoading && docGraph && docGraph.nodes.length === 0 && (
            <Empty>לא חולצו קשרים מהמסמך הזה.</Empty>
          )}
        </div>
      )}

      {meta && !loading && <Pager page={meta.page} pages={meta.pages} onPage={setPage} />}
    </div>
  );
}
