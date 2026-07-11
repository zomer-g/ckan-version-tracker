import { useState, useEffect, useCallback, useRef } from "react";
import { knessetDb, MmmSearchResult, MmmFacets } from "../api/client";

// The "מסמכי ממ״מ" tab of /knesset: metadata search over the MMM document
// catalog mirrored into knesset.mmm_documents (title/keywords/abstract,
// author, doc type, years). Results link to the incident page and the PDF on
// the Knesset servers; the same table is queryable in the SQL tab.

const PAGE = 20;

export default function KnessetMmmSearch() {
  const [q, setQ] = useState("");
  const [author, setAuthor] = useState("");
  const [docType, setDocType] = useState("");
  const [facets, setFacets] = useState<MmmFacets | null>(null);
  const [result, setResult] = useState<MmmSearchResult | null>(null);
  const [offset, setOffset] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [openAbstract, setOpenAbstract] = useState<number | null>(null);

  useEffect(() => {
    knessetDb.mmmFacets().then(setFacets).catch(() => {});
  }, []);

  const load = useCallback((off: number) => {
    setLoading(true);
    knessetDb
      .mmmSearch({ q, author, doc_type: docType, limit: PAGE, offset: off })
      .then((r) => { setResult(r); setError(null); })
      .catch((e) => { setResult(null); setError(e?.message || "שגיאה בחיפוש"); })
      .finally(() => setLoading(false));
  }, [q, author, docType]);

  // Debounced auto-search on filter change (and initial load).
  const tRef = useRef<number | undefined>(undefined);
  useEffect(() => {
    window.clearTimeout(tRef.current);
    tRef.current = window.setTimeout(() => { setOffset(0); load(0); }, 350);
    return () => window.clearTimeout(tRef.current);
  }, [load]);

  const total = result?.total ?? 0;

  return (
    <div>
      <div className="card" style={{ padding: "1rem", marginBottom: "1rem" }}>
        <div className="flex" style={{ gap: "0.75rem", flexWrap: "wrap" }}>
          <input
            type="search"
            value={q}
            onChange={(e) => setQ(e.target.value)}
            placeholder="חיפוש בכותרת, במילות המפתח ובתמצית…"
            aria-label="חיפוש במסמכי ממ״מ"
            style={{ flex: "2 1 280px", padding: "0.5rem 0.75rem", border: "1px solid var(--border, #d1d5db)", borderRadius: 4 }}
          />
          <input
            type="search"
            value={author}
            onChange={(e) => setAuthor(e.target.value)}
            placeholder="כותב/ת או מאשר/ת…"
            aria-label="סינון לפי כותב"
            style={{ flex: "1 1 170px", padding: "0.5rem 0.75rem", border: "1px solid var(--border, #d1d5db)", borderRadius: 4 }}
          />
          <select
            value={docType}
            onChange={(e) => setDocType(e.target.value)}
            aria-label="סוג מסמך"
            style={{ flex: "1 1 150px", padding: "0.5rem 0.6rem", border: "1px solid var(--border, #d1d5db)", borderRadius: 4 }}
          >
            <option value="">כל סוגי המסמכים</option>
            {facets?.doc_types.map((t) => (
              <option key={t.doc_type} value={t.doc_type}>
                {t.doc_type} ({t.count.toLocaleString()})
              </option>
            ))}
          </select>
        </div>
        <div className="text-sm text-muted" style={{ marginTop: "0.5rem" }}>
          {facets && <>קטלוג מלא של {facets.total.toLocaleString()} מסמכי מרכז המחקר והמידע (ממ״מ)</>}
          {" · "}המטא-דאטה זמינה גם ב-SQL: טבלת <code>mmm_documents</code> בלשונית ממשק SQL.
        </div>
      </div>

      {error && <div className="empty-state">{error}</div>}
      {loading && !result && <div className="loading" role="status">מחפש…</div>}

      {result && (
        <>
          <div className="text-sm text-muted" style={{ marginBottom: "0.5rem" }}>
            {total.toLocaleString()} תוצאות
          </div>
          <div className="card" style={{ padding: 0, overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.86rem" }}>
              <thead>
                <tr>
                  {["תאריך", "כותרת", "סוג", "כתיבה", "נכתב לבקשת", "PDF"].map((h) => (
                    <th key={h} style={{ textAlign: "start", padding: "0.5rem 0.7rem", background: "var(--bg-muted, #eef2f5)", borderBottom: "2px solid var(--border, #cbd5e1)", whiteSpace: "nowrap" }}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {result.items.map((d) => (
                  <tr key={d.rid} style={{ borderBottom: "1px solid var(--border, #f1f5f9)", verticalAlign: "top" }}>
                    <td style={{ padding: "0.45rem 0.7rem", whiteSpace: "nowrap" }}>{d.date_text || d.date || ""}</td>
                    <td style={{ padding: "0.45rem 0.7rem", maxWidth: 420 }}>
                      <button
                        type="button"
                        onClick={() => setOpenAbstract(openAbstract === d.rid ? null : d.rid)}
                        title={openAbstract === d.rid ? "הסתרת התמצית" : "הצגת התמצית"}
                        style={{ background: "none", border: "none", padding: 0, cursor: "pointer", textAlign: "start", color: "var(--text)", fontWeight: 600, fontSize: "inherit" }}
                      >
                        {d.title || `מסמך ${d.rid}`}
                      </button>
                      {openAbstract === d.rid && (
                        <div className="text-sm" style={{ marginTop: "0.35rem", color: "var(--text-muted)", lineHeight: 1.6 }}>
                          {d.abstract || "אין תמצית."}
                          {d.keywords && <div style={{ marginTop: "0.25rem" }}>מילות מפתח: {d.keywords}</div>}
                        </div>
                      )}
                    </td>
                    <td style={{ padding: "0.45rem 0.7rem", whiteSpace: "nowrap" }}>{d.doc_type || ""}</td>
                    <td style={{ padding: "0.45rem 0.7rem" }}>{d.author || ""}</td>
                    <td style={{ padding: "0.45rem 0.7rem", maxWidth: 220 }}>{d.requested_by || ""}</td>
                    <td style={{ padding: "0.45rem 0.7rem", whiteSpace: "nowrap" }}>
                      {d.pdf_url && (
                        <a href={d.pdf_url} target="_blank" rel="noreferrer" style={{ color: "var(--primary)" }}>PDF</a>
                      )}
                    </td>
                  </tr>
                ))}
                {result.items.length === 0 && (
                  <tr><td colSpan={6} style={{ padding: "1rem", textAlign: "center", color: "var(--text-muted)" }}>אין תוצאות</td></tr>
                )}
              </tbody>
            </table>
          </div>
          <div className="flex-between" style={{ marginTop: "0.75rem", flexWrap: "wrap", gap: "0.5rem" }}>
            <span className="text-sm text-muted">
              {total === 0 ? 0 : offset + 1}–{Math.min(offset + PAGE, total)} מתוך {total.toLocaleString()}
            </span>
            <div className="flex" style={{ gap: "0.5rem" }}>
              <button type="button" disabled={offset === 0 || loading}
                onClick={() => { const o = Math.max(0, offset - PAGE); setOffset(o); load(o); }}
                style={{ padding: "0.3rem 0.8rem", border: "1px solid var(--border, #d1d5db)", borderRadius: 4, background: "none", cursor: offset === 0 ? "not-allowed" : "pointer" }}>
                &rarr; הקודם
              </button>
              <button type="button" disabled={offset + PAGE >= total || loading}
                onClick={() => { const o = offset + PAGE; setOffset(o); load(o); }}
                style={{ padding: "0.3rem 0.8rem", border: "1px solid var(--border, #d1d5db)", borderRadius: 4, background: "none", cursor: offset + PAGE >= total ? "not-allowed" : "pointer" }}>
                הבא &larr;
              </button>
            </div>
          </div>
        </>
      )}
    </div>
  );
}
