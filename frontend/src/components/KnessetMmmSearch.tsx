import { useState, useEffect, useCallback, useRef } from "react";
import { useSearchParams } from "react-router-dom";
import { knessetDb, MmmSearchResult, MmmFacets, MmmDeepResult } from "../api/client";

// The "מסמכי ממ״מ" tab of /knesset. Two search modes:
//   • fast (default) — metadata search over the MMM catalog mirrored into
//     knesset.mmm_documents (title/keywords/abstract, author, doc type). Same
//     table is queryable in the SQL tab.
//   • deep/slow — full-text search INSIDE the document bodies, run remotely on
//     TAG-IT (scope 14) via its MCP. Reaches the actual text, not just the
//     catalog; a remote round-trip so it's slower and has no author/type filter.

const PAGE = 20;
type Mode = "fast" | "deep";

// TAG-IT snippets are highlighted MD fragments — they carry markup tags and
// often markdown-table noise (| --- | …). Render as clean plain text (no HTML).
function cleanSnippet(s: string): string {
  return s
    .replace(/<[^>]*>/g, " ")   // highlight/markup tags
    .replace(/\|/g, " ")        // markdown table pipes
    .replace(/-{2,}/g, " ")     // table rule dashes
    .replace(/\s+/g, " ")       // collapse whitespace/newlines
    .trim();
}

export default function KnessetMmmSearch() {
  // Search state is mirrored into the URL (?tab=mmm&mode=&q=…&author=…&type=…&offset=…)
  // so every search is deep-linkable / shareable.
  const [searchParams, setSearchParams] = useSearchParams();
  const [mode, setMode] = useState<Mode>(() => (searchParams.get("mode") === "deep" ? "deep" : "fast"));
  const [q, setQ] = useState(() => searchParams.get("q") || "");
  const [author, setAuthor] = useState(() => searchParams.get("author") || "");
  const [docType, setDocType] = useState(() => searchParams.get("type") || "");
  const [facets, setFacets] = useState<MmmFacets | null>(null);
  const [result, setResult] = useState<MmmSearchResult | null>(null);
  const [deepResult, setDeepResult] = useState<MmmDeepResult | null>(null);
  const [offset, setOffset] = useState(() => Math.max(0, Number(searchParams.get("offset")) || 0));

  const writeUrl = useCallback((m: Mode, off: number, qv: string, av: string, tv: string) => {
    const p: Record<string, string> = { tab: "mmm" };
    if (m === "deep") p.mode = "deep";
    if (qv.trim()) p.q = qv.trim();
    if (m === "fast" && av.trim()) p.author = av.trim();
    if (m === "fast" && tv) p.type = tv;
    if (off > 0) p.offset = String(off);
    setSearchParams(p, { replace: true });
  }, [setSearchParams]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [openAbstract, setOpenAbstract] = useState<number | null>(null);

  useEffect(() => {
    knessetDb.mmmFacets().then(setFacets).catch(() => {});
  }, []);

  const load = useCallback((off: number) => {
    setLoading(true);
    writeUrl(mode, off, q, author, docType);
    const attempt = async (retriesLeft: number): Promise<void> => {
      try {
        if (mode === "deep") {
          const r = await knessetDb.mmmDeepSearch({ q, page: Math.floor(off / PAGE) + 1, size: PAGE });
          setDeepResult(r);
          setResult(null);
        } else {
          const r = await knessetDb.mmmSearch({ q, author, doc_type: docType, limit: PAGE, offset: off });
          setResult(r);
          setDeepResult(null);
        }
        setError(null);
      } catch (e: unknown) {
        // "Failed to fetch" is a transient network error (dyno restart mid-
        // deploy, or a Neon cold-start). Retry a couple of times with backoff
        // before surfacing it, so a blip doesn't strand the search.
        const msg = e instanceof Error ? e.message : String(e);
        const transient = /failed to fetch|networkerror|load failed/i.test(msg);
        if (transient && retriesLeft > 0) {
          await new Promise((res) => setTimeout(res, 700));
          return attempt(retriesLeft - 1);
        }
        setResult(null);
        setDeepResult(null);
        setError(transient ? "התקשורת עם השרת נכשלה זמנית. נסו שוב." : (msg || "שגיאה בחיפוש"));
      }
    };
    attempt(2).finally(() => setLoading(false));
  }, [mode, q, author, docType, writeUrl]);

  const retry = useCallback(() => load(offset), [load, offset]);

  // Debounced auto-search on filter/mode change; the very first run keeps the
  // offset that arrived in the URL (deep link into page N).
  const tRef = useRef<number | undefined>(undefined);
  const firstRef = useRef(true);
  useEffect(() => {
    window.clearTimeout(tRef.current);
    const off = firstRef.current ? offset : 0;
    firstRef.current = false;
    tRef.current = window.setTimeout(() => { setOffset(off); load(off); }, 350);
    return () => window.clearTimeout(tRef.current);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [load]);

  // Deep mode: total may be inexact (TAG-IT doesn't always return a count), so
  // "next" is enabled whenever the current page came back full.
  const total = mode === "deep" ? (deepResult?.total ?? 0) : (result?.total ?? 0);
  const deepExact = deepResult?.total_exact ?? false;
  const hasNext = mode === "deep"
    ? (deepExact ? offset + PAGE < total : (deepResult?.items.length ?? 0) === PAGE)
    : offset + PAGE < total;

  return (
    <div>
      <div className="card" style={{ padding: "1rem", marginBottom: "1rem" }}>
        {/* Mode toggle: fast metadata (SQL) vs deep full-text (TAG-IT). */}
        <div className="flex" role="tablist" aria-label="מצב חיפוש" style={{ gap: "0.4rem", marginBottom: "0.75rem", flexWrap: "wrap" }}>
          {([["fast", "חיפוש מהיר (מטא-דאטה)"], ["deep", "חיפוש עמוק בתוכן (איטי)"]] as [Mode, string][]).map(([m, label]) => (
            <button
              key={m}
              type="button"
              role="tab"
              aria-selected={mode === m}
              onClick={() => { if (mode !== m) { setMode(m); setOffset(0); } }}
              style={{
                padding: "0.35rem 0.9rem", borderRadius: 999, cursor: "pointer",
                fontSize: "0.85rem", fontWeight: 600,
                border: `1px solid ${mode === m ? "var(--primary, #0f766e)" : "var(--border, #d1d5db)"}`,
                background: mode === m ? "var(--primary, #0f766e)" : "none",
                color: mode === m ? "#fff" : "var(--text)",
              }}
            >
              {label}
            </button>
          ))}
        </div>

        <div className="flex" style={{ gap: "0.75rem", flexWrap: "wrap" }}>
          <input
            type="search"
            value={q}
            onChange={(e) => setQ(e.target.value)}
            placeholder={mode === "deep" ? "חיפוש בתוך תוכן המסמכים המלא…" : "חיפוש בכותרת, במילות המפתח ובתמצית…"}
            aria-label="חיפוש במסמכי ממ״מ"
            style={{ flex: "2 1 280px", padding: "0.5rem 0.75rem", border: "1px solid var(--border, #d1d5db)", borderRadius: 4 }}
          />
          {mode === "fast" && (
            <>
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
            </>
          )}
        </div>
        <div className="text-sm text-muted" style={{ marginTop: "0.5rem" }}>
          {mode === "deep" ? (
            <>חיפוש טקסט מלא בתוך גוף המסמכים דרך TAG-IT — איטי יותר, אך מגיע לתוכן עצמו ולא רק לקטלוג.</>
          ) : (
            <>
              {facets && <>קטלוג מלא של {facets.total.toLocaleString()} מסמכי מרכז המחקר והמידע (ממ״מ)</>}
              {" · "}המטא-דאטה זמינה גם ב-SQL: טבלת <code>mmm_documents</code> בלשונית ממשק SQL.
            </>
          )}
        </div>
      </div>

      {error && (
        <div className="empty-state">
          {error}
          <div style={{ marginTop: "0.6rem" }}>
            <button type="button" onClick={retry}
              style={{ padding: "0.35rem 1rem", borderRadius: 4, border: "1px solid var(--primary, #0f766e)", background: "none", color: "var(--primary, #0f766e)", cursor: "pointer", fontWeight: 600 }}>
              נסו שוב
            </button>
          </div>
        </div>
      )}
      {/* Always surface an active-search indicator — even over stale results —
          so it's never ambiguous whether a query is running or just returned
          nothing. Deep search warns about the TAG-IT cold-start wait. */}
      {loading && (
        <div className="loading" role="status" style={{ marginBottom: "0.75rem" }}>
          {mode === "deep"
            ? "מחפש בתוך תוכן המסמכים… (החיפוש הראשון עשוי להימשך עד דקה בזמן שהשרת מתעורר)"
            : "מחפש…"}
        </div>
      )}

      {/* ── Fast metadata results ── */}
      {mode === "fast" && result && (
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
        </>
      )}

      {/* ── Deep full-text results (snippet cards) ── */}
      {mode === "deep" && deepResult && !loading && (
        <>
          <div className="text-sm text-muted" style={{ marginBottom: "0.5rem" }}>
            {deepExact ? `${total.toLocaleString()} תוצאות` : `${deepResult.items.length} תוצאות בעמוד זה`}
          </div>
          {deepResult.items.length === 0 ? (
            <div className="empty-state">
              {q.trim()
                ? "לא נמצאו מסמכים שתוכנם כולל את הביטוי שחיפשת."
                : "הקלד ביטוי כדי לחפש בתוך תוכן המסמכים."}
            </div>
          ) : (
            // Not `.flex` — that class centers items; we want cards stretched full-width.
            <div style={{ display: "flex", flexDirection: "column", gap: "0.6rem", alignItems: "stretch" }}>
              {deepResult.items.map((d, i) => (
                <div key={d.doc_id ?? i} className="card" style={{ padding: "0.75rem 0.9rem" }}>
                  <div className="flex-between" style={{ gap: "0.5rem", alignItems: "baseline", flexWrap: "wrap" }}>
                    <div style={{ fontWeight: 600 }}>
                      {d.link ? (
                        <a href={d.link} target="_blank" rel="noreferrer" style={{ color: "var(--primary)" }}>
                          {d.title || `מסמך ${d.doc_id ?? ""}`}
                        </a>
                      ) : (
                        d.title || `מסמך ${d.doc_id ?? ""}`
                      )}
                    </div>
                    <div className="text-sm text-muted" style={{ whiteSpace: "nowrap" }}>
                      {[d.doc_type, d.date].filter(Boolean).join(" · ")}
                    </div>
                  </div>
                  {d.abstract && (
                    <div className="text-sm" style={{ marginTop: "0.4rem", color: "var(--text)", lineHeight: 1.6 }}>
                      {d.abstract}
                    </div>
                  )}
                  {d.snippet && (
                    <div className="text-sm" style={{ marginTop: "0.4rem", color: "var(--text-muted)", lineHeight: 1.6, borderInlineStart: "3px solid var(--border, #cbd5e1)", paddingInlineStart: "0.6rem" }}>
                      …{cleanSnippet(d.snippet)}…
                    </div>
                  )}
                </div>
              ))}
            </div>
          )}
        </>
      )}

      {/* ── Shared pagination ── */}
      {((mode === "fast" && result) || (mode === "deep" && deepResult && deepResult.items.length > 0)) && (
        <div className="flex-between" style={{ marginTop: "0.75rem", flexWrap: "wrap", gap: "0.5rem" }}>
          <span className="text-sm text-muted">
            {mode === "deep" && !deepExact
              ? `${offset + 1}–${offset + (deepResult?.items.length ?? 0)}`
              : `${total === 0 ? 0 : offset + 1}–${Math.min(offset + PAGE, total)} מתוך ${total.toLocaleString()}`}
          </span>
          <div className="flex" style={{ gap: "0.5rem" }}>
            <button type="button" disabled={offset === 0 || loading}
              onClick={() => { const o = Math.max(0, offset - PAGE); setOffset(o); load(o); }}
              style={{ padding: "0.3rem 0.8rem", border: "1px solid var(--border, #d1d5db)", borderRadius: 4, background: "none", cursor: offset === 0 ? "not-allowed" : "pointer" }}>
              &rarr; הקודם
            </button>
            <button type="button" disabled={!hasNext || loading}
              onClick={() => { const o = offset + PAGE; setOffset(o); load(o); }}
              style={{ padding: "0.3rem 0.8rem", border: "1px solid var(--border, #d1d5db)", borderRadius: 4, background: "none", cursor: !hasNext ? "not-allowed" : "pointer" }}>
              הבא &larr;
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
