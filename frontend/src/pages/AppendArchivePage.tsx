import { useState, useEffect, useMemo, useCallback, useRef } from "react";
import { useParams, Link } from "react-router-dom";
import { appendArchive, AppendSchema, AppendRows } from "../api/client";

// DD.MM.YYYY HH:MM for the first_seen timestamps (Israel-style, like VersionsPage).
function fmtDate(value: string | null): string {
  if (!value) return "";
  const d = new Date(value);
  if (isNaN(d.getTime())) return value.slice(0, 19);
  const p = (n: number) => String(n).padStart(2, "0");
  return `${p(d.getDate())}.${p(d.getMonth() + 1)}.${d.getFullYear()} ${p(d.getHours())}:${p(d.getMinutes())}`;
}

const PAGE_SIZES = [50, 100, 200];

export default function AppendArchivePage() {
  const { datasetId } = useParams<{ datasetId: string }>();
  const [schema, setSchema] = useState<AppendSchema | null>(null);
  const [data, setData] = useState<AppendRows | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const [limit, setLimit] = useState(50);
  const [offset, setOffset] = useState(0);
  const [sort, setSort] = useState<string | undefined>(undefined);
  const [order, setOrder] = useState<"asc" | "desc">("desc");
  const [q, setQ] = useState("");
  const [filters, setFilters] = useState<Record<string, string>>({});

  // Debounce text inputs so each keystroke doesn't fire a query.
  const [debounced, setDebounced] = useState({ q: "", filters: {} as Record<string, string> });
  const tRef = useRef<number | undefined>(undefined);
  useEffect(() => {
    window.clearTimeout(tRef.current);
    tRef.current = window.setTimeout(() => {
      setOffset(0);
      setDebounced({ q, filters });
    }, 350);
    return () => window.clearTimeout(tRef.current);
  }, [q, filters]);

  useEffect(() => {
    if (!datasetId) return;
    appendArchive
      .schema(datasetId)
      .then(setSchema)
      .catch((e) => setError(e?.message || "schema error"));
  }, [datasetId]);

  const load = useCallback(() => {
    if (!datasetId) return;
    setLoading(true);
    appendArchive
      .rows(datasetId, { limit, offset, sort, order, q: debounced.q, filters: debounced.filters })
      .then((r) => {
        setData(r);
        setError(null);
      })
      .catch((e) => setError(e?.message || "rows error"))
      .finally(() => setLoading(false));
  }, [datasetId, limit, offset, sort, order, debounced]);

  useEffect(() => {
    load();
  }, [load]);

  const cols = schema?.columns || data?.columns || [];
  const total = data?.total ?? schema?.total ?? 0;
  const pageStart = total === 0 ? 0 : offset + 1;
  const pageEnd = Math.min(offset + limit, total);

  function toggleSort(col: string) {
    if (sort === col) {
      setOrder((o) => (o === "asc" ? "desc" : "asc"));
    } else {
      setSort(col);
      setOrder("asc");
    }
    setOffset(0);
  }

  const downloadHref = useMemo(
    () =>
      datasetId
        ? appendArchive.downloadUrl(datasetId, { sort, order, q: debounced.q, filters: debounced.filters })
        : "#",
    [datasetId, sort, order, debounced],
  );

  if (error && !schema) {
    return (
      <div className="container mt-3">
        <div className="empty-state">{error}</div>
        <Link to="/" style={{ color: "var(--primary)" }}>&larr; חזרה</Link>
      </div>
    );
  }

  return (
    <div className="container mt-3">
      <div className="page-header flex-between" style={{ flexWrap: "wrap", gap: "0.75rem" }}>
        <div>
          <h1 style={{ margin: 0 }}>{schema?.dataset_title || "ארכיון מצטבר"}</h1>
          <div className="text-sm text-muted" style={{ marginTop: "0.25rem" }}>
            ארכיון מצטבר (APPEND) · {total.toLocaleString()} שורות
            {schema?.key ? <> · מפתח: <code>{schema.key}</code></> : <> · לכידת כל מצב</>}
            {" · עמודת "}<code>first_seen</code>{" = זמן הוספת השורה"}
          </div>
        </div>
        <div className="flex" style={{ alignItems: "center", gap: "1rem" }}>
          <a
            href={downloadHref}
            style={{
              fontSize: "0.85rem",
              padding: "0.4rem 0.9rem",
              background: "var(--primary, #0f766e)",
              color: "white",
              borderRadius: 4,
              textDecoration: "none",
              fontWeight: 500,
            }}
          >
            &#8595; הורד CSV{(debounced.q || Object.values(debounced.filters).some(Boolean)) ? " (מסונן)" : ""}
          </a>
          {datasetId && (
            <Link to={`/versions/${datasetId}`} style={{ fontSize: "0.85rem", color: "var(--text-muted)", textDecoration: "none" }}>
              גרסאות &rarr;
            </Link>
          )}
          <Link to="/" style={{ fontSize: "0.85rem", color: "var(--text-muted)", textDecoration: "none" }}>
            &larr; חזרה
          </Link>
        </div>
      </div>

      <div className="flex" style={{ gap: "0.75rem", alignItems: "center", margin: "0.5rem 0 1rem", flexWrap: "wrap" }}>
        <input
          type="search"
          value={q}
          onChange={(e) => setQ(e.target.value)}
          placeholder="חיפוש חופשי בכל העמודות…"
          style={{ flex: "1 1 280px", padding: "0.45rem 0.7rem", border: "1px solid var(--border, #d1d5db)", borderRadius: 4 }}
          aria-label="חיפוש חופשי"
        />
        <label className="text-sm text-muted">
          שורות בעמוד:{" "}
          <select value={limit} onChange={(e) => { setLimit(Number(e.target.value)); setOffset(0); }}>
            {PAGE_SIZES.map((n) => <option key={n} value={n}>{n}</option>)}
          </select>
        </label>
      </div>

      <div className="card" style={{ padding: 0, overflowX: "auto" }}>
        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.85rem", whiteSpace: "nowrap" }}>
          <thead>
            <tr style={{ background: "var(--bg-muted, #f8fafc)", borderBottom: "2px solid var(--border, #e5e7eb)" }}>
              {cols.map((c) => (
                <th
                  key={c}
                  onClick={() => toggleSort(c)}
                  title="מיון"
                  style={{ textAlign: "start", padding: "0.5rem 0.7rem", cursor: "pointer", position: "sticky", top: 0, userSelect: "none" }}
                >
                  {c}{sort === c ? (order === "asc" ? " ▲" : " ▼") : ""}
                </th>
              ))}
            </tr>
            <tr style={{ borderBottom: "1px solid var(--border, #e5e7eb)" }}>
              {cols.map((c) => (
                <th key={c} style={{ padding: "0.25rem 0.4rem" }}>
                  <input
                    value={filters[c] || ""}
                    onChange={(e) => setFilters((f) => ({ ...f, [c]: e.target.value }))}
                    placeholder="סנן…"
                    aria-label={`סנן ${c}`}
                    style={{ width: "100%", minWidth: 80, padding: "0.2rem 0.35rem", border: "1px solid var(--border, #e5e7eb)", borderRadius: 3, fontSize: "0.78rem", fontWeight: 400 }}
                  />
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {loading && (
              <tr><td colSpan={cols.length || 1} style={{ padding: "1rem", textAlign: "center", color: "var(--text-muted)" }}>טוען…</td></tr>
            )}
            {!loading && data && data.rows.length === 0 && (
              <tr><td colSpan={cols.length || 1} style={{ padding: "1rem", textAlign: "center", color: "var(--text-muted)" }}>אין שורות תואמות</td></tr>
            )}
            {!loading && data?.rows.map((row, i) => (
              <tr key={i} style={{ borderBottom: "1px solid var(--border, #f1f5f9)" }}>
                {cols.map((c) => (
                  <td key={c} style={{ padding: "0.4rem 0.7rem" }}>
                    {c === "first_seen" ? fmtDate(row[c]) : (row[c] ?? "")}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="flex-between" style={{ marginTop: "0.75rem", flexWrap: "wrap", gap: "0.5rem" }}>
        <span className="text-sm text-muted">
          {pageStart.toLocaleString()}–{pageEnd.toLocaleString()} מתוך {total.toLocaleString()}
        </span>
        <div className="flex" style={{ gap: "0.5rem" }}>
          <button type="button" className="btn" disabled={offset === 0 || loading} onClick={() => setOffset(Math.max(0, offset - limit))}
            style={{ padding: "0.3rem 0.8rem", border: "1px solid var(--border, #d1d5db)", borderRadius: 4, background: "none", cursor: offset === 0 ? "not-allowed" : "pointer" }}>
            &rarr; הקודם
          </button>
          <button type="button" className="btn" disabled={pageEnd >= total || loading} onClick={() => setOffset(offset + limit)}
            style={{ padding: "0.3rem 0.8rem", border: "1px solid var(--border, #d1d5db)", borderRadius: 4, background: "none", cursor: pageEnd >= total ? "not-allowed" : "pointer" }}>
            הבא &larr;
          </button>
        </div>
      </div>
    </div>
  );
}
