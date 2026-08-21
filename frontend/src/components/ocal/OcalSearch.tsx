import { useCallback, useEffect, useRef, useState } from "react";
import { ocal, OcalEvent, OcalSource } from "../../api/client";
import { fmtDateHe, fmtTime, truncate } from "./ocalUtils";

const PER_PAGE = 50;

type Sort = "relevance" | "date_desc" | "date_asc";

function SourceChip({ name, color }: { name: string; color: string }) {
  return (
    <span
      style={{
        display: "inline-flex", alignItems: "center", gap: "0.3rem",
        fontSize: "0.75rem", color: "var(--text-muted)",
      }}
    >
      <span aria-hidden style={{ width: 9, height: 9, borderRadius: "50%", background: color || "#3B82F6", flex: "0 0 auto" }} />
      {name}
    </span>
  );
}

function EventCard({ ev }: { ev: OcalEvent }) {
  const time = [fmtTime(ev.start_time), fmtTime(ev.end_time)].filter(Boolean).join("–");
  return (
    <li className="card" style={{ padding: "0.75rem 0.9rem", marginBottom: "0.6rem", listStyle: "none" }}>
      <div style={{ display: "flex", justifyContent: "space-between", gap: "0.75rem", flexWrap: "wrap" }}>
        <div style={{ fontWeight: 600, lineHeight: 1.4 }}>
          {ev.dataset_link ? (
            <a href={ev.dataset_link} target="_blank" rel="noopener noreferrer" style={{ color: "var(--primary)" }}>
              {ev.title}
            <span className="sr-only"> (נפתח בחלון חדש)</span></a>
          ) : (
            ev.title
          )}
        </div>
        <div className="text-sm text-muted" style={{ whiteSpace: "nowrap" }}>
          {fmtDateHe(ev.event_date)}{time ? ` · ${time}` : ""}
        </div>
      </div>
      <div style={{ display: "flex", gap: "0.8rem", flexWrap: "wrap", marginTop: "0.35rem", alignItems: "center" }}>
        <SourceChip name={ev.source_name} color={ev.source_color} />
        {ev.location && <span className="text-sm text-muted">📍 {ev.location}</span>}
        {typeof ev.match_count === "number" && ev.match_count > 1 && (
          <span className="text-sm" style={{ color: "var(--primary)" }}>
            מופיע ב-{ev.match_count} יומנים
          </span>
        )}
      </div>
      {ev.participants && (
        <div className="text-sm text-muted" style={{ marginTop: "0.3rem", lineHeight: 1.5 }}>
          {truncate(ev.participants)}
        </div>
      )}
      {ev.top_entities && ev.top_entities.length > 0 && (
        <div style={{ display: "flex", gap: "0.35rem", flexWrap: "wrap", marginTop: "0.4rem" }}>
          {ev.top_entities.map((e, i) => (
            <span
              key={`${e.name}-${i}`}
              style={{
                fontSize: "0.72rem", padding: "0.1rem 0.45rem", borderRadius: 10,
                background: "var(--surface-2)", color: "var(--text-muted)",
              }}
            >
              {e.name}
            </span>
          ))}
        </div>
      )}
    </li>
  );
}

export default function OcalSearch() {
  const [query, setQuery] = useState("");
  const [activeQuery, setActiveQuery] = useState("");
  const [fromDate, setFromDate] = useState("");
  const [toDate, setToDate] = useState("");
  const [sourceId, setSourceId] = useState("");
  const [sort, setSort] = useState<Sort>("date_desc");
  const [page, setPage] = useState(1);

  const [rows, setRows] = useState<OcalEvent[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [sources, setSources] = useState<OcalSource[]>([]);

  const abortRef = useRef<AbortController | null>(null);

  useEffect(() => {
    ocal.sources().then((r) => setSources(r.data)).catch(() => {});
  }, []);

  const load = useCallback(() => {
    setLoading(true);
    setError(null);
    ocal
      .events({
        q: activeQuery || undefined,
        from_date: fromDate || undefined,
        to_date: toDate || undefined,
        source_ids: sourceId ? [sourceId] : undefined,
        sort: activeQuery && sort === "date_desc" ? "relevance" : sort,
        page,
        per_page: PER_PAGE,
      })
      .then((r) => {
        setRows(r.data);
        setTotal(r.pagination.total);
      })
      .catch((e) => {
        setError(e?.message || "שגיאה בחיפוש");
        setRows([]);
        setTotal(0);
      })
      .finally(() => setLoading(false));
  }, [activeQuery, fromDate, toDate, sourceId, sort, page]);

  useEffect(() => {
    load();
    return () => abortRef.current?.abort();
  }, [load]);

  const onSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    setPage(1);
    setActiveQuery(query.trim());
  };

  const totalPages = Math.max(1, Math.ceil(total / PER_PAGE));

  const inputStyle: React.CSSProperties = {
    padding: "0.4rem 0.6rem", border: "1px solid var(--border)", borderRadius: 4, fontSize: "0.9rem",
  };

  return (
    <div>
      <form onSubmit={onSubmit} role="search" style={{ display: "flex", gap: "0.5rem", flexWrap: "wrap", marginBottom: "0.6rem" }}>
        <input
          type="search"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="חיפוש חופשי ביומנים (תומך ב-AND/OR/NOT)…"
          aria-label="חיפוש ביומנים"
          style={{ ...inputStyle, flex: "1 1 320px" }}
        />
        <button type="submit" className="btn-primary" disabled={loading}>
          {loading ? "מחפש…" : "חיפוש"}
        </button>
      </form>

      <div style={{ display: "flex", gap: "0.5rem", flexWrap: "wrap", marginBottom: "0.8rem", alignItems: "center" }}>
        <label className="text-sm text-muted">
          מתאריך{" "}
          <input type="date" value={fromDate} onChange={(e) => { setPage(1); setFromDate(e.target.value); }} style={inputStyle} />
        </label>
        <label className="text-sm text-muted">
          עד{" "}
          <input type="date" value={toDate} onChange={(e) => { setPage(1); setToDate(e.target.value); }} style={inputStyle} />
        </label>
        <select
          aria-label="סינון לפי יומן"
          value={sourceId}
          onChange={(e) => { setPage(1); setSourceId(e.target.value); }}
          style={{ ...inputStyle, maxWidth: 280 }}
        >
          <option value="">כל היומנים ({sources.length})</option>
          {sources.map((s) => (
            <option key={s.id} value={s.id}>{s.name}</option>
          ))}
        </select>
        <select
          aria-label="מיון"
          value={sort}
          onChange={(e) => { setPage(1); setSort(e.target.value as Sort); }}
          style={inputStyle}
        >
          <option value="date_desc">חדש → ישן</option>
          <option value="date_asc">ישן → חדש</option>
          <option value="relevance">רלוונטיות</option>
        </select>
      </div>

      {error && <div style={{ color: "var(--danger)", marginBottom: "0.6rem" }}>{error}</div>}

      <div className="text-sm text-muted" style={{ marginBottom: "0.5rem" }} role="status">
        {loading ? "טוען…" : `${total.toLocaleString()} תוצאות`}
      </div>

      <ul style={{ padding: 0, margin: 0 }}>
        {rows.map((ev) => <EventCard key={ev.id} ev={ev} />)}
      </ul>

      {!loading && rows.length === 0 && (
        <div className="text-sm text-muted" style={{ padding: "1rem 0" }}>לא נמצאו אירועים.</div>
      )}

      {total > PER_PAGE && (
        <div style={{ display: "flex", gap: "0.6rem", alignItems: "center", justifyContent: "center", marginTop: "0.8rem" }}>
          <button type="button" disabled={page <= 1} onClick={() => setPage((p) => Math.max(1, p - 1))} className="btn-secondary">
            ← הקודם
          </button>
          <span className="text-sm text-muted">עמוד {page} מתוך {totalPages.toLocaleString()}</span>
          <button type="button" disabled={page >= totalPages} onClick={() => setPage((p) => p + 1)} className="btn-secondary">
            הבא →
          </button>
        </div>
      )}
    </div>
  );
}
