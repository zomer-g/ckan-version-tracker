import { useCallback, useEffect, useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { ocal, OcalEvent, OcalSource } from "../../api/client";
import { fmtDateHe, fmtTime, truncate } from "./ocalUtils";
import { Highlight, fieldMatches, searchTerms, snippetAround } from "./ocalHighlight";

const PER_PAGE = 50;

type Sort = "relevance" | "date_desc" | "date_asc";
const SORTS: Sort[] = ["relevance", "date_desc", "date_asc"];

function SourceChip({ name, color, terms }: { name: string; color: string; terms: string[] }) {
  return (
    <span
      style={{
        display: "inline-flex", alignItems: "center", gap: "0.3rem",
        fontSize: "0.75rem", color: "var(--text-muted)",
      }}
    >
      <span aria-hidden style={{ width: 9, height: 9, borderRadius: "50%", background: color || "#3B82F6", flex: "0 0 auto" }} />
      <Highlight text={name} terms={terms} />
    </span>
  );
}

/**
 * Which of the searched fields this row actually matched.
 *
 * These four are what the tsvector is built from; the card shows at most two of
 * them, which is how a hit deep in a participants list or on the source
 * dataset's name reads as "why am I being shown this event". Naming the field
 * (and rendering the ones that would otherwise stay hidden) answers that.
 */
const SEARCHED_FIELDS: [keyof OcalEvent, string][] = [
  ["title", "כותרת"],
  ["location", "מיקום"],
  ["participants", "משתתפים"],
  ["dataset_name", "שם המאגר"],
];

function MatchWhy({ ev, terms }: { ev: OcalEvent; terms: string[] }) {
  if (!terms.length) return null;
  const hit = SEARCHED_FIELDS.filter(([f]) => fieldMatches(ev[f] as string | null, terms));
  if (!hit.length) {
    // Postgres' 'hebrew' config stems; this client-side mirror does not. Rather
    // than let the row look like a mistake, say where the match came from.
    return (
      <div className="text-sm text-muted" style={{ marginTop: "0.3rem" }}>
        ההתאמה נמצאה בטקסט המאונדקס של האירוע (צורות נטויות של מילות החיפוש).
      </div>
    );
  }
  return (
    <div className="text-sm text-muted" style={{ marginTop: "0.3rem" }}>
      התאמה ב: {hit.map(([, label]) => label).join(" · ")}
    </div>
  );
}

function EventCard({ ev, terms }: { ev: OcalEvent; terms: string[] }) {
  const time = [fmtTime(ev.start_time), fmtTime(ev.end_time)].filter(Boolean).join("–");
  const title = <Highlight text={ev.title} terms={terms} />;
  // A hit inside a participants list is usually past the plain 160-char cut, so
  // while searching we window the text around the match instead of its head.
  const participants = terms.length
    ? snippetAround(ev.participants, terms)
    : truncate(ev.participants);
  // dataset_name is searched but never displayed — surface it only when it is
  // the reason this row is here AND it isn't already on screen: for most
  // diaries it repeats the source name in the chip, which now highlights too.
  const datasetHit =
    terms.length > 0 &&
    fieldMatches(ev.dataset_name, terms) &&
    ev.dataset_name !== ev.source_name;
  return (
    <li className="card" style={{ padding: "0.75rem 0.9rem", marginBottom: "0.6rem", listStyle: "none" }}>
      <div style={{ display: "flex", justifyContent: "space-between", gap: "0.75rem", flexWrap: "wrap" }}>
        <div style={{ fontWeight: 600, lineHeight: 1.4 }}>
          {ev.dataset_link ? (
            <a href={ev.dataset_link} target="_blank" rel="noopener noreferrer" style={{ color: "var(--primary)" }}>
              {title}
            <span className="sr-only"> (נפתח בחלון חדש)</span></a>
          ) : (
            title
          )}
        </div>
        <div className="text-sm text-muted" style={{ whiteSpace: "nowrap" }}>
          {fmtDateHe(ev.event_date)}{time ? ` · ${time}` : ""}
        </div>
      </div>
      <div style={{ display: "flex", gap: "0.8rem", flexWrap: "wrap", marginTop: "0.35rem", alignItems: "center" }}>
        <SourceChip name={ev.source_name} color={ev.source_color} terms={terms} />
        {ev.location && (
          <span className="text-sm text-muted">📍 <Highlight text={ev.location} terms={terms} /></span>
        )}
        {typeof ev.match_count === "number" && ev.match_count > 1 && (
          <span className="text-sm" style={{ color: "var(--primary)" }}>
            מופיע ב-{ev.match_count} יומנים
          </span>
        )}
      </div>
      {participants && (
        <div className="text-sm text-muted" style={{ marginTop: "0.3rem", lineHeight: 1.5 }}>
          <Highlight text={participants} terms={terms} />
        </div>
      )}
      {datasetHit && (
        <div className="text-sm text-muted" style={{ marginTop: "0.3rem", lineHeight: 1.5 }}>
          מאגר המקור: <Highlight text={ev.dataset_name} terms={terms} />
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
      <MatchWhy ev={ev} terms={terms} />
    </li>
  );
}

/**
 * Free-text search over the diaries.
 *
 * Every input lives in the URL (?q=&from=&to=&source=&sort=&page=) rather than
 * in component state, for two reasons. A result list is worth sharing — the
 * address bar is the only way to hand someone the search you are looking at.
 * And the old form could show a full unfiltered list while the box held text
 * that had been typed but never submitted, which reads as "the search returned
 * 494,000 meetings, none of them his". The URL is the applied search, the box
 * is a draft, and the two are told apart on screen whenever they differ.
 */
export default function OcalSearch() {
  const [searchParams, setSearchParams] = useSearchParams();

  const applied = useMemo(() => {
    const raw = (searchParams.get("sort") || "") as Sort;
    const q = searchParams.get("q") || "";
    return {
      q,
      from: searchParams.get("from") || "",
      to: searchParams.get("to") || "",
      source: searchParams.get("source") || "",
      // With no explicit sort, relevance is the only useful default for a
      // query and newest-first for a plain browse. Either way it is shown in
      // the dropdown, so the control never disagrees with what was requested.
      sort: (SORTS.includes(raw) ? raw : q ? "relevance" : "date_desc") as Sort,
      page: Math.max(1, Number(searchParams.get("page")) || 1),
    };
  }, [searchParams]);

  const [query, setQuery] = useState(applied.q);
  // Back/forward, or a link someone pasted, changes the applied query from
  // outside this form — follow it.
  useEffect(() => { setQuery(applied.q); }, [applied.q]);

  const [rows, setRows] = useState<OcalEvent[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [sources, setSources] = useState<OcalSource[]>([]);
  const [copied, setCopied] = useState(false);

  useEffect(() => {
    ocal.sources().then((r) => setSources(r.data)).catch(() => {});
  }, []);

  /** Write params, dropping empties; anything but paging returns to page 1. */
  const patch = useCallback(
    (next: Record<string, string>, keepPage = false) => {
      const sp = new URLSearchParams(searchParams);
      if (!keepPage) sp.delete("page");
      for (const [k, v] of Object.entries(next)) {
        if (v) sp.set(k, v);
        else sp.delete(k);
      }
      setSearchParams(sp);
    },
    [searchParams, setSearchParams],
  );

  const { q, from, to, source, sort, page } = applied;

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    ocal
      .events({
        q: q || undefined,
        from_date: from || undefined,
        to_date: to || undefined,
        source_ids: source ? [source] : undefined,
        sort,
        page,
        per_page: PER_PAGE,
      })
      .then((r) => {
        if (cancelled) return;
        setRows(r.data);
        setTotal(r.pagination.total);
      })
      .catch((e) => {
        if (cancelled) return;
        setError(e?.message || "שגיאה בחיפוש");
        setRows([]);
        setTotal(0);
      })
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [q, from, to, source, sort, page]);

  const onSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    patch({ q: query.trim() });
  };

  const terms = useMemo(() => searchTerms(q), [q]);
  const dirty = query.trim() !== q;
  const hasFilters = Boolean(q || from || to || source || searchParams.get("sort"));
  const totalPages = Math.max(1, Math.ceil(total / PER_PAGE));

  const copyLink = async () => {
    try {
      await navigator.clipboard.writeText(window.location.href);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch {
      /* clipboard blocked (insecure context / permission) — no-op */
    }
  };

  const inputStyle: React.CSSProperties = {
    padding: "0.4rem 0.6rem", border: "1px solid var(--border)", borderRadius: 4, fontSize: "0.9rem",
  };
  const ghostBtn: React.CSSProperties = {
    fontSize: "0.85rem", padding: "0.3rem 0.7rem", background: "none",
    border: "1px solid var(--border)", color: "var(--text-muted)",
    borderRadius: 4, cursor: "pointer",
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
          <input type="date" value={from} onChange={(e) => patch({ from: e.target.value })} style={inputStyle} />
        </label>
        <label className="text-sm text-muted">
          עד{" "}
          <input type="date" value={to} onChange={(e) => patch({ to: e.target.value })} style={inputStyle} />
        </label>
        <select
          aria-label="סינון לפי יומן"
          value={source}
          onChange={(e) => patch({ source: e.target.value })}
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
          onChange={(e) => patch({ sort: e.target.value })}
          style={inputStyle}
        >
          <option value="date_desc">חדש → ישן</option>
          <option value="date_asc">ישן → חדש</option>
          <option value="relevance">רלוונטיות</option>
        </select>
        {hasFilters && (
          <>
            <button type="button" onClick={copyLink} title="העתקת קישור לתוצאות האלה" style={ghostBtn}>
              {copied ? "הועתק ✓" : "🔗 העתקת קישור"}
            </button>
            <button type="button" onClick={() => setSearchParams(new URLSearchParams())} style={ghostBtn}>
              ניקוי חיפוש
            </button>
          </>
        )}
      </div>

      {error && <div style={{ color: "var(--danger)", marginBottom: "0.6rem" }}>{error}</div>}

      {/* The applied query, spelled out. The bare count used to be the only
          clue, and a count says nothing about WHAT was searched. */}
      <div className="text-sm text-muted" style={{ marginBottom: "0.5rem" }} role="status">
        {loading
          ? "טוען…"
          : q
            ? `${total.toLocaleString()} תוצאות עבור «${q}»`
            : `${total.toLocaleString()} אירועים (ללא חיפוש)`}
      </div>

      {dirty && (
        <div
          className="text-sm"
          style={{
            marginBottom: "0.6rem", padding: "0.4rem 0.6rem", borderRadius: 4,
            background: "var(--surface-2)", color: "var(--text-muted)",
          }}
        >
          {q ? `התוצאות למטה עדיין מציגות את החיפוש «${q}»` : "התוצאות למטה מציגות את כל האירועים"}
          {" — "}
          לחצו “חיפוש” כדי לחפש {query.trim() ? `«${query.trim()}»` : "מחדש"}.
        </div>
      )}

      <ul style={{ padding: 0, margin: 0 }}>
        {rows.map((ev) => <EventCard key={ev.id} ev={ev} terms={terms} />)}
      </ul>

      {!loading && rows.length === 0 && (
        <div className="text-sm text-muted" style={{ padding: "1rem 0" }}>לא נמצאו אירועים.</div>
      )}

      {total > PER_PAGE && (
        <div style={{ display: "flex", gap: "0.6rem", alignItems: "center", justifyContent: "center", marginTop: "0.8rem" }}>
          <button
            type="button"
            disabled={page <= 1}
            onClick={() => patch({ page: String(Math.max(1, page - 1)) }, true)}
            className="btn-secondary"
          >
            ← הקודם
          </button>
          <span className="text-sm text-muted">עמוד {page} מתוך {totalPages.toLocaleString()}</span>
          <button
            type="button"
            disabled={page >= totalPages}
            onClick={() => patch({ page: String(page + 1) }, true)}
            className="btn-secondary"
          >
            הבא →
          </button>
        </div>
      )}
    </div>
  );
}
