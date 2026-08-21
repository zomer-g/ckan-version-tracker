/**
 * OCOI search tab — free-text across persons / companies / associations / domains.
 *
 * Ports the legacy site's search page. Note that OCOI has no full-text index:
 * the server does an ILIKE substring scan (measured at ~150ms on the migrated
 * corpus), so there is no ranking to surface and results are ordered by name
 * length then alphabetically — shortest match first, which in practice puts the
 * exact entity above the ones that merely contain the term.
 */
import { useCallback, useEffect, useRef, useState } from "react";
import {
  ocoi,
  OcoiEntityType,
  OcoiMeta,
  OcoiSearchHit,
  OcoiStats,
  OCOI_TYPE_LABELS,
} from "../../api/client";
import { Empty, ErrorNote, formatTotal, Pager, Spinner, TypeChip } from "./ocoiShared";

const TYPES: (OcoiEntityType | "")[] = ["", "person", "company", "association", "domain"];
const PER_PAGE = 20;

export default function OcoiSearch({
  onOpenEntity,
  stats,
}: {
  onOpenEntity: (type: OcoiEntityType, id: string, name: string) => void;
  /** Live counters for the empty state. Hardcoding them would drift the moment
   *  an entity is hidden or a new import lands — and it already had: the header
   *  read 8,928 companies while a literal below it said 8,939. */
  stats?: OcoiStats | null;
}) {
  const [q, setQ] = useState("");
  const [type, setType] = useState<OcoiEntityType | "">("");
  const [page, setPage] = useState(1);
  const [hits, setHits] = useState<OcoiSearchHit[]>([]);
  const [meta, setMeta] = useState<OcoiMeta | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [searched, setSearched] = useState(false);

  // Guards against a slow early request overwriting a later one's results.
  const reqId = useRef(0);

  const run = useCallback(
    async (term: string, t: OcoiEntityType | "", p: number) => {
      if (!term.trim()) return;
      const mine = ++reqId.current;
      setLoading(true);
      setError("");
      try {
        const res = await ocoi.search({
          q: term.trim(),
          ...(t ? { type: t } : {}),
          page: p,
          limit: PER_PAGE,
        });
        if (mine !== reqId.current) return;
        setHits(res.data || []);
        setMeta(res.meta || null);
        setSearched(true);
      } catch (e) {
        if (mine !== reqId.current) return;
        setError(e instanceof Error ? e.message : "החיפוש נכשל");
        setHits([]);
        setMeta(null);
      } finally {
        if (mine === reqId.current) setLoading(false);
      }
    },
    [],
  );

  // Re-run when the type filter or page changes, but only once a search exists.
  useEffect(() => {
    if (searched) run(q, type, page);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [type, page]);

  const submit = (e: React.FormEvent) => {
    e.preventDefault();
    setPage(1);
    run(q, type, 1);
  };

  return (
    <div>
      <form onSubmit={submit} className="flex" style={{ gap: "0.5rem", flexWrap: "wrap", marginBottom: "0.9rem" }}>
        <input
          type="search"
          value={q}
          onChange={(e) => setQ(e.target.value)}
          placeholder="חיפוש שם של אדם, חברה, עמותה או תחום…"
          style={{ flex: "1 1 320px", minWidth: 0, padding: "0.55rem 0.8rem", fontSize: "1rem" }}
          aria-label="חיפוש"
        />
        <select
          value={type}
          onChange={(e) => {
            setPage(1);
            setType(e.target.value as OcoiEntityType | "");
          }}
          style={{ padding: "0.55rem 0.7rem", fontSize: "0.95rem" }}
          aria-label="סוג ישות"
        >
          {TYPES.map((t) => (
            <option key={t || "all"} value={t}>
              {t ? OCOI_TYPE_LABELS[t] : "כל הסוגים"}
            </option>
          ))}
        </select>
        <button type="submit" className="btn btn-primary" disabled={!q.trim() || loading}>
          {loading ? "מחפש…" : "חיפוש"}
        </button>
      </form>

      {error && <ErrorNote error={error} />}

      {meta && !loading && (
        <div className="text-sm text-muted" style={{ marginBottom: "0.6rem" }}>
          {meta.total === 0
            ? "לא נמצאו תוצאות"
            : `${formatTotal(meta.total, meta.total_capped)} תוצאות`}
        </div>
      )}

      {loading && <Spinner label="מחפש…" />}

      {!loading && searched && hits.length === 0 && !error && (
        <Empty>לא נמצאו ישויות התואמות לחיפוש.</Empty>
      )}

      {!loading && hits.length > 0 && (
        <ul style={{ listStyle: "none", padding: 0, margin: 0, display: "grid", gap: "0.4rem" }}>
          {hits.map((h) => (
            <li key={`${h.entity_type}:${h.id}`}>
              <button
                type="button"
                onClick={() => onOpenEntity(h.entity_type, h.id, h.name)}
                style={{
                  width: "100%",
                  textAlign: "start",
                  display: "flex",
                  gap: "0.6rem",
                  alignItems: "center",
                  padding: "0.6rem 0.75rem",
                  border: "1px solid var(--border, var(--border))",
                  borderRadius: 8,
                  background: "var(--surface)",
                  cursor: "pointer",
                  fontSize: "0.95rem",
                }}
              >
                <TypeChip type={h.entity_type} small />
                <span style={{ fontWeight: 500 }}>{h.name || "(ללא שם)"}</span>
              </button>
            </li>
          ))}
        </ul>
      )}

      {meta && !loading && <Pager page={meta.page} pages={meta.pages} onPage={setPage} />}

      {!searched && !loading && (
        <Empty>
          {stats
            ? `הקלד שם כדי לחפש בין ${stats.persons.toLocaleString()} אנשים, ` +
              `${stats.companies.toLocaleString()} חברות, ` +
              `${stats.associations.toLocaleString()} עמותות ו-${stats.domains.toLocaleString()} תחומים.`
            : "הקלד שם של אדם, חברה, עמותה או תחום כדי להתחיל."}
        </Empty>
      )}
    </div>
  );
}
