/**
 * The graph tab: pick an entity, see its relationship neighbourhood.
 *
 * Opens on the "showcase" pair — two well-connected officials and their orbits,
 * which the server picks and caches daily. That matters for a first visit: an
 * empty graph with a search box teaches nothing, whereas a real conflict-of-
 * interest web is the whole argument for the project.
 */
import { lazy, Suspense, useCallback, useEffect, useState } from "react";
import {
  ocoi,
  OcoiEntityType,
  OcoiGraph,
  OcoiSearchHit,
  OCOI_DEFAULT_EXCLUDE,
} from "../../api/client";
import { Empty, ErrorNote, Spinner, TypeChip } from "./ocoiShared";

const OcoiGraphView = lazy(() => import("./OcoiGraphView"));

export interface GraphTarget {
  type: OcoiEntityType;
  id: string;
  name: string;
}

export default function OcoiGraphTab({
  target,
  onTarget,
}: {
  target: GraphTarget | null;
  onTarget: (t: GraphTarget | null) => void;
}) {
  const [graph, setGraph] = useState<OcoiGraph | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [depth, setDepth] = useState(1);
  const [includeExpenses, setIncludeExpenses] = useState(false);
  const [q, setQ] = useState("");
  const [hits, setHits] = useState<OcoiSearchHit[]>([]);
  const [showcaseNote, setShowcaseNote] = useState(false);

  const exclude = includeExpenses ? undefined : OCOI_DEFAULT_EXCLUDE;

  const load = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      if (target) {
        const res = await ocoi.neighbors(target.id, {
          type: target.type,
          depth,
          ...(exclude ? { exclude_origins: exclude } : {}),
        });
        setGraph(res.data);
        setShowcaseNote(false);
      } else {
        const res = await ocoi.showcase(exclude);
        setGraph(res.data);
        setShowcaseNote(true);
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : "טעינת הגרף נכשלה");
      setGraph(null);
    } finally {
      setLoading(false);
    }
  }, [target, depth, exclude]);

  useEffect(() => {
    load();
  }, [load]);

  const search = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!q.trim()) return;
    try {
      const res = await ocoi.search({ q: q.trim(), limit: 8 });
      setHits(res.data || []);
    } catch {
      setHits([]);
    }
  };

  return (
    <div>
      <div className="flex" style={{ gap: "0.75rem", flexWrap: "wrap", alignItems: "flex-end", marginBottom: "0.75rem" }}>
        <form onSubmit={search} className="flex" style={{ gap: "0.4rem", flex: "1 1 300px" }}>
          <input
            type="search"
            value={q}
            onChange={(e) => setQ(e.target.value)}
            placeholder="מרכז את הגרף על… (שם אדם, חברה, עמותה)"
            style={{ flex: 1, minWidth: 0, padding: "0.5rem 0.7rem" }}
            aria-label="חיפוש ישות למרכז הגרף"
          />
          <button type="submit" className="btn btn-sm" disabled={!q.trim()}>
            חפש
          </button>
        </form>

        <label className="text-sm flex" style={{ gap: "0.35rem", alignItems: "center" }}>
          עומק
          <select
            value={depth}
            onChange={(e) => setDepth(Number(e.target.value))}
            disabled={!target}
            style={{ padding: "0.35rem 0.5rem" }}
          >
            <option value={1}>1</option>
            <option value={2}>2</option>
            <option value={3}>3</option>
          </select>
        </label>

        <label className="text-sm flex" style={{ gap: "0.35rem", alignItems: "center" }}>
          <input
            type="checkbox"
            checked={includeExpenses}
            onChange={(e) => setIncludeExpenses(e.target.checked)}
          />
          כלול הוצאות ח״כים
        </label>
      </div>

      {hits.length > 0 && (
        <div
          className="flex"
          style={{ gap: "0.4rem", flexWrap: "wrap", marginBottom: "0.75rem" }}
        >
          {hits.map((h) => (
            <button
              key={`${h.entity_type}:${h.id}`}
              type="button"
              className="btn btn-sm"
              onClick={() => {
                onTarget({ type: h.entity_type, id: h.id, name: h.name });
                setHits([]);
                setQ("");
              }}
              style={{ display: "flex", gap: "0.35rem", alignItems: "center" }}
            >
              <TypeChip type={h.entity_type} small />
              {h.name}
            </button>
          ))}
        </div>
      )}

      {target && (
        <div className="flex text-sm" style={{ gap: "0.5rem", alignItems: "center", marginBottom: "0.6rem" }}>
          <span className="text-muted">במרכז:</span>
          <TypeChip type={target.type} small />
          <strong>{target.name}</strong>
          <button type="button" className="btn btn-sm" onClick={() => onTarget(null)}>
            חזרה לתצוגת הפתיחה
          </button>
        </div>
      )}

      {showcaseNote && !loading && graph && (
        <div className="text-sm text-muted" style={{ marginBottom: "0.6rem" }}>
          תצוגת פתיחה: שני בעלי תפקידים מקושרים במיוחד והסביבה שלהם. חפש שם למעלה כדי למרכז את
          הגרף על ישות אחרת, או לחץ על צומת בגרף.
        </div>
      )}

      {error && <ErrorNote error={error} />}
      {loading && <Spinner label="בונה את הגרף…" />}

      {!loading && graph && graph.nodes.length > 0 && (
        <Suspense fallback={<Spinner label="טוען את רכיב הגרף…" />}>
          <OcoiGraphView
            graph={graph}
            onSelect={(type, id, name) => onTarget({ type, id, name })}
          />
        </Suspense>
      )}

      {!loading && graph && graph.nodes.length === 0 && (
        <Empty>לא נמצאו קשרים לישות הזאת.</Empty>
      )}
    </div>
  );
}
