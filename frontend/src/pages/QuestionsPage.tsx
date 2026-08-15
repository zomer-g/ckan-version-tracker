/**
 * שאלות לעם — חיפוש רוחבי across everything OVER exposes over MCP.
 *
 * One question, fanned out to every corpus: the tracked-dataset catalog, the
 * SQL table catalog, the CBS index, Knesset committee protocols, ממ״מ research
 * papers, officials' calendars, and the corporate registries.
 *
 * ORCHESTRATION lives here, and it is deliberately client-side: the page issues
 * ONE request per source, so each column paints the moment it lands instead of
 * the whole page waiting on the slowest corpus. Sources that share an MCP
 * server run one after another; different servers run concurrently. There is no
 * SSE anywhere in this app and this needs none.
 *
 * The two display modes are the point of the page:
 *   לרוחב  — a column per source, side by side, for comparing corpora.
 *   לאורך  — one merged list, round-robin across sources. That is NOT a
 *            cross-source relevance score (none exists); the copy says so.
 */
import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";

import { deepSearch } from "../api/client";
import type { DeepSource } from "../api/client";
import {
  Attribution,
  ColState,
  mergeInterleaved,
  ResultCard,
  SourceChips,
  SourceColumn,
  SourceFilterBox,
} from "../components/DeepSearchResults";

type Mode = "bysource" | "byrelevance";

const MODE_KEY = "overQuestions.mode";
const HIDDEN_KEY = "overQuestions.hidden";
const PER_SOURCE = 15;

function readMode(params: URLSearchParams): Mode {
  // A shared link must render what the sharer saw, so the URL wins over the
  // local preference.
  const fromUrl = params.get("mode");
  if (fromUrl === "bysource" || fromUrl === "byrelevance") return fromUrl;
  try {
    return localStorage.getItem(MODE_KEY) === "bysource" ? "bysource" : "byrelevance";
  } catch {
    return "byrelevance";
  }
}

function readHidden(params: URLSearchParams): Set<string> {
  const fromUrl = params.get("hide");
  if (fromUrl !== null) return new Set(fromUrl.split(",").filter(Boolean));
  try {
    const raw = localStorage.getItem(HIDDEN_KEY);
    return new Set(raw ? (JSON.parse(raw) as string[]) : []);
  } catch {
    return new Set();
  }
}

export default function QuestionsPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  // Seed from the URL exactly once — re-seeding fights the controlled inputs.
  const initial = useMemo(() => new URLSearchParams(searchParams), []); // eslint-disable-line react-hooks/exhaustive-deps

  const [sources, setSources] = useState<DeepSource[]>([]);
  const [sourcesError, setSourcesError] = useState<string | null>(null);
  const [q, setQ] = useState(() => initial.get("q") || "");
  const [lastQuery, setLastQuery] = useState("");
  const [mode, setMode] = useState<Mode>(() => readMode(initial));
  const [hidden, setHidden] = useState<Set<string>>(() => readHidden(initial));
  const [filters, setFilters] = useState<Record<string, Record<string, string>>>({});
  const [openFilter, setOpenFilter] = useState<string | null>(null);
  const [results, setResults] = useState<Record<string, ColState>>({});
  const [searched, setSearched] = useState(false);

  // Every in-flight column checks this after its await: with N requests in the
  // air, fast typing would otherwise paint columns from three different queries.
  const runId = useRef(0);
  // Read inside async loops, where the state closure would be stale.
  const filtersRef = useRef(filters);
  const hiddenRef = useRef(hidden);
  useEffect(() => {
    filtersRef.current = filters;
  }, [filters]);
  useEffect(() => {
    hiddenRef.current = hidden;
  }, [hidden]);

  useEffect(() => {
    deepSearch
      .sources()
      .then((r) => setSources(r.sources))
      .catch((e) => setSourcesError(e?.message || "שגיאה בטעינת רשימת המקורות"));
  }, []);

  const fillColumn = useCallback(
    async (src: DeepSource, query: string, rid: number) => {
      setResults((p) => ({ ...p, [src.id]: { status: "loading" } }));
      try {
        const r = await deepSearch.search({
          q: query,
          sources: src.id,
          limit: PER_SOURCE,
          filters: filtersRef.current[src.id],
        });
        if (rid !== runId.current) return;
        setResults((p) => ({ ...p, [src.id]: { status: "done", column: r.sources[0] } }));
      } catch (e: any) {
        if (rid !== runId.current) return;
        setResults((p) => ({
          ...p,
          [src.id]: { status: "error", error: e?.message || "שגיאה בחיפוש" },
        }));
      }
    },
    [],
  );

  const runSearch = useCallback(
    (query: string, list: DeepSource[]) => {
      const qq = query.trim();
      if (!qq || list.length === 0) return;
      const rid = ++runId.current;
      setLastQuery(qq);
      setSearched(true);

      const visible = list.filter((s) => !hiddenRef.current.has(s.id));
      setResults(
        Object.fromEntries(visible.map((s) => [s.id, { status: "queued" } as ColState])),
      );

      // ONE setSearchParams call: two functional updates in the same tick
      // clobber each other in react-router 7.
      const p: Record<string, string> = { q: qq };
      if (mode === "bysource") p.mode = "bysource";
      if (hiddenRef.current.size) p.hide = [...hiddenRef.current].join(",");
      setSearchParams(p, { replace: true });

      // Group by MCP server: groups concurrently, members sequentially, so two
      // corpora on the same server never hit its pool at once.
      const groups = new Map<string, DeepSource[]>();
      for (const s of visible) groups.set(s.server, [...(groups.get(s.server) ?? []), s]);
      for (const group of groups.values()) {
        void (async () => {
          for (const s of group) {
            if (rid !== runId.current) return;
            await fillColumn(s, qq, rid);
          }
        })();
      }
    },
    [mode, setSearchParams, fillColumn],
  );

  // Deep link: run once, as soon as the registry has arrived.
  const autoRan = useRef(false);
  useEffect(() => {
    if (autoRan.current || sources.length === 0) return;
    autoRan.current = true;
    const q0 = initial.get("q");
    if (q0 && q0.trim()) runSearch(q0, sources);
  }, [sources, initial, runSearch]);

  const toggleSource = (id: string) => {
    const next = new Set(hiddenRef.current);
    if (next.has(id)) next.delete(id);
    else next.add(id);
    hiddenRef.current = next;
    setHidden(next);
    try {
      localStorage.setItem(HIDDEN_KEY, JSON.stringify([...next]));
    } catch {
      /* private mode — the preference just won't persist */
    }
    if (!next.has(id) && lastQuery) {
      const s = sources.find((x) => x.id === id);
      if (s && !results[id]?.column) void fillColumn(s, lastQuery, runId.current);
    }
  };

  const setAllHidden = (next: Set<string>) => {
    hiddenRef.current = next;
    setHidden(next);
    try {
      localStorage.setItem(HIDDEN_KEY, JSON.stringify([...next]));
    } catch {
      /* ignore */
    }
    if (lastQuery) runSearch(lastQuery, sources);
  };

  const changeMode = (m: Mode) => {
    setMode(m);
    try {
      localStorage.setItem(MODE_KEY, m);
    } catch {
      /* ignore */
    }
  };

  const onFilterChange = (srcId: string, filterId: string, value: string) => {
    // Set the ref eagerly so the immediate re-run below reads the new value.
    const next = {
      ...filtersRef.current,
      [srcId]: { ...(filtersRef.current[srcId] || {}), [filterId]: value },
    };
    filtersRef.current = next;
    setFilters(next);
    if (lastQuery) {
      const s = sources.find((x) => x.id === srcId);
      if (s) void fillColumn(s, lastQuery, runId.current);
    }
  };

  const visible = sources.filter((s) => !hidden.has(s.id));
  const done = visible.filter((s) => results[s.id]?.status === "done").length;
  const totalCards = visible.reduce(
    (n, s) => n + (results[s.id]?.column?.results.length ?? 0),
    0,
  );
  const openFilterSource = openFilter ? sources.find((s) => s.id === openFilter) : null;

  return (
    <div className="container mt-3" dir="rtl">
      <div className="page-header">
        <h1>שאלות לעם</h1>
        <p className="text-muted" style={{ maxWidth: 760, lineHeight: 1.6 }}>
          שאלה אחת, חיפוש בכל מה שגרסאות לעם אוספת — מאגרי המידע שבמעקב, טבלאות
          מסד הנתונים, אינדקס הלמ״ס, פרוטוקולי ועדות הכנסת, מסמכי ממ״מ, יומני בעלי
          תפקידים ומרשמי התאגידים. כל מקור עונה בנפרד ומופיע ברגע שהוא מוכן.
        </p>
      </div>

      <form
        role="search"
        onSubmit={(e) => {
          e.preventDefault();
          runSearch(q, sources);
        }}
        className="card"
        style={{
          padding: "0.85rem",
          marginBottom: "1rem",
          display: "flex",
          gap: "0.6rem",
          flexWrap: "wrap",
          alignItems: "center",
        }}
      >
        <input
          type="search"
          value={q}
          onChange={(e) => setQ(e.target.value)}
          placeholder='למשל: תקציב · "תקציב הביטחון" · דיור -ירושלים'
          aria-label="טקסט לחיפוש בכל המקורות"
          style={{
            flex: "1 1 260px",
            padding: "0.5rem 0.7rem",
            border: "1px solid var(--border, #d1d5db)",
            borderRadius: "var(--radius, 8px)",
            fontSize: "0.95rem",
          }}
        />
        {/* btn-primary rather than a hand-rolled style, so this button is the
            site's button — same radius, colour, hover and focus ring. */}
        {/* The button needs the source list to know what to query, so it stays
            disabled until /sources lands — but it must SAY that. It previously
            showed a spinner cursor and nothing else, which reads as a broken
            button rather than a page still loading. */}
        <button
          type="submit"
          className="btn-primary"
          disabled={sources.length === 0}
          title={
            sourcesError
              ? sourcesError
              : sources.length === 0
                ? "טוען את רשימת המקורות…"
                : undefined
          }
          style={{ cursor: sources.length === 0 ? "progress" : "pointer" }}
        >
          {sources.length === 0 && !sourcesError ? "טוען מקורות…" : "🔍 חיפוש רוחבי"}
        </button>

        <div
          role="tablist"
          aria-label="תצוגת תוצאות"
          style={{ display: "flex", gap: "0.35rem", flexWrap: "wrap" }}
        >
          {(
            [
              ["byrelevance", "לאורך — רשימה אחת"],
              ["bysource", "לרוחב — עמודה לכל מקור"],
            ] as [Mode, string][]
          ).map(([m, label]) => (
            <button
              key={m}
              type="button"
              role="tab"
              aria-selected={mode === m}
              onClick={() => changeMode(m)}
              style={{
                // A segmented control of BUTTONS, so --radius like every other
                // button on the site — full-round is reserved for chips/badges.
                padding: "0.4rem 0.9rem",
                borderRadius: "var(--radius, 8px)",
                cursor: "pointer",
                fontSize: "0.82rem",
                fontWeight: 600,
                border: `1px solid ${mode === m ? "var(--primary-700, #06607C)" : "var(--border, #d1d5db)"}`,
                background: mode === m ? "var(--primary-700, #06607C)" : "transparent",
                color: mode === m ? "#fff" : "var(--primary-700, #06607C)",
              }}
            >
              {label}
            </button>
          ))}
        </div>
      </form>

      {/* The syntax is only discoverable if we say it out loud. Grouping
          parentheses are deliberately absent: the full-text backend does not
          honour them, so offering them would quietly mislead. */}
      <div
        className="text-sm text-muted"
        style={{ marginTop: "-0.5rem", marginBottom: "0.85rem", fontSize: "0.78rem" }}
      >
        <strong>"מרכאות"</strong> לביטוי מדויק · <strong>-מילה</strong> להחרגה ·{" "}
        <strong>OR</strong> לחלופה. בעמודות הטקסט המלא החיפוש רץ בתוך גוף המסמך,
        ובשאר לפי מטא-דאטה.{" "}
        {/* Not a footnote: the full-text index does not stem, so a user who
            searches תקציב and misses התקציב will read it as "no such document"
            rather than "different prefix". */}
        <span style={{ opacity: 0.85 }}>
          שימו לב: החיפוש בגוף המסמך אינו מזהה הטיות — <strong>תקציב</strong> לא
          ימצא <strong>התקציב</strong> או <strong>בתקציב</strong>.
        </span>
      </div>

      {sourcesError && (
        <div role="alert" className="badge badge-danger mb-2">
          {sourcesError}
        </div>
      )}

      {sources.length > 0 && (
        <SourceChips
          sources={sources}
          hidden={hidden}
          states={results}
          openFilter={openFilter}
          onToggle={toggleSource}
          onOpenFilter={setOpenFilter}
          onSelectAll={() => setAllHidden(new Set())}
          onClearAll={() => setAllHidden(new Set(sources.map((s) => s.id)))}
        />
      )}

      {openFilterSource && (
        <SourceFilterBox
          source={openFilterSource}
          values={filters[openFilterSource.id] || {}}
          onChange={(fid, v) => onFilterChange(openFilterSource.id, fid, v)}
          onClose={() => setOpenFilter(null)}
        />
      )}

      {searched && (
        <div
          aria-live="polite"
          aria-atomic="true"
          className="text-sm text-muted"
          style={{ marginBottom: "0.75rem" }}
        >
          {/* Plain quotes, not «» — those are the highlight markers, and using
              them here too would read as if the whole query had matched. */}
          “{lastQuery}” · {totalCards.toLocaleString("he-IL")} תוצאות ·{" "}
          {done}/{visible.length} מקורות
          {done < visible.length ? " · מחפש…" : ""}
        </div>
      )}

      {!searched && (
        <div className="empty-state">
          הקלידו שאלה או מונח ולחצו «חיפוש רוחבי» — כל המקורות ייחפשו במקביל.
        </div>
      )}

      {searched && visible.length === 0 && (
        <div className="empty-state">לא נבחרו מקורות לחיפוש.</div>
      )}

      {searched && visible.length > 0 && mode === "bysource" && (
        <div
          style={{
            display: "flex",
            gap: "14px",
            alignItems: "flex-start",
            overflowX: "auto",
            paddingBottom: "0.75rem",
          }}
        >
          {visible.map((s) => (
            <SourceColumn
              key={s.id}
              source={s}
              state={results[s.id]}
              onHide={toggleSource}
            />
          ))}
        </div>
      )}

      {searched && visible.length > 0 && mode === "byrelevance" && (
        <MergedList sources={visible} results={results} pending={visible.length - done} />
      )}
    </div>
  );
}

function MergedList({
  sources,
  results,
  pending,
}: {
  sources: DeepSource[];
  results: Record<string, ColState>;
  pending: number;
}) {
  const merged = mergeInterleaved(sources, results);
  const errored = sources.filter(
    (s) => results[s.id]?.error || results[s.id]?.column?.error,
  );
  return (
    <div style={{ maxWidth: 900 }}>
      <div className="text-sm text-muted" style={{ marginBottom: "0.6rem" }}>
        התוצאה המובילה מכל מקור, אחריה השנייה מכל מקור וכן הלאה — אין דירוג
        רלוונטיות משותף בין המקורות.
      </div>

      {/* Not `.flex` — that utility centers items; cards must stretch. */}
      <div style={{ display: "flex", flexDirection: "column", gap: "0.55rem" }}>
        {merged.map(({ card, source }, i) => (
          <ResultCard
            key={`${source.id}-${card.title}-${i}`}
            card={card}
            source={source}
            showSource
          />
        ))}
      </div>

      {pending > 0 && (
        <div className="loading" role="status">
          טוען… ({pending} מקורות בהמתנה)
        </div>
      )}

      {merged.length === 0 && pending === 0 && (
        <div className="empty-state">לא נמצאו תוצאות באף אחד מהמקורות.</div>
      )}

      {errored.length > 0 && (
        <div
          className="text-sm"
          style={{ marginTop: "0.75rem", color: "var(--danger, #dc2626)" }}
        >
          מקורות שנכשלו:{" "}
          {errored
            .map((s) => `${s.name} (${results[s.id]?.error || results[s.id]?.column?.error})`)
            .join(" · ")}
        </div>
      )}

      {/* Attribution is required per source, in both display modes. */}
      <div style={{ marginTop: "1rem", borderTop: "1px solid var(--border, #e5e7eb)", paddingTop: "0.6rem" }}>
        {sources.map((s) => (
          <Attribution key={s.id} source={s} />
        ))}
      </div>
    </div>
  );
}
