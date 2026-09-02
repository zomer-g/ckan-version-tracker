/**
 * Cytoscape rendering of an OCOI relationship subgraph.
 *
 * Cytoscape (~400KB) is imported DYNAMICALLY inside the effect rather than at
 * module scope: OVER is one bundle serving many projects, and a graph library
 * that only /projects/ocoi uses has no business loading on the dataset pages.
 *
 * The layout is fcose — a force-directed layout that handles the shape this
 * data actually has (a few high-degree hubs with long thin chains hanging off
 * them) far better than a grid or a circle, which is why OCOI chose it.
 */
import { useEffect, useRef, useState } from "react";
import { OcoiEntityType, OcoiGraph } from "../../api/client";
import { TYPE_COLORS } from "./ocoiShared";
import OcoiConnectionTable from "./OcoiConnectionTable";
import {
  dominantRelType,
  edgeLabel,
  EDGE_COLORS,
  EDGE_FALLBACK_COLOR,
  EDGE_PRIORITY,
} from "./ocoiGraphLabels";

/**
 * Resolve the palette to concrete colours.
 *
 * The shared TYPE_COLORS are CSS custom properties, which is right for the
 * legend and the chips — they are DOM, so `var(--fill-teal)` just works, and
 * they follow the theme for free. Cytoscape does NOT: it paints to a canvas
 * and never consults the cascade, so a `var(...)` reaches it as an
 * uninterpretable string and every node silently fell back to one grey. The
 * legend promised four colours and the graph drew none of them.
 *
 * Read once per render pass, against the live document, so both themes get
 * their own values.
 */
function resolvePalette() {
  const cs = getComputedStyle(document.documentElement);
  const v = (name: string, fallback: string) =>
    cs.getPropertyValue(name).trim() || fallback;
  const raw = (c: string, fallback: string) => {
    const m = c.match(/^var\(\s*(--[\w-]+)\s*\)$/);
    return m ? v(m[1], fallback) : c;
  };
  const edge: Record<string, string> = {};
  for (const [k, token] of Object.entries(EDGE_COLORS)) {
    edge[k] = raw(token, "#464F5E");
  }
  return {
    node: {
      person: raw(TYPE_COLORS.person, "#12564F"),
      company: raw(TYPE_COLORS.company, "#833909"),
      association: raw(TYPE_COLORS.association, "#5B21B6"),
      domain: raw(TYPE_COLORS.domain, "#14458F"),
    } as Record<OcoiEntityType, string>,
    rel: edge,
    relFallback: raw(EDGE_FALLBACK_COLOR, "#464F5E"),
    text: v("--text", "#16161a"),
    // The label halo: labels sit over edges and other nodes, and without a
    // ring of the page colour behind them they are read against whatever
    // happens to be underneath.
    halo: v("--surface", "#ffffff"),
    edge: v("--border", "#94a3b8"),
    muted: v("--text-muted", "#5c5c66"),
  };
}

export default function OcoiGraphView({
  graph,
  height = 520,
  onSelect,
}: {
  graph: OcoiGraph;
  height?: number;
  onSelect?: (type: OcoiEntityType, id: string, name: string) => void;
}) {
  const box = useRef<HTMLDivElement | null>(null);
  const cyRef = useRef<{ destroy: () => void } | null>(null);
  const [err, setErr] = useState("");
  const onSelectRef = useRef(onSelect);
  onSelectRef.current = onSelect;

  // The palette is baked into a canvas, so unlike every DOM element on the
  // page the graph does NOT follow a theme switch on its own — it would keep
  // painting the old theme's colours until something else forced a rebuild.
  // Bumping this key on a theme change is what makes it follow.
  const [themeKey, setThemeKey] = useState(0);
  useEffect(() => {
    const bump = () => setThemeKey((n) => n + 1);
    const obs = new MutationObserver(bump);
    obs.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ["data-theme", "class"],
    });
    const mq = window.matchMedia("(prefers-color-scheme: dark)");
    mq.addEventListener("change", bump);
    return () => {
      obs.disconnect();
      mq.removeEventListener("change", bump);
    };
  }, []);

  useEffect(() => {
    let cancelled = false;

    (async () => {
      if (!box.current) return;
      try {
        const [{ default: cytoscape }, { default: fcose }] = await Promise.all([
          import("cytoscape"),
          import("cytoscape-fcose"),
        ]);
        if (cancelled || !box.current) return;
        // register() throws if called twice across remounts — harmless.
        try {
          (cytoscape as unknown as { use: (e: unknown) => void }).use(fcose);
        } catch {
          /* already registered */
        }

        // Merge every relationship between the same two entities into ONE
        // line. A person is routinely both "קשור ל־" a company and "מוגבל מ־"
        // it, and drawing those as separate curves produced the thing that
        // says nothing: five identical grey arcs between two dots. One line,
        // coloured by the most consequential relationship, labelled with all
        // of them.
        const pairs = new Map<string, {
          source: string; target: string; types: string[];
          // Verified only when EVERY contributing row was signed off — one
          // unreviewed edge is enough to drop the tick, so the mark never
          // over-claims.
          allVerified: boolean;
          count: number;
        }>();
        for (const e of graph.edges) {
          const s = `${e.source_type}:${e.source_id}`;
          const t = `${e.target_type}:${e.target_id}`;
          const key = [s, t].sort().join("||");
          const found = pairs.get(key);
          if (found) {
            if (!found.types.includes(e.relationship_type)) {
              found.types.push(e.relationship_type);
            }
            if (!e.verified) found.allVerified = false;
            found.count += 1;
          } else {
            pairs.set(key, {
              source: s, target: t, types: [e.relationship_type],
              allVerified: !!e.verified, count: 1,
            });
          }
        }
        const mergedEdges = Array.from(pairs.entries()).map(([key, p]) => {
          const rel = dominantRelType(p.types);
          const names = p.types.map(edgeLabel).join(" + ");
          return {
            data: {
              id: `e_${key}`,
              source: p.source,
              target: p.target,
              label: (p.allVerified ? "✓ " : "") + names,
              rel,
              restricted: rel === "restricted_from" ? 1 : 0,
              // Named at rest for the same reason the nodes are: nothing is
              // competing for the space.
              always: graph.nodes.length <= 12 ? 1 : 0,
            },
          };
        });

        // Degree drives node size and which labels survive, and it counts
        // MERGED pairs — distinct neighbours, not rows. Counting rows would
        // size a node by how verbosely one relationship was recorded rather
        // than by how connected it is, and would disagree with the lines the
        // reader can actually count. Computed here rather than in a style
        // callback, which runs on every repaint.
        const degree = new Map<string, number>();
        for (const p of pairs.values()) {
          degree.set(p.source, (degree.get(p.source) || 0) + 1);
          degree.set(p.target, (degree.get(p.target) || 0) + 1);
        }
        const maxDegree = Math.max(1, ...degree.values());
        // Hiding labels is a crowding remedy, and a small neighbourhood is not
        // crowded. Below this size everything is named at rest — otherwise a
        // two-node graph, where there is room for both names twice over,
        // renders as two anonymous dots and a line.
        const sparse = graph.nodes.length <= 12;

        const elements = [
          ...graph.nodes.map((n) => {
            const key = `${n.entity_type}:${n.id}`;
            const deg = degree.get(key) || 0;
            return {
              data: {
                id: key,
                rawId: n.id,
                etype: n.entity_type,
                label: n.name || "(ללא שם)",
                deg,
                // The hub is the entity the user asked about; it should read as
                // the subject rather than as one dot among fifty.
                hub: sparse || deg >= Math.max(4, maxDegree * 0.5) ? 1 : 0,
              },
            };
          }),
          ...mergedEdges,
        ];

        const pal = resolvePalette();

        const cy = cytoscape({
          container: box.current,
          elements,
          style: [
            {
              selector: "node",
              style: {
                "background-color": (el: { data: (k: string) => string }) =>
                  pal.node[el.data("etype") as OcoiEntityType] || pal.muted,
                // Size carries degree, so the subject of the query is findable
                // without reading a single label.
                width: (el: { data: (k: string) => number }) =>
                  12 + Math.min(26, Math.sqrt(el.data("deg") || 0) * 5),
                height: (el: { data: (k: string) => number }) =>
                  12 + Math.min(26, Math.sqrt(el.data("deg") || 0) * 5),
                "border-width": 1,
                "border-color": pal.halo,
                label: "data(label)",
                "font-size": 11,
                "font-family": "system-ui, sans-serif",
                color: pal.text,
                "text-valign": "bottom",
                "text-margin-y": 5,
                "text-wrap": "ellipsis",
                "text-max-width": "104px",
                // The halo is what makes a label readable where it crosses an
                // edge or another label instead of dissolving into them.
                "text-outline-width": 2.5,
                "text-outline-color": pal.halo,
                "text-outline-opacity": 1,
                "min-zoomed-font-size": 9,
              },
            },
            {
              // Fifty labels in a radial cluster overlap into an unreadable
              // smear at the default zoom. Only the hubs are labelled until
              // the user zooms in, at which point the rest fade up. This is
              // the difference between a picture and a hairball.
              selector: "node[hub = 0]",
              style: { "text-opacity": 0 },
            },
            {
              selector: "node[hub = 1]",
              style: { "font-size": 13, "font-weight": 700, "z-index": 10 },
            },
            {
              selector: "edge",
              style: {
                // Colour BY RELATIONSHIP TYPE. One undifferentiated grey made
                // "בן משפחה" and "מוגבל מ־" look like the same fact, which is
                // the difference the whole project exists to show.
                "line-color": (el: { data: (k: string) => string }) =>
                  pal.rel[el.data("rel")] || pal.relFallback,
                "target-arrow-color": (el: { data: (k: string) => string }) =>
                  pal.rel[el.data("rel")] || pal.relFallback,
                "target-arrow-shape": (el: { data: (k: string) => number }) =>
                  (el.data("restricted") ? "triangle" : "none"),
                "arrow-scale": 0.9,
                width: (el: { data: (k: string) => number }) => (el.data("restricted") ? 2.6 : 1.5),
                "curve-style": "bezier",
                opacity: 0.85,
                label: "data(label)",
                "font-size": 9,
                "font-family": "system-ui, sans-serif",
                color: pal.text,
                "text-outline-width": 2.5,
                "text-outline-color": pal.halo,
                "text-rotation": "autorotate",
                "text-opacity": 0,
                "min-zoomed-font-size": 8,
              },
            },
            {
              // A restriction is the finding, so its line is named even at
              // rest; the rest of the vocabulary is carried by colour until
              // the reader hovers or zooms.
              selector: "edge[restricted = 1], edge[always = 1]",
              style: { "text-opacity": 1, "z-index": 5 },
            },
            {
              selector: "node:selected",
              style: { "border-width": 3, "border-color": pal.text, "text-opacity": 1 },
            },
          ],
          layout: {
            name: "fcose",
            animate: false,
            randomize: true,
            // Well above the old 9000/70: the previous values packed a
            // fifty-node neighbourhood into a disc barely wider than one
            // label, which is what put every name on top of every other.
            nodeRepulsion: 26000,
            idealEdgeLength: 135,
            nodeSeparation: 140,
            gravity: 0.12,
            // A hub with hundreds of edges makes fcose crawl; cap the work.
            numIter: elements.length > 800 ? 900 : 2500,
          } as unknown as cytoscape.LayoutOptions,
          wheelSensitivity: 0.25,
          maxZoom: 3,
          minZoom: 0.08,
        });

        // Reveal the quiet labels once there is room for them. The threshold is
        // RELATIVE to the zoom the layout settled at, not an absolute number:
        // that fitted zoom depends on how big the neighbourhood is, so a fixed
        // threshold means a small graph shows every label at rest while a large
        // one never shows any however far you zoom.
        let baseZoom = cy.zoom();
        const syncLabels = () => {
          const show = cy.zoom() >= baseZoom * 1.7;
          cy.batch(() => {
            cy.nodes("[hub = 0]").style("text-opacity", show ? 1 : 0);
            cy.edges("[restricted = 0][always = 0]").style("text-opacity", show ? 1 : 0);
          });
        };
        cy.one("layoutstop", () => {
          baseZoom = cy.zoom();
          syncLabels();
        });
        cy.on("zoom", syncLabels);
        syncLabels();

        // Hovering answers "who is this?" without zooming at all — the common
        // question, and the one that hiding labels would otherwise make
        // expensive. The node also grows slightly so the cursor has a target.
        cy.on("mouseover", "node", (evt: { target: { style: (k: string, v: unknown) => void } }) => {
          evt.target.style("text-opacity", 1);
          evt.target.style("z-index", 20);
          if (box.current) box.current.style.cursor = "pointer";
        });
        cy.on("mouseout", "node", (evt: {
          target: { data: (k: string) => number; style: (k: string, v: unknown) => void };
        }) => {
          const keep = evt.target.data("hub") === 1 || cy.zoom() >= baseZoom * 1.7;
          evt.target.style("text-opacity", keep ? 1 : 0);
          evt.target.style("z-index", evt.target.data("hub") === 1 ? 10 : 1);
          if (box.current) box.current.style.cursor = "";
        });

        // Hovering a line names the relationship, which is the question a
        // coloured line raises and the legend only half answers.
        cy.on("mouseover", "edge", (evt: { target: { style: (k: string, v: unknown) => void } }) => {
          evt.target.style("text-opacity", 1);
          evt.target.style("z-index", 30);
        });
        cy.on("mouseout", "edge", (evt: {
          target: { data: (k: string) => number; style: (k: string, v: unknown) => void };
        }) => {
          const keep = evt.target.data("restricted") === 1 || cy.zoom() >= baseZoom * 1.7;
          evt.target.style("text-opacity", keep ? 1 : 0);
          evt.target.style("z-index", evt.target.data("restricted") === 1 ? 5 : 1);
        });

        cy.on("tap", "node", (evt: { target: { data: (k: string) => string } }) => {
          const d = evt.target;
          onSelectRef.current?.(
            d.data("etype") as OcoiEntityType,
            d.data("rawId"),
            d.data("label"),
          );
        });

        cyRef.current = cy as unknown as { destroy: () => void };
      } catch (e) {
        if (!cancelled) setErr(e instanceof Error ? e.message : "טעינת הגרף נכשלה");
      }
    })();

    return () => {
      cancelled = true;
      cyRef.current?.destroy();
      cyRef.current = null;
    };
  }, [graph, themeKey]);

  if (err) {
    return (
      <div className="text-muted" style={{ padding: "1rem", textAlign: "center" }}>
        {err}
      </div>
    );
  }

  // Ordered by the same priority the lines use, so the legend reads in the
  // order of consequence rather than in whatever order the rows arrived.
  const relTypesPresent = EDGE_PRIORITY.filter((t) =>
    graph.edges.some((e) => e.relationship_type === t),
  ).concat(
    Array.from(
      new Set(
        graph.edges
          .map((e) => e.relationship_type)
          .filter((t) => !EDGE_PRIORITY.includes(t)),
      ),
    ),
  );

  return (
    <div>
      <div
        ref={box}
        style={{
          height,
          width: "100%",
          border: "1px solid var(--border)",
          borderRadius: 8,
          background: "var(--surface)",
        }}
      />
      <div
        className="flex text-sm text-muted"
        style={{ gap: "0.9rem", flexWrap: "wrap", marginTop: "0.5rem", alignItems: "center" }}
      >
        {(Object.keys(TYPE_COLORS) as OcoiEntityType[]).map((t) => (
          <span key={t} className="flex" style={{ gap: "0.3rem", alignItems: "center" }}>
            <span
              style={{
                width: 10,
                height: 10,
                borderRadius: "50%",
                background: TYPE_COLORS[t],
                display: "inline-block",
              }}
            />
            {{ person: "אדם", company: "חברה", association: "עמותה", domain: "תחום" }[t]}
          </span>
        ))}
        <span>· {graph.nodes.length.toLocaleString()} צמתים, {graph.edges.length.toLocaleString()} קשרים</span>
        {graph.truncated && (
          <span style={{ color: "var(--warning)", fontWeight: 600 }}>
            · תצוגה חלקית — הגרף נחתך בתקרת השרת
          </span>
        )}
      </div>

      {/* The edge legend. Colouring lines by relationship is only informative
          if the reader is told what the colours mean, and only the types
          actually present are listed — a fixed legend of nine entries for a
          graph containing two is noise. */}
      {relTypesPresent.length > 0 && (
        <div
          className="flex text-sm text-muted"
          style={{ gap: "0.9rem", flexWrap: "wrap", marginTop: "0.35rem", alignItems: "center" }}
        >
          <span>סוגי קשר:</span>
          {relTypesPresent.map((t) => (
            <span key={t} className="flex" style={{ gap: "0.3rem", alignItems: "center" }}>
              <span
                aria-hidden="true"
                style={{
                  width: 16,
                  height: 0,
                  borderTop: `${t === "restricted_from" ? 3 : 2}px solid ${
                    EDGE_COLORS[t] || EDGE_FALLBACK_COLOR
                  }`,
                  display: "inline-block",
                }}
              />
              {edgeLabel(t)}
            </span>
          ))}
        </div>
      )}

      <OcoiConnectionTable edges={graph.edges} nodes={graph.nodes} />
    </div>
  );
}
