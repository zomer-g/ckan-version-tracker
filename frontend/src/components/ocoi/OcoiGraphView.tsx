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
  return {
    node: {
      person: raw(TYPE_COLORS.person, "#12564F"),
      company: raw(TYPE_COLORS.company, "#833909"),
      association: raw(TYPE_COLORS.association, "#5B21B6"),
      domain: raw(TYPE_COLORS.domain, "#14458F"),
    } as Record<OcoiEntityType, string>,
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

        // Degree drives node size and which labels survive. Counting it here
        // is cheaper than asking cytoscape per node inside a style callback,
        // which runs on every repaint.
        const degree = new Map<string, number>();
        for (const e of graph.edges) {
          const s = `${e.source_type}:${e.source_id}`;
          const t = `${e.target_type}:${e.target_id}`;
          degree.set(s, (degree.get(s) || 0) + 1);
          degree.set(t, (degree.get(t) || 0) + 1);
        }
        const maxDegree = Math.max(1, ...degree.values());

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
                hub: deg >= Math.max(4, maxDegree * 0.5) ? 1 : 0,
              },
            };
          }),
          ...graph.edges.map((e, i) => ({
            data: {
              id: `e${i}`,
              source: `${e.source_type}:${e.source_id}`,
              target: `${e.target_type}:${e.target_id}`,
              label: e.relationship_type || "",
              // Expense edges are visually de-emphasised: they are bulk data,
              // not a declared conflict of interest.
              expense: e.origin_kind === "mk_expense" ? 1 : 0,
            },
          })),
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
                width: (el: { data: (k: string) => number }) => (el.data("expense") ? 0.8 : 1.3),
                "line-color": pal.edge,
                "curve-style": "bezier",
                opacity: (el: { data: (k: string) => number }) => (el.data("expense") ? 0.3 : 0.6),
              },
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
    </div>
  );
}
