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

        const elements = [
          ...graph.nodes.map((n) => ({
            data: {
              id: `${n.entity_type}:${n.id}`,
              rawId: n.id,
              etype: n.entity_type,
              label: n.name || "(ללא שם)",
            },
          })),
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

        const cy = cytoscape({
          container: box.current,
          elements,
          style: [
            {
              selector: "node",
              style: {
                "background-color": (el: { data: (k: string) => string }) =>
                  TYPE_COLORS[el.data("etype") as OcoiEntityType] || "#464F5E",
                label: "data(label)",
                "font-size": 10,
                "font-family": "system-ui, sans-serif",
                color: "var(--text)",
                "text-valign": "bottom",
                "text-margin-y": 4,
                "text-wrap": "ellipsis",
                "text-max-width": "110px",
                width: 18,
                height: 18,
                "border-width": 0,
              },
            },
            {
              selector: "edge",
              style: {
                width: 1.2,
                "line-color": (el: { data: (k: string) => number }) =>
                  el.data("expense") ? "#cbd5e1" : "#94a3b8",
                "curve-style": "bezier",
                opacity: (el: { data: (k: string) => number }) => (el.data("expense") ? 0.45 : 0.85),
              },
            },
            {
              selector: "node:selected",
              style: { "border-width": 3, "border-color": "#0f172a" },
            },
          ],
          layout: {
            name: "fcose",
            animate: false,
            randomize: true,
            nodeRepulsion: 9000,
            idealEdgeLength: 70,
            // A hub with hundreds of edges makes fcose crawl; cap the work.
            numIter: elements.length > 800 ? 900 : 2500,
          } as unknown as cytoscape.LayoutOptions,
          wheelSensitivity: 0.25,
          maxZoom: 3,
          minZoom: 0.08,
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
  }, [graph]);

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
