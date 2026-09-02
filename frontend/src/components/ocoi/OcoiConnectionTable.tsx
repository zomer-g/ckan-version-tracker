/**
 * The relationship graph as a table — the accessible equivalent of the canvas.
 *
 * Ported from OCOI's components/graph/ConnectionTable.tsx, which the first pass
 * of this migration dropped. A cytoscape graph is pixels on a canvas: it has no
 * roles, no reading order and nothing for a screen reader or a keyboard to
 * reach, so without a text equivalent the entire relationship view is simply
 * unavailable to anyone not using a mouse and eyes. Every column here answers a
 * question the picture answers visually — who, to whom, how, on whose say-so,
 * is it a restriction, was it checked, and where is the document.
 *
 * It also carries something the map cannot: `details`, the free-text wording of
 * the restriction itself.
 */
import { OcoiEntityType, OcoiGraphEdge, OcoiGraphNode } from "../../api/client";
import { edgeLabel, originLabel, TYPE_LABELS } from "./ocoiGraphLabels";

const cell: React.CSSProperties = {
  padding: "0.5rem 0.7rem",
  borderBottom: "1px solid var(--border)",
  verticalAlign: "top",
};

const head: React.CSSProperties = {
  ...cell,
  textAlign: "start",
  fontWeight: 700,
  whiteSpace: "nowrap",
  position: "sticky",
  top: 0,
  background: "var(--surface-2, var(--surface))",
};

function Pill({ children, tone }: { children: React.ReactNode; tone: "on" | "off" }) {
  return (
    <span
      style={{
        display: "inline-block",
        padding: "0.1rem 0.5rem",
        borderRadius: 999,
        fontSize: "0.78rem",
        whiteSpace: "nowrap",
        background: tone === "on" ? "var(--fill-brand)" : "transparent",
        color: tone === "on" ? "var(--on-fill)" : "var(--text-muted)",
        border: tone === "on" ? "none" : "1px solid var(--border)",
      }}
    >
      {children}
    </span>
  );
}

export default function OcoiConnectionTable({
  edges,
  nodes,
  caption,
}: {
  edges: OcoiGraphEdge[];
  nodes: OcoiGraphNode[];
  caption?: string;
}) {
  if (!edges.length) return null;

  const byId = new Map(nodes.map((n) => [n.id, n]));
  const nameOf = (id: string, fallbackType: OcoiEntityType | string) => {
    const n = byId.get(id);
    if (!n) return id.slice(0, 8);
    const meta = [n.position, n.ministry].filter(Boolean).join(", ");
    return meta ? `${n.name} (${meta})` : n.name || String(fallbackType);
  };

  return (
    <details style={{ marginTop: "0.75rem" }}>
      <summary style={{ cursor: "pointer", color: "var(--text-muted)" }}>
        טבלת הקשרים — גרסה נגישה ({edges.length.toLocaleString()} שורות)
      </summary>
      <div
        style={{
          overflowX: "auto",
          maxHeight: 420,
          overflowY: "auto",
          border: "1px solid var(--border)",
          borderRadius: 8,
          marginTop: "0.6rem",
        }}
      >
        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.88rem" }}>
          <caption className="sr-only">
            {caption || "כל הקשרים המוצגים במפת הקשרים, כטבלה"}
          </caption>
          <thead>
            <tr>
              <th scope="col" style={head}>מקור</th>
              <th scope="col" style={head}>סוג מקור</th>
              <th scope="col" style={head}>יעד</th>
              <th scope="col" style={head}>סוג יעד</th>
              <th scope="col" style={head}>סוג קשר</th>
              <th scope="col" style={head}>פירוט</th>
              <th scope="col" style={head}>מקור הנתונים</th>
              <th scope="col" style={head}>מגבלה</th>
              <th scope="col" style={head}>נבדק</th>
              <th scope="col" style={head}>מסמך</th>
            </tr>
          </thead>
          <tbody>
            {edges.map((e, i) => (
              <tr key={`${e.source_id}-${e.target_id}-${e.relationship_type}-${i}`}>
                <td style={cell}>{nameOf(e.source_id, e.source_type)}</td>
                <td style={{ ...cell, color: "var(--text-muted)" }}>
                  {TYPE_LABELS[e.source_type] || e.source_type}
                </td>
                <td style={cell}>{nameOf(e.target_id, e.target_type)}</td>
                <td style={{ ...cell, color: "var(--text-muted)" }}>
                  {TYPE_LABELS[e.target_type] || e.target_type}
                </td>
                <td style={cell}>{edgeLabel(e.relationship_type)}</td>
                <td style={{ ...cell, maxWidth: 320, color: "var(--text-muted)" }}>
                  {e.details || "—"}
                </td>
                <td style={{ ...cell, color: "var(--text-muted)" }}>
                  {originLabel(e.origin_kind)}
                </td>
                <td style={cell}>
                  {e.relationship_type === "restricted_from" ? (
                    <Pill tone="on">כן</Pill>
                  ) : (
                    <Pill tone="off">לא</Pill>
                  )}
                </td>
                <td style={cell}>
                  {/* "machine processing" rather than "not checked": nothing here
                      is wrong until a human says so, but nothing is confirmed
                      either, and the distinction is the project's whole claim. */}
                  {e.verified ? <Pill tone="on">✓ נבדק</Pill> : <Pill tone="off">עיבוד מכונה</Pill>}
                </td>
                <td style={cell}>
                  {e.document_url && !e.document_url.startsWith("upload://") ? (
                    <a
                      href={e.document_url}
                      target="_blank"
                      rel="noopener noreferrer"
                      title={e.document_title || "מסמך מקור"}
                    >
                      מסמך
                    </a>
                  ) : (
                    <span style={{ color: "var(--text-muted)" }}>—</span>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </details>
  );
}
