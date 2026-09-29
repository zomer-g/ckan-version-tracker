/**
 * The findings-and-fixes log of the deal register.
 *
 * The register is served as published, so every change around it has to be
 * accounted for somewhere a user of the data will find it: what was found in
 * the source, and what changed on this site because of it (the search, the
 * notes, the tools — never the rows). The content lives in data/deals_log.json
 * and is served by /api/deals/log, so the page, the API and the repo say the
 * same thing.
 */
import { useEffect, useState } from "react";
import { deals as dealsApi, DealsLog as DealsLogData } from "../../api/client";

const KIND: Record<"finding" | "fix", { label: string; color: string }> = {
  finding: { label: "ממצא במקור", color: "var(--warning)" },
  fix: { label: "תיקון אצלנו", color: "var(--success, var(--primary))" },
};

function heDate(iso: string): string {
  const [y, m, d] = iso.split("-");
  return d ? `${d}/${m}/${y}` : iso;
}

export default function DealsLog() {
  const [log, setLog] = useState<DealsLogData | null>(null);
  const [error, setError] = useState(false);

  useEffect(() => {
    dealsApi.log().then(setLog).catch(() => setError(true));
  }, []);

  if (error) return <div className="text-sm" style={{ color: "var(--danger)" }}>לא הצלחנו לטעון את היומן.</div>;
  if (!log) return <div className="text-sm text-muted">טוען את היומן…</div>;

  return (
    <div style={{ maxWidth: 820 }}>
      <p className="text-sm" style={{ lineHeight: 1.75, marginTop: 0 }}>{log.intro}</p>

      <ol style={{ listStyle: "none", padding: 0, margin: 0 }}>
        {log.entries.map((e, i) => (
          <li key={i} style={{
            borderInlineStart: `4px solid ${KIND[e.kind].color}`,
            padding: "0.55rem 0.9rem", marginBottom: "0.8rem",
            background: "var(--surface-2)", borderRadius: 6,
          }}>
            <div className="text-sm text-muted" style={{ display: "flex", gap: "0.6rem", flexWrap: "wrap" }}>
              <span style={{ unicodeBidi: "isolate", direction: "ltr" }}>{heDate(e.date)}</span>
              <span style={{ color: KIND[e.kind].color, fontWeight: 700 }}>{KIND[e.kind].label}</span>
              {e.credit && <span>· {e.credit}</span>}
            </div>
            <h3 style={{ margin: "0.2rem 0 0.35rem", fontSize: "1rem" }}>{e.title}</h3>
            {e.body.map((p, j) => (
              <p key={j} className="text-sm" style={{ margin: "0 0 0.35rem", lineHeight: 1.7 }}>{p}</p>
            ))}
          </li>
        ))}
      </ol>

      {log.open.length > 0 && (
        <div style={{
          padding: "0.6rem 0.9rem", borderRadius: 6, border: "1px dashed var(--border)",
          marginTop: "0.4rem",
        }}>
          <div style={{ fontWeight: 700, fontSize: "0.92rem", marginBottom: "0.25rem" }}>עדיין פתוח</div>
          <div className="text-sm text-muted" style={{ marginBottom: "0.3rem" }}>{log.open_note}</div>
          <ul className="text-sm" style={{ margin: 0, paddingInlineStart: "1.2rem", lineHeight: 1.7 }}>
            {log.open.map((o, i) => <li key={i}>{o}</li>)}
          </ul>
        </div>
      )}

      <p className="text-sm text-muted" style={{ marginTop: "1rem" }}>
        אותו יומן זמין גם כ-JSON בכתובת <code>/api/deals/log</code>. מצאתם משהו במאגר?{" "}
        כתבו לנו ל-<a href="mailto:guy@z-g.co.il">guy@z-g.co.il</a>, ונתעד אותו כאן.
      </p>
    </div>
  );
}
