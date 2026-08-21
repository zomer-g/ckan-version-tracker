/**
 * Entity browser: the most-connected entities, and the per-ministry breakdown.
 *
 * Both views default to EXCLUDING Knesset expense edges. That is not cosmetic:
 * expense rows outnumber declaration rows several times over, so including them
 * ranks by "who filed the most expense claims" rather than "who has the most
 * declared conflicts" — a different question than the page is asking.
 */
import { useCallback, useEffect, useState } from "react";
import {
  ocoi,
  OcoiEntityType,
  OcoiMinistry,
  OcoiTopConnected,
  OCOI_DEFAULT_EXCLUDE,
  OCOI_TYPE_LABELS,
} from "../../api/client";
import { Empty, ErrorNote, Spinner, TypeChip } from "./ocoiShared";

type View = "connected" | "ministries";

export default function OcoiEntities({
  onOpenEntity,
}: {
  onOpenEntity: (type: OcoiEntityType, id: string, name: string) => void;
}) {
  const [view, setView] = useState<View>("connected");
  const [type, setType] = useState<OcoiEntityType | "">("");
  const [includeExpenses, setIncludeExpenses] = useState(false);
  const [top, setTop] = useState<OcoiTopConnected[]>([]);
  const [ministries, setMinistries] = useState<OcoiMinistry[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  const exclude = includeExpenses ? undefined : OCOI_DEFAULT_EXCLUDE;

  const load = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      if (view === "connected") {
        const res = await ocoi.topConnected({
          limit: 40,
          ...(type ? { type } : {}),
          ...(exclude ? { exclude_origins: exclude } : {}),
        });
        setTop(res.data || []);
      } else {
        const res = await ocoi.ministries(exclude);
        setMinistries(res.data || []);
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : "הטעינה נכשלה");
    } finally {
      setLoading(false);
    }
  }, [view, type, exclude]);

  useEffect(() => {
    load();
  }, [load]);

  return (
    <div>
      <div className="flex" style={{ gap: "0.5rem", flexWrap: "wrap", alignItems: "center", marginBottom: "0.9rem" }}>
        <div className="flex" style={{ gap: "0.3rem" }}>
          {(["connected", "ministries"] as View[]).map((v) => (
            <button
              key={v}
              type="button"
              className={`btn btn-sm${view === v ? " btn-primary" : ""}`}
              onClick={() => setView(v)}
            >
              {v === "connected" ? "המקושרים ביותר" : "לפי משרד"}
            </button>
          ))}
        </div>

        {view === "connected" && (
          <select
            value={type}
            onChange={(e) => setType(e.target.value as OcoiEntityType | "")}
            style={{ padding: "0.4rem 0.6rem" }}
            aria-label="סוג ישות"
          >
            <option value="">כל הסוגים</option>
            {(Object.keys(OCOI_TYPE_LABELS) as OcoiEntityType[]).map((t) => (
              <option key={t} value={t}>
                {OCOI_TYPE_LABELS[t]}
              </option>
            ))}
          </select>
        )}

        <label className="text-sm flex" style={{ gap: "0.35rem", alignItems: "center" }}>
          <input
            type="checkbox"
            checked={includeExpenses}
            onChange={(e) => setIncludeExpenses(e.target.checked)}
          />
          כלול הוצאות ח״כים
        </label>
      </div>

      {error && <ErrorNote error={error} />}
      {loading && <Spinner />}

      {!loading && view === "connected" && (
        top.length === 0 ? (
          <Empty>אין נתונים להצגה.</Empty>
        ) : (
          <ol style={{ listStyle: "none", padding: 0, margin: 0, display: "grid", gap: "0.35rem" }}>
            {top.map((e, i) => (
              <li key={`${e.entity_type}:${e.id}`}>
                <button
                  type="button"
                  onClick={() => onOpenEntity(e.entity_type, e.id, e.name)}
                  style={{
                    width: "100%",
                    display: "flex",
                    gap: "0.6rem",
                    alignItems: "center",
                    textAlign: "start",
                    padding: "0.5rem 0.7rem",
                    border: "1px solid var(--border)",
                    borderRadius: 8,
                    background: "var(--surface)",
                    cursor: "pointer",
                  }}
                >
                  <span className="text-muted" style={{ minWidth: "1.6rem", fontVariantNumeric: "tabular-nums" }}>
                    {i + 1}.
                  </span>
                  <TypeChip type={e.entity_type} small />
                  <span style={{ flex: 1, fontWeight: 500 }}>{e.name || "(ללא שם)"}</span>
                  <span className="text-sm text-muted" style={{ whiteSpace: "nowrap" }}>
                    {e.connections.toLocaleString()} קשרים
                  </span>
                </button>
              </li>
            ))}
          </ol>
        )
      )}

      {!loading && view === "ministries" && (
        ministries.length === 0 ? (
          <Empty>אין נתונים להצגה.</Empty>
        ) : (
          <div tabIndex={0} role="region" aria-label="ישויות" className="scroll-region" style={{ overflowX: "auto" }}>
            <table className="table" style={{ width: "100%", fontSize: "0.9rem" }}>
              <thead>
                <tr>
                  <th scope="col" style={{ textAlign: "start" }}>משרד / גוף</th>
                  <th scope="col" style={{ textAlign: "center" }}>בעלי תפקיד</th>
                  <th scope="col" style={{ textAlign: "center" }}>קשרים</th>
                </tr>
              </thead>
              <tbody>
                {ministries.map((m) => (
                  <tr key={m.ministry}>
                    <td>{m.ministry}</td>
                    <td style={{ textAlign: "center" }}>{m.person_count.toLocaleString()}</td>
                    <td style={{ textAlign: "center" }}>{m.connection_count.toLocaleString()}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )
      )}
    </div>
  );
}
