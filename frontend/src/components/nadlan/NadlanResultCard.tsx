/**
 * One resolved property, rendered as the cross-source answer.
 *
 * The card's job is to make the crosswalk legible AND honest: the identity block
 * shows the property in every codespace at once (גוש/חלקה, יישוב, רחוב, מיקוד,
 * נקודה), and each source block links out to that source's untouched full row on
 * /data rather than reproducing its columns here — the crosswalk tables stay
 * thin by design, so "all the other fields" live one click away at the source.
 *
 * A `match.confidence` of "approximate" is shown, not hidden: the gazetteer
 * publishes no תת-גוש, so for the 0.63% of gush/parcel pairs that cover several
 * real parcels its data may belong to a different one.
 */
import type { NadlanProperty, NadlanSourceBlock } from "../../api/client";

const SOURCE_LABELS: Record<string, string> = {
  parcels: "חלקות (מרכז למיפוי ישראל)",
  gazetteer: "גזטיר הנכסים",
  postal: "קובץ המיקוד (דואר ישראל)",
  address_list: "רשימת כתובות בישראל",
};

const FIELD_LABELS: Record<string, string> = {
  legal_area: "שטח רשום (מ״ר)",
  status: "סטטוס",
  locality: "יישוב",
  n_assets: "נכסים בגזטיר",
  n_dwellings: "דירות מגורים",
  n_subparcels: "תת-חלקות",
  floors_max: "קומות (מקס׳)",
  building_year_min: "שנת בנייה (מוקדמת)",
  building_year_max: "שנת בנייה (מאוחרת)",
  apartments_est: "אומדן דירות",
  street_name_src: "רחוב (כפי שבגזטיר)",
  street_code: "קוד רחוב",
  zip7: "מיקוד 7",
  zip5: "מיקוד 5",
  n_addresses: "כתובות מקושרות",
};

function fmt(v: unknown): string {
  if (v == null || v === "") return "—";
  if (Array.isArray(v)) return v.length ? v.join(", ") : "—";
  if (typeof v === "number") return v.toLocaleString("he-IL");
  return String(v);
}

function SourceCard({ id, block }: { id: string; block: NadlanSourceBlock }) {
  const fields = Object.entries(block.fields).filter(([, v]) => v != null && v !== "" &&
    !(Array.isArray(v) && v.length === 0));
  return (
    <div style={{
      border: "1px solid var(--border)", borderRadius: 8, padding: "0.7rem 0.85rem",
      background: "var(--surface)",
    }}>
      <div style={{ fontWeight: 700, fontSize: "0.9rem", marginBottom: "0.4rem" }}>
        {SOURCE_LABELS[id] ?? id}
      </div>
      {fields.length ? (
        <dl style={{ margin: 0, display: "grid", gridTemplateColumns: "auto 1fr", gap: "0.15rem 0.6rem", fontSize: "0.85rem" }}>
          {fields.map(([k, v]) => (
            <div key={k} style={{ display: "contents" }}>
              <dt style={{ color: "var(--text-muted)" }}>{FIELD_LABELS[k] ?? k}</dt>
              <dd style={{ margin: 0 }}>{fmt(v)}</dd>
            </div>
          ))}
        </dl>
      ) : (
        <div className="text-sm text-muted">אין נתונים במקור הזה לחלקה זו.</div>
      )}
      <a
        href={block.row_url}
        target="_blank"
        rel="noopener noreferrer"
        style={{ display: "inline-block", marginTop: "0.5rem", fontSize: "0.82rem" }}
        title={block.console_sql}
      >
        צפייה בשורות המקור ב-/data ↗
      <span className="sr-only"> (נפתח בחלון חדש)</span></a>
      <div className="text-sm text-muted" style={{ fontSize: "0.75rem", marginTop: "0.2rem" }}>
        {block.table}
      </div>
    </div>
  );
}

export default function NadlanResultCard({
  property, expanded, onToggle,
}: {
  property: NadlanProperty;
  expanded: boolean;
  onToggle: () => void;
}) {
  const id = property.identity;
  const approx = property.match.confidence !== "exact";

  return (
    <div style={{
      border: "1px solid var(--border)", borderRadius: 10, padding: "0.9rem 1rem",
      marginBottom: "0.75rem", background: "var(--surface)",
    }}>
      <div className="flex" style={{ justifyContent: "space-between", alignItems: "baseline", gap: "0.6rem", flexWrap: "wrap" }}>
        <h3 style={{ margin: 0, fontSize: "1.05rem" }}>
          גוש {id.gush}
          {id.gush_suffix ? `/${id.gush_suffix}` : ""} · חלקה {id.helka}
          {id.settlement.name ? ` · ${id.settlement.name}` : ""}
        </h3>
        <div className="flex" style={{ gap: "0.4rem", alignItems: "center" }}>
          {id.distance_m != null && (
            <span className="text-sm text-muted">{Math.round(id.distance_m).toLocaleString("he-IL")} מ׳</span>
          )}
          <span style={{
            fontSize: "0.75rem", padding: "0.12rem 0.5rem", borderRadius: 999,
            background: approx ? "#fef3c7" : "var(--tint-good-bg)",
            color: approx ? "#833909" : "var(--success)",
          }}>
            {approx ? "התאמה משוערת" : "התאמה מדויקת"}
          </span>
        </div>
      </div>

      <div className="text-sm" style={{ marginTop: "0.45rem", lineHeight: 1.8 }}>
        {id.streets.length > 0 && <div>רחוב: {id.streets.join(" · ")}</div>}
        {id.zip7.length > 0 && <div>מיקוד: {id.zip7.slice(0, 6).join(", ")}{id.zip7.length > 6 ? ` (+${id.zip7.length - 6})` : ""}</div>}
        {id.point && (
          <div className="text-muted" style={{ fontSize: "0.82rem" }}>
            נקודה: {id.point.lat.toFixed(6)}, {id.point.lon.toFixed(6)}
          </div>
        )}
        <div className="text-muted" style={{ fontSize: "0.82rem" }}>
          {id.addresses.length.toLocaleString("he-IL")} כתובות מקושרות
        </div>
      </div>

      {property.match.notes.length > 0 && (
        <ul style={{ margin: "0.5rem 0 0", paddingInlineStart: "1.1rem", fontSize: "0.82rem", color: "var(--warning)" }}>
          {property.match.notes.map((n, i) => <li key={i}>{n}</li>)}
        </ul>
      )}

      <button
        type="button"
        onClick={onToggle}
        style={{
          marginTop: "0.6rem", padding: "0.3rem 0.8rem", fontSize: "0.85rem", cursor: "pointer",
          border: "1px solid var(--border)", borderRadius: 6, background: "none",
        }}
      >
        {expanded ? "הסתרת ההצלבה המלאה" : "ההצלבה המלאה בכל המקורות"}
      </button>

      {expanded && (
        <>
          <div style={{
            display: "grid", gap: "0.6rem", marginTop: "0.7rem",
            gridTemplateColumns: "repeat(auto-fit, minmax(230px, 1fr))",
          }}>
            {Object.entries(property.sources).map(([k, b]) => (
              <SourceCard key={k} id={k} block={b as NadlanSourceBlock} />
            ))}
          </div>

          {id.addresses.length > 0 && (
            <div tabIndex={0} role="region" aria-label="פרטי הנכס" className="scroll-region" style={{ marginTop: "0.8rem", overflowX: "auto" }}>
              <table style={{ width: "100%", fontSize: "0.83rem", borderCollapse: "collapse" }}>
                <thead>
                  <tr style={{ textAlign: "start", color: "var(--text-muted)" }}>
                    <th scope="col" style={{ textAlign: "start", padding: "0.2rem 0.4rem" }}>רחוב</th>
                    <th scope="col" style={{ textAlign: "start", padding: "0.2rem 0.4rem" }}>מס׳</th>
                    <th scope="col" style={{ textAlign: "start", padding: "0.2rem 0.4rem" }}>מיקוד</th>
                    <th scope="col" style={{ textAlign: "start", padding: "0.2rem 0.4rem" }}>שכונה</th>
                    <th scope="col" style={{ textAlign: "start", padding: "0.2rem 0.4rem" }}>שיוך</th>
                  </tr>
                </thead>
                <tbody>
                  {id.addresses.slice(0, 40).map((a, i) => (
                    <tr key={i} style={{ borderTop: "1px solid var(--border)" }}>
                      <td style={{ padding: "0.2rem 0.4rem" }}>{a.street ?? "—"}</td>
                      <td style={{ padding: "0.2rem 0.4rem" }}>{a.house ?? "—"}{a.suffix ?? ""}</td>
                      <td style={{ padding: "0.2rem 0.4rem" }}>{a.zip7 ?? "—"}</td>
                      <td style={{ padding: "0.2rem 0.4rem" }}>{a.neighbourhood ?? "—"}</td>
                      <td style={{ padding: "0.2rem 0.4rem" }}>
                        {a.match === "pip" ? "נקודה בתוך החלקה" : a.match ?? "—"}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
              {id.addresses.length > 40 && (
                <div className="text-sm text-muted" style={{ marginTop: "0.3rem" }}>
                  מוצגות 40 מתוך {id.addresses.length.toLocaleString("he-IL")} כתובות.
                </div>
              )}
            </div>
          )}
        </>
      )}
    </div>
  );
}
