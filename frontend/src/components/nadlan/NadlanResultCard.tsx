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
 *
 * Two blocks here DESCRIBE the property rather than identify it, and both are
 * shown with the identity rather than behind it, because they are what someone
 * looking up a point actually came for: its CBS statistical area (א"ס), and its
 * מיסוי מקרקעין deal history. The card carries the SUMMARY the envelope
 * already holds; the full list is fetched on demand, because one parcel in a
 * condo tower holds up to 1,850 deals.
 */
import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import {
  nadlan, type NadlanDeal, type NadlanProperty, type NadlanSourceBlock,
} from "../../api/client";

const SOURCE_LABELS: Record<string, string> = {
  parcels: "חלקות (מרכז למיפוי ישראל)",
  gazetteer: "גזטיר הנכסים",
  postal: "קובץ המיקוד (דואר ישראל)",
  address_list: "רשימת כתובות בישראל",
  deals: "עסקאות נדל\"ן (רשות המסים)",
  stat_area: "אזורים סטטיסטיים (למ\"ס)",
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
  deals: "עסקאות",
  first_deal: "עסקה ראשונה",
  last_deal: "עסקה אחרונה",
  sub_parcels: "תת-חלקות עם עסקאות",
  code: "אזור סטטיסטי",
  yishuv_stat: "קוד אזור מלא",
  rova: "רובע",
  tat_rova: "תת-רובע",
  population: "אוכלוסייה (2024)",
  main_function: "ייעוד עיקרי",
};

const NIS = new Intl.NumberFormat("he-IL", {
  style: "currency", currency: "ILS", maximumFractionDigits: 0,
});

/** RTL reorders a run of digits next to a sign or a separator, so every number
 *  that is read as one unit goes through an LTR isolate — the same fix the
 *  gaps report needed. */
function Ltr({ children }: { children: React.ReactNode }) {
  return <span style={{ unicodeBidi: "isolate", direction: "ltr", display: "inline-block" }}>{children}</span>;
}

function heDate(iso: string | null | undefined): string {
  if (!iso) return "—";
  const [y, m, d] = iso.split("-");
  return d ? `${d}/${m}/${y}` : iso;
}

// Codes and years are identifiers, not quantities: "2,014" and "50,000,613"
// are both wrong, and the second is unreadable as the thing it identifies.
const PLAIN_NUMBER_FIELDS = new Set([
  "code", "yishuv_stat", "rova", "tat_rova", "street_code", "zip7", "zip5",
  "building_year_min", "building_year_max",
]);

function fmt(v: unknown, key?: string): string {
  if (v == null || v === "") return "—";
  if (Array.isArray(v)) return v.length ? v.map((x) => fmt(x, key)).join(", ") : "—";
  if (typeof v === "string" && /^\d{4}-\d{2}-\d{2}$/.test(v)) return heDate(v);
  if (typeof v === "number") {
    return key && PLAIN_NUMBER_FIELDS.has(key) ? String(v) : v.toLocaleString("he-IL");
  }
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
              <dd style={{ margin: 0 }}><Ltr>{fmt(v, k)}</Ltr></dd>
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
      {/* A physical table name, and several are a schema plus a 32-hex resource
          id joined by underscores — `odata.ac1ae1fa_6d43_4685_8434_9953e950ca9b_19c5be7f`
          is 58 characters. An underscore is not a break opportunity in CSS, so
          the name is one unbreakable word: it ran straight out of the card and
          past the edge of the grid on the postal-code and address-list blocks,
          whose ids are the longest. Breaking anywhere keeps it inside whatever
          width the card happens to have. */}
      <div className="text-sm text-muted"
           style={{ fontSize: "0.75rem", marginTop: "0.2rem", overflowWrap: "anywhere" }}>
        {block.table}
      </div>
    </div>
  );
}

/** The CBS statistical area. The socio-economic cluster sits apart and carries
 *  its own division year on purpose: it is published on the 2011 areas, which
 *  are NOT the 2022 areas this property's code comes from, and collapsing the
 *  two would read as one fact when it is two. */
function StatAreaBlock({ area }: { area: NonNullable<NadlanProperty["stat_area"]> }) {
  return (
    <div style={{
      border: "1px solid var(--border)", borderRadius: 8, padding: "0.6rem 0.8rem",
      background: "var(--surface-2)",
    }}>
      <div style={{ fontWeight: 700, fontSize: "0.9rem", marginBottom: "0.3rem" }}>
        אזור סטטיסטי (א״ס)
      </div>
      <div className="text-sm" style={{ lineHeight: 1.8 }}>
        <div>
          א״ס <Ltr>{area.code ?? "—"}</Ltr>
          {area.settlement_name ? ` · ${area.settlement_name}` : ""}
          {area.yishuv_stat != null && (
            <span className="text-muted"> (קוד מלא <Ltr>{area.yishuv_stat}</Ltr>)</span>
          )}
        </div>
        {(area.rova != null || area.tat_rova != null) && (
          <div className="text-muted">
            רובע <Ltr>{area.rova ?? "—"}</Ltr> · תת-רובע <Ltr>{area.tat_rova ?? "—"}</Ltr>
          </div>
        )}
        <div className="text-muted">
          {area.population != null && (
            <>אוכלוסייה <Ltr>{area.population.toLocaleString("he-IL")}</Ltr> ({area.population_year}) · </>
          )}
          {area.main_function ?? "ייעוד לא מדווח"}
        </div>
        {area.socio?.eshkol != null && (
          <div className="text-muted">
            מדד חברתי-כלכלי: אשכול <Ltr>{area.socio.eshkol}</Ltr>{" "}
            <span style={{ fontSize: "0.78rem" }}>
              (מדד {area.socio.index_year}, על חלוקת האזורים של {area.socio.division})
            </span>
          </div>
        )}
        <div className="text-muted" style={{ fontSize: "0.78rem" }}>
          נקבע לפי מרכז החלקה בתוך שכבת האזורים הסטטיסטיים של {area.division}.
        </div>
      </div>
    </div>
  );
}

/** The deal history. The summary is already in the envelope; the list is
 *  fetched only when it is opened, because a parcel in a condo tower holds
 *  hundreds of deals and nobody asked for them by clicking a point. */
function DealsBlock({ property }: { property: NadlanProperty }) {
  const summary = property.deals!;
  const id = property.identity;
  const [open, setOpen] = useState(false);
  const [rows, setRows] = useState<NadlanDeal[] | null>(null);
  const [total, setTotal] = useState(summary.deals);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!open || rows) return;
    let cancelled = false;
    nadlan.deals(id.gush, id.helka, 50)
      .then((r) => { if (!cancelled) { setRows(r.data); setTotal(r.total); } })
      .catch((e) => { if (!cancelled) setError(e instanceof Error ? e.message : "טעינת העסקאות נכשלה"); });
    return () => { cancelled = true; };
  }, [open, rows, id.gush, id.helka]);

  const dealsHref = `/projects/deals?gush=${id.gush}&helka=${id.helka}`;

  return (
    <div style={{
      border: "1px solid var(--border)", borderRadius: 8, padding: "0.6rem 0.8rem",
      background: "var(--surface-2)",
    }}>
      <div className="flex" style={{ justifyContent: "space-between", alignItems: "baseline", gap: "0.5rem", flexWrap: "wrap" }}>
        <div style={{ fontWeight: 700, fontSize: "0.9rem" }}>עסקאות מקרקעין</div>
        <Link to={dealsHref} style={{ fontSize: "0.82rem" }}>
          במאגר עסקאות נדל״ן ←
        </Link>
      </div>
      <div className="text-sm" style={{ lineHeight: 1.8 }}>
        <div>
          <Ltr>{summary.deals.toLocaleString("he-IL")}</Ltr> עסקאות מדווחות ·{" "}
          <Ltr>{heDate(summary.first_deal)}</Ltr> עד <Ltr>{heDate(summary.last_deal)}</Ltr>
          {summary.sub_parcels > 1 && (
            <span className="text-muted"> · <Ltr>{summary.sub_parcels}</Ltr> תת-חלקות</span>
          )}
        </div>
        {summary.latest?.date && (
          <div className="text-muted">
            אחרונה: <Ltr>{heDate(summary.latest.date)}</Ltr>
            {summary.latest.amount != null && <> · <Ltr>{NIS.format(summary.latest.amount)}</Ltr></>}
            {summary.latest.nature ? ` · ${summary.latest.nature}` : ""}
            {summary.latest.rooms ? <> · <Ltr>{summary.latest.rooms}</Ltr> חד׳</> : null}
          </div>
        )}
      </div>

      <button
        type="button"
        onClick={() => setOpen(!open)}
        aria-expanded={open}
        style={{
          marginTop: "0.45rem", padding: "0.25rem 0.7rem", fontSize: "0.82rem",
          cursor: "pointer", border: "1px solid var(--border)", borderRadius: 6,
          background: "none",
        }}
      >
        {open ? "סגירת רשימת העסקאות" : "כל העסקאות בחלקה"}
      </button>

      {open && (
        <div style={{ marginTop: "0.5rem" }}>
          {error && <div className="text-sm" style={{ color: "var(--danger)" }}>{error}</div>}
          {!error && !rows && <div className="text-sm text-muted">טוען עסקאות…</div>}
          {rows && rows.length > 0 && (
            <div tabIndex={0} role="region" aria-label="עסקאות בחלקה" className="scroll-region" style={{ overflowX: "auto" }}>
              <table style={{ width: "100%", fontSize: "0.83rem", borderCollapse: "collapse" }}>
                <thead>
                  <tr style={{ textAlign: "start", color: "var(--text-muted)" }}>
                    {["תאריך", "שווי", "מהות", "תת-חלקה", "חדרים", "שטח (מ״ר)", "שנת בנייה"].map((h) => (
                      <th key={h} scope="col" style={{ textAlign: "start", padding: "0.2rem 0.4rem", whiteSpace: "nowrap" }}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {rows.map((d, i) => (
                    <tr key={i} style={{ borderTop: "1px solid var(--border)" }}>
                      <td style={{ padding: "0.2rem 0.4rem", whiteSpace: "nowrap" }}><Ltr>{heDate(d.date)}</Ltr></td>
                      <td style={{ padding: "0.2rem 0.4rem", whiteSpace: "nowrap" }}>
                        <Ltr>{d.amount != null ? NIS.format(d.amount) : "—"}</Ltr>
                      </td>
                      <td style={{ padding: "0.2rem 0.4rem" }}>{d.nature ?? "—"}</td>
                      <td style={{ padding: "0.2rem 0.4rem" }}><Ltr>{d.sub_parcel ?? "—"}</Ltr></td>
                      <td style={{ padding: "0.2rem 0.4rem" }}><Ltr>{d.rooms || "—"}</Ltr></td>
                      <td style={{ padding: "0.2rem 0.4rem" }}><Ltr>{d.area_sqm || "—"}</Ltr></td>
                      <td style={{ padding: "0.2rem 0.4rem" }}><Ltr>{d.year_built || "—"}</Ltr></td>
                    </tr>
                  ))}
                </tbody>
              </table>
              {total > rows.length && (
                <div className="text-sm text-muted" style={{ marginTop: "0.3rem" }}>
                  מוצגות <Ltr>{rows.length}</Ltr> מתוך <Ltr>{total.toLocaleString("he-IL")}</Ltr> עסקאות.{" "}
                  <Link to={dealsHref}>להמשך במאגר עסקאות נדל״ן</Link>
                </div>
              )}
            </div>
          )}
          {rows && rows.length === 0 && (
            <div className="text-sm text-muted">לא נמצאו עסקאות לחלקה זו.</div>
          )}
        </div>
      )}
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

      {(property.stat_area || property.deals) && (
        <div style={{
          display: "grid", gap: "0.5rem", marginTop: "0.6rem",
          gridTemplateColumns: "repeat(auto-fit, minmax(260px, 1fr))",
        }}>
          {property.stat_area && <StatAreaBlock area={property.stat_area} />}
          {property.deals && <DealsBlock property={property} />}
        </div>
      )}

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
