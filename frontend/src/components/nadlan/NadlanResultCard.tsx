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
  nadlan, type NadlanAddress, type NadlanDeal, type NadlanProperty, type NadlanSourceBlock,
  type NadlanSourceRows,
} from "../../api/client";
import NadlanCubes from "./NadlanCubes";

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
  building_years: "שנת בנייה",
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
  "building_year_min", "building_year_max", "building_years",
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

/** The sources whose own rows can be opened inside their card. */
const DETAIL_SOURCES = new Set(["gazetteer", "parcels", "postal", "stat_area"]);

/** Sources whose fields the card's summary already shows — the parcel's area
 *  and status, the zips, the statistical area and the deals. Their cards used
 *  to repeat each of those a second time, field for field. */
const COMPACT_SOURCES = new Set(["parcels", "postal", "stat_area", "deals"]);

const DETAIL_PAGE = 50;

function detailButtonLabel(id: string, block: NadlanSourceBlock, open: boolean): string {
  if (open) return "הסתרת הפרטים";
  const n = Number(block.fields.n_assets);
  if (id === "gazetteer" && n > 0) return `כל ${n.toLocaleString("he-IL")} הנכסים בגזטיר`;
  if (id === "postal") return "שורות המיקוד המלאות";
  return "כל השדות במקור";
}

/** A source's own rows for this parcel, fetched when its card is opened.
 *
 *  The card shows the summary the envelope carries ("12 נכסים"); this is the
 *  twelve. One row reads as a field list, several as a table, paged. */
function SourceRowsView({ property, source }: { property: NadlanProperty; source: string }) {
  const id = property.identity;
  const [data, setData] = useState<NadlanSourceRows | null>(null);
  const [loadingMore, setLoadingMore] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    nadlan.sourceRows(id.gush, id.helka, source, DETAIL_PAGE, 0, id.gush_suffix)
      .then((r) => { if (!cancelled) setData(r); })
      .catch((e) => { if (!cancelled) setError(e instanceof Error ? e.message : "הטעינה נכשלה"); });
    return () => { cancelled = true; };
  }, [id.gush, id.helka, id.gush_suffix, source]);

  const more = () => {
    if (!data) return;
    setLoadingMore(true);
    nadlan.sourceRows(id.gush, id.helka, source, DETAIL_PAGE, data.rows.length, id.gush_suffix)
      .then((r) => setData({ ...data, rows: [...data.rows, ...r.rows] }))
      .catch((e) => setError(e instanceof Error ? e.message : "הטעינה נכשלה"))
      .finally(() => setLoadingMore(false));
  };

  if (error) return <div className="text-sm" style={{ color: "var(--danger)", marginTop: "0.4rem" }}>{error}</div>;
  if (!data) return <div className="text-sm text-muted" style={{ marginTop: "0.4rem" }}>טוען…</div>;
  if (!data.rows.length) {
    return <div className="text-sm text-muted" style={{ marginTop: "0.4rem" }}>לא נמצאו שורות במקור לחלקה זו.</div>;
  }

  if (data.total === 1) {
    const row = data.rows[0];
    return (
      <dl style={{
        margin: "0.5rem 0 0", display: "grid", gridTemplateColumns: "auto 1fr",
        gap: "0.15rem 0.6rem", fontSize: "0.83rem", borderTop: "1px solid var(--border)",
        paddingTop: "0.4rem",
      }}>
        {data.columns.map((c, i) => (
          <div key={c.key} style={{ display: "contents" }}>
            <dt style={{ color: "var(--text-muted)" }} title={c.key}>{c.label}</dt>
            <dd style={{ margin: 0, overflowWrap: "anywhere" }}>{row[i] ?? "—"}</dd>
          </div>
        ))}
      </dl>
    );
  }

  return (
    <div style={{ marginTop: "0.5rem" }}>
      <div tabIndex={0} role="region" aria-label={`שורות ${SOURCE_LABELS[source] ?? source}`}
           className="scroll-region" style={{ overflowX: "auto" }}>
        <table style={{ width: "100%", fontSize: "0.82rem", borderCollapse: "collapse" }}>
          <thead>
            <tr style={{ color: "var(--text-muted)" }}>
              {data.columns.map((c) => (
                <th key={c.key} scope="col" title={c.key}
                    style={{ textAlign: "start", padding: "0.2rem 0.4rem", whiteSpace: "nowrap" }}>
                  {c.label}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {data.rows.map((r, i) => (
              <tr key={i} style={{ borderTop: "1px solid var(--border)" }}>
                {r.map((v, j) => (
                  <td key={j} style={{ padding: "0.2rem 0.4rem" }}>{v ?? "—"}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className="text-sm text-muted" style={{ marginTop: "0.3rem" }}>
        מוצגות <Ltr>{data.rows.length.toLocaleString("he-IL")}</Ltr> מתוך{" "}
        <Ltr>{data.total.toLocaleString("he-IL")}</Ltr> שורות.
        {data.rows.length < data.total && (
          <button type="button" onClick={more} disabled={loadingMore}
            style={{
              marginInlineStart: "0.5rem", padding: "0.1rem 0.6rem", fontSize: "0.78rem",
              cursor: "pointer", border: "1px solid var(--border)", borderRadius: 6, background: "none",
            }}>
            {loadingMore ? "טוען…" : "עוד"}
          </button>
        )}
      </div>
    </div>
  );
}

/** The gazetteer's fields, minus what the card already says or what reads as
 *  noise: an internal street code, the street when it is the one shown above,
 *  and a build year split into "earliest" and "latest" when they are equal. */
function visibleFields(id: string, block: NadlanSourceBlock, property: NadlanProperty) {
  const f = { ...block.fields } as Record<string, unknown>;
  if (id === "gazetteer") {
    delete f.street_code;
    const streets = property.identity.addresses.map((a) => a.street);
    if (f.street_name_src && streets.includes(String(f.street_name_src))) delete f.street_name_src;
    if (f.n_subparcels != null && f.n_subparcels === f.n_assets) delete f.n_subparcels;
    const lo = f.building_year_min, hi = f.building_year_max;
    delete f.building_year_min; delete f.building_year_max;
    if (lo != null || hi != null) {
      f.building_years = lo === hi || hi == null ? String(lo) : lo == null ? String(hi) : `${lo}–${hi}`;
    }
  }
  return Object.entries(f).filter(([, v]) => v != null && v !== "" &&
    !(Array.isArray(v) && v.length === 0));
}

function SourceCard({ id, block, children, wide, property, compact }: {
  id: string;
  block: NadlanSourceBlock;
  children?: React.ReactNode;
  wide?: boolean;
  property: NadlanProperty;
  /** The summary above already shows this source's fields: keep only the way
   *  to its own rows, so nothing is said twice. */
  compact?: boolean;
}) {
  const [open, setOpen] = useState(false);
  const fields = visibleFields(id, block, property);
  const canOpen = DETAIL_SOURCES.has(id) && Object.keys(block.fields).some((k) => {
    const v = block.fields[k];
    return v != null && v !== "" && !(Array.isArray(v) && v.length === 0);
  });
  return (
    <div style={{
      border: "1px solid var(--border)", borderRadius: 8, padding: compact ? "0.5rem 0.85rem" : "0.7rem 0.85rem",
      background: "var(--surface)", gridColumn: wide || open ? "1 / -1" : undefined,
    }}>
      <div style={{ fontWeight: 700, fontSize: "0.9rem", marginBottom: compact ? 0 : "0.4rem" }}>
        {SOURCE_LABELS[id] ?? id}
      </div>
      {compact ? null : fields.length ? (
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
      {children}
      {canOpen && (
        <button type="button" onClick={() => setOpen(!open)} aria-expanded={open}
          style={{
            display: "block", marginTop: "0.5rem", padding: "0.2rem 0.65rem", fontSize: "0.8rem",
            cursor: "pointer", border: "1px solid var(--border)", borderRadius: 6, background: "none",
          }}>
          {detailButtonLabel(id, block, open)}
        </button>
      )}
      {open && <SourceRowsView property={property} source={id} />}
      {/* The physical table name used to be printed under every card — a
          58-character `odata.ac1ae1fa_…` id that meant nothing to a reader and
          wrapped mid-word. It is the link's tooltip now, for whoever wants it. */}
      <a
        href={block.row_url}
        target="_blank"
        rel="noopener noreferrer"
        style={{ display: "inline-block", marginTop: compact ? "0.3rem" : "0.5rem", fontSize: "0.82rem" }}
        title={`${block.table}\n${block.console_sql}`}
      >
        שורות המקור ב-/data ↗
      <span className="sr-only"> (נפתח בחלון חדש)</span></a>
    </div>
  );
}

/** Where an address's point came from, in a few words — shown only for the
 *  sources a reader should weigh (the register is the default and says
 *  nothing). The raw `pip` match code used to be printed here instead. */
const POINT_SOURCE_SHORT: Record<string, string> = {
  municipal: "מיקום מהרשות המקומית",
  govmap_address: "מיקום מ-GovMap",
  govmap: "מיקום משוער",
  govmap_parcel: "מיקום משוער (מרכז חלקה)",
};

function pointFamily(s: string): string {
  return s.startsWith("municipal") ? "municipal" : s;
}

/** The parcel's addresses, shown INSIDE the address-list source card.
 *
 *  They used to be a table of their own under the source cards, which said the
 *  same thing twice: the address-list card counted the addresses and the table
 *  listed them. The list now lives in the card it comes from. */
function AddressList({ addresses }: { addresses: NadlanAddress[] }) {
  const [all, setAll] = useState(false);
  const shown = all ? addresses : addresses.slice(0, 12);
  return (
    <div style={{ marginTop: "0.5rem" }}>
      <ul style={{
        margin: 0, padding: 0, listStyle: "none", display: "grid", gap: "0.15rem 0.9rem",
        gridTemplateColumns: "repeat(auto-fill, minmax(200px, 1fr))", fontSize: "0.83rem",
      }}>
        {shown.map((a, i) => (
          <li key={i} style={{ borderTop: "1px solid var(--border)", padding: "0.2rem 0" }}>
            {a.street ?? "—"} <Ltr>{a.house ?? ""}{a.suffix ?? ""}</Ltr>
            {a.entrance ? <> · כניסה {a.entrance}</> : null}
            <span className="text-muted" style={{ fontSize: "0.75rem" }}>
              {a.zip7 ? <> · <Ltr>{a.zip7}</Ltr></> : null}
              {a.neighbourhood ? ` · ${a.neighbourhood}` : ""}
              {a.point_source && POINT_SOURCE_SHORT[pointFamily(a.point_source)]
                ? ` · ${POINT_SOURCE_SHORT[pointFamily(a.point_source)]}` : ""}
            </span>
          </li>
        ))}
      </ul>
      {addresses.length > 12 && (
        <button type="button" onClick={() => setAll(!all)}
          style={{
            marginTop: "0.3rem", padding: "0.15rem 0.6rem", fontSize: "0.78rem", cursor: "pointer",
            border: "1px solid var(--border)", borderRadius: 6, background: "none",
          }}>
          {all ? "פחות" : `כל ${addresses.length.toLocaleString("he-IL")} הכתובות`}
        </button>
      )}
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

/** A building's address rows, one line per house: the address list carries a
 *  row per ENTRANCE, so "דיזנגוף 50" used to appear twice with nothing to tell
 *  the two apart. */
function groupAddresses(addresses: NadlanAddress[]) {
  const groups = new Map<string, { label: string; entrances: string[]; zips: string[] }>();
  for (const a of addresses) {
    const label = `${a.street ?? "—"} ${a.house ?? ""}${a.suffix ?? ""}`.trim();
    const g = groups.get(label) ?? { label, entrances: [], zips: [] };
    if (a.entrance && !g.entrances.includes(a.entrance)) g.entrances.push(a.entrance);
    if (a.zip7 && !g.zips.includes(a.zip7)) g.zips.push(a.zip7);
    groups.set(label, g);
  }
  return [...groups.values()];
}

/** The pill next to the parcel title. It answers ONE question: how sure are we
 *  that this is the parcel. It used to say "התאמה מדויקת" on every result —
 *  including both parcels of an address whose rows disagreed by 250 m — because
 *  it described the gazetteer join, not the address. */
function MatchPill({ property }: { property: NadlanProperty }) {
  const am = property.address_match;
  let text: string | null = null;
  let good = false;
  if (am) {
    text = am.primary ? am.label : `מיקום חלופי · ${am.label}`;
    good = am.primary && am.precise;
  } else if (property.match.confidence !== "exact") {
    text = "גוש-חלקה לא חד-ערכי";
  }
  if (!text) return null;
  return (
    <span style={{
      fontSize: "0.75rem", padding: "0.12rem 0.5rem", borderRadius: 999,
      background: good ? "var(--tint-good-bg)" : "#fef3c7",
      color: good ? "var(--success)" : "#833909",
    }}>
      {text}
    </span>
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
  const houses = groupAddresses(id.addresses);
  const SHOWN = 3;

  return (
    <div style={{
      border: "1px solid var(--border)", borderRadius: 10, padding: "0.9rem 1rem",
      marginBottom: "0.75rem", background: "var(--surface)",
      opacity: property.address_match && !property.address_match.primary ? 0.92 : 1,
    }}>
      <div className="flex" style={{ justifyContent: "space-between", alignItems: "baseline", gap: "0.6rem", flexWrap: "wrap" }}>
        <h3 style={{ margin: 0, fontSize: "1.05rem" }}
            title={id.point ? `${id.point.lat.toFixed(6)}, ${id.point.lon.toFixed(6)}` : undefined}>
          גוש {id.gush}
          {id.gush_suffix ? `/${id.gush_suffix}` : ""} · חלקה {id.helka}
          {id.settlement.name ? ` · ${id.settlement.name}` : ""}
        </h3>
        <div className="flex" style={{ gap: "0.4rem", alignItems: "center" }}>
          {id.distance_m != null && (
            <span className="text-sm text-muted">{Math.round(id.distance_m).toLocaleString("he-IL")} מ׳</span>
          )}
          <MatchPill property={property} />
        </div>
      </div>

      {/* What is ON the parcel, said once: each house with its entrances and
          zips. The street, the zips and a raw coordinate used to be three
          separate lines here and then repeated in three source cards below. */}
      <div className="text-sm" style={{ marginTop: "0.45rem", lineHeight: 1.8 }}>
        {houses.length > 0 ? (
          <div>
            {houses.slice(0, SHOWN).map((h, i) => (
              <span key={h.label}>
                {i > 0 && " · "}
                <b>{h.label}</b>
                {h.entrances.length > 1 && (
                  <span className="text-muted"> ({h.entrances.length} כניסות)</span>
                )}
                {h.zips.length > 0 && (
                  <span className="text-muted"> · מיקוד <Ltr>{h.zips.join(", ")}</Ltr></span>
                )}
              </span>
            ))}
            {houses.length > SHOWN && (
              <span className="text-muted"> · ועוד {(houses.length - SHOWN).toLocaleString("he-IL")} כתובות</span>
            )}
          </div>
        ) : id.streets.length > 0 ? (
          <div>
            רחוב: {id.streets.join(" · ")}
            <span className="text-muted"> · אין כתובות עם מספר בית בחלקה</span>
          </div>
        ) : (
          <div className="text-muted">אין כתובות ידועות בחלקה.</div>
        )}
        {property.sources.parcels.fields.legal_area != null && (
          <div className="text-muted" style={{ fontSize: "0.82rem" }}>
            שטח רשום <Ltr>{fmt(property.sources.parcels.fields.legal_area)}</Ltr> מ״ר
            {property.sources.parcels.fields.status
              ? ` · ${String(property.sources.parcels.fields.status)}` : ""}
          </div>
        )}
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
        {expanded ? "הסתרת המידע המלא" : "כל המידע על תא השטח"}
      </button>

      {expanded && (
        <>
          <div style={{
            display: "grid", gap: "0.6rem", marginTop: "0.7rem",
            gridTemplateColumns: "repeat(auto-fit, minmax(230px, 1fr))",
          }}>
            {/* Sources with something the summary does not already say come
                first and in full; the rest keep only their way to the rows. */}
            {Object.entries(property.sources)
              .sort(([a], [b]) => Number(COMPACT_SOURCES.has(a)) - Number(COMPACT_SOURCES.has(b)))
              .map(([k, b]) => (
              <SourceCard key={k} id={k} block={b as NadlanSourceBlock} property={property}
                          compact={COMPACT_SOURCES.has(k)}
                          wide={k === "address_list" && id.addresses.length > 0}>
                {k === "address_list" && id.addresses.length > 0 && (
                  <AddressList addresses={id.addresses} />
                )}
              </SourceCard>
            ))}
          </div>

          <NadlanCubes property={property} />
        </>
      )}
    </div>
  );
}
