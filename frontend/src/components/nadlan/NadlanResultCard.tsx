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
import NadlanTile, { Ltr, TileTable, type TileLink } from "./NadlanTile";
import { DEAL_HEADER_TITLES, declaredDiffers, portionLabel } from "../../utils/dealFields";

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

/** Tile titles: what the source IS for this parcel, with its publisher on the
 *  muted line under it (SOURCE_ORIGIN). */
const SOURCE_TITLES: Record<string, string> = {
  parcels: "חלקה רשומה",
  gazetteer: "גזטיר הנכסים",
  postal: "מיקוד",
  address_list: "כתובות בחלקה",
};
const SOURCE_ORIGIN: Record<string, string> = {
  parcels: "המרכז למיפוי ישראל — שכבת החלקות",
  gazetteer: "רשות המסים — גזטיר הנכסים",
  postal: "דואר ישראל — קובץ המיקוד",
  address_list: "רשימת כתובות בישראל",
};
/** Richest first: what a reader opens before what only confirms the parcel. */
const SOURCE_ORDER = ["gazetteer", "address_list", "postal", "parcels"];

/** The sources whose own rows can be opened inside their card. */
const DETAIL_SOURCES = new Set(["gazetteer", "parcels", "postal", "stat_area"]);

/** Sources whose summary block above carries their way to the rows too, so
 *  they get no card of their own below. The statistical area and the deals
 *  used to appear twice: in full in the summary, and again further down as a
 *  card holding nothing but a link — which read as a second, empty answer. */
const FOLDED_SOURCES = new Set(["stat_area", "deals"]);

/** True when a source has anything for this parcel. A card for a source with
 *  nothing (no postal row for a parcel without zips, "0 linked addresses"
 *  under a summary that already says there are none) is a box with a link to
 *  "WHERE false" — it is left out. */
function sourceHasData(block: NadlanSourceBlock): boolean {
  return Object.values(block.fields).some(
    (v) => v != null && v !== "" && v !== 0 && !(Array.isArray(v) && v.length === 0));
}

/** The link to a source's own rows on /data, as a tile footer link. */
function sourceLink(block: NadlanSourceBlock | undefined): TileLink[] {
  if (!block) return [];
  return [{ href: block.row_url, label: "שורות המקור ב-/data", title: `${block.table}\n${block.console_sql}` }];
}

const DETAIL_PAGE = 50;

function expandLabel(id: string, block: NadlanSourceBlock): string {
  const n = Number(block.fields.n_assets);
  if (id === "gazetteer" && n > 0) return `כל ${n.toLocaleString("he-IL")} הנכסים`;
  if (id === "postal") return "שורות המיקוד";
  return "כל השדות במקור";
}

/** A source's own rows for this parcel, fetched when its tile is opened.
 *
 *  The tile shows the summary the envelope carries ("12 נכסים"); this is the
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

  if (error) return <div className="text-sm" style={{ color: "var(--danger)" }}>{error}</div>;
  if (!data) return <div className="text-sm text-muted">טוען…</div>;
  if (!data.rows.length) return <div className="text-sm text-muted">לא נמצאו שורות במקור לחלקה זו.</div>;

  if (data.total === 1) {
    // One row: a two-column field list reads better than a one-row table.
    const row = data.rows[0];
    return (
      <TileTable
        label={`שורת ${SOURCE_LABELS[source] ?? source}`}
        columns={[{ key: "f", label: "שדה" }, { key: "v", label: "ערך" }]}
        rows={data.columns.map((c, i) => [<span title={c.key}>{c.label}</span>, row[i] ?? "—"])}
      />
    );
  }

  return (
    <div>
      <TileTable
        label={`שורות ${SOURCE_LABELS[source] ?? source}`}
        columns={data.columns.map((c) => ({ key: c.key, label: c.label, title: c.key }))}
        rows={data.rows.map((r) => r.map((v) => v ?? "—"))}
      />
      <div className="text-sm text-muted" style={{ marginTop: "0.4rem" }}>
        מוצגות <Ltr>{data.rows.length.toLocaleString("he-IL")}</Ltr> מתוך{" "}
        <Ltr>{data.total.toLocaleString("he-IL")}</Ltr> שורות.
        {data.rows.length < data.total && (
          <button type="button" className="btn-secondary" onClick={more} disabled={loadingMore}
            style={{ marginInlineStart: "0.6rem", padding: "0.2rem 0.7rem", fontSize: "0.8rem" }}>
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

/** Where an address's point came from, in a few words — shown only for the
 *  sources a reader should weigh (the register is the default and says
 *  nothing). */
const POINT_SOURCE_SHORT: Record<string, string> = {
  municipal: "מיקום מהרשות המקומית",
  govmap_address: "מיקום מ-GovMap",
  govmap: "מיקום משוער",
  govmap_parcel: "מיקום משוער (מרכז חלקה)",
};

function pointFamily(s: string): string {
  return s.startsWith("municipal") ? "municipal" : s;
}

/** The parcel's addresses as a table — the address-list tile's detail. */
function AddressTable({ addresses }: { addresses: NadlanAddress[] }) {
  return (
    <TileTable
      label="כתובות בחלקה"
      columns={[
        { key: "a", label: "כתובת" }, { key: "e", label: "כניסה" }, { key: "z", label: "מיקוד" },
        { key: "n", label: "שכונה" }, { key: "p", label: "מקור המיקום" },
      ]}
      rows={addresses.map((a) => [
        <>{a.street ?? "—"} <Ltr>{a.house ?? ""}{a.suffix ?? ""}</Ltr></>,
        a.entrance ?? "—",
        a.zip7 ? <Ltr>{a.zip7}</Ltr> : "—",
        a.neighbourhood ?? "—",
        a.point_source ? (POINT_SOURCE_SHORT[pointFamily(a.point_source)] ?? "מרשם הכתובות") : "—",
      ])}
    />
  );
}

/** One source of the crosswalk as a tile: its count, its few fields, and its
 *  own rows behind "הרחבה". */
function SourceTile({ id, block, property }: {
  id: string; block: NadlanSourceBlock; property: NadlanProperty;
}) {
  const [open, setOpen] = useState(false);
  const addrs = property.identity.addresses;
  const fields = visibleFields(id, block, property);
  const isAddresses = id === "address_list";
  const canOpen = isAddresses ? addrs.length > 0 : DETAIL_SOURCES.has(id) && sourceHasData(block);

  let badge: React.ReactNode;
  let body: React.ReactNode;
  if (isAddresses) {
    badge = <Ltr>{addrs.length.toLocaleString("he-IL")}</Ltr>;
    const houses = groupAddresses(addrs);
    body = houses.length ? (
      <div className="nadlan-tile-clamp">
        {houses.slice(0, 4).map((h) => h.label).join(" · ")}
        {houses.length > 4 && <span className="text-muted"> ועוד {houses.length - 4}</span>}
      </div>
    ) : <span className="text-muted">אין כתובות מקושרות.</span>;
  } else {
    if (id === "gazetteer" && block.fields.n_assets != null) {
      badge = <Ltr>{Number(block.fields.n_assets).toLocaleString("he-IL")}</Ltr>;
    }
    const shown = fields.filter(([k]) => !(id === "gazetteer" && k === "n_assets")).slice(0, 5);
    body = shown.length ? (
      <dl className="nadlan-kv">
        {shown.map(([k, v]) => (
          <div key={k} style={{ display: "contents" }}>
            <dt>{FIELD_LABELS[k] ?? k}</dt>
            <dd><Ltr>{fmt(v, k)}</Ltr></dd>
          </div>
        ))}
      </dl>
    ) : null;
  }

  return (
    <NadlanTile
      title={SOURCE_TITLES[id] ?? SOURCE_LABELS[id] ?? id}
      badge={badge}
      label={SOURCE_ORIGIN[id]}
      open={open}
      onToggle={canOpen ? () => setOpen(!open) : undefined}
      expandLabel={isAddresses ? "כל הכתובות" : expandLabel(id, block)}
      detail={isAddresses ? <AddressTable addresses={addrs} /> : <SourceRowsView property={property} source={id} />}
      links={sourceLink(block)}
    >
      {body}
    </NadlanTile>
  );
}

/** The CBS statistical area. The socio-economic cluster carries its own
 *  division year on purpose: it is published on the 2011 areas, which are NOT
 *  the 2022 areas this property's code comes from. */
function StatAreaTile({ area, property }: {
  area: NonNullable<NadlanProperty["stat_area"]>;
  property: NadlanProperty;
}) {
  const [open, setOpen] = useState(false);
  const block = property.sources.stat_area as NadlanSourceBlock | undefined;
  return (
    <NadlanTile
      title="אזור סטטיסטי (א״ס)"
      badge={area.code != null ? <Ltr>{area.code}</Ltr> : undefined}
      label={'הלשכה המרכזית לסטטיסטיקה'}
      open={open}
      onToggle={block ? () => setOpen(!open) : undefined}
      expandLabel="כל השדות במקור"
      detail={<SourceRowsView property={property} source="stat_area" />}
      links={sourceLink(block)}
    >
      <div>
        {area.settlement_name ?? "—"}
        {area.yishuv_stat != null && <span className="text-muted"> · קוד מלא <Ltr>{area.yishuv_stat}</Ltr></span>}
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
          אשכול חברתי-כלכלי <Ltr>{area.socio.eshkol}</Ltr>{" "}
          <span style={{ fontSize: "0.78rem" }}>
            (מדד {area.socio.index_year}, חלוקת {area.socio.division})
          </span>
        </div>
      )}
    </NadlanTile>
  );
}

const DEAL_COLUMNS = [
  "תאריך", "שווי מכירה", "תמורה מוצהרת", "חלק נמכר", "מהות", "תת-חלקה", "חדרים",
  "שטח (מ״ר)", "₪ למ״ר", "שנת בנייה",
].map((label) => ({ key: label, label, title: DEAL_HEADER_TITLES[label] }));

/** The deal history. The summary is already in the envelope; the list is
 *  fetched only when the tile is opened, because a parcel in a condo tower
 *  holds hundreds of deals. */
function DealsTile({ property }: { property: NadlanProperty }) {
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
  const latest = summary.latest;

  return (
    <NadlanTile
      title="עסקאות מקרקעין"
      badge={<Ltr>{summary.deals.toLocaleString("he-IL")}</Ltr>}
      label="רשות המסים — מיסוי מקרקעין"
      open={open}
      onToggle={() => setOpen(!open)}
      expandLabel="כל העסקאות"
      links={[
        ...sourceLink(property.sources.deals as NadlanSourceBlock | undefined),
        { href: dealsHref, label: "במאגר עסקאות נדל״ן ←", internal: true },
      ]}
      detail={
        <>
          {error && <div className="text-sm" style={{ color: "var(--danger)" }}>{error}</div>}
          {!error && !rows && <div className="text-sm text-muted">טוען עסקאות…</div>}
          {rows && rows.length === 0 && <div className="text-sm text-muted">לא נמצאו עסקאות לחלקה זו.</div>}
          {rows && rows.length > 0 && (
            <>
              <TileTable
                label="עסקאות בחלקה"
                columns={DEAL_COLUMNS}
                rows={rows.map((d) => [
                  <Ltr>{heDate(d.date)}</Ltr>,
                  <Ltr>{d.amount != null ? NIS.format(d.amount) : "—"}</Ltr>,
                  <span style={{
                    color: declaredDiffers(d) ? "var(--warning)" : "var(--text-muted)",
                    fontWeight: declaredDiffers(d) ? 600 : undefined,
                  }} title={declaredDiffers(d) ? "שונה משווי המכירה" : "זהה לשווי המכירה"}>
                    <Ltr>{d.declared_amount != null ? NIS.format(d.declared_amount) : "—"}</Ltr>
                  </span>,
                  <Ltr>{portionLabel(d)}</Ltr>,
                  d.nature ?? "—",
                  <Ltr>{d.sub_parcel ?? "—"}</Ltr>,
                  <Ltr>{d.rooms || "—"}</Ltr>,
                  <Ltr>{d.area_sqm || "—"}</Ltr>,
                  <Ltr>{d.price_per_sqm_normalized ? NIS.format(d.price_per_sqm_normalized) : "—"}</Ltr>,
                  <Ltr>{d.year_built || "—"}</Ltr>,
                ])}
              />
              {total > rows.length && (
                <div className="text-sm text-muted" style={{ marginTop: "0.4rem" }}>
                  מוצגות <Ltr>{rows.length}</Ltr> מתוך <Ltr>{total.toLocaleString("he-IL")}</Ltr> עסקאות.{" "}
                  <Link to={dealsHref}>להמשך במאגר עסקאות נדל״ן</Link>
                </div>
              )}
            </>
          )}
        </>
      }
    >
      <div>
        <Ltr>{heDate(summary.first_deal)}</Ltr> עד <Ltr>{heDate(summary.last_deal)}</Ltr>
        {summary.sub_parcels > 1 && (
          <span className="text-muted"> · <Ltr>{summary.sub_parcels}</Ltr> תת-חלקות</span>
        )}
      </div>
      {latest?.date && (
        <div className="text-muted">
          אחרונה: <Ltr>{heDate(latest.date)}</Ltr>
          {latest.amount != null && <> · <Ltr>{NIS.format(latest.amount)}</Ltr></>}
          {declaredDiffers(latest) && latest.declared_amount != null && (
            <> (מוצהר <Ltr>{NIS.format(latest.declared_amount)}</Ltr>)</>
          )}
          {latest.portion_fraction != null && latest.portion_fraction < 0.9995 && (
            <> · חלק נמכר <Ltr>{portionLabel(latest)}</Ltr></>
          )}
          {latest.nature ? ` · ${latest.nature}` : ""}
          {latest.rooms ? <> · <Ltr>{latest.rooms}</Ltr> חד׳</> : null}
        </div>
      )}
    </NadlanTile>
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
    <section className="card nadlan-property" style={{
      opacity: property.address_match && !property.address_match.primary ? 0.92 : 1,
    }}>
      <div className="flex" style={{ justifyContent: "space-between", alignItems: "baseline", gap: "0.6rem", flexWrap: "wrap" }}>
        <h3 style={{ margin: 0, fontSize: "1.15rem" }}
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

      {property.match.notes.length > 0 && (
        <ul style={{ margin: "0.5rem 0 0", paddingInlineStart: "1.1rem", fontSize: "0.82rem", color: "var(--warning)" }}>
          {property.match.notes.map((n, i) => <li key={i}>{n}</li>)}
        </ul>
      )}

      {/* One grid for all of it: the two facts people come for, then — when
          the property is opened — every source and every information cube,
          each a tile of the same shape. */}
      <div className="nadlan-tiles">
        {property.deals && <DealsTile property={property} />}
        {property.stat_area && <StatAreaTile area={property.stat_area} property={property} />}
        {expanded && Object.entries(property.sources)
          .filter(([k, b]) => !FOLDED_SOURCES.has(k) && sourceHasData(b as NadlanSourceBlock))
          .sort(([a], [b]) => SOURCE_ORDER.indexOf(a) - SOURCE_ORDER.indexOf(b))
          .map(([k, b]) => (
            <SourceTile key={k} id={k} block={b as NadlanSourceBlock} property={property} />
          ))}
        {expanded && <NadlanCubes property={property} />}
      </div>

      <button type="button" className="btn-secondary" onClick={onToggle} aria-expanded={expanded}
        style={{ marginTop: "1rem", padding: "0.4rem 1rem", fontSize: "0.9rem" }}>
        {expanded ? "הסתרת המידע המלא" : "כל המידע על תא השטח"}
      </button>
    </section>
  );
}
