/**
 * Everything else OVER holds about one parcel's ground, as cubes you open.
 *
 * The lookup envelope answers what a property IS. This answers what is known
 * ABOUT the ground it stands on — its תב"ע, the רמ"י plans and tenders around
 * it, the crime in its statistical area, the noise over it — and that is far
 * too much to compute for every result of a radius search. So the panel loads
 * in two steps, each only when asked for:
 *
 *   1. the catalog for this parcel, with a feature count per cube from ONE
 *      bbox probe (cheap: index lookups, no attributes read);
 *   2. one cube's contents, when the reader opens that cube.
 *
 * Cubes the deployment does not hold are not shown at all; cubes that hold
 * nothing at this point are folded away under one line, so the reader sees
 * what IS there first and can still check what was looked for.
 */
import { useEffect, useMemo, useState } from "react";
import {
  nadlan, type NadlanCubeData, type NadlanCubeInfo, type NadlanProperty,
} from "../../api/client";
import NadlanTile, { Ltr, TileTable } from "./NadlanTile";

const n = (v: number | null | undefined, digits = 0) =>
  v == null ? "—" : v.toLocaleString("he-IL", { maximumFractionDigits: digits });

const PILL: React.CSSProperties = {
  padding: "0.2rem 0.65rem", fontSize: "0.8rem", cursor: "pointer",
  border: "1px solid var(--border)", borderRadius: 999, background: "none",
};

// ── crime ──
type Scope = "area" | "settlement" | "national";
const SCOPE_LABEL: Record<Scope, string> = {
  area: "האזור הסטטיסטי", settlement: "היישוב", national: "כל הארץ",
};

/** The house's statistical area against its town and the country. Per 1,000
 *  residents, because an absolute count of cases means nothing across areas of
 *  different size; the counts are kept beside the rate so a small denominator
 *  can be seen rather than trusted. */
function CrimeView({ data, onYear }: { data: NadlanCubeData; onYear: (y: string) => void }) {
  const s = data.scopes!;
  // No area resolved means nothing to focus on there; the town is the next grain.
  const [focus, setFocus] = useState<Scope>(s.area.code != null ? "area" : "settlement");
  const scopes: Scope[] = ["area", "settlement", "national"];
  const ratio = (a: number | null, b: number | null) =>
    a != null && b ? (a / b) : null;
  const vsNational = ratio(s[focus].per_1000, s.national.per_1000);

  return (
    <div>
      <div className="flex" style={{ gap: "0.35rem", flexWrap: "wrap", alignItems: "center", marginBottom: "0.5rem" }}>
        {scopes.map((k) => (
          <button key={k} type="button" onClick={() => setFocus(k)} aria-pressed={focus === k}
            style={{ ...PILL, fontWeight: focus === k ? 700 : 400,
                     borderColor: focus === k ? "var(--primary)" : "var(--border)",
                     color: focus === k ? "var(--primary)" : undefined }}>
            {SCOPE_LABEL[k]}
          </button>
        ))}
        {data.years && data.years.length > 1 && (
          <label className="text-sm" style={{ marginInlineStart: "auto" }}>
            שנה:{" "}
            <select value={data.year} onChange={(e) => onYear(e.target.value)}
              style={{ padding: "0.15rem 0.4rem" }}>
              {data.years.map((y) => <option key={y} value={y}>{y}</option>)}
            </select>
          </label>
        )}
      </div>

      <div className="text-sm" style={{ lineHeight: 1.8 }}>
        <strong>{SCOPE_LABEL[focus]}</strong>
        {s[focus].name ? ` · ${s[focus].name}` : ""}
        {" — "}<Ltr>{n(s[focus].total)}</Ltr> תיקים ב-{data.year}
        {s[focus].per_1000 != null && <> · <Ltr>{n(s[focus].per_1000, 1)}</Ltr> לכל 1,000 תושבים</>}
        {s[focus].population != null && (
          <span className="text-muted"> (אוכלוסייה <Ltr>{n(s[focus].population)}</Ltr>)</span>
        )}
        {focus !== "national" && vsNational != null && (
          <div className="text-muted">
            {vsNational >= 1
              ? <>פי <Ltr>{n(vsNational, 2)}</Ltr> מהשיעור הארצי</>
              : <><Ltr>{n(vsNational * 100)}%</Ltr> מהשיעור הארצי</>}
          </div>
        )}
      </div>

      <div tabIndex={0} role="region" aria-label="פשיעה לפי קבוצת עבירה"
           className="scroll-region nadlan-table-wrap" style={{ marginTop: "0.5rem" }}>
        <table className="nadlan-table">
          <thead>
            <tr>
              <th scope="col">קבוצת עבירה</th>
              {scopes.map((k) => (
                <th key={k} scope="col" style={{ fontWeight: focus === k ? 700 : 500 }}>
                  {SCOPE_LABEL[k]}
                  <div style={{ fontWeight: 400, fontSize: "0.72rem" }}>ל-1,000 (תיקים)</div>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {data.groups!.filter((g) => g.national > 0).map((g) => (
              <tr key={g.group}>
                <td>{g.group}</td>
                {scopes.map((k) => (
                  <td key={k} style={{ whiteSpace: "nowrap", fontWeight: focus === k ? 600 : 400 }}>
                    <Ltr>{n(g[`${k}_per_1000` as const], 1)}</Ltr>{" "}
                    <span className="text-muted" style={{ fontSize: "0.75rem" }}>(<Ltr>{n(g[k])}</Ltr>)</span>
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {data.notes && data.notes.length > 0 && (
        <ul style={{ margin: "0.4rem 0 0", paddingInlineStart: "1.1rem", fontSize: "0.8rem", color: "var(--warning)" }}>
          {data.notes.map((t, i) => <li key={i}>{t}</li>)}
        </ul>
      )}
      <div className="text-muted" style={{ fontSize: "0.75rem", marginTop: "0.35rem", lineHeight: 1.6 }}>
        תיק נרשם במקום העבירה, לא במקום מגוריו של מי שביצע אותה. השיעור מחושב על אוכלוסיית
        האזור (למ״ס 2024) ועל אוכלוסיית היישובים שבמרשם; באזור קטן כמה תיקים מזיזים אותו מאוד.
        {data.row_url && <> <a href={data.row_url} target="_blank" rel="noopener noreferrer">שורות המקור ב-/data ↗</a></>}
      </div>
    </div>
  );
}

// ── spatial ──
/** A layer value as a reader reads it. GIS layers publish dates as ISO
 *  timestamps ("2022-12-07T00:00:00.000Z") or as packed digits
 *  ("20221211124944441"); both read as a date here, and the raw value stays in
 *  the tooltip. Anything else is shown as published. */
function displayValue(v: string): string {
  let m = /^(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{2}):(\d{2})(?::\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}(?::?\d{2})?)?)?$/.exec(v);
  if (!m) {
    const p = /^((?:19|20)\d{2})(\d{2})(\d{2})(\d{2})(\d{2})\d{2,5}$/.exec(v);
    if (p && Number(p[2]) >= 1 && Number(p[2]) <= 12 && Number(p[3]) >= 1 && Number(p[3]) <= 31) m = p;
  }
  if (!m) return v;
  const [, y, mo, d, hh, mm] = m;
  const time = hh && mm && !(hh === "00" && mm === "00") ? ` ${hh}:${mm}` : "";
  return `${d}/${mo}/${y}${time}`;
}

/** A layer's records as one table: a row per record, a column per field
 *  (in the order the records first name them), and the distance first when
 *  the cube looks around the parcel rather than on it. */
function SpatialView({ data }: { data: NadlanCubeData }) {
  const feats = data.features ?? [];
  const radius = data.cube.radius_m ?? 0;
  const columns = useMemo(() => {
    const seen = new Map<string, string>();
    for (const f of feats) for (const fl of f.fields) if (!seen.has(fl.key)) seen.set(fl.key, fl.label);
    return [...seen.entries()].map(([key, label]) => ({ key, label }));
  }, [feats]);
  const rows = feats.map((f) => {
    const by = new Map(f.fields.map((fl) => [fl.key, fl]));
    const cells: React.ReactNode[] = columns.map((c) => {
      const fl = by.get(c.key);
      if (!fl) return null;
      return fl.url
        ? <a href={fl.url} target="_blank" rel="noopener noreferrer">
            {fl.value}<span className="sr-only"> (נפתח בחלון חדש)</span>
          </a>
        : <span title={displayValue(fl.value) !== fl.value ? fl.value : undefined}>
            {displayValue(fl.value)}
          </span>;
    });
    if (radius > 0) {
      cells.unshift(f.distance_m < 1 ? "על החלקה" : <><Ltr>{n(f.distance_m)}</Ltr> מ׳</>);
    }
    return cells;
  });
  const cols = radius > 0 ? [{ key: "_d", label: "מרחק" }, ...columns] : columns;

  return (
    <div>
      {feats.length === 0 ? (
        <div className="text-sm text-muted">
          {data.cube.grain === "gush_helka"
            ? "לא נמצאו רשומות על הגוש והחלקה האלה."
            : radius ? `אין רשומות בשכבה בתוך ${radius} מ׳ מהחלקה.` : "החלקה אינה בתוך אף רשומה בשכבה."}
        </div>
      ) : (
        <>
          <TileTable label={data.cube.title} columns={cols} rows={rows} />
          {(data.count ?? 0) > feats.length && (
            <div className="text-sm text-muted" style={{ marginTop: "0.35rem" }}>
              מוצגות {feats.length} מתוך {data.count}.
            </div>
          )}
        </>
      )}
      {data.notes && data.notes.length > 0 && (
        <ul className="text-muted" style={{ fontSize: "0.78rem", margin: "0.5rem 0 0", paddingInlineStart: "1.1rem", lineHeight: 1.6 }}>
          {data.notes.map((nt, i) => <li key={i}>{nt}</li>)}
        </ul>
      )}
    </div>
  );
}

// ── one cube ──
function Cube({ info, property }: { info: NadlanCubeInfo; property: NadlanProperty }) {
  const id = property.identity;
  const [open, setOpen] = useState(false);
  const [year, setYear] = useState<string | undefined>(undefined);
  const [data, setData] = useState<NadlanCubeData | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!open) return;
    let cancelled = false;
    setError(null);
    nadlan.parcelLayer(id.gush, id.helka, info.id, id.gush_suffix, year)
      .then((r) => { if (!cancelled) setData(r); })
      .catch((e) => { if (!cancelled) setError(e instanceof Error ? e.message : "הטעינה נכשלה"); });
    return () => { cancelled = true; };
  }, [open, year, id.gush, id.helka, id.gush_suffix, info.id]);

  // The source links come with the data; until the cube is opened there are none.
  const srcs = data?.sources ?? [];
  const links = srcs.map((s) => ({
    href: s.row_url, title: s.table,
    label: srcs.length > 1 ? `שורות: ${s.table}` : "שורות המקור",
  }));
  for (const u of new Set(srcs.map((s) => s.page_url))) links.push({ href: u, label: "גרסאות המאגר", title: "" });
  if (data?.row_url && !links.length) links.push({ href: data.row_url, label: "שורות המקור", title: "" });

  return (
    <NadlanTile
      title={info.title}
      badge={info.hits != null ? <Ltr>{info.hits.toLocaleString("he-IL")}</Ltr> : undefined}
      badgeTone={info.hits ? "info" : "muted"}
      label={info.group}
      open={open}
      onToggle={() => setOpen(!open)}
      muted={info.hits === 0}
      links={open ? links : []}
      detail={
        <>
          {error && <div className="text-sm" style={{ color: "var(--danger)" }}>{error}</div>}
          {!error && !data && <div className="text-sm text-muted">טוען…</div>}
          {data && !data.available && <div className="text-sm text-muted">השכבה אינה זמינה כרגע.</div>}
          {data?.available && (info.grain === "stat_area"
            ? <CrimeView data={data} onYear={setYear} />
            : <SpatialView data={data} />)}
        </>
      }
    >
      <div className="text-muted nadlan-tile-clamp">{info.description}</div>
    </NadlanTile>
  );
}

// ── the cubes, as tiles in the property's grid ──
/** Rendered INSIDE the property card's tile grid: a heading row, a tile per
 *  cube that has records here, and the empty ones behind one toggle. */
export default function NadlanCubes({ property }: { property: NadlanProperty }) {
  const id = property.identity;
  const [cubes, setCubes] = useState<NadlanCubeInfo[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [showEmpty, setShowEmpty] = useState(false);

  useEffect(() => {
    let cancelled = false;
    setCubes(null); setError(null);
    nadlan.parcelLayers(id.gush, id.helka, id.gush_suffix)
      .then((r) => { if (!cancelled) setCubes(r.data.filter((c) => c.available)); })
      .catch((e) => { if (!cancelled) setError(e instanceof Error ? e.message : "טעינת השכבות נכשלה"); });
    return () => { cancelled = true; };
  }, [id.gush, id.helka, id.gush_suffix]);

  const [withData, empty] = useMemo(() => {
    const all = cubes ?? [];
    return [all.filter((c) => c.hits !== 0), all.filter((c) => c.hits === 0)];
  }, [cubes]);

  const govmapHref = `https://www.govmap.gov.il/?q=${encodeURIComponent(`גוש ${id.gush} חלקה ${id.helka}`)}`;

  return (
    <>
      <div className="nadlan-tiles-heading">
        <div>
          <h4>מה עוד ידוע על תא השטח</h4>
          <div className="text-muted" style={{ fontSize: "0.8rem", marginTop: "0.2rem", lineHeight: 1.6 }}>
            המספר בכל כרטיס הוא כמה רשומות בשכבה נוגעות בחלקה (או נמצאות ברדיוס שצוין).
            הפרטים נטענים בלחיצה על "הרחבה".
          </div>
        </div>
        <a href={govmapHref} target="_blank" rel="noopener noreferrer" className="text-sm">
          החלקה ב-GovMap ↗<span className="sr-only"> (נפתח בחלון חדש)</span>
        </a>
      </div>
      {error && <div className="text-sm" style={{ color: "var(--danger)", gridColumn: "1 / -1" }}>{error}</div>}
      {!error && !cubes && <div className="text-sm text-muted" style={{ gridColumn: "1 / -1" }}>טוען שכבות…</div>}
      {withData.map((c) => <Cube key={c.id} info={c} property={property} />)}
      {empty.length > 0 && (
        <div style={{ gridColumn: "1 / -1" }}>
          <button type="button" className="btn-secondary" onClick={() => setShowEmpty(!showEmpty)}
                  aria-expanded={showEmpty} style={{ fontSize: "0.85rem", padding: "0.35rem 0.85rem" }}>
            {showEmpty ? "הסתרת" : "הצגת"} {empty.length} שכבות ללא רשומות בחלקה
          </button>
        </div>
      )}
      {showEmpty && empty.map((c) => <Cube key={c.id} info={c} property={property} />)}
    </>
  );
}
