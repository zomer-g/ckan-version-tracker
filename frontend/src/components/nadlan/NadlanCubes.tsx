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

function Ltr({ children }: { children: React.ReactNode }) {
  return <span style={{ unicodeBidi: "isolate", direction: "ltr", display: "inline-block" }}>{children}</span>;
}

const n = (v: number | null | undefined, digits = 0) =>
  v == null ? "—" : v.toLocaleString("he-IL", { maximumFractionDigits: digits });

const PILL: React.CSSProperties = {
  padding: "0.2rem 0.65rem", fontSize: "0.8rem", cursor: "pointer",
  border: "1px solid var(--border)", borderRadius: 999, background: "none",
};

// ── crime ─────────────────────────────────────────────────────────────────────
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

      <div tabIndex={0} role="region" aria-label="פשיעה לפי קבוצת עבירה" className="scroll-region"
           style={{ overflowX: "auto", marginTop: "0.4rem" }}>
        <table style={{ width: "100%", fontSize: "0.82rem", borderCollapse: "collapse" }}>
          <thead>
            <tr style={{ color: "var(--text-muted)" }}>
              <th scope="col" style={{ textAlign: "start", padding: "0.2rem 0.4rem" }}>קבוצת עבירה</th>
              {scopes.map((k) => (
                <th key={k} scope="col" style={{
                  textAlign: "start", padding: "0.2rem 0.4rem", whiteSpace: "nowrap",
                  fontWeight: focus === k ? 700 : 500,
                }}>
                  {SCOPE_LABEL[k]}
                  <div style={{ fontWeight: 400, fontSize: "0.72rem" }}>ל-1,000 (תיקים)</div>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {data.groups!.filter((g) => g.national > 0).map((g) => (
              <tr key={g.group} style={{ borderTop: "1px solid var(--border)" }}>
                <td style={{ padding: "0.2rem 0.4rem" }}>{g.group}</td>
                {scopes.map((k) => (
                  <td key={k} style={{ padding: "0.2rem 0.4rem", whiteSpace: "nowrap",
                                       fontWeight: focus === k ? 600 : 400 }}>
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

// ── spatial ───────────────────────────────────────────────────────────────────
function SpatialView({ data }: { data: NadlanCubeData }) {
  const feats = data.features ?? [];
  const radius = data.cube.radius_m ?? 0;
  return (
    <div>
      {feats.length === 0 ? (
        <div className="text-sm text-muted">
          {radius ? `אין רשומות בשכבה בתוך ${radius} מ׳ מהחלקה.` : "החלקה אינה בתוך אף רשומה בשכבה."}
        </div>
      ) : (
        <div style={{ display: "grid", gap: "0.45rem" }}>
          {feats.map((f, i) => (
            <div key={i} style={{ border: "1px solid var(--border)", borderRadius: 6, padding: "0.45rem 0.6rem" }}>
              {radius > 0 && (
                <div className="text-muted" style={{ fontSize: "0.75rem", marginBottom: "0.2rem" }}>
                  {f.distance_m < 1 ? "על החלקה" : <><Ltr>{n(f.distance_m)}</Ltr> מ׳ מהחלקה</>}
                </div>
              )}
              <dl style={{ margin: 0, display: "grid", gridTemplateColumns: "auto 1fr", gap: "0.1rem 0.6rem", fontSize: "0.82rem" }}>
                {f.fields.map((fl) => (
                  <div key={fl.key} style={{ display: "contents" }}>
                    <dt style={{ color: "var(--text-muted)", overflowWrap: "anywhere" }}>{fl.label}</dt>
                    <dd style={{ margin: 0, overflowWrap: "anywhere" }}>{fl.value}</dd>
                  </div>
                ))}
              </dl>
            </div>
          ))}
          {(data.count ?? 0) > feats.length && (
            <div className="text-sm text-muted">מוצגות {feats.length} מתוך {data.count}.</div>
          )}
        </div>
      )}
      {data.sources && data.sources.length > 0 && (
        <div className="text-muted" style={{ fontSize: "0.75rem", marginTop: "0.35rem", display: "flex", gap: "0.6rem", flexWrap: "wrap" }}>
          {data.sources.map((s) => (
            <span key={s.dataset_id}>
              <a href={s.row_url} target="_blank" rel="noopener noreferrer">שורות המקור ↗</a>
              {" · "}
              <a href={s.page_url} target="_blank" rel="noopener noreferrer">גרסאות המאגר ↗</a>
            </span>
          ))}
        </div>
      )}
    </div>
  );
}

// ── one cube ──────────────────────────────────────────────────────────────────
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

  const empty = info.hits === 0;
  return (
    <div style={{
      border: "1px solid var(--border)", borderRadius: 8, background: "var(--surface)",
      gridColumn: open ? "1 / -1" : undefined, opacity: empty && !open ? 0.7 : 1,
    }}>
      <button type="button" onClick={() => setOpen(!open)} aria-expanded={open}
        style={{
          width: "100%", textAlign: "start", background: "none", border: "none", cursor: "pointer",
          padding: "0.55rem 0.75rem", display: "flex", gap: "0.5rem", alignItems: "baseline",
        }}>
        <span style={{ fontWeight: 700, fontSize: "0.88rem", flex: 1 }}>{info.title}</span>
        {info.hits != null && (
          <span style={{
            fontSize: "0.72rem", padding: "0.05rem 0.45rem", borderRadius: 999,
            background: info.hits ? "var(--tint-good-bg)" : "var(--surface-2)",
            color: info.hits ? "var(--success)" : "var(--text-muted)",
          }}>
            <Ltr>{info.hits}</Ltr>
          </span>
        )}
        <span aria-hidden="true" className="text-muted">{open ? "▴" : "▾"}</span>
      </button>
      {!open && (
        <div className="text-muted" style={{ fontSize: "0.76rem", padding: "0 0.75rem 0.55rem", lineHeight: 1.5 }}>
          {info.description}
        </div>
      )}
      {open && (
        <div style={{ padding: "0 0.75rem 0.7rem" }}>
          <div className="text-muted" style={{ fontSize: "0.78rem", marginBottom: "0.4rem" }}>{info.description}</div>
          {error && <div className="text-sm" style={{ color: "var(--danger)" }}>{error}</div>}
          {!error && !data && <div className="text-sm text-muted">טוען…</div>}
          {data && !data.available && <div className="text-sm text-muted">השכבה אינה זמינה כרגע.</div>}
          {data?.available && (info.grain === "stat_area"
            ? <CrimeView data={data} onYear={setYear} />
            : <SpatialView data={data} />)}
        </div>
      )}
    </div>
  );
}

// ── the panel ─────────────────────────────────────────────────────────────────
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

  const byGroup = (list: NadlanCubeInfo[]) => {
    const groups: [string, NadlanCubeInfo[]][] = [];
    for (const c of list) {
      const g = groups.find(([name]) => name === c.group);
      if (g) g[1].push(c); else groups.push([c.group, [c]]);
    }
    return groups;
  };

  const govmapHref = `https://www.govmap.gov.il/?q=${encodeURIComponent(`גוש ${id.gush} חלקה ${id.helka}`)}`;

  return (
    <section aria-label="שכבות מידע על תא השטח" style={{ marginTop: "0.9rem" }}>
      <div className="flex" style={{ justifyContent: "space-between", alignItems: "baseline", gap: "0.5rem", flexWrap: "wrap" }}>
        <h4 style={{ margin: 0, fontSize: "0.95rem" }}>מה עוד ידוע על תא השטח</h4>
        <a href={govmapHref} target="_blank" rel="noopener noreferrer" style={{ fontSize: "0.8rem" }}>
          החלקה ב-GovMap ↗<span className="sr-only"> (נפתח בחלון חדש)</span>
        </a>
      </div>
      <div className="text-muted" style={{ fontSize: "0.78rem", margin: "0.2rem 0 0.5rem", lineHeight: 1.6 }}>
        כל קובייה נטענת רק כשפותחים אותה. המספר הוא כמה רשומות בשכבה נוגעות בחלקה
        (או נמצאות ברדיוס שצוין); שכבות ארציות מדווחות לכל הארץ, ושכבות עירוניות רק ליישוב שפרסם אותן.
      </div>
      {error && <div className="text-sm" style={{ color: "var(--danger)" }}>{error}</div>}
      {!error && !cubes && <div className="text-sm text-muted">טוען שכבות…</div>}
      {byGroup(withData).map(([group, list]) => (
        <div key={group} style={{ marginBottom: "0.6rem" }}>
          <div className="text-muted" style={{ fontSize: "0.78rem", fontWeight: 700, marginBottom: "0.25rem" }}>{group}</div>
          <div style={{ display: "grid", gap: "0.45rem", gridTemplateColumns: "repeat(auto-fill, minmax(220px, 1fr))" }}>
            {list.map((c) => <Cube key={c.id} info={c} property={property} />)}
          </div>
        </div>
      ))}
      {empty.length > 0 && (
        <div>
          <button type="button" onClick={() => setShowEmpty(!showEmpty)} aria-expanded={showEmpty}
            style={{ ...PILL, marginBottom: "0.4rem" }}>
            {showEmpty ? "הסתרת" : "הצגת"} {empty.length} שכבות ללא רשומות בנקודה
          </button>
          {showEmpty && (
            <div style={{ display: "grid", gap: "0.45rem", gridTemplateColumns: "repeat(auto-fill, minmax(220px, 1fr))" }}>
              {empty.map((c) => <Cube key={c.id} info={c} property={property} />)}
            </div>
          )}
        </div>
      )}
    </section>
  );
}
