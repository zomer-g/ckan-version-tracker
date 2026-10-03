/**
 * עסקאות GovMap — the deals GovMap draws on its map (layer 16), searched and
 * shown on a map.
 *
 * GovMap puts every deal of a building on that building's point, so the map
 * draws one circle per point, sized by how many deals it carries, rather than
 * hundreds of markers on one spot. Clicking a point narrows the table to its
 * parcel. No building outlines: GovMap serves them only as map images, and the
 * project draws from data, not from pictures of it.
 *
 * Unfiltered, the map asks for points only once it is zoomed in far enough to
 * hold a neighbourhood — 2.5M deals at country scale would be noise, and the
 * API refuses it.
 */
import { useCallback, useEffect, useMemo, useState } from "react";
import { CircleMarker, MapContainer, Popup, TileLayer, useMap, useMapEvents } from "react-leaflet";
import "leaflet/dist/leaflet.css";
import {
  govmapDeals as api, GovmapDeal, GovmapDealFilters, GovmapPoint, GovmapStats,
} from "../../api/client";
import SearchableSelect, { SearchableOption } from "../SearchableSelect";

const PAGE_SIZE = 50;
const MIN_ZOOM_UNFILTERED = 15;
const ISRAEL: [number, number] = [31.6, 34.95];

const NIS = new Intl.NumberFormat("he-IL", { style: "currency", currency: "ILS", maximumFractionDigits: 0 });

function Ltr({ children }: { children: React.ReactNode }) {
  return <bdi dir="ltr" style={{ unicodeBidi: "isolate" }}>{children}</bdi>;
}

function heDate(iso: string | null | undefined): string {
  if (!iso || iso.length < 10) return "—";
  return `${iso.slice(8, 10)}/${iso.slice(5, 7)}/${iso.slice(0, 4)}`;
}

function address(d: { street: string | null; house_num: string | null }): string {
  if (!d.street) return "—";
  return d.house_num ? `${d.street} ${d.house_num}` : d.street;
}

type Draft = {
  settlement: string; street: string; house: string; gush: string; helka: string;
  from: string; to: string; min: string; sort: string;
};
const EMPTY: Draft = { settlement: "", street: "", house: "", gush: "", helka: "", from: "", to: "", min: "", sort: "date_desc" };

function filtersOf(d: Draft): GovmapDealFilters {
  const f: GovmapDealFilters = {};
  if (d.settlement) f.settlement = d.settlement;
  if (d.street.trim()) f.street = d.street.trim();
  if (d.house.trim() && d.street.trim()) f.house = d.house.trim();
  if (/^\d+$/.test(d.gush)) f.gush = d.gush;
  if (/^\d+$/.test(d.helka) && f.gush) f.helka = d.helka;
  if (d.from) f.date_from = d.from;
  if (d.to) f.date_to = d.to;
  if (/^\d+$/.test(d.min)) f.min_amount = d.min;
  return f;
}

/** Fits the map to the points of a filtered search. */
function FitTo({ points }: { points: GovmapPoint[] }) {
  const map = useMap();
  useEffect(() => {
    if (!points.length) return;
    const lats = points.map((p) => p.lat), lons = points.map((p) => p.lon);
    map.fitBounds([[Math.min(...lats), Math.min(...lons)], [Math.max(...lats), Math.max(...lons)]],
      { padding: [30, 30], maxZoom: 17 });
  }, [points, map]);
  return null;
}

/** Reports the view after every pan/zoom, so an unfiltered map can ask for
 *  the points inside it. */
function ViewWatcher({ onView }: { onView: (bbox: string, zoom: number) => void }) {
  const map = useMapEvents({
    moveend: () => {
      const b = map.getBounds();
      onView(`${b.getWest()},${b.getSouth()},${b.getEast()},${b.getNorth()}`, map.getZoom());
    },
  });
  return null;
}

export default function GovmapDeals() {
  const [stats, setStats] = useState<GovmapStats | null>(null);
  const [notReady, setNotReady] = useState<string | null>(null);
  const [settlements, setSettlements] = useState<SearchableOption[]>([]);
  const [draft, setDraft] = useState<Draft>(EMPTY);
  const [applied, setApplied] = useState<Draft>(EMPTY);
  const [offset, setOffset] = useState(0);
  const [rows, setRows] = useState<GovmapDeal[]>([]);
  const [total, setTotal] = useState<{ n: number; capped: boolean } | null>(null);
  const [points, setPoints] = useState<GovmapPoint[]>([]);
  const [pointsNote, setPointsNote] = useState<string | null>(null);
  const [fitPoints, setFitPoints] = useState<GovmapPoint[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const filters = useMemo(() => filtersOf(applied), [applied]);
  const hasFilter = Object.keys(filters).length > 0;

  useEffect(() => {
    api.stats().then(setStats).catch((e) => setNotReady(e instanceof Error ? e.message : "לא זמין"));
    api.settlements()
      .then((r) => setSettlements(r.data.map((s) => ({
        value: s.settlement, label: s.settlement, hint: s.deals.toLocaleString("he-IL"),
      }))))
      .catch(() => setSettlements([]));
  }, []);

  // The table: the filter (or nothing), paged.
  useEffect(() => {
    if (notReady) return;
    let cancelled = false;
    setLoading(true);
    setError(null);
    api.search(filters, PAGE_SIZE, offset, applied.sort)
      .then((r) => {
        if (cancelled) return;
        setRows(r.data);
        setTotal({ n: r.total, capped: r.total_capped });
      })
      .catch((e) => { if (!cancelled) setError(e instanceof Error ? e.message : "החיפוש נכשל"); })
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [filters, offset, applied.sort, notReady]);

  // The map, when there is a filter: its points, and fit to them.
  useEffect(() => {
    if (notReady || !hasFilter) return;
    let cancelled = false;
    api.points(filters)
      .then((r) => {
        if (cancelled) return;
        setPoints(r.data);
        setFitPoints(r.data);
        setPointsNote(r.capped ? `מוצגות ${r.count.toLocaleString("he-IL")} הנקודות הגדולות; צמצמו את הסינון לראות את כולן.` : null);
      })
      .catch(() => { if (!cancelled) setPoints([]); });
    return () => { cancelled = true; };
  }, [filters, hasFilter, notReady]);

  // Unfiltered: points of the view, once zoomed in.
  const onView = useCallback((bbox: string, zoom: number) => {
    if (hasFilter || notReady) return;
    if (zoom < MIN_ZOOM_UNFILTERED) {
      setPoints([]);
      setPointsNote("התקרבו למפה (או בחרו יישוב) כדי לראות את נקודות העסקאות.");
      return;
    }
    api.points({}, bbox)
      .then((r) => {
        setPoints(r.data);
        setPointsNote(r.capped ? "יש באזור הזה יותר נקודות ממה שמוצג; התקרבו." : null);
      })
      .catch(() => setPoints([]));
  }, [hasFilter, notReady]);

  const run = (next?: Partial<Draft>) => {
    const d = { ...draft, ...next };
    setDraft(d);
    setApplied(d);
    setOffset(0);
  };

  const maxDeals = Math.max(1, ...points.map((p) => p.deals));

  if (notReady) {
    return (
      <div className="text-sm" style={{ padding: "1rem 0" }}>
        עסקאות GovMap עדיין נטענות למסד הנתונים. נסו שוב בעוד כמה דקות.
      </div>
    );
  }

  return (
    <div>
      <p className="text-sm text-muted" style={{ marginTop: 0 }}>
        עסקאות הנדל״ן כפי ש־GovMap מפרסם אותן בשכבת העסקאות שלו (שכבה 16) — אותן עסקאות שמציג אתר
        הנדל״ן הממשלתי, בלי עיבוד. כל העסקאות של בניין מסומנות במפה על נקודה אחת.
        {stats && (
          <>
            {" "}<Ltr>{stats.deals.toLocaleString("he-IL")}</Ltr> עסקאות ·{" "}
            <Ltr>{stats.points.toLocaleString("he-IL")}</Ltr> נקודות ·{" "}
            <Ltr>{stats.settlements.toLocaleString("he-IL")}</Ltr> יישובים · גרסה {stats.version} ·{" "}
            <a href={`/versions/${stats.dataset_id}`}>היסטוריית הגרסאות</a>
          </>
        )}
      </p>

      <form
        className="flex"
        style={{ gap: "0.5rem", flexWrap: "wrap", marginBottom: "0.8rem", alignItems: "flex-end" }}
        onSubmit={(e) => { e.preventDefault(); run(); }}
      >
        <label className="text-sm">יישוב<br />
          <SearchableSelect ariaLabel="יישוב" value={draft.settlement} allLabel="כל היישובים"
            options={settlements} style={{ width: 210 }}
            onChange={(v) => setDraft({ ...draft, settlement: v || "", street: "", house: "" })} />
        </label>
        <label className="text-sm">רחוב<br />
          <input value={draft.street} onChange={(e) => setDraft({ ...draft, street: e.target.value })}
            placeholder={draft.settlement ? "שם הרחוב" : "בחרו יישוב קודם"} disabled={!draft.settlement}
            style={{ padding: "0.35rem 0.5rem", width: 140 }} />
        </label>
        <label className="text-sm">מס׳ בית<br />
          <input value={draft.house} onChange={(e) => setDraft({ ...draft, house: e.target.value })}
            style={{ padding: "0.35rem 0.5rem", width: 70 }} />
        </label>
        <label className="text-sm">גוש<br />
          <input value={draft.gush} inputMode="numeric" onChange={(e) => setDraft({ ...draft, gush: e.target.value })}
            style={{ padding: "0.35rem 0.5rem", width: 90 }} />
        </label>
        <label className="text-sm">חלקה<br />
          <input value={draft.helka} inputMode="numeric" onChange={(e) => setDraft({ ...draft, helka: e.target.value })}
            style={{ padding: "0.35rem 0.5rem", width: 80 }} />
        </label>
        <label className="text-sm">מתאריך<br />
          <input type="date" value={draft.from} onChange={(e) => setDraft({ ...draft, from: e.target.value })}
            style={{ padding: "0.3rem 0.4rem" }} />
        </label>
        <label className="text-sm">עד תאריך<br />
          <input type="date" value={draft.to} onChange={(e) => setDraft({ ...draft, to: e.target.value })}
            style={{ padding: "0.3rem 0.4rem" }} />
        </label>
        <label className="text-sm">שווי מינימלי<br />
          <input value={draft.min} inputMode="numeric" placeholder="₪" onChange={(e) => setDraft({ ...draft, min: e.target.value })}
            style={{ padding: "0.35rem 0.5rem", width: 110 }} />
        </label>
        <label className="text-sm">מיון<br />
          <select value={draft.sort} onChange={(e) => setDraft({ ...draft, sort: e.target.value })}
            style={{ padding: "0.35rem 0.5rem" }}>
            <option value="date_desc">תאריך, מהחדש</option>
            <option value="date_asc">תאריך, מהישן</option>
            <option value="amount_desc">שווי, מהגבוה</option>
            <option value="amount_asc">שווי, מהנמוך</option>
          </select>
        </label>
        <button type="submit" className="btn btn-primary"
          style={{ padding: "0.4rem 1.3rem", fontSize: "0.95rem", fontWeight: 700, cursor: "pointer" }}>
          🔍 חיפוש
        </button>
        {hasFilter && (
          <button type="button" onClick={() => { setDraft(EMPTY); setApplied(EMPTY); setOffset(0); setPoints([]); }}
            style={{ padding: "0.35rem 0.8rem", fontSize: "0.85rem", cursor: "pointer", border: "1px solid var(--border)", borderRadius: 6, background: "none" }}>
            ניקוי הסינון
          </button>
        )}
      </form>

      {/* ── the map ── */}
      <div style={{ height: 420, border: "1px solid var(--border)", borderRadius: 8, overflow: "hidden", marginBottom: "0.4rem" }}>
        <MapContainer center={ISRAEL} zoom={8} style={{ height: "100%", width: "100%" }} preferCanvas>
          <TileLayer url="https://tile.openstreetmap.org/{z}/{x}/{y}.png"
            attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>' />
          <ViewWatcher onView={onView} />
          {hasFilter && <FitTo points={fitPoints} />}
          {points.map((p) => (
            <CircleMarker key={`${p.lon},${p.lat}`} center={[p.lat, p.lon]}
              radius={4 + 10 * Math.sqrt(p.deals / maxDeals)}
              pathOptions={{ color: "#2b5d8a", fillColor: "#3b82c4", fillOpacity: 0.55, weight: 1 }}>
              <Popup>
                <div dir="rtl" style={{ fontSize: "0.82rem" }}>
                  <div><b>{p.settlement ?? "ללא יישוב"}</b> · {address(p)}</div>
                  <div>גוש {p.gush ?? "—"} חלקה {p.parcel ?? "—"}</div>
                  <div><Ltr>{p.deals.toLocaleString("he-IL")}</Ltr> עסקאות · אחרונה {heDate(p.last_deal)}</div>
                  {p.gush != null && p.parcel != null && (
                    <button type="button" style={{ marginTop: 4, cursor: "pointer" }}
                      onClick={() => run({ ...EMPTY, gush: String(p.gush), helka: String(p.parcel), sort: draft.sort })}>
                      הצג את העסקאות בחלקה
                    </button>
                  )}
                </div>
              </Popup>
            </CircleMarker>
          ))}
        </MapContainer>
      </div>
      {pointsNote && <div className="text-sm text-muted" style={{ marginBottom: "0.5rem" }}>{pointsNote}</div>}

      {/* ── the table ── */}
      {loading && <div className="text-sm text-muted">מחפש…</div>}
      {error && <div className="text-sm" style={{ color: "var(--danger)" }}>{error}</div>}
      {total && (
        <div className="text-sm text-muted" style={{ margin: "0.6rem 0 0.4rem" }}>
          {total.n === 0 ? "לא נמצאו עסקאות לסינון הזה." : (
            <><Ltr>{total.n.toLocaleString("he-IL")}</Ltr>{total.capped ? "+" : ""} עסקאות · מוצגות{" "}
              <Ltr>{offset + 1}</Ltr>–<Ltr>{offset + rows.length}</Ltr></>
          )}
        </div>
      )}
      {rows.length > 0 && (
        <div tabIndex={0} role="region" aria-label="עסקאות GovMap" className="scroll-region" style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", fontSize: "0.86rem", borderCollapse: "collapse" }}>
            <thead>
              <tr style={{ color: "var(--text-muted)" }}>
                {["תאריך", "יישוב", "כתובת", "גוש־חלקה", "תת-חלקה", "שווי", "סוג נכס", "מהות", "חדרים", "שטח (מ״ר)", "קומה"].map((h) => (
                  <th key={h} scope="col" style={{ textAlign: "start", padding: "0.3rem 0.45rem", whiteSpace: "nowrap" }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((d) => (
                <tr key={d.objectid} style={{ borderTop: "1px solid var(--border)" }}>
                  <td style={{ padding: "0.3rem 0.45rem", whiteSpace: "nowrap" }}><Ltr>{heDate(d.deal_date)}</Ltr></td>
                  <td style={{ padding: "0.3rem 0.45rem" }}>{d.settlement ?? "—"}</td>
                  <td style={{ padding: "0.3rem 0.45rem" }}>{address(d)}</td>
                  <td style={{ padding: "0.3rem 0.45rem", whiteSpace: "nowrap" }}>
                    {d.gush != null && d.parcel != null ? <Ltr>{d.gush}-{d.parcel}</Ltr> : "—"}
                  </td>
                  <td style={{ padding: "0.3rem 0.45rem" }}><Ltr>{d.sub_parcel ?? "—"}</Ltr></td>
                  <td style={{ padding: "0.3rem 0.45rem", whiteSpace: "nowrap" }}>
                    <Ltr>{d.deal_amount != null ? NIS.format(d.deal_amount) : "—"}</Ltr>
                  </td>
                  <td style={{ padding: "0.3rem 0.45rem" }}>{d.property_type ?? "—"}</td>
                  <td style={{ padding: "0.3rem 0.45rem" }}>{d.deal_nature ?? "—"}</td>
                  <td style={{ padding: "0.3rem 0.45rem" }}><Ltr>{d.rooms ?? "—"}</Ltr></td>
                  <td style={{ padding: "0.3rem 0.45rem" }}><Ltr>{d.asset_area ?? "—"}</Ltr></td>
                  <td style={{ padding: "0.3rem 0.45rem" }}>{d.floor ?? "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      {total && total.n > PAGE_SIZE && (
        <div className="flex" style={{ gap: "0.5rem", marginTop: "0.7rem" }}>
          <button type="button" disabled={offset === 0} onClick={() => setOffset(Math.max(0, offset - PAGE_SIZE))}
            style={{ padding: "0.3rem 0.8rem", fontSize: "0.85rem", cursor: "pointer", border: "1px solid var(--border)", borderRadius: 6, background: "none" }}>
            הקודם
          </button>
          <button type="button" disabled={offset + rows.length >= total.n && !total.capped}
            onClick={() => setOffset(offset + PAGE_SIZE)}
            style={{ padding: "0.3rem 0.8rem", fontSize: "0.85rem", cursor: "pointer", border: "1px solid var(--border)", borderRadius: 6, background: "none" }}>
            הבא
          </button>
        </div>
      )}

      {stats && (
        <ul className="text-sm text-muted" style={{ marginTop: "1rem", paddingInlineStart: "1.1rem" }}>
          {stats.caveats.map((c, i) => <li key={i} style={{ marginBottom: "0.25rem" }}>{c}</li>)}
          <li>המאגר נשמר בטבלה <code>{stats.table}</code> וניתן לתשאול ישיר ב<a href="/data">/data</a>.</li>
        </ul>
      )}
    </div>
  );
}
