/**
 * עסקאות נדל"ן — the מיסוי מקרקעין deal register, and what it disagrees with.
 *
 * Three tabs, one subject. The first is the register itself: 3.84 M reported
 * deals back to 1998, which the publisher only lets you read one גוש at a time,
 * served here as something you can filter, sort, page and chart. The second is
 * the written comparison of nadlan.gov.il against that register, and the third
 * turns the comparison into a game — both of them used to live on נדל"ן לעם,
 * where they were the only two tabs that were not a property lookup.
 *
 * The chart and the table are driven by the SAME filter object, deliberately:
 * a summary that could describe a different population than the rows beneath it
 * is worse than no summary.
 *
 * Everything lives in the query string (?tab=&settlement=&gush=&helka=&from=…)
 * so every view is a shareable link — the convention /data and נדל"ן לעם
 * already follow. A parcel link from a property page (?gush=&helka=) lands
 * straight on that parcel's history.
 */
import { lazy, Suspense, useCallback, useEffect, useMemo, useState } from "react";
import { Trans } from "react-i18next";
import { useSearchParams } from "react-router-dom";
import {
  deals as dealsApi, DealFilters, DealNature, DealSettlement, DealsSearchResult,
  DealsStats, DealYear, NadlanDeal,
} from "../api/client";

import { useDocumentTitle } from "../hooks/useDocumentTitle";
const NadlanGaps = lazy(() => import("../components/deals/NadlanGaps"));
const NadlanQuiz = lazy(() => import("../components/deals/NadlanQuiz"));

type Tab = "browse" | "gaps" | "quiz";
const TAB_IDS: Tab[] = ["browse", "gaps", "quiz"];
const TAB_LABELS: [Tab, string][] = [
  ["browse", "📊 מאגר העסקאות"],
  ["gaps", "🔍 פערים מול מיסוי מקרקעין"],
  ["quiz", "🏛 שניים אוחזין בעסקה"],
];

const SORTS: [string, string][] = [
  ["date_desc", "תאריך, מהחדש"],
  ["date_asc", "תאריך, מהישן"],
  ["amount_desc", "שווי, מהגבוה"],
  ["amount_asc", "שווי, מהנמוך"],
  ["area_desc", "שטח, מהגדול"],
];

const PAGE_SIZE = 50;

const NIS = new Intl.NumberFormat("he-IL", {
  style: "currency", currency: "ILS", maximumFractionDigits: 0,
});

/** RTL reorders a run of digits next to a sign or a separator, so anything read
 *  as one number goes through an LTR isolate. */
function Ltr({ children }: { children: React.ReactNode }) {
  return (
    <span style={{ unicodeBidi: "isolate", direction: "ltr", display: "inline-block" }}>
      {children}
    </span>
  );
}

function heDate(iso: string | null | undefined): string {
  if (!iso) return "—";
  const [y, m, d] = iso.split("-");
  return d ? `${d}/${m}/${y}` : iso;
}

export default function DealsPage() {
  useDocumentTitle("עסקאות נדל\"ן");
  const [params, setParams] = useSearchParams();

  const urlTab = params.get("tab") as Tab | null;
  const tab: Tab = urlTab && TAB_IDS.includes(urlTab) ? urlTab : "browse";

  const [stats, setStats] = useState<DealsStats | null>(null);
  const [settlements, setSettlements] = useState<DealSettlement[]>([]);
  const [natures, setNatures] = useState<DealNature[]>([]);
  const [result, setResult] = useState<DealsSearchResult | null>(null);
  const [series, setSeries] = useState<DealYear[] | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const patch = useCallback((next: Record<string, string | null>) => {
    setParams((prev) => {
      const p = new URLSearchParams(prev);
      for (const [k, v] of Object.entries(next)) {
        if (v == null || v === "") p.delete(k);
        else p.set(k, v);
      }
      // Any change to the filter puts you back on the first page: staying on
      // page 12 of a result set that no longer has one is never what was meant.
      if (!("offset" in next)) p.delete("offset");
      return p;
    }, { replace: true });
  }, [setParams]);

  // ── the filter, read straight out of the URL ──────────────────────────────
  const filters: DealFilters = useMemo(() => {
    const f: DealFilters = {};
    const settlement = params.get("settlement");
    const gush = params.get("gush");
    const helka = params.get("helka");
    const nature = params.get("nature");
    const from = params.get("from");
    const to = params.get("to");
    const minAmount = params.get("min");
    const maxAmount = params.get("max");
    const minRooms = params.get("rooms");
    if (settlement) f.settlement = settlement;
    if (gush) f.gush = gush;
    if (helka) f.helka = helka;
    if (nature) f.nature = nature;
    if (from) f.date_from = from;
    if (to) f.date_to = to;
    if (minAmount) f.min_amount = Number(minAmount);
    if (maxAmount) f.max_amount = Number(maxAmount);
    if (minRooms) f.min_rooms = Number(minRooms);
    return f;
  }, [params]);

  const offset = Number(params.get("offset") ?? 0);
  const sort = params.get("sort") ?? "date_desc";
  const filterKey = JSON.stringify(filters);

  useEffect(() => {
    if (tab !== "browse") return;
    dealsApi.stats().then(setStats).catch(() => setStats(null));
    dealsApi.settlements().then((r) => setSettlements(r.data)).catch(() => setSettlements([]));
    dealsApi.natures().then((r) => setNatures(r.data)).catch(() => setNatures([]));
  }, [tab]);

  useEffect(() => {
    if (tab !== "browse") return;
    let cancelled = false;
    setLoading(true);
    setError(null);
    // The listing and the yearly series go out together and under the same
    // filter — that is what keeps the chart honest about the table.
    Promise.all([
      dealsApi.search(filters, PAGE_SIZE, offset, sort),
      dealsApi.series(filters).then((r) => r.data).catch(() => null),
    ])
      .then(([res, ser]) => {
        if (cancelled) return;
        setResult(res);
        setSeries(ser);
      })
      .catch((e) => {
        if (!cancelled) {
          setResult(null);
          setError(e instanceof Error ? e.message : "החיפוש נכשל");
        }
      })
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [tab, filterKey, offset, sort]);

  const rows: NadlanDeal[] = result?.data ?? [];
  const hasFilter = Object.keys(filters).length > 0;

  return (
    <div>
      {/* Not the "processed" banner נדל"ן לעם shows: nothing here is derived.
          These are one publisher's rows, served as they were published. */}
      <div className="processed-banner" role="note">
        <div className="container">
          <span className="processed-banner-badge">מקור ראשוני</span>
          <span className="processed-banner-text">
            <Trans
              i18nKey="projects.deals_note"
              defaults="הנתונים כאן הם <strong>שורות המקור של רשות המסים</strong>, כפי שפורסמו. לא חושבו, לא תוקנו ולא הושלמו — רק נאספו למקום אחד שאפשר לתשאל."
              components={{ strong: <strong /> }}
            />
          </span>
        </div>
      </div>

      <div className="container mt-3">
        <div className="page-header" style={{ marginBottom: "0.75rem" }}>
          <h1 style={{ margin: 0 }}>עסקאות נדל"ן</h1>
          <div className="text-sm text-muted" style={{ marginTop: "0.35rem", lineHeight: 1.7 }}>
            מאגר עסקאות המקרקעין המדווחות לרשות המסים, שהמפרסם מאפשר לקרוא גוש אחד בכל פעם,
            כאן כמאגר אחד שאפשר לסנן, למיין ולהשוות. שתי הלשוניות האחרונות עוסקות בפער שבין
            המאגר הזה לבין אתר הנדל״ן הממשלתי: האחת מסבירה אותו, והשנייה הופכת אותו למשחק.
            {stats && (
              <div style={{ marginTop: "0.4rem" }}>
                <Ltr>{stats.deals.toLocaleString("he-IL")}</Ltr> עסקאות ·{" "}
                <Ltr>{heDate(stats.first_deal)}</Ltr> עד <Ltr>{heDate(stats.last_deal)}</Ltr> ·{" "}
                <Ltr>{stats.settlements.toLocaleString("he-IL")}</Ltr> יישובים ·{" "}
                <Ltr>{stats.parcels.toLocaleString("he-IL")}</Ltr> חלקות ·{" "}
                <a href={`/versions/${stats.dataset_id}`}>היסטוריית הגרסאות של המאגר</a>
              </div>
            )}
          </div>
        </div>

        {/* Tabs */}
        <div className="flex" style={{ gap: "0.3rem", borderBottom: "2px solid var(--border)", marginBottom: "1rem", flexWrap: "wrap" }}>
          {TAB_LABELS.map(([id, label]) => (
            <button
              key={id}
              type="button"
              onClick={() => patch({ tab: id === "browse" ? null : id })}
              style={{
                padding: "0.5rem 1.05rem", border: "none", cursor: "pointer", background: "none",
                fontSize: "0.95rem", fontWeight: tab === id ? 700 : 500,
                color: tab === id ? "var(--primary)" : "var(--text-muted)",
                borderBottom: tab === id ? "3px solid var(--primary)" : "3px solid transparent",
                marginBottom: -2,
              }}
            >
              {label}
            </button>
          ))}
        </div>

        {tab === "browse" && (
          <>
            {/* ── the filter ── */}
            <form
              className="flex"
              style={{ gap: "0.5rem", flexWrap: "wrap", marginBottom: "0.8rem", alignItems: "flex-end" }}
              onSubmit={(e) => e.preventDefault()}
            >
              <label className="text-sm">
                יישוב
                <br />
                <select
                  value={filters.settlement ?? ""}
                  onChange={(e) => patch({ settlement: e.target.value || null })}
                  style={{ padding: "0.35rem 0.5rem", minWidth: 190, maxWidth: 260 }}
                >
                  <option value="">כל היישובים</option>
                  {settlements.map((s) => (
                    <option key={s.settlement} value={s.settlement}>
                      {s.settlement} ({s.deals.toLocaleString("he-IL")})
                    </option>
                  ))}
                </select>
              </label>

              <label className="text-sm">
                מהות
                <br />
                <select
                  value={filters.nature ?? ""}
                  onChange={(e) => patch({ nature: e.target.value || null })}
                  style={{ padding: "0.35rem 0.5rem", minWidth: 180, maxWidth: 240 }}
                >
                  <option value="">כל המהויות</option>
                  {natures.map((n) => (
                    <option key={n.nature ?? ""} value={n.nature ?? ""}>
                      {n.nature} ({n.deals.toLocaleString("he-IL")})
                    </option>
                  ))}
                </select>
              </label>

              <label className="text-sm">
                גוש
                <br />
                <input
                  defaultValue={String(filters.gush ?? "")}
                  onBlur={(e) => patch({ gush: e.target.value.trim() })}
                  inputMode="numeric"
                  style={{ padding: "0.35rem 0.5rem", width: 100 }}
                />
              </label>

              <label className="text-sm">
                חלקה
                <br />
                <input
                  defaultValue={String(filters.helka ?? "")}
                  onBlur={(e) => patch({ helka: e.target.value.trim() })}
                  inputMode="numeric"
                  style={{ padding: "0.35rem 0.5rem", width: 100 }}
                />
              </label>

              <label className="text-sm">
                מתאריך
                <br />
                <input
                  type="date"
                  defaultValue={filters.date_from ?? ""}
                  onChange={(e) => patch({ from: e.target.value })}
                  style={{ padding: "0.3rem 0.4rem" }}
                />
              </label>

              <label className="text-sm">
                עד תאריך
                <br />
                <input
                  type="date"
                  defaultValue={filters.date_to ?? ""}
                  onChange={(e) => patch({ to: e.target.value })}
                  style={{ padding: "0.3rem 0.4rem" }}
                />
              </label>

              <label className="text-sm">
                שווי מינימלי
                <br />
                <input
                  defaultValue={filters.min_amount ? String(filters.min_amount) : ""}
                  onBlur={(e) => patch({ min: e.target.value.replace(/\D/g, "") })}
                  inputMode="numeric"
                  placeholder="₪"
                  style={{ padding: "0.35rem 0.5rem", width: 120 }}
                />
              </label>

              <label className="text-sm">
                מיון
                <br />
                <select
                  value={sort}
                  onChange={(e) => patch({ sort: e.target.value === "date_desc" ? null : e.target.value })}
                  style={{ padding: "0.35rem 0.5rem" }}
                >
                  {SORTS.map(([id, label]) => <option key={id} value={id}>{label}</option>)}
                </select>
              </label>

              {hasFilter && (
                <button
                  type="button"
                  onClick={() => setParams(new URLSearchParams(), { replace: true })}
                  style={{
                    padding: "0.35rem 0.8rem", fontSize: "0.85rem", cursor: "pointer",
                    border: "1px solid var(--border)", borderRadius: 6, background: "none",
                  }}
                >
                  ניקוי הסינון
                </button>
              )}
            </form>

            {/* ── the shape of what is selected, above the rows it describes ── */}
            {series && series.length > 1 && <YearChart series={series} />}

            {/* ── results ── */}
            {loading && <div className="text-sm text-muted">מחפש…</div>}
            {error && <div className="text-sm" style={{ color: "var(--danger)" }}>{error}</div>}

            {result && (
              <div className="text-sm text-muted" style={{ margin: "0.6rem 0 0.4rem" }}>
                {result.total === 0
                  ? "לא נמצאו עסקאות לסינון הזה."
                  : <>
                      <Ltr>{result.total.toLocaleString("he-IL")}</Ltr>
                      {result.total_capped ? "+" : ""} עסקאות ·{" "}
                      מוצגות <Ltr>{offset + 1}</Ltr>–<Ltr>{offset + rows.length}</Ltr> ·{" "}
                      <a href={result.row_url} target="_blank" rel="noopener noreferrer"
                         title={result.console_sql}>
                        אותה שאילתה ב-/data ↗
                        <span className="sr-only"> (נפתח בחלון חדש)</span>
                      </a>
                    </>}
              </div>
            )}

            {rows.length > 0 && (
              <div tabIndex={0} role="region" aria-label="עסקאות נדל״ן" className="scroll-region" style={{ overflowX: "auto" }}>
                <table style={{ width: "100%", fontSize: "0.86rem", borderCollapse: "collapse" }}>
                  <thead>
                    <tr style={{ textAlign: "start", color: "var(--text-muted)" }}>
                      {["תאריך", "יישוב", "גוש־חלקה", "תת-חלקה", "שווי", "מהות", "חדרים",
                        "שטח (מ״ר)", "שנת בנייה", "חלק"].map((h) => (
                        <th key={h} scope="col" style={{ textAlign: "start", padding: "0.3rem 0.45rem", whiteSpace: "nowrap" }}>
                          {h}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {rows.map((d, i) => (
                      <tr key={i} style={{ borderTop: "1px solid var(--border)" }}>
                        <td style={{ padding: "0.3rem 0.45rem", whiteSpace: "nowrap" }}><Ltr>{heDate(d.date)}</Ltr></td>
                        <td style={{ padding: "0.3rem 0.45rem" }}>{d.settlement ?? "—"}</td>
                        <td style={{ padding: "0.3rem 0.45rem", whiteSpace: "nowrap" }}>
                          {d.gush && d.helka ? (
                            <a href={`/projects/nadlan?tab=gush&g=${d.gush}&h=${d.helka}`}
                               title="לעמוד הנכס בנדל״ן לעם">
                              <Ltr>{d.gush}־{d.helka}</Ltr>
                            </a>
                          ) : "—"}
                        </td>
                        <td style={{ padding: "0.3rem 0.45rem" }}><Ltr>{d.sub_parcel ?? "—"}</Ltr></td>
                        <td style={{ padding: "0.3rem 0.45rem", whiteSpace: "nowrap" }}>
                          <Ltr>{d.amount != null ? NIS.format(d.amount) : "—"}</Ltr>
                        </td>
                        <td style={{ padding: "0.3rem 0.45rem" }}>{d.nature ?? "—"}</td>
                        <td style={{ padding: "0.3rem 0.45rem" }}><Ltr>{d.rooms || "—"}</Ltr></td>
                        <td style={{ padding: "0.3rem 0.45rem" }}><Ltr>{d.area_sqm || "—"}</Ltr></td>
                        <td style={{ padding: "0.3rem 0.45rem" }}><Ltr>{d.year_built || "—"}</Ltr></td>
                        <td style={{ padding: "0.3rem 0.45rem" }}><Ltr>{d.portion ?? "—"}</Ltr></td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}

            {result && result.total > PAGE_SIZE && (
              <div className="flex" style={{ gap: "0.5rem", marginTop: "0.7rem", alignItems: "center" }}>
                <button
                  type="button"
                  disabled={offset === 0}
                  onClick={() => patch({ offset: String(Math.max(0, offset - PAGE_SIZE)) })}
                  style={{ padding: "0.3rem 0.8rem", fontSize: "0.85rem", cursor: offset === 0 ? "default" : "pointer", border: "1px solid var(--border)", borderRadius: 6, background: "none" }}
                >
                  הקודם
                </button>
                <button
                  type="button"
                  disabled={offset + rows.length >= result.total && !result.total_capped}
                  onClick={() => patch({ offset: String(offset + PAGE_SIZE) })}
                  style={{ padding: "0.3rem 0.8rem", fontSize: "0.85rem", cursor: "pointer", border: "1px solid var(--border)", borderRadius: 6, background: "none" }}
                >
                  הבא
                </button>
              </div>
            )}

            {/* ── what the register does and does not record ── */}
            <div style={{
              marginTop: "1.5rem", padding: "0.8rem 1rem", borderRadius: 8,
              background: "var(--surface-2)", border: "1px solid var(--border)",
            }}>
              <div style={{ fontWeight: 700, fontSize: "0.9rem", marginBottom: "0.35rem" }}>
                מה המאגר הזה כן ולא אומר
              </div>
              <ul style={{ margin: 0, paddingInlineStart: "1.1rem", fontSize: "0.85rem", lineHeight: 1.8 }}>
                {(result?.caveats ?? []).map((c, i) => <li key={i}>{c}</li>)}
                <li>
                  הפער בין המאגר הזה לבין אתר הנדל״ן הממשלתי מתועד בלשונית{" "}
                  <button
                    type="button"
                    onClick={() => patch({ tab: "gaps" })}
                    style={{
                      background: "none", border: "none", padding: 0, font: "inherit",
                      color: "var(--primary)", textDecoration: "underline", cursor: "pointer",
                    }}
                  >
                    פערים מול מיסוי מקרקעין
                  </button>.
                </li>
              </ul>
            </div>

            <div className="text-sm text-muted" style={{ margin: "1rem 0 0.5rem" }}>
              זיהוי הנכס עצמו — כתובת, מיקוד, אזור סטטיסטי וגבולות החלקה — נמצא ב
              <a href="/projects/nadlan">נדל״ן לעם</a>, ושם גם מוצגות העסקאות של כל נכס.
              {stats && <> המאגר נשמר בטבלה <code>{stats.table}</code> וניתן לתשאול ישיר ב<a href="/data">/data</a>.</>}
            </div>
          </>
        )}

        {tab === "gaps" && (
          <Suspense fallback={<div className="text-sm text-muted">טוען את הדוח…</div>}>
            <NadlanGaps />
          </Suspense>
        )}

        {tab === "quiz" && (
          <Suspense fallback={<div className="text-sm text-muted">מסדר את השאלות…</div>}>
            <NadlanQuiz onReadReport={() => patch({ tab: "gaps" })} />
          </Suspense>
        )}
      </div>
    </div>
  );
}

/**
 * Deals and the median reported price per year, under the filter that produced
 * the table below it. Plain bars over an SVG rather than a chart library: two
 * series, no interaction beyond a tooltip, and the page already pays for
 * Leaflet next door.
 *
 * Median, not mean — the register mixes one flat with the sale of a whole
 * building, and a single such row moves an average by millions.
 */
function YearChart({ series }: { series: DealYear[] }) {
  const maxDeals = Math.max(...series.map((d) => d.deals), 1);
  const maxMedian = Math.max(...series.map((d) => d.median_amount ?? 0), 1);
  const W = 100 / series.length;

  return (
    <div style={{
      border: "1px solid var(--border)", borderRadius: 8, padding: "0.7rem 0.9rem",
      background: "var(--surface)", marginBottom: "0.8rem",
    }}>
      <div style={{ fontWeight: 700, fontSize: "0.9rem", marginBottom: "0.1rem" }}>
        עסקאות וחציון השווי לפי שנה
      </div>
      <div className="text-muted" style={{ fontSize: "0.78rem", marginBottom: "0.5rem" }}>
        העמודות הן מספר העסקאות; הקו הוא חציון השווי המדווח. שניהם מחושבים על אותו
        סינון בדיוק כמו הטבלה שמתחת.
      </div>
      <svg viewBox="0 0 100 34" preserveAspectRatio="none" role="img"
           aria-label="עסקאות וחציון שווי לפי שנה"
           style={{ width: "100%", height: 150, direction: "ltr" }}>
        {series.map((d, i) => (
          <rect
            key={d.year}
            x={i * W + W * 0.15} width={W * 0.7}
            y={30 - (d.deals / maxDeals) * 28} height={(d.deals / maxDeals) * 28}
            fill="var(--primary)" opacity={0.55}
          >
            <title>
              {d.year}: {d.deals.toLocaleString("he-IL")} עסקאות
              {d.median_amount ? `, חציון ${NIS.format(d.median_amount)}` : ""}
            </title>
          </rect>
        ))}
        <polyline
          fill="none" stroke="var(--danger)" strokeWidth={0.6}
          points={series.map((d, i) =>
            `${i * W + W / 2},${30 - ((d.median_amount ?? 0) / maxMedian) * 28}`).join(" ")}
        />
      </svg>
      <div className="flex" style={{ justifyContent: "space-between", fontSize: "0.72rem", color: "var(--text-muted)", direction: "ltr" }}>
        <span>{series[0].year}</span>
        <span>{series[series.length - 1].year}</span>
      </div>
    </div>
  );
}
