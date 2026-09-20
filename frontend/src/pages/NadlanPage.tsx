/**
 * נדל"ן לעם — the property-level spatial crosswalk.
 *
 * Four ways into the same answer: a point on the map (with a radius), a postal
 * code, an address, or a gush/helka. Whichever you use, the result is the same
 * envelope from /api/nadlan, the property's identity in every other codespace
 * plus a link to each source's full row on /data.
 *
 * Every answer also carries the two layers that DESCRIBE the property rather
 * than identify it: its CBS statistical area (א"ס) and a summary of the מיסוי
 * מקרקעין deals reported on its גוש/חלקה. The deal REGISTER itself, and the
 * written comparison of it against nadlan.gov.il, are a project of their own at
 * /projects/deals — they were the only two tabs here that were not a lookup.
 *
 * Everything lives in the query string (?tab=&lat=&lon=&r=&g=&h=&zip=&city=…)
 * so every result is a shareable link, the convention the /data console and
 * GovmapView already follow. The map is lazy-imported: Leaflet is ~150 KB and
 * three of the four tabs never need it.
 */
import { lazy, Suspense, useCallback, useEffect, useMemo, useState } from "react";
import { Trans, useTranslation } from "react-i18next";
import { useNavigate, useSearchParams } from "react-router-dom";
import type { GeoJsonObject } from "geojson";
import { nadlan, NadlanEnvelope, NadlanProperty, NadlanStats } from "../api/client";
import NadlanResultCard from "../components/nadlan/NadlanResultCard";

import { useDocumentTitle } from "../hooks/useDocumentTitle";
const NadlanMap = lazy(() => import("../components/nadlan/NadlanMap"));

type Tab = "map" | "address" | "zip" | "gush";
const TAB_IDS: Tab[] = ["map", "address", "zip", "gush"];
const TAB_LABELS: [Tab, string][] = [
  ["map", "🗺 לפי מפה"],
  ["address", "🏠 לפי כתובת"],
  ["zip", "✉️ לפי מיקוד"],
  ["gush", "📐 לפי גוש־חלקה"],
];

const RADII = [0, 100, 250, 500, 1000, 2000];

// 16px or larger, or iOS Safari zooms the whole page in when the field takes
// focus and never zooms back out — which on a phone reads as the page breaking
// the moment you try to type in it.
const FIELD: React.CSSProperties = { padding: "0.5rem 0.6rem", fontSize: 16 };

/** A lookup form that can actually be submitted.
 *
 *  The three forms below used to commit on blur alone, which a phone never
 *  reliably delivers: you type, press the keyboard's Go, and nothing happens
 *  because there is no submit target and no blur. This keeps the draft locally
 *  and commits it on submit — from the button or from Enter — while still
 *  committing on blur so the desktop habit is unchanged. */
function LookupForm({ children, onSubmit }: {
  children: React.ReactNode;
  onSubmit: () => void;
}) {
  return (
    <form
      className="flex"
      style={{ gap: "0.5rem", flexWrap: "wrap", alignItems: "flex-end", marginBottom: "1rem" }}
      onSubmit={(e) => { e.preventDefault(); onSubmit(); }}
    >
      {children}
      <button type="submit" className="btn" style={{ padding: "0.5rem 1.2rem", fontSize: 16 }}>
        חיפוש
      </button>
    </form>
  );
}

function useParam(params: URLSearchParams, key: string): string {
  return params.get(key) ?? "";
}

export default function NadlanPage() {
  useDocumentTitle("נדל\"ן לעם");
  const { t } = useTranslation();
  const [params, setParams] = useSearchParams();

  const urlTab = params.get("tab");
  const tab: Tab = urlTab && TAB_IDS.includes(urlTab as Tab) ? (urlTab as Tab) : "map";

  // The gap report and the quiz were tabs here until they became a project of
  // their own. Links to them are out in the world (the quiz shares its own
  // URL), so the two old names still resolve — to the new address.
  const navigate = useNavigate();
  useEffect(() => {
    if (urlTab === "gaps" || urlTab === "quiz") {
      navigate(`/projects/deals?tab=${urlTab}`, { replace: true });
    }
  }, [urlTab, navigate]);

  const [stats, setStats] = useState<NadlanStats | null>(null);
  const [env, setEnv] = useState<NadlanEnvelope | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [expanded, setExpanded] = useState<string | null>(null);
  const [polygon, setPolygon] = useState<GeoJsonObject | null>(null);

  useEffect(() => {
    nadlan.stats().then(setStats).catch(() => setStats(null));
  }, []);

  // ── URL-driven state ──────────────────────────────────────────────────────
  const lat = params.get("lat") ? Number(params.get("lat")) : null;
  const lon = params.get("lon") ? Number(params.get("lon")) : null;
  const radiusM = Number(params.get("r") ?? 0);
  const gush = useParam(params, "g");
  const helka = useParam(params, "h");
  const zip = useParam(params, "zip");
  const city = useParam(params, "city");
  const street = useParam(params, "street");
  const houseNo = useParam(params, "no");

  // Each form keeps its own draft so it can be SUBMITTED; the URL stays the
  // single source of truth for what was actually looked up, and a link opened
  // with parameters fills the fields.
  const [draftCity, setDraftCity] = useState(city);
  const [draftStreet, setDraftStreet] = useState(street);
  const [draftNo, setDraftNo] = useState(houseNo);
  const [draftZip, setDraftZip] = useState(zip);
  const [draftGush, setDraftGush] = useState(gush);
  const [draftHelka, setDraftHelka] = useState(helka);

  useEffect(() => { setDraftCity(city); }, [city]);
  useEffect(() => { setDraftStreet(street); }, [street]);
  useEffect(() => { setDraftNo(houseNo); }, [houseNo]);
  useEffect(() => { setDraftZip(zip); }, [zip]);
  useEffect(() => { setDraftGush(gush); }, [gush]);
  useEffect(() => { setDraftHelka(helka); }, [helka]);

  const patch = useCallback((next: Record<string, string | null>) => {
    setParams((prev) => {
      const p = new URLSearchParams(prev);
      for (const [k, v] of Object.entries(next)) {
        if (v == null || v === "") p.delete(k);
        else p.set(k, v);
      }
      return p;
    }, { replace: true });
  }, [setParams]);

  // ── the single lookup effect, driven entirely by the URL ──────────────────
  useEffect(() => {
    let cancelled = false;
    const run = async () => {
      let promise: Promise<NadlanEnvelope> | null = null;
      if (tab === "map" && lat != null && lon != null) {
        promise = nadlan.point(lat, lon, radiusM, 50, true);
      } else if (tab === "gush" && gush && helka) {
        promise = nadlan.parcel(Number(gush), Number(helka), undefined, true);
      } else if (tab === "zip" && /^[0-9]{5}([0-9]{2})?$/.test(zip)) {
        promise = nadlan.zip(zip, true);
      } else if (tab === "address" && city && street) {
        promise = nadlan.address(city, street, houseNo || undefined, true);
      }
      if (!promise) { setEnv(null); setError(null); return; }

      setLoading(true); setError(null);
      try {
        const res = await promise;
        if (!cancelled) { setEnv(res); setExpanded(res.data[0]?.parcel_key ?? null); }
      } catch (e) {
        if (!cancelled) {
          setEnv(null);
          setError(e instanceof Error ? e.message : "החיפוש נכשל");
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    };
    run();
    return () => { cancelled = true; };
  }, [tab, lat, lon, radiusM, gush, helka, zip, city, street, houseNo]);

  // The selected parcel's real polygon — fetched separately because it is the
  // only payload that touches the 4.58 GB source table.
  useEffect(() => {
    const p = env?.data.find((d) => d.parcel_key === expanded);
    if (!p) { setPolygon(null); return; }
    let cancelled = false;
    nadlan.geometry(p.identity.gush, p.identity.helka, p.identity.gush_suffix)
      .then((r) => { if (!cancelled) setPolygon(JSON.parse(r.geojson)); })
      .catch(() => { if (!cancelled) setPolygon(null); });
    return () => { cancelled = true; };
  }, [env, expanded]);

  const results: NadlanProperty[] = useMemo(() => env?.data ?? [], [env]);

  return (
    <div>
      {/* Processed-data notice, shared with the other "לעם" projects. */}
      <div className="processed-banner" role="note">
        <div className="container">
          <span className="processed-banner-badge">{t("projects.processed_badge")}</span>
          <span className="processed-banner-text">
            <Trans i18nKey="projects.processed_note" components={{ strong: <strong /> }} />
          </span>
        </div>
      </div>

      <div className="container mt-3">
        <div className="page-header" style={{ marginBottom: "0.75rem" }}>
          <h1 style={{ margin: 0 }}>נדל"ן לעם</h1>
          <div className="text-sm text-muted" style={{ marginTop: "0.35rem", lineHeight: 1.7 }}>
            טיוב וקישור של מידע מרחבי ברמת הנכס: שכבת החלקות, גזטיר הנכסים, קובץ המיקוד ורשימת
            הכתובות, מוצלבים זה לזה. הזינו כל אחת מצורות הזיהוי, נקודה על המפה, מיקוד, כתובת או
            גוש־חלקה, וקבלו את כל השאר — כולל האזור הסטטיסטי של הנקודה ועסקאות המקרקעין
            שדווחו עליה. מאגר העסקאות המלא, והפערים שבינו לבין אתר הנדל״ן הממשלתי, נמצאים
            ב<a href="/projects/deals">עסקאות נדל״ן</a>.
            {stats && (
              <div style={{ marginTop: "0.4rem" }}>
                {stats.parcels.toLocaleString("he-IL")} חלקות ·{" "}
                {stats.addresses.toLocaleString("he-IL")} כתובות ·{" "}
                {stats.streets.toLocaleString("he-IL")} רחובות ·{" "}
                {stats.zip5_codes.toLocaleString("he-IL")} מיקודים
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
              onClick={() => patch({ tab: id === "map" ? null : id })}
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

        {/* ── the four entry forms ── */}
        {tab === "map" && (
          <div className="flex" style={{ gap: "0.6rem", alignItems: "center", flexWrap: "wrap", marginBottom: "0.6rem" }}>
            <label className="text-sm">
              רדיוס:{" "}
              <select
                value={String(radiusM)}
                onChange={(e) => patch({ r: e.target.value === "0" ? null : e.target.value })}
                style={{ padding: "0.25rem 0.5rem" }}
              >
                {RADII.map((r) => (
                  <option key={r} value={r}>{r === 0 ? "החלקה שמתחת לסמן" : `${r} מ׳`}</option>
                ))}
              </select>
            </label>
            <span className="text-sm text-muted">לחצו על המפה כדי לבחור נקודה.</span>
            {lat != null && lon != null && (
              <span className="text-sm text-muted">({lat.toFixed(5)}, {lon.toFixed(5)})</span>
            )}
          </div>
        )}

        {tab === "address" && (
          <LookupForm onSubmit={() => patch({
            city: draftCity.trim(), street: draftStreet.trim(), no: draftNo.trim(),
          })}>
            <input aria-label="יישוב (למשל פתח תקווה)"
              placeholder="יישוב (למשל פתח תקווה)"
              value={draftCity}
              enterKeyHint="search"
              onChange={(e) => setDraftCity(e.target.value)}
              onBlur={(e) => patch({ city: e.target.value.trim() })}
              style={{ ...FIELD, minWidth: 180, flex: "1 1 180px" }}
            />
            <input aria-label="רחוב (למשל אבימלך)"
              placeholder="רחוב (למשל אבימלך)"
              value={draftStreet}
              enterKeyHint="search"
              onChange={(e) => setDraftStreet(e.target.value)}
              onBlur={(e) => patch({ street: e.target.value.trim() })}
              style={{ ...FIELD, minWidth: 180, flex: "1 1 180px" }}
            />
            <input aria-label="מספר בית"
              placeholder="מספר בית"
              value={draftNo}
              enterKeyHint="search"
              onChange={(e) => setDraftNo(e.target.value)}
              onBlur={(e) => patch({ no: e.target.value.trim() })}
              style={{ ...FIELD, width: 110 }}
            />
          </LookupForm>
        )}

        {tab === "zip" && (
          <LookupForm onSubmit={() => patch({ zip: draftZip.trim() })}>
            <input aria-label="מיקוד (5 או 7 ספרות)"
              placeholder="מיקוד (5 או 7 ספרות)"
              value={draftZip}
              onChange={(e) => setDraftZip(e.target.value)}
              onBlur={(e) => patch({ zip: e.target.value.trim() })}
              inputMode="numeric"
              enterKeyHint="search"
              style={{ ...FIELD, minWidth: 200, flex: "1 1 200px" }}
            />
          </LookupForm>
        )}

        {tab === "gush" && (
          <LookupForm onSubmit={() => patch({ g: draftGush.trim(), h: draftHelka.trim() })}>
            <input aria-label="גוש"
              placeholder="גוש"
              value={draftGush}
              onChange={(e) => setDraftGush(e.target.value)}
              onBlur={(e) => patch({ g: e.target.value.trim() })}
              inputMode="numeric"
              enterKeyHint="search"
              style={{ ...FIELD, width: 130 }}
            />
            <input aria-label="חלקה"
              placeholder="חלקה"
              value={draftHelka}
              onChange={(e) => setDraftHelka(e.target.value)}
              onBlur={(e) => patch({ h: e.target.value.trim() })}
              inputMode="numeric"
              enterKeyHint="search"
              style={{ ...FIELD, width: 130 }}
            />
          </LookupForm>
        )}

        {/* Everything below is the lookup half of the page, and now the whole of
            it: the two report tabs moved to /projects/deals. */}
        <>
            {/* The map is NOT exclusive to the map tab: a property found by address,
                zip or gush/helka has to be locatable on the map too, so the same
                polygon layer is shown for every mode and fits itself to the result. */}
            <div style={{ marginBottom: "1rem" }}>
              <Suspense fallback={<div className="text-sm text-muted">טוען מפה…</div>}>
                <NadlanMap
                  lat={lat}
                  lon={lon}
                  radiusM={radiusM}
                  results={results}
                  selected={expanded}
                  polygon={polygon}
                  onPick={(la, lo) => patch({ tab: null, lat: String(la), lon: String(lo) })}
                  onSelect={(k) => setExpanded(k)}
                />
              </Suspense>
              {env?.query?.widened === true && (
                <div className="text-sm" style={{
                  marginTop: "0.4rem", padding: "0.5rem 0.75rem", borderRadius: 8,
                  background: "var(--tint-warn-bg, #fef3c7)", color: "#833909",
                }}>
                  אין חלקה רשומה מתחת לנקודה שנבחרה — חלקות אינן מרצפות את השטח, ובין
                  כביש, שטח פתוח וקרקע לא מוסדרת יש רווחים. מוצגות החלקות הקרובות
                  ביותר ברדיוס {String(env.query.radius_used)} מ׳.
                </div>
              )}
              {results.length > 0 && (
                <div className="text-sm text-muted" style={{ marginTop: "0.3rem" }}>
                  {results.filter((r) => r.geometry).length.toLocaleString("he-IL")} מתוך{" "}
                  {results.length.toLocaleString("he-IL")} חלקות מוצגות עם גבולות החלקה.
                  לחיצה על חלקה במפה תפתח את ההצלבה שלה.
                </div>
              )}
            </div>

            {/* ── results ── */}
            {loading && <div className="text-sm text-muted">מחפש…</div>}
            {error && <div className="text-sm" style={{ color: "var(--danger)" }}>{error}</div>}
            {!loading && !error && env && results.length === 0 && (
              env.miss ? (
                <div style={{
                  padding: "0.7rem 0.9rem", borderRadius: 8,
                  background: "var(--surface-2)", border: "1px solid var(--border)",
                }}>
                  <div className="text-sm" style={{ lineHeight: 1.8 }}>{env.miss.message}</div>
                  {env.miss.suggestions && env.miss.suggestions.length > 0 && (
                    <div className="flex" style={{ gap: "0.35rem", flexWrap: "wrap", marginTop: "0.5rem" }}>
                      {env.miss.suggestions.map((n) => (
                        <button
                          key={n}
                          type="button"
                          onClick={() => patch({ street: n })}
                          style={{
                            padding: "0.25rem 0.7rem", fontSize: "0.85rem", cursor: "pointer",
                            border: "1px solid var(--border)", borderRadius: 999, background: "none",
                          }}
                        >
                          {n}
                        </button>
                      ))}
                    </div>
                  )}
                  {env.miss.reason === "street_not_located" && (
                    <button
                      type="button"
                      onClick={() => patch({ tab: null })}
                      style={{
                        marginTop: "0.5rem", padding: "0.3rem 0.8rem", fontSize: "0.85rem",
                        cursor: "pointer", border: "1px solid var(--border)", borderRadius: 6,
                        background: "none",
                      }}
                    >
                      מעבר למפה ואיתור החלקה בנגיעה
                    </button>
                  )}
                </div>
              ) : (
                <div className="text-sm text-muted">לא נמצאו חלקות להזנה הזו.</div>
              )
            )}

            {results.map((p) => (
              <NadlanResultCard
                key={p.parcel_key}
                property={p}
                expanded={expanded === p.parcel_key}
                onToggle={() => setExpanded(expanded === p.parcel_key ? null : p.parcel_key)}
              />
            ))}

            {/* ── coverage, stated up front rather than discovered ── */}
            <div style={{
              marginTop: "1.5rem", padding: "0.8rem 1rem", borderRadius: 8,
              background: "var(--surface-2)", border: "1px solid var(--border)",
            }}>
              <div style={{ fontWeight: 700, fontSize: "0.9rem", marginBottom: "0.35rem" }}>
                מה הקישור הזה כן ולא יודע
              </div>
              <ul style={{ margin: 0, paddingInlineStart: "1.1rem", fontSize: "0.85rem", lineHeight: 1.8 }}>
                {(env?.caveats ?? [
                  "מיקוד ברמת הכתובת קיים ל-91 יישובים בלבד; בשאר היישובים המיקוד הוא מיקוד כלל-יישובי אחד.",
                  "גזטיר הנכסים מקשר גוש-חלקה לרחוב בלבד, לא למספר בית.",
                  "כ-42% מרשימת הכתובות ללא קואורדינטות, ולכן ללא שיוך מדויק לחלקה.",
                ]).map((c, i) => <li key={i}>{c}</li>)}
                {stats?.coverage && (
                  <li>
                    כיסוי בפועל: {stats.coverage.addresses_with_point_pct}% מהכתובות עם נקודה ·{" "}
                    {stats.coverage.addresses_linked_pct}% משויכות לחלקה ·{" "}
                    {stats.coverage.addresses_with_zip_pct}% עם מיקוד (מתוכם{" "}
                    {stats.coverage.addresses_with_address_zip_pct}% ברמת הכתובת) ·{" "}
                    {stats.coverage.parcels_with_gazetteer_pct}% מהחלקות עם נתוני גזטיר ·{" "}
                    {stats.coverage.streets_in_gazetteer_pct}% מהרחובות שיש להם מיקום מוכרים
                    גם לגזטיר, ועוד {stats.coverage.streets_register_only_pct}% מהרחובות
                    באינדקס מגיעים ממרשם הרחובות הרשמי בלבד ואין להם מיקום.
                  </li>
                )}
              </ul>
            </div>

            <div className="text-sm text-muted" style={{ margin: "1rem 0 0.5rem" }}>
              המידע מעובד, הצלבה שנגזרה מארבעה מקורות, לא מקור ממשלתי ראשוני. כל שדה מקושר לשורת
              המקור שלו בקונסולת <a href="/data">/data</a>. העסקאות עצמן הן שורות המקור של רשות
              המסים, ומוצגות במלואן ב<a href="/projects/deals">עסקאות נדל״ן</a>.
            </div>
        </>
      </div>
    </div>
  );
}
