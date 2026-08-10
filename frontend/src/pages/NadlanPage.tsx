/**
 * נדל"ן לעם — the property-level spatial crosswalk.
 *
 * Four ways into the same answer: a point on the map (with a radius), a postal
 * code, an address, or a gush/helka. Whichever you use, the result is the same
 * envelope from /api/nadlan — the property's identity in every other codespace
 * plus a link to each source's full row on /data.
 *
 * Everything lives in the query string (?tab=&lat=&lon=&r=&g=&h=&zip=&city=…)
 * so every result is a shareable link, the convention the /data console and
 * GovmapView already follow. The map is lazy-imported: Leaflet is ~150 KB and
 * three of the four tabs never need it.
 */
import { lazy, Suspense, useCallback, useEffect, useMemo, useState } from "react";
import { Trans, useTranslation } from "react-i18next";
import { useSearchParams } from "react-router-dom";
import type { GeoJsonObject } from "geojson";
import { nadlan, NadlanEnvelope, NadlanProperty, NadlanStats } from "../api/client";
import NadlanResultCard from "../components/nadlan/NadlanResultCard";

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

function useParam(params: URLSearchParams, key: string): string {
  return params.get(key) ?? "";
}

export default function NadlanPage() {
  const { t } = useTranslation();
  const [params, setParams] = useSearchParams();

  const urlTab = params.get("tab") as Tab | null;
  const tab: Tab = urlTab && TAB_IDS.includes(urlTab) ? urlTab : "map";

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
            הכתובות — מוצלבים זה לזה. הזינו כל אחת מצורות הזיהוי — נקודה על המפה, מיקוד, כתובת או
            גוש־חלקה — וקבלו את כל השאר.
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
        <div className="flex" style={{ gap: "0.3rem", borderBottom: "2px solid var(--border, #e2e8f0)", marginBottom: "1rem", flexWrap: "wrap" }}>
          {TAB_LABELS.map(([id, label]) => (
            <button
              key={id}
              type="button"
              onClick={() => patch({ tab: id === "map" ? null : id })}
              style={{
                padding: "0.5rem 1.05rem", border: "none", cursor: "pointer", background: "none",
                fontSize: "0.95rem", fontWeight: tab === id ? 700 : 500,
                color: tab === id ? "var(--primary, #0f766e)" : "var(--text-muted)",
                borderBottom: tab === id ? "3px solid var(--primary, #0f766e)" : "3px solid transparent",
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
          <form
            className="flex"
            style={{ gap: "0.5rem", flexWrap: "wrap", marginBottom: "1rem" }}
            onSubmit={(e) => e.preventDefault()}
          >
            <input
              placeholder="יישוב (למשל פתח תקווה)"
              defaultValue={city}
              onBlur={(e) => patch({ city: e.target.value })}
              style={{ padding: "0.45rem 0.6rem", minWidth: 180 }}
            />
            <input
              placeholder="רחוב (למשל אבימלך)"
              defaultValue={street}
              onBlur={(e) => patch({ street: e.target.value })}
              style={{ padding: "0.45rem 0.6rem", minWidth: 180 }}
            />
            <input
              placeholder="מספר בית"
              defaultValue={houseNo}
              onBlur={(e) => patch({ no: e.target.value })}
              style={{ padding: "0.45rem 0.6rem", width: 110 }}
            />
          </form>
        )}

        {tab === "zip" && (
          <form className="flex" style={{ gap: "0.5rem", marginBottom: "1rem" }} onSubmit={(e) => e.preventDefault()}>
            <input
              placeholder="מיקוד (5 או 7 ספרות)"
              defaultValue={zip}
              onBlur={(e) => patch({ zip: e.target.value.trim() })}
              inputMode="numeric"
              style={{ padding: "0.45rem 0.6rem", minWidth: 200 }}
            />
          </form>
        )}

        {tab === "gush" && (
          <form className="flex" style={{ gap: "0.5rem", marginBottom: "1rem" }} onSubmit={(e) => e.preventDefault()}>
            <input
              placeholder="גוש"
              defaultValue={gush}
              onBlur={(e) => patch({ g: e.target.value.trim() })}
              inputMode="numeric"
              style={{ padding: "0.45rem 0.6rem", width: 130 }}
            />
            <input
              placeholder="חלקה"
              defaultValue={helka}
              onBlur={(e) => patch({ h: e.target.value.trim() })}
              inputMode="numeric"
              style={{ padding: "0.45rem 0.6rem", width: 130 }}
            />
          </form>
        )}

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
        {error && <div className="text-sm" style={{ color: "#b91c1c" }}>{error}</div>}
        {!loading && !error && env && results.length === 0 && (
          <div className="text-sm text-muted">לא נמצאו חלקות להזנה הזו.</div>
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
          background: "var(--surface-muted,#f8fafc)", border: "1px solid var(--border,#e2e8f0)",
        }}>
          <div style={{ fontWeight: 700, fontSize: "0.9rem", marginBottom: "0.35rem" }}>
            מה הקישור הזה כן ולא יודע
          </div>
          <ul style={{ margin: 0, paddingInlineStart: "1.1rem", fontSize: "0.85rem", lineHeight: 1.8 }}>
            {(env?.caveats ?? [
              "המיקוד זמין ל-91 יישובים בלבד (קובץ המיקוד של דואר ישראל).",
              "גזטיר הנכסים מקשר גוש-חלקה לרחוב בלבד — לא למספר בית.",
              "כ-30% מרשימת הכתובות ללא קואורדינטות, ולכן ללא שיוך לחלקה.",
            ]).map((c, i) => <li key={i}>{c}</li>)}
            {stats?.coverage && (
              <li>
                כיסוי בפועל: {stats.coverage.addresses_with_point_pct}% מהכתובות עם נקודה ·{" "}
                {stats.coverage.addresses_linked_pct}% משויכות לחלקה ·{" "}
                {stats.coverage.addresses_with_zip_pct}% עם מיקוד ·{" "}
                {stats.coverage.parcels_with_gazetteer_pct}% מהחלקות עם נתוני גזטיר.
              </li>
            )}
          </ul>
        </div>

        <div className="text-sm text-muted" style={{ margin: "1rem 0 0.5rem" }}>
          המידע מעובד — הצלבה שנגזרה מארבעה מקורות, לא מקור ממשלתי ראשוני. כל שדה מקושר לשורת
          המקור שלו בקונסולת <a href="/data">/data</a>.
        </div>
      </div>
    </div>
  );
}
