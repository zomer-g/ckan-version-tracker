import { useEffect, useMemo, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";
import {
  AbroadFeature,
  AbroadFeaturesPage,
  AbroadLayer,
  AbroadSummary,
  govmapAbroad,
  utf8ToBase64,
} from "../api/client";
import { useDocumentTitle } from "../hooks/useDocumentTitle";

/**
 * פרויקט חו"ל — GovMap features that lie outside Israel's outline.
 *
 * The outline is over_israel_boundary ("מפת ישראל": six Interior-Ministry
 * districts, איו"ש, and the maritime area out to the atlas's maritime
 * boundaries); app/services/govmap_abroad.py checks every mirrored GovMap
 * layer against it. This page is the reading side, built for one job: write
 * to each publisher and ask them to fix their misplaced features. So it leads
 * with WHO publishes them (with a CSV per publisher to attach to a letter),
 * then the layers, then the features themselves with a GovMap link each.
 *
 * Selection lives in the URL (?publisher=, ?layer=) so a list can be sent.
 */

const PAGE = 200;

/** A layer where only a small share of features is abroad: the ones abroad are
 *  likely misplaced, rather than the layer deliberately covering neighbours. */
const SUSPECT_SHARE = 0.1;

const fmt = (n: number | null | undefined) => (n == null ? "—" : n.toLocaleString("he-IL"));

function km(v: number | null | undefined): string {
  if (v == null) return "—";
  if (v < 1) return `${Math.round(v * 1000).toLocaleString("he-IL")} מ'`;
  return `${v.toLocaleString("he-IL", { maximumFractionDigits: v < 10 ? 1 : 0 })} ק"מ`;
}

function pct(v: number | null | undefined): string {
  if (v == null) return "—";
  const p = v * 100;
  return `${p < 1 ? p.toFixed(1) : Math.round(p)}%`;
}

function ExtLink({ href, children }: { href: string; children?: React.ReactNode }) {
  return (
    <a href={href} target="_blank" rel="noopener noreferrer">
      {children}
      <span className="sr-only"> (נפתח בחלון חדש)</span>
    </a>
  );
}

function govmapLayerUrl(l: { layer_id: string | null; title: string | null }): string | null {
  if (!l.layer_id) return null;
  const atlas = (l.title || "").includes("(אטלס ישראל)");
  return `https://www.${atlas ? "atlas" : "govmap"}.gov.il/?lay=${l.layer_id}`;
}

/** The layer's abroad features in the /data console, drawn on its map. */
function dataMapUrl(datasetId: string): string {
  const sql =
    `SELECT layer_id, round(distance_km::numeric, 3) AS distance_km, attrs::text AS attrs, geom\n` +
    `FROM over_govmap_abroad_features\nWHERE dataset_id = '${datasetId}'\nORDER BY distance_km DESC`;
  return `/data?q=${encodeURIComponent(utf8ToBase64(sql))}&chart=map`;
}

function safeFilename(s: string): string {
  return s.replace(/[\\/:*?"<>|]+/g, "_").slice(0, 60);
}

export default function AbroadPage() {
  useDocumentTitle('חו"ל — פריטי GovMap מחוץ לגבולות המדינה');
  const [params, setParams] = useSearchParams();
  const publisher = params.get("publisher") || "";
  const layerId = params.get("layer") || "";
  const [suspectOnly, setSuspectOnly] = useState(params.get("suspect") === "1");

  const [summary, setSummary] = useState<AbroadSummary | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [page, setPage] = useState<AbroadFeaturesPage | null>(null);
  const [pageErr, setPageErr] = useState<string | null>(null);
  const [offset, setOffset] = useState(0);
  const [loadingFeatures, setLoadingFeatures] = useState(false);
  const [csvBusy, setCsvBusy] = useState<string | null>(null);

  useEffect(() => {
    govmapAbroad.summary().then(setSummary).catch((e) => setErr(String(e.message || e)));
  }, []);

  const layers = summary?.layers ?? [];
  const selectedLayer = useMemo(
    () => layers.find((l) => l.dataset_id === layerId) || null,
    [layers, layerId],
  );
  const shownLayers = useMemo(
    () =>
      layers.filter(
        (l) =>
          (!publisher || l.publisher === publisher) &&
          (!suspectOnly || (l.share != null && l.share < SUSPECT_SHARE)),
      ),
    [layers, publisher, suspectOnly],
  );

  // The feature list follows the selection: one layer, else one publisher.
  const featureFilter = useMemo(
    () => (layerId ? { dataset_id: layerId } : publisher ? { publisher } : null),
    [layerId, publisher],
  );
  useEffect(() => setOffset(0), [featureFilter]);
  useEffect(() => {
    if (!featureFilter) {
      setPage(null);
      return;
    }
    let alive = true;
    setLoadingFeatures(true);
    setPageErr(null);
    govmapAbroad
      .features(featureFilter, PAGE, offset)
      .then((p) => alive && setPage(p))
      .catch((e) => alive && setPageErr(String(e.message || e)))
      .finally(() => alive && setLoadingFeatures(false));
    return () => {
      alive = false;
    };
  }, [featureFilter, offset]);

  const select = (next: { publisher?: string; layer?: string }) => {
    const p = new URLSearchParams(params);
    if (next.publisher !== undefined) {
      if (next.publisher) p.set("publisher", next.publisher);
      else p.delete("publisher");
    }
    if (next.layer !== undefined) {
      if (next.layer) p.set("layer", next.layer);
      else p.delete("layer");
    }
    setParams(p, { replace: false });
  };

  const toggleSuspect = () => {
    const v = !suspectOnly;
    setSuspectOnly(v);
    const p = new URLSearchParams(params);
    if (v) p.set("suspect", "1");
    else p.delete("suspect");
    setParams(p, { replace: true });
  };

  const downloadCsv = async (f: { publisher?: string; dataset_id?: string }, label: string) => {
    const key = f.publisher || f.dataset_id || "";
    setCsvBusy(key);
    try {
      await govmapAbroad.downloadCsv(f, `govmap_abroad_${safeFilename(label)}.csv`);
    } catch (e) {
      setPageErr(String((e as Error).message || e));
    } finally {
      setCsvBusy(null);
    }
  };

  const t = summary?.totals;
  const maritime = summary?.regions?.find((r) => r.region === "maritime");

  return (
    <div>
      <div className="about-hero">
        <div className="container">
          <h1>חו"ל</h1>
          <p className="rationale-hero-sub">
            פריטים בשכבות GovMap שנמצאים מחוץ לגבולות המדינה: מי פרסם אותם, באילו שכבות, ואיפה
            בדיוק. לכל גוף מפרסם יש רשימה מלאה להורדה, לצרף למכתב שמבקש לתקן.
          </p>
        </div>
      </div>

      <div className="abroad-page">
        {err && <div className="abroad-error" role="alert">{err}</div>}
        {!summary && !err && <p className="text-muted">טוען…</p>}
        {summary && !summary.ready && (
          <div className="about-card">
            <p>הבדיקה עוד לא רצה. היא רצה כל לילה, ובפעם הראשונה כחצי שעה אחרי עליית השרת.</p>
          </div>
        )}

        {summary?.ready && t && (
          <>
            <section className="abroad-counts" aria-label="סיכום">
              <div><b>{fmt(t.abroad)}</b><span>פריטים בחו"ל</span></div>
              <div><b>{fmt(t.layers_abroad)}</b><span>שכבות, מתוך {fmt(t.layers_checked)} שנבדקו</span></div>
              <div><b>{fmt(summary.publishers?.length)}</b><span>גופים מפרסמים</span></div>
              <div><b>{fmt(t.features_checked)}</b><span>פריטים נבדקו בסך הכול</span></div>
            </section>

            <details className="abroad-method">
              <summary>איך נקבע מה בחו"ל</summary>
              <p>
                כל פריט בכל שכבת GovMap שנשמרת בגרסאות לעם נבדק מול{" "}
                <Link to="/data?table=over_israel_boundary">מפת ישראל</Link> (הטבלה{" "}
                <code>over_israel_boundary</code>), שבנויה משלושה אזורים: ששת מחוזות משרד הפנים
                (כולל ירושלים המזרחית והגולן), מחוז יהודה ושומרון, והמרחב הימי
                {maritime?.area_km2 ? ` (${fmt(Math.round(maritime.area_km2))} קמ"ר)` : ""}, שנבנה
                מקווי הגבול הימי באטלס ישראל. פריט נחשב "בחו"ל" רק אם הוא לא נוגע באף אחד משלושתם
                {t.tolerance_m ? ` ונמצא יותר מ-${fmt(t.tolerance_m)} מטר מהגבול` : ""}. פריט שחוצה את
                הגבול נחשב בפנים.
              </p>
              <p>
                לא כל פריט בחו"ל הוא טעות. יש שכבות שמכסות בכוונה גם את המדינות השכנות (בעיקר המפות
                הגאולוגיות והגאומורפולוגיות), ויש שכבות ימיות שחורגות מהמרחב הימי. כדי למצוא טעויות
                כדאי להתחיל בשכבות שבהן רק מיעוט קטן מהפריטים נמצא בחו"ל: שם הפריטים החריגים הם
                כמעט תמיד מיקום שגוי.
              </p>
              <p>
                הנתונים עצמם פתוחים ב-SQL:{" "}
                <Link to="/data?table=over_govmap_abroad">over_govmap_abroad</Link> (שורה לכל
                שכבה) ו-
                <Link to="/data?table=over_govmap_abroad_features">over_govmap_abroad_features</Link>{" "}
                (שורה לכל פריט, עם הגאומטריה). עודכן לאחרונה:{" "}
                {t.checked_at ? new Date(t.checked_at).toLocaleString("he-IL") : "—"}.
              </p>
            </details>

            <section className="about-card abroad-section">
              <h2>לפי גוף מפרסם</h2>
              <div className="abroad-table-wrap">
                <table className="abroad-table">
                  <caption className="sr-only">גופים מפרסמים לפי מספר הפריטים בחו"ל</caption>
                  <thead>
                    <tr>
                      <th scope="col">גוף מפרסם</th>
                      <th scope="col" className="num">שכבות</th>
                      <th scope="col" className="num">פריטים בחו"ל</th>
                      <th scope="col"><span className="sr-only">פעולות</span></th>
                    </tr>
                  </thead>
                  <tbody>
                    {(summary.publishers || []).map((p) => (
                      <tr key={p.publisher} className={publisher === p.publisher ? "is-selected" : ""}>
                        <th scope="row">
                          <button type="button" className="linkish"
                                  aria-pressed={publisher === p.publisher}
                                  onClick={() => select({ publisher: publisher === p.publisher ? "" : p.publisher, layer: "" })}>
                            {p.publisher}
                          </button>
                        </th>
                        <td className="num">{fmt(p.layers)}</td>
                        <td className="num">{fmt(p.abroad)}</td>
                        <td>
                          <button type="button" className="btn-secondary abroad-btn"
                                  disabled={csvBusy === p.publisher}
                                  onClick={() => downloadCsv({ publisher: p.publisher }, p.publisher)}>
                            {csvBusy === p.publisher ? "מוריד…" : "CSV למכתב"}
                          </button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>

            <section className="about-card abroad-section">
              <div className="abroad-section-head">
                <h2>
                  שכבות{publisher ? ` של ${publisher}` : ""}{" "}
                  <span className="text-muted text-sm">({fmt(shownLayers.length)})</span>
                </h2>
                <div className="abroad-filters">
                  {publisher && (
                    <button type="button" className="btn-secondary abroad-btn"
                            onClick={() => select({ publisher: "", layer: "" })}>
                      כל הגופים ✕
                    </button>
                  )}
                  <label className="abroad-check">
                    <input id="abroad-suspect" type="checkbox" checked={suspectOnly} onChange={toggleSuspect} />
                    רק שכבות שבהן פחות מ-{Math.round(SUSPECT_SHARE * 100)}% מהפריטים בחו"ל (חשד לטעות)
                  </label>
                </div>
              </div>
              <div className="abroad-table-wrap">
                <table className="abroad-table">
                  <caption className="sr-only">שכבות לפי מספר הפריטים בחו"ל</caption>
                  <thead>
                    <tr>
                      <th scope="col">שכבה</th>
                      <th scope="col">גוף מפרסם</th>
                      <th scope="col" className="num">בחו"ל</th>
                      <th scope="col" className="num">מתוך</th>
                      <th scope="col" className="num">שיעור</th>
                      <th scope="col" className="num">מרחק מהגבול</th>
                    </tr>
                  </thead>
                  <tbody>
                    {shownLayers.map((l) => (
                      <LayerRow key={l.dataset_id} l={l} selected={l.dataset_id === layerId}
                                onSelect={() => select({ layer: l.dataset_id === layerId ? "" : l.dataset_id })} />
                    ))}
                    {shownLayers.length === 0 && (
                      <tr><td colSpan={6} className="text-muted">אין שכבות שעונות על הסינון.</td></tr>
                    )}
                  </tbody>
                </table>
              </div>
            </section>

            <section className="about-card abroad-section" aria-live="polite">
              {!featureFilter && (
                <p className="text-muted">
                  בחרו גוף מפרסם או שכבה כדי לראות את רשימת הפריטים עצמם.
                </p>
              )}
              {featureFilter && (
                <>
                  <div className="abroad-section-head">
                    <h2>
                      הפריטים{" "}
                      {selectedLayer ? `בשכבה "${selectedLayer.title}"` : publisher ? `של ${publisher}` : ""}{" "}
                      <span className="text-muted text-sm">({fmt(page?.total)})</span>
                    </h2>
                    <div className="abroad-filters">
                      {selectedLayer && (
                        <>
                          <Link className="btn-secondary abroad-btn" to={dataMapUrl(selectedLayer.dataset_id)}>
                            הצג במפה
                          </Link>
                          <button type="button" className="btn-secondary abroad-btn"
                                  onClick={() => select({ layer: "" })}>
                            כל השכבות ✕
                          </button>
                        </>
                      )}
                      <button type="button" className="btn-primary abroad-btn"
                              disabled={!!csvBusy}
                              onClick={() => downloadCsv(featureFilter,
                                selectedLayer ? selectedLayer.title || selectedLayer.dataset_id : publisher)}>
                        {csvBusy ? "מוריד…" : "הורדת הרשימה (CSV)"}
                      </button>
                    </div>
                  </div>
                  {pageErr && <div className="abroad-error" role="alert">{pageErr}</div>}
                  {loadingFeatures && <p className="text-muted">טוען…</p>}
                  {page && (
                    <>
                      <div className="abroad-table-wrap">
                        <table className="abroad-table abroad-features">
                          <caption className="sr-only">הפריטים שנמצאו מחוץ לגבולות המדינה</caption>
                          <thead>
                            <tr>
                              <th scope="col">פריט</th>
                              {!selectedLayer && <th scope="col">שכבה</th>}
                              <th scope="col" className="num">מרחק מהגבול</th>
                              <th scope="col">מיקום</th>
                              <th scope="col">שדות</th>
                            </tr>
                          </thead>
                          <tbody>
                            {page.items.map((f) => (
                              <FeatureRow key={`${f.dataset_id}:${f.row_hash}`} f={f} showLayer={!selectedLayer} />
                            ))}
                          </tbody>
                        </table>
                      </div>
                      {page.total > PAGE && (
                        <nav className="abroad-pager" aria-label="עמודים">
                          <button type="button" className="btn-secondary abroad-btn"
                                  disabled={offset === 0} onClick={() => setOffset(Math.max(0, offset - PAGE))}>
                            הקודם
                          </button>
                          <span>
                            {fmt(offset + 1)}–{fmt(Math.min(offset + PAGE, page.total))} מתוך {fmt(page.total)}
                          </span>
                          <button type="button" className="btn-secondary abroad-btn"
                                  disabled={offset + PAGE >= page.total} onClick={() => setOffset(offset + PAGE)}>
                            הבא
                          </button>
                        </nav>
                      )}
                    </>
                  )}
                </>
              )}
            </section>
          </>
        )}
      </div>
    </div>
  );
}

function LayerRow({ l, selected, onSelect }: { l: AbroadLayer; selected: boolean; onSelect: () => void }) {
  const url = govmapLayerUrl(l);
  const suspect = l.share != null && l.share < SUSPECT_SHARE;
  return (
    <tr className={selected ? "is-selected" : ""}>
      <th scope="row">
        <button type="button" className="linkish" aria-pressed={selected} onClick={onSelect}>
          {l.title || l.table_name}
        </button>
        <div className="text-sm text-muted">
          שכבה {l.layer_id ?? "—"}
          {url && <> · <ExtLink href={url}>ב-GovMap</ExtLink></>}
          {" · "}
          <Link to={`/versions/${l.dataset_id}`}>בגרסאות לעם</Link>
          {suspect && <span className="abroad-tag">חשד לטעות</span>}
        </div>
      </th>
      <td>{l.publisher}</td>
      <td className="num"><b>{fmt(l.abroad)}</b></td>
      <td className="num">{fmt(l.rows_with_geom)}</td>
      <td className="num">{pct(l.share)}</td>
      <td className="num">
        {l.min_distance_km != null && l.max_distance_km != null && l.min_distance_km !== l.max_distance_km
          ? `${km(l.min_distance_km)}–${km(l.max_distance_km)}`
          : km(l.max_distance_km)}
      </td>
    </tr>
  );
}

function FeatureRow({ f, showLayer }: { f: AbroadFeature; showLayer: boolean }) {
  const entries = Object.entries(f.attrs || {}).filter(([, v]) => v !== "" && v != null);
  const objectId = (f.attrs || {})["objectId"];
  return (
    <tr>
      <th scope="row">
        {f.name || <span className="text-muted">ללא שם</span>}
        {objectId != null && <div className="text-sm text-muted">objectId {String(objectId)}</div>}
      </th>
      {showLayer && (
        <td>
          {f.title}
          <div className="text-sm text-muted">{f.publisher}</div>
        </td>
      )}
      <td className="num">{km(f.distance_km)}</td>
      <td>
        {f.lat != null && f.lon != null && (
          <code dir="ltr" className="abroad-coord">{f.lat.toFixed(5)}, {f.lon.toFixed(5)}</code>
        )}
        {f.govmap_url && <div><ExtLink href={f.govmap_url}>פתיחה ב-GovMap</ExtLink></div>}
      </td>
      <td>
        {entries.length > 0 && (
          <details>
            <summary>{entries.length} שדות</summary>
            <dl className="abroad-attrs">
              {entries.map(([k, v]) => (
                <div key={k}>
                  <dt>{k}</dt>
                  <dd>{typeof v === "object" ? JSON.stringify(v) : String(v)}</dd>
                </div>
              ))}
            </dl>
          </details>
        )}
      </td>
    </tr>
  );
}
