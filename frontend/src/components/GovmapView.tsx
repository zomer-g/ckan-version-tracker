/**
 * In-page map + categorical-filter view for govmap datasets.
 *
 * Renders the latest version's GeoJSON layer with OSM tiles, auto-fits
 * the viewport to the data bbox once, and auto-discovers
 * categorical-string filters from feature properties. AND across
 * fields, OR within a field; an empty selection on a field means "no
 * constraint on this field" (vs. "exclude everything"), matching the
 * UI affordance of unticking the last checkbox.
 *
 * This module imports Leaflet CSS and the Leaflet library itself, so
 * it lives behind a ``React.lazy`` import in ``VersionsPage`` — pages
 * that don't show a map (CKAN, scraper, idf) don't pay the bundle
 * cost. See plan file for the placement contract.
 */
import { useEffect, useMemo, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { useSearchParams } from "react-router-dom";
import { MapContainer, TileLayer, GeoJSON } from "react-leaflet";
import L from "leaflet";
import "leaflet/dist/leaflet.css";

/** Reserved query-string keys this component may NOT use as filter
 *  fields. Today the dataset page itself doesn't read any URL params,
 *  but pages it may live inside (login redirect, etc.) sometimes do.
 *  Adding to this list is cheap and keeps us forward-compatible. */
const RESERVED_URL_PARAMS = new Set<string>([]);

/** Query-string ↔ filter dict.
 *  ?yeshuvname=תקוע,אלון%20שבות&moatza=גוש%20עציון
 *    ↔ { yeshuvname: {"תקוע","אלון שבות"}, moatza: {"גוש עציון"} }
 *  Comma is the separator. Commas inside a value (rare on GovMap
 *  layers; never in our reference dataset) would lose round-trip,
 *  which we accept — URLSearchParams encodes everything else
 *  (Hebrew, spaces) cleanly. */
function searchParamsToFilters(sp: URLSearchParams): Record<string, Set<string>> {
  const out: Record<string, Set<string>> = {};
  for (const [k, v] of sp.entries()) {
    if (RESERVED_URL_PARAMS.has(k)) continue;
    if (!v) continue;
    const vals = v.split(",").map((s) => s.trim()).filter(Boolean);
    if (!vals.length) continue;
    out[k] = new Set(vals);
  }
  return out;
}

function filtersToSearchParams(
  filters: Record<string, ReadonlySet<string>>,
  existing: URLSearchParams,
): URLSearchParams {
  // Start from a clone so we don't blow away unrelated params the
  // host page is using.
  const next = new URLSearchParams(existing);
  // Drop any existing filter params from the previous render. We can
  // tell a filter param apart from a non-filter one by looking at the
  // previous filter state — anything that was a filter key and isn't
  // any more should be removed.
  for (const key of Array.from(next.keys())) {
    if (RESERVED_URL_PARAMS.has(key)) continue;
    // Heuristic: any key that *could* be a filter — we delete it,
    // then re-add the ones that should still be there. This is safe
    // because the dataset page doesn't read other params today, and
    // RESERVED_URL_PARAMS protects future ones.
    next.delete(key);
  }
  for (const [field, set] of Object.entries(filters)) {
    if (!set.size) continue;
    next.set(field, Array.from(set).join(","));
  }
  return next;
}

import {
  applyFilters,
  COLLAPSED_VISIBLE_VALUES,
  discoverCategoricalFields,
  type MinimalFeature,
} from "../utils/geoFilters";

// Leaflet's default marker icon paths are baked relative to its own
// distribution folder, which breaks under Vite/webpack bundlers that
// rehash assets. We rebind them to the bundled URLs so any point
// features render with a proper pin instead of a broken-image icon.
// Polygons (the common case) ignore this entirely.
import iconUrl from "leaflet/dist/images/marker-icon.png";
import iconRetinaUrl from "leaflet/dist/images/marker-icon-2x.png";
import shadowUrl from "leaflet/dist/images/marker-shadow.png";
// eslint-disable-next-line @typescript-eslint/no-explicit-any
delete (L.Icon.Default.prototype as any)._getIconUrl;
L.Icon.Default.mergeOptions({ iconUrl, iconRetinaUrl, shadowUrl });

interface GovmapViewProps {
  geojsonDownloadUrl: string;
}

interface FeatureCollection {
  type: "FeatureCollection";
  features: MinimalFeature[];
}

// Filter state: per field, the SET of values the user has ticked.
// Empty Set = no constraint (vs. literally exclude everything — see
// applyFilters semantics).
type FilterState = Record<string, Set<string>>;

const MAP_HEIGHT = 500;

// Fields we deliberately keep OUT of the filter sidebar even when
// they're otherwise categorical-eligible. The popup still shows
// blocked fields so the user can read the value per-feature. Empty
// by default — every eligible field shows as a filter. Add field
// names here case-sensitively when a specific field turns out to be
// noisy / unhelpful as a checklist on real data.
const FILTER_BLOCKLIST: string[] = [];
// Light green fill (same palette family as the IDF badge so the
// design language stays coherent across non-CKAN sources). The fill
// is intentionally faint so polygons in close proximity still read
// as distinct. Defaults; the user can override them at runtime via
// the "תצוגה" panel in the sidebar.
const DEFAULT_LAYER_STYLE = {
  color: "#0f766e",
  weight: 1,
  fillColor: "#5d936c",
  fillOpacity: 0.25,
};

type SortMode = "count" | "alpha";

interface LayerStyle {
  color: string;
  weight: number;
  fillColor: string;
  fillOpacity: number;
}

export default function GovmapView({ geojsonDownloadUrl }: GovmapViewProps) {
  const { t } = useTranslation();
  const [fc, setFc] = useState<FeatureCollection | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  // Three-stage progress reporter so the loading placeholder shows
  // something useful instead of a static spinner — phone users
  // otherwise see ~10-20 s of "טוען..." with no feedback and assume
  // the page is broken. Each stage's progress is 0..1, null while
  // not active.
  const [downloadProgress, setDownloadProgress] = useState<number | null>(null);
  const [parsing, setParsing] = useState(false);

  // Detect small viewports once and tune the GeoJSON layer's
  // smoothFactor accordingly. Canvas rendering work scales with
  // vertex count; on a 360px-wide phone, the default smoothFactor=1
  // is drawing sub-pixel detail that nobody can see. 4 ≈ "keep only
  // ~1/4 of the vertices at this zoom" — dramatic perf improvement
  // on mobile, indistinguishable visually until you zoom way in.
  const isSmallScreen = useMemo(() => {
    if (typeof window === "undefined") return false;
    return window.matchMedia("(max-width: 768px)").matches;
  }, []);
  const layerSmoothFactor = isSmallScreen ? 4 : 1.5;

  // On phones we DON'T auto-fetch the GeoJSON. Large layers (the
  // agricultural-parcels one is ~50MB gzipped → ~250MB parsed) push
  // mobile browsers past their per-tab memory ceiling and Safari /
  // Chrome respond by killing the tab — the user sees iOS's
  // "אירעה בעיה חוזרת" / "a problem repeatedly occurred" prompt
  // and the page is unusable. Gating behind an explicit tap turns
  // an "instant crash" into an "informed choice": small datasets
  // are still one tap away, big ones now warn the user before they
  // sink their phone.
  // Desktop browsers auto-load as before.
  const [userOptedIn, setUserOptedIn] = useState(!isSmallScreen);

  // Filter-list sort mode. "count" is the default (descending by
  // feature count, so the most-represented values surface first);
  // "alpha" sorts by Hebrew/locale alphabetical. Stored at the
  // component level so the toggle applies across all fieldsets.
  const [sortMode, setSortMode] = useState<SortMode>("count");

  // Layer style — overridable from the sidebar's "תצוגה" panel so
  // the user can tune colours and opacity to make their layer
  // legible against whatever basemap area they're inspecting.
  // Kept in component state (not URL) because it's a personal
  // viewing preference, not part of the shareable filter view.
  const [layerStyle, setLayerStyle] = useState<LayerStyle>(DEFAULT_LAYER_STYLE);
  const [stylePanelOpen, setStylePanelOpen] = useState(false);

  // Ref to the Leaflet GeoJSON layer so we can call setStyle() when
  // the user moves a slider, without remounting the 200k-feature
  // collection (remount would burn 3-5 s every tick). setStyle on
  // the existing layer is essentially free — it just rewrites the
  // path options and triggers a redraw. smoothFactor isn't a Path
  // visual option (it's a topology setting), so we don't pass it
  // through setStyle — it stays whatever value the layer was
  // constructed with.
  const geojsonRef = useRef<L.GeoJSON | null>(null);
  useEffect(() => {
    const layer = geojsonRef.current;
    if (!layer) return;
    layer.setStyle(layerStyle);
  }, [layerStyle]);

  // Filter state lives in the URL — so a URL with ?yeshuvname=תקוע
  // restores the same filtered view, and any toggle the user makes
  // updates the URL in place (no scroll, no history spam).
  const [searchParams, setSearchParams] = useSearchParams();
  const [filters, setFiltersState] = useState<FilterState>(() =>
    searchParamsToFilters(searchParams),
  );
  // Wrap setFilters so every update also writes to the query string.
  const setFilters = (
    update: FilterState | ((prev: FilterState) => FilterState),
  ) => {
    setFiltersState((prev) => {
      const next = typeof update === "function"
        ? (update as (p: FilterState) => FilterState)(prev)
        : update;
      // `replace: true` keeps the back button useful — filter toggles
      // shouldn't add 50 history entries during exploration.
      setSearchParams(filtersToSearchParams(next, searchParams), {
        replace: true,
      });
      return next;
    });
  };

  useEffect(() => {
    // Wait for explicit opt-in on small screens — the fetch is heavy
    // enough to OOM-kill the tab on phones, so we don't start it
    // until the user clicks "טען מפה".
    if (!userOptedIn) return;
    let cancelled = false;
    (async () => {
      try {
        const resp = await fetch(geojsonDownloadUrl);
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        // Stream the body manually so we can show a progress bar.
        // resp.body is a ReadableStream of Uint8Array chunks; we
        // accumulate them and report bytes-received vs the
        // Content-Length header. Phones on cellular need this — a
        // single arrayBuffer() await means the user sees nothing
        // for 10-20s and assumes the page is hung.
        if (!resp.body) throw new Error("Response has no body");
        const totalRaw = resp.headers.get("content-length");
        const total = totalRaw ? Number(totalRaw) : 0;
        const reader = resp.body.getReader();
        const chunks: Uint8Array[] = [];
        let received = 0;
        // eslint-disable-next-line no-constant-condition
        while (true) {
          const { done, value } = await reader.read();
          if (cancelled) return;
          if (done) break;
          if (value) {
            chunks.push(value);
            received += value.byteLength;
            if (total > 0) {
              setDownloadProgress(received / total);
            }
          }
        }
        // Concatenate chunks into one buffer.
        const buf = new Uint8Array(received);
        let off = 0;
        for (const c of chunks) {
          buf.set(c, off);
          off += c.byteLength;
        }
        setDownloadProgress(1);

        // Large GeoJSON layers (~200MB+) are stored gzipped on odata
        // because CKAN resource_create rejects plain bodies above
        // ~100MB. odata's /download route serves them WITHOUT the
        // filename suffix and WITHOUT a Content-Encoding header, so
        // detecting by URL or headers isn't reliable. Instead we
        // sniff the body's first two bytes: gzip is unambiguously
        // 0x1F 0x8B and a JSON document can never begin with those.
        const isGz =
          buf.length >= 2 && buf[0] === 0x1f && buf[1] === 0x8b;
        let text: string;
        if (isGz) {
          if (typeof DecompressionStream === "undefined") {
            throw new Error("Browser lacks DecompressionStream support");
          }
          const stream = new Blob([buf])
            .stream()
            .pipeThrough(new DecompressionStream("gzip"));
          text = await new Response(stream).text();
        } else {
          text = new TextDecoder("utf-8").decode(buf);
        }
        if (cancelled) return;

        // JSON.parse of ~200MB takes 3-15s on a mobile CPU and
        // blocks the main thread — there's nothing we can show
        // during it, but we DO want the previous "downloading"
        // bar to flip into a "parsing" message so the user knows
        // the bytes arrived. setParsing(true), yield one frame to
        // let React paint, THEN call JSON.parse.
        setParsing(true);
        await new Promise((r) => setTimeout(r, 0));
        if (cancelled) return;
        const data = JSON.parse(text) as FeatureCollection;
        if (cancelled) return;
        setFc(data);
      } catch (e) {
        if (!cancelled) setLoadError(String((e as Error)?.message ?? e));
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [geojsonDownloadUrl, userOptedIn]);

  // The full set of feature properties → distinct value counts. Doesn't
  // change when the user toggles filters — the chip list itself stays
  // stable; only the visible-feature counter and checkbox states move.
  const fieldCounts = useMemo(
    () =>
      fc
        ? discoverCategoricalFields(fc.features, {
            blocklist: FILTER_BLOCKLIST,
          })
        : {},
    [fc],
  );

  const filteredFeatures = useMemo(() => {
    if (!fc) return [];
    return applyFilters(fc.features, filters);
  }, [fc, filters]);

  // The serialized filter state drives the GeoJSON component's `key`:
  // changing the key forces Leaflet to drop the old layer and add a
  // new one with the filtered data. react-leaflet's GeoJSON doesn't
  // reactively re-filter on prop changes, so this is the standard
  // escape hatch.
  const filterKey = useMemo(() => {
    const sorted = Object.entries(filters)
      .map(([k, s]) => `${k}=${[...s].sort().join("|")}`)
      .sort()
      .join(";");
    return sorted || "none";
  }, [filters]);

  const filteredCollection = useMemo<FeatureCollection>(
    () => ({ type: "FeatureCollection", features: filteredFeatures }),
    [filteredFeatures],
  );

  // Compute the layer bbox once, from the unfiltered FeatureCollection.
  // We use Leaflet's own geoJSON parser as a one-shot bbox engine so
  // the math handles every geometry type (Point, LineString, Polygon,
  // MultiPolygon, …) without us re-implementing recursive coordinate
  // walking. Passing the result via MapContainer's `bounds` prop is
  // the timing-safe way to fit the view: Leaflet does fitBounds at
  // mount, and *doesn't* re-fit on filter toggles (the user's pan/zoom
  // is preserved across filtering). That's why we read from `fc`, not
  // `filteredCollection`.
  const layerBounds = useMemo<L.LatLngBoundsExpression | null>(() => {
    if (!fc || fc.features.length === 0) return null;
    try {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const tmpLayer = L.geoJSON(fc as any);
      const b = tmpLayer.getBounds();
      if (!b.isValid()) return null;
      return b;
    } catch {
      return null;
    }
  }, [fc]);

  const totalCount = fc?.features.length ?? 0;
  const visibleCount = filteredFeatures.length;

  // Fullscreen toggle. When true the whole section is positioned
  // fixed over the viewport so the map + sidebar use all available
  // pixels. Escape exits. Leaflet caches its viewport size — we have
  // to call invalidateSize() after the container changes size,
  // otherwise the tiles render in a 500px box inside the larger
  // fullscreen area.
  const [isFullscreen, setIsFullscreen] = useState(false);
  const mapRef = useRef<L.Map | null>(null);
  useEffect(() => {
    if (!isFullscreen) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") setIsFullscreen(false);
    };
    document.addEventListener("keydown", onKey);
    // Lock background scroll so the user can't accidentally scroll the
    // dataset page underneath while the overlay is up.
    const prev = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => {
      document.removeEventListener("keydown", onKey);
      document.body.style.overflow = prev;
    };
  }, [isFullscreen]);
  useEffect(() => {
    // Defer one frame so the CSS layout settles to the new size before
    // Leaflet reads the container's dimensions. Without this, the
    // first invalidateSize() runs before the browser applied the
    // fullscreen styles and the map remains misaligned.
    if (!mapRef.current) return;
    const m = mapRef.current;
    const id = requestAnimationFrame(() => m.invalidateSize());
    return () => cancelAnimationFrame(id);
  }, [isFullscreen]);

  const toggleValue = (field: string, value: string) => {
    setFilters((prev) => {
      const next: FilterState = { ...prev };
      const set = new Set(next[field] ?? []);
      if (set.has(value)) set.delete(value);
      else set.add(value);
      next[field] = set;
      return next;
    });
  };

  const reset = () => setFilters({});

  // Fullscreen styles: cover the viewport, sit above headers, give the
  // map / sidebar 100% of the available height instead of the fixed
  // MAP_HEIGHT. Keeping the same JSX layout under both modes — only
  // the wrapping section's positioning and the children's height change.
  const sectionStyle: React.CSSProperties = isFullscreen
    ? {
        position: "fixed",
        inset: 0,
        zIndex: 1000,
        background: "var(--bg, #fff)",
        margin: 0,
        padding: "0.5rem",
        display: "flex",
        gap: "0.5rem",
        flexWrap: "wrap",
      }
    : {
        marginBottom: "1.5rem",
        display: "flex",
        gap: "0.75rem",
        flexWrap: "wrap",
      };
  const childHeight = isFullscreen ? "calc(100vh - 1rem)" : MAP_HEIGHT;

  return (
    <section aria-label={t("map.title")} style={sectionStyle}>
      {/* Map column. flex:1 with a sane minWidth so the sidebar wraps
          below the map on narrow viewports rather than fighting for
          width and clipping both. */}
      <div
        className="card"
        style={{
          flex: "1 1 600px",
          minWidth: 320,
          padding: 0,
          overflow: "hidden",
          height: childHeight,
          position: "relative",
        }}
      >
        {/* Fullscreen toggle floats inside the map card, top-left so it
            doesn't fight Leaflet's own zoom controls (top-right). */}
        <button
          type="button"
          onClick={() => setIsFullscreen((v) => !v)}
          style={{
            position: "absolute",
            top: "0.5rem",
            left: "0.5rem",
            zIndex: 500,
            background: "white",
            border: "1px solid var(--border, #cbd5e1)",
            borderRadius: 4,
            padding: "0.25rem 0.6rem",
            fontSize: "0.75rem",
            cursor: "pointer",
            boxShadow: "0 1px 2px rgba(0,0,0,0.15)",
          }}
          aria-label={isFullscreen ? t("map.fullscreen_exit") : t("map.fullscreen_enter")}
          title={isFullscreen ? t("map.fullscreen_exit") : t("map.fullscreen_enter")}
        >
          {isFullscreen ? t("map.fullscreen_exit") : t("map.fullscreen_enter")}
        </button>
        {loadError ? (
          <div
            role="alert"
            style={{
              padding: "1rem",
              color: "var(--danger, #b91c1c)",
              fontSize: "0.9rem",
            }}
          >
            {t("map.load_error")}
            <br />
            <a
              href={geojsonDownloadUrl}
              target="_blank"
              rel="noopener noreferrer"
              style={{ color: "var(--primary)" }}
            >
              {geojsonDownloadUrl}
            </a>
          </div>
        ) : !userOptedIn ? (
          <div
            role="status"
            style={{
              height: "100%",
              display: "flex",
              flexDirection: "column",
              alignItems: "center",
              justifyContent: "center",
              gap: "0.75rem",
              color: "var(--text-muted)",
              padding: "1rem",
              textAlign: "center",
            }}
          >
            <div style={{ fontWeight: 500, color: "var(--text)" }}>
              {t("map.mobile_gate_title")}
            </div>
            <div style={{ fontSize: "0.85rem", maxWidth: 280 }}>
              {t("map.mobile_gate_body")}
            </div>
            <button
              type="button"
              onClick={() => setUserOptedIn(true)}
              className="btn-primary"
              style={{
                padding: "0.4rem 1rem",
                fontSize: "0.9rem",
                marginTop: "0.25rem",
              }}
            >
              {t("map.mobile_gate_button")}
            </button>
          </div>
        ) : !fc ? (
          <div
            role="status"
            style={{
              height: "100%",
              display: "flex",
              flexDirection: "column",
              alignItems: "center",
              justifyContent: "center",
              gap: "0.75rem",
              color: "var(--text-muted)",
              padding: "1rem",
              textAlign: "center",
            }}
          >
            <div style={{ fontWeight: 500 }}>
              {parsing
                ? t("map.parsing")
                : downloadProgress !== null
                ? t("map.downloading", {
                    pct: Math.round(downloadProgress * 100),
                  })
                : t("common.loading")}
            </div>
            {downloadProgress !== null && !parsing && (
              <div
                aria-hidden
                style={{
                  width: "70%",
                  maxWidth: 320,
                  height: 4,
                  background: "var(--border, #e2e8f0)",
                  borderRadius: 2,
                  overflow: "hidden",
                }}
              >
                <div
                  style={{
                    height: "100%",
                    width: `${downloadProgress * 100}%`,
                    background: "var(--primary, #0f766e)",
                    transition: "width 120ms linear",
                  }}
                />
              </div>
            )}
          </div>
        ) : (
          <MapContainer
            // Bounds — not center/zoom — so the map is correctly
            // framed on the data at mount. Falls back to a country-
            // wide view only when the layer's bbox can't be
            // computed (genuinely empty collection or invalid geometry).
            // Passing bounds rather than calling fitBounds() in an
            // effect avoids the timing trap where the ref isn't yet
            // bound when fitBounds tries to run.
            bounds={layerBounds ?? undefined}
            boundsOptions={{ padding: [20, 20] }}
            center={layerBounds ? undefined : [31.78, 35.21]}
            zoom={layerBounds ? undefined : 8}
            style={{ height: "100%", width: "100%" }}
            scrollWheelZoom
            // Canvas renderer instead of the default SVG. For
            // datasets above a few hundred polygons this is a
            // massive win — SVG creates one DOM node per shape
            // (200,751 nodes for the agricultural-parcels layer kills
            // the browser), while canvas does the same work as N
            // drawcalls on a single bitmap and stays smooth.
            preferCanvas
            ref={(m) => {
              mapRef.current = m;
            }}
          >
            <TileLayer
              attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
              url="https://tile.openstreetmap.org/{z}/{x}/{y}.png"
              maxZoom={19}
            />
            <GeoJSON
              key={filterKey}
              data={filteredCollection}
              // smoothFactor goes inside the per-feature style
              // function — it's a Leaflet Path option, not a
              // top-level react-leaflet prop. Higher = fewer
              // vertices drawn at the current zoom = much less
              // canvas work, especially on mobile. Visually
              // indistinguishable on phones until the user zooms
              // way in.
              style={() => ({ ...layerStyle, smoothFactor: layerSmoothFactor })}
              ref={(layer) => {
                geojsonRef.current = layer;
              }}
              onEachFeature={(feature, layer) => {
                layer.bindPopup(() => renderPopup(feature, t));
              }}
            />
          </MapContainer>
        )}
      </div>

      {/* Filter sidebar. The visible/total counter sits at the top so
          the user can see the impact of their toggles without
          scrolling. Reset is at the top too — predictable. */}
      <aside
        className="card"
        style={{
          flex: "0 0 280px",
          height: childHeight,
          overflowY: "auto",
          padding: "0.75rem",
          fontSize: "0.85rem",
        }}
        aria-label={t("map.filters_title")}
      >
        <div
          className="flex-between"
          style={{ alignItems: "center", marginBottom: "0.5rem" }}
        >
          <strong>{t("map.filters_title")}</strong>
          <button
            type="button"
            onClick={reset}
            disabled={Object.values(filters).every((s) => s.size === 0)}
            style={{
              fontSize: "0.75rem",
              padding: "0.2rem 0.55rem",
              background: "none",
              border: "1px solid var(--border, #cbd5e1)",
              color: "var(--text-muted)",
              borderRadius: 4,
              cursor: "pointer",
            }}
          >
            {t("map.reset")}
          </button>
        </div>
        <div className="text-sm text-muted" style={{ marginBottom: "0.5rem" }}>
          {t("map.visible_count", { visible: visibleCount, total: totalCount })}
        </div>

        {/* Display controls — collapsed by default so the filter list
            stays the focus. Opens on click; lets the user override
            fill / stroke colour and opacity to make the layer pop
            against whatever basemap area they're inspecting. */}
        <StylePanel
          open={stylePanelOpen}
          onToggle={() => setStylePanelOpen((v) => !v)}
          style={layerStyle}
          onChange={setLayerStyle}
          onReset={() => setLayerStyle(DEFAULT_LAYER_STYLE)}
        />

        {/* Sort-mode toggle — applies to every field below. Default
            is by feature frequency; alpha is useful when the user
            knows the name they're looking for and can't be bothered
            to scan a long count-sorted list. */}
        {Object.keys(fieldCounts).length > 0 && (
          <div
            role="group"
            aria-label={t("map.sort_label")}
            style={{
              display: "flex",
              gap: "0.3rem",
              alignItems: "center",
              fontSize: "0.75rem",
              color: "var(--text-muted)",
              marginBottom: "0.5rem",
              flexWrap: "wrap",
            }}
          >
            <span>{t("map.sort_label")}</span>
            <SortToggle mode={sortMode} onChange={setSortMode} t={t} />
          </div>
        )}

        {fc && Object.keys(fieldCounts).length === 0 ? (
          <div className="text-sm text-muted">
            {t("map.no_filterable_fields")}
          </div>
        ) : (
          Object.entries(fieldCounts).map(([field, vals]) => (
            <FieldFilter
              key={field}
              field={field}
              values={vals}
              selected={filters[field] ?? new Set()}
              onToggle={(v) => toggleValue(field, v)}
              sortMode={sortMode}
            />
          ))
        )}
      </aside>
    </section>
  );
}

/** One field's checkbox group — sorted by descending count so the
 *  most common values are visible without scrolling. When the field
 *  has more than ``COLLAPSED_VISIBLE_VALUES`` distinct values, only
 *  the top-N show by default and a "הצג עוד (M)" toggle reveals the
 *  rest. Currently-selected values are ALWAYS shown so the user
 *  doesn't lose sight of their own filter state when a value is in
 *  the long tail of the distribution. */
function FieldFilter(props: {
  field: string;
  values: Record<string, number>;
  selected: ReadonlySet<string>;
  onToggle: (v: string) => void;
  sortMode: SortMode;
}) {
  const { t } = useTranslation();
  const { field, values, selected, onToggle, sortMode } = props;
  const [expanded, setExpanded] = useState(false);
  const entries = useMemo(() => {
    const pairs = Object.entries(values);
    if (sortMode === "alpha") {
      // Hebrew-locale collation for the natural alphabetical order;
      // count is the tiebreaker so duplicates stay deterministic.
      return pairs.sort(
        (a, b) => a[0].localeCompare(b[0], "he") || b[1] - a[1],
      );
    }
    // count (default): highest first, tiebreak by alpha so the order
    // is deterministic across renders.
    return pairs.sort(
      (a, b) => b[1] - a[1] || a[0].localeCompare(b[0], "he"),
    );
  }, [values, sortMode]);
  // Pin currently-selected values into the visible head so the user
  // can always see / untick them. Show top-N by count plus any
  // selected values that fall outside that head. Order: selected
  // ones surface at the very top, then unselected by count.
  const visibleEntries = useMemo(() => {
    if (expanded || entries.length <= COLLAPSED_VISIBLE_VALUES) return entries;
    const head = entries.slice(0, COLLAPSED_VISIBLE_VALUES);
    const headSet = new Set(head.map(([k]) => k));
    const extraSelected = entries.filter(
      ([k]) => selected.has(k) && !headSet.has(k),
    );
    return [...extraSelected, ...head];
  }, [entries, expanded, selected]);
  const hiddenCount = entries.length - visibleEntries.length;
  return (
    <fieldset
      style={{
        border: "1px solid var(--border, #e2e8f0)",
        borderRadius: 6,
        padding: "0.5rem 0.6rem",
        margin: "0 0 0.6rem 0",
      }}
    >
      <legend
        style={{
          fontSize: "0.75rem",
          fontWeight: 600,
          padding: "0 0.3rem",
          color: "var(--text-muted)",
        }}
      >
        {field}
      </legend>
      <div
        style={{ display: "flex", flexDirection: "column", gap: "0.15rem" }}
      >
        {visibleEntries.map(([v, n]) => {
          const checked = selected.has(v);
          return (
            // Block layout intentionally — no flex on the row. Earlier
            // experiments with `display:flex + flex:1 + wordBreak:
            // break-word` in this RTL parent were wrapping Hebrew text
            // character-by-character (one letter per line) because the
            // sidebar's effective width interacted badly with min-
            // content sizing. Plain block layout lets the value text
            // wrap at WORD boundaries like any normal paragraph.
            <label
              key={v}
              style={{
                display: "block",
                cursor: "pointer",
                fontWeight: checked ? 600 : 400,
                lineHeight: 1.4,
                marginBottom: "0.2rem",
              }}
              title={v}
            >
              <input
                type="checkbox"
                checked={checked}
                onChange={() => onToggle(v)}
                style={{
                  verticalAlign: "middle",
                  marginInlineEnd: "0.35rem",
                }}
              />
              <span style={{ verticalAlign: "middle" }}>
                {v}{" "}
                <span className="text-muted" style={{ fontSize: "0.75rem" }}>
                  ({n})
                </span>
              </span>
            </label>
          );
        })}
        {/* Show-more / show-less toggle. Only renders when there are
            actually hidden values; we don't want a noisy "show all"
            button on a 3-value field. */}
        {entries.length > COLLAPSED_VISIBLE_VALUES && (
          <button
            type="button"
            onClick={() => setExpanded((v) => !v)}
            style={{
              alignSelf: "flex-start",
              background: "none",
              border: "none",
              color: "var(--primary)",
              fontSize: "0.75rem",
              cursor: "pointer",
              padding: "0.15rem 0",
              marginTop: "0.15rem",
            }}
          >
            {expanded
              ? t("map.show_less")
              : t("map.show_more", { n: hiddenCount })}
          </button>
        )}
      </div>
    </fieldset>
  );
}

/** Two-button segmented control for the global filter sort mode. */
function SortToggle(props: {
  mode: SortMode;
  onChange: (m: SortMode) => void;
  t: (k: string) => string;
}) {
  const { mode, onChange, t } = props;
  const baseStyle: React.CSSProperties = {
    background: "none",
    border: "1px solid var(--border, #cbd5e1)",
    padding: "0.15rem 0.55rem",
    fontSize: "0.7rem",
    cursor: "pointer",
    color: "var(--text-muted)",
    borderRadius: 4,
  };
  const activeStyle: React.CSSProperties = {
    background: "var(--primary, #0f766e)",
    borderColor: "var(--primary, #0f766e)",
    color: "white",
  };
  return (
    <div style={{ display: "inline-flex", gap: "0.2rem" }}>
      <button
        type="button"
        onClick={() => onChange("count")}
        style={mode === "count" ? { ...baseStyle, ...activeStyle } : baseStyle}
        aria-pressed={mode === "count"}
      >
        {t("map.sort_count")}
      </button>
      <button
        type="button"
        onClick={() => onChange("alpha")}
        style={mode === "alpha" ? { ...baseStyle, ...activeStyle } : baseStyle}
        aria-pressed={mode === "alpha"}
      >
        {t("map.sort_alpha")}
      </button>
    </div>
  );
}

/** Collapsible "תצוגה" panel — lets the user override fill / stroke
 *  colour and opacity on the live layer. The actual GeoJSON layer
 *  picks the changes up via setStyle() in a useEffect on the parent,
 *  so dragging the opacity slider doesn't remount 200k features. */
function StylePanel(props: {
  open: boolean;
  onToggle: () => void;
  style: LayerStyle;
  onChange: (s: LayerStyle) => void;
  onReset: () => void;
}) {
  const { t } = useTranslation();
  const { open, onToggle, style, onChange, onReset } = props;
  return (
    <fieldset
      style={{
        border: "1px solid var(--border, #e2e8f0)",
        borderRadius: 6,
        padding: open ? "0.5rem 0.6rem" : "0.25rem 0.6rem",
        margin: "0 0 0.6rem 0",
      }}
    >
      <legend
        onClick={onToggle}
        style={{
          fontSize: "0.75rem",
          fontWeight: 600,
          padding: "0 0.3rem",
          color: "var(--text-muted)",
          cursor: "pointer",
        }}
      >
        {open ? "▾ " : "▸ "}
        {t("map.style_title")}
      </legend>
      {open && (
        <div
          style={{
            display: "flex",
            flexDirection: "column",
            gap: "0.5rem",
            fontSize: "0.75rem",
            color: "var(--text-muted)",
          }}
        >
          <StyleRow label={t("map.style_fill_color")}>
            <input
              type="color"
              value={style.fillColor}
              onChange={(e) => onChange({ ...style, fillColor: e.target.value })}
              style={{ width: 36, height: 24, border: "none", padding: 0 }}
            />
          </StyleRow>
          <StyleRow label={t("map.style_fill_opacity")}>
            <input
              type="range"
              min={0}
              max={100}
              step={1}
              value={Math.round(style.fillOpacity * 100)}
              onChange={(e) =>
                onChange({ ...style, fillOpacity: Number(e.target.value) / 100 })
              }
              style={{ flex: 1, minWidth: 0 }}
            />
            <span style={{ minWidth: 30, textAlign: "end" }}>
              {Math.round(style.fillOpacity * 100)}%
            </span>
          </StyleRow>
          <StyleRow label={t("map.style_stroke_color")}>
            <input
              type="color"
              value={style.color}
              onChange={(e) => onChange({ ...style, color: e.target.value })}
              style={{ width: 36, height: 24, border: "none", padding: 0 }}
            />
          </StyleRow>
          <StyleRow label={t("map.style_stroke_weight")}>
            <input
              type="range"
              min={0}
              max={5}
              step={0.5}
              value={style.weight}
              onChange={(e) =>
                onChange({ ...style, weight: Number(e.target.value) })
              }
              style={{ flex: 1, minWidth: 0 }}
            />
            <span style={{ minWidth: 30, textAlign: "end" }}>{style.weight}</span>
          </StyleRow>
          <button
            type="button"
            onClick={onReset}
            style={{
              alignSelf: "flex-start",
              background: "none",
              border: "1px solid var(--border, #cbd5e1)",
              color: "var(--text-muted)",
              fontSize: "0.7rem",
              padding: "0.15rem 0.5rem",
              borderRadius: 4,
              cursor: "pointer",
            }}
          >
            {t("map.style_reset")}
          </button>
        </div>
      )}
    </fieldset>
  );
}

/** Small label-and-control row inside the StylePanel. Keeps the
 *  spacing / typography consistent without repeating the same flex
 *  CSS at every input. */
function StyleRow(props: { label: string; children: React.ReactNode }) {
  return (
    <label
      style={{
        display: "flex",
        alignItems: "center",
        gap: "0.5rem",
        cursor: "default",
      }}
    >
      <span style={{ minWidth: 70 }}>{props.label}</span>
      {props.children}
    </label>
  );
}

/** Build the popup body HTML for one feature. Returns a DOM element
 *  Leaflet appends to its popup. Listing every property as a dl pair
 *  is the most useful default — users have asked for specific fields
 *  by name on different datasets, so committing to a curated subset
 *  would lock us into per-dataset config.
 *
 *  ``t`` is typed loosely (just key → string) because the i18next
 *  signature with overloads doesn't survive being passed as a value
 *  here — the popup only needs the simplest form. */
function renderPopup(
  feature: MinimalFeature,
  t: (k: string) => string,
): HTMLElement {
  const wrap = document.createElement("div");
  wrap.dir = "rtl";
  wrap.style.maxHeight = "240px";
  wrap.style.overflowY = "auto";
  wrap.style.minWidth = "180px";
  wrap.style.fontSize = "0.8rem";
  const props = feature.properties || {};
  const entries = Object.entries(props).filter(
    ([, v]) => v !== null && v !== undefined && v !== "",
  );
  if (entries.length === 0) {
    wrap.textContent = t("map.popup_no_data");
    return wrap;
  }
  const dl = document.createElement("dl");
  dl.style.margin = "0";
  dl.style.display = "grid";
  dl.style.gridTemplateColumns = "auto 1fr";
  dl.style.gap = "0.15rem 0.5rem";
  for (const [k, v] of entries) {
    const dt = document.createElement("dt");
    dt.textContent = k;
    dt.style.fontWeight = "600";
    dt.style.color = "var(--text-muted)";
    const dd = document.createElement("dd");
    dd.textContent = String(v);
    dd.style.margin = "0";
    dd.style.wordBreak = "break-word";
    dl.appendChild(dt);
    dl.appendChild(dd);
  }
  wrap.appendChild(dl);
  return wrap;
}
