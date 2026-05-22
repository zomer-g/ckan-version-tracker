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
import { useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { MapContainer, TileLayer, GeoJSON } from "react-leaflet";
import L from "leaflet";
import "leaflet/dist/leaflet.css";

import {
  applyFilters,
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
// Light green fill (same palette family as the IDF badge so the
// design language stays coherent across non-CKAN sources). The fill
// is intentionally faint so polygons in close proximity still read
// as distinct.
const LAYER_STYLE = {
  color: "#0f766e",
  weight: 1,
  fillColor: "#5d936c",
  fillOpacity: 0.25,
};

export default function GovmapView({ geojsonDownloadUrl }: GovmapViewProps) {
  const { t } = useTranslation();
  const [fc, setFc] = useState<FeatureCollection | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [filters, setFilters] = useState<FilterState>({});

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const resp = await fetch(geojsonDownloadUrl);
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        // Large GeoJSON layers (~200MB+) are stored gzipped on odata
        // because CKAN resource_create rejects plain bodies above
        // ~100MB. odata's /download route serves them WITHOUT the
        // filename suffix and WITHOUT a Content-Encoding header, so
        // detecting by URL or headers isn't reliable. Instead we
        // sniff the body's first two bytes: gzip is unambiguously
        // 0x1F 0x8B and a JSON document can never begin with those.
        const buf = await resp.arrayBuffer();
        const bytes = new Uint8Array(buf);
        const isGz =
          bytes.length >= 2 && bytes[0] === 0x1f && bytes[1] === 0x8b;
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
        const data = JSON.parse(text) as FeatureCollection;
        if (!cancelled) setFc(data);
      } catch (e) {
        if (!cancelled) setLoadError(String((e as Error)?.message ?? e));
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [geojsonDownloadUrl]);

  // The full set of feature properties → distinct value counts. Doesn't
  // change when the user toggles filters — the chip list itself stays
  // stable; only the visible-feature counter and checkbox states move.
  const fieldCounts = useMemo(
    () => (fc ? discoverCategoricalFields(fc.features) : {}),
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

  return (
    <section
      aria-label={t("map.title")}
      style={{
        marginBottom: "1.5rem",
        display: "flex",
        gap: "0.75rem",
        flexWrap: "wrap",
      }}
    >
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
          height: MAP_HEIGHT,
        }}
      >
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
        ) : !fc ? (
          <div
            role="status"
            style={{
              height: "100%",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              color: "var(--text-muted)",
            }}
          >
            {t("common.loading")}
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
          >
            <TileLayer
              attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
              url="https://tile.openstreetmap.org/{z}/{x}/{y}.png"
              maxZoom={19}
            />
            <GeoJSON
              key={filterKey}
              data={filteredCollection}
              style={() => LAYER_STYLE}
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
          height: MAP_HEIGHT,
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
            />
          ))
        )}
      </aside>
    </section>
  );
}

/** One field's checkbox group — sorted by descending count so the
 *  most common values are visible without scrolling. */
function FieldFilter(props: {
  field: string;
  values: Record<string, number>;
  selected: ReadonlySet<string>;
  onToggle: (v: string) => void;
}) {
  const { field, values, selected, onToggle } = props;
  const entries = useMemo(
    () =>
      Object.entries(values).sort((a, b) =>
        b[1] - a[1] || a[0].localeCompare(b[0], "he"),
      ),
    [values],
  );
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
        {entries.map(([v, n]) => {
          const checked = selected.has(v);
          return (
            <label
              key={v}
              style={{
                display: "flex",
                alignItems: "center",
                gap: "0.4rem",
                cursor: "pointer",
                fontWeight: checked ? 600 : 400,
                lineHeight: 1.3,
              }}
              title={v}
            >
              <input
                type="checkbox"
                checked={checked}
                onChange={() => onToggle(v)}
                style={{ flexShrink: 0 }}
              />
              {/* Single span with value + count. Earlier I split them
                  into two flex children with the value in a flex:1
                  ellipsis-truncated span; in an RTL parent the value
                  visually collapsed to invisible while the count
                  remained, hiding all labels. Combining them removes
                  the failure mode and reads naturally either way:
                  "זיתים (16)". */}
              <span
                style={{
                  flex: 1,
                  minWidth: 0,
                  wordBreak: "break-word",
                }}
              >
                {v}{" "}
                <span className="text-muted" style={{ fontSize: "0.75rem" }}>
                  ({n})
                </span>
              </span>
            </label>
          );
        })}
      </div>
    </fieldset>
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
