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
  // We fit bounds ONCE per dataset load. Subsequent filter changes
  // must not jump the viewport — the user has presumably panned/zoomed
  // already and would be furious to lose it.
  const [hasFitBounds, setHasFitBounds] = useState(false);
  const mapRef = useRef<L.Map | null>(null);

  useEffect(() => {
    let cancelled = false;
    fetch(geojsonDownloadUrl)
      .then(async (resp) => {
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        const data = (await resp.json()) as FeatureCollection;
        if (!cancelled) setFc(data);
      })
      .catch((e) => {
        if (!cancelled) setLoadError(String(e?.message ?? e));
      });
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

  // GeoJSON layer mount callback — wires popup binding and (only on
  // first load) the auto-fit. We do the fit here, not in a ref-based
  // useEffect, because the layer's bounds aren't available until the
  // shapes are added to the map.
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const onLayerCreated = (layer: any) => {
    if (!hasFitBounds && mapRef.current && layer && layer.getBounds) {
      try {
        const bounds = layer.getBounds();
        if (bounds.isValid()) {
          mapRef.current.fitBounds(bounds, { padding: [20, 20] });
          setHasFitBounds(true);
        }
      } catch {
        // getBounds throws on empty FeatureCollections — fine to
        // silently skip; the user can pan/zoom manually.
      }
    }
  };

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
            // Tel Aviv-ish initial view; immediately overridden by
            // fitBounds after the layer mounts. The default tile
            // placeholders during the brief gap are less jarring than
            // a blank canvas at zoom=2.
            center={[31.78, 35.21]}
            zoom={8}
            style={{ height: "100%", width: "100%" }}
            ref={(m) => {
              mapRef.current = m;
            }}
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
                onLayerCreated(layer);
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
              }}
            >
              <input
                type="checkbox"
                checked={checked}
                onChange={() => onToggle(v)}
              />
              <span
                style={{
                  flex: 1,
                  whiteSpace: "nowrap",
                  overflow: "hidden",
                  textOverflow: "ellipsis",
                }}
                title={v}
              >
                {v}
              </span>
              <span className="text-muted" style={{ fontSize: "0.75rem" }}>
                {n}
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
