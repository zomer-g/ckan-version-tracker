/**
 * Public-facing page at ``/growth``.
 *
 * Renders the most recent version of the agricultural-parcels layer
 * (tracked dataset 9574d100-…) restricted to the curated subset
 * defined in ``config/growthLayers.ts``. The user toggles individual
 * layers (citrus, avocado, …) with independent checkboxes and can
 * switch the basemap between OpenStreetMap and Esri World Imagery
 * (aerial). No filter discovery, no version list — this is a
 * focussed view, not the generic dataset page.
 *
 * Data flow:
 *   1. Hit the public ``/api/v1/datasets/{id}/versions`` endpoint
 *      (no auth) for the dataset's version index.
 *   2. Take the latest version's GeoJSON resource URL from its
 *      ``resources[]`` entry where ``format === "GeoJSON"``.
 *   3. Stream-download the body once (gzipped, cached as Blob).
 *   4. Stream-parse ``$.features.*`` once, splitting features into
 *      per-layer arrays based on the layer's ``growthname`` matcher.
 *      Features that don't match any layer are discarded — this is a
 *      curated view, not a "show everything" view.
 *
 * Memory cost: linear in the matched feature count, not in the full
 * 200 K-feature layer. The citrus + avocado subset is a small
 * fraction of the source, so even iOS Safari handles it cleanly.
 */
import { useEffect, useMemo, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { MapContainer, TileLayer, GeoJSON, LayersControl } from "react-leaflet";
import L from "leaflet";
import "leaflet/dist/leaflet.css";
import iconUrl from "leaflet/dist/images/marker-icon.png";
import iconRetinaUrl from "leaflet/dist/images/marker-icon-2x.png";
import shadowUrl from "leaflet/dist/images/marker-shadow.png";
import { downloadToBlob, parseStream } from "../utils/geoStream";
import {
  DATASET_ID,
  GROWTH_LAYERS,
  GrowthLayer,
  featureMatchesLayer,
} from "../config/growthLayers";
import type { MinimalFeature } from "../utils/geoFilters";

// Rebind the default marker icon paths — same workaround GovmapView
// uses. Polygons (the common case) ignore this, but if a future
// layer carries Point geometries we don't want broken icons.
// eslint-disable-next-line @typescript-eslint/no-explicit-any
delete (L.Icon.Default.prototype as any)._getIconUrl;
L.Icon.Default.mergeOptions({ iconUrl, iconRetinaUrl, shadowUrl });

interface VersionDetail {
  id: string;
  version_number: number;
  detected_at: string;
  resources: Array<{
    name: string;
    odata_resource_id: string;
    odata_resource_url: string;
    download_url: string;
    format: string | null;
  }>;
}

const MAP_HEIGHT = 600;

type LayerFeatures = Record<string, MinimalFeature[]>;

export default function GrowthPage() {
  const { t, i18n } = useTranslation();
  const isHe = i18n.language === "he";

  // Which version we're showing (for the "latest as of …" caption).
  const [version, setVersion] = useState<VersionDetail | null>(null);
  // Per-layer feature lists, built once at parse time.
  const [layerFeatures, setLayerFeatures] = useState<LayerFeatures | null>(
    null,
  );
  const [loadError, setLoadError] = useState<string | null>(null);
  // Progress reporter so the loading placeholder shows something
  // useful instead of a silent spinner — the GeoJSON is ~50 MB
  // gzipped on slow networks.
  const [downloadProgress, setDownloadProgress] = useState<number | null>(
    null,
  );
  const [parsing, setParsing] = useState(false);

  // Active layer toggles. Both on by default so the user sees the
  // curated view immediately.
  const [activeLayerIds, setActiveLayerIds] = useState<Set<string>>(
    () => new Set(GROWTH_LAYERS.map((l) => l.id)),
  );

  // Pass 1: fetch version index, find GeoJSON download URL, stream
  // and split into per-layer arrays.
  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const resp = await fetch(`/api/v1/datasets/${DATASET_ID}/versions`);
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        const versions = (await resp.json()) as VersionDetail[];
        if (cancelled) return;
        if (!versions.length) throw new Error("no versions");
        const latest = versions[0];
        const geojsonResource = latest.resources.find(
          (r) => r.format === "GeoJSON",
        );
        if (!geojsonResource) throw new Error("no GeoJSON resource");
        setVersion(latest);

        const cached = await downloadToBlob({
          url: geojsonResource.download_url,
          isCancelled: () => cancelled,
          onProgress: setDownloadProgress,
        });
        if (cancelled) return;
        setDownloadProgress(1);
        setParsing(true);
        // Yield once so the parse spinner paints before we tie the
        // main thread up inside the parser.
        await new Promise((r) => setTimeout(r, 0));
        if (cancelled) return;

        // Initialise per-layer buckets.
        const buckets: LayerFeatures = {};
        for (const layer of GROWTH_LAYERS) buckets[layer.id] = [];
        await parseStream<MinimalFeature>({
          blob: cached.blob,
          isGz: cached.isGz,
          path: "$.features.*",
          onValue: (f) => {
            const props = (f.properties || null) as
              | Record<string, unknown>
              | null;
            for (const layer of GROWTH_LAYERS) {
              if (featureMatchesLayer(layer, props)) {
                buckets[layer.id].push(f);
                break; // each polygon belongs to at most one layer in v1
              }
            }
          },
          isCancelled: () => cancelled,
        });
        if (cancelled) return;
        setLayerFeatures(buckets);
        setParsing(false);
        setDownloadProgress(null);
      } catch (e) {
        if (cancelled) return;
        const msg = (e as Error)?.message ?? String(e);
        if (msg !== "cancelled") setLoadError(msg);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  // Compute the bbox once, from every loaded layer's features — so
  // toggling a layer doesn't snap the viewport around.
  const layerBounds = useMemo<L.LatLngBoundsExpression | null>(() => {
    if (!layerFeatures) return null;
    const all: MinimalFeature[] = [];
    for (const id of Object.keys(layerFeatures)) {
      all.push(...layerFeatures[id]);
    }
    if (all.length === 0) return null;
    try {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const tmp = L.geoJSON({ type: "FeatureCollection", features: all } as any);
      const b = tmp.getBounds();
      return b.isValid() ? b : null;
    } catch {
      return null;
    }
  }, [layerFeatures]);

  const toggleLayer = (id: string) => {
    setActiveLayerIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  // Total / per-layer counts for the legend.
  const layerCounts = useMemo(() => {
    if (!layerFeatures) return {} as Record<string, number>;
    const out: Record<string, number> = {};
    for (const id of Object.keys(layerFeatures)) {
      out[id] = layerFeatures[id].length;
    }
    return out;
  }, [layerFeatures]);

  return (
    <div className="container mt-3">
      <div className="page-header">
        <h1 style={{ margin: 0 }}>{t("growth.title")}</h1>
        <div className="text-sm text-muted" style={{ marginTop: "0.25rem" }}>
          {t("growth.subtitle")}
          {version && (
            <>
              {" · "}
              {t("growth.version_caption", {
                n: version.version_number,
                date: new Date(version.detected_at).toLocaleDateString(
                  isHe ? "he-IL" : "en-US",
                ),
              })}
            </>
          )}
        </div>
      </div>

      <section
        aria-label={t("growth.title")}
        style={{
          marginTop: "1rem",
          display: "flex",
          flexDirection: "column",
          gap: "0.75rem",
        }}
      >
        {/* Layer legend / toggle. Above the map (not in a sidebar) so
            it stays usable on phones without horizontal scroll. */}
        <div
          className="card"
          style={{
            display: "flex",
            flexWrap: "wrap",
            gap: "1rem",
            padding: "0.75rem 1rem",
            alignItems: "center",
          }}
        >
          {GROWTH_LAYERS.map((layer) => {
            const active = activeLayerIds.has(layer.id);
            const count = layerCounts[layer.id] ?? null;
            return (
              <label
                key={layer.id}
                style={{
                  display: "inline-flex",
                  alignItems: "center",
                  gap: "0.4rem",
                  cursor: "pointer",
                  fontWeight: active ? 600 : 400,
                }}
              >
                <input
                  type="checkbox"
                  checked={active}
                  onChange={() => toggleLayer(layer.id)}
                />
                <span
                  aria-hidden
                  style={{
                    display: "inline-block",
                    width: 14,
                    height: 14,
                    background: layer.color,
                    borderRadius: 2,
                    border: "1px solid rgba(0,0,0,0.15)",
                  }}
                />
                <span>{isHe ? layer.labelHe : layer.labelEn}</span>
                {count !== null && (
                  <span
                    className="text-muted"
                    style={{ fontSize: "0.8rem" }}
                  >
                    ({count.toLocaleString()})
                  </span>
                )}
              </label>
            );
          })}
        </div>

        {/* Map card. */}
        <div
          className="card"
          style={{
            padding: 0,
            overflow: "hidden",
            height: MAP_HEIGHT,
            position: "relative",
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
              {t("growth.load_error")}: {loadError}
            </div>
          ) : !layerFeatures ? (
            <LoadingPlaceholder
              t={t}
              parsing={parsing}
              progress={downloadProgress}
            />
          ) : (
            <MapContainer
              bounds={layerBounds ?? undefined}
              boundsOptions={{ padding: [20, 20] }}
              center={layerBounds ? undefined : [31.78, 35.21]}
              zoom={layerBounds ? undefined : 8}
              style={{ height: "100%", width: "100%" }}
              scrollWheelZoom
              // Canvas renderer keeps performance acceptable even
              // when both layers are toggled on with thousands of
              // polygons each. SVG would balloon the DOM and stall
              // the page on every pan.
              preferCanvas
            >
              <LayersControl position="topright">
                <LayersControl.BaseLayer checked name={t("growth.basemap_osm")}>
                  <TileLayer
                    attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
                    url="https://tile.openstreetmap.org/{z}/{x}/{y}.png"
                    maxZoom={19}
                  />
                </LayersControl.BaseLayer>
                <LayersControl.BaseLayer name={t("growth.basemap_aerial")}>
                  {/* Esri World Imagery — free, no API key, no
                      rate-limited tile key, attribution required.
                      Coverage in Israel is recent and high-resolution. */}
                  <TileLayer
                    attribution='Tiles &copy; Esri &mdash; Source: Esri, Maxar, Earthstar Geographics, and the GIS User Community'
                    url="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}"
                    maxZoom={19}
                  />
                </LayersControl.BaseLayer>
              </LayersControl>

              {GROWTH_LAYERS.filter((l) => activeLayerIds.has(l.id)).map(
                (layer) => (
                  <GeoJSON
                    key={layer.id}
                    data={{
                      type: "FeatureCollection",
                      features: layerFeatures[layer.id],
                      // eslint-disable-next-line @typescript-eslint/no-explicit-any
                    } as any}
                    style={() => ({
                      color: layer.color,
                      weight: 1,
                      fillColor: layer.color,
                      fillOpacity: 0.45,
                      smoothFactor: 1.5,
                    })}
                    onEachFeature={(feature, leafletLayer) => {
                      leafletLayer.bindPopup(() =>
                        renderPopup(feature, layer, isHe),
                      );
                    }}
                  />
                ),
              )}
            </MapContainer>
          )}
        </div>
      </section>
    </div>
  );
}

function LoadingPlaceholder(props: {
  t: (k: string, opts?: Record<string, unknown>) => string;
  parsing: boolean;
  progress: number | null;
}) {
  const { t, parsing, progress } = props;
  return (
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
          ? t("growth.parsing")
          : progress !== null
          ? t("growth.downloading", { pct: Math.round(progress * 100) })
          : t("common.loading")}
      </div>
      {progress !== null && !parsing && (
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
              width: `${progress * 100}%`,
              background: "var(--primary, #0f766e)",
              transition: "width 120ms linear",
            }}
          />
        </div>
      )}
    </div>
  );
}

/**
 * Popup body for one polygon. Header line names the layer the polygon
 * belongs to (so the user can recognise it without checking the
 * legend); the rest is a definition list of every non-empty property.
 */
function renderPopup(
  feature: MinimalFeature,
  layer: GrowthLayer,
  isHe: boolean,
): HTMLElement {
  const wrap = document.createElement("div");
  wrap.dir = isHe ? "rtl" : "ltr";
  wrap.style.maxHeight = "260px";
  wrap.style.overflowY = "auto";
  wrap.style.minWidth = "200px";
  wrap.style.fontSize = "0.8rem";

  const header = document.createElement("div");
  header.textContent = isHe ? layer.labelHe : layer.labelEn;
  header.style.fontWeight = "700";
  header.style.color = layer.color;
  header.style.marginBottom = "0.4rem";
  header.style.fontSize = "0.9rem";
  wrap.appendChild(header);

  const props = feature.properties || {};
  const entries = Object.entries(props).filter(
    ([, v]) => v !== null && v !== undefined && v !== "",
  );
  if (entries.length === 0) return wrap;
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
