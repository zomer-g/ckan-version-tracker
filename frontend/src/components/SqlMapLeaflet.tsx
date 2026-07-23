/**
 * The Leaflet half of the /data console map. Kept in its own module and
 * lazy-imported by SqlChartPanel so the ~150 KB of Leaflet only loads when a
 * user actually opens the map view — every other console visit pays nothing.
 *
 * Renders one GeoJSON layer over tiles. Points become canvas circleMarkers (via
 * pointToLayer) rather than the default DOM <img> markers, so a thousand points
 * don't stall the page; polygons and lines ride the same canvas renderer under
 * preferCanvas. Each feature gets a popup listing its non-geometry columns —
 * i.e. the row that produced it.
 *
 * Per-feature colour arrives stamped on `properties.__color` by
 * utils/mapFeatures, so all styling decisions live with the panel that owns the
 * settings UI and this module just draws what it is handed.
 */
import { useEffect } from "react";
import { MapContainer, TileLayer, GeoJSON, useMap } from "react-leaflet";
import L from "leaflet";
import "leaflet/dist/leaflet.css";
import type { MapFeatureCollection } from "../utils/mapFeatures";

export type Basemap = "streets" | "satellite" | "none";

// Same tile endpoints GovmapView and GrowthPage already use. NOT the {s}
// subdomain form (a./b./c.) — OSM retired it, and it fails SILENTLY: shapes
// draw fine over a blank grey background, which reads as a styling bug rather
// than a dead tile host.
const TILES: Record<Exclude<Basemap, "none">, { url: string; attr: string }> = {
  streets: {
    url: "https://tile.openstreetmap.org/{z}/{x}/{y}.png",
    attr: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
  },
  satellite: {
    url: "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
    attr: "Esri, Maxar, Earthstar Geographics",
  },
};

const DEFAULT_COLOR = "#15803d";

function colorOf(props: Record<string, unknown> | undefined): string {
  const c = props?.__color;
  return typeof c === "string" ? c : DEFAULT_COLOR;
}

function FitBounds({ fc }: { fc: MapFeatureCollection }) {
  const map = useMap();
  useEffect(() => {
    try {
      const layer = L.geoJSON(fc as unknown as GeoJSON.GeoJsonObject);
      const b = layer.getBounds();
      if (b.isValid()) map.fitBounds(b, { padding: [24, 24], maxZoom: 15 });
    } catch {
      /* an unfittable collection just leaves the default view */
    }
  }, [fc, map]);
  return null;
}

function popupHtml(props: Record<string, unknown>): string {
  const rows = Object.entries(props)
    .filter(([k, v]) => k !== "__color" && v !== null && v !== undefined && v !== "")
    .slice(0, 20)
    .map(([k, v]) => {
      const key = String(k).replace(/</g, "&lt;");
      const val = String(v).replace(/</g, "&lt;").slice(0, 200);
      return `<div style="margin:.1rem 0"><b>${key}:</b> ${val}</div>`;
    })
    .join("");
  return `<div dir="rtl" style="font-size:.8rem;max-height:220px;overflow:auto">${rows || "—"}</div>`;
}

export default function SqlMapLeaflet({
  fc,
  basemap = "streets",
  fillOpacity = 0.2,
  pointRadius = 6,
  height = 460,
}: {
  fc: MapFeatureCollection;
  basemap?: Basemap;
  fillOpacity?: number;
  pointRadius?: number;
  height?: number;
}) {
  const tiles = basemap === "none" ? null : TILES[basemap];
  return (
    <MapContainer
      preferCanvas
      center={[31.7, 35.0]}
      zoom={7}
      style={{ height, width: "100%", borderRadius: 6, background: "var(--bg-muted, #eef2f5)" }}
      scrollWheelZoom
    >
      {tiles && <TileLayer url={tiles.url} attribution={tiles.attr} maxZoom={19} />}
      <GeoJSON
        // Remount when the data or the styling knobs change — Leaflet's GeoJSON
        // layer reads style/pointToLayer once, at creation.
        key={`${fc.features.length}:${fillOpacity}:${pointRadius}:${
          fc.features.map((f) => f.properties.__color).join("")
        }`}
        data={fc as unknown as GeoJSON.GeoJsonObject}
        style={(f) => {
          const c = colorOf(f?.properties as Record<string, unknown>);
          return { color: c, weight: 1.6, fillColor: c, fillOpacity };
        }}
        pointToLayer={(f, latlng) => {
          const c = colorOf(f?.properties as Record<string, unknown>);
          return L.circleMarker(latlng, {
            // Points sit ON TOP of the polygons they fall inside, so they get a
            // white ring — otherwise a site inside a filled municipality is
            // invisible against it.
            radius: pointRadius, color: "#fff", weight: 2,
            fillColor: c, fillOpacity: 1,
          });
        }}
        onEachFeature={(f, layer) => {
          const props = (f.properties || {}) as Record<string, unknown>;
          layer.bindPopup(() => popupHtml(props), { maxWidth: 320 });
        }}
      />
      <FitBounds fc={fc} />
    </MapContainer>
  );
}
