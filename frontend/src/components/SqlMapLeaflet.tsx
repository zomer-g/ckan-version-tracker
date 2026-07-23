/**
 * The Leaflet half of the /data console map. Kept in its own module and
 * lazy-imported by SqlMapPanel so the ~150 KB of Leaflet only loads when a user
 * with a spatial result actually opens the map — every other console visit pays
 * nothing.
 *
 * Renders one GeoJSON layer over OSM tiles. Points become canvas circleMarkers
 * (via pointToLayer) rather than the default DOM <img> markers, so a thousand
 * points don't stall the page; polygons and lines ride the same canvas renderer
 * under preferCanvas. Each feature gets a popup listing its non-geometry
 * columns — i.e. the row that produced it.
 */
import { useEffect } from "react";
import { MapContainer, TileLayer, GeoJSON, useMap } from "react-leaflet";
import L from "leaflet";
import "leaflet/dist/leaflet.css";

export interface MapFeature {
  type: "Feature";
  geometry: Record<string, unknown>;
  properties: Record<string, unknown>;
}
export interface MapFeatureCollection {
  type: "FeatureCollection";
  features: MapFeature[];
}

const STROKE = "#15803d";
const FILL = "#22c55e";

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
    .filter(([, v]) => v !== null && v !== undefined && v !== "")
    .slice(0, 20)
    .map(([k, v]) => {
      const key = String(k).replace(/</g, "&lt;");
      const val = String(v).replace(/</g, "&lt;").slice(0, 200);
      return `<div style="margin:.1rem 0"><b>${key}:</b> ${val}</div>`;
    })
    .join("");
  return `<div dir="rtl" style="font-size:.8rem;max-height:220px;overflow:auto">${rows || "—"}</div>`;
}

export default function SqlMapLeaflet({ fc }: { fc: MapFeatureCollection }) {
  return (
    <MapContainer
      preferCanvas
      center={[31.7, 35.0]}
      zoom={7}
      style={{ height: 460, width: "100%", borderRadius: 6 }}
      scrollWheelZoom
    >
      <TileLayer
        url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        attribution='&copy; OpenStreetMap'
      />
      <GeoJSON
        key={fc.features.length + "-" + JSON.stringify(fc.features[0]?.geometry ?? {}).length}
        data={fc as unknown as GeoJSON.GeoJsonObject}
        style={() => ({ color: STROKE, weight: 1.5, fillColor: FILL, fillOpacity: 0.25 })}
        pointToLayer={(_f, latlng) =>
          L.circleMarker(latlng, {
            radius: 5, color: STROKE, weight: 1.5, fillColor: FILL, fillOpacity: 0.7,
          })
        }
        onEachFeature={(f, layer) => {
          const props = (f.properties || {}) as Record<string, unknown>;
          layer.bindPopup(() => popupHtml(props), { maxWidth: 320 });
        }}
      />
      <FitBounds fc={fc} />
    </MapContainer>
  );
}
