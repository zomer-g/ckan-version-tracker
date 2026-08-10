/**
 * The map half of נדל"ן לעם. Kept in its own module and lazy-imported by
 * NadlanPage so Leaflet's ~150 KB only loads for someone who actually opens the
 * map tab — the same reason SqlMapLeaflet and GovmapView are split out.
 *
 * Click anywhere to drop a pin; the page turns that into a lookup. A radius
 * larger than 0 draws the circle being searched, so what the query means is
 * visible rather than implied. Result parcels are canvas circleMarkers (not DOM
 * markers) because a 2 km radius can return a couple hundred of them, and the
 * selected parcel's real polygon is drawn on top when the page hands one over.
 */
import { useEffect } from "react";
import { MapContainer, TileLayer, GeoJSON, Circle, CircleMarker, useMap, useMapEvents } from "react-leaflet";

import "leaflet/dist/leaflet.css";
import type { GeoJsonObject } from "geojson";
import type { NadlanProperty } from "../../api/client";

// Same tile endpoints the rest of the site uses. NOT the {s} subdomain form —
// OSM retired it and it fails SILENTLY (shapes over a blank grey background).
const TILES = {
  streets: {
    url: "https://tile.openstreetmap.org/{z}/{x}/{y}.png",
    attr: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
  },
  satellite: {
    url: "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
    attr: "Esri, Maxar, Earthstar Geographics",
  },
} as const;

export type Basemap = keyof typeof TILES;

function ClickHandler({ onPick }: { onPick: (lat: number, lon: number) => void }) {
  useMapEvents({ click: (e) => onPick(e.latlng.lat, e.latlng.lng) });
  return null;
}

/** Recentre when the caller moves the pin (e.g. from a gush/helka lookup). */
function Recenter({ lat, lon, zoom }: { lat: number | null; lon: number | null; zoom?: number }) {
  const map = useMap();
  useEffect(() => {
    if (lat != null && lon != null) map.setView([lat, lon], zoom ?? map.getZoom());
  }, [lat, lon, zoom, map]);
  return null;
}

export default function NadlanMap({
  lat, lon, radiusM, results, selected, polygon, basemap = "streets", onPick,
}: {
  lat: number | null;
  lon: number | null;
  radiusM: number;
  results: NadlanProperty[];
  selected: string | null;
  // Explicit `geojson` type: the react-leaflet <GeoJSON> import above shadows
  // the global GeoJSON namespace, so the bare form would resolve elsewhere.
  polygon: GeoJsonObject | null;
  basemap?: Basemap;
  onPick: (lat: number, lon: number) => void;
}) {
  const tile = TILES[basemap];
  const center: [number, number] = [lat ?? 32.0853, lon ?? 34.7818];

  return (
    <MapContainer
      center={center}
      zoom={lat != null ? 17 : 8}
      preferCanvas
      style={{ height: 460, width: "100%", borderRadius: 8, border: "1px solid var(--border,#e2e8f0)" }}
    >
      <TileLayer url={tile.url} attribution={tile.attr} maxZoom={19} />
      <ClickHandler onPick={onPick} />
      <Recenter lat={lat} lon={lon} />

      {lat != null && lon != null && (
        <CircleMarker
          center={[lat, lon]}
          radius={7}
          pathOptions={{ color: "#b91c1c", fillColor: "#ef4444", fillOpacity: 0.9, weight: 2 }}
        />
      )}
      {lat != null && lon != null && radiusM > 0 && (
        <Circle
          center={[lat, lon]}
          radius={radiusM}
          pathOptions={{ color: "#0f766e", fillOpacity: 0.06, weight: 1 }}
        />
      )}

      {results.map((p) =>
        p.identity.point ? (
          <CircleMarker
            key={p.parcel_key}
            center={[p.identity.point.lat, p.identity.point.lon]}
            radius={p.parcel_key === selected ? 8 : 5}
            pathOptions={{
              color: p.parcel_key === selected ? "#0f766e" : "#15803d",
              fillOpacity: 0.75,
              weight: p.parcel_key === selected ? 3 : 1,
            }}
            eventHandlers={{
              click: () => onPick(p.identity.point!.lat, p.identity.point!.lon),
            }}
          />
        ) : null,
      )}

      {polygon && (
        <GeoJSON
          key={selected ?? "poly"}
          data={polygon}
          style={() => ({ color: "#0f766e", weight: 2, fillOpacity: 0.12 })}
        />
      )}
    </MapContainer>
  );
}
