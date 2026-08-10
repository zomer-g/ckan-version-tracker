/**
 * The map half of נדל"ן לעם. Kept in its own module and lazy-imported by
 * NadlanPage so Leaflet's ~150 KB only loads for someone who actually opens the
 * map tab — the same reason SqlMapLeaflet and GovmapView are split out.
 *
 * Click anywhere to drop a pin; the page turns that into a lookup. A radius
 * larger than 0 draws the circle being searched, so what the query means is
 * visible rather than implied.
 *
 * Results are drawn as their REAL parcel outlines, whichever tab produced them —
 * a property found by zip or by address is as locatable here as one found by
 * clicking the map. A centroid marker is only the fallback for a parcel whose
 * polygon is missing. When the search carried no map pin, the view fits itself
 * to the results instead of stranding them off-screen.
 */
import { useEffect, useMemo } from "react";
import { MapContainer, TileLayer, GeoJSON, Circle, CircleMarker, useMap, useMapEvents } from "react-leaflet";
import L from "leaflet";

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

/** Fit the view to the results when the search had no pin of its own — an
 *  address or zip lookup otherwise leaves its parcels outside the viewport. */
function FitToResults({ results, active }: { results: NadlanProperty[]; active: boolean }) {
  const map = useMap();
  const sig = results.map((r) => r.parcel_key).join(",");
  useEffect(() => {
    if (!active || !results.length) return;
    const layer = L.geoJSON();
    let any = false;
    for (const r of results) {
      if (r.geometry) {
        try { layer.addData(JSON.parse(r.geometry)); any = true; } catch { /* skip */ }
      } else if (r.identity.point) {
        layer.addData({
          type: "Feature", properties: {},
          geometry: { type: "Point", coordinates: [r.identity.point.lon, r.identity.point.lat] },
        } as GeoJSON.Feature);
        any = true;
      }
    }
    if (!any) return;
    const b = layer.getBounds();
    if (b.isValid()) map.fitBounds(b, { padding: [30, 30], maxZoom: 18 });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sig, active, map]);
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
  lat, lon, radiusM, results, selected, polygon, basemap = "streets", onPick, onSelect,
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
  onSelect?: (parcelKey: string) => void;
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
      <FitToResults results={results} active={lat == null || lon == null} />

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

      {/* Every result's real parcel outline — this is what makes a property
          locatable ON the map rather than merely represented by a dot. Falls
          back to a centroid marker only where the polygon is missing. */}
      {results.map((p) => {
        const isSel = p.parcel_key === selected;
        if (p.geometry) {
          return (
            <GeoJSON
              key={`${p.parcel_key}:${isSel}`}
              data={JSON.parse(p.geometry) as GeoJsonObject}
              style={() => ({
                color: isSel ? "#0f766e" : "#15803d",
                weight: isSel ? 3 : 1.5,
                fillOpacity: isSel ? 0.28 : 0.1,
              })}
              eventHandlers={{ click: () => onSelect?.(p.parcel_key) }}
            />
          );
        }
        return p.identity.point ? (
          <CircleMarker
            key={p.parcel_key}
            center={[p.identity.point.lat, p.identity.point.lon]}
            radius={isSel ? 8 : 5}
            pathOptions={{
              color: isSel ? "#0f766e" : "#15803d",
              fillOpacity: 0.75,
              weight: isSel ? 3 : 1,
            }}
            eventHandlers={{ click: () => onSelect?.(p.parcel_key) }}
          />
        ) : null;
      })}

      {/* The high-detail outline of the selected parcel, when the page fetched
          one separately (a finer simplification than the bulk layer). */}
      {polygon && (
        <GeoJSON
          key={selected ?? "poly"}
          data={polygon}
          style={() => ({ color: "#0f766e", weight: 3, fillOpacity: 0.2 })}
        />
      )}
    </MapContainer>
  );
}
