/**
 * Topology-preserving simplification for GeoJSON FeatureCollections.
 *
 * Why topology, not plain Douglas-Peucker:
 *   When two adjacent polygons share a border (e.g. the boundary
 *   between two planning regions), per-feature simplification of each
 *   polygon independently produces *different* simplified borders for
 *   each side — visually a "river" of background colour opens between
 *   them. The topojson pipeline solves this by converting to TopoJSON
 *   first (which stores shared arcs once), simplifying the arcs, and
 *   converting back to GeoJSON. Shared edges stay shared.
 *
 * Pipeline:
 *   GeoJSON  --topology()--->  TopoJSON
 *   TopoJSON  --presimplify()--->  TopoJSON with arc weights
 *   TopoJSON --simplify(w)--->  reduced TopoJSON
 *   TopoJSON --feature()--->  GeoJSON
 *
 * The "weight" of an arc point is the area of the triangle formed by
 * it and its two neighbours; below the chosen threshold the point is
 * dropped. ``quantile`` lets us pick a threshold that keeps a target
 * fraction of points without us hard-coding a CRS-dependent absolute
 * value (the agricultural-parcels layer is in WGS84 degrees, others
 * could in theory ship in different projections).
 */
import { topology } from "topojson-server";
import { feature } from "topojson-client";
import { presimplify, simplify, quantile } from "topojson-simplify";
import type { Feature, FeatureCollection, GeoJsonObject } from "geojson";

/** Skip simplification when the layer is bigger than this. The
 *  topology-building step is O(vertex) and on a 200K-feature layer
 *  it would freeze the main thread for several seconds — worse than
 *  the rendering cost we're trying to reduce. For small layers
 *  (mostly admin regions, ~hundreds of features) topology building
 *  is fast and the rendering win is huge. */
export const SIMPLIFY_MAX_FEATURES = 30000;

/** Fraction of arc points to drop. 0.5 keeps the heaviest 50% of
 *  points (those whose triangle area is in the top half); 0.7 is
 *  more aggressive. The visual difference at typical map zoom is
 *  imperceptible until you zoom way in past the level the source
 *  data was collected at. */
const DEFAULT_KEEP_FRACTION = 0.5;

/**
 * Simplify a FeatureCollection in-place-friendly way. Returns either
 * the simplified collection or the original input untouched — caller
 * doesn't need to distinguish the two paths.
 *
 * Failure modes:
 *   - Feature count > SIMPLIFY_MAX_FEATURES → returns input unchanged
 *   - topojson library throws (geometry it can't represent, e.g.
 *     malformed rings) → caught, returns input unchanged
 *   - Empty collection → returns input unchanged
 */
export function simplifyFeatureCollection(
  fc: FeatureCollection,
  keepFraction: number = DEFAULT_KEEP_FRACTION,
): FeatureCollection {
  if (!fc.features || fc.features.length === 0) return fc;
  if (fc.features.length > SIMPLIFY_MAX_FEATURES) return fc;
  try {
    // topojson-server wants a record of GeoJSON objects; we stuff
    // the whole collection under a single key. Cast through any
    // because @types/topojson-simplify constrains object properties
    // to ``{}`` while @types/topojson-server emits the looser
    // ``GeoJsonProperties`` (which permits null). Functionally
    // compatible; the type mismatch is only at the TS surface.
    const topo = topology({ data: fc as GeoJsonObject });
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const pre = presimplify(topo as any);
    const tolerance = quantile(pre, keepFraction);
    if (!Number.isFinite(tolerance)) return fc;
    const reduced = simplify(pre, tolerance);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const out = feature(reduced, reduced.objects.data as any) as Feature | FeatureCollection;
    // ``feature`` returns either a single Feature or a FeatureCollection
    // depending on the source object's type. We always pass a
    // GeometryCollection-shaped wrapper, so we always get a FeatureCollection
    // back — but TypeScript can't prove that, hence the narrow.
    if (out && (out as FeatureCollection).type === "FeatureCollection") {
      return out as FeatureCollection;
    }
    return fc;
  } catch {
    return fc;
  }
}
