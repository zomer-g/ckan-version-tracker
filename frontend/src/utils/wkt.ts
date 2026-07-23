/**
 * Minimal WKT → GeoJSON geometry parser for the /data console map.
 *
 * The console's spatial answers arrive as text: `ST_AsText(geom)` yields WKT,
 * which is also exactly what the raw `geometry_wkt` column holds. To draw that
 * on a map the browser has to turn it into GeoJSON, and no dependency here does
 * it — the dataset-page map is fed pre-built GeoJSON files, never WKT. So this
 * is that one missing step, deliberately small and dependency-free.
 *
 * Supports the seven OGC types plus EMPTY and trailing Z/M ordinates (which are
 * read and dropped — the map is 2-D). Coordinates are lon/lat, matching
 * everything the mirror stores (EPSG:4326). Returns null for anything it does
 * not understand rather than throwing, because it runs over user-supplied cells
 * and one bad value must not blank the whole map.
 */
export type GeoJsonGeometry =
  | { type: "Point"; coordinates: number[] }
  | { type: "LineString"; coordinates: number[][] }
  | { type: "Polygon"; coordinates: number[][][] }
  | { type: "MultiPoint"; coordinates: number[][] }
  | { type: "MultiLineString"; coordinates: number[][][] }
  | { type: "MultiPolygon"; coordinates: number[][][][] }
  | { type: "GeometryCollection"; geometries: GeoJsonGeometry[] };

// A cell already holding GeoJSON (ST_AsGeoJSON) — accept it as-is after a
// shape check, so the panel works whichever representation the user selected.
function tryGeoJson(s: string): GeoJsonGeometry | null {
  const t = s.trim();
  if (t[0] !== "{") return null;
  try {
    const o = JSON.parse(t);
    if (o && typeof o.type === "string" &&
        ("coordinates" in o || o.type === "GeometryCollection")) {
      return o as GeoJsonGeometry;
    }
  } catch {
    /* not JSON — fall through to WKT */
  }
  return null;
}

/** Split a comma-separated list of coordinate pairs: "34.7 32.0, 34.8 32.1". */
function parsePositions(body: string): number[][] {
  const out: number[][] = [];
  for (const pair of body.split(",")) {
    const nums = pair.trim().split(/\s+/).map(Number);
    if (nums.length < 2 || !Number.isFinite(nums[0]) || !Number.isFinite(nums[1])) {
      return []; // malformed → caller treats the whole geometry as unreadable
    }
    out.push([nums[0], nums[1]]); // drop any Z/M
  }
  return out;
}

/** Peel one balanced (...) group starting at `s[i]` (s[i] must be "("). Returns
 *  the inner text and the index just past the matching ")". */
function balanced(s: string, i: number): { inner: string; next: number } | null {
  if (s[i] !== "(") return null;
  let depth = 0;
  for (let j = i; j < s.length; j++) {
    if (s[j] === "(") depth++;
    else if (s[j] === ")") {
      depth--;
      if (depth === 0) return { inner: s.slice(i + 1, j), next: j + 1 };
    }
  }
  return null;
}

/** Split a group body into its top-level (...) sub-groups. */
function subGroups(body: string): string[] {
  const groups: string[] = [];
  let i = 0;
  while (i < body.length) {
    if (body[i] === "(") {
      const b = balanced(body, i);
      if (!b) break;
      groups.push(b.inner);
      i = b.next;
    } else {
      i++; // skip whitespace and commas between groups
    }
  }
  return groups;
}

export function wktToGeoJson(value: unknown): GeoJsonGeometry | null {
  if (typeof value !== "string") return null;
  const s = value.trim();
  if (!s) return null;

  const asJson = tryGeoJson(s);
  if (asJson) return asJson;

  // TYPE [Z|M|ZM] ( body )   — or   TYPE EMPTY
  const m = /^([A-Za-z]+)\s*(?:ZM?|M)?\s*(EMPTY|\()/i.exec(s);
  if (!m) return null;
  const type = m[1].toUpperCase();
  if (/EMPTY$/i.test(m[2])) return null; // empty geometry → nothing to draw

  const open = s.indexOf("(");
  const top = balanced(s, open);
  if (!top) return null;
  const body = top.inner;

  try {
    switch (type) {
      case "POINT": {
        const p = parsePositions(body);
        return p.length ? { type: "Point", coordinates: p[0] } : null;
      }
      case "LINESTRING": {
        const c = parsePositions(body);
        return c.length ? { type: "LineString", coordinates: c } : null;
      }
      case "MULTIPOINT": {
        // Two legal spellings: MULTIPOINT(1 2, 3 4) and MULTIPOINT((1 2),(3 4)).
        const inner = body.includes("(")
          ? subGroups(body).map((g) => parsePositions(g)[0]).filter(Boolean)
          : parsePositions(body);
        return inner.length ? { type: "MultiPoint", coordinates: inner } : null;
      }
      case "POLYGON": {
        const rings = subGroups(body).map(parsePositions).filter((r) => r.length);
        return rings.length ? { type: "Polygon", coordinates: rings } : null;
      }
      case "MULTILINESTRING": {
        const lines = subGroups(body).map(parsePositions).filter((l) => l.length);
        return lines.length ? { type: "MultiLineString", coordinates: lines } : null;
      }
      case "MULTIPOLYGON": {
        const polys = subGroups(body)
          .map((poly) => subGroups(poly).map(parsePositions).filter((r) => r.length))
          .filter((p) => p.length);
        return polys.length ? { type: "MultiPolygon", coordinates: polys } : null;
      }
      default:
        return null; // GEOMETRYCOLLECTION and unknowns: not drawn
    }
  } catch {
    return null;
  }
}

/** Heuristic: does this string look like a geometry we could draw? Cheap — used
 *  to pick the geometry column out of a result without parsing every cell. */
export function looksLikeGeometry(value: unknown): boolean {
  if (typeof value !== "string") return false;
  const s = value.trimStart();
  return /^(POINT|LINESTRING|POLYGON|MULTIPOINT|MULTILINESTRING|MULTIPOLYGON|GEOMETRYCOLLECTION)\s*[(Z M]/i.test(s)
    || (s[0] === "{" && /"type"\s*:/.test(s) && /"coordinates"|GeometryCollection/.test(s));
}
