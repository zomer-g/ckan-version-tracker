import { describe, expect, it } from "vitest";
import {
  applyFilters,
  discoverCategoricalFields,
  facetCounts,
  LABEL_MAX_LENGTH,
  SMALL_LAYER_MAX_FEATURES,
  truncateLabel,
  type MinimalFeature,
} from "./geoFilters";

/** Shape of dataset 30c1da3c: a chain of river segments where each
 *  segment's `lowerpoint` is the next segment's `point`. Section
 *  names are long Hebrew strings, points are all distinct. */
function riverChain(): MinimalFeature[] {
  const points = ["א", "ב", "ג", "ד", "ה", "ו", "ז", "ח", "ט", "י", "כ", "ל"];
  return points.slice(0, 11).map((p, i) => ({
    type: "Feature",
    properties: {
      river: i < 6 ? "נהר הירדן" : i < 9 ? "נחל יהודייה" : "נחל משושים",
      section: `מקטע ${i + 1}: קטע ארוך מאוד של הנחל מנקודה ${p} ועד נקודה ${points[i + 1]}`,
      point: `נקודה ${p}`,
      lowerpoint: `נקודה ${points[i + 1]}`,
      result: i % 2 === 0 ? "1100" : "900",
      objectid: String(1000 + i),
      globalid: `{A1B2C3D4-0000-4000-8000-${String(i).padStart(12, "0")}}`,
    },
  }));
}

function feats(rows: Record<string, unknown>[]): MinimalFeature[] {
  return rows.map((properties) => ({ type: "Feature", properties }));
}

describe("discoverCategoricalFields", () => {
  it("keeps long values instead of dropping them", () => {
    const fields = discoverCategoricalFields(riverChain());
    expect(Object.keys(fields["section"] ?? {})).toHaveLength(11);
  });

  it("shows all-distinct text fields when the layer is small", () => {
    const fields = discoverCategoricalFields(riverChain());
    expect(Object.keys(fields["point"] ?? {})).toHaveLength(11);
    expect(Object.keys(fields["lowerpoint"] ?? {})).toHaveLength(11);
  });

  it("still drops identifier-looking all-distinct fields on a small layer", () => {
    const fields = discoverCategoricalFields(riverChain());
    expect(fields["objectid"]).toBeUndefined();
    expect(fields["globalid"]).toBeUndefined();
  });

  it("drops all-distinct fields on a large layer", () => {
    const n = SMALL_LAYER_MAX_FEATURES + 1;
    const rows = Array.from({ length: n }, (_, i) => ({
      name: `שם ${i}`,
      kind: i % 2 ? "a" : "b",
    }));
    const fields = discoverCategoricalFields(feats(rows));
    expect(fields["name"]).toBeUndefined();
    expect(fields["kind"]).toEqual({ a: Math.floor(n / 2), b: Math.ceil(n / 2) });
  });

  it("drops single-valued fields and non-string values", () => {
    const fields = discoverCategoricalFields(
      feats([
        { same: "x", num: 1, kind: "a" },
        { same: "x", num: 2, kind: "b" },
      ]),
    );
    expect(fields["same"]).toBeUndefined();
    expect(fields["num"]).toBeUndefined();
    expect(fields["kind"]).toEqual({ a: 1, b: 1 });
  });
});

describe("truncateLabel", () => {
  it("returns short values unchanged", () => {
    expect(truncateLabel("נהר הירדן")).toBe("נהר הירדן");
  });
  it("truncates long values with an ellipsis", () => {
    const long = "א".repeat(LABEL_MAX_LENGTH + 20);
    const out = truncateLabel(long);
    expect(out.length).toBeLessThanOrEqual(LABEL_MAX_LENGTH + 1);
    expect(out.endsWith("…")).toBe(true);
  });
});

describe("facetCounts", () => {
  const features = riverChain();
  const base = discoverCategoricalFields(features);

  it("equals the base counts when nothing is selected", () => {
    expect(facetCounts(features, base, {})).toEqual(base);
  });

  it("counts each field against the OTHER fields' selection, with zeros kept", () => {
    const filters = { section: new Set([features[6].properties!["section"] as string]) };
    const faceted = facetCounts(features, base, filters);
    // The selected field keeps its own full counts so the user can
    // still switch to a sibling value.
    expect(faceted["section"]).toEqual(base["section"]);
    // Other fields are narrowed to the one matching segment: one
    // value at 1, every other value present at 0.
    const lower = faceted["lowerpoint"];
    expect(Object.keys(lower)).toEqual(Object.keys(base["lowerpoint"]));
    const nonZero = Object.entries(lower).filter(([, n]) => n > 0);
    expect(nonZero).toEqual([[features[6].properties!["lowerpoint"], 1]]);
    expect(faceted["river"]).toEqual({ "נהר הירדן": 0, "נחל יהודייה": 1, "נחל משושים": 0 });
  });

  it("applies two other fields jointly (AND)", () => {
    const filters = {
      river: new Set(["נהר הירדן"]),
      result: new Set(["1100"]),
    };
    const faceted = facetCounts(features, base, filters);
    const pointsLeft = Object.values(faceted["point"]).reduce((a, b) => a + b, 0);
    // Segments 0, 2, 4 are on the Jordan with result 1100.
    expect(pointsLeft).toBe(3);
    // `river` is narrowed only by `result`: 1100 is on 6 of 11 rows.
    expect(faceted["river"]["נהר הירדן"]).toBe(3);
    expect(faceted["river"]["נחל יהודייה"]).toBe(2);
    expect(faceted["river"]["נחל משושים"]).toBe(1);
  });

  it("applies selected fields missing from the base counts, as the map does", () => {
    const filters = { nosuch: new Set(["x"]) };
    const faceted = facetCounts(features, base, filters);
    expect(Object.values(faceted["river"]).reduce((a, b) => a + b, 0)).toBe(0);
  });
});

describe("applyFilters", () => {
  it("is OR within a field and AND across fields", () => {
    const features = riverChain();
    const or = applyFilters(features, { river: new Set(["נהר הירדן", "נחל משושים"]) });
    expect(or).toHaveLength(8);
    const and = applyFilters(features, {
      river: new Set(["נהר הירדן"]),
      result: new Set(["900"]),
    });
    expect(and).toHaveLength(3);
  });
  it("treats an empty set as no constraint", () => {
    const features = riverChain();
    expect(applyFilters(features, { river: new Set() })).toHaveLength(11);
  });
});
