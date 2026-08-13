"""SLD → ArcGIS Pro (.lyrx) conversion.

The archived GovMap symbology is an OGC SLD; ArcGIS cannot read one. What
matters here is not that a file comes out, but that the file says the SAME
cartography: the classifying FIELD and its values, the colours, the sizes in
the units ArcGIS actually uses, and the markers — including the SVG pins, which
ArcGIS refuses to accept as pictures and which therefore have to arrive as
geometry or not at all.

Everything a rule expresses that CIM cannot must surface as a warning: a
silently dropped rule is a style that looks converted and draws the wrong map.

Pure functions over bytes — no DB, no network.
"""
import io
import json
import os
import sys
import zipfile

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest  # noqa: E402

from app.services import lyrx  # noqa: E402


def _sld(body: str, ns: str = "") -> str:
    """Wrap rule XML in an SLD envelope, optionally namespace-prefixed."""
    p = f"{ns}:" if ns else ""
    xmlns = (f'xmlns:{ns}="http://www.opengis.net/sld"' if ns
             else 'xmlns="http://www.opengis.net/sld"')
    return (
        f'<?xml version="1.0" encoding="utf-8"?>'
        f'<{p}StyledLayerDescriptor version="1.0.0" {xmlns} '
        f'xmlns:ogc="http://www.opengis.net/ogc" '
        f'xmlns:xlink="http://www.w3.org/1999/xlink">'
        f'<{p}NamedLayer><{p}Name>שכבה</{p}Name><{p}UserStyle>'
        f'<{p}Title>שכבה</{p}Title><{p}FeatureTypeStyle>{body}'
        f'</{p}FeatureTypeStyle></{p}UserStyle></{p}NamedLayer>'
        f'</{p}StyledLayerDescriptor>'
    )


POLY_RULE = """
<Rule><Name>אילת</Name><Title>אילת</Title>
  <ogc:Filter><ogc:Or>
    <ogc:PropertyIsEqualTo><ogc:PropertyName>name_h</ogc:PropertyName>
      <ogc:Literal>אילת</ogc:Literal></ogc:PropertyIsEqualTo>
    <ogc:PropertyIsEqualTo><ogc:PropertyName>name_h</ogc:PropertyName>
      <ogc:Literal>אילות</ogc:Literal></ogc:PropertyIsEqualTo>
  </ogc:Or></ogc:Filter>
  <PolygonSymbolizer><Fill>
    <CssParameter name="fill">#fcc9bd</CssParameter>
    <CssParameter name="fill-opacity">0.5</CssParameter>
  </Fill></PolygonSymbolizer>
  <PolygonSymbolizer><Stroke>
    <CssParameter name="stroke">#004da8</CssParameter>
    <CssParameter name="stroke-width">2</CssParameter>
  </Stroke></PolygonSymbolizer>
</Rule>
<Rule><Title>אשדוד</Title>
  <ogc:Filter><ogc:PropertyIsEqualTo><ogc:PropertyName>name_h</ogc:PropertyName>
    <ogc:Literal>אשדוד</ogc:Literal></ogc:PropertyIsEqualTo></ogc:Filter>
  <PolygonSymbolizer><Fill>
    <CssParameter name="fill">#b3edfc</CssParameter></Fill></PolygonSymbolizer>
</Rule>
"""

SINGLE_LINE_RULE = """
<Rule><Title>קו</Title><LineSymbolizer><Stroke>
  <CssParameter name="stroke">#7ab6f5</CssParameter>
  <CssParameter name="stroke-width">4</CssParameter>
  <CssParameter name="stroke-linecap">round</CssParameter>
  <CssParameter name="stroke-dasharray">8 4</CssParameter>
</Stroke></LineSymbolizer></Rule>
"""

RANGE_RULES = """
<Rule><Title>1</Title>
  <ogc:Filter><ogc:PropertyIsLessThanOrEqualTo>
    <ogc:PropertyName>strm_order</ogc:PropertyName><ogc:Literal>1</ogc:Literal>
  </ogc:PropertyIsLessThanOrEqualTo></ogc:Filter>
  <LineSymbolizer><Stroke><CssParameter name="stroke">#111111</CssParameter>
  </Stroke></LineSymbolizer></Rule>
<Rule><Title>2</Title>
  <ogc:Filter><ogc:And>
    <ogc:PropertyIsGreaterThan><ogc:PropertyName>strm_order</ogc:PropertyName>
      <ogc:Literal>1</ogc:Literal></ogc:PropertyIsGreaterThan>
    <ogc:PropertyIsLessThanOrEqualTo><ogc:PropertyName>strm_order</ogc:PropertyName>
      <ogc:Literal>3</ogc:Literal></ogc:PropertyIsLessThanOrEqualTo>
  </ogc:And></ogc:Filter>
  <LineSymbolizer><Stroke><CssParameter name="stroke">#222222</CssParameter>
  </Stroke></LineSymbolizer></Rule>
"""

POINT_RULE = """
<Rule><Title>נקודה</Title><PointSymbolizer><Graphic>
  <ExternalGraphic>
    <OnlineResource xlink:href="icons/pin_1a2b3c4d.{ext}"/>
    <Format>image/{fmt}</Format>
  </ExternalGraphic>
  <Opacity>1.0</Opacity><Size>28</Size><Rotation>0.0</Rotation>
</Graphic></PointSymbolizer></Rule>
"""

# A GovMap pin: an outer circle path plus a glyph, the whole thing coloured by
# the URL query. `param(fill)` is what an UNBAKED archive holds.
SVG_PIN = (
    '<svg version="1.2" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 28 28">'
    '<style>.s1 { fill: #ffffff }</style>'
    '<mask id="hideStrokeInside"><rect x="0" y="0" width="28" height="28" fill="white"/>'
    '<circle cx="14" cy="14" r="11.5" fill="black"/></mask>'
    '<path id="Circle" fill="FILL" d="m14 25.5c-6.4 0-11.5-5.1-11.5-11.5 '
    '0-6.4 5.1-11.5 11.5-11.5 6.4 0 11.5 5.1 11.5 11.5 0 6.4-5.1 11.5-11.5 11.5z"/>'
    '<path id="Icon" class="s1" d="m7 14h14v3h-14z"/>'
    '</svg>'
)

PNG = (b"\x89PNG\r\n\x1a\n" + b"\x00" * 40)


def _convert(body: str, **kw):
    return lyrx.convert_sld(_sld(body), name="layer", **kw)


def _layer(doc: dict) -> dict:
    return doc["layerDefinitions"][0]


# ── the document itself ────────────────────────────────────────────────


def test_document_is_a_valid_lyrx_skeleton():
    doc, _ = _convert(SINGLE_LINE_RULE)
    assert doc["type"] == "CIMLayerDocument"
    assert doc["version"] == lyrx.LYRX_VERSION
    # The layer must be reachable from the document's `layers` index, or Pro
    # opens a file with nothing in it.
    assert doc["layers"] == [_layer(doc)["uRI"]]
    assert _layer(doc)["type"] == "CIMFeatureLayer"
    # Round-trips as UTF-8 JSON (Hebrew names included).
    assert json.loads(lyrx.lyrx_bytes(doc).decode("utf-8"))["layers"]


def test_namespace_prefixed_sld_is_read_the_same_way():
    # Roughly half of GovMap's catalog serves `sld:`-prefixed elements.
    plain, _ = lyrx.convert_sld(_sld(POLY_RULE), name="layer")
    prefixed, _ = lyrx.convert_sld(_sld(POLY_RULE, ns="sld"), name="layer")
    assert (_layer(prefixed)["renderer"]["fields"]
            == _layer(plain)["renderer"]["fields"] == ["name_h"])


def test_a_non_sld_document_is_rejected_rather_than_half_converted():
    with pytest.raises(lyrx.LyrxError):
        lyrx.convert_sld("<html><body>Govmap</body></html>", name="x")
    with pytest.raises(lyrx.LyrxError):
        lyrx.convert_sld("<StyledLayerDescriptor", name="x")


# ── renderers ──────────────────────────────────────────────────────────


def test_equality_rules_become_a_unique_value_renderer_on_that_field():
    doc, warnings = _convert(POLY_RULE)
    r = _layer(doc)["renderer"]
    assert r["type"] == "CIMUniqueValueRenderer"
    assert r["fields"] == ["name_h"]
    classes = r["groups"][0]["classes"]
    assert [c["label"] for c in classes] == ["אילת", "אשדוד"]
    # An Or of equalities is ONE class holding both values, not two classes.
    assert [v["fieldValues"][0] for v in classes[0]["values"]] == ["אילת", "אילות"]
    assert not warnings


def test_numeric_range_rules_become_class_breaks():
    doc, _ = _convert(RANGE_RULES)
    r = _layer(doc)["renderer"]
    assert r["type"] == "CIMClassBreaksRenderer"
    assert r["field"] == "strm_order"
    assert [b["upperBound"] for b in r["breaks"]] == [1.0, 3.0]


def test_one_unfiltered_rule_becomes_a_simple_renderer():
    doc, _ = _convert(SINGLE_LINE_RULE)
    assert _layer(doc)["renderer"]["type"] == "CIMSimpleRenderer"


def test_an_else_rule_becomes_the_default_symbol():
    body = POLY_RULE + """
    <Rule><Title>אחר</Title><ElseFilter/>
      <PolygonSymbolizer><Fill><CssParameter name="fill">#cccccc</CssParameter>
      </Fill></PolygonSymbolizer></Rule>"""
    doc, _ = _convert(body)
    r = _layer(doc)["renderer"]
    assert r["useDefaultSymbol"] is True
    assert r["defaultSymbol"]["symbol"]["symbolLayers"][0]["color"]["values"][:3] == [204, 204, 204]


def test_a_filter_arcgis_cannot_express_is_reported_not_dropped_quietly():
    body = POLY_RULE + """
    <Rule><Title>לא ידוע</Title>
      <ogc:Filter><ogc:PropertyIsNull><ogc:PropertyName>name_h</ogc:PropertyName>
      </ogc:PropertyIsNull></ogc:Filter>
      <PolygonSymbolizer><Fill><CssParameter name="fill">#000000</CssParameter>
      </Fill></PolygonSymbolizer></Rule>"""
    doc, warnings = _convert(body)
    assert _layer(doc)["renderer"]["type"] == "CIMUniqueValueRenderer"
    assert any("לא ידוע" in w for w in warnings)


# ── symbols ────────────────────────────────────────────────────────────


def test_fill_and_stroke_survive_with_opacity_and_point_units():
    doc, _ = _convert(POLY_RULE)
    sym = _layer(doc)["renderer"]["groups"][0]["classes"][0]["symbol"]["symbol"]
    assert sym["type"] == "CIMPolygonSymbol"
    stroke, fill = sym["symbolLayers"]
    assert stroke["type"] == "CIMSolidStroke"
    # 2 CSS px at 96 dpi is 1.5 points — ArcGIS sizes are points, always.
    assert stroke["width"] == pytest.approx(1.5)
    assert fill["color"]["values"] == [252, 201, 189, 50.0]


def test_dashes_and_caps_reach_the_stroke():
    doc, _ = _convert(SINGLE_LINE_RULE)
    stroke = _layer(doc)["renderer"]["symbol"]["symbol"]["symbolLayers"][0]
    assert stroke["capStyle"] == "Round"
    assert stroke["width"] == pytest.approx(3.0)
    assert stroke["effects"][0]["dashTemplate"] == [6.0, 3.0]


def test_a_raster_icon_is_embedded_as_a_picture_marker():
    doc, warnings = _convert(POINT_RULE.format(ext="png", fmt="png"),
                             icons={"icons/pin_1a2b3c4d.png": PNG})
    marker = _layer(doc)["renderer"]["symbol"]["symbol"]["symbolLayers"][0]
    assert marker["type"] == "CIMPictureMarker"
    assert marker["url"].startswith("data:image/png;base64,")
    assert marker["size"] == pytest.approx(21.0)  # 28 px → 21 pt
    assert not warnings


def test_an_svg_icon_becomes_geometry_because_arcgis_rejects_svg_pictures():
    doc, _ = _convert(
        POINT_RULE.format(ext="svg", fmt="svg+xml"),
        icons={"icons/pin_1a2b3c4d.svg": SVG_PIN.replace("FILL", "#0000FF").encode()})
    marker = _layer(doc)["renderer"]["symbol"]["symbol"]["symbolLayers"][0]
    assert marker["type"] == "CIMVectorMarker"
    graphics = marker["markerGraphics"]
    # The pin's circle and its glyph — and NOT the <mask>'s rectangle, which
    # would paint over the whole marker.
    assert len(graphics) == 2
    assert graphics[0]["symbol"]["symbolLayers"][0]["color"]["values"][:3] == [0, 0, 255]
    assert all(len(ring) > 2 for g in graphics for ring in g["geometry"]["rings"])


def test_an_unbaked_svg_says_the_colour_was_lost_instead_of_inventing_one():
    # GovMap serves its pins as templates; an archive taken before the colour
    # was baked in has no colour to convert (see the scraper's bake_svg_params).
    doc, warnings = _convert(
        POINT_RULE.format(ext="svg", fmt="svg+xml"),
        icons={"icons/pin_1a2b3c4d.svg": SVG_PIN.replace("FILL", "param(fill)").encode()})
    marker = _layer(doc)["renderer"]["symbol"]["symbol"]["symbolLayers"][0]
    assert marker["type"] == "CIMVectorMarker"
    assert any("param(fill)" in w for w in warnings)


def test_a_missing_icon_degrades_to_a_circle_and_says_so():
    doc, warnings = _convert(POINT_RULE.format(ext="png", fmt="png"), icons={})
    marker = _layer(doc)["renderer"]["symbol"]["symbol"]["symbolLayers"][0]
    assert marker["type"] == "CIMVectorMarker"
    assert any("חסר בחבילה" in w for w in warnings)


def test_a_hatched_polygon_becomes_a_hatch_fill_at_the_right_angle():
    body = """
    <Rule><Title>קווקוו</Title><PolygonSymbolizer><Fill><GraphicFill><Graphic>
      <Mark><WellKnownName>shape://backslash</WellKnownName>
        <Stroke><CssParameter name="stroke">#006064</CssParameter></Stroke>
      </Mark><Size>8</Size></Graphic></GraphicFill></Fill></PolygonSymbolizer></Rule>"""
    doc, _ = _convert(body)
    fill = _layer(doc)["renderer"]["symbol"]["symbol"]["symbolLayers"][0]
    assert fill["type"] == "CIMHatchFill"
    assert fill["rotation"] == 135.0
    assert fill["lineSymbol"]["symbolLayers"][0]["color"]["values"][:3] == [0, 96, 100]


def test_labels_become_a_label_class_with_an_arcade_expression():
    body = """
    <Rule><Title>שם</Title>
      <PolygonSymbolizer><Fill><CssParameter name="fill">#ffffff</CssParameter>
      </Fill></PolygonSymbolizer>
      <TextSymbolizer>
        <Label><ogc:PropertyName>fname</ogc:PropertyName></Label>
        <Font><CssParameter name="font-family">Arial</CssParameter>
          <CssParameter name="font-size">12.0</CssParameter></Font>
        <Halo><Radius>2</Radius><Fill>
          <CssParameter name="fill">#ffffff</CssParameter></Fill></Halo>
        <Fill><CssParameter name="fill">#000000</CssParameter></Fill>
      </TextSymbolizer></Rule>"""
    doc, _ = _convert(body)
    layer = _layer(doc)
    lc = layer["labelClasses"][0]
    assert lc["expression"] == "$feature.fname"
    assert lc["expressionEngine"] == "Arcade"
    assert layer["labelVisibility"] is True
    text = lc["textSymbol"]["symbol"]
    assert text["fontFamilyName"] == "Arial"
    assert text["height"] == 12.0
    assert text["haloSize"] == pytest.approx(1.5)


def test_scale_denominators_land_on_the_layer_the_way_arcgis_reads_them():
    body = """
    <Rule><Title>קרוב</Title>
      <MinScaleDenominator>1000</MinScaleDenominator>
      <MaxScaleDenominator>50000</MaxScaleDenominator>
      <LineSymbolizer><Stroke><CssParameter name="stroke">#000000</CssParameter>
      </Stroke></LineSymbolizer></Rule>"""
    doc, _ = _convert(body)
    layer = _layer(doc)
    # SLD's *Min*ScaleDenominator is the zoomed-IN limit, which ArcGIS calls
    # maxScale. Swapping these hides the layer exactly where it should draw.
    assert layer["maxScale"] == 1000
    assert layer["minScale"] == 50000


# ── the bundle ─────────────────────────────────────────────────────────


def _bundle(**extra) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("שכבה_19.sld", _sld(POINT_RULE.format(ext="png", fmt="png")))
        z.writestr("icons/pin_1a2b3c4d.png", PNG)
        z.writestr("symbology_index.csv", "layer_id\r\n19\r\n")
        z.writestr("שכבה_fields.csv", "machine_name\r\nname_h\r\n")
        for name, data in extra.items():
            z.writestr(name, data)
    return buf.getvalue()


def test_bundle_conversion_keeps_the_documentation_beside_the_layer_file():
    out = zipfile.ZipFile(io.BytesIO(lyrx.convert_bundle(_bundle())))
    names = out.namelist()
    assert "שכבה_19.lyrx" in names
    # The field dictionary decodes the machine names the renderer classifies on
    # — it has to travel with the .lyrx, not stay behind in the SLD bundle.
    assert "שכבה_fields.csv" in names
    assert "symbology_index.csv" in names
    assert "icons/pin_1a2b3c4d.png" in names
    readme = out.read("README_ArcGIS.txt").decode("utf-8")
    assert "Apply Symbology From Layer" in readme
    assert json.loads(out.read("שכבה_19.lyrx"))["type"] == "CIMLayerDocument"


def test_conversion_warnings_reach_the_reader_in_the_readme():
    bundle = io.BytesIO()
    with zipfile.ZipFile(bundle, "w") as z:
        z.writestr("x.sld", _sld(POINT_RULE.format(ext="png", fmt="png")))  # no icon
    readme = zipfile.ZipFile(
        io.BytesIO(lyrx.convert_bundle(bundle.getvalue()))
    ).read("README_ArcGIS.txt").decode("utf-8")
    assert "הערות המרה" in readme
    assert "חסר בחבילה" in readme


def test_a_bundle_with_nothing_convertible_fails_loudly():
    empty = io.BytesIO()
    with zipfile.ZipFile(empty, "w") as z:
        z.writestr("readme.txt", "no styles here")
    with pytest.raises(lyrx.LyrxError):
        lyrx.convert_bundle(empty.getvalue())
    with pytest.raises(lyrx.LyrxError):
        lyrx.convert_bundle(b"not a zip at all")
