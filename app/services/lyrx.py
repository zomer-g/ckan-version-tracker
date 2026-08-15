"""GovMap symbology → ArcGIS Pro layer file (``.lyrx``).

The archive stores every GovMap layer's cartography as the OGC **SLD** GovMap
itself serves (see the scraper's ``govmap/symbology.py``). QGIS reads that
directly; **ArcGIS reads nothing of it**. ArcGIS Pro's own interchange format is
a ``.lyrx`` — a JSON document in Esri's CIM (Cartographic Information Model) —
so a reader on ArcGIS has to redraw a 688-rule land-use style by hand, which in
practice means the archived symbology is unusable to them.

This module converts one SLD into one CIM layer document. It runs INSIDE OVER,
on the bundle already sitting in R2, rather than in the scraper: that way every
version ever archived gains an ArcGIS download without re-scraping ~870 layers,
and there is one implementation to keep honest instead of two.

What maps onto what
-------------------
=====================================  =====================================
SLD                                    CIM (.lyrx)
=====================================  =====================================
one unfiltered ``Rule``                ``CIMSimpleRenderer``
``Rule``s filtered by ``=`` on a field ``CIMUniqueValueRenderer``
``Rule``s filtered by numeric ranges   ``CIMClassBreaksRenderer``
``PolygonSymbolizer``                  ``CIMPolygonSymbol`` (fill+stroke layers)
``LineSymbolizer``                     ``CIMLineSymbol``
``PointSymbolizer`` + PNG icon         ``CIMPictureMarker`` (base64 data URI)
``PointSymbolizer`` + SVG icon         ``CIMVectorMarker`` (paths → CIM rings)
``GraphicFill`` ``shape://slash`` …    ``CIMHatchFill``
``TextSymbolizer``                     ``CIMLabelClass`` + ``CIMTextSymbol``
``Min/MaxScaleDenominator``            layer ``maxScale`` / ``minScale``
=====================================  =====================================

ArcGIS Pro will not load a picture marker that holds an SVG — pictures are
raster-only there (bmp/jpg/png/gif), and vector artwork lives in a *shape*
marker. Roughly a third of GovMap's point layers use SVG pins, so those are
parsed into CIM geometry instead of embedded as an image; anything the parser
can't make sense of degrades to a plain circle and says so in the bundle's
README rather than producing a marker that silently draws nothing.

Nothing here does I/O or touches the DB — it is a pure transform over bytes, so
it is unit-testable end to end (``tests/test_lyrx_conversion.py``).
"""
from __future__ import annotations

import base64
import io
import json
import logging
import math
import re
import xml.etree.ElementTree as ET
import zipfile
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# A .lyrx declares the CIM schema it was written against, and ArcGIS Pro refuses
# a document NEWER than itself while happily upgrading an older one. 2.6.0 is
# therefore deliberate: every property emitted here exists in 2.6, so the file
# opens in Pro 2.6 and in every 3.x.
LYRX_VERSION = "2.6.0"
LYRX_BUILD = 24783

# SLD sizes are CSS pixels at 96 dpi; every size in CIM is points at 72 dpi.
# Skipping this makes every stroke and marker a third too big.
PX_TO_PT = 0.75

# The data source a symbology-only layer file points at. It is a placeholder by
# design — the file exists to carry cartography onto the reader's OWN copy of
# the layer (Apply Symbology From Layer), and Pro reads the renderer out of a
# .lyrx whose source is broken.
_PLACEHOLDER_DATASET = "layer.shp"

# Stand-in for a colour the archive does not hold (an unbaked SVG placeholder,
# a missing icon). Neutral on purpose: a guessed colour would read as the
# layer's own, and the bundle's README names every place this was used.
_GRAY = {"type": "CIMRGBColor", "values": [127, 127, 127, 100]}

_NUM_RE = re.compile(r"-?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?")


class LyrxError(Exception):
    """The input is not an SLD we can read at all (malformed XML, no rules)."""


# ---------------------------------------------------------------------------
# XML helpers — SLD in the wild mixes default-namespaced and `sld:`-prefixed
# elements (both forms occur across GovMap's catalog), so everything here works
# on LOCAL names only.
# ---------------------------------------------------------------------------


def _ln(tag: object) -> str:
    return str(tag).rsplit("}", 1)[-1]


def _kids(el, name: str) -> list:
    return [c for c in list(el) if _ln(c.tag) == name]


def _kid(el, name: str):
    ks = _kids(el, name)
    return ks[0] if ks else None


def _deep(el, name: str) -> list:
    return [e for e in el.iter() if _ln(e.tag) == name]


def _text(el) -> str:
    if el is None:
        return ""
    return "".join(el.itertext()).strip()


def _child_text(el, name: str, default: str = "") -> str:
    k = _kid(el, name)
    return _text(k) if k is not None else default


def _num(value: str | None, default: float = 0.0) -> float:
    if value is None:
        return default
    m = _NUM_RE.search(str(value))
    if not m:
        return default
    try:
        return float(m.group(0))
    except ValueError:
        return default


def _css(el) -> dict[str, str]:
    """``CssParameter``/``SvgParameter`` children as a plain dict.

    SLD 1.0 says CssParameter and 1.1 says SvgParameter; GovMap emits the
    former, but reading both costs one line and makes the converter usable for
    any other SLD source.
    """
    out: dict[str, str] = {}
    for c in el:
        if _ln(c.tag) in ("CssParameter", "SvgParameter"):
            name = c.get("name")
            if name:
                out[name] = _text(c)
    return out


# ---------------------------------------------------------------------------
# Colors
# ---------------------------------------------------------------------------


def _rgb(color: str | None, opacity: str | float | None = None) -> dict:
    """``#rrggbb`` + an SLD opacity (0..1) → ``CIMRGBColor`` (alpha 0..100)."""
    r = g = b = 127
    s = (color or "").strip()
    m = re.match(r"^#?([0-9a-fA-F]{6})$", s)
    if m:
        r, g, b = (int(m.group(1)[i:i + 2], 16) for i in (0, 2, 4))
    else:
        m3 = re.match(r"^#?([0-9a-fA-F]{3})$", s)
        if m3:
            r, g, b = (int(c * 2, 16) for c in m3.group(1))
        else:
            m_rgb = re.match(r"^rgba?\(([^)]*)\)$", s)
            if m_rgb:
                parts = [p.strip() for p in m_rgb.group(1).split(",")]
                if len(parts) >= 3:
                    r, g, b = (int(_num(p)) for p in parts[:3])
    alpha = 100.0
    if opacity is not None and str(opacity) != "":
        alpha = max(0.0, min(1.0, _num(str(opacity), 1.0))) * 100.0
    return {"type": "CIMRGBColor", "values": [r, g, b, round(alpha, 2)]}


def _is_none(value: str | None) -> bool:
    return (value or "").strip().lower() in ("none", "transparent")


# ---------------------------------------------------------------------------
# Symbol layers
# ---------------------------------------------------------------------------


_CAPS = {"butt": "Butt", "round": "Round", "square": "Square"}
_JOINS = {"miter": "Miter", "round": "Round", "bevel": "Bevel"}


def _solid_stroke(css: dict[str, str]) -> dict | None:
    if _is_none(css.get("stroke")):
        return None
    layer = {
        "type": "CIMSolidStroke",
        "enable": True,
        "capStyle": _CAPS.get((css.get("stroke-linecap") or "butt").lower(), "Butt"),
        "joinStyle": _JOINS.get((css.get("stroke-linejoin") or "miter").lower(), "Miter"),
        "lineStyle3D": "Strip",
        "miterLimit": 10,
        "width": round(_num(css.get("stroke-width"), 1.0) * PX_TO_PT, 4),
        "color": _rgb(css.get("stroke", "#000000"), css.get("stroke-opacity")),
    }
    dashes = [_num(x) for x in re.split(r"[,\s]+", css.get("stroke-dasharray", "").strip()) if x]
    if dashes:
        layer["effects"] = [{
            "type": "CIMGeometricEffectDashes",
            "dashTemplate": [round(d * PX_TO_PT, 4) for d in dashes],
            "lineDashEnding": "NoConstraint",
            "controlPointEnding": "NoConstraint",
        }]
    return layer


def _hatch_fill(graphic_fill, warnings: list[str]) -> dict | None:
    """``GraphicFill`` with a ``shape://…`` mark → ``CIMHatchFill``.

    GovMap uses these for the "this area is restricted/planned" cross-hatches;
    they are by far the most common non-solid fill in the catalog.
    """
    mark = None
    for g in _deep(graphic_fill, "Mark"):
        mark = g
        break
    if mark is None:
        warnings.append("מילוי גרפי שאינו סימן מוכר לא הומר")
        return None
    wkn = _child_text(mark, "WellKnownName").strip().lower()
    stroke_el = _kid(mark, "Stroke")
    css = _css(stroke_el) if stroke_el is not None else {}
    stroke = _solid_stroke(css) or {
        "type": "CIMSolidStroke", "enable": True, "capStyle": "Butt",
        "joinStyle": "Miter", "lineStyle3D": "Strip", "miterLimit": 10,
        "width": 0.75, "color": _rgb("#000000"),
    }
    rotations = {
        "shape://slash": 45.0, "shape://backslash": 135.0,
        "shape://vertline": 90.0, "shape://horline": 0.0,
        "shape://times": 45.0, "shape://plus": 0.0,
    }
    if wkn not in rotations:
        warnings.append(f"תבנית מילוי לא מוכרת: {wkn or 'ללא שם'} — הומרה לקווקוו אלכסוני")
    graphic = _kid(graphic_fill, "Graphic")
    size = _num(_child_text(graphic if graphic is not None else graphic_fill, "Size"), 8.0)
    return {
        "type": "CIMHatchFill",
        "enable": True,
        "lineSymbol": {"type": "CIMLineSymbol", "symbolLayers": [stroke]},
        "rotation": rotations.get(wkn, 45.0),
        "separation": round(max(size, 4.0) * PX_TO_PT, 4),
        "offsetX": 0,
        "offsetY": 0,
    }


def _fill_layers(fill_el, warnings: list[str]) -> list[dict]:
    graphic_fill = _kid(fill_el, "GraphicFill")
    if graphic_fill is not None:
        hatch = _hatch_fill(graphic_fill, warnings)
        return [hatch] if hatch else []
    css = _css(fill_el)
    if _is_none(css.get("fill")):
        return []
    return [{
        "type": "CIMSolidFill",
        "enable": True,
        "color": _rgb(css.get("fill", "#808080"), css.get("fill-opacity")),
    }]


# ---------------------------------------------------------------------------
# SVG → CIM vector marker
#
# ArcGIS Pro's picture markers are raster-only, so an SVG pin has to become
# geometry. GovMap's pins are plain filled paths (m/c/q/h/v/l/z, no arcs), a
# circle and a rect — all of which flatten cleanly into CIM rings.
# ---------------------------------------------------------------------------


_SVG_TOKEN_RE = re.compile(r"([MmLlHhVvCcSsQqTtAaZz])|(-?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?)")
_MAX_BEZIER_STEPS = 8


def _steps_for(points: list[tuple[float, float]]) -> int:
    """How many segments this curve deserves.

    GovMap's icon glyphs are drawn as HUNDREDS of sub-pixel curves; subdividing
    each into a fixed 8 turned one 9-icon layer into a 2 MB .lyrx. Scale the
    subdivision to the curve's own extent instead — a curve shorter than a
    pixel is a straight line at any zoom the marker is ever drawn at.
    """
    span = max(max(p[0] for p in points) - min(p[0] for p in points),
               max(p[1] for p in points) - min(p[1] for p in points))
    return max(1, min(_MAX_BEZIER_STEPS, int(span / 1.5) + 1))


def _flatten_cubic(p0, p1, p2, p3) -> list[tuple[float, float]]:
    n = _steps_for([p0, p1, p2, p3])
    pts = []
    for i in range(1, n + 1):
        t = i / n
        u = 1 - t
        pts.append((
            u * u * u * p0[0] + 3 * u * u * t * p1[0] + 3 * u * t * t * p2[0] + t * t * t * p3[0],
            u * u * u * p0[1] + 3 * u * u * t * p1[1] + 3 * u * t * t * p2[1] + t * t * t * p3[1],
        ))
    return pts


def _flatten_quad(p0, p1, p2) -> list[tuple[float, float]]:
    n = _steps_for([p0, p1, p2])
    pts = []
    for i in range(1, n + 1):
        t = i / n
        u = 1 - t
        pts.append((
            u * u * p0[0] + 2 * u * t * p1[0] + t * t * p2[0],
            u * u * p0[1] + 2 * u * t * p1[1] + t * t * p2[1],
        ))
    return pts


def _parse_svg_path(d: str) -> list[list[tuple[float, float]]]:
    """SVG path data → a list of point rings (subpaths), in SVG coordinates."""
    tokens = [(m.group(1), m.group(2)) for m in _SVG_TOKEN_RE.finditer(d or "")]
    rings: list[list[tuple[float, float]]] = []
    ring: list[tuple[float, float]] = []
    cur = (0.0, 0.0)
    start = (0.0, 0.0)
    prev_ctrl: tuple[float, float] | None = None
    cmd = ""
    i = 0

    def take(n: int) -> list[float] | None:
        nonlocal i
        vals: list[float] = []
        while len(vals) < n:
            if i >= len(tokens) or tokens[i][0] is not None:
                return None
            vals.append(float(tokens[i][1]))
            i += 1
        return vals

    while i < len(tokens):
        letter, number = tokens[i]
        if letter is not None:
            cmd = letter
            i += 1
            if cmd in ("Z", "z"):
                if len(ring) > 2:
                    rings.append(ring)
                ring = []
                cur = start
                prev_ctrl = None
                continue
        elif not cmd:
            i += 1
            continue
        rel = cmd.islower()
        up = cmd.upper()
        if up == "M":
            v = take(2)
            if v is None:
                break
            pt = (cur[0] + v[0], cur[1] + v[1]) if rel else (v[0], v[1])
            if len(ring) > 2:
                rings.append(ring)
            ring = [pt]
            cur = start = pt
            cmd = "l" if rel else "L"  # implicit lineto for repeated pairs
            prev_ctrl = None
        elif up == "L":
            v = take(2)
            if v is None:
                break
            cur = (cur[0] + v[0], cur[1] + v[1]) if rel else (v[0], v[1])
            ring.append(cur)
            prev_ctrl = None
        elif up == "H":
            v = take(1)
            if v is None:
                break
            cur = (cur[0] + v[0], cur[1]) if rel else (v[0], cur[1])
            ring.append(cur)
            prev_ctrl = None
        elif up == "V":
            v = take(1)
            if v is None:
                break
            cur = (cur[0], cur[1] + v[0]) if rel else (cur[0], v[0])
            ring.append(cur)
            prev_ctrl = None
        elif up in ("C", "S"):
            need = 6 if up == "C" else 4
            v = take(need)
            if v is None:
                break
            if up == "C":
                c1 = (cur[0] + v[0], cur[1] + v[1]) if rel else (v[0], v[1])
                c2 = (cur[0] + v[2], cur[1] + v[3]) if rel else (v[2], v[3])
                end = (cur[0] + v[4], cur[1] + v[5]) if rel else (v[4], v[5])
            else:
                c1 = (2 * cur[0] - prev_ctrl[0], 2 * cur[1] - prev_ctrl[1]) if prev_ctrl else cur
                c2 = (cur[0] + v[0], cur[1] + v[1]) if rel else (v[0], v[1])
                end = (cur[0] + v[2], cur[1] + v[3]) if rel else (v[2], v[3])
            ring.extend(_flatten_cubic(cur, c1, c2, end))
            prev_ctrl, cur = c2, end
        elif up in ("Q", "T"):
            need = 4 if up == "Q" else 2
            v = take(need)
            if v is None:
                break
            if up == "Q":
                c1 = (cur[0] + v[0], cur[1] + v[1]) if rel else (v[0], v[1])
                end = (cur[0] + v[2], cur[1] + v[3]) if rel else (v[2], v[3])
            else:
                c1 = (2 * cur[0] - prev_ctrl[0], 2 * cur[1] - prev_ctrl[1]) if prev_ctrl else cur
                end = (cur[0] + v[0], cur[1] + v[1]) if rel else (v[0], v[1])
            ring.extend(_flatten_quad(cur, c1, end))
            prev_ctrl, cur = c1, end
        elif up == "A":
            # Elliptical arcs do not occur in GovMap's icons; approximating one
            # with its chord keeps the outline closed instead of dropping it.
            v = take(7)
            if v is None:
                break
            cur = (cur[0] + v[5], cur[1] + v[6]) if rel else (v[5], v[6])
            ring.append(cur)
            prev_ctrl = None
        else:
            i += 1
    if len(ring) > 2:
        rings.append(ring)
    return rings


def _circle_ring(cx: float, cy: float, r: float, steps: int = 32) -> list[tuple[float, float]]:
    return [(cx + r * math.cos(2 * math.pi * i / steps),
             cy + r * math.sin(2 * math.pi * i / steps)) for i in range(steps)]


_SVG_CLASS_RE = re.compile(r"\.([A-Za-z0-9_-]+)\s*\{([^}]*)\}")


def _svg_styles(root) -> dict[str, dict[str, str]]:
    """The ``<style>`` block's ``.sN { fill: #fff }`` rules, flattened."""
    out: dict[str, dict[str, str]] = {}
    for st in _deep(root, "style"):
        for name, body in _SVG_CLASS_RE.findall(_text(st)):
            props = {}
            for decl in body.split(";"):
                if ":" in decl:
                    k, v = decl.split(":", 1)
                    props[k.strip().lower()] = v.strip()
            out[name] = props
    return out


def _svg_paint(el, styles: dict[str, dict[str, str]], prop: str) -> str | None:
    """Resolve ``fill``/``stroke`` for one SVG element, class rules included.

    GovMap's icon store serves its pins with ``param(fill)`` placeholders that
    the site's own viewer substitutes from the URL query — a downloaded icon
    keeps the placeholder. Those resolve to None here, and the caller paints a
    neutral colour and records a warning rather than emitting an invalid one.
    """
    value = el.get(prop)
    if value is None:
        for cls in (el.get("class") or "").split():
            value = styles.get(cls, {}).get(prop)
            if value:
                break
    if not value or value.startswith("param("):
        return None
    return value


def _svg_to_marker_graphics(svg_bytes: bytes, warnings: list[str],
                            label: str) -> tuple[list[dict], tuple[float, float, float, float]] | None:
    """Parse an SVG icon into CIM marker graphics + the frame envelope.

    Returns None when nothing drawable came out, so the caller can fall back to
    a plain circle instead of shipping an empty marker.
    """
    try:
        root = ET.fromstring(svg_bytes)
    except ET.ParseError:
        warnings.append(f"סמל SVG לא תקין ({label}) — הוחלף בעיגול")
        return None
    styles = _svg_styles(root)
    view = [_num(x) for x in re.split(r"[,\s]+", (root.get("viewBox") or "").strip()) if x]
    if len(view) == 4:
        vx, vy, vw, vh = view
    else:
        vx, vy = 0.0, 0.0
        vw = _num(root.get("width"), 24.0) or 24.0
        vh = _num(root.get("height"), 24.0) or 24.0
    span = max(vw, vh) or 24.0
    cx0, cy0 = vx + vw / 2, vy + vh / 2

    def to_cim(ring: list[tuple[float, float]]) -> list[list[float]]:
        # SVG's y grows downward and CIM's grows upward; normalise into a
        # [-5,5] frame so the marker scales purely by its `size`. Consecutive
        # points that round to the same place are dropped — the glyphs are full
        # of sub-pixel steps and every one of them costs bytes in every class.
        out: list[list[float]] = []
        for pt in ring:
            p = [round((pt[0] - cx0) / span * 10, 3), round((cy0 - pt[1]) / span * 10, 3)]
            if not out or p != out[-1]:
                out.append(p)
        return out

    # <mask> children are the icon's clipping helper, not artwork: drawing them
    # paints an opaque rectangle over the whole pin.
    masked = {id(e) for m in _deep(root, "mask") for e in m.iter()}
    graphics: list[dict] = []
    placeholder_paint = False
    for el in root.iter():
        tag = _ln(el.tag)
        rings: list[list[tuple[float, float]]] = []
        if tag == "path":
            rings = _parse_svg_path(el.get("d") or "")
        elif tag == "circle":
            rings = [_circle_ring(_num(el.get("cx")), _num(el.get("cy")), _num(el.get("r")))]
        elif tag == "ellipse":
            rx, ry = _num(el.get("rx")), _num(el.get("ry"))
            cx, cy = _num(el.get("cx")), _num(el.get("cy"))
            rings = [[(cx + rx * math.cos(a), cy + ry * math.sin(a))
                      for a in (2 * math.pi * i / 32 for i in range(32))]]
        elif tag == "rect":
            x, y = _num(el.get("x")), _num(el.get("y"))
            w, h = _num(el.get("width")), _num(el.get("height"))
            if w <= 0 or h <= 0:
                continue
            rings = [[(x, y), (x + w, y), (x + w, y + h), (x, y + h)]]
        elif tag in ("polygon", "polyline"):
            nums = [_num(n) for n in _NUM_RE.findall(el.get("points") or "")]
            rings = [list(zip(nums[0::2], nums[1::2]))] if len(nums) >= 6 else []
        else:
            continue
        rings = [r for r in rings if len(r) > 2]
        if not rings or id(el) in masked:
            continue
        fill = _svg_paint(el, styles, "fill")
        stroke = _svg_paint(el, styles, "stroke")
        if el.get("fill", "").startswith("param(") or el.get("stroke", "").startswith("param("):
            placeholder_paint = True
        layers: list[dict] = []
        if stroke and not _is_none(stroke):
            layers.append({
                "type": "CIMSolidStroke", "enable": True, "capStyle": "Round",
                "joinStyle": "Round", "lineStyle3D": "Strip", "miterLimit": 10,
                "width": round(_num(el.get("stroke-width"), 1.0) / span * 10, 4),
                "color": _rgb(stroke, el.get("stroke-opacity")),
            })
        if not _is_none(fill) and not (fill is None and stroke):
            layers.append({
                "type": "CIMSolidFill", "enable": True,
                "color": _rgb(fill, el.get("fill-opacity")) if fill else dict(_GRAY),
            })
        if not layers:
            continue
        cim_rings = [r for r in (to_cim(r) for r in rings) if len(r) > 2]
        if not cim_rings:
            continue
        graphics.append({
            "type": "CIMMarkerGraphic",
            "geometry": {"rings": cim_rings},
            "symbol": {"type": "CIMPolygonSymbol", "symbolLayers": layers},
        })
    if not graphics:
        return None
    if placeholder_paint:
        warnings.append(
            f"הצבע של הסמל {label} לא נשמר במקור (GovMap מגיש SVG עם param(fill)) — "
            "הצורה הומרה במלואה, הצבע הוחלף באפור")
    return graphics, (-5.0, -5.0, 5.0, 5.0)


def _vector_circle_marker(size_pt: float, color: dict) -> dict:
    return {
        "type": "CIMVectorMarker",
        "enable": True,
        "anchorPointUnits": "Relative",
        "dominantSizeAxis3D": "Y",
        "size": round(size_pt, 4),
        "billboardMode3D": "FaceNearPlane",
        "frame": {"xmin": -5, "ymin": -5, "xmax": 5, "ymax": 5},
        "markerGraphics": [{
            "type": "CIMMarkerGraphic",
            "geometry": {"rings": [[[round(x, 4), round(y, 4)]
                                    for x, y in _circle_ring(0, 0, 5)]]},
            "symbol": {"type": "CIMPolygonSymbol", "symbolLayers": [
                {"type": "CIMSolidFill", "enable": True, "color": color},
            ]},
        }],
        "respectFrame": True,
        "scaleSymbolsProportionally": True,
    }


_WKN_RINGS: dict[str, list[tuple[float, float]]] = {
    "circle": _circle_ring(0, 0, 5),
    "square": [(-5, -5), (5, -5), (5, 5), (-5, 5)],
    "triangle": [(-5, -4.33), (5, -4.33), (0, 4.33)],
    "star": [(0, 5), (1.5, 1.5), (5, 1.5), (2.2, -0.8), (3.2, -4.5), (0, -2.2),
             (-3.2, -4.5), (-2.2, -0.8), (-5, 1.5), (-1.5, 1.5)],
    "cross": [(-1, -5), (1, -5), (1, -1), (5, -1), (5, 1), (1, 1), (1, 5),
              (-1, 5), (-1, 1), (-5, 1), (-5, -1), (-1, -1)],
}


def _mark_marker(mark, size_pt: float, warnings: list[str]) -> dict:
    wkn = (_child_text(mark, "WellKnownName") or "circle").strip().lower()
    ring = _WKN_RINGS.get(wkn)
    if ring is None:
        if wkn not in ("", "circle"):
            warnings.append(f"סימן לא מוכר: {wkn} — הומר לעיגול")
        ring = _WKN_RINGS["circle"]
    layers: list[dict] = []
    stroke_el = _kid(mark, "Stroke")
    if stroke_el is not None:
        s = _solid_stroke(_css(stroke_el))
        if s:
            layers.append(s)
    fill_el = _kid(mark, "Fill")
    if fill_el is not None:
        layers.extend(_fill_layers(fill_el, warnings))
    if not layers:
        layers.append({"type": "CIMSolidFill", "enable": True, "color": dict(_GRAY)})
    return {
        "type": "CIMVectorMarker",
        "enable": True,
        "anchorPointUnits": "Relative",
        "dominantSizeAxis3D": "Y",
        "size": round(size_pt, 4),
        "billboardMode3D": "FaceNearPlane",
        "frame": {"xmin": -5, "ymin": -5, "xmax": 5, "ymax": 5},
        "markerGraphics": [{
            "type": "CIMMarkerGraphic",
            "geometry": {"rings": [[[round(x, 4), round(y, 4)] for x, y in ring]]},
            "symbol": {"type": "CIMPolygonSymbol", "symbolLayers": layers},
        }],
        "respectFrame": True,
        "scaleSymbolsProportionally": True,
    }


_RASTER_MIME = {"png": "image/png", "gif": "image/gif", "jpg": "image/jpeg",
                "jpeg": "image/jpeg", "bmp": "image/bmp"}


def _graphic_marker(graphic, icons: dict[str, bytes], warnings: list[str]) -> dict | None:
    """One ``<Graphic>`` → a CIM marker symbol layer."""
    size_pt = _num(_child_text(graphic, "Size"), 12.0) * PX_TO_PT or 9.0
    rotation = _num(_child_text(graphic, "Rotation"), 0.0)
    opacity = _child_text(graphic, "Opacity") or "1"
    ext = _kid(graphic, "ExternalGraphic")
    if ext is not None:
        res = _kid(ext, "OnlineResource")
        href = ""
        if res is not None:
            for k, v in res.attrib.items():
                if _ln(k) == "href":
                    href = v
                    break
        name = href.split("?")[0].lstrip("./")
        base = name.split("/")[-1]
        data = icons.get(name) or icons.get(base) or icons.get(f"icons/{base}")
        suffix = name.rsplit(".", 1)[-1].lower() if "." in name else ""
        if data is None:
            warnings.append(f"הסמל {name or 'ללא שם'} חסר בחבילה — הוחלף בעיגול")
            marker = _vector_circle_marker(size_pt, dict(_GRAY))
        elif suffix == "svg" or data[:200].lstrip()[:5].lower() in (b"<svg ", b"<svg>"):
            parsed = _svg_to_marker_graphics(data, warnings, name.split("/")[-1])
            if parsed is None:
                marker = _vector_circle_marker(size_pt, dict(_GRAY))
            else:
                graphics, frame = parsed
                marker = {
                    "type": "CIMVectorMarker",
                    "enable": True,
                    "anchorPointUnits": "Relative",
                    "dominantSizeAxis3D": "Y",
                    "size": round(size_pt, 4),
                    "billboardMode3D": "FaceNearPlane",
                    "frame": {"xmin": frame[0], "ymin": frame[1],
                              "xmax": frame[2], "ymax": frame[3]},
                    "markerGraphics": graphics,
                    "respectFrame": True,
                    "scaleSymbolsProportionally": True,
                }
        else:
            mime = _RASTER_MIME.get(suffix, "image/png")
            alpha = max(0.0, min(1.0, _num(opacity, 1.0))) * 100
            marker = {
                "type": "CIMPictureMarker",
                "enable": True,
                "anchorPoint": {"x": 0, "y": 0, "z": 0},
                "anchorPointUnits": "Relative",
                "dominantSizeAxis3D": "Y",
                "size": round(size_pt, 4),
                "billboardMode3D": "FaceNearPlane",
                "invertBackfaceTexture": True,
                "scaleX": 1,
                "textureFilter": "Picture",
                # White tint = the image's own colours; its alpha is the only
                # place a picture marker's transparency can live.
                "tintColor": {"type": "CIMRGBColor",
                              "values": [255, 255, 255, round(alpha, 2)]},
                "url": f"data:{mime};base64," + base64.b64encode(data).decode("ascii"),
            }
        if rotation:
            marker["rotation"] = -rotation  # SLD turns clockwise, CIM anticlockwise
        return marker
    mark = _kid(graphic, "Mark")
    if mark is not None:
        marker = _mark_marker(mark, size_pt, warnings)
        if rotation:
            marker["rotation"] = -rotation
        return marker
    return None


# ---------------------------------------------------------------------------
# Symbolizers → CIM symbols
# ---------------------------------------------------------------------------


def _text_symbol(sym) -> dict:
    font = _kid(sym, "Font")
    fcss = _css(font) if font is not None else {}
    fill = _kid(sym, "Fill")
    color = _rgb(_css(fill).get("fill", "#000000"), _css(fill).get("fill-opacity")) if fill is not None \
        else _rgb("#000000")
    style_name = "Regular"
    weight = (fcss.get("font-weight") or "").lower()
    italic = (fcss.get("font-style") or "").lower() == "italic"
    if weight == "bold" and italic:
        style_name = "Bold Italic"
    elif weight == "bold":
        style_name = "Bold"
    elif italic:
        style_name = "Italic"
    out = {
        "type": "CIMTextSymbol",
        "blockProgression": "TTB",
        "depth3D": 1,
        "extrapolateBaselines": True,
        "fontEffects": "Normal",
        "fontEncoding": "Unicode",
        "fontFamilyName": fcss.get("font-family") or "Arial",
        "fontStyleName": style_name,
        "fontType": "Unspecified",
        "haloSize": 1,
        "height": round(_num(fcss.get("font-size"), 10.0), 2),
        "hinting": "Default",
        "horizontalAlignment": "Center",
        "kerning": True,
        "letterWidth": 100,
        "ligatures": True,
        "lineGapType": "ExtraLeading",
        "symbol": {"type": "CIMPolygonSymbol", "symbolLayers": [
            {"type": "CIMSolidFill", "enable": True, "color": color},
        ]},
        "textCase": "Normal",
        # Hebrew labels: Pro reorders RTL runs itself, but only when the
        # symbol's direction is not pinned to LTR.
        "textDirection": "RTL",
        "verticalAlignment": "Center",
        "verticalGlyphOrientation": "Right",
        "wordSpacing": 100,
        "billboardMode3D": "FaceNearPlane",
    }
    halo = _kid(sym, "Halo")
    if halo is not None:
        hfill = _kid(halo, "Fill")
        hcss = _css(hfill) if hfill is not None else {}
        out["haloSize"] = round(_num(_child_text(halo, "Radius"), 1.0) * PX_TO_PT, 2)
        out["haloSymbol"] = {"type": "CIMPolygonSymbol", "symbolLayers": [
            {"type": "CIMSolidFill", "enable": True,
             "color": _rgb(hcss.get("fill", "#ffffff"), hcss.get("fill-opacity"))},
        ]}
    return out


def _quote(text: str) -> str:
    return '"' + text.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _label_expression(sym) -> tuple[str | None, str]:
    """The label's Arcade expression, plus a note when it had to be simplified.

    Two shapes occur in GovMap's styles and they must not be read the same way:

    * **Mixed content** — ``<Label><PropertyName>street</> - <PropertyName>num</></Label>``
      — is a concatenation, and is rebuilt in document order, literals included.
    * **A wrapped ``ogc:Function``** — e.g. layer 240871's
      ``if_then_else(strMatches(house_num,…), numberFormat('#,##0', house_num), house_num)``
      — is a computation. Its inner ``PropertyName`` elements are ARGUMENTS,
      not fields to concatenate: reading them as mixed content labelled every
      feature "5 5 5", because ``house_num`` appears three times in the one
      expression. Distinct fields are taken, deduplicated, and the caller is
      told the conditional/formatting logic did not survive.
    """
    label = _kid(sym, "Label")
    if label is None:
        return None, ""
    if _deep(label, "Function"):
        props: list[str] = []
        for p in _deep(label, "PropertyName"):
            name = _text(p)
            if name and name not in props:
                props.append(name)
        if not props:
            return None, "תווית שנשענת על פונקציה ללא שדה לא הומרה"
        expr = " + ' ' + ".join(f"$feature.{p}" for p in props)
        return expr, ("תווית התכתיב חושבה בפונקציה של GeoServer "
                      "(תנאי/עיצוב מספרים) — הועתק השדה עצמו, בלי החישוב")

    parts: list[str] = []

    def _push_text(raw: str | None) -> None:
        if raw and raw.strip():
            parts.append(_quote(re.sub(r"\s+", " ", raw)))

    _push_text(label.text)
    for node in label:
        name = _ln(node.tag)
        if name == "PropertyName" and _text(node):
            parts.append(f"$feature.{_text(node)}")
        elif name == "Literal":
            _push_text(_text(node))
        _push_text(node.tail)
    if not parts:
        return None, ""
    return " + ".join(parts), ""


def _label_class(sym, name: str, geom: str, warnings: list[str]) -> dict | None:
    expr, note = _label_expression(sym)
    if note:
        warnings.append(note)
    if not expr:
        return None
    return {
        "type": "CIMLabelClass",
        "expression": expr,
        "expressionEngine": "Arcade",
        "featuresToLabel": "AllVisibleFeatures",
        "name": name,
        "priority": -1,
        "standardLabelPlacementProperties": {
            "type": "CIMStandardLabelPlacementProperties",
            "featureType": {"esriGeometryPoint": "Point",
                            "esriGeometryPolyline": "Line"}.get(geom, "Polygon"),
            "featureWeight": "None",
            "labelWeight": "High",
            "numLabelsOption": "OneLabelPerName",
            "lineLabelPosition": {"type": "CIMStandardLineLabelPosition",
                                  "above": True, "inLine": True, "parallel": True},
            "pointPlacementMethod": "AroundPoint",
            "rotationType": "Arithmetic",
            "additionalOffset": 0,
        },
        "textSymbol": {"type": "CIMSymbolReference", "symbol": _text_symbol(sym)},
        "useCodedValue": True,
        "visibility": True,
        "iD": -1,
    }


@dataclass
class _RuleSymbols:
    polygon: list[dict] = field(default_factory=list)   # fill/stroke layers
    line: list[dict] = field(default_factory=list)      # stroke layers
    markers: list[dict] = field(default_factory=list)   # marker layers
    labels: list = field(default_factory=list)          # TextSymbolizer elements


def _rule_symbols(rule, icons: dict[str, bytes], warnings: list[str]) -> _RuleSymbols:
    out = _RuleSymbols()
    for sym in list(rule):
        kind = _ln(sym.tag)
        if kind == "PolygonSymbolizer":
            fill = _kid(sym, "Fill")
            if fill is not None:
                out.polygon.extend(_fill_layers(fill, warnings))
            stroke_el = _kid(sym, "Stroke")
            if stroke_el is not None:
                s = _solid_stroke(_css(stroke_el))
                if s:
                    out.polygon.insert(0, s)
        elif kind == "LineSymbolizer":
            stroke_el = _kid(sym, "Stroke")
            if stroke_el is not None:
                s = _solid_stroke(_css(stroke_el))
                if s:
                    out.line.append(s)
        elif kind == "PointSymbolizer":
            graphic = _kid(sym, "Graphic")
            if graphic is not None:
                m = _graphic_marker(graphic, icons, warnings)
                if m:
                    out.markers.append(m)
        elif kind == "TextSymbolizer":
            out.labels.append(sym)
        elif kind in ("RasterSymbolizer",):
            warnings.append("RasterSymbolizer אינו נתמך בהמרה ל-ArcGIS")
    return out


def _cim_symbol(rs: _RuleSymbols) -> tuple[dict | None, str]:
    """The rule's drawing symbol + the geometry type it implies."""
    if rs.markers:
        return {"type": "CIMPointSymbol", "symbolLayers": rs.markers}, "esriGeometryPoint"
    if rs.polygon:
        return {"type": "CIMPolygonSymbol", "symbolLayers": rs.polygon}, "esriGeometryPolygon"
    if rs.line:
        return {"type": "CIMLineSymbol", "symbolLayers": rs.line}, "esriGeometryPolyline"
    return None, ""


# ---------------------------------------------------------------------------
# Filters → renderer classes
# ---------------------------------------------------------------------------


@dataclass
class _Rule:
    title: str
    symbol: dict | None
    geometry: str
    labels: list
    is_else: bool = False
    eq_field: str | None = None
    eq_values: list[str] = field(default_factory=list)
    range_field: str | None = None
    upper: float | None = None
    # A filter shaped as "none of these values" — GeoServer's spelling of an
    # ElseFilter. Becomes the renderer's default symbol when the values it
    # excludes are exactly the ones the other rules classify.
    neg_field: str | None = None
    neg_values: list[str] = field(default_factory=list)
    unsupported: str = ""
    min_scale: float | None = None   # SLD MaxScaleDenominator
    max_scale: float | None = None   # SLD MinScaleDenominator


def _binary(op) -> tuple[str, str] | None:
    prop = None
    literal = None
    for c in op:
        n = _ln(c.tag)
        if n == "PropertyName":
            prop = _text(c)
        elif n == "Literal":
            literal = _text(c)
    if prop is None or literal is None:
        return None
    return prop, literal


def _negation_shape(op) -> tuple[str, list[str]] | None:
    """Read a filter that says "anything but these values", or None.

    GovMap's styles do not use ``ElseFilter``; the catch-all rule is written
    out, e.g. layer 213420's::

        Or( And(type != 'מבנים לשימור', type != 'מבנים פרטיים לשימור'),
            type IS NULL )

    which is precisely a unique-value renderer's ``<all other values>``. Read
    as an ordinary predicate it is untranslatable, and the rule — the symbol
    every OTHER feature in the layer draws with — was being dropped from the
    classification with a note.

    Accepts ``!=`` chains under ``And``/``Or``, and tolerates an ``IsNull`` on
    the same field (null is "no value", which the default covers anyway).
    Returns ``(field, excluded values)``; the caller decides whether those
    values really are the classified set.
    """
    fields: set[str] = set()
    values: list[str] = []

    def _walk(el) -> bool:
        name = _ln(el.tag)
        if name in ("And", "Or"):
            return all(_walk(c) for c in el)
        if name == "PropertyIsNotEqualTo":
            pair = _binary(el)
            if not pair:
                return False
            fields.add(pair[0])
            values.append(pair[1])
            return True
        if name == "PropertyIsNull":
            prop = _kid(el, "PropertyName")
            if prop is None:
                return False
            fields.add(_text(prop))
            return True
        return False

    if not _walk(op) or len(fields) != 1 or not values:
        return None
    return fields.pop(), values


def _analyse_filter(filt, rule: _Rule) -> None:
    """Read as much of one ``ogc:Filter`` as CIM can express.

    ArcGIS has no per-class arbitrary predicate: a renderer classifies either by
    exact values or by numeric breaks. Everything else is recorded on the rule
    (``unsupported``) and surfaces in the bundle's README — an untranslatable
    rule must be visible, not silently dropped.
    """
    ops = [c for c in filt if _ln(c.tag) != "ElseFilter"]
    if not ops:
        return
    op = ops[0]
    name = _ln(op.tag)
    negation = _negation_shape(op)
    if negation:
        rule.neg_field, rule.neg_values = negation
        return
    if name == "Or":
        values: list[str] = []
        fields: set[str] = set()
        for sub in op:
            if _ln(sub.tag) != "PropertyIsEqualTo":
                rule.unsupported = f"תנאי מסוג {_ln(sub.tag)} בתוך Or"
                return
            pair = _binary(sub)
            if not pair:
                rule.unsupported = "תנאי שוויון ללא שדה/ערך"
                return
            fields.add(pair[0])
            values.append(pair[1])
        if len(fields) == 1 and values:
            rule.eq_field, rule.eq_values = fields.pop(), values
        else:
            rule.unsupported = "Or על יותר משדה אחד"
        return
    if name == "PropertyIsEqualTo":
        pair = _binary(op)
        if pair:
            rule.eq_field, rule.eq_values = pair[0], [pair[1]]
        else:
            rule.unsupported = "תנאי שוויון ללא שדה/ערך"
        return
    if name in ("PropertyIsLessThanOrEqualTo", "PropertyIsLessThan"):
        pair = _binary(op)
        if pair:
            rule.range_field, rule.upper = pair[0], _num(pair[1])
        return
    if name == "And":
        upper = None
        fields = set()
        ok = True
        for sub in op:
            sub_name = _ln(sub.tag)
            pair = _binary(sub)
            if not pair:
                ok = False
                break
            fields.add(pair[0])
            if sub_name in ("PropertyIsLessThanOrEqualTo", "PropertyIsLessThan"):
                upper = _num(pair[1])
            elif sub_name in ("PropertyIsGreaterThan", "PropertyIsGreaterThanOrEqualTo"):
                continue  # the previous break already fixes the lower edge
            else:
                ok = False
                break
        if ok and upper is not None and len(fields) == 1:
            rule.range_field, rule.upper = fields.pop(), upper
        else:
            rule.unsupported = "תנאי And שאינו טווח מספרי על שדה אחד"
        return
    rule.unsupported = f"תנאי מסוג {name}"


def _parse_rules(root, icons: dict[str, bytes], warnings: list[str]) -> list[_Rule]:
    rules: list[_Rule] = []
    for rule_el in _deep(root, "Rule"):
        rs = _rule_symbols(rule_el, icons, warnings)
        symbol, geom = _cim_symbol(rs)
        r = _Rule(
            title=_child_text(rule_el, "Title") or _child_text(rule_el, "Name"),
            symbol=symbol,
            geometry=geom,
            labels=rs.labels,
        )
        min_s = _child_text(rule_el, "MinScaleDenominator")
        max_s = _child_text(rule_el, "MaxScaleDenominator")
        if min_s:
            r.max_scale = _num(min_s)
        if max_s:
            r.min_scale = _num(max_s)
        filt = _kid(rule_el, "Filter")
        if filt is not None:
            if _kid(filt, "ElseFilter") is not None:
                r.is_else = True
            else:
                _analyse_filter(filt, r)
        elif _kid(rule_el, "ElseFilter") is not None:
            r.is_else = True
        rules.append(r)
    return rules


def _ref(symbol: dict) -> dict:
    return {"type": "CIMSymbolReference", "symbol": symbol}


def _build_renderer(rules: list[_Rule], warnings: list[str]) -> dict | None:
    drawable = [r for r in rules if r.symbol]
    if not drawable:
        return None
    classified = [r for r in drawable if r.eq_field and r.eq_values]
    ranged = [r for r in drawable if r.range_field and r.upper is not None]
    default = next((r for r in drawable if r.is_else), None)

    eq_fields = {r.eq_field for r in classified}
    # A "not any of these" rule IS the default — but only when the values it
    # excludes are exactly the ones the other rules classify. Anything else is
    # a genuine predicate CIM cannot hold, and stays a warning.
    if default is None and len(eq_fields) == 1:
        seen = {v for r in classified for v in r.eq_values}
        for r in drawable:
            if r.neg_field and r.neg_field in eq_fields and set(r.neg_values) == seen:
                default = r
                break
    if classified and len(eq_fields) == 1:
        field_name = classified[0].eq_field
        groups = [{
            "type": "CIMUniqueValueGroup",
            "classes": [{
                "type": "CIMUniqueValueClass",
                "label": r.title or ", ".join(r.eq_values),
                "patch": "Default",
                "symbol": _ref(r.symbol),
                "values": [{"type": "CIMUniqueValue", "fieldValues": [v]}
                           for v in r.eq_values],
                "visible": True,
            } for r in classified],
            "heading": field_name,
        }]
        renderer = {
            "type": "CIMUniqueValueRenderer",
            "defaultLabel": "<all other values>",
            "defaultSymbolPatch": "Default",
            "fields": [field_name],
            "groups": groups,
            "useDefaultSymbol": default is not None,
            "polygonSymbolColorTarget": "Fill",
        }
        if default is not None:
            renderer["defaultSymbol"] = _ref(default.symbol)
        skipped = [r for r in drawable
                   if r is not default and r not in classified]
        for r in skipped:
            warnings.append(
                f"הכלל \"{r.title or 'ללא שם'}\" לא נכלל בסיווג"
                + (f" ({r.unsupported})" if r.unsupported else ""))
        return renderer

    range_fields = {r.range_field for r in ranged}
    if ranged and len(range_fields) == 1 and len(ranged) == len(drawable) - (1 if default else 0):
        field_name = ranged[0].range_field
        ordered = sorted(ranged, key=lambda r: r.upper or 0.0)
        return {
            "type": "CIMClassBreaksRenderer",
            "barrierWeight": "High",
            "classBreakType": "GraduatedColor",
            "classificationMethod": "Manual",
            "field": field_name,
            "minimumBreak": ordered[0].upper,
            "showInAscendingOrder": True,
            "heading": field_name,
            "breaks": [{
                "type": "CIMClassBreak",
                "label": r.title or str(r.upper),
                "patch": "Default",
                "symbol": _ref(r.symbol),
                "upperBound": r.upper,
            } for r in ordered],
            "polygonSymbolColorTarget": "Fill",
        }

    if len(drawable) > 1:
        for r in drawable[1:]:
            warnings.append(
                f"הכלל \"{r.title or 'ללא שם'}\" לא ניתן לתרגום לסיווג של ArcGIS"
                + (f" ({r.unsupported})" if r.unsupported else "")
                + " — נשמר רק הסמל הראשון")
    return {
        "type": "CIMSimpleRenderer",
        "patch": "Default",
        "label": drawable[0].title or "",
        "symbol": _ref(drawable[0].symbol),
    }


# ---------------------------------------------------------------------------
# The layer document
# ---------------------------------------------------------------------------


def convert_sld(xml: str | bytes, *, name: str,
                icons: dict[str, bytes] | None = None) -> tuple[dict, list[str]]:
    """One SLD → one ``CIMLayerDocument`` dict + the warnings worth telling.

    ``icons`` maps a bundle entry name (``"icons/pin.png"``) to its bytes.
    """
    warnings: list[str] = []
    if isinstance(xml, bytes):
        xml = xml.decode("utf-8-sig", "replace")
    try:
        root = ET.fromstring(xml.encode("utf-8"))
    except ET.ParseError as e:
        raise LyrxError(f"invalid SLD XML: {e}") from e
    if _ln(root.tag) != "StyledLayerDescriptor":
        raise LyrxError(f"not an SLD (root element is <{_ln(root.tag)}>)")

    rules = _parse_rules(root, icons or {}, warnings)
    if not rules:
        raise LyrxError("the SLD carries no <Rule>")

    renderer = _build_renderer(rules, warnings)
    geom = next((r.geometry for r in rules if r.geometry), "esriGeometryPolygon")

    label_classes: list[dict] = []
    for r in rules:
        for i, sym in enumerate(r.labels):
            lc = _label_class(sym, f"{r.title or 'Class'} {i + 1}".strip(), geom, warnings)
            if lc:
                label_classes.append(lc)

    # Prefer the style's own Title (the human caption GovMap shows), then the
    # NamedLayer's machine Name, then the filename.
    named = _kid(root, "NamedLayer")
    title = _child_text(named, "Name") if named is not None else ""
    for user_style in _deep(root, "UserStyle"):
        title = _child_text(user_style, "Title") or title
        break
    title = title or name

    layer: dict = {
        "type": "CIMFeatureLayer",
        "name": title or name,
        "uRI": "CIMPATH=over/{}.xml".format(re.sub(r"[^A-Za-z0-9_]+", "_", name) or "layer"),
        "useSourceMetadata": True,
        "description": "סימבולוגיה מקורית מ-GovMap, שהומרה מ-SLD בגרסאות לעם (over.org.il)",
        "layerType": "Operational",
        "showLegends": True,
        "visibility": True,
        "displayCacheType": "Permanent",
        "maxDisplayCacheAge": 5,
        "showPopups": True,
        "serviceLayerID": -1,
        "refreshRate": -1,
        "refreshRateUnit": "esriTimeUnitsSeconds",
        "autoGenerateFeatureTemplates": True,
        "featureElevationExpression": "0",
        "featureTable": {
            "type": "CIMFeatureTable",
            "editable": True,
            "dataConnection": {
                "type": "CIMStandardDataConnection",
                "workspaceConnectionString": "DATABASE=.",
                "workspaceFactory": "Shapefile",
                "dataset": _PLACEHOLDER_DATASET,
                "datasetType": "esriDTFeatureClass",
            },
            "studyAreaSpatialRel": "esriSpatialRelUndefined",
            "searchOrder": "esriSearchOrderSpatial",
        },
        "htmlPopupEnabled": True,
        "selectable": True,
        "featureCacheType": "Session",
        "scaleSymbols": True,
        "snappable": True,
    }
    if renderer:
        layer["renderer"] = renderer
    if label_classes:
        layer["labelClasses"] = label_classes
        layer["labelVisibility"] = True

    # SLD's Min/MaxScaleDenominator are per rule; ArcGIS keeps a visible-scale
    # range per layer, so take the widest span the rules describe.
    mins = [r.min_scale for r in rules if r.min_scale]
    maxs = [r.max_scale for r in rules if r.max_scale]
    if mins:
        layer["minScale"] = max(mins)
    if maxs:
        layer["maxScale"] = min(maxs)
    if len(rules) > 1 and (mins or maxs):
        warnings.append("טווחי הצגה לפי קנה מידה הוגדרו ברמת השכבה — "
                        "ArcGIS אינו תומך בטווח נפרד לכל כלל")

    doc = {
        "type": "CIMLayerDocument",
        "version": LYRX_VERSION,
        "build": LYRX_BUILD,
        "layers": [layer["uRI"]],
        "layerDefinitions": [layer],
        "binaryReferences": [],
        "elevationSurfaces": [],
    }
    return doc, warnings


def lyrx_bytes(doc: dict) -> bytes:
    """The document as the UTF-8 JSON ArcGIS Pro expects in a ``.lyrx``."""
    return json.dumps(doc, ensure_ascii=False, indent=2).encode("utf-8")


# ---------------------------------------------------------------------------
# Bundle conversion
# ---------------------------------------------------------------------------


_README = """\
סימבולוגיה של שכבת GovMap עבור ArcGIS Pro
=========================================
הקבצים כאן נוצרו אוטומטית בגרסאות לעם (over.org.il) מתוך קובץ ה-SLD המקורי
של GovMap — אותו קובץ שנמצא בחבילת הסימבולוגיה הרגילה (ה-ZIP שמתאים ל-QGIS).

מה יש כאן
---------
{files}

איך משתמשים
-----------
1. הוסיפו למפה את שכבת הנתונים שלכם (ה-GeoJSON / ה-GPKG שהורדתם מגרסאות לעם).
2. Geoprocessing → Apply Symbology From Layer:
      Input Layer            = השכבה שלכם
      Symbology Layer        = קובץ ה-.lyrx הזה
      Symbology Fields       = מיפוי השדה שבקובץ לשדה בשכבה שלכם (בדרך כלל זהה)
      Update Symbology Ranges = MAINTAIN_RANGES
   לחלופין אפשר פשוט לגרור את ה-.lyrx למפה ואז Change Data Source לנתונים שלכם.

שימו לב
-------
* מקור הנתונים בקובץ הוא מציין מקום ("{dataset}") ולכן ArcGIS יסמן אותו כשבור —
  זו התנהגות צפויה. הקובץ נועד לשאת את הקרטוגרפיה בלבד.
* הסיווג נשען על שמות השדות המקוריים (machine names) שבהם משתמש ה-SLD. הפירוש
  שלהם בעברית נמצא בקובץ מילון השדות שבחבילה.
* גדלים הומרו מפיקסלים (SLD) לנקודות (ArcGIS) ביחס של 0.75.
{warnings}
מקור: {source}
"""


def _readme(entries: list[str], warnings: list[str], source: str) -> bytes:
    warn_block = ""
    if warnings:
        lines = "\n".join(f"  - {w}" for w in dict.fromkeys(warnings))
        warn_block = ("\nהערות המרה (מה שלא ניתן היה לתרגם במדויק)\n"
                      "-----------------------------------------\n" + lines + "\n")
    body = _README.format(
        files="\n".join(f"  {e}" for e in entries),
        dataset=_PLACEHOLDER_DATASET,
        warnings=warn_block,
        source=source,
    )
    return body.encode("utf-8")


def convert_bundle(zip_bytes: bytes, *, source_url: str = "over.org.il") -> bytes:
    """A GovMap symbology bundle (SLD + icons + field dictionary) → an ArcGIS
    bundle (``.lyrx`` per style + the same documentation CSVs + a README).

    The icons travel along even though the .lyrx embeds them: a reader who
    wants a different marker needs the original artwork, and a picture marker
    that Pro rejects is recoverable by hand from the file next to it.
    """
    try:
        src = zipfile.ZipFile(io.BytesIO(zip_bytes))
    except zipfile.BadZipFile as e:
        raise LyrxError("the symbology bundle is not a readable ZIP") from e

    names = src.namelist()
    icons = {n: src.read(n) for n in names if n.lower().startswith("icons/")}
    slds = [n for n in names if n.lower().endswith(".sld")]
    if not slds:
        raise LyrxError("the bundle holds no .sld file")

    warnings: list[str] = []
    out_buf = io.BytesIO()
    entries: list[str] = []
    with zipfile.ZipFile(out_buf, "w", zipfile.ZIP_DEFLATED) as out:
        for sld_name in slds:
            stem = sld_name.rsplit("/", 1)[-1][:-4]
            try:
                doc, warns = convert_sld(src.read(sld_name), name=stem, icons=icons)
            except LyrxError as e:
                warnings.append(f"{stem}: ההמרה נכשלה ({e})")
                logger.warning("lyrx conversion failed for %s: %s", sld_name, e)
                continue
            warnings.extend(warns)
            entry = f"{stem}.lyrx"
            out.writestr(entry, lyrx_bytes(doc))
            entries.append(f"{entry} — קובץ שכבה ל-ArcGIS Pro")
        if not entries:
            raise LyrxError("no SLD in the bundle could be converted")
        for n in names:
            if n.lower().endswith(".csv") or n.lower().startswith("icons/"):
                out.writestr(n, src.read(n))
                if n.lower().endswith(".csv"):
                    entries.append(f"{n} — כפי שהוא בחבילת ה-SLD")
        entries.append("icons/ — קבצי הסמלים המקוריים (משובצים כבר בתוך ה-.lyrx)"
                       if icons else "")
        out.writestr("README_ArcGIS.txt",
                     _readme([e for e in entries if e], warnings, source_url))
    return out_buf.getvalue()
