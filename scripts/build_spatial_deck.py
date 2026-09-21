"""Build over-spatial-deck.pptx — the mapping/GIS/spatial story of גרסאות לעם.

Why a generator and not a hand-made file: a .pptx is a zip of XML, so a binary
committed on its own is a dead end — nobody can diff it, correct a number in it
or rebuild it a month later. The deck is therefore DATA in this file (the SLIDES
list at the bottom) and the rendering is generic, so updating it is editing a
string and re-running:

    python scripts/build_spatial_deck.py

Every figure carries a `src` comment naming where it was measured. They came
from production on 2026-09-21, not from memory:
  GET /api/nadlan/stats, GET /api/deals/stats, and two queries on the public
  SQL console (over_datasets grouped by geometry_status, and a pg_class scan
  for tables carrying a PostGIS geometry column).

Hebrew in PowerPoint needs three things python-pptx does not do for you, and
all three are handled in `_style` and `_rtl`: the paragraph needs rtl="1" or
punctuation lands on the wrong side, the run needs a COMPLEX-SCRIPT font
(`a:cs`) because `font.name` only sets the latin one, and the complex-script
size (`sz` on `a:cs`... in practice `font.size` covers it, but the cs typeface
does not inherit).
"""
import copy
import os
import sys

from pptx import Presentation
from pptx.dml.color import RGBColor
from pptx.enum.shapes import MSO_SHAPE
from pptx.enum.text import MSO_ANCHOR, PP_ALIGN
from pptx.util import Emu, Inches, Pt

# ── palette, shared with over-features-deck.html so the family looks related ──
INK = RGBColor(0x10, 0x1B, 0x33)
SOFT = RGBColor(0x3A, 0x4A, 0x66)
FAINT = RGBColor(0x6B, 0x78, 0x91)
PAPER = RGBColor(0xF6, 0xF7, 0xFB)
WHITE = RGBColor(0xFF, 0xFF, 0xFF)
LINE = RGBColor(0xDC, 0xE1, 0xEC)
ACCENT = RGBColor(0x1F, 0x6F, 0x8B)
DEEP = RGBColor(0x15, 0x50, 0x66)
NIGHT = RGBColor(0x0C, 0x13, 0x22)

CAT = {
    "spatial": RGBColor(0x0F, 0x76, 0x6E),
    "sql": RGBColor(0x6D, 0x28, 0xD9),
    "mcp": RGBColor(0x1D, 0x4E, 0xD8),
    "source": RGBColor(0xC2, 0x41, 0x0C),
    "infra": RGBColor(0x52, 0x60, 0x7A),
    "product": RGBColor(0xA1, 0x62, 0x07),
    "sec": RGBColor(0xB4, 0x23, 0x2C),
}

FONT = "Segoe UI"
W, H = 13.333, 7.5            # 16:9
MARGIN = 0.78
CONTENT_W = W - 2 * MARGIN


# ── text plumbing ────────────────────────────────────────────────────────────
def _rtl(par, align=PP_ALIGN.RIGHT):
    """Right-to-left paragraph. Without rtl="1" PowerPoint puts the full stop
    on the left of a Hebrew line and reorders any Latin run inside it."""
    par.alignment = align
    par._p.get_or_add_pPr().set("rtl", "1")


def _style(run, size, *, bold=False, color=INK, font=FONT, spacing=None):
    f = run.font
    f.size = Pt(size)
    f.bold = bold
    f.color.rgb = color
    f.name = font
    # font.name sets only <a:latin>. Hebrew is a complex script and falls back
    # to the theme font unless <a:cs> says otherwise — which is what makes the
    # difference between Segoe UI and whatever Calibri does to Hebrew.
    rPr = run._r.get_or_add_rPr()
    for tag in ("a:cs", "a:ea"):
        el = rPr.find("{http://schemas.openxmlformats.org/drawingml/2006/main}"
                      + tag.split(":")[1])
        if el is None:
            el = rPr.makeelement(
                "{http://schemas.openxmlformats.org/drawingml/2006/main}"
                + tag.split(":")[1], {})
            rPr.append(el)
        el.set("typeface", font)
    if spacing is not None:
        rPr.set("spc", str(int(spacing * 100)))
    return run


def _textbox(slide, x, y, w, h, *, anchor=MSO_ANCHOR.TOP):
    box = slide.shapes.add_textbox(Inches(x), Inches(y), Inches(w), Inches(h))
    tf = box.text_frame
    tf.word_wrap = True
    tf.vertical_anchor = anchor
    tf.margin_left = tf.margin_right = tf.margin_top = tf.margin_bottom = 0
    return tf


def _para(tf, text, size, *, bold=False, color=INK, space_before=0,
          space_after=0, line=1.25, first=False, spacing=None,
          align=PP_ALIGN.RIGHT):
    par = tf.paragraphs[0] if first else tf.add_paragraph()
    _rtl(par, align)
    par.space_before = Pt(space_before)
    par.space_after = Pt(space_after)
    par.line_spacing = line
    _style(par.add_run(), size, bold=bold, color=color, spacing=spacing).text = text
    return par


def _rect(slide, x, y, w, h, *, fill=None, line_color=None, line_w=1.0,
          shape=MSO_SHAPE.ROUNDED_RECTANGLE, adj=0.07):
    sh = slide.shapes.add_shape(shape, Inches(x), Inches(y), Inches(w), Inches(h))
    if shape == MSO_SHAPE.ROUNDED_RECTANGLE:
        try:
            sh.adjustments[0] = adj
        except (IndexError, ValueError):
            pass
    if fill is None:
        sh.fill.background()
    else:
        sh.fill.solid()
        sh.fill.fore_color.rgb = fill
    if line_color is None:
        sh.line.fill.background()
    else:
        sh.line.color.rgb = line_color
        sh.line.width = Pt(line_w)
    sh.shadow.inherit = False
    return sh


def _cols(n, gap=0.3):
    """Column x-positions, RIGHT to LEFT: index 0 is the rightmost."""
    w = (CONTENT_W - gap * (n - 1)) / n
    return w, [MARGIN + (n - 1 - i) * (w + gap) for i in range(n)]


# ── slide furniture ──────────────────────────────────────────────────────────
def _background(slide, color):
    _rect(slide, -0.05, -0.05, W + 0.1, H + 0.1, fill=color,
          shape=MSO_SHAPE.RECTANGLE)


def _head(slide, eyebrow, title, kicker=None):
    _rect(slide, W - MARGIN - 0.9, 0.5, 0.9, 0.045, fill=ACCENT,
          shape=MSO_SHAPE.RECTANGLE)
    tf = _textbox(slide, MARGIN, 0.62, CONTENT_W, 0.32)
    _para(tf, eyebrow, 11.5, bold=True, color=DEEP, first=True, spacing=2.2)

    tf = _textbox(slide, MARGIN, 1.0, CONTENT_W, 0.78)
    _para(tf, title, 30, bold=True, color=INK, first=True, line=1.06)

    y = 1.86
    if kicker:
        tf = _textbox(slide, MARGIN, y, CONTENT_W, 0.62)
        _para(tf, kicker, 14, color=SOFT, first=True, line=1.35)
        y = 2.52
    return y


def _footer(slide, n, total):
    tf = _textbox(slide, MARGIN, H - 0.62, CONTENT_W, 0.3)
    _para(tf, "over.org.il  ·  גרסאות לעם", 9.5, color=FAINT, first=True)
    tf = _textbox(slide, MARGIN, H - 0.62, CONTENT_W, 0.3)
    _para(tf, "%02d / %02d" % (n, total), 9.5, color=FAINT, first=True,
          align=PP_ALIGN.LEFT)


def _card(slide, x, y, w, h, card):
    cat = CAT[card.get("cat", "spatial")]
    _rect(slide, x, y, w, h, fill=PAPER, line_color=LINE)
    # The accent edge sits on the RIGHT, where an RTL reader starts.
    _rect(slide, x + w - 0.055, y + 0.04, 0.055, h - 0.08, fill=cat,
          shape=MSO_SHAPE.RECTANGLE)

    pad = 0.24
    tf = _textbox(slide, x + pad, y + 0.17, w - 2 * pad, h - 0.3)
    if card.get("tag"):
        _para(tf, card["tag"], 9.5, bold=True, color=cat, first=True,
              spacing=0.8, space_after=3)
        _para(tf, card["h"], 14.5, bold=True, color=INK, line=1.16,
              space_after=4)
    else:
        _para(tf, card["h"], 14.5, bold=True, color=INK, first=True,
              line=1.16, space_after=4)
    _para(tf, card["p"], 11, color=SOFT, line=1.32)


def _stat(slide, x, y, w, h, value, label):
    _rect(slide, x, y, w, h, fill=PAPER, line_color=LINE)
    tf = _textbox(slide, x + 0.16, y + 0.2, w - 0.32, h - 0.32,
                  anchor=MSO_ANCHOR.MIDDLE)
    _para(tf, value, 27, bold=True, color=ACCENT, first=True,
          align=PP_ALIGN.CENTER, line=1.0)
    _para(tf, label, 10.5, color=FAINT, align=PP_ALIGN.CENTER, line=1.2,
          space_before=3)


# ── slide renderers ──────────────────────────────────────────────────────────
def render_title(slide, s, n, total):
    _background(slide, NIGHT)
    _rect(slide, W - MARGIN - 1.5, 1.85, 1.5, 0.05, fill=RGBColor(0x57, 0xB6, 0xD0),
          shape=MSO_SHAPE.RECTANGLE)

    tf = _textbox(slide, MARGIN, 1.35, CONTENT_W, 0.35)
    _para(tf, s["eyebrow"], 12.5, bold=True, color=RGBColor(0x8F, 0xD4, 0xE6),
          first=True, spacing=2.6)

    tf = _textbox(slide, MARGIN, 2.15, CONTENT_W, 2.1)
    _para(tf, s["title"], 54, bold=True, color=WHITE, first=True, line=1.02)
    _para(tf, s["sub"], 54, bold=True, color=RGBColor(0x57, 0xB6, 0xD0),
          line=1.02)

    tf = _textbox(slide, MARGIN, 4.5, CONTENT_W * 0.74, 1.2)
    _para(tf, s["lede"], 16, color=RGBColor(0xB7, 0xC2, 0xD8), first=True,
          line=1.45)

    tf = _textbox(slide, MARGIN, 6.35, CONTENT_W, 0.3)
    _para(tf, s["foot"], 11.5, bold=True, color=RGBColor(0x7C, 0x89, 0xA3),
          first=True, spacing=1.6)


def render_stats(slide, s, n, total):
    y = _head(slide, s["eyebrow"], s["title"], s.get("kicker"))
    boxes = s["stats"]
    per_row = 3 if len(boxes) <= 6 else 4
    rows = [boxes[i:i + per_row] for i in range(0, len(boxes), per_row)]
    h = 1.32
    for r, row in enumerate(rows):
        w, xs = _cols(per_row)
        for i, (value, label) in enumerate(row):
            _stat(slide, xs[i], y + r * (h + 0.26), w, h, value, label)
    if s.get("note"):
        tf = _textbox(slide, MARGIN, y + len(rows) * (h + 0.26) + 0.1,
                      CONTENT_W, 0.7)
        _para(tf, s["note"], 12, color=SOFT, first=True, line=1.35)
    _footer(slide, n, total)


def render_cards(slide, s, n, total):
    y = _head(slide, s["eyebrow"], s["title"], s.get("kicker"))
    cards = s["cards"]
    per_row = s.get("cols", 2 if len(cards) <= 4 else 3)
    rows = [cards[i:i + per_row] for i in range(0, len(cards), per_row)]

    note_h = 0.62 if s.get("note") else 0.0
    avail = (H - 0.95) - y - note_h
    gap = 0.24
    # A single row of cards at two-row height leaves the slide bottom-heavy with
    # emptiness, so one row is allowed to be taller and the whole block is
    # centred in what is left. Two rows fill the space on their own.
    cap = 2.0 if len(rows) == 1 else 1.62
    h = min(cap, (avail - gap * (len(rows) - 1)) / len(rows))
    block = len(rows) * h + gap * (len(rows) - 1)
    top = y + max(0.0, (avail - block) * 0.38)

    for r, row in enumerate(rows):
        w, xs = _cols(per_row)
        for i, card in enumerate(row):
            _card(slide, xs[i], top + r * (h + gap), w, h, card)
    if s.get("note"):
        tf = _textbox(slide, MARGIN, top + block + 0.2, CONTENT_W, note_h)
        _para(tf, s["note"], 11.5, color=SOFT, first=True, line=1.35)
    _footer(slide, n, total)


def render_flow(slide, s, n, total):
    """One identifier in, all the others out — drawn, because it is the whole
    point of the crosswalk and a bullet list does not show it."""
    y = _head(slide, s["eyebrow"], s["title"], s.get("kicker"))

    ins = s["inputs"]
    bw, bh, gap = 2.62, 0.62, 0.18
    for i, label in enumerate(ins):
        x = W - MARGIN - bw
        yy = y + i * (bh + gap)
        _rect(slide, x, yy, bw, bh, fill=WHITE, line_color=ACCENT, line_w=1.5)
        tf = _textbox(slide, x + 0.12, yy, bw - 0.24, bh,
                      anchor=MSO_ANCHOR.MIDDLE)
        _para(tf, label, 13, bold=True, color=DEEP, first=True,
              align=PP_ALIGN.CENTER)

    mid_h = len(ins) * (bh + gap) - gap
    hub_w, hub_h = 2.05, 1.05
    hub_x = W - MARGIN - bw - 0.42 - hub_w
    hub_y = y + (mid_h - hub_h) / 2
    _rect(slide, hub_x, hub_y, hub_w, hub_h, fill=ACCENT)
    tf = _textbox(slide, hub_x + 0.1, hub_y, hub_w - 0.2, hub_h,
                  anchor=MSO_ANCHOR.MIDDLE)
    _para(tf, s["hub"], 14, bold=True, color=WHITE, first=True,
          align=PP_ALIGN.CENTER, line=1.15)

    outs = s["outputs"]
    ow = hub_x - 0.42 - MARGIN
    oh = (mid_h - 0.14 * (len(outs) - 1)) / len(outs)
    for i, (title, body) in enumerate(outs):
        yy = y + i * (oh + 0.14)
        _rect(slide, MARGIN, yy, ow, oh, fill=PAPER, line_color=LINE)
        tf = _textbox(slide, MARGIN + 0.2, yy + 0.1, ow - 0.4, oh - 0.2,
                      anchor=MSO_ANCHOR.MIDDLE)
        _para(tf, title, 12.5, bold=True, color=INK, first=True, line=1.15)
        _para(tf, body, 10.5, color=SOFT, line=1.25, space_before=2)

    if s.get("note"):
        tf = _textbox(slide, MARGIN, y + mid_h + 0.26, CONTENT_W, 0.7)
        _para(tf, s["note"], 11.5, color=SOFT, first=True, line=1.35)
    _footer(slide, n, total)


def render_closing(slide, s, n, total):
    _background(slide, DEEP)
    tf = _textbox(slide, MARGIN, 1.5, CONTENT_W, 0.35)
    _para(tf, s["eyebrow"], 12, bold=True, color=RGBColor(0x8F, 0xD4, 0xE6),
          first=True, spacing=2.4)
    tf = _textbox(slide, MARGIN, 2.0, CONTENT_W, 1.1)
    _para(tf, s["title"], 38, bold=True, color=WHITE, first=True, line=1.08)
    tf = _textbox(slide, MARGIN, 3.25, CONTENT_W * 0.8, 1.4)
    _para(tf, s["lede"], 15.5, color=RGBColor(0xDC, 0xE9, 0xF2), first=True,
          line=1.5)

    chips = s["chips"]
    x, yy, cw, ch = W - MARGIN, 5.0, 0.0, 0.42
    for chip in chips:
        cw = 0.22 + len(chip) * 0.105
        if x - cw < MARGIN:
            x, yy = W - MARGIN, yy + ch + 0.16
        _rect(slide, x - cw, yy, cw, ch, fill=RGBColor(0x2A, 0x6C, 0x84))
        tf = _textbox(slide, x - cw, yy, cw, ch, anchor=MSO_ANCHOR.MIDDLE)
        _para(tf, chip, 11, bold=True, color=WHITE, first=True,
              align=PP_ALIGN.CENTER)
        x -= cw + 0.14

    tf = _textbox(slide, MARGIN, H - 0.9, CONTENT_W, 0.35)
    _para(tf, s["foot"], 13, bold=True, color=RGBColor(0x8F, 0xD4, 0xE6),
          first=True, spacing=1.4)


RENDERERS = {"title": render_title, "stats": render_stats, "cards": render_cards,
             "flow": render_flow, "closing": render_closing}


def bidi_warnings(slides):
    """Hebrew strings that start with a digit or a Latin letter.

    In an RTL paragraph such a string is legal and the Unicode algorithm says
    exactly where the leading run belongs — but PowerPoint, LibreOffice and
    Google Slides do not agree on it, and the failure looks like "430,057
    קישורים" rendered as "קישורים 430,057", which reads as a different claim.
    Rather than trust a renderer, the deck is authored so every Hebrew string
    opens with a Hebrew letter, and this reports the ones that do not.
    """
    def hebrew(s):
        return any("֐" <= c <= "׿" for c in s)

    out = []

    def walk(value, where):
        if isinstance(value, str):
            if hebrew(value) and not ("֐" <= value[0] <= "׿"):
                out.append("%s: %s" % (where, value[:60]))
        elif isinstance(value, dict):
            for k, v in value.items():
                walk(v, "%s.%s" % (where, k))
        elif isinstance(value, (list, tuple)):
            for i, v in enumerate(value):
                walk(v, "%s[%d]" % (where, i))

    for i, s in enumerate(slides, 1):
        walk(s, "slide %02d" % i)
    return out


def build(slides, path):
    prs = Presentation()
    prs.slide_width, prs.slide_height = Inches(W), Inches(H)
    blank = prs.slide_layouts[6]
    total = len(slides)
    for i, s in enumerate(slides, 1):
        slide = prs.slides.add_slide(blank)
        if s["kind"] not in ("title", "closing"):
            _background(slide, WHITE)
        RENDERERS[s["kind"]](slide, s, i, total)
    prs.save(path)
    return path


if __name__ == "__main__":
    from spatial_deck_content import SLIDES  # noqa: E402
    here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    out = os.path.join(here, "over-spatial-deck.pptx")

    warnings = bidi_warnings(SLIDES)
    if warnings:
        print("bidi: %d string(s) open with a digit or Latin letter — a "
              "renderer may place that run on the wrong side:" % len(warnings))
        for w in warnings:
            print("  " + w)
        sys.exit(1)

    build(SLIDES, out)
    print("wrote %s (%d slides, %.0f KB)"
          % (out, len(SLIDES), os.path.getsize(out) / 1024))
