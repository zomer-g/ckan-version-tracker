"""Shared rendering for the generated PPTX decks of גרסאות לעם.

Two decks are built on this: scripts/build_spatial_deck.py (the mapping/GIS
story) and scripts/build_timeline_deck.py (the chronological one). Each owns a
content module of plain data; everything about how a slide LOOKS lives here, so
the two cannot drift into different typography, palette or margins.

Why generated at all: a .pptx is a zip of XML, so a binary committed on its own
is a dead end — nobody can diff it, correct a figure in it, or rebuild it a
month later. Content is data, rendering is code, and a correction is an edit
plus a re-run.

Hebrew in PowerPoint needs two things python-pptx does not do for you, both
handled in _rtl and _style: the paragraph needs rtl="1" or punctuation lands on
the wrong side, and the run needs a COMPLEX-SCRIPT typeface (<a:cs>) because
font.name only sets the latin one.

A third is an authoring rule the builds ENFORCE through bidi_warnings(): no
Hebrew string may open with a digit or a Latin word. Renderers disagree about
where that leading run belongs, and "430,057 קישורים" coming out as
"קישורים 430,057" reads as a different sentence.
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


def render_chapters(slide, s, n, total):
    """The whole arc on one slide: nine chapters as a grid, before the deck
    walks them one at a time. A reader who sees only this slide should still
    know the shape of the story."""
    y = _head(slide, s["eyebrow"], s["title"], s.get("kicker"))
    items = s["chapters"]
    per_row = s.get("cols", 3)
    rows = [items[i:i + per_row] for i in range(0, len(items), per_row)]
    avail = (H - 0.95) - y
    gap = 0.22
    h = min(1.42, (avail - gap * (len(rows) - 1)) / len(rows))
    for r, row in enumerate(rows):
        w, xs = _cols(per_row, gap)
        for i, ch in enumerate(row):
            cat = CAT[ch.get("cat", "infra")]
            yy = y + r * (h + gap)
            _rect(slide, xs[i], yy, w, h, fill=PAPER, line_color=LINE)
            _rect(slide, xs[i] + w - 0.055, yy + 0.04, 0.055, h - 0.08,
                  fill=cat, shape=MSO_SHAPE.RECTANGLE)
            tf = _textbox(slide, xs[i] + 0.22, yy + 0.16, w - 0.44, h - 0.3)
            _para(tf, ch["num"], 10, bold=True, color=cat, first=True,
                  spacing=1.4, space_after=2)
            _para(tf, ch["title"], 14, bold=True, color=INK, line=1.15,
                  space_after=3)
            _para(tf, ch["when"], 10.5, color=FAINT, line=1.2)
    _footer(slide, n, total)


def render_rail(slide, s, n, total):
    """One chapter: its entries down a rail, in date order.

    The rail is the same device as the HTML timeline — a vertical line on the
    RIGHT, where an RTL reader's eye starts, with a dot per entry coloured by
    category and the date in a chip beside it."""
    y = _head(slide, s["eyebrow"], s["title"], s.get("kicker"))
    entries = s["entries"]
    avail = (H - 0.95) - y
    row = min(0.86, avail / max(len(entries), 1))
    block = row * len(entries)

    # The rail stops at the LAST dot rather than running the height of the
    # block: a line continuing into white space below the final entry reads as
    # "and then something else happened", which is the opposite of what the
    # end of a chapter means.
    rail_x = W - MARGIN - 1.02
    last_dot = y + (len(entries) - 1) * row + 0.17
    _rect(slide, rail_x, y + 0.08, 0.022, max(last_dot - y - 0.08, 0.1),
          fill=LINE, shape=MSO_SHAPE.RECTANGLE)

    for i, e in enumerate(entries):
        cat = CAT[e.get("cat", "infra")]
        yy = y + i * row
        # date chip, hard against the right margin
        _rect(slide, W - MARGIN - 0.82, yy + 0.02, 0.82, 0.26,
              fill=WHITE, line_color=LINE, adj=0.35)
        tf = _textbox(slide, W - MARGIN - 0.82, yy + 0.04, 0.82, 0.24)
        _para(tf, e["date"], 9.5, bold=True, color=FAINT, first=True,
              align=PP_ALIGN.CENTER)
        # the dot on the rail, filled for a milestone and hollow otherwise
        d = 0.17 if e.get("major") else 0.12
        _rect(slide, rail_x + 0.011 - d / 2, yy + 0.09, d, d,
              fill=cat if e.get("major") else WHITE, line_color=cat,
              line_w=1.6, shape=MSO_SHAPE.OVAL)
        tf = _textbox(slide, MARGIN, yy - 0.02, CONTENT_W - 1.22, row)
        _para(tf, e["h"], 13 if e.get("major") else 12.2,
              bold=True, color=INK, first=True, line=1.12)
        if e.get("p"):
            _para(tf, e["p"], 10.5, color=SOFT, line=1.25, space_before=1)
    _footer(slide, n, total)


def render_thread(slide, s, n, total):
    """One theme pulled out of the chronology and laid end to end.

    A chapter slide answers "what happened in July"; this answers "how did the
    map get here", which the date order hides by scattering it across nine
    chapters."""
    y = _head(slide, s["eyebrow"], s["title"], s.get("kicker"))
    items = s["items"]
    per_col = (len(items) + 1) // 2
    cols = [items[:per_col], items[per_col:]]
    w, xs = _cols(2, 0.44)
    avail = (H - 0.95) - y
    row = min(0.78, avail / max(per_col, 1))

    for c, col in enumerate(cols):
        if not col:
            continue
        rail_x = xs[c] + w - 0.92
        last_dot = y + (len(col) - 1) * row + 0.16
        _rect(slide, rail_x, y + 0.08, 0.02, max(last_dot - y - 0.08, 0.1),
              fill=LINE, shape=MSO_SHAPE.RECTANGLE)
        for i, e in enumerate(col):
            cat = CAT[e.get("cat", "spatial")]
            yy = y + i * row
            tf = _textbox(slide, xs[c] + w - 0.86, yy + 0.02, 0.86, 0.24)
            _para(tf, e["date"], 9.5, bold=True, color=FAINT, first=True)
            _rect(slide, rail_x + 0.01 - 0.07, yy + 0.09, 0.14, 0.14,
                  fill=cat, shape=MSO_SHAPE.OVAL)
            tf = _textbox(slide, xs[c], yy - 0.02, w - 1.02, row)
            _para(tf, e["h"], 12.2, bold=True, color=INK, first=True, line=1.12)
            if e.get("p"):
                _para(tf, e["p"], 10, color=SOFT, line=1.22, space_before=1)
    if s.get("note"):
        tf = _textbox(slide, MARGIN, y + row * per_col + 0.16, CONTENT_W, 0.6)
        _para(tf, s["note"], 11.5, color=SOFT, first=True, line=1.35)
    _footer(slide, n, total)


RENDERERS = {"title": render_title, "stats": render_stats, "cards": render_cards,
             "flow": render_flow, "closing": render_closing,
             "chapters": render_chapters, "rail": render_rail,
             "thread": render_thread}


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


def render_to(slides, filename):
    """The whole entry point for a deck: gate, build, report.

    Shared so that a second deck cannot quietly skip the bidi check — the gate
    is the only thing standing between an authoring slip and a sentence that
    renders backwards in front of an audience, and it exits non-zero rather
    than producing a file it knows is wrong.
    """
    out = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), filename)

    warnings = bidi_warnings(slides)
    if warnings:
        print("bidi: %d string(s) open with a digit or Latin letter — a "
              "renderer may place that run on the wrong side:" % len(warnings))
        for w in warnings:
            print("  " + w)
        sys.exit(1)

    build(slides, out)
    print("wrote %s (%d slides, %.0f KB)"
          % (out, len(slides), os.path.getsize(out) / 1024))
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
