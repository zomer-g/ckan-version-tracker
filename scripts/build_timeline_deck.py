"""Build over-timeline-deck.pptx — the chronological story as a presentation.

Content lives in timeline_deck_content.py, rendering in deck_kit.py, and the
canonical source of the story itself is over-timeline.html in the repo root.
To change the deck, update the HTML first (skill `over-timeline`), carry the
new entries into the content module, and run:

    python scripts/build_timeline_deck.py
"""
from deck_kit import render_to

from timeline_deck_content import SLIDES

if __name__ == "__main__":
    render_to(SLIDES, "over-timeline-deck.pptx")
