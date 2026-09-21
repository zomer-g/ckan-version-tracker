"""Build over-spatial-deck.pptx — the mapping/GIS/spatial story of גרסאות לעם.

Content lives in spatial_deck_content.py, rendering in deck_kit.py. To change
the deck, edit the content and run:

    python scripts/build_spatial_deck.py

Every figure in the content module carries a comment naming where it was
measured. They came from production on 2026-09-21, not from memory: GET
/api/nadlan/stats, GET /api/deals/stats, and two queries on the public SQL
console (over_datasets grouped by geometry_status, and a pg_class scan for
tables carrying a PostGIS geometry column).
"""
from deck_kit import render_to

from spatial_deck_content import SLIDES

if __name__ == "__main__":
    render_to(SLIDES, "over-spatial-deck.pptx")
