"""Locality gazetteer for the CBS search (סמל יישוב registry).

One row per Israeli locality, sourced from the CBS "קובץ היישובים" (bycode)
file — the same flagship file the WhatsApp benchmark points people to. It
powers two things the plain index cannot do:

* Resolving a place NAME in a free-text question ("כמה עולים יש בבית שמש")
  to a locality entity, its district/נפה and its geo level — 22 benchmark
  questions name a specific place.
* The advanced tab's locality autocomplete (GET /api/cbs/gazetteer).

Loaded via ``POST /api/cbs/gazetteer/load`` (worker-key) from the seed JSON
committed under data/ — see scripts in the govil-scraper repo for regenerating
the seed from a newer bycode edition.
"""
from datetime import datetime, timezone

from sqlalchemy import DateTime, Integer, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class CbsGazetteer(Base):
    __tablename__ = "cbs_gazetteer"

    # CBS locality code (סמל יישוב) — the national key, stable across years.
    code: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    name_en: Mapped[str | None] = mapped_column(Text)
    # Alternate spellings people actually type ("תל אביב" for "תל אביב-יפו").
    aliases: Mapped[list | None] = mapped_column(JSONB)
    district: Mapped[str | None] = mapped_column(Text)          # מחוז
    subdistrict: Mapped[str | None] = mapped_column(Text)       # נפה
    municipal_status: Mapped[str | None] = mapped_column(Text)  # עירייה/מ"מ/מ"א
    regional_council: Mapped[str | None] = mapped_column(Text)
    population: Mapped[int | None] = mapped_column(Integer)
    ses_cluster: Mapped[int | None] = mapped_column(Integer)    # אשכול חברתי-כלכלי

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
