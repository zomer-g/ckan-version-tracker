from datetime import datetime, timezone

from sqlalchemy import DateTime, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class SqlShare(Base):
    """One shared /data console view, addressable by a short slug.

    The console used to carry the whole query in the link (`?q=` base64), which
    made a real query's URL kilobytes long — and, past the 4,000-character
    encoded cap, produced no link at all: the share button silently degraded to
    copying raw SQL. Storing the query here decouples the link's length from the
    query's, so `/s/AbC12345` works for a 200-character query and a 40,000-
    character one alike.

    Rows are PERMANENT on purpose. A shared link's whole job is to keep working
    after it has been pasted into a document, a ticket or an article; an expiry
    would turn every such reference into a dead end months later, silently.

    Dedup is by ``content_hash`` (sha256 of the exact sql + params pair), so
    pressing share twice on the same view returns the same slug instead of
    growing the table. That also bounds the damage an abusive writer can do:
    replaying one payload costs one row, not N.
    """

    __tablename__ = "sql_shares"

    # Short, URL-safe, case-sensitive id — what actually appears in the link.
    slug: Mapped[str] = mapped_column(String(16), primary_key=True)
    # sha256 hex of sql + "\n" + params. Unique: the dedup key.
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True, index=True)
    # The query itself. Text, not String(n): the point of this table is that the
    # query has no practical length limit (the API caps the request body).
    sql_text: Mapped[str] = mapped_column(Text, nullable=False)
    # The view around the query — chart type, axis mapping, map colouring, the
    # selected table. Stored as a raw query string of KNOWN console keys only
    # (the API filters), never a free-form redirect target.
    params: Mapped[str | None] = mapped_column(String(2048))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )
    # Cheap usage signal: which shared links are actually being opened. Updated
    # best-effort on resolve; a failed bump never fails the read.
    view_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_viewed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
