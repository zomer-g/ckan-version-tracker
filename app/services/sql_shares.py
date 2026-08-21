"""Short links for the /data SQL console.

The console's share button used to hand out a URL carrying the whole query in
`?q=` base64 — kilobytes long, and above the 4,000-character encoded cap no link
at all. Here the query is stored once and addressed by a short slug, so the
link's length no longer tracks the query's.

Two properties are deliberate:

* **Permanent.** A shared link exists to survive being pasted somewhere. An
  expiry would silently break every such reference later, which is worse than
  keeping a few kilobytes of text.
* **Deduped by content.** The same (sql, params) pair always resolves to the
  same slug. Pressing share twice costs nothing, and replaying one payload from
  the public endpoint costs one row rather than N.

The write endpoint is anonymous because the console is: gating it behind a login
would remove the feature from exactly the people it exists for. What bounds it
instead is the size cap, the rate limit on the route, dedup, and the fact that
``params`` is filtered down to the console's own known keys — so a share can
never carry an arbitrary redirect target or attacker-chosen markup.
"""
import hashlib
import logging
import secrets
from datetime import datetime, timezone

from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.sql_share import SqlShare

logger = logging.getLogger(__name__)

# Slug alphabet excludes look-alike characters (0/O, 1/l/I): these ids get read
# aloud, retyped off a slide, and pasted out of a PDF.
_ALPHABET = "23456789abcdefghijkmnpqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ"
_SLUG_LEN = 8

# The console's own URL keys. Anything else in a submitted params string is
# dropped — the stored view is a console view, not a free-form query string.
_ALLOWED_PARAMS = frozenset(
    {"table", "chart", "cx", "cy", "cagg", "csort", "ctop", "cmode", "cflags", "ctitle"}
)

# Generous next to any real query (the 43-type crime analysis is ~2.5 KB) and
# still small enough that the table cannot be used as bulk storage.
MAX_SQL_CHARS = 100_000
MAX_PARAMS_CHARS = 2048


def _new_slug() -> str:
    return "".join(secrets.choice(_ALPHABET) for _ in range(_SLUG_LEN))


def filter_params(raw: str | None) -> str:
    """Keep only the console's known view keys, in a stable order.

    Stable order matters for dedup: the same view reached by two different
    click paths must hash the same, or the table grows a row per path.
    """
    if not raw:
        return ""
    from urllib.parse import parse_qsl, urlencode

    pairs = [
        (k, v)
        for k, v in parse_qsl(raw.lstrip("?"), keep_blank_values=True)
        if k in _ALLOWED_PARAMS
    ]
    pairs.sort()
    return urlencode(pairs)


def _hash(sql: str, params: str) -> str:
    return hashlib.sha256(f"{sql}\n{params}".encode("utf-8")).hexdigest()


async def create(db: AsyncSession, sql: str, params: str | None) -> str:
    """Store a shared view (or find the identical one) and return its slug."""
    sql = (sql or "").strip()
    if not sql:
        raise ValueError("sql is required")
    if len(sql) > MAX_SQL_CHARS:
        raise ValueError(f"query is too long to share (max {MAX_SQL_CHARS:,} characters)")
    clean = filter_params(params)
    if len(clean) > MAX_PARAMS_CHARS:
        raise ValueError("view settings are too long to share")

    digest = _hash(sql, clean)
    existing = await db.scalar(select(SqlShare.slug).where(SqlShare.content_hash == digest))
    if existing:
        return existing

    # A slug collision is astronomically unlikely (56^8); a content_hash
    # collision is not — two people sharing the same view at the same moment
    # race here, and the unique index is what settles it. Catching the violation
    # and re-reading (rather than ON CONFLICT) keeps this portable to the
    # SQLite the tests run on, and both callers still get the same working link.
    row = SqlShare(
        slug=_new_slug(),
        content_hash=digest,
        sql_text=sql,
        params=clean or None,
        created_at=datetime.now(timezone.utc),
        view_count=0,
    )
    db.add(row)
    try:
        await db.commit()
        return row.slug
    except IntegrityError:
        await db.rollback()
        winner = await db.scalar(select(SqlShare.slug).where(SqlShare.content_hash == digest))
        if winner:
            return winner
        raise


async def resolve(db: AsyncSession, slug: str) -> dict | None:
    """Return {sql, params} for a slug, or None. Bumps the view counter."""
    row = (
        await db.execute(
            select(SqlShare.sql_text, SqlShare.params).where(SqlShare.slug == slug)
        )
    ).first()
    if row is None:
        return None
    try:
        await db.execute(
            update(SqlShare.__table__)
            .where(SqlShare.__table__.c.slug == slug)
            .values(
                view_count=SqlShare.__table__.c.view_count + 1,
                last_viewed_at=datetime.now(timezone.utc),
            )
        )
        await db.commit()
    except Exception:  # noqa: BLE001 — a stats bump must never break the read
        await db.rollback()
        logger.warning("sql_shares: view bump failed for %s", slug, exc_info=True)
    return {"sql": row[0], "params": row[1] or ""}
