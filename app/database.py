import ssl as _ssl

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from app.config import settings


def _prepare_db_url_and_args() -> tuple[str, dict]:
    """Strip sslmode from URL (asyncpg doesn't support it) and set SSL via connect_args."""
    url = settings.database_url
    connect_args: dict = {}

    # asyncpg doesn't accept sslmode as a query param — remove it
    if "sslmode=" in url:
        # Remove sslmode param from URL
        from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        params.pop("sslmode", None)
        new_query = urlencode(params, doseq=True)
        url = urlunparse(parsed._replace(query=new_query))

    # Neon requires SSL
    if "neon.tech" in url or "neon" in settings.database_url:
        ssl_context = _ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = _ssl.CERT_NONE
        connect_args = {"ssl": ssl_context, "statement_cache_size": 0}

    return url, connect_args


db_url, connect_args = _prepare_db_url_and_args()
engine = create_async_engine(db_url, echo=False, connect_args=connect_args)
async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


async def get_db():
    async with async_session() as session:
        yield session
