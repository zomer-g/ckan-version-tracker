import ssl as _ssl
from urllib.parse import urlsplit

from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from app.config import parse_pg_target, settings

# The application's own tables live in schema `app`; credentials in `auth`.
# `public` is for published data only, because on xhostd the platform's read-only
# role (the public SQL console) can read everything in it. See alembic 066.
APP_SCHEMA = "app"
APP_SEARCH_PATH = "app, public"
APP_TABLES = frozenset({
    "activity_log",
    "api_access_log",
    "cbs_featured",
    "cbs_feedback",
    "cbs_gazetteer",
    "cbs_index",
    "dataset_tags",
    "datastore_push_jobs",
    "decision_analysis",
    "drive_export_jobs",
    "govmap_coverage",
    "llm_daily_usage",
    "nl_query_cache",
    "nl_query_config",
    "nl_query_log",
    "nl_suggest_log",
    "nl_synonyms",
    "organizations",
    "page_content",
    "scrape_tasks",
    "source_limits",
    "source_registry",
    "sql_shares",
    "tags",
    "tracked_datasets",
    "version_index",
    "workers",
})


def _is_pgbouncer(url: str) -> bool:
    # Neon's pooler endpoints carry "-pooler" in the host. On xhostd, DATABASE_URL
    # goes through a transaction-mode PgBouncer when it differs from
    # DATABASE_URL_DIRECT. In transaction mode PgBouncer hands a server connection
    # to whichever client comes next, so a session SET would leak to others and be
    # missing for us.
    if "-pooler" in (urlsplit(url).hostname or ""):
        return True
    direct = settings.database_url_direct
    return bool(direct) and parse_pg_target(url) != parse_pg_target(direct)


def app_db_connect_args(url: str) -> dict:
    """connect_args for the APP database engine (and alembic).

    Not for the archive pools: they must keep the default search_path so that
    append tables are created in `public`, where the console can read them.
    """
    connect_args: dict = {}
    # Neon requires SSL
    if "neon" in url:
        ssl_context = _ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = _ssl.CERT_NONE
        connect_args = {"ssl": ssl_context, "statement_cache_size": 0}
    return connect_args


def app_search_path_applies(url: str) -> bool:
    """Whether the app engine pins its search_path on each new connection."""
    return url.startswith("postgresql") and not _is_pgbouncer(url)


def install_app_search_path(sync_engine, url: str) -> None:
    """Pin `search_path = app, public` for the APP engine.

    On a direct connection, a SET inside the session on every new connection, not
    a startup parameter. Measured on a Neon branch: asyncpg's server_settings
    search_path did not take effect through Neon's endpoint, while SET SESSION
    did. Autocommit around it, because a SET issued inside a transaction that is
    later rolled back is rolled back with it.

    Behind a transaction-mode pooler (xhostd's DATABASE_URL), a SET LOCAL at the
    start of every transaction instead. It ends with the transaction, so it never
    reaches the next client of that server connection, and every transaction of
    ours sets it again. xhostd's pooler also refuses search_path as a startup
    parameter.
    """
    if app_search_path_applies(url):
        @event.listens_for(sync_engine, "connect")
        def _pin_app_search_path(dbapi_connection, _connection_record):
            previous = dbapi_connection.autocommit
            dbapi_connection.autocommit = True
            cursor = dbapi_connection.cursor()
            cursor.execute(f"SET SESSION search_path TO {APP_SEARCH_PATH}")
            cursor.close()
            dbapi_connection.autocommit = previous
    elif url.startswith("postgresql"):
        @event.listens_for(sync_engine, "begin")
        def _pin_app_search_path_per_transaction(conn):
            conn.exec_driver_sql(f"SET LOCAL search_path TO {APP_SEARCH_PATH}")


def _prepare_db_url_and_args() -> tuple[str, dict]:
    """Strip sslmode from URL (asyncpg doesn't support it) and build connect_args."""
    url = settings.database_url

    # asyncpg doesn't accept sslmode as a query param — remove it
    if "sslmode=" in url:
        # Remove sslmode param from URL
        from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        params.pop("sslmode", None)
        new_query = urlencode(params, doseq=True)
        url = urlunparse(parsed._replace(query=new_query))

    return url, app_db_connect_args(url)


db_url, connect_args = _prepare_db_url_and_args()
engine = create_async_engine(
    db_url,
    echo=False,
    # Errors must not carry bound values into the logs: they include visitor
    # IPs and emails (api_access_log) and tokens (auth).
    hide_parameters=True,
    connect_args=connect_args,
    pool_recycle=300,        # Recycle connections after 5 min (Neon idle timeout)
    pool_pre_ping=True,      # Test connections before use (detects closed connections)
)
install_app_search_path(engine.sync_engine, db_url)
async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


async def get_db():
    async with async_session() as session:
        yield session


async def release_connection(db: AsyncSession) -> None:
    """Give the request's pooled connection back before slow work that isn't ours.

    A session checks a connection out on its first query and keeps it — idle in
    transaction — until it closes, and get_db closes it only when the request
    ends. A route that reads a row and then waits on something else (the append
    archive's own asyncpg pool, a 45-second ILIKE over 3.8M rows, a ZIP streamed
    from R2) held one of the pool's 15 connections for that whole wait. Fifteen
    such requests and every other route in the app timed out on
    "QueuePool limit of size 5 overflow 10 reached" (2026-09-27: ~12,000 of them).

    Ending the read transaction returns the connection. expire_on_commit=False
    keeps the loaded objects readable, and the session opens a fresh transaction
    if it is used again. Read paths only: pending writes would be committed, so
    they are refused rather than published by accident.
    """
    if db.new or db.dirty or db.deleted:
        raise RuntimeError("release_connection() on a session with pending writes")
    if db.in_transaction():
        await db.commit()
