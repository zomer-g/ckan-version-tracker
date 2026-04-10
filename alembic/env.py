import asyncio
import ssl as _ssl
from logging.config import fileConfig
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

from alembic import context
from sqlalchemy import pool
from sqlalchemy.ext.asyncio import async_engine_from_config

from app.config import settings
from app.database import Base
from app.models import User, TrackedDataset, VersionIndex  # noqa: F401

config = context.config

# Strip sslmode from URL — asyncpg doesn't support it as a query param
raw_url = settings.database_url
if "sslmode=" in raw_url:
    parsed = urlparse(raw_url)
    params = parse_qs(parsed.query)
    params.pop("sslmode", None)
    new_query = urlencode(params, doseq=True)
    raw_url = urlunparse(parsed._replace(query=new_query))

config.set_main_option("sqlalchemy.url", raw_url)

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata

# Neon SSL support via connect_args
connect_args: dict = {}
if "neon.tech" in raw_url or "neon" in settings.database_url:
    ssl_context = _ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = _ssl.CERT_NONE
    connect_args = {"ssl": ssl_context, "statement_cache_size": 0}


def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(url=url, target_metadata=target_metadata, literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection):
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
        connect_args=connect_args,
    )
    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await connectable.dispose()


def run_migrations_online() -> None:
    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
