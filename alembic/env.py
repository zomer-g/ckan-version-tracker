import asyncio
from logging.config import fileConfig
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

from alembic import context
from sqlalchemy import pool
from sqlalchemy.ext.asyncio import async_engine_from_config

from app.config import settings
from app.database import Base, app_db_connect_args, install_app_search_path
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

# Same connection setup as the app engine: SSL for Neon here, and below the same
# search_path = app, public, so a table a migration creates without naming a
# schema lands in `app`.
connect_args: dict = app_db_connect_args(raw_url)


def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(url=url, target_metadata=target_metadata, literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection):
    # alembic_version stays in public wherever search_path points. It is harmless
    # there, and moving it would strand every existing deployment.
    context.configure(connection=connection, target_metadata=target_metadata,
                      version_table_schema="public")
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
        connect_args=connect_args,
    )
    install_app_search_path(connectable.sync_engine, raw_url)
    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await connectable.dispose()


def run_migrations_online() -> None:
    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
