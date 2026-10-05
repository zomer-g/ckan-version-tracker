# Sourced by launch.sh. Maps what xhostd injects onto the names OVER reads.
# Kept apart from launch.sh so tests/test_xhostd_env.py can run it on its own.
#
# xhostd injects DATABASE_URL (reserved: it cannot be set) and
# DATABASE_URL_READONLY for the channel's own Postgres, as postgres:// URLs.
# OVER reads DATABASE_URL through SQLAlchemy's asyncpg dialect, and reads the
# archive, the read-only console role, ocal and ocoi from their own variables.
#
# One database is the xhostd topology, so each of those defaults to the injected
# one. OVER_DATABASE_URL exists only because DATABASE_URL is reserved: it points
# the app at another database, and then the archive variables must be given
# explicitly too, or the app would pair that database with the platform's
# read-only role. Neon is retired: app/main.py refuses to boot on a Neon address.

over_to_asyncpg() {
  case "$1" in
    postgresql+asyncpg://*) printf '%s' "$1" ;;
    postgresql://*) printf 'postgresql+asyncpg://%s' "${1#postgresql://}" ;;
    postgres://*) printf 'postgresql+asyncpg://%s' "${1#postgres://}" ;;
    *) printf '%s' "$1" ;;
  esac
}

if [ -n "${OVER_DATABASE_URL:-}" ]; then
  if [ -z "${APPEND_DATABASE_URL:-}" ] || [ -z "${APPEND_READONLY_DATABASE_URL:-}" ]; then
    echo "xhostd-env: OVER_DATABASE_URL points the app elsewhere, so APPEND_DATABASE_URL and APPEND_READONLY_DATABASE_URL must be set explicitly too" >&2
    return 1 2>/dev/null || exit 1
  fi
fi

# xhostd's DATABASE_URL can reach Postgres through a transaction-mode PgBouncer,
# which hands a server connection to another client after every transaction.
# DATABASE_URL_DIRECT reaches Postgres itself. What needs its session to last
# uses the direct address: alembic, and the ocal and ocoi pools, whose
# search_path is a startup parameter the pooler refuses. Without a pooler the
# direct address is DATABASE_URL itself.
if [ -n "${OVER_DATABASE_URL:-}" ]; then
  DATABASE_URL_DIRECT=$(over_to_asyncpg "$OVER_DATABASE_URL")
else
  DATABASE_URL_DIRECT=$(over_to_asyncpg "${DATABASE_URL_DIRECT:-${DATABASE_URL:-}}")
fi
export DATABASE_URL_DIRECT
archive_direct="${APPEND_DATABASE_URL:-$DATABASE_URL_DIRECT}"

# 2026-10-05: the app engine goes direct as well. Through the transaction-mode
# pooler the write role has 5 server slots. Scheduler jobs keep a transaction
# open for minutes, the 5 slots filled, and every further query of the role
# waited on the pooler: /api answered 500 after 30s, and the boot grants script
# waited on its first query until the 120s health check killed the container.
# A direct connection costs one of the role's 15, so app/database.py caps the
# engine's pool; the read-only console role keeps the pooler.
DATABASE_URL="$DATABASE_URL_DIRECT"
export DATABASE_URL
export APPEND_DATABASE_URL="${APPEND_DATABASE_URL:-$DATABASE_URL}"
export APPEND_READONLY_DATABASE_URL="${APPEND_READONLY_DATABASE_URL:-${DATABASE_URL_READONLY:-}}"
export OCAL_DATABASE_URL="${OCAL_DATABASE_URL:-$archive_direct}"
export OCOI_DATABASE_URL="${OCOI_DATABASE_URL:-$archive_direct}"
# See render.yaml: caps glibc malloc arenas, the other half of the OOM fix.
export MALLOC_ARENA_MAX="${MALLOC_ARENA_MAX:-2}"
