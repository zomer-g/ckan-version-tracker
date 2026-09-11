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
# the app somewhere else while the data still lives there (wave 2: Neon), and
# then the archive variables must be given explicitly too, or the app would pair
# a Neon app database with the platform's read-only role.

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

DATABASE_URL=$(over_to_asyncpg "${OVER_DATABASE_URL:-${DATABASE_URL:-}}")
export DATABASE_URL
export APPEND_DATABASE_URL="${APPEND_DATABASE_URL:-$DATABASE_URL}"
export APPEND_READONLY_DATABASE_URL="${APPEND_READONLY_DATABASE_URL:-${DATABASE_URL_READONLY:-}}"
export OCAL_DATABASE_URL="${OCAL_DATABASE_URL:-$APPEND_DATABASE_URL}"
export OCOI_DATABASE_URL="${OCOI_DATABASE_URL:-$APPEND_DATABASE_URL}"
# See render.yaml: caps glibc malloc arenas, the other half of the OOM fix.
export MALLOC_ARENA_MAX="${MALLOC_ARENA_MAX:-2}"
