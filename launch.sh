#!/bin/sh
# xhostd runtime step: runs at boot as user `app` with the full env. It must
# answer HTTP on $XHOSTD_HTTP_PORT within 120 seconds, and exec so uvicorn
# receives stop signals.
set -eu
cd "$(dirname "$0")"
. ./xhostd-env.sh
export PYTHONPATH="$PWD"

# Migrations run here, not in install.sh: the build has no database. Two
# instances on one database (wave 2, while Render still runs production) must
# not both migrate, so the second sets RUN_MIGRATIONS=false. A migration that
# needs longer than the boot window is run once by hand before the deploy.
if [ "${RUN_MIGRATIONS:-true}" = "true" ]; then
  python3 -m alembic upgrade head
fi

# On the shared channel database, re-assert the console role's function grants
# and hidden-table revokes before serving. See scripts/xhostd_db_grants.py.
if [ -n "${DATABASE_URL:-}" ] && [ "${APPEND_DATABASE_URL:-}" = "${DATABASE_URL:-}" ]; then
  python3 scripts/xhostd_db_grants.py || true
fi


# Trusted forwarders: loopback and the private ranges, as on Render, never "*".
# Measured 2026-11-07: the xhostd ingress (Caddy) writes the real peer into
# X-Forwarded-For and a forged value does not survive it (app/client_ip.py).
# The ingress's own private address is not published, so the range stays the
# private blocks rather than one host.
# xhostd renamed its variables (XHOST_* -> XHOSTD_*, 2026-10); with `set -u` the
# old name alone made every boot fail. New name first, then the old, then PORT.
HTTP_PORT="${XHOSTD_HTTP_PORT:-${XHOST_HTTP_PORT:-${PORT:-}}}"
exec python3 -m uvicorn app.main:app --host 0.0.0.0 --port "$HTTP_PORT" \
  --proxy-headers \
  --forwarded-allow-ips="127.0.0.1,::1,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16"
