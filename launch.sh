#!/bin/sh
# xhostd runtime step: runs at boot as user `app` with the full env. It must
# answer HTTP on $XHOST_HTTP_PORT within 120 seconds, and exec so uvicorn
# receives stop signals.
set -eu
cd "$(dirname "$0")"
# The channel's own database, saved before xhostd-env.sh maps DATABASE_URL onto
# whatever the app should use (Neon, until the archive switch).
export XHOST_LOCAL_DATABASE_URL="${DATABASE_URL:-}"
. ./xhostd-env.sh
export PYTHONPATH="$PWD"

# Migrations run here, not in install.sh: the build has no database. Two
# instances on one database (wave 2, while Render still runs production) must
# not both migrate, so the second sets RUN_MIGRATIONS=false. A migration that
# needs longer than the boot window is run once by hand before the deploy.
if [ "${RUN_MIGRATIONS:-true}" = "true" ]; then
  python3 -m alembic upgrade head
fi

# The archive copy from Neon (ARCHIVE_LOADER=run) runs beside the web server and
# resumes where it stopped after any restart. See scripts/xhostd_archive_loader.py.
if [ "${ARCHIVE_LOADER:-}" = "run" ]; then
  python3 scripts/xhostd_archive_loader.py &
fi
# The app database (schemas app, auth) from Neon, once per APP_DB_LOADER token.
# See scripts/xhostd_app_db_loader.py.
if [ -n "${APP_DB_LOADER:-}" ]; then
  python3 scripts/xhostd_app_db_loader.py &
fi

# Trusted forwarders: loopback and the private ranges, as on Render, never "*".
# Narrow to the ingress range measured on xhostd once it is known (plan, wave 2).
exec python3 -m uvicorn app.main:app --host 0.0.0.0 --port "$XHOST_HTTP_PORT" \
  --proxy-headers \
  --forwarded-allow-ips="127.0.0.1,::1,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16"
