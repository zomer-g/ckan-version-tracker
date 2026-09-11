#!/bin/sh
# xhostd build step: runs once at build time, as root, with NO env injected
# (no DATABASE_URL, no secrets). So unlike render.yaml's buildCommand there are
# no migrations here; launch.sh runs them, where the database exists.
set -eu
python3 --version
node --version
# PostgreSQL 18 client from PGDG, for scripts/xhostd_archive_loader.py (pg_dump |
# psql from Neon into this channel's database). Proven in the dress rehearsal.
apt-get update -qq
apt-get install -y -qq --no-install-recommends curl ca-certificates gnupg >/dev/null
install -d /usr/share/postgresql-common/pgdg
curl -sS --fail -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc   https://www.postgresql.org/media/keys/ACCC4CF8.asc
CODENAME=$(. /etc/os-release && echo "$VERSION_CODENAME")
echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt ${CODENAME}-pgdg main"   > /etc/apt/sources.list.d/pgdg.list
apt-get update -qq
apt-get install -y -qq --no-install-recommends postgresql-client-18 >/dev/null
rm -rf /var/lib/apt/lists/*
/usr/lib/postgresql/18/bin/pg_dump --version
pip install --no-cache-dir --root-user-action=ignore -r requirements.txt
cd frontend
npm ci
npm run build
