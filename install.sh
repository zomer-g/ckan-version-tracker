#!/bin/sh
# xhostd build step: runs once at build time, as root, with NO env injected
# (no DATABASE_URL, no secrets). So unlike render.yaml's buildCommand there are
# no migrations here; launch.sh runs them, where the database exists.
set -eu
python3 --version
node --version
pip install --no-cache-dir --root-user-action=ignore -r requirements.txt
cd frontend
npm ci
npm run build
