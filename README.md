# CKAN Version Tracker

Track and version Israeli government datasets from data.gov.il with historical snapshots on odata.org.il.

## Features

- Monitors datasets on Israel's national data portal (data.gov.il)
- Captures dataset versions and stores historical snapshots
- Scheduled background worker for automatic tracking
- REST API for querying datasets and version history
- Frontend dashboard for browsing tracked datasets
- OAuth and API key authentication
- Rate limiting and proxy support for CKAN API calls

## Tech Stack

- **Backend:** Python, FastAPI, SQLAlchemy, Alembic (migrations)
- **Frontend:** Separate frontend app (see `frontend/`)
- **Worker:** Background scheduler for periodic dataset checks
- **Database:** PostgreSQL
- **Deployment:** Render

## Getting Started

```bash
# Install dependencies
pip install -r requirements.txt

# Run database migrations
alembic upgrade head

# Start the server
uvicorn app.main:app --reload
```

## Deployment

Configured for Render via `render.yaml`.
