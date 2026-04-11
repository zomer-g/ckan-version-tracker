# CKAN Version Tracker

**Tracking changes to Israeli government datasets / מעקב אחר שינויים במאגרי מידע ממשלתיים**

---

## Description

CKAN Version Tracker monitors datasets published on Israel's national open-data portal ([data.gov.il](https://data.gov.il)) and captures a versioned history of every change. Each time a tracked dataset is modified, the system downloads a snapshot and mirrors it to [odata.org.il](https://www.odata.org.il) so that citizens, journalists, and researchers can compare any two points in time.

## Features

- **Automatic change detection** -- background worker polls data.gov.il on a configurable schedule
- **Resource-level diffing** -- SHA-256 hashing detects which files actually changed between versions
- **Version snapshots** -- metadata and data files are archived on odata.org.il as CKAN resources
- **Datastore integration** -- CSV resources are parsed and pushed into the CKAN DataStore for SQL-style querying
- **Google SSO authentication** -- secure login via Google OAuth2
- **Admin approval workflow** -- new dataset tracking requests require admin approval
- **REST API** -- full CRUD for tracked datasets, version history, and manual poll triggers
- **Frontend dashboard** -- React SPA for browsing tracked datasets and their version history
- **CKAN proxy** -- transparent proxy for searching data.gov.il from the frontend

## Architecture

```
+------------------+       +-----------------+       +------------------+
|    Frontend      | <---> |    FastAPI       | <---> |   PostgreSQL     |
|  (React + Vite)  |       |    Backend       |       |   (Neon)         |
+------------------+       +-----------------+       +------------------+
                                  |   ^
                    +-------------+   +------------------+
                    v                                     |
            +--------------+                   +-------------------+
            | data.gov.il  |                   |   odata.org.il    |
            | (source)     |                   |   (mirror/archive)|
            +--------------+                   +-------------------+
                    ^
                    |
            +--------------+
            | APScheduler  |
            | (worker)     |
            +--------------+
```

## Prerequisites

- Python 3.11+
- Node.js 20+
- PostgreSQL (or a free [Neon](https://neon.tech) instance)
- A CKAN API key for odata.org.il (optional, for mirroring)
- Google OAuth2 credentials (for authentication)

## Local Development

```bash
# 1. Clone the repository
git clone https://github.com/zomer-g/ckan-versions.git
cd ckan-versions

# 2. Create a virtual environment and install Python dependencies
python -m venv .venv
source .venv/bin/activate        # Linux/macOS
.venv\Scripts\activate           # Windows
pip install -r requirements.txt

# 3. Copy and configure environment variables
cp .env.example .env
# Edit .env with your DATABASE_URL, JWT_SECRET_KEY, etc.

# 4. Run database migrations
alembic upgrade head

# 5. Start the backend
uvicorn app.main:app --reload

# 6. In a separate terminal, build/run the frontend
cd frontend
npm install
npm run dev
```

The backend serves on `http://localhost:8000` and the frontend dev server on `http://localhost:5173`.

## Deployment (Render)

The project includes a `render.yaml` blueprint for one-click deployment on [Render](https://render.com):

1. Connect the GitHub repository to Render.
2. Create a new **Blueprint** and point it at `render.yaml`.
3. Set the required environment variables (see below) in the Render dashboard.
4. Render will build the frontend, run migrations, and start the server.

The frontend is built as static files and served by FastAPI alongside the API.

## Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `DATABASE_URL` | Yes | -- | PostgreSQL connection string (`postgresql+asyncpg://...`) |
| `JWT_SECRET_KEY` | Yes | -- | Secret key for signing JWT tokens |
| `JWT_ALGORITHM` | No | `HS256` | JWT signing algorithm |
| `JWT_EXPIRY_MINUTES` | No | `1440` | JWT token lifetime in minutes (default 24h) |
| `DATA_GOV_IL_URL` | No | `https://data.gov.il` | Base URL for the source CKAN instance |
| `ODATA_URL` | No | `https://www.odata.org.il` | Base URL for the mirror CKAN instance |
| `ODATA_API_KEY` | No | -- | API key for writing to odata.org.il |
| `ODATA_OWNER_ORG` | No | `zomer` | Organization slug on odata.org.il |
| `DEFAULT_POLL_INTERVAL` | No | `604800` | Default polling interval in seconds (1 week) |
| `MIN_POLL_INTERVAL` | No | `300` | Minimum allowed polling interval in seconds |
| `MAX_RESOURCE_DOWNLOAD_SIZE` | No | `500000000` | Max resource file size to download (bytes) |
| `CORS_ORIGINS` | No | -- | Comma-separated list of allowed CORS origins |
| `APP_BASE_URL` | No | `http://localhost:8000` | Public URL of the app (used for OAuth redirects) |
| `GOOGLE_CLIENT_ID` | No | -- | Google OAuth2 client ID |
| `GOOGLE_CLIENT_SECRET` | No | -- | Google OAuth2 client secret |

## API Endpoints

### Authentication
| Method | Path | Description |
|---|---|---|
| `GET` | `/api/auth/me` | Get current user profile |
| `GET` | `/api/auth/sso/google` | Initiate Google SSO login |
| `GET` | `/api/auth/sso/google/callback` | Google OAuth callback |
| `GET` | `/api/auth/sso/providers` | List available SSO providers |

### Datasets
| Method | Path | Description |
|---|---|---|
| `GET` | `/api/datasets` | List tracked datasets |
| `POST` | `/api/datasets` | Track a new dataset |
| `PATCH` | `/api/datasets/{id}` | Update tracking settings |
| `DELETE` | `/api/datasets/{id}` | Stop tracking a dataset |
| `POST` | `/api/datasets/{id}/poll` | Manually trigger a poll |

### Versions
| Method | Path | Description |
|---|---|---|
| `GET` | `/api/versions/{dataset_id}` | List versions for a dataset |

### Proxy
| Method | Path | Description |
|---|---|---|
| `GET` | `/api/proxy/ckan/search` | Proxy search to data.gov.il |

## Tech Stack

- **Backend:** Python 3.11, FastAPI, SQLAlchemy 2.0 (async), Alembic
- **Frontend:** React, TypeScript, Vite, Tailwind CSS
- **Database:** PostgreSQL (Neon free tier)
- **Worker:** APScheduler (background polling)
- **Auth:** Google OAuth2, JWT (PyJWT)
- **HTTP Client:** httpx (async)
- **Deployment:** Render

## License

[MIT](LICENSE)

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Commit your changes
4. Push to the branch (`git push origin feature/my-feature`)
5. Open a Pull Request
