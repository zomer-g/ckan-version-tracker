# גרסאות לעם — Versions for the People

**https://over.org.il**

מעקב אחר שינויים במאגרי מידע ממשלתיים פתוחים בישראל.

Tracking changes to Israeli government open data — datasets, registries, tenders,
Knesset records and more — and preserving version history for citizens,
journalists, and researchers.

---

## מה זה?

מאגרי מידע ממשלתיים רבים מתעדכנים ע"י דריסת המידע הקיים — כלומר, ברגע שמידע חדש
עולה, המידע הישן נמחק. **גרסאות לעם** עוקב אחרי מקורות מידע ממשלתיים, שומר עותק של
כל גרסה, ומאפשר לצפות בשינויים לאורך זמן, להשוות בין גרסאות, ולהוריד את הקבצים
ההיסטוריים.

המערכת עוקבת גם אחרי [data.gov.il](https://data.gov.il) וגם אחרי עשרות מקורות
נוספים (הכנסת, למ"ס, GovMap, מכרזים, מרשמים של משרד הבריאות, מרכז המחקר והמידע של
הכנסת ועוד). כל גרסה נשמרת ב-Cloudflare R2, ומאגרים טבלאיים נטענים גם ל-Postgres
(Neon) כדי שניתן יהיה לשאול אותם דרך API.

## Features

- **Automatic change detection** — a scheduler polls each tracked source on a
  configurable cadence and only creates a new version when the content actually
  changed (content-hash / row-hash diffing, no empty versions).
- **Many source types** — CKAN (`data.gov.il`), Knesset ODATA-v4, whole-corpus
  scrapers (ממ"מ, חוזרי מנכ"ל החינוך), incremental tender archives (JDA, עדן),
  GovMap spatial layers, CBS content index, append-only registries (רכב, החלטות
  ממשלה), and more.
- **Versioned file storage on R2** — every version's files are mirrored to
  Cloudflare R2 (zero egress) and served from a public bucket domain; large
  attachments upload direct-to-R2 via presigned multipart. A per-dataset
  override can still route to `local`, `odata`, `neon`, or `append`.
- **NEON datastore** — tabular resources are parsed and loaded into Postgres so
  history can be queried via API; append-only sources accumulate rows keyed by a
  stable identifier.
- **Version history & diff** — browse, compare, and download any two versions of
  a tracked dataset.
- **Knesset DB mirror + SQL console** — all ~48 Knesset ODATA-v4 tables mirrored
  into a `knesset` schema, with a read-only `/knesset` SQL page and full-text
  "deep search" over document bodies.
- **Public REST API** — a stable, read-only `/api/v1` surface with a per-IP byte
  budget to guard against bulk scraping.
- **Remote MCP server** — `over.org.il/mcp` exposes the version data to
  Claude.ai / ChatGPT / Cursor via OAuth 2.1 + PKCE (invite-only).
- **Google SSO + admin approval** — secure login via Google OAuth2; new tracking
  requests require admin approval.
- **Bilingual UI** — Hebrew (RTL) and English with toggle; About/Rationale copy
  is editable at runtime from the admin panel.

## Architecture

```
                         ┌─────────────────────────────┐
   over.org.il  ──────>  │   FastAPI backend + APScheduler
   (React SPA)           │   ├─ /api/v1  (public, budgeted)
                         │   ├─ /api/... (internal SPA)
                         │   ├─ /mcp      (remote MCP, OAuth)
                         │   └─ scheduler (poll / dispatch)
                         └───────┬─────────────────┬─────┘
                                 │                 │
                    ┌────────────┘                 └─────────────┐
                    v                                            v
          ┌──────────────────┐                       ┌────────────────────────┐
          │  PostgreSQL (Neon)│                      │  GOVSCRAPER worker       │
          │  • metadata/versions                     │  (zomer-g/govil-scraper) │
          │  • NEON datastore │                      │  self-updating, version- │
          │  • knesset schema │                      │  pinned; scrapes sources │
          └──────────────────┘                       └───────────┬────────────┘
                    ^                                             │
                    │            ┌──────────────────┐            │
                    └────────────┤  Cloudflare R2    │<───────────┘
                                 │  (version files)  │
                                 └──────────────────┘

   sources: data.gov.il · Knesset ODATA · CBS · GovMap · ממ"מ · registries · tenders · …
```

Heavy scraping runs in an **external worker repo** (`zomer-g/govil-scraper`)
that polls the backend for tasks, downloads/transforms the data, and writes
files to R2 + rows to NEON. The worker auto-pulls and re-execs between tasks; the
backend refuses to dispatch to a worker whose git SHA doesn't match the pinned
`worker_required_version`.

## Local Development

```bash
git clone https://github.com/zomer-g/ckan-version-tracker.git
cd ckan-version-tracker

python -m venv .venv
source .venv/bin/activate    # Linux/macOS
.venv\Scripts\activate       # Windows
pip install -r requirements.txt

cp .env.example .env
# Edit .env with your DATABASE_URL, JWT_SECRET_KEY, R2 credentials, etc.

alembic upgrade head
uvicorn app.main:app --reload

# Frontend (separate terminal)
cd frontend && npm install && npm run dev
```

## Deployment

Deployed on [Render](https://render.com) with the `render.yaml` blueprint.
Database on [Neon](https://neon.tech) (PostgreSQL). Version files on
[Cloudflare R2](https://developers.cloudflare.com/r2/). The scraping worker is
deployed separately from `zomer-g/govil-scraper`.

### Environment Variables

| Variable | Required | Description |
|---|---|---|
| `DATABASE_URL` | Yes | PostgreSQL connection string (`postgresql+asyncpg://...`) |
| `JWT_SECRET_KEY` | Yes | Secret for JWT tokens |
| `STORAGE_BACKEND` | No | Default file backend: `r2` (default) or `odata` |
| `S3_ENDPOINT` / `S3_BUCKET` / `S3_PUBLIC_BASE_URL` | For R2 | Cloudflare R2 endpoint, bucket, and public download domain |
| `APPEND_DATABASE_URL` | No | Postgres URL for the NEON datastore / append tables |
| `WORKER_API_KEY` | No | Shared key the GOVSCRAPER worker authenticates with |
| `WORKER_REQUIRED_VERSION` | No | Pinned git SHA of the accepted worker build |
| `API_DAILY_BYTE_BUDGET` / `API_BUDGET_ENABLED` | No | Per-IP rolling byte budget on the public API |
| `GOOGLE_CLIENT_ID` / `GOOGLE_CLIENT_SECRET` | No | Google OAuth2 credentials |
| `APP_BASE_URL` | No | Public URL (`https://over.org.il`) |
| `ODATA_API_KEY` | No | Legacy: API key for writing to odata.org.il |

## API

A stable, read-only public API lives under `/api/v1/` — see
**[docs/API.md](docs/API.md)** for the full reference. It is rate-limited by a
per-IP daily byte budget; exceeding it returns `429`. Highlights:

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/v1/datasets` | List datasets, filter by `organization_id`, `tag_id` / `tag` (AND-combined), `status` |
| `GET` | `/api/v1/datasets/{id}` | One dataset by UUID |
| `GET` | `/api/v1/datasets/{id}/versions` | Version history with source + download URLs |
| `GET` | `/api/v1/datasets/{id}/versions/latest` | The most recent version |
| `GET` | `/api/v1/datasets/{id}/versions/{number}` | A specific version by number |
| `GET` | `/api/v1/tags` | All tags with dataset counts |
| `GET` | `/api/v1/tags/{id}` | One tag + every dataset under it |
| `GET` | `/api/v1/organizations` | All organizations |

Internal SPA endpoints (`/api/auth`, `/api/datasets` POST/PATCH/DELETE,
`/api/admin/...`) and the worker dispatch endpoints are not part of the public
contract and may change. See **[docs/WORKER_API.md](docs/WORKER_API.md)** for the
worker protocol.

## Tech Stack

- **Backend:** Python 3.11, FastAPI, SQLAlchemy 2.0 (async), Alembic, APScheduler
- **Frontend:** React 19, TypeScript, Vite, react-i18next
- **Database:** PostgreSQL (Neon) — operational DB + `knesset` schema + NEON datastore
- **File storage:** Cloudflare R2 (S3-compatible)
- **Worker:** separate `zomer-g/govil-scraper` repo (self-updating, version-pinned)
- **Auth:** Google OAuth2, JWT (PyJWT); MCP via OAuth 2.1 + PKCE
- **Deployment:** Render

## License

[MIT](LICENSE) — Guy Zomer

## Contributing

1. Fork → branch → commit → PR
2. Issues and feature requests welcome
