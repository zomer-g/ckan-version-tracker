# גרסאות לעם — Versions for the People

**https://over.org.il**

מעקב אחר שינויים במאגרי מידע ממשלתיים פתוחים בישראל.

Tracking changes to Israeli government open datasets, preserving version history for citizens, journalists, and researchers.

---

## מה זה?

מאגרי המידע הממשלתיים באתר [data.gov.il](https://data.gov.il) מתעדכנים ע"י דריסת המידע הקיים — כלומר, ברגע שמידע חדש עולה, המידע הישן נמחק. **גרסאות לעם** עוקב אחרי מאגרים ושומר עותק של כל גרסה, כך שניתן לצפות בשינויים לאורך זמן.

הגרסאות נשמרות ב-[מידע לעם](https://www.odata.org.il) כ-Datastore — כלומר ניתן לשאול את הנתונים ההיסטוריים דרך API.

## Features

- **Automatic change detection** — background worker polls data.gov.il on a configurable schedule
- **Datastore integration** — CSV resources are parsed and pushed into CKAN DataStore for API querying
- **Version history** — browse and compare any two versions of a tracked dataset
- **Google SSO** — secure login via Google OAuth2
- **Admin approval** — new tracking requests require admin approval
- **Bilingual UI** — Hebrew (RTL) and English with toggle
- **REST API** — full CRUD for tracked datasets, version history, and manual poll triggers

## Architecture

```
┌──────────────────┐     ┌─────────────────┐     ┌──────────────┐
│  React Frontend  │────>│  FastAPI Backend │────>│  PostgreSQL  │
│  (over.org.il)   │     │  + APScheduler   │     │  (Neon)      │
└──────────────────┘     └────────┬────────┘     └──────────────┘
                                  │
                    ┌─────────────┴──────────────┐
                    v                            v
             ┌──────────────┐          ┌──────────────────┐
             │ data.gov.il  │          │  odata.org.il    │
             │ (source)     │          │  (version mirror)│
             └──────────────┘          └──────────────────┘
```

## Local Development

```bash
git clone https://github.com/zomer-g/ckan-version-tracker.git
cd ckan-version-tracker

python -m venv .venv
source .venv/bin/activate    # Linux/macOS
.venv\Scripts\activate       # Windows
pip install -r requirements.txt

cp .env.example .env
# Edit .env with your DATABASE_URL, JWT_SECRET_KEY, etc.

alembic upgrade head
uvicorn app.main:app --reload

# Frontend (separate terminal)
cd frontend && npm install && npm run dev
```

## Deployment

Deployed on [Render](https://render.com) with `render.yaml` blueprint. Database on [Neon](https://neon.tech) (PostgreSQL free tier).

### Environment Variables

| Variable | Required | Description |
|---|---|---|
| `DATABASE_URL` | Yes | PostgreSQL connection string (`postgresql+asyncpg://...`) |
| `JWT_SECRET_KEY` | Yes | Secret for JWT tokens |
| `ODATA_API_KEY` | No | API key for writing to odata.org.il |
| `APP_BASE_URL` | No | Public URL (`https://over.org.il`) |
| `GOOGLE_CLIENT_ID` | No | Google OAuth2 client ID |
| `GOOGLE_CLIENT_SECRET` | No | Google OAuth2 client secret |

## API

A stable, read-only public API lives under `/api/v1/` — see
**[docs/API.md](docs/API.md)** for the full reference. Highlights:

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/v1/datasets` | List datasets, filter by `organization_id`, `tag_id` / `tag` (AND-combined), `status` |
| `GET` | `/api/v1/datasets/{id}` | One dataset by UUID |
| `GET` | `/api/v1/datasets/{id}/versions` | Version history with source + ODATA download URLs |
| `GET` | `/api/v1/tags` | All tags with dataset counts |
| `GET` | `/api/v1/tags/{id}` | One tag + every dataset under it |
| `GET` | `/api/v1/organizations` | All organizations |

Internal SPA endpoints (`/api/auth`, `/api/datasets` POST/PATCH/DELETE,
`/api/admin/...`) are not part of the public contract and may change.

## Tech Stack

- **Backend:** Python 3.11, FastAPI, SQLAlchemy 2.0, Alembic, APScheduler
- **Frontend:** React 19, TypeScript, Vite, react-i18next
- **Database:** PostgreSQL (Neon)
- **Auth:** Google OAuth2, JWT (PyJWT)
- **Deployment:** Render

## License

[MIT](LICENSE) — Gai Zomer

## Contributing

1. Fork → branch → commit → PR
2. Issues and feature requests welcome
