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

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/auth/sso/google` | Google SSO login |
| `GET` | `/api/datasets` | List tracked datasets |
| `POST` | `/api/datasets` | Track a new dataset |
| `POST` | `/api/datasets/{id}/poll` | Trigger manual poll |
| `GET` | `/api/datasets/{id}/versions` | List version history |
| `GET` | `/api/ckan/search?q=...` | Search data.gov.il |

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
