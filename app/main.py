import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from app.api.auth import router as auth_router
from app.api.oauth import router as oauth_router
from app.api.proxy import router as proxy_router
from app.api.datasets import router as datasets_router
from app.api.versions import router as versions_router
from app.config import settings
from app.rate_limit import limiter
from app.worker.scheduler import init_scheduler, shutdown_scheduler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting CKAN Version Tracker")
    await init_scheduler()
    yield
    shutdown_scheduler()
    logger.info("Shutting down CKAN Version Tracker")


app = FastAPI(
    title="CKAN Version Tracker",
    description="Track and version government datasets from data.gov.il",
    version="1.0.0",
    lifespan=lifespan,
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# CORS — restrict to configured origins, fall back to permissive only in dev
cors_origins = settings.get_cors_origins()
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins if cors_origins else ["*"],
    allow_credentials=bool(cors_origins),
    allow_methods=["*"],
    allow_headers=["*"],
)

# API routes
app.include_router(auth_router)
app.include_router(oauth_router)
app.include_router(proxy_router)
app.include_router(datasets_router)
app.include_router(versions_router)

# Serve frontend static files (built by Vite)
frontend_dist = Path(__file__).parent.parent / "frontend" / "dist"
if frontend_dist.exists():
    app.mount("/", StaticFiles(directory=str(frontend_dist), html=True), name="frontend")
