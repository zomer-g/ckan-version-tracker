import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.exceptions import HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.api.auth import router as auth_router
from app.api.oauth import router as oauth_router
from app.api.proxy import router as proxy_router
from app.api.datasets import router as datasets_router
from app.api.versions import router as versions_router
from app.api.admin import router as admin_router
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
app.include_router(admin_router)

# Serve frontend SPA (built by Vite)
frontend_dist = Path(__file__).parent.parent / "frontend" / "dist"
index_html = frontend_dist / "index.html"

if frontend_dist.exists() and index_html.exists():
    logger.info("Frontend dist found at %s", frontend_dist)

    # Mount Vite's hashed static assets
    assets_dir = frontend_dist / "assets"
    if assets_dir.exists():
        app.mount("/assets", StaticFiles(directory=str(assets_dir)), name="assets")

    # SPA fallback: intercept 404s on non-API routes and serve index.html
    @app.exception_handler(StarletteHTTPException)
    async def spa_fallback(request: Request, exc: StarletteHTTPException):
        # Only intercept 404s; let other HTTP errors pass through
        if exc.status_code != 404:
            return JSONResponse({"detail": exc.detail}, status_code=exc.status_code)
        # Don't intercept API 404s — return JSON
        if request.url.path.startswith("/api/"):
            return JSONResponse({"detail": exc.detail}, status_code=404)
        # Check if it's an actual static file in dist/ (e.g. favicon.svg)
        static_path = frontend_dist / request.url.path.lstrip("/")
        if static_path.is_file() and str(static_path).startswith(str(frontend_dist)):
            return FileResponse(static_path)
        # For all other 404s, serve the SPA
        return FileResponse(index_html)

    # Explicit root route (some load balancers hit / for health checks)
    @app.get("/")
    async def serve_root():
        return FileResponse(index_html)
else:
    logger.warning("Frontend dist not found at %s — SPA disabled", frontend_dist)
