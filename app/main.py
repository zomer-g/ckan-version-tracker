import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.exceptions import HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import (
    FileResponse,
    HTMLResponse,
    JSONResponse,
    RedirectResponse,
    Response,
)
from fastapi.staticfiles import StaticFiles
from slowapi.errors import RateLimitExceeded
from starlette.exceptions import HTTPException as StarletteHTTPException
from urllib.parse import quote

from app.api.auth import router as auth_router
from app.api.oauth import router as oauth_router
from app.api.proxy import router as proxy_router
from app.api.datasets import router as datasets_router
from app.api.versions import router as versions_router
from app.api.drive import router as drive_router
from app.api.append import router as append_router
from app.api.admin import router as admin_router
from app.api.worker import router as worker_router
from app.api.govil import router as govil_router
from app.api.govmap import router as govmap_router
from app.api.idf import router as idf_router
from app.api.health import router as health_router
from app.api.registries import router as registries_router
from app.api.avodata import router as avodata_router
from app.api.munidata import router as munidata_router
from app.api.emun import router as emun_router
from app.api.servicescompass import router as servicescompass_router
from app.api.mevaker import router as mevaker_router
from app.api.hatzav import router as hatzav_router
from app.api.mankal import router as mankal_router
from app.api.jda import router as jda_router
from app.api.sources import router as sources_router
from app.api.resolve import router as resolve_router
from app.api.resolve import direct_router
from app.api.eden import router as eden_router
from app.api.knesset import router as knesset_router
from app.api.knesset_protocols import router as knesset_protocols_router
from app.api.knesset_db import router as knesset_db_router
from app.api.admin_nl_query import router as admin_nl_query_router
from app.api.nl_query import router as nl_query_router
from app.api.tables import router as tables_router
from app.api.settlements import router as settlements_router
from app.api.seo import router as seo_router
from app.services import seo
from app.api.site_stats import router as site_stats_router
from app.api.connector import router as connector_router
from app.api.cbs import router as cbs_router
from app.api.cbs_ask import router as cbs_ask_router
from app.api.ocal import router as ocal_router
from app.api.ocoi import router as ocoi_router
from app.api.ocoi_admin import router as ocoi_admin_router
from app.api.ocal_admin import router as ocal_admin_router
from app.api.nadlan import router as nadlan_router
from app.api.organizations import router as organizations_router
from app.api.organizations import admin_router as admin_organizations_router
from app.api.tags import router as tags_router
from app.api.tags import admin_router as admin_tags_router
from app.api.admin_mcp_users import router as admin_mcp_users_router
from app.api.page_content import router as page_content_router
from app.api.page_content import admin_router as admin_page_content_router
from app.api.decision_analysis import router as decision_analysis_router
from app.api.decision_analysis import admin_router as admin_decision_analysis_router
from app.api.deep_search import router as deep_search_router
from app.api.v1 import router as v1_router
from app.config import settings
from app.rate_limit import limiter, rate_limit_exceeded_handler
from app.worker.scheduler import init_scheduler, shutdown_scheduler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def _guard_db_separation() -> bool:
    """Report whether the public SQL consoles share a database with the secrets.

    The consoles (/api/append/{id}/sql, /api/tables/sql, /api/knesset-db/sql and
    their CSV exports) execute arbitrary read-only SELECTs over APPEND_DATABASE_URL.
    The credential tables live in DATABASE_URL.

    This used to abort startup whenever the two addresses matched, because the
    only thing then keeping a console away from the credentials was the address.
    That stopped being true with migration 065 and the fail-closed read-only pool:
    every free-SQL path now runs as APPEND_READONLY_DATABASE_URL's role, the
    credentials sit in the `auth` schema that role is never granted, and
    ``_prove_console_role_cannot_read_secrets`` asks Postgres directly what that
    role can read. On xhostd there is one database per app by design, so an
    address match is the normal topology there, not a mistake.

    A match therefore no longer aborts here. It is logged, and it makes the
    capability proof mandatory: see ``lifespan``. Returns True on a match.
    """
    collides, details = settings.append_db_shares_main_db()
    if collides:
        logger.warning(
            "APPEND_DATABASE_URL and DATABASE_URL address the same database (%s). "
            "The schema boundary is the guard: the console capability proof must "
            "pass before startup continues.",
            details["append"],
        )
    return collides


SENSITIVE_SCHEMA = "auth"

# Names that must never be readable by the console role, in ANY schema. The
# schema check below is the real boundary; this list is the tripwire for the way
# that boundary actually gets lost — a future migration that creates a table
# without naming a schema, so it lands in `public`, which the platform's
# read-only role can read automatically and forever.
SENSITIVE_TABLES = frozenset({
    "users",
    "auth_codes",
    "api_users",
    "mcp_oauth_clients",
    "mcp_oauth_codes",
    "mcp_usage_events",
})

# Schemas the console role must hold nothing in: `auth` has the credentials,
# `app` has everything else the application owns (alembic 066).
PRIVATE_SCHEMAS = frozenset({"auth", "app"})


async def _prove_console_role_cannot_read_secrets(*, shared_db: bool = False) -> None:
    """Ask Postgres what the public console's role can actually read.

    ``_guard_db_separation`` compares two connection strings. That is a proxy:
    it infers the property from an address. This asks the database the question
    directly — connect as the role the public consoles use, and enumerate the
    tables it holds SELECT on.

    Why it matters now: on Neon the two databases are physically separate and a
    cross-database read is impossible, so the address comparison is sufficient.
    On xhostd there is one database per app and the platform injects a read-only
    role that automatically holds SELECT on everything in ``public`` — including
    tables created after the grant. There, the boundary is which schema a table
    sits in, and an address tells you nothing about it.

    Fails closed: anything readable that should not be aborts startup.

    ``shared_db`` is True when the consoles and the credentials are in one
    database. It does not change what is checked, only what is logged when no
    read-only role is configured: the consoles are then disabled either way,
    because get_readonly_pool refuses to hand out the read/write pool.
    """
    from app.database import APP_TABLES
    from app.services import append_store

    try:
        pool = await append_store.get_readonly_pool()
    except RuntimeError:
        # No console role configured. get_readonly_pool already refuses to hand
        # out the read/write pool, so no console can run — nothing to prove.
        logger.warning(
            "console capability proof skipped: no read-only role is configured, "
            "so the public SQL consoles are disabled%s.",
            " (and must stay so: they share a database with the credentials)"
            if shared_db else "",
        )
        return

    rows = await pool.fetch(
        """
        SELECT n.nspname AS schema, c.relname AS name
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind IN ('r', 'p', 'v', 'm', 'f')
          AND n.nspname NOT IN ('pg_catalog', 'information_schema')
          AND has_table_privilege(c.oid, 'SELECT')
        """
    )

    exposed = [
        f"{r['schema']}.{r['name']}"
        for r in rows
        if r["schema"] in PRIVATE_SCHEMAS
        or r["name"] in SENSITIVE_TABLES
        # An application table that a migration put in `public` by mistake. Only
        # `public`: data schemas legitimately reuse names (ocal.organizations).
        or (r["schema"] == "public" and r["name"] in APP_TABLES)
    ]
    if exposed:
        logger.critical(
            "SECURITY: the public SQL console's role can read %d table(s) it must "
            "not: %s. Revoke them, or move them into the %r schema and never grant "
            "on it. Refusing to start.",
            len(exposed), ", ".join(sorted(exposed)[:10]), SENSITIVE_SCHEMA,
        )
        raise RuntimeError(
            "public SQL console role can read credential tables: "
            + ", ".join(sorted(exposed))
        )

    logger.info(
        "console capability proof: role may read %d tables, none sensitive.",
        len(rows),
    )


async def _prove_app_tables_are_private(*, shared_db: bool = False) -> None:
    """Check the other half: that the application keeps its own tables out of `public`.

    ``_prove_console_role_cannot_read_secrets`` asks what the console role can
    read. This asks whether the application's connection really resolves names
    to `app`. If the engine's search_path were not applied, a future migration that
    creates a table without naming a schema would put it in `public`, which the
    xhostd read-only role reads automatically and forever.

    With one shared database that is an exposure, so startup stops. With separate
    databases (Neon today) nothing is reachable either way, so it is logged loudly
    and startup continues.
    """
    from sqlalchemy import text

    from app.database import APP_SCHEMA, APP_TABLES, engine

    if engine.dialect.name != "postgresql":
        return
    async with engine.connect() as conn:
        current = (await conn.execute(text("SELECT current_schema()"))).scalar()
        in_public = (await conn.execute(
            text(
                "SELECT coalesce(array_agg(c.relname::text ORDER BY c.relname), '{}') "
                "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p') "
                "AND c.relname = ANY(:names)"
            ),
            {"names": sorted(APP_TABLES)},
        )).scalar()

    problems = []
    if current != APP_SCHEMA:
        problems.append(f"the app engine resolves names to {current!r}, not {APP_SCHEMA!r}")
    if in_public:
        problems.append("application tables still in public: " + ", ".join(in_public))
    if not problems:
        logger.info("app tables are private: current schema %r, none in public.", current)
        return

    detail = "; ".join(problems)
    if shared_db:
        logger.critical(
            "SECURITY: %s. The public console shares this database. Refusing to start.",
            detail,
        )
        raise RuntimeError(detail)
    logger.critical("app schema check failed (separate databases, nothing exposed): %s", detail)


# NOTE: an earlier guard forbade OCAL_DATABASE_URL from sharing the append DB.
# That is now the DESIRED topology: the ocal DATA tables are co-located in the
# append DB under schema `ocal` so the public /data console can live-query and
# JOIN them (search_path includes `ocal`). Safety is preserved by EXCLUSION, not
# separation — only the data tables are migrated into `ocal`; ocal's auth tables
# (api_users/admin_users/mcp_oauth_*) are NOT copied into the append DB and the
# console's read-only role (over_readonly) is granted SELECT on schema `ocal`
# only. The console capability proof above still stands. See app/services/ocal_db.py.


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting גרסאות לעם")
    shared_db = _guard_db_separation()
    # Before the scheduler and before any request: prove, from the database's own
    # answer, that the public console's role cannot read a credential table. This
    # is the guard on every topology; with one database it is the only one.
    await _prove_console_role_cannot_read_secrets(shared_db=shared_db)
    await _prove_app_tables_are_private(shared_db=shared_db)
    await init_scheduler()
    # One-time seed of the CBS content index into the NEON append archive
    # (no-op once populated). Non-blocking so it never delays boot. See
    # app/services/cbs_neon.py.
    import asyncio
    from app.services import cbs_neon
    asyncio.create_task(cbs_neon.backfill_if_empty())
    # One-time auto-activation of the MMM dataset's daily incremental archive
    # mode (guarded + idempotent — no-op once active or if the catalog isn't
    # synced yet). Non-blocking so it never delays boot. See mmm_activate.py.
    from app.services import mmm_activate
    asyncio.create_task(mmm_activate.activate_mmm_archive_if_needed())
    # Warm the declarative source-registry cache so the first pasted URL and
    # the first neon-eligibility check don't race an empty cache. Manifests
    # are pushed by the worker; see app/services/source_registry.py.
    from app.database import async_session
    from app.services import source_registry
    asyncio.create_task(source_registry.warm_cache(async_session))
    yield
    shutdown_scheduler()
    logger.info("Shutting down גרסאות לעם")


app = FastAPI(
    title="גרסאות לעם",
    description="גרסאות לעם — מעקב גרסאות אחרי מאגרי מידע ממשלתיים | over.org.il",
    version="1.0.0",
    lifespan=lifespan,
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, rate_limit_exceeded_handler)

# Per-IP data budget for the bulk public API (/api/v1, /api/append): blocks a
# single client from siphoning an unreasonable VOLUME of data (cost guard) on
# top of the per-minute request limiter. See app/api_budget_middleware.py.
from app.api_budget_middleware import ApiBudgetMiddleware
app.add_middleware(ApiBudgetMiddleware)

# CORS — restrict to configured origins, fall back to permissive only in dev
cors_origins = settings.get_cors_origins()
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins if cors_origins else ["*"],
    allow_credentials=bool(cors_origins),
    allow_methods=["*"],
    allow_headers=["*"],
)

# MCP needs permissive cross-origin access (claude.ai etc.). Added AFTER the
# global CORS so it's the OUTERMOST layer — it answers /mcp preflight before the
# restrictive global policy can reject it, and no-ops on every other path.
from app.mcp.routes import MCPCorsMiddleware
app.add_middleware(MCPCorsMiddleware)


# Referrer policy. The /data console keeps the current query in the page URL so
# it can be shared and reloaded — which means the browser's default policy
# (strict-origin-when-cross-origin sends the FULL url on same-origin requests)
# puts raw SQL into the Referer header of every subsequent API call. Cloudflare's
# managed SQL-injection rules inspect that header, see SELECT/FROM/UNION in
# cleartext, and answer 403 — so opening a shared query link left the console
# unable to run ANYTHING in that tab until the URL changed, and no amount of
# waiting helped because the block is deterministic, not rate-based. Verified
# against production: same request, plain Referer 200, Referer carrying the
# query 403.
#
# `strict-origin` sends only "https://www.over.org.il/" — enough for any
# same-origin check, nothing for a WAF to match on, and it repairs every link
# already shared, since the policy applies when the browser SENDS.
@app.middleware("http")
async def _referrer_policy(request, call_next):
    response = await call_next(request)
    response.headers.setdefault("Referrer-Policy", "strict-origin")
    return response

# API routes
app.include_router(auth_router)
app.include_router(oauth_router)
app.include_router(proxy_router)
app.include_router(datasets_router)
app.include_router(versions_router)
app.include_router(drive_router)
app.include_router(append_router)
app.include_router(admin_router)
app.include_router(worker_router)
app.include_router(govil_router)
app.include_router(govmap_router)
app.include_router(idf_router)
app.include_router(health_router)
app.include_router(registries_router)
app.include_router(avodata_router)
app.include_router(munidata_router)
app.include_router(emun_router)
app.include_router(servicescompass_router)
app.include_router(mevaker_router)
app.include_router(hatzav_router)
app.include_router(mankal_router)
app.include_router(jda_router)
app.include_router(eden_router)
app.include_router(knesset_router)
app.include_router(knesset_protocols_router)
app.include_router(knesset_db_router)
# Generic validate/registry endpoints for worker-declared sources — the one
# router that never needs another entry here when a source is added.
app.include_router(sources_router)
app.include_router(resolve_router)
app.include_router(direct_router)
app.include_router(tables_router)
app.include_router(nl_query_router)
app.include_router(admin_nl_query_router)
app.include_router(settlements_router)
app.include_router(seo_router)
app.include_router(site_stats_router)
app.include_router(connector_router)
app.include_router(cbs_router)
app.include_router(cbs_ask_router)
app.include_router(ocal_router)
app.include_router(ocoi_router)
app.include_router(ocoi_admin_router)
app.include_router(ocal_admin_router)
app.include_router(nadlan_router)
app.include_router(organizations_router)
app.include_router(admin_organizations_router)
app.include_router(tags_router)
app.include_router(admin_tags_router)
app.include_router(v1_router)
app.include_router(admin_mcp_users_router)
app.include_router(page_content_router)
app.include_router(admin_page_content_router)
app.include_router(decision_analysis_router)
app.include_router(admin_decision_analysis_router)
app.include_router(deep_search_router)

# MCP server + its OAuth (registered before the SPA fallback so /mcp and the
# root-path /.well-known/*/mcp metadata return real responses, not index.html).
from app.mcp.routes import mcp_router, mcp_wellknown_router
app.include_router(mcp_wellknown_router)
app.include_router(mcp_router)

# Dedicated CBS index MCP at /cbs/mcp (same OAuth server + api_users gate; its
# own tools + resource metadata). Registered before the SPA fallback so
# /cbs/mcp and /.well-known/oauth-protected-resource/cbs/mcp aren't swallowed by
# the React /cbs route.
from app.mcp.cbs_routes import cbs_mcp_router, cbs_mcp_wellknown_router
app.include_router(cbs_mcp_wellknown_router)
app.include_router(cbs_mcp_router)

# Dedicated Knesset committee-protocols MCP at /knesset/mcp (same OAuth server +
# api_users gate + service token; SQL-mirror tools only — no document content).
# Registered before the SPA fallback so /knesset/mcp isn't swallowed by the
# React /knesset route.
from app.mcp.knesset_routes import knesset_mcp_router, knesset_mcp_wellknown_router
app.include_router(knesset_mcp_wellknown_router)
app.include_router(knesset_mcp_router)

# Dedicated יומן לעם (Ocal) MCP at /ocal/mcp (same OAuth server + api_users gate;
# calendar-events tools over the migrated Ocal Neon DB). Registered before the
# SPA fallback so /ocal/mcp isn't swallowed by the React /projects/ocal route.
from app.mcp.ocal_routes import ocal_mcp_router, ocal_mcp_wellknown_router
app.include_router(ocal_mcp_wellknown_router)
app.include_router(ocal_mcp_router)
from app.mcp.ocoi_routes import ocoi_mcp_router, ocoi_mcp_wellknown_router
app.include_router(ocoi_mcp_wellknown_router)
app.include_router(ocoi_mcp_router)

# Dedicated מידע לעם MCP at /odata/mcp (same OAuth server + api_users gate; a
# pass-through to odata.org.il's public CKAN — OVER stores nothing for it).
# Registered before the SPA fallback so /odata/mcp isn't swallowed by the React
# /projects/odata route.
from app.mcp.odata_routes import odata_mcp_router, odata_mcp_wellknown_router
app.include_router(odata_mcp_wellknown_router)
app.include_router(odata_mcp_router)

# Dedicated whole-site SQL MCP at /data/mcp (same OAuth server + api_users gate;
# the MCP twin of the public /data console — table catalog, DDL and free
# read-only SELECT across every schema). Registered before the SPA fallback so
# /data/mcp isn't swallowed by the React /data route.
# Dedicated election-finance MCP at /elections/mcp (same OAuth server + api_users
# gate). The State Comptroller's register of who funded Israeli campaigns, asked
# by PERSON — a question that spans six tables with different recipient columns,
# so it gets a resource rather than living in the generic SQL console.
# Registered before the SPA fallback so /elections/mcp isn't swallowed by it.
from app.mcp.elections_routes import (
    elections_mcp_router, elections_mcp_wellknown_router,
)
app.include_router(elections_mcp_wellknown_router)
app.include_router(elections_mcp_router)

from app.mcp.sql_routes import sql_mcp_router, sql_mcp_wellknown_router
app.include_router(sql_mcp_wellknown_router)
app.include_router(sql_mcp_router)

# Short links for shared /data console views: /s/<slug>. Registered before the
# SPA fallback so the path resolves here rather than being swallowed as a React
# route. This handler stays deliberately dumb — it does not touch the DB, it
# just hands the slug to the console, which resolves it through
# /api/tables/share/<slug>. Keeping the query OUT of this URL is the whole
# point: the link's length no longer tracks the query's, so a 40 KB query
# shares as cleanly as a one-liner. An unknown slug is reported by the console
# itself, which can say so in Hebrew in the page rather than as a bare 404.
@app.get("/s/{slug}")
async def short_share_link(slug: str):
    return RedirectResponse(url=f"/data?share={quote(slug, safe='')}", status_code=302)


# ── legacy ocoi.org.il paths ────────────────────────────────────────────────
# ניגוד עניינים לעם moved here. OCOI's credibility model was "every claim links
# back to its source document" — its MCP server instructs models to cite
# https://www.ocoi.org.il/document?id=… and /entity?type=&id= — so those URLs
# are in the wild in text we cannot edit, and they have to keep resolving after
# the old service is shut down.
#
# ocoi.org.il is NOT being pointed at over.org.il: the domain keeps its own
# static site (Ocoi/static-site), and THAT is what actually carries those links
# across. These routes are the backstop for the same paths typed or rewritten
# against over.org.il directly, and the landing they redirect to — including
# ?doc= on the documents tab — is the contract the static site targets.
#
# 301, not 302: the destination is permanent and search engines should move the
# authority across. Registered before the SPA fallback so React never sees them.

@app.get("/entity")
async def legacy_ocoi_entity(type: str = "", id: str = ""):
    if not type or not id:
        return RedirectResponse(url="/projects/ocoi", status_code=301)
    return RedirectResponse(
        url=f"/projects/ocoi?tab=graph&type={quote(type, safe='')}&id={quote(id, safe='')}",
        status_code=301)


@app.get("/document")
async def legacy_ocoi_document(id: str = ""):
    if not id:
        return RedirectResponse(url="/projects/ocoi?tab=documents", status_code=301)
    return RedirectResponse(
        url=f"/projects/ocoi?tab=documents&doc={quote(id, safe='')}", status_code=301)


# /search is gated on the legacy host, unlike the two above: "entity" and
# "document" are names nothing else here wants, but /search is one OVER may
# well want for itself one day, and claiming it site-wide to serve a redirect
# would be borrowing against that.
_LEGACY_OCOI_HOSTS = {"ocoi.org.il", "www.ocoi.org.il"}


@app.get("/search")
async def legacy_ocoi_search(request: Request, q: str = ""):
    # An exact set, not endswith(): "ocoi.org.il" is a suffix of
    # "myocoi.org.il", so a suffix test hands the redirect to any lookalike
    # domain that points itself at us.
    host = (request.headers.get("host") or "").split(":")[0].lower()
    if host not in _LEGACY_OCOI_HOSTS:
        raise HTTPException(status_code=404, detail="Not Found")
    suffix = f"?q={quote(q, safe='')}" if q else ""
    return RedirectResponse(url=f"/projects/ocoi{suffix}", status_code=301)


# Serve frontend SPA (built by Vite)
frontend_dist = Path(__file__).parent.parent / "frontend" / "dist"
index_html = frontend_dist / "index.html"

if frontend_dist.exists() and index_html.exists():
    logger.info("Frontend dist found at %s", frontend_dist)

    # Mount Vite's hashed static assets.
    #
    # Vite puts a content hash in every filename here, so a given URL's bytes
    # can never change — a new build produces a new name. That makes these the
    # one thing on the site safe to cache forever, and they were being served
    # with no Cache-Control at all: Cloudflare reported cf-cache-status DYNAMIC
    # and every repeat visitor re-fetched ~334 KB of JavaScript from the origin.
    class _ImmutableAssets(StaticFiles):
        def file_response(self, *args, **kwargs):
            resp = super().file_response(*args, **kwargs)
            resp.headers["Cache-Control"] = "public, max-age=31536000, immutable"
            return resp

    assets_dir = frontend_dist / "assets"
    if assets_dir.exists():
        app.mount("/assets", _ImmutableAssets(directory=str(assets_dir)), name="assets")

    # The SPA shell, read once. Every route serves the same bytes apart from the
    # <head> block composed per request, so re-reading the file per view would
    # buy nothing but disk I/O.
    _shell = index_html.read_text(encoding="utf-8")

    async def _spa_response(path: str) -> Response:
        """index.html with a real <head> for this route.

        The body is still client-rendered; what changes is that a crawler — and
        anything unfurling a pasted link — now gets a title, a description, a
        canonical URL and, on a dataset page, schema.org/Dataset markup instead
        of one sitewide title and an empty <div id="root">.

        Wrapped end to end: if anything in the SEO path misbehaves the shell
        goes out untouched, exactly as it did before.
        """
        try:
            meta = await seo.meta_for(path)
            body = seo.render(_shell, meta)
        except Exception:  # noqa: BLE001
            logger.warning("SEO head injection failed for %s", path, exc_info=True)
            body = _shell
        return HTMLResponse(
            content=body,
            headers={
                # Short, revalidated: the HTML names hashed assets, so a stale
                # copy would pin an old bundle. Long enough to absorb a burst.
                "Cache-Control": "public, max-age=60, stale-while-revalidate=600",
            },
        )

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
        return await _spa_response(request.url.path)

    # Explicit root route (some load balancers hit / for health checks)
    @app.get("/")
    async def serve_root():
        return await _spa_response("/")
else:
    logger.warning("Frontend dist not found at %s — SPA disabled", frontend_dist)
