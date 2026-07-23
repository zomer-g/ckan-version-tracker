from urllib.parse import urlsplit

from pydantic_settings import BaseSettings


def parse_pg_target(dsn: str) -> tuple[str, int, str] | None:
    """Reduce a Postgres DSN to the (host, port, dbname) tuple that identifies
    the physical database, for comparing two DSNs.

    Normalizes the pieces that vary without changing WHICH database is addressed:
    strips the SQLAlchemy dialect suffix (``postgresql+asyncpg`` → ``postgresql``),
    lower-cases the host, defaults the port to 5432, and drops the leading slash
    from the path. Credentials and query params (sslmode, channel_binding…) are
    intentionally ignored — two URLs with different passwords but the same
    host+port+dbname still point at the same data. Returns None for an empty/
    unparseable DSN.

    NOTE: this is an EXACT identity check. It reliably catches the dangerous
    case (both env vars set to the literally-same endpoint) but cannot see that
    two *different* Neon hostnames (e.g. a ``-pooler`` endpoint vs. the direct
    one) resolve to the same underlying database — so a False here is "not
    proven identical", not "proven separate".
    """
    if not dsn or not dsn.strip():
        return None
    u = urlsplit(dsn.strip())
    host = (u.hostname or "").lower()
    port = u.port or 5432
    dbname = (u.path or "").lstrip("/")
    if not host and not dbname:
        return None
    return (host, port, dbname)


class Settings(BaseSettings):
    database_url: str = "postgresql+asyncpg://localhost:5432/ckan_tracker"

    jwt_secret_key: str = ""
    jwt_algorithm: str = "HS256"
    # Admin session lifetime. Kept SHORT (2h) so a token that somehow leaks is
    # useless within hours, not a full day. The SPA slides the session forward
    # via POST /api/auth/refresh (on load + on a timer), so an active admin is
    # never logged out mid-work. Was 1440 (24h). See app/api/auth.refresh.
    jwt_expiry_minutes: int = 120

    data_gov_il_url: str = "https://data.gov.il"
    odata_url: str = "https://www.odata.org.il"
    odata_api_key: str = ""
    odata_owner_org: str = "zomer"

    # ── Object storage (independent file backend, decoupled from ODATA) ──
    # File archiving target — the GLOBAL DEFAULT for any dataset that hasn't
    # pinned its own destination (scraper_config.storage_backend). "r2" routes
    # file uploads to the S3-compatible object store configured below
    # (Cloudflare R2 — zero egress) and serves downloads straight from
    # S3_PUBLIC_BASE_URL. "odata" keeps the legacy CKAN-mirror behavior.
    # Default is "r2": every dataset not explicitly configured otherwise is
    # archived as a full independent snapshot on R2 (the user's storage model).
    # A per-dataset override (append, odata, local) still wins. The
    # STORAGE_BACKEND env var, if set, overrides this code default.
    # See app/services/storage_client.py.
    storage_backend: str = "r2"  # "odata" | "r2"
    s3_endpoint: str = ""            # e.g. https://<account>.r2.cloudflarestorage.com
    s3_bucket: str = ""
    s3_access_key: str = ""
    s3_secret_key: str = ""
    s3_region: str = "auto"          # R2 ignores region; "auto" is the convention
    # Public custom domain bound to the bucket for direct downloads
    # (e.g. https://files.over.org.il). Downloads 302-redirect here, so the
    # file bytes never pass through the OVER backend.
    s3_public_base_url: str = ""

    # Max ZIP part size the worker splits attachments into, per destination.
    # ODATA stays small (CKAN/Cloudflare ~100MB upload edge limit); R2 has no
    # such limit (multipart streams GBs), so bigger parts ⇒ fewer parts. The
    # worker reads `max_zip_part_bytes` from the poll response and splits to it.
    zip_part_bytes_odata: int = 80 * 1024 * 1024     # 80 MB
    zip_part_bytes_r2: int = 1024 * 1024 * 1024      # 1 GB

    # ── Append archive DB (dedicated Postgres for data.gov.il datastore pulls) ──
    # Where the row-level APPEND archive lives. data.gov.il datastore-backed
    # datasets tracked as append_only stream their rows here — one table per
    # dataset, deduped by the DB (UNIQUE index + ON CONFLICT DO NOTHING), each
    # row stamped with first_seen DEFAULT now(). Decoupled from the app's
    # operational DB (and from the broken ODATA write path). Empty = feature
    # off (falls back to the legacy/metadata path). See app/services/append_store.py.
    #
    # This URL is the READ/WRITE connection the sync/poll pipeline uses (it runs
    # the CREATE SCHEMA / ALTER TABLE / INSERT DDL+DML), so its role has full
    # privileges on the append DB.
    append_database_url: str = ""

    # ── Least-privilege read-only role for the PUBLIC SQL consoles ──
    # The three public SQL consoles (append run_readonly_sql, knesset run_sql /
    # iter_sql_csv) must NOT run on the read/write role above — that role can
    # write, and the READ ONLY transaction is then the ONLY thing stopping a
    # write. This URL points at the SAME physical append DB but authenticates as
    # a dedicated role that has been GRANTed SELECT only (no INSERT/UPDATE/DDL,
    # not a superuser, not a member of pg_read_server_files) — so a write is
    # refused by Postgres itself, defense-in-depth beneath the app-layer guards.
    # Provision the role with scripts/create_append_readonly_role.sql. Empty ⇒
    # the consoles fall back to the read/write pool with a one-time log warning
    # (keeps dev/prod working until the role is created). See
    # app/services/append_store.get_readonly_pool.
    append_readonly_database_url: str = ""

    # ── Knesset ODATA mirror ("מסד הנתונים של הכנסת") ──
    # Syncs all ~48 Knesset ODATA-v4 entity sets into a `knesset` schema in the
    # append DB above, with a public read-only SQL console at /knesset. Requires
    # append_database_url; this flag is the feature kill switch, and the
    # interval controls how often each table is refreshed incrementally.
    # See app/services/knesset_db.py.
    knesset_db_enabled: bool = True
    knesset_db_sync_interval_hours: float = 12.0
    # Per scheduler tick, how long the sync may work before yielding (the
    # initial ~3M-row load spans many ticks; each tick checkpoints).
    knesset_db_tick_budget_seconds: float = 240.0

    # ── Committee-protocol batch ZIP guards (the /knesset "אצוות" tab) ──
    # /api/knesset-db/protocols/batch.zip is public and unauthenticated: for
    # each matching row it fetches a file live from fs.knesset.gov.il, holds it
    # whole in memory, deflates it, and streams it. Without hard caps a single
    # broad filter turns one anonymous request into hundreds of MB–GB of egress,
    # memory pressure on the 512MB dyno, and a worker pinned for minutes. These
    # bound the anonymous ZIP path; oversized selections are pushed to the cheap
    # links.csv manifest (which streams row-by-row, no file bytes).
    # Max protocol files packed into one anonymous ZIP. Kept low deliberately;
    # larger selections must use links.csv or narrow the filter.
    knesset_zip_max_files: int = 200
    # How many ZIP builds may run CONCURRENTLY across ALL clients. Extra
    # requests get an immediate 429 rather than piling coroutines (each pulling
    # files into memory) onto the dyno. 1–2 keeps a single build's footprint.
    knesset_zip_max_concurrency: int = 2
    # Cumulative downloaded-bytes ceiling for one ZIP. When the running total of
    # fetched file bodies crosses this, the build stops early, notes it in
    # _errors.txt, and closes the archive with whatever it has. Bounds total
    # egress/RSS per request independent of the file COUNT (a few giant files
    # can blow past a small count). 400MB fits under the 512MB dyno.
    knesset_zip_max_total_bytes: int = 400 * 1024 * 1024

    large_dataset_threshold: int = 50000  # rows — datasets above this use lightweight versioning

    # ── Poll memory guards (512MB Render dyno) ──
    # Max dataset polls allowed to run CONCURRENTLY on the web dyno. Every poll
    # can parse a full CSV or stream a datastore into memory; without a global
    # cap, each deploy re-schedules every overdue dataset to fire at once (see
    # scheduler.add_poll_job → start_date=now+1s) and the resulting stampede
    # OOM-kills the 512MB dyno. Automatic polls only — a manual "דגום" (force)
    # bypasses this so a user click is never starved behind a long-running
    # stream (see poll_job.poll_dataset). 2 gives a light poll a lane alongside
    # one giant stream while still bounding the stampede; raise with more RAM.
    poll_max_concurrency: int = 2
    # Rows per datastore_search page when streaming a datastore-backed dataset.
    # Each page's row batch AND its JSON response are held in memory, so this is
    # the dominant per-poll peak. Lower = smaller RSS at the cost of more HTTP
    # round-trips. Tuned down from 32000 to fit under the 512MB dyno.
    datastore_page_size: int = 10000

    default_poll_interval: int = 604800  # 1 week
    min_poll_interval: int = 300

    # ── Auto-discovery of new data.gov.il datasets ──
    # A scheduled job that maps the FULL data.gov.il CKAN catalog, diffs it
    # against the CKAN datasets already tracked here, and onboards ONE random
    # untracked dataset per run — steadily growing coverage of the catalog
    # without manual curation. Onboarded datasets are archived NEON-only
    # (SQL-queryable rows into the /data console, no file/ODATA mirror) and
    # polled quarterly. Off by default — set AUTO_DISCOVER_ENABLED=true to run it.
    # See app/services/auto_discovery.py.
    # Index-CSV mirror: every scraper/govmap version carries a "נתוני הסורק"
    # CSV (a GovMap layer's feature table, an FOI dataset's item + file index).
    # Mirroring it into the `idx` schema is what lets /data search INSIDE the
    # collections instead of only over their metadata. LATEST VERSION ONLY —
    # history stays in R2. Sized from a 310-dataset pilot: ~7.9 GB and ~$2.8/mo
    # for the whole corpus (docs/neon-index-pilot/).
    # See docs/neon-index-pilot/README.md §10.9. Running this unbounded on the
    # 512MB web dyno OOM-killed it three times: the loader is memory-bounded on
    # its own (202MB peak streaming a 3.5GB CSV), but it shares the dyno with
    # FastAPI, the poll pipeline and the Knesset sync, whose baseline is already
    # ~280MB — and a large CSV's peak does not fit in what is left.
    #
    # The fix is a SIZE GATE rather than an off switch: the crash correlated
    # with size (a 395MB CSV took RSS to 427MB; everything ≤24.67MB loaded fine),
    # and size is extremely skewed — a 25MB cap still covers 2,854 of the 2,910
    # datasets (98.1%). The 56 oversized ones are deferred, not lost: they are
    # recorded as such and wait for a run with real memory headroom (the
    # over-worker service, a one-off job, or an out-of-Render backfill).
    index_mirror_enabled: bool = True
    # Skip (and record as deferred) any index CSV larger than this. Checked with
    # a HEAD before a byte is downloaded.
    index_mirror_max_csv_mb: int = 25
    # Datasets per tick. Each is streamed one at a time, so this bounds how long
    # a tick runs, not how much memory it uses. Kept small because the tick
    # shares a 512MB dyno with the web app: a measured tick reached 427MB RSS.
    # Datasets per tick. Was 3, set when the OOM was fresh and the fear was that
    # more datasets per tick meant more memory. It does not: sync_due processes
    # them SEQUENTIALLY on purpose, so peak RSS is one dataset's streaming peak
    # no matter what this is — the chunk controls how long a tick RUNS, not how
    # much it holds. And that one dataset is now doubly bounded: the 25MB size
    # gate caps the CSV before download, COPY_BATCH_BYTES caps what is in flight.
    #
    # At 3 the mirror moved 18 datasets/hour against a backlog of 589, i.e. ~33
    # hours to catch up, for no safety gained. 12 is ~2 minutes of work inside a
    # 10-minute tick, and max_instances=1 means a long tick skips the next one
    # rather than overlapping it.
    index_mirror_chunk: int = 12
    index_mirror_interval_minutes: int = 10
    # Build a PostGIS `geom` column + GiST index on mirrored GovMap layers, so
    # /data can run spatial SQL instead of only matching geometry_wkt as text.
    # Off by default: it needs `CREATE EXTENSION postgis SCHEMA extensions` to
    # have been run on the append DB, and a deploy with this still false must
    # behave exactly as before. Measured on the append DB 2026-07-23 (see
    # docs/neon-postgis/README.md §5 stage 1): conversion is ~21µs per geometry
    # and the GiST index ~40 bytes per row, so the cost is storage — about +80%
    # on a geometry table, ~$0.20/month across the whole corpus.
    #
    # NOTE the asymmetry: turning this ON adds geom to each table as it next
    # syncs, and turning it OFF removes it the same way, because every sync
    # rebuilds the table from scratch (COPY to staging, then swap). Neither
    # direction is retroactive, so `postgis_rows` in the checkpoint is what
    # tells you which tables actually have it.
    index_mirror_postgis_enabled: bool = False
    # Layers per tick for the in-place geometry backfill (see
    # index_mirror.backfill_geometry). Only relevant while the corpus is
    # catching up: once every layer has the column the query finds nothing and
    # the tick is free.
    #
    # Was 10, chosen before there were measurements. With them: a conversion is
    # ~1 second of DATABASE-side work (ALTER + UPDATE + CREATE INDEX; the dyno
    # only issues the SQL and never touches a geometry), and the largest layer
    # in the corpus converted 31,849 rows without the dyno moving. 10/tick left
    # ~250 layers needing four hours for no reason. 40 keeps a tick around
    # ~40 seconds of DB time — still comfortably inside the tick and the pool's
    # 180s command timeout, and max_instances=1 means ticks cannot overlap.
    index_mirror_geom_backfill_chunk: int = 40

    auto_discover_enabled: bool = False
    auto_discover_interval_hours: float = 6.0
    # Skip any candidate whose largest datastore resource exceeds this many
    # rows — random selection would otherwise occasionally pick a multi-million
    # -row registry and spike the dyno's memory / the NEON archive. Oversized
    # candidates are passed over and another is drawn.
    auto_discover_max_rows: int = 2_000_000
    # Poll cadence assigned to each auto-onboarded dataset. QUARTERLY (90d):
    # these are bulk-onboarded at 4/day with no human curating them, so a
    # weekly re-poll of an ever-growing set would keep re-streaming hundreds of
    # datastores into NEON for little gain — most of this long tail changes
    # rarely. Manually-tracked datasets keep their own (default_poll_interval)
    # cadence; this only applies to auto-discovered ones.
    auto_discover_poll_interval: int = 7_776_000  # 90 days
    # How many random candidates to evaluate per run before giving up (each
    # evaluation is a package_show + a datastore probe per resource).
    auto_discover_max_attempts: int = 30
    # Hard cap per resource. Even with stream-to-disk downloads we still
    # have to PARSE every CSV in memory (csv_parser.parse_csv loads the
    # whole record set), so the cap has to fit comfortably below the
    # 512MB Render dyno. 200MB covers ~99% of data.gov.il resources;
    # bigger ones are skipped with a clean error rather than OOM-killing
    # the worker.
    max_resource_download_size: int = 200_000_000

    worker_api_key: str = ""  # API key for govil-scraper worker

    # LLM keys for the natural-language CBS search endpoint (POST /api/cbs/ask —
    # app/api/cbs_ask.py). The provider is chosen by whichever key is set:
    # DeepSeek is preferred (cheaper), then Anthropic. Both empty ⇒ the endpoint
    # returns 503 (feature off). Set the VALUE in the Render dashboard.
    deepseek_api_key: str = ""
    anthropic_api_key: str = ""

    # ── MCP machine-to-machine service token ──
    # Optional shared secret that lets a trusted machine — the "חיפוש רוחבי"
    # discovery gateway — call /mcp WITHOUT the interactive Google OAuth flow.
    # A request whose Bearer token equals this value is authenticated as the
    # fixed "service-gateway" principal (see app/mcp/auth.py), bypassing JWT
    # verification and the api_users allow-list. Empty ⇒ this path is OFF
    # entirely (no bypass possible). Equivalent to full MCP access — keep it in
    # secrets only, never in the repo. Rotate by changing it here AND in the
    # gateway. Human users keep using OAuth; this is a parallel path, not a
    # replacement. See docs (mcp-service-token) for the gateway side.
    mcp_service_token: str = ""

    # ── TAG-IT (tag-it.biz) MCP — OUTBOUND, for the ממ״מ "deep search" ──
    # The ממ״מ tab's default search runs against our fast SQL metadata mirror
    # (knesset.mmm_documents). The optional "deep/slow" mode instead does a
    # full-text search INSIDE the converted document bodies on TAG-IT, via its
    # MCP service-token bypass (search_documents + text_query on a scope).
    # tagit_mcp_token is the shared secret TAG-IT issued us (its MCP_SERVICE_TOKEN)
    # — set the VALUE in Render; empty ⇒ deep search returns 503 (feature off).
    # tagit_mmm_scope is the TAG-IT workspace/scope id holding the ממ״מ corpus.
    # See docs/service-integration.md. NOT related to mcp_service_token above
    # (that is our own INBOUND token).
    tagit_mcp_url: str = "https://tag-it.biz/mcp"
    tagit_mcp_token: str = ""
    tagit_mmm_scope: int = 14
    # TAG-IT scope holding the Knesset committee-protocols corpus (indexed like
    # the ממ״מ one). Currently only Knesset-25 protocols are loaded; the full
    # corpus of all committees is being backfilled gradually.
    tagit_protocols_scope: int = 15

    # Worker version gate: refuse to dispatch tasks to a worker whose git
    # commit doesn't match what's on the upstream repo's master branch.
    # Prevents stale workers from picking up tasks and producing opaque
    # errors that newer code would have surfaced clearly. Set
    # WORKER_VERSION_CHECK_ENABLED=false to disable (e.g. local dev).
    # WORKER_REQUIRED_VERSION pins to a specific SHA, skipping the GitHub
    # fetch entirely.
    worker_version_check_enabled: bool = True
    # Fail CLOSED when the required SHA can't be determined? Safe ONLY
    # because worker_required_version below is pinned (so it's never
    # undetermined). The engine-hash axis stays fail-open in worker.py, so
    # a GitHub blip can't block the correct worker — the pinned SHA is the
    # sole gate.
    worker_version_fail_closed: bool = True
    worker_repo: str = "zomer-g/govil-scraper"
    worker_branch: str = "master"
    # PINNED to the worker's full git SHA. NOTE: auto-tracking (empty →
    # follow GitHub master HEAD) is NOT usable here — govil-scraper is a
    # PRIVATE repo and this server has no GitHub token, so the commits API
    # returns 404 and the fail-closed gate would refuse every worker. So the
    # pin must stay explicit and GitHub-independent.
    # ⚠ This pin is the worker DEPLOY LEVER: bumping it (and deploying) is
    #   what rolls new govil-scraper code into production. Since worker
    #   commit c406ed4d the worker self-syncs to this exact SHA on every
    #   refusal (sync-to-pin), so forgetting to bump no longer strands the
    #   worker — it just keeps running the pinned version until you bump.
    #   Never pin to a pre-c406ed4d commit: those lack sync-to-pin and a
    #   worker landed on one can't follow later bumps by itself.
    #   (Additional constraint since fa0ab757: pin only commits carrying the
    #   "sync-to-pin-escape-v2" marker — c406ed4..f709acb have an inert,
    #   cwd-broken guard and workers refuse to land on them.)
    worker_required_version: str = "f5ad3d8fb416dea3f72a5c48172fc1b5ce54d6de"
    # SHA-256 of legacy_engine.py the worker's loaded module must match.
    # Defends against WORKER_VERSION env spoofing and the "pulled but
    # didn't restart" failure mode where git HEAD moved but the running
    # process still has the old module in memory. Empty = fetch latest
    # bytes from GitHub raw and hash them.
    worker_required_engine_hash: str = ""

    # Conditional archiver: cheap HEAD / datastore_info probe before
    # the full download+hash pipeline. When all resources are confirmed
    # unchanged via HTTP headers, a metadata-only version is created
    # that reuses the previous version's ODATA resource_ids (zero new
    # uploads, zero new downloads). On any unverifiable signal the
    # legacy snapshot path runs unchanged. Kill switch for the rare
    # case it misbehaves on a particular dataset shape.
    conditional_archive_enabled: bool = True

    # How many govmap-coverage scrape tasks may be ACTIVE (pending/running)
    # at once. Sized to the worker fleet PLUS a small buffer: the operator
    # runs 4 OVER workers on 3 machines, and keeping a couple of tasks
    # pending means a worker that finishes claims the next one instantly
    # instead of idling until the next top-up tick. Regular (non-coverage)
    # datasets' tasks share the same queue and interleave.
    # Set GOVMAP_COVERAGE_CONCURRENCY=1 to restore the old one-at-a-time pace.
    govmap_coverage_concurrency: int = 6
    # Maintenance-mode cadence (took effect once the initial 859-layer bulk
    # import finished, 2026-07-11). Coverage datasets are skipped by the
    # per-dataset scheduler — the rollout tick is their ONLY refresh driver —
    # so these knobs, not deletion of the rollout, are how the "proactive
    # scraping" is throttled. A layer is DUE when never triggered, when its
    # last trigger is older than REFRESH_DAYS (routine re-scrape), or when its
    # latest attempt FAILED and RETRY_HOURS passed (bounded retry). A fresh &
    # healthy inventory makes every tick a no-op.
    govmap_coverage_refresh_days: float = 90.0
    govmap_coverage_retry_hours: float = 6.0

    cors_origins: str = ""

    # ── Public-API data budget (anti-abuse) ──
    # Caps how many bytes a single client IP may pull from the bulk public data
    # API (/api/v1, /api/append) within a rolling window, on top of slowapi's
    # per-minute request limit. Over the cap → HTTP 429 + a "contact us to
    # arrange access" message. Sized well below the cost-pain threshold (Render
    # egress ~$0.3/GB, so "tens of shekels" ≈ 25-80GB) so a scraper is stopped
    # long before it costs real money, while normal research never hits it.
    api_budget_enabled: bool = True
    api_daily_byte_budget: int = 2 * 1024 ** 3   # 2 GB per IP per window
    api_budget_window_seconds: int = 86400        # rolling 24h
    api_contact_email: str = "guy@z-g.co.il"

    # ── Looker Studio community-connector API (/api/connector) ──
    # All Looker Studio traffic egresses from a small pool of Google IPs, so it
    # cannot ride the per-IP budget above — a valid X-Connector-Key routes it to
    # one shared "connector" bucket with its own (larger) cap instead. Empty key
    # = feature off (the router answers 503).
    connector_api_key: str = ""
    connector_daily_byte_budget: int = 10 * 1024 ** 3   # shared bucket, 10 GB/window

    # ── Global daily hard cap on paid-LLM calls (anti-abuse) ──
    # The public natural-language endpoints (/api/cbs/ask + /api/cbs/resolve)
    # invoke a paid LLM on EVERY request. The per-IP request limiter throttles a
    # single client, but an attacker rotating IPs could still drive unbounded
    # spend. This is a SINGLE global counter, persisted per calendar day in
    # Postgres (table llm_daily_usage) and keyed ONLY by the day — never by IP —
    # so it caps total spend across ALL callers and cannot be reset by rotating
    # X-Forwarded-For or by a process restart/deploy. Enforced BEFORE the LLM
    # call, so a blocked request costs nothing. Authenticated MCP callers are
    # exempt. Set the budget to 0 (or llm_budget_enabled=False) to disable.
    # See app/services/llm_budget.py.
    llm_budget_enabled: bool = True
    cbs_ask_daily_budget: int = 2000   # max LLM parses/day, summed over everyone

    # SSO
    app_base_url: str = "http://localhost:8000"
    google_client_id: str = ""
    google_client_secret: str = ""

    model_config = {"env_file": ".env", "extra": "ignore"}

    def get_jwt_secret(self) -> str:
        if not self.jwt_secret_key:
            raise RuntimeError(
                "JWT_SECRET_KEY is not set. Set it in .env or as an environment variable."
            )
        return self.jwt_secret_key

    def get_cors_origins(self) -> list[str]:
        if not self.cors_origins:
            return []
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]

    def append_db_shares_main_db(self) -> tuple[bool, dict]:
        """True if APPEND_DATABASE_URL and DATABASE_URL address the SAME physical
        Postgres (host+port+dbname).

        This is a security invariant: the PUBLIC SQL consoles
        (/api/append/{id}/sql, /api/knesset-db/sql) run against the append DB,
        while the sensitive tables (api_users with bearer tokens, users) live in
        the operational DB. If both env vars point at one Neon database, those
        consoles can read the tokens. The two URLs MUST resolve to two separate
        databases. See app/main.py startup guard.

        Returns (collides, details) where details carries the parsed targets for
        logging. collides is False when the append DB is not configured (feature
        off — nothing exposed) or the two targets differ.
        """
        main = parse_pg_target(self.database_url)
        append = parse_pg_target(self.append_database_url)
        details = {
            "main": None if main is None else {"host": main[0], "port": main[1], "dbname": main[2]},
            "append": None if append is None else {"host": append[0], "port": append[1], "dbname": append[2]},
        }
        if main is None or append is None:
            return False, details
        return (main == append), details


settings = Settings()
