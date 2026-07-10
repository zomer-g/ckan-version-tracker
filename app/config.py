from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql+asyncpg://localhost:5432/ckan_tracker"

    jwt_secret_key: str = ""
    jwt_algorithm: str = "HS256"
    jwt_expiry_minutes: int = 1440

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
    append_database_url: str = ""

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
    # ⚠ UPDATE THIS whenever the govil-scraper worker code changes — set it
    #   to the new `git rev-parse HEAD` of that repo, or the new worker is
    #   refused. (OVER-only commits don't change it.)
    worker_required_version: str = "d52e8e738d039d83a944c310fb57edd108c51261"
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


settings = Settings()
