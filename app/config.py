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

    large_dataset_threshold: int = 50000  # rows — datasets above this use lightweight versioning

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

    # Worker version gate: refuse to dispatch tasks to a worker whose git
    # commit doesn't match what's on the upstream repo's master branch.
    # Prevents stale workers from picking up tasks and producing opaque
    # errors that newer code would have surfaced clearly. Set
    # WORKER_VERSION_CHECK_ENABLED=false to disable (e.g. local dev).
    # WORKER_REQUIRED_VERSION pins to a specific SHA, skipping the GitHub
    # fetch entirely.
    worker_version_check_enabled: bool = True
    # When the required SHA/engine-hash can't be determined (GitHub
    # unreachable AND no cached known-good value), fail the dispatch gate
    # CLOSED — refuse to hand out tasks rather than risk a stale worker
    # grabbing one and crashing it. The sticky cache in worker_version.py
    # makes the "undetermined" case rare (a known-good value persists
    # across GitHub blips), so this almost never blocks legitimate work;
    # pin worker_required_version to sidestep GitHub entirely if needed.
    worker_version_fail_closed: bool = True
    worker_repo: str = "zomer-g/govil-scraper"
    worker_branch: str = "master"
    worker_required_version: str = ""  # explicit SHA override; empty = fetch latest
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

    cors_origins: str = ""

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
