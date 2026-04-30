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
    max_resource_download_size: int = 1_000_000_000  # 1GB — covers most data.gov.il ZIPs

    worker_api_key: str = ""  # API key for govil-scraper worker

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
