from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _load_env_file(path: str = ".env") -> None:
    env_path = Path(path)
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


_load_env_file()


def _get_bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(slots=True)
class Settings:
    source_url: str = os.getenv("SOURCE_URL", "http://localhost:8080/flights.html")
    source_urls: str = os.getenv("SOURCE_URLS", "http://localhost:8080/flights.html")
    database_url: str = os.getenv(
        "DATABASE_URL",
        "postgresql+asyncpg://scrapping:scrapping@localhost:5432/scrapping_app",
    )
    real_scrape_scheduler_enabled: bool = _get_bool_env(
        "REAL_SCRAPE_SCHEDULER_ENABLED", True
    )
    real_scrape_interval_seconds: int = int(os.getenv("REAL_SCRAPE_INTERVAL_SECONDS", "300"))
    real_scrape_retry_attempts: int = int(os.getenv("REAL_SCRAPE_RETRY_ATTEMPTS", "3"))
    real_scrape_retry_backoff_seconds: float = float(
        os.getenv("REAL_SCRAPE_RETRY_BACKOFF_SECONDS", "1.0")
    )

    def get_source_urls(self) -> list[str]:
        return [url.strip() for url in self.source_urls.split(",") if url.strip()]


settings = Settings()
