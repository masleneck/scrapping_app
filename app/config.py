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


@dataclass(slots=True)
class Settings:
    source_url: str = os.getenv("SOURCE_URL", "http://localhost:8080/flights.html")
    source_urls: str = os.getenv("SOURCE_URLS", "http://localhost:8080/flights.html")
    events_db_path: str = os.getenv("EVENTS_DB_PATH", "data/events.db")

    def get_source_urls(self) -> list[str]:
        return [url.strip() for url in self.source_urls.split(",") if url.strip()]


settings = Settings()
