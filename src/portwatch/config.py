from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration loaded from environment variables and `.env`."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    census_api_key: str | None = Field(default=None, alias="CENSUS_API_KEY")
    database_path: Path = Field(
        default=Path("data/portwatch.duckdb"),
        alias="PORTWATCH_DATABASE_PATH",
    )
    http_timeout_seconds: float = Field(
        default=30.0,
        gt=0,
        alias="PORTWATCH_HTTP_TIMEOUT_SECONDS",
    )
    user_agent: str = Field(
        default="PortWatch/0.1 (research)",
        alias="PORTWATCH_USER_AGENT",
    )
    log_level: str = Field(default="INFO", alias="PORTWATCH_LOG_LEVEL")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
