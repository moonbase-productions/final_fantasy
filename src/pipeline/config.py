from __future__ import annotations
import os
from typing import ClassVar
from urllib.parse import urlparse

_HAS_PYDANTIC_SETTINGS = True
try:
    from pydantic_settings import BaseSettings, SettingsConfigDict
except ModuleNotFoundError:
    _HAS_PYDANTIC_SETTINGS = False
    # Fallback for constrained environments where pydantic-settings is absent.
    class BaseSettings:  # type: ignore[no-redef]
        def __init__(self, **_: object) -> None:
            pass

    def SettingsConfigDict(**kwargs: object) -> dict[str, object]:
        return dict(kwargs)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # --- External service credentials (from .env) ---
    sportsdb_api_key: str = os.getenv("SPORTSDB_API_KEY", "")
    supabase_url: str = os.getenv("SUPABASE_URL", "")
    supabase_service_role_key: str = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")
    db_host: str = os.getenv("DB_HOST", "")
    db_port: int = 5432
    db_name: str = os.getenv("DB_NAME", "")
    db_user: str = os.getenv("DB_USER", "")
    db_password: str = os.getenv("DB_PASSWORD", "")

    # --- Pipeline constants (not from env) ---

    # Number of past seasons to keep in derived tables
    SEASON_WINDOW: ClassVar[int] = 5

    # TheSportsDB API rate limit
    API_RATE_LIMIT: ClassVar[int] = 100  # requests per minute

    # Supabase upsert batch size
    UPSERT_CHUNK_SIZE: ClassVar[int] = 100

    # Starting Elo for teams with no history
    INIT_ELO: ClassVar[int] = 1500

    # Elo K-values by sport. Governs how much each result shifts ratings.
    # Soccer: 20.75 (standard for international Elo)
    # Baseball: 4.0 (low-scoring, high variance)
    # default: applies to all other sports
    K_VALUES: ClassVar[dict[str, float]] = {
        "Soccer": 20.75,
        "Baseball": 4.0,
        "default": 20.0,
    }

    # Tier thresholds. Each tuple is (min_percentile, tier_name).
    # Applied in order: first matching threshold wins.
    TIER_THRESHOLDS: ClassVar[list[tuple[float, str]]] = [
        (0.995, "MOL"),
        (0.95,  "SS"),
        (0.85,  "S"),
        (0.70,  "A"),
        (0.60,  "B"),
        (0.50,  "C"),
        (0.30,  "D"),
        (0.15,  "E"),
        (0.05,  "F"),
        (0.005, "FF"),
        # Below 0.005: "DIE"
    ]

    # F1 championship points by finishing position (1st through 10th)
    F1_POINTS: ClassVar[dict[int, float]] = {
        1: 25, 2: 18, 3: 15, 4: 12, 5: 10,
        6: 8,  7: 6,  8: 4,  9: 2,  10: 1,
    }

    # NASCAR Cup points are complex; use simplified finish-based points
    NASCAR_POINTS: ClassVar[dict[int, float]] = {
        1: 40, 2: 35, 3: 34, 4: 33, 5: 32,
        6: 31, 7: 30, 8: 29, 9: 28, 10: 27,
    }

    # Number of recent games to use for luck calculation
    LUCK_WINDOW: ClassVar[int] = 20


settings = Settings()

if not _HAS_PYDANTIC_SETTINGS:
    # Best-effort .env loader for environments without pydantic-settings.
    env_path = ".env"
    if os.path.exists(env_path):
        with open(env_path, encoding="utf-8") as fh:
            for raw in fh:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                os.environ.setdefault(key, value)

    # Refresh values from environment after fallback .env load.
    settings.sportsdb_api_key = os.getenv("SPORTSDB_API_KEY", settings.sportsdb_api_key)
    settings.supabase_url = os.getenv("SUPABASE_URL", settings.supabase_url)
    settings.supabase_service_role_key = os.getenv(
        "SUPABASE_SERVICE_ROLE_KEY", settings.supabase_service_role_key
    )
    settings.db_host = os.getenv("DB_HOST", settings.db_host)
    settings.db_name = os.getenv("DB_NAME", settings.db_name)
    settings.db_user = os.getenv("DB_USER", settings.db_user)
    settings.db_password = os.getenv("DB_PASSWORD", settings.db_password)
    settings.db_port = int(os.getenv("DB_PORT", str(settings.db_port)))


def validate_runtime_settings() -> None:
    """Raise a clear error if required runtime settings are missing."""
    required = {
        "SPORTSDB_API_KEY": settings.sportsdb_api_key,
        "SUPABASE_URL": settings.supabase_url,
        "SUPABASE_SERVICE_ROLE_KEY": settings.supabase_service_role_key,
        "DB_HOST": settings.db_host,
        "DB_NAME": settings.db_name,
        "DB_USER": settings.db_user,
        "DB_PASSWORD": settings.db_password,
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        raise RuntimeError(
            "Missing required environment variables: "
            + ", ".join(sorted(missing))
        )

    # Guard against common connection-string/template mistakes.
    host = settings.db_host or ""
    if "://" in host or "@" in host:
        raise RuntimeError(
            "DB_HOST must be a hostname only (e.g. db.<project-ref>.supabase.co), "
            "not a full postgres connection URI."
        )
    lowered = (
        f"{settings.db_host} {settings.db_password} {settings.supabase_service_role_key}"
    ).lower()
    if "your-password" in lowered or "[your-password]" in lowered:
        raise RuntimeError(
            "Detected placeholder credentials. Replace template values (e.g. [your-password]) "
            "with real secrets in .env."
        )


def _parse_db_host_uri() -> None:
    """Support DB_HOST being provided as a full postgres URI."""
    host_val = settings.db_host or ""
    if not (host_val.startswith("postgres://") or host_val.startswith("postgresql://")):
        return

    try:
        parsed = urlparse(host_val)
    except Exception:
        return
    if parsed.hostname:
        settings.db_host = parsed.hostname
    if parsed.port:
        settings.db_port = parsed.port
    if parsed.path and parsed.path != "/":
        settings.db_name = parsed.path.lstrip("/")
    if parsed.username:
        settings.db_user = parsed.username
    if parsed.password:
        settings.db_password = parsed.password


_parse_db_host_uri()
