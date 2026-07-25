"""Repository-level .env discovery and loading."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def load_environment(
    path: str | Path | None = None,
    *,
    override: bool = False,
) -> Path | None:
    """Load the first configured .env file and return its resolved path."""
    explicit = path or os.getenv("FRAUDFLUX_ENV_FILE")
    candidates = (
        (Path(explicit).expanduser(),)
        if explicit
        else (Path.cwd() / ".env", PROJECT_ROOT / ".env")
    )
    for candidate in candidates:
        resolved = candidate.resolve()
        if resolved.is_file():
            load_dotenv(resolved, override=override)
            return resolved
    if explicit:
        raise FileNotFoundError(f"environment file not found: {explicit}")
    return None


def environment_value(name: str, default: str = "") -> str:
    """Load .env on demand and return one environment value."""
    load_environment()
    return os.getenv(name, default)
