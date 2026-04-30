import os
from typing import Literal

Target = Literal["prod", "dev"]


def resolve_dataset_name(target: Target, base: str = "strava") -> str:
    if target == "prod":
        return base
    user = os.environ.get("STRAVA_DLT_DEV_USER") or os.environ.get("USER") or "local"
    return f"{base}_dev_{user}"


def resolve_target() -> Target:
    raw = os.environ.get("STRAVA_DLT_TARGET", "dev").lower()
    if raw not in ("prod", "dev"):
        raise ValueError(f"STRAVA_DLT_TARGET must be 'prod' or 'dev', got {raw!r}")
    return raw  # type: ignore[return-value]
