from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

from .config import ConfigurationError


def _secret(path: Path, label: str) -> str:
    if not path.is_file() or path.is_symlink():
        raise ConfigurationError(f"Autodev secret file is missing or unsafe: {label}")
    value = path.read_text(encoding="utf-8").strip()
    if not value:
        raise ConfigurationError(f"Autodev secret file is empty: {label}")
    return value


def _positive_int(name: str, default: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise ConfigurationError(f"Invalid integer environment variable: {name}") from exc
    if value < 1:
        raise ConfigurationError(f"Environment variable must be positive: {name}")
    return value


def _loopback_url(name: str, default: str) -> str:
    value = os.getenv(name, default).strip().rstrip("/")
    parsed = urlsplit(value)
    if parsed.scheme != "http" or parsed.hostname != "127.0.0.1" or parsed.path not in {"", "/"}:
        raise ConfigurationError(f"{name} must be an http loopback URL")
    if parsed.query or parsed.fragment or parsed.port is None:
        raise ConfigurationError(f"{name} must include a loopback port")
    return value


@dataclass(frozen=True, slots=True)
class AutodevSettings:
    app_id: str
    app_secret: str
    owner_open_id: str
    operator_hmac_secret: str
    target_id: str
    state_db: Path
    operator_base_url: str = "http://127.0.0.1:8765"
    host: str = "127.0.0.1"
    port: int = 18085
    max_file_bytes: int = 10 * 1024 * 1024
    max_text_chars: int = 100_000
    operator_timeout_seconds: float = 15.0
    poll_interval_seconds: float = 2.0

    def __post_init__(self) -> None:
        if self.host != "127.0.0.1":
            raise ConfigurationError("Autodev bridge must bind to 127.0.0.1")
        if not self.target_id.strip():
            raise ConfigurationError("AUTODEV_TARGET_ID must be non-empty")
        if self.operator_timeout_seconds <= 0 or self.poll_interval_seconds <= 0:
            raise ConfigurationError("Autodev timing settings must be positive")

    @classmethod
    def from_environment(cls) -> AutodevSettings:
        secrets_dir = Path(
            os.getenv(
                "AUTODEV_FEISHU_SECRETS_DIR",
                str(Path(os.getenv("PROGRAMDATA", ".")) / "AutonomousDevelopment" / "secrets"),
            )
        )
        state_default = (
            Path(
                os.getenv(
                    "PROGRAMDATA",
                    "/var/lib",
                )
            )
            / "AutonomousDevelopment"
            / "feishu-autodev"
            / "state.db"
        )
        state_db = Path(os.getenv("AUTODEV_FEISHU_STATE_DB", str(state_default)))
        if not state_db.is_absolute():
            raise ConfigurationError("AUTODEV_FEISHU_STATE_DB must be absolute")
        return cls(
            app_id=_secret(secrets_dir / "app_id", "app_id"),
            app_secret=_secret(secrets_dir / "app_secret", "app_secret"),
            owner_open_id=_secret(secrets_dir / "owner_open_id", "owner_open_id"),
            operator_hmac_secret=_secret(
                secrets_dir / "operator_hmac_secret", "operator_hmac_secret"
            ),
            target_id=os.getenv("AUTODEV_TARGET_ID", "primary").strip(),
            state_db=state_db,
            operator_base_url=_loopback_url("AUTODEV_OPERATOR_BASE_URL", "http://127.0.0.1:8765"),
            host=os.getenv("AUTODEV_FEISHU_HOST", "127.0.0.1"),
            port=_positive_int("AUTODEV_FEISHU_PORT", 18085),
            max_file_bytes=_positive_int("AUTODEV_FEISHU_MAX_FILE_BYTES", 10 * 1024 * 1024),
            max_text_chars=_positive_int("AUTODEV_FEISHU_MAX_TEXT_CHARS", 100_000),
            operator_timeout_seconds=float(os.getenv("AUTODEV_OPERATOR_TIMEOUT_SECONDS", "15")),
            poll_interval_seconds=float(os.getenv("AUTODEV_FEISHU_POLL_INTERVAL_SECONDS", "2")),
        )
