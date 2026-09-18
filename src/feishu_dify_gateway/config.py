from __future__ import annotations

import os
import stat
from dataclasses import dataclass
from pathlib import Path


class ConfigurationError(RuntimeError):
    """Raised when runtime configuration is missing or unsafe."""


def _read_secret_path(path: Path, label: str) -> str:
    if not path.is_file() or path.is_symlink():
        raise ConfigurationError(f"Required secret file is missing or unsafe: {label}")
    if os.name != "nt" and stat.S_IMODE(path.stat().st_mode) & 0o077:
        raise ConfigurationError(f"Secret file permissions must be 600: {label}")
    value = path.read_text(encoding="utf-8").strip()
    if not value:
        raise ConfigurationError(f"Required secret file is empty: {label}")
    return value


def _read_secret(directory: Path, name: str) -> str:
    return _read_secret_path(directory / name, name)


def _optional_secret(file_env: str, value_env: str, label: str) -> str:
    file_name = os.getenv(file_env, "").strip()
    if file_name:
        return _read_secret_path(Path(file_name), label)
    return os.getenv(value_env, "")


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ConfigurationError(f"Invalid boolean environment variable: {name}")


def _route_prefix(name: str, default: str) -> str:
    value = os.getenv(name, default).strip()
    if value and (not value.startswith("/") or any(char.isspace() for char in value)):
        raise ConfigurationError(f"Invalid route prefix environment variable: {name}")
    return value.rstrip("/")


@dataclass(frozen=True, slots=True)
class Settings:
    feishu_app_id: str
    feishu_app_secret: str
    feishu_allowed_open_id: str
    feishu_alert_recipient_open_id: str
    user_hmac_key: str
    notification_hmac_key: str
    control_plane_key: str
    state_db: Path
    prometheus_base_url: str = "http://prometheus:9090"
    control_plane_base_url: str = "http://host.docker.internal:18083"
    administrative_ingress_base_url: str = ""
    administrative_ingress_shared_secret: str = ""
    administrative_communication_hmac_key: str = ""
    administrative_route_prefix: str = ""
    feishu_base_url: str = "https://open.feishu.cn"
    host: str = "0.0.0.0"
    port: int = 8082
    public_port: int = 8083
    ws_enabled: bool = True
    notification_ttl_seconds: int = 300
    event_retention_seconds: int = 604_800

    @classmethod
    def from_environment(cls) -> Settings:
        secrets_dir = Path(os.getenv("GATEWAY_SECRETS_DIR", "/run/secrets"))
        user_open_id = _read_secret(secrets_dir, "feishu_user_open_id")
        shared_secret_file = os.getenv("ADMINISTRATIVE_INGRESS_SHARED_SECRET_FILE", "").strip()
        shared_secret = (
            _read_secret_path(Path(shared_secret_file), "administrative ingress shared secret")
            if shared_secret_file
            else os.getenv("ADMINISTRATIVE_INGRESS_SHARED_SECRET", "")
        )
        communication_key = _optional_secret(
            "ADMINISTRATIVE_COMMUNICATION_HMAC_KEY_FILE",
            "ADMINISTRATIVE_COMMUNICATION_HMAC_KEY",
            "administrative communication HMAC key",
        )
        return cls(
            feishu_app_id=_read_secret(secrets_dir, "feishu_app_id"),
            feishu_app_secret=_read_secret(secrets_dir, "feishu_app_secret"),
            feishu_allowed_open_id=user_open_id,
            feishu_alert_recipient_open_id=user_open_id,
            user_hmac_key=_read_secret(secrets_dir, "user_hmac_key"),
            notification_hmac_key=_read_secret(secrets_dir, "notification_hmac_key"),
            control_plane_key=_read_secret(secrets_dir, "control_plane_key"),
            state_db=Path(os.getenv("GATEWAY_STATE_DB", "/var/lib/feishu-gateway/state.db")),
            prometheus_base_url=os.getenv("PROMETHEUS_BASE_URL", "http://prometheus:9090"),
            control_plane_base_url=os.getenv(
                "CONTROL_PLANE_BASE_URL", "http://host.docker.internal:18083"
            ),
            administrative_ingress_base_url=os.getenv("ADMINISTRATIVE_INGRESS_BASE_URL", ""),
            administrative_ingress_shared_secret=shared_secret,
            administrative_communication_hmac_key=communication_key,
            administrative_route_prefix=_route_prefix("ADMINISTRATIVE_ROUTE_PREFIX", "/admin"),
            feishu_base_url=os.getenv("FEISHU_BASE_URL", "https://open.feishu.cn"),
            host=os.getenv("GATEWAY_HOST", "0.0.0.0"),
            port=int(os.getenv("GATEWAY_PORT", "8082")),
            public_port=int(os.getenv("GATEWAY_PUBLIC_PORT", "8083")),
            ws_enabled=_env_bool("FEISHU_WS_ENABLED", True),
        )
