from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from feishu_dify_gateway.config import Settings
from feishu_dify_gateway.metrics import Metrics
from feishu_dify_gateway.service import GatewayService
from feishu_dify_gateway.store import StateStore


class FakeSender:
    def __init__(self) -> None:
        self.messages: list[tuple[str, str]] = []
        self.idempotency_keys: list[str] = []
        self.is_ready = True
        self.failure: Exception | None = None

    async def send_text(self, recipient_open_id: str, text: str, idempotency_key: str) -> None:
        self.idempotency_keys.append(idempotency_key)
        if self.failure is not None:
            raise self.failure
        self.messages.append((recipient_open_id, text))

    async def ready(self) -> bool:
        return self.is_ready

    async def close(self) -> None:
        return None


class FakePrometheus:
    def __init__(self) -> None:
        self.is_ready = True
        self.alerts: list[tuple[str, str]] = []

    async def ready(self) -> bool:
        return self.is_ready

    async def active_alerts(self) -> list[tuple[str, str]]:
        return list(self.alerts)

    async def close(self) -> None:
        return None


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        feishu_app_id="app-id",
        feishu_app_secret="app-secret",
        feishu_allowed_open_id="allowed-user",
        feishu_alert_recipient_open_id="alert-user",
        user_hmac_key="user-key",
        notification_hmac_key="notify-key",
        control_plane_key="cp-key",
        state_db=tmp_path / "state.db",
        ws_enabled=False,
    )


class FakeControlPlane:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict | None]] = []

    async def request(self, method: str, path: str, body: dict | None = None) -> str:
        self.calls.append((method, path, body))
        return f"control-plane:{method} {path}"

    async def close(self) -> None:
        return None


@pytest.fixture
async def service(
    settings: Settings,
) -> AsyncIterator[tuple[GatewayService, FakeSender, FakePrometheus, FakeControlPlane]]:
    sender = FakeSender()
    prometheus = FakePrometheus()
    control_plane = FakeControlPlane()
    gateway = GatewayService(
        settings,
        StateStore(settings.state_db),
        Metrics(),
        sender,
        prometheus,
        control_plane,
    )
    yield gateway, sender, prometheus, control_plane
    await gateway.close()
