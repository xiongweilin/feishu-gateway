from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from fastapi.testclient import TestClient
from lark_channel import (  # type: ignore[import-untyped]
    CardActionEvent,
    CardActionPayload,
    EventOperator,
    FileContent,
    ResourceDescriptor,
)

from feishu_dify_gateway.autodev import (
    AutodevBridge,
    AutodevMetrics,
    _action_value,
    _channel_error,
    _connection_notice,
    _event_text,
    _intervention_card,
    _is_status_command,
    _safe,
    _status_text,
    create_autodev_app,
)
from feishu_dify_gateway.autodev_config import AutodevSettings
from feishu_dify_gateway.autodev_content import (
    RequirementContentRejected,
    attachment_from_message,
    text_from_message,
)
from feishu_dify_gateway.autodev_operator import (
    OperatorApiError,
    OperatorClient,
    OperatorEvent,
    OperatorRequirement,
)
from feishu_dify_gateway.autodev_store import AutodevStore
from feishu_dify_gateway.config import ConfigurationError

from .test_autodev_profile import FakeChannel, FakeOperator, message, settings


def test_autodev_settings_loads_secrets_and_rejects_unsafe_values(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    secrets = tmp_path / "secrets"
    secrets.mkdir()
    for name, value in {
        "app_id": "app-id",
        "app_secret": "app-secret",
        "owner_open_id": "ou-owner",
        "operator_hmac_secret": "operator-secret",
    }.items():
        (secrets / name).write_text(value, encoding="utf-8")
    monkeypatch.setenv("AUTODEV_FEISHU_SECRETS_DIR", str(secrets))
    monkeypatch.setenv("AUTODEV_FEISHU_STATE_DB", str(tmp_path / "state.db"))
    monkeypatch.setenv("AUTODEV_OPERATOR_BASE_URL", "http://127.0.0.1:9000/")
    monkeypatch.setenv("AUTODEV_FEISHU_PORT", "19085")
    monkeypatch.setenv("AUTODEV_FEISHU_MAX_FILE_BYTES", "2048")

    resolved = AutodevSettings.from_environment()

    assert resolved.app_id == "app-id"
    assert resolved.operator_base_url == "http://127.0.0.1:9000"
    assert resolved.port == 19085
    assert resolved.max_file_bytes == 2048

    with pytest.raises(ConfigurationError, match="positive"):
        monkeypatch.setenv("AUTODEV_FEISHU_PORT", "0")
        AutodevSettings.from_environment()
    with pytest.raises(ConfigurationError, match="loopback URL"):
        monkeypatch.setenv("AUTODEV_OPERATOR_BASE_URL", "http://localhost:9000")
        AutodevSettings.from_environment()
    with pytest.raises(ConfigurationError, match="bind to"):
        AutodevSettings(
            app_id="app",
            app_secret="secret",
            owner_open_id="owner",
            operator_hmac_secret="secret",
            target_id="target",
            state_db=tmp_path / "state.db",
            host="0.0.0.0",
        )


def test_text_message_variants_and_attachment_failures() -> None:
    post = SimpleNamespace(
        content=SimpleNamespace(kind="post", title="Title", text="Post body"),
        resources=(),
    )
    plain = SimpleNamespace(content=None, content_text="Fallback body", resources=())
    with_resource = SimpleNamespace(content=None, content_text="Fallback body", resources=("file",))

    assert text_from_message(post, max_chars=100).title == "Title"
    assert text_from_message(plain, max_chars=100).text == "Fallback body"
    assert text_from_message(with_resource, max_chars=100) is None


@pytest.mark.asyncio
async def test_attachment_failures_are_rejected() -> None:
    class EmptyChannel:
        async def download_resource(self, *_: object, **__: object) -> bytes:
            return b""

    missing = SimpleNamespace(
        id="missing",
        content=FileContent(file_key="", file_name="requirement.txt"),
        resources=(),
    )
    image = SimpleNamespace(
        id="image",
        content=FileContent(file_key="file-key", file_name="requirement.png"),
        resources=[
            ResourceDescriptor(type="image", file_key="file-key", file_name="requirement.png")
        ],
    )
    bad_utf8 = SimpleNamespace(
        id="bad-utf8",
        content=FileContent(file_key="file-key", file_name="requirement.txt"),
        resources=[
            ResourceDescriptor(type="file", file_key="file-key", file_name="requirement.txt")
        ],
    )

    with pytest.raises(RequirementContentRejected, match="附件"):
        await attachment_from_message(EmptyChannel(), missing, max_bytes=100, max_chars=100)
    with pytest.raises(RequirementContentRejected, match="只支持"):
        await attachment_from_message(EmptyChannel(), image, max_bytes=100, max_chars=100)
    with pytest.raises(RequirementContentRejected, match="为空"):
        await attachment_from_message(EmptyChannel(), bad_utf8, max_bytes=100, max_chars=100)


@pytest.mark.asyncio
async def test_operator_client_covers_actions_events_and_retries() -> None:
    calls: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        if request.url.path == "/v1/operator/events":
            return httpx.Response(
                200,
                json={
                    "events": [
                        {
                            "eventId": "event-1",
                            "sequence": 1,
                            "requestId": "request-1",
                            "cycleId": "cycle-1",
                            "eventType": "completed",
                            "payload": {"status": "completed"},
                        }
                    ]
                },
            )
        return httpx.Response(200, json={"status": "ok"})

    client = OperatorClient(
        "http://127.0.0.1:8765",
        "operator-secret",
        max_attempts=1,
        transport=httpx.MockTransport(handler),
    )
    try:
        requirement = OperatorRequirement(
            request_id="request-1",
            target_id="target-1",
            source="test",
            external_reference_digest="external",
            title="Title",
            normalized_requirement_text="Body",
            content_sha256="a" * 64,
        )
        assert (await client.submit(requirement))["status"] == "ok"
        assert (await client.status("request-1"))["status"] == "ok"
        assert (await client.start("request-1"))["status"] == "ok"
        assert (await client.cancel("request-1"))["status"] == "ok"
        assert (await client.respond("intervention-1", "answer"))["status"] == "ok"
        events = await client.events(0, limit=10)
        assert events == (
            OperatorEvent(
                event_id="event-1",
                sequence=1,
                request_id="request-1",
                cycle_id="cycle-1",
                event_type="completed",
                payload={"status": "completed"},
            ),
        )
        assert (await client.acknowledge("event-1"))["status"] == "ok"
        assert "/v1/operator/events" in calls
    finally:
        await client.close()

    attempts = 0

    async def retry_handler(_: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return httpx.Response(503 if attempts == 1 else 200, json={"status": "recovered"})

    retrying = OperatorClient(
        "http://127.0.0.1:8765",
        "operator-secret",
        max_attempts=2,
        transport=httpx.MockTransport(retry_handler),
    )
    try:
        assert (await retrying.status("request-1"))["status"] == "recovered"
    finally:
        await retrying.close()
    assert attempts == 2


@pytest.mark.asyncio
async def test_operator_client_rejects_bad_responses_and_configuration() -> None:
    with pytest.raises(ValueError):
        OperatorClient("http://localhost:8765", "secret")
    with pytest.raises(ValueError):
        OperatorClient("http://127.0.0.1:8765", "")
    with pytest.raises(ValueError):
        OperatorClient("http://127.0.0.1:8765", "secret", max_attempts=0)

    async def invalid_handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[])

    invalid = OperatorClient(
        "http://127.0.0.1:8765",
        "secret",
        max_attempts=1,
        transport=httpx.MockTransport(invalid_handler),
    )
    try:
        with pytest.raises(OperatorApiError, match="invalid response"):
            await invalid.status("request-1")
    finally:
        await invalid.close()


class _AppBridge:
    def __init__(self, ready: bool) -> None:
        self.ready = ready
        self.metrics = AutodevMetrics()

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None


def test_autodev_app_helpers_and_health_endpoints() -> None:
    ready_app = create_autodev_app(_AppBridge(True))
    with TestClient(ready_app) as client:
        assert client.get("/healthz").json() == {"status": "ok"}
        assert client.get("/readyz").status_code == 200
        assert "autodev_feishu_connection_up" in client.get("/metrics").text

    not_ready_app = create_autodev_app(_AppBridge(False))
    with TestClient(not_ready_app) as client:
        assert client.get("/readyz").status_code == 503

    event = OperatorEvent(
        event_id="event-1",
        sequence=1,
        request_id=None,
        cycle_id=None,
        event_type="unknown",
        payload={"question": "Choose", "intervention_id": "int-1", "choices": ["yes", 1]},
    )
    card = _intervention_card(event)
    assert len(card["body"]["elements"]) == 3
    assert _event_text(event).startswith("Autonomous Development 状态发生重要变化。")
    assert "request_id=-" in _status_text({})
    assert _safe(None) == "-"
    assert len(_safe("x" * 300)) == 256
    assert _is_status_command(" /STATUS ")
    assert not _is_status_command("start")


def test_action_value_parses_provider_shapes() -> None:
    base = dict(message_id="message-1", chat_id="chat-1", operator=EventOperator(open_id="owner"))
    dict_event = CardActionEvent(
        **base,
        action=CardActionPayload(value={"action": "start"}),
    )
    string_event = CardActionEvent(
        **base,
        action=CardActionPayload(value=json.dumps({"action": "cancel"})),
    )
    invalid_event = CardActionEvent(
        **base,
        action=CardActionPayload(value="not-json"),
    )
    assert _action_value(dict_event) == {"action": "start"}
    assert _action_value(string_event) == {"action": "cancel"}
    assert _action_value(invalid_event) == {}


@pytest.mark.asyncio
async def test_card_cancel_and_intervention_actions_are_processed(tmp_path: Path) -> None:
    channel = FakeChannel()
    operator = FakeOperator()
    store = AutodevStore(tmp_path / "state.db")
    bridge = AutodevBridge(settings(tmp_path), store, operator, channel)
    await bridge.handle_message(message())
    await bridge.handle_card_action(
        CardActionEvent(
            message_id="cancel-message",
            chat_id="oc-chat",
            operator=EventOperator(open_id="ou-owner"),
            action=CardActionPayload(value={"action": "cancel", "request_id": "feishu:message-1"}),
        )
    )
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert operator.cancelled == ["feishu:message-1"]

    store.set_pending_intervention(
        chat_id="oc-chat",
        request_id="feishu:message-1",
        intervention_id="intervention-1",
        notification_message_id="notification-1",
    )
    await bridge.handle_card_action(
        CardActionEvent(
            message_id="intervention-message",
            chat_id="oc-chat",
            operator=EventOperator(open_id="ou-owner"),
            action=CardActionPayload(
                value={
                    "action": "intervention",
                    "request_id": "feishu:message-1",
                    "intervention_id": "intervention-1",
                    "response": "yes",
                }
            ),
        )
    )
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert operator.responses == [("intervention-1", "yes")]
    assert store.pending_intervention("oc-chat") is None
    await bridge.stop()


@pytest.mark.asyncio
async def test_connection_callbacks_are_callable() -> None:
    await _connection_notice("reconnected")(None)
    await _channel_error(None)
