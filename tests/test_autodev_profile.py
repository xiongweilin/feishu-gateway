from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest
from lark_channel import (  # type: ignore[import-untyped]
    CardActionEvent,
    CardActionPayload,
    Conversation,
    EventOperator,
    Identity,
    InboundMessage,
    SendResult,
    TextContent,
)

from feishu_dify_gateway.autodev import AutodevBridge
from feishu_dify_gateway.autodev_config import AutodevSettings
from feishu_dify_gateway.autodev_operator import OperatorEvent
from feishu_dify_gateway.autodev_store import AutodevStore
from feishu_dify_gateway.config import Settings


class FakeChannel:
    def __init__(self) -> None:
        self.sent: list[tuple[str, object, object]] = []

    async def send(self, chat_id: str, message: object, opts: object) -> SendResult:
        self.sent.append((chat_id, message, opts))
        return SendResult(success=True, message_id=f"sent-{len(self.sent)}")

    async def download_resource(self, file_key: str, **_: object) -> bytes:
        return b"# requirement\n\nDo the thing."


class FakeOperator:
    def __init__(self) -> None:
        self.submissions: list[object] = []
        self.started: list[str] = []
        self.cancelled: list[str] = []
        self.responses: list[tuple[str, str]] = []
        self.acknowledged: list[str] = []
        self.event_items: tuple[OperatorEvent, ...] = ()

    async def submit(self, requirement: object) -> dict[str, Any]:
        self.submissions.append(requirement)
        return {"requestId": "feishu:message-1", "targetId": "target-1"}

    async def status(self, request_id: str) -> dict[str, Any]:
        return {
            "requestId": request_id,
            "targetId": "target-1",
            "status": "received",
            "servingReleaseId": "release-1",
        }

    async def start(self, request_id: str) -> dict[str, Any]:
        self.started.append(request_id)
        return {"requestId": request_id, "status": "ready"}

    async def cancel(self, request_id: str) -> dict[str, Any]:
        self.cancelled.append(request_id)
        return {"requestId": request_id, "status": "cancelled"}

    async def respond(self, intervention_id: str, response: str) -> dict[str, Any]:
        self.responses.append((intervention_id, response))
        return {"status": "ready"}

    async def events(self, after: int, limit: int = 50) -> tuple[OperatorEvent, ...]:
        return tuple(item for item in self.event_items if item.sequence > after)[:limit]

    async def acknowledge(self, event_id: str) -> dict[str, Any]:
        self.acknowledged.append(event_id)
        return {"eventId": event_id, "acknowledged": True}

    async def close(self) -> None:
        return None


def settings(tmp_path: Path) -> AutodevSettings:
    return AutodevSettings(
        app_id="new-app",
        app_secret="new-secret",
        owner_open_id="ou-owner",
        operator_hmac_secret="operator-secret",
        target_id="target-1",
        state_db=tmp_path / "autodev.db",
    )


def test_autodev_profile_is_loopback_and_old_settings_constructor_is_separate(
    tmp_path: Path,
) -> None:
    profile = settings(tmp_path)
    assert profile.host == "127.0.0.1"
    assert profile.state_db != Path("/var/lib/feishu-gateway/state.db")
    assert Settings.__name__ == "Settings"


def message(
    *,
    sender: str = "ou-owner",
    chat_type: str = "p2p",
    message_id: str = "message-1",
) -> InboundMessage:
    return InboundMessage(
        id=message_id,
        create_time=0,
        conversation=Conversation(chat_id="oc-chat", chat_type=chat_type),
        sender=Identity(open_id=sender),
        content=TextContent(text="Implement a deterministic response."),
    )


@pytest.mark.asyncio
async def test_owner_p2p_requirement_is_submitted_once_and_non_owner_is_rejected(
    tmp_path: Path,
) -> None:
    channel = FakeChannel()
    operator = FakeOperator()
    bridge = AutodevBridge(
        settings(tmp_path),
        AutodevStore(tmp_path / "state.db"),
        operator,
        channel,
    )

    await bridge.handle_message(message())
    await bridge.handle_message(message())
    await bridge.handle_message(message(sender="ou-other", message_id="message-2"))
    await bridge.handle_message(message(chat_type="group", message_id="message-3"))

    assert len(operator.submissions) == 1
    assert len(channel.sent) == 1
    assert channel.sent[0][0] == "ou-owner"
    assert channel.sent[0][2].receive_id_type == "open_id"
    assert channel.sent[0][2].uuid.startswith("autodev-")
    assert len(channel.sent[0][2].uuid) <= 50
    card = channel.sent[0][1]["card"]
    button_group = card["body"]["elements"][-1]
    assert button_group["tag"] == "column_set"
    button = button_group["columns"][0]["elements"][0]
    assert button["behaviors"][0]["type"] == "callback"
    assert button["behaviors"][0]["value"]["action"] == "start"
    assert bridge.store.request_for_chat("oc-chat") == "feishu:message-1"


@pytest.mark.asyncio
async def test_card_action_is_fast_acknowledged_and_start_is_idempotent(tmp_path: Path) -> None:
    channel = FakeChannel()
    operator = FakeOperator()
    store = AutodevStore(tmp_path / "state.db")
    bridge = AutodevBridge(settings(tmp_path), store, operator, channel)
    await bridge.handle_message(message())

    event = CardActionEvent(
        message_id="sent-1",
        chat_id="oc-chat",
        operator=EventOperator(open_id="ou-owner"),
        action=CardActionPayload(value={"action": "start", "request_id": "feishu:message-1"}),
    )
    await bridge.handle_card_action(event)
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert operator.started == ["feishu:message-1"]
    assert len(channel.sent) >= 3
    assert all(item[0] == "ou-owner" for item in channel.sent)
    assert all(item[2].receive_id_type == "open_id" for item in channel.sent)
    await bridge.stop()


@pytest.mark.asyncio
async def test_attachment_resource_is_not_needed_for_text_and_outbox_survives_restart(
    tmp_path: Path,
) -> None:
    channel = FakeChannel()
    operator = FakeOperator()
    store = AutodevStore(tmp_path / "state.db")
    bridge = AutodevBridge(settings(tmp_path), store, operator, channel)
    await bridge.handle_message(message())
    operator.event_items = (
        OperatorEvent(
            event_id="event-1",
            sequence=1,
            request_id="feishu:message-1",
            cycle_id="cycle-1",
            event_type="completed",
            payload={"status": "completed"},
        ),
    )
    await bridge._drain_events()
    assert operator.acknowledged == ["event-1"]
    assert store.operator_cursor() == 1
    await bridge._drain_events()
    assert operator.acknowledged == ["event-1"]


@pytest.mark.asyncio
async def test_unbound_operator_event_does_not_block_feishu_delivery(tmp_path: Path) -> None:
    channel = FakeChannel()
    operator = FakeOperator()
    store = AutodevStore(tmp_path / "state.db")
    bridge = AutodevBridge(settings(tmp_path), store, operator, channel)
    await bridge.handle_message(message())
    operator.event_items = (
        OperatorEvent(
            event_id="unbound-event",
            sequence=1,
            request_id="local-acceptance-request",
            cycle_id="cycle-0",
            event_type="failed",
            payload={"status": "failed"},
        ),
        OperatorEvent(
            event_id="bound-event",
            sequence=2,
            request_id="feishu:message-1",
            cycle_id="cycle-1",
            event_type="completed",
            payload={"status": "completed"},
        ),
    )

    await bridge._drain_events()

    assert store.operator_cursor() == 2
    assert operator.acknowledged == ["bound-event"]
    assert len(channel.sent) == 2


def test_store_has_separate_durable_state(tmp_path: Path) -> None:
    first = AutodevStore(tmp_path / "first.db")
    second = AutodevStore(tmp_path / "second.db")
    assert first.operator_cursor() == 0
    assert second.operator_cursor() == 0
    first.set_operator_cursor(3)
    assert first.operator_cursor() == 3
    assert second.operator_cursor() == 0
