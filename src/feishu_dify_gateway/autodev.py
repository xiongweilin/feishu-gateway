from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager, suppress
from typing import Any

from fastapi import FastAPI
from fastapi.responses import JSONResponse, Response
from lark_channel import (  # type: ignore[import-untyped]
    CardActionEvent,
    Events,
    FeishuChannel,
    InboundConfig,
    InboundMessage,
    PolicyConfig,
    SecurityConfig,
    SendOpts,
)
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    generate_latest,
)

from .autodev_config import AutodevSettings
from .autodev_content import (
    NormalizedRequirement,
    RequirementContentRejected,
    attachment_from_message,
    text_from_message,
)
from .autodev_operator import OperatorApiError, OperatorClient, OperatorEvent, OperatorRequirement
from .autodev_store import AutodevStore

logger = logging.getLogger(__name__)


class AutodevMetrics:
    def __init__(self) -> None:
        self.registry = CollectorRegistry()
        self.connection_up = Gauge(
            "autodev_feishu_connection_up",
            "Whether the independent Autonomous Development Feishu channel is connected.",
            registry=self.registry,
        )
        self.events = Counter(
            "autodev_feishu_events_total",
            "Inbound events observed by the independent profile.",
            ("kind", "outcome"),
            registry=self.registry,
        )
        self.deliveries = Counter(
            "operator_outbox_delivery_total",
            "Durable operator event delivery attempts.",
            ("event_type", "outcome"),
            registry=self.registry,
        )
        self.pending_events = Gauge(
            "pending_operator_events",
            "Last observed count of unacknowledged operator events.",
            registry=self.registry,
        )
        self.pending_interventions = Gauge(
            "pending_interventions",
            "Pending intervention notifications owned by this bridge.",
            registry=self.registry,
        )


class AutodevBridge:
    def __init__(
        self,
        settings: AutodevSettings,
        store: AutodevStore,
        operator: OperatorClient,
        channel: Any,
        *,
        metrics: AutodevMetrics | None = None,
    ) -> None:
        self.settings = settings
        self.store = store
        self.operator = operator
        self.channel = channel
        self.metrics = metrics or AutodevMetrics()
        self._stop = asyncio.Event()
        self._connection_task: asyncio.Task[None] | None = None
        self._outbox_task: asyncio.Task[None] | None = None
        self._action_tasks: set[asyncio.Task[None]] = set()
        self._connection_state = False

    @property
    def connection_up(self) -> bool:
        return self._connection_state

    @property
    def ready(self) -> bool:
        return self.connection_up and self._outbox_task is not None

    async def start(self) -> None:
        self._stop.clear()
        self._connection_task = asyncio.create_task(self._connection_loop())
        self._outbox_task = asyncio.create_task(self._outbox_loop())

    async def stop(self) -> None:
        self._stop.set()
        tasks = [task for task in (self._connection_task, self._outbox_task) if task is not None]
        for task in tasks:
            task.cancel()
        for task in self._action_tasks:
            task.cancel()
        tasks.extend(self._action_tasks)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._connection_task = None
        self._outbox_task = None
        self._action_tasks.clear()
        self._connection_state = False
        self.metrics.connection_up.set(0)
        stop_background = getattr(self.channel, "stop_background", None)
        if callable(stop_background):
            try:
                await stop_background()
            except Exception:
                logger.warning("autodev feishu channel stop failed", extra={"event": "stop_failed"})
        await self.operator.close()

    async def handle_message(self, message: InboundMessage) -> None:
        if not self._authorized_message(message):
            self.metrics.events.labels("message", "rejected").inc()
            return
        event_id = str(message.id)
        if not self.store.claim_inbound(event_id, "message"):
            self.metrics.events.labels("message", "duplicate").inc()
            return
        chat_id = str(message.conversation.chat_id)
        try:
            normalized = text_from_message(message, max_chars=self.settings.max_text_chars)
            if normalized is None:
                normalized = await attachment_from_message(
                    self.channel,
                    message,
                    max_bytes=self.settings.max_file_bytes,
                    max_chars=self.settings.max_text_chars,
                )
            if _is_status_command(normalized.text):
                await self._send_status(chat_id, uuid=f"status:{event_id}")
            elif await self._try_intervention_response(message, normalized):
                pass
            else:
                await self._submit_requirement(message, chat_id, normalized)
            self.store.complete_inbound(event_id)
            self.metrics.events.labels("message", "accepted").inc()
        except RequirementContentRejected as exc:
            self.store.complete_inbound(event_id)
            await self._safe_send(chat_id, str(exc), uuid=f"reject:{event_id}")
            self.metrics.events.labels("message", "rejected").inc()
        except OperatorApiError:
            self.store.release_inbound(event_id)
            await self._safe_send(
                chat_id,
                "Autonomous Development 当前不可用，请稍后重试；已有开发流程不会因飞书掉线而停止。",
                uuid=f"operator-unavailable:{event_id}",
            )
            self.metrics.events.labels("message", "operator_unavailable").inc()
        except Exception:
            self.store.release_inbound(event_id)
            logger.exception("autodev message handling failed", extra={"event": "message_failed"})
            await self._safe_send(chat_id, "需求处理失败，请稍后重试。", uuid=f"failed:{event_id}")
            self.metrics.events.labels("message", "failed").inc()

    async def handle_card_action(self, event: CardActionEvent) -> None:
        if event.operator.open_id != self.settings.owner_open_id:
            self.metrics.events.labels("card_action", "rejected").inc()
            return
        action = _action_value(event)
        request_id = action.get("request_id")
        action_name = action.get("action")
        if not isinstance(request_id, str) or not isinstance(action_name, str):
            self.metrics.events.labels("card_action", "rejected").inc()
            return
        if self.store.request_for_chat(event.chat_id) != request_id:
            self.metrics.events.labels("card_action", "rejected").inc()
            return
        action_id = hashlib.sha256(
            json.dumps(
                {"message_id": event.message_id, "request_id": request_id, "action": action},
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        if not self.store.claim_card_action(action_id):
            self.metrics.events.labels("card_action", "duplicate").inc()
            return
        try:
            await self._send_text(
                event.chat_id,
                "已收到操作请求，正在处理。",
                uuid=f"card-ack:{action_id}",
            )
        except Exception:
            self.store.release_card_action(action_id)
            self.metrics.events.labels("card_action", "failed").inc()
            return
        task = asyncio.create_task(
            self._process_card_action(event.chat_id, request_id, action_name, action)
        )
        self._action_tasks.add(task)
        task.add_done_callback(self._action_tasks.discard)
        self.metrics.events.labels("card_action", "accepted").inc()

    async def _process_card_action(
        self,
        chat_id: str,
        request_id: str,
        action_name: str,
        action: dict[str, Any],
    ) -> None:
        try:
            if action_name == "start":
                await self.operator.start(request_id)
                await self._send_text(
                    chat_id,
                    f"自主开发已启动。request_id={request_id}",
                    uuid=f"started:{request_id}",
                )
            elif action_name == "cancel":
                await self.operator.cancel(request_id)
                await self._send_text(
                    chat_id,
                    f"已提交安全取消请求。request_id={request_id}",
                    uuid=f"cancelled:{request_id}",
                )
            elif action_name == "intervention":
                intervention_id = action.get("intervention_id")
                response = action.get("response")
                if not isinstance(intervention_id, str) or not isinstance(response, str):
                    return
                await self.operator.respond(intervention_id, response)
                self.store.clear_pending_intervention(chat_id, intervention_id)
                await self._send_text(
                    chat_id,
                    "人工介入答案已收到，系统将从持久化状态继续。",
                    uuid=f"intervention-accepted:{intervention_id}",
                )
        except OperatorApiError:
            await self._safe_send(
                chat_id,
                "操作暂未完成，请稍后查看状态并重试。",
                uuid=f"action-failed:{request_id}",
            )
        except Exception:
            logger.exception("autodev card action failed", extra={"event": "card_action_failed"})

    async def _submit_requirement(
        self,
        message: InboundMessage,
        chat_id: str,
        normalized: NormalizedRequirement,
    ) -> None:
        request_id = f"feishu:{message.id}"
        external_digest = hashlib.sha256(f"feishu:{message.id}".encode()).hexdigest()
        await self.operator.submit(
            OperatorRequirement(
                request_id=request_id,
                target_id=self.settings.target_id,
                source="feishu-autodev",
                external_reference_digest=external_digest,
                title=normalized.title,
                normalized_requirement_text=normalized.text,
                content_sha256=normalized.content_sha256,
            )
        )
        self.store.record_request(
            request_id=request_id,
            chat_id=chat_id,
            source_message_id=message.id,
            title=normalized.title,
            content_sha256=normalized.content_sha256,
        )
        status = await self.operator.status(request_id)
        await self._send_card(
            chat_id,
            _confirmation_card(request_id, normalized, status),
            uuid=f"received:{request_id}",
        )

    async def _try_intervention_response(
        self,
        message: InboundMessage,
        normalized: NormalizedRequirement,
    ) -> bool:
        pending = self.store.pending_intervention(message.conversation.chat_id)
        reply = getattr(message, "reply", None)
        if pending is None or reply is None or reply.message_id != pending[2]:
            return False
        await self.operator.respond(pending[1], normalized.text)
        self.store.clear_pending_intervention(message.conversation.chat_id, pending[1])
        await self._send_text(
            message.conversation.chat_id,
            "人工介入答案已收到，系统将从持久化状态继续。",
            uuid=f"intervention-accepted:{pending[1]}",
        )
        return True

    async def _send_status(self, chat_id: str, *, uuid: str) -> None:
        request_id = self.store.request_for_chat(chat_id)
        if request_id is None:
            await self._send_text(
                chat_id,
                "当前没有正在跟踪的 Autonomous Development 需求。",
                uuid=uuid,
            )
            return
        status = await self.operator.status(request_id)
        await self._send_text(chat_id, _status_text(status), uuid=uuid)

    async def _outbox_loop(self) -> None:
        while not self._stop.is_set():
            try:
                await self._drain_events()
            except OperatorApiError:
                logger.warning("operator outbox poll failed", extra={"event": "outbox_poll_failed"})
            except Exception:
                logger.exception("operator outbox loop failed", extra={"event": "outbox_failed"})
            with suppress(TimeoutError):
                await asyncio.wait_for(
                    self._stop.wait(), timeout=self.settings.poll_interval_seconds
                )

    async def _drain_events(self) -> None:
        cursor = self.store.operator_cursor()
        events = await self.operator.events(cursor)
        for event in events:
            self.metrics.pending_events.set(max(0, len(events)))
            if event.event_type == "requirement_received":
                await self._ack_event(event)
                continue
            chat_id = self.store.chat_for_request(event.request_id or "")
            if chat_id is None:
                return
            if self.store.delivery_status(event.event_id) != "sent":
                self.store.record_delivery_attempt(event.event_id)
                try:
                    message_id = await self._deliver_event(chat_id, event)
                except Exception:
                    self.store.record_delivery_attempt(event.event_id, "send failed")
                    self.metrics.deliveries.labels(event.event_type, "failed").inc()
                    return
                if event.event_type == "needs_human":
                    intervention_id = event.payload.get("intervention_id")
                    if isinstance(intervention_id, str):
                        self.store.set_pending_intervention(
                            chat_id=chat_id,
                            request_id=event.request_id or "",
                            intervention_id=intervention_id,
                            notification_message_id=message_id,
                        )
                self.store.mark_delivery_sent(event.event_id)
                self.metrics.deliveries.labels(event.event_type, "sent").inc()
            await self._ack_event(event)

    async def _ack_event(self, event: OperatorEvent) -> None:
        await self.operator.acknowledge(event.event_id)
        self.store.set_operator_cursor(event.sequence)

    async def _deliver_event(self, chat_id: str, event: OperatorEvent) -> str:
        if event.event_type == "needs_human":
            return await self._send_card(
                chat_id,
                _intervention_card(event),
                uuid=f"event:{event.event_id}",
            )
        return await self._send_text(
            chat_id,
            _event_text(event),
            uuid=f"event:{event.event_id}",
        )

    async def _safe_send(self, chat_id: str, text: str, *, uuid: str) -> None:
        try:
            await self._send_text(chat_id, text, uuid=uuid)
        except Exception:
            logger.warning("autodev response send failed", extra={"event": "send_failed"})

    async def _send_text(self, chat_id: str, text: str, *, uuid: str) -> str:
        # The V1 profile is owner-only P2P.  The inbound conversation's chat_id
        # is retained for durable correlation, but Lark's P2P create-message
        # endpoint requires the owner's open_id as the recipient.
        result = await self.channel.send(
            self.settings.owner_open_id,
            {"text": text},
            SendOpts(receive_id_type="open_id", uuid=uuid),
        )
        if not result.success:
            raise OperatorApiError("Feishu message send failed")
        return result.message_id or ""

    async def _send_card(self, chat_id: str, card: dict[str, Any], *, uuid: str) -> str:
        result = await self.channel.send(
            self.settings.owner_open_id,
            {"card": card},
            SendOpts(receive_id_type="open_id", uuid=uuid),
        )
        if not result.success:
            raise OperatorApiError("Feishu card send failed")
        return result.message_id or ""

    def _authorized_message(self, message: InboundMessage) -> bool:
        conversation = message.conversation
        sender = message.sender
        return bool(
            conversation.chat_type == "p2p" and sender.open_id == self.settings.owner_open_id
        )

    async def _connection_loop(self) -> None:
        while not self._stop.is_set():
            try:
                await self.channel.start_background(timeout=30.0)
                self._connection_state = True
                self.metrics.connection_up.set(1)
                while not self._stop.is_set() and bool(getattr(self.channel, "is_ready", False)):
                    await asyncio.sleep(1)
            except Exception:
                self._connection_state = False
                self.metrics.connection_up.set(0)
                logger.warning(
                    "autodev Feishu connection unavailable",
                    extra={"event": "connection_down"},
                )
            if not self._stop.is_set():
                await asyncio.sleep(5)


def create_autodev_channel(settings: AutodevSettings, bridge: AutodevBridge) -> FeishuChannel:
    channel = FeishuChannel(
        app_id=settings.app_id,
        app_secret=settings.app_secret,
        policy=PolicyConfig(
            dm_policy="allowlist",
            group_policy="disabled",
            require_mention=False,
            allow_from=[settings.owner_open_id],
        ),
        inbound=InboundConfig(media_max_mb=max(1, settings.max_file_bytes // (1024 * 1024))),
        security=SecurityConfig(
            mode="strict",
            max_ws_fragment_bytes=max(settings.max_file_bytes * 2, 2 * 1024 * 1024),
        ),
    )
    channel.on(Events.MESSAGE, bridge.handle_message)
    channel.on(Events.CARD_ACTION, bridge.handle_card_action)
    channel.on(Events.RECONNECTING, _connection_notice("reconnecting"))
    channel.on(Events.RECONNECTED, _connection_notice("reconnected"))
    channel.on(Events.ERROR, _channel_error)
    return channel


def create_autodev_app(bridge: AutodevBridge) -> FastAPI:
    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        await bridge.start()
        yield
        await bridge.stop()

    app = FastAPI(
        title="Autonomous Development Feishu Bridge",
        version="0.1.0",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )

    @app.get("/healthz", include_in_schema=False)
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz", include_in_schema=False)
    async def ready() -> Response:
        if bridge.ready:
            return JSONResponse({"status": "ready"})
        return JSONResponse({"status": "not-ready"}, status_code=503)

    @app.get("/metrics", include_in_schema=False)
    async def metrics() -> Response:
        return Response(
            generate_latest(bridge.metrics.registry),
            media_type=CONTENT_TYPE_LATEST,
        )

    return app


def create_runtime(
    settings: AutodevSettings | None = None,
) -> tuple[AutodevSettings, AutodevBridge]:
    resolved = settings or AutodevSettings.from_environment()
    store = AutodevStore(resolved.state_db)
    operator = OperatorClient(
        resolved.operator_base_url,
        resolved.operator_hmac_secret,
        timeout_seconds=resolved.operator_timeout_seconds,
    )
    bridge = AutodevBridge(resolved, store, operator, channel=None)
    bridge.channel = create_autodev_channel(resolved, bridge)
    return resolved, bridge


def main() -> None:
    import uvicorn

    settings, bridge = create_runtime()
    uvicorn.run(
        create_autodev_app(bridge),
        host=settings.host,
        port=settings.port,
        log_level="info",
    )


def _action_value(event: CardActionEvent) -> dict[str, Any]:
    value = event.action.value
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except ValueError:
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    return {}


def _is_status_command(text: str) -> bool:
    return text.strip().lower() in {"状态", "status", "/status"}


def _status_text(status: dict[str, Any]) -> str:
    return (
        "Autonomous Development 状态\n"
        f"request_id={_safe(status.get('requestId'))}\n"
        f"title={_safe(status.get('title'))}\n"
        f"status={_safe(status.get('status'))}\n"
        f"cycle_state={_safe(status.get('cycleState'))}\n"
        f"cycle_id={_safe(status.get('cycleId'))}\n"
        f"serving_release={_safe(status.get('servingReleaseId'))}\n"
        f"pending_intervention={_safe(status.get('pendingInterventionId'))}"
    )


def _event_text(event: OperatorEvent) -> str:
    labels = {
        "requirement_ready": "需求已完成分析，等待启动。",
        "development_started": "自主开发已启动。",
        "completed": "自主闭环完成。",
        "rolled_back": "自主开发已回滚，系统已恢复到安全状态。",
        "failed": "自主开发失败，系统未继续扩大影响。",
        "cancelled": "自主开发已取消。",
    }
    message = labels.get(event.event_type, "Autonomous Development 状态发生重要变化。")
    return f"{message}\nrequest_id={_safe(event.request_id)}\ncycle_id={_safe(event.cycle_id)}"


def _confirmation_card(
    request_id: str,
    normalized: NormalizedRequirement,
    status: dict[str, Any],
) -> dict[str, Any]:
    buttons = [
        _callback_button(
            "开始自主开发",
            {"action": "start", "request_id": request_id},
            button_type="primary_filled",
        ),
        _callback_button("取消", {"action": "cancel", "request_id": request_id}),
    ]
    return {
        "schema": "2.0",
        "header": {"title": {"tag": "plain_text", "content": "已收到需求"}},
        "body": {
            "elements": [
                {"tag": "markdown", "content": normalized.title},
                {
                    "tag": "markdown",
                    "content": (
                        f"长度：{len(normalized.text)}\n"
                        f"摘要指纹：{normalized.content_sha256[:12]}\n"
                        f"target：{status.get('targetId', 'configured')}\n"
                        f"serving release：{status.get('servingReleaseId', 'unknown')}\n"
                        "状态：等待启动"
                    ),
                },
                _button_group(buttons),
            ]
        },
    }


def _intervention_card(event: OperatorEvent) -> dict[str, Any]:
    question = _safe(event.payload.get("question"))
    intervention_id = _safe(event.payload.get("intervention_id"))
    choices = event.payload.get("choices")
    buttons: list[dict[str, Any]] = []
    if isinstance(choices, list):
        for choice in choices[:8]:
            if isinstance(choice, str):
                buttons.append(
                    _callback_button(
                        choice[:80],
                        {
                            "action": "intervention",
                            "request_id": event.request_id,
                            "intervention_id": intervention_id,
                            "response": choice,
                        },
                    )
                )
    elements: list[dict[str, Any]] = [{"tag": "markdown", "content": question}]
    if buttons:
        elements.append(_button_group(buttons))
    elements.append({"tag": "markdown", "content": "如需自由文本，请直接回复这条卡片。"})
    return {
        "schema": "2.0",
        "header": {"title": {"tag": "plain_text", "content": "需要人工介入"}},
        "body": {"elements": elements},
    }


def _callback_button(
    text: str,
    value: dict[str, Any],
    *,
    button_type: str = "default",
) -> dict[str, Any]:
    return {
        "tag": "button",
        "text": {"tag": "plain_text", "content": text},
        "type": button_type,
        "behaviors": [{"type": "callback", "value": value}],
    }


def _button_group(buttons: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "tag": "column_set",
        "horizontal_spacing": "8px",
        "columns": [
            {"tag": "column", "width": "auto", "elements": [button]}
            for button in buttons
        ],
    }


def _safe(value: object) -> str:
    if value is None:
        return "-"
    return str(value)[:256]


def _connection_notice(kind: str) -> Callable[[Any], Awaitable[None]]:
    async def handler(_: Any) -> None:
        logger.info("autodev Feishu connection event", extra={"event": kind})

    return handler


async def _channel_error(_: Any) -> None:
    logger.warning("autodev Feishu channel error", extra={"event": "channel_error"})
