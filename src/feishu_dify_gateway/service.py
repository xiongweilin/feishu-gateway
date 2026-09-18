from __future__ import annotations

import hashlib
import hmac
import logging
import time
import uuid
from collections.abc import Awaitable
from typing import Literal, Protocol, cast

import httpx

from .config import Settings
from .errors import GatewayError
from .feishu import FeishuSender
from .metrics import Metrics
from .models import (
    AcceptedResponse,
    AdministrativeCommunicationAcceptedResponse,
    AdministrativeCommunicationRequest,
    AdministrativeCommunicationStatus,
    Alert,
    AlertmanagerPayload,
    Notification,
    NotificationAcceptedResponse,
    SyntheticPrepareResponse,
    SyntheticProbeResponse,
)
from .prometheus import PrometheusClient
from .security import pseudonymous_user
from .store import AdministrativeCommunicationLedgerEntry, DeliveryLedgerEntry, StateStore

logger = logging.getLogger(__name__)
FEISHU_DELIVERY_NAMESPACE = uuid.UUID("589e9076-153c-40de-8751-b2469ea1af41")
RETRYABLE_DELIVERY_STATUS_CODES = frozenset({408, 425, 429, 500, 502, 503, 504})


class ControlPlaneClient:
    """Minimal signed-adjacent client for the control plane API (shared key header)."""

    def __init__(self, base_url: str, api_key: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self._http = httpx.AsyncClient(timeout=20)

    async def request(self, method: str, path: str, body: dict[str, object] | None = None) -> str:
        headers = {
            "X-Control-Plane-Key": self.api_key,
            "Accept": "application/json",
        }
        try:
            response = await self._http.request(
                method, f"{self.base_url}{path}", json=body, headers=headers
            )
        except httpx.HTTPError as exc:
            return f"控制平面不可达：{exc}"
        if response.status_code >= 400:
            return f"控制平面错误 HTTP {response.status_code}: {response.text[:500]}"
        try:
            data = response.json()
        except ValueError:
            return response.text[:1_000]
        if isinstance(data, dict) and data.get("accepted") is False:
            return f"控制平面拒绝：{data.get('message', '')}"
        if isinstance(data, dict) and data.get("message"):
            return f"控制平面：{data['message']}"
        return str(data)[:1_000]

    async def close(self) -> None:
        await self._http.aclose()


class Sender(Protocol):
    async def send_text(
        self, recipient_open_id: str, text: str, idempotency_key: str
    ) -> str | None: ...

    async def ready(self) -> bool: ...

    async def close(self) -> None: ...


class MonitorClient(Protocol):
    async def ready(self) -> bool: ...

    async def active_alerts(self) -> list[tuple[str, str]]: ...

    async def close(self) -> None: ...


def split_text(text: str, limit: int = 3_500) -> list[str]:
    stripped = text.strip()
    if not stripped:
        return []
    chunks: list[str] = []
    remaining = stripped
    while len(remaining) > limit:
        split_at = max(remaining.rfind("\n", 0, limit), remaining.rfind(" ", 0, limit))
        if split_at < limit // 2:
            split_at = limit
        chunks.append(remaining[:split_at].rstrip())
        remaining = remaining[split_at:].lstrip()
    if remaining:
        chunks.append(remaining)
    return chunks


class GatewayService:
    def __init__(
        self,
        settings: Settings,
        store: StateStore,
        metrics: Metrics,
        sender: Sender,
        prometheus: MonitorClient,
        control_plane: ControlPlaneClient | None = None,
    ) -> None:
        self.settings = settings
        self.store = store
        self.metrics = metrics
        self.sender = sender
        self.prometheus = prometheus
        self.control_plane = control_plane or ControlPlaneClient(
            settings.control_plane_base_url,
            settings.control_plane_key,
        )

    @classmethod
    def build(cls, settings: Settings, store: StateStore, metrics: Metrics) -> GatewayService:
        return cls(
            settings,
            store,
            metrics,
            FeishuSender(
                settings.feishu_base_url,
                settings.feishu_app_id,
                settings.feishu_app_secret,
                metrics,
            ),
            PrometheusClient(settings.prometheus_base_url),
        )

    async def _send_chunks(self, recipient: str, text: str, delivery_key: str) -> None:
        chunks = split_text(text)
        if not chunks:
            raise GatewayError("EMPTY_MESSAGE", "Message is empty", 422)
        for index, chunk in enumerate(chunks):
            idempotency_key = str(uuid.uuid5(FEISHU_DELIVERY_NAMESPACE, f"{delivery_key}:{index}"))
            await self.sender.send_text(recipient, chunk, idempotency_key)

    async def deliver_notification(
        self, event_id: str, notification: Notification
    ) -> NotificationAcceptedResponse:
        key = f"notification:{event_id}"
        if not self.store.claim_event(key):
            entry = self.store.delivery_for(event_id)
            self.metrics.deliveries.labels(notification.source, "deduplicated").inc()
            self.metrics.delivery_states.labels(notification.source, "deduplicated").inc()
            status = cast(
                Literal[
                    "deduplicated",
                    "delivering",
                    "retrying",
                    "permanent_failed",
                    "delivery_confirmed",
                ],
                (
                    entry.status
                    if entry is not None
                    and entry.status
                    in {"delivering", "retrying", "permanent_failed", "delivery_confirmed"}
                    else "deduplicated"
                ),
            )
            return NotificationAcceptedResponse(
                accepted=0,
                deduplicated=1,
                event_id=event_id,
                status=status,
                transportAccepted=entry.transport_accepted if entry else False,
                deliveryConfirmed=entry.delivery_confirmed if entry else False,
            )
        self.store.begin_delivery(event_id, notification.source)
        self.metrics.delivery_states.labels(notification.source, "transport_accepted").inc()
        self.metrics.delivery_states.labels(notification.source, "delivering").inc()
        self.metrics.delivery_attempts.labels(notification.source, "attempt").inc()
        message = f"[{notification.severity.upper()}] {notification.title}\n{notification.text}"
        if notification.url is not None:
            message += f"\n{notification.url}"
        try:
            await self._send_chunks(self.settings.feishu_alert_recipient_open_id, message, key)
        except Exception as exc:
            error_code, retryable = self._delivery_failure(exc)
            if retryable:
                self.store.mark_delivery_retrying(event_id, error_code, int(time.time()))
                self.metrics.delivery_states.labels(notification.source, "retrying").inc()
                self.metrics.delivery_attempts.labels(
                    notification.source, "retryable_failure"
                ).inc()
                self.store.release_event(key)
            else:
                self.store.mark_delivery_permanent_failed(event_id, error_code)
                self.store.mark_processed(key, status="permanent_failed")
                self.metrics.delivery_states.labels(notification.source, "permanent_failed").inc()
                self.metrics.delivery_attempts.labels(
                    notification.source, "permanent_failure"
                ).inc()
            self.metrics.deliveries.labels(notification.source, "error").inc()
            raise
        self.store.mark_delivery_confirmed(event_id)
        self.store.mark_processed(key)
        self.metrics.deliveries.labels(notification.source, "success").inc()
        self.metrics.delivery_states.labels(notification.source, "delivery_confirmed").inc()
        self.metrics.delivery_attempts.labels(notification.source, "confirmed").inc()
        self.metrics.last_success.labels("notification").set(time.time())
        logger.info(
            "notification delivered",
            extra={
                "event": "notification_delivered",
                "source": notification.source,
                "result": "success",
            },
        )
        return NotificationAcceptedResponse(
            accepted=1,
            deduplicated=0,
            event_id=event_id,
            status="delivery_confirmed",
            transportAccepted=True,
            deliveryConfirmed=True,
        )

    async def send_administrative_communication(
        self,
        event_id: str,
        communication: AdministrativeCommunicationRequest,
        *,
        body_digest: str,
    ) -> AdministrativeCommunicationAcceptedResponse:
        """Send one governed internal message without retaining its body.

        A replay with the same event and digest returns the recorded outcome.
        A transport outcome that cannot be classified is terminal for this
        event; callers must reconcile the ledger rather than send a duplicate.
        """

        recipient_digest = hashlib.sha256(
            communication.recipient_open_id.encode("utf-8")
        ).hexdigest()
        existing = self.store.administrative_communication_for(event_id)
        if existing is not None:
            if existing.body_digest != body_digest or existing.recipient_digest != recipient_digest:
                raise GatewayError(
                    "EVENT_CONFLICT",
                    "Communication event identity was reused with different content",
                    409,
                )
            self.metrics.administrative_communications.labels("deduplicated").inc()
            return AdministrativeCommunicationAcceptedResponse(
                eventId=event_id,
                status=cast(AdministrativeCommunicationStatus, existing.status),
                transportAccepted=existing.transport_accepted,
                deliveryConfirmed=existing.delivery_confirmed,
                providerMessageRef=existing.provider_message_ref,
            )

        if not self.store.begin_administrative_communication(
            event_id, body_digest, recipient_digest
        ):
            replay = self.store.administrative_communication_for(event_id)
            if replay is None:
                raise GatewayError("LEDGER_UNAVAILABLE", "Communication ledger unavailable", 503)
            if replay.body_digest != body_digest or replay.recipient_digest != recipient_digest:
                raise GatewayError(
                    "EVENT_CONFLICT",
                    "Communication event identity was reused with different content",
                    409,
                )
            return AdministrativeCommunicationAcceptedResponse(
                eventId=event_id,
                status=cast(AdministrativeCommunicationStatus, replay.status),
                transportAccepted=replay.transport_accepted,
                deliveryConfirmed=replay.delivery_confirmed,
                providerMessageRef=replay.provider_message_ref,
            )

        self.metrics.administrative_communication_states.labels("prepared").inc()
        self.metrics.administrative_communication_attempts.labels("attempt").inc()
        idempotency_key = str(uuid.uuid5(FEISHU_DELIVERY_NAMESPACE, f"administrative:{event_id}"))
        try:
            provider_ref = await self.sender.send_text(
                communication.recipient_open_id,
                communication.text,
                idempotency_key,
            )
        except Exception as exc:
            error_code, _ = self._delivery_failure(exc)
            self.store.mark_administrative_communication_unknown(event_id, error_code)
            self.metrics.administrative_communication_states.labels("outcome_unknown").inc()
            raise GatewayError(
                "COMMUNICATION_OUTCOME_UNKNOWN",
                "Communication transport outcome is unknown; reconcile before retrying",
                503,
            ) from exc
        self.store.mark_administrative_communication_transport_accepted(event_id, provider_ref)
        self.metrics.administrative_communication_states.labels("transport_accepted").inc()
        self.metrics.administrative_communication_attempts.labels("accepted").inc()
        self.metrics.last_success.labels("administrative_communication").set(time.time())
        return AdministrativeCommunicationAcceptedResponse(
            eventId=event_id,
            status="transport_accepted",
            transportAccepted=True,
            deliveryConfirmed=False,
            providerMessageRef=provider_ref,
        )

    async def deliver_alerts(self, payload: AlertmanagerPayload) -> AcceptedResponse:
        accepted = 0
        deduplicated = 0
        for alert in payload.alerts:
            key = self._alert_key(alert)
            if not self.store.claim_event(key):
                deduplicated += 1
                self.metrics.deliveries.labels("alertmanager", "deduplicated").inc()
                self.metrics.delivery_states.labels("alertmanager", "deduplicated").inc()
                continue
            self.store.begin_delivery(key, "alertmanager")
            self.metrics.delivery_states.labels("alertmanager", "transport_accepted").inc()
            self.metrics.delivery_states.labels("alertmanager", "delivering").inc()
            self.metrics.delivery_attempts.labels("alertmanager", "attempt").inc()
            try:
                await self._send_chunks(
                    self.settings.feishu_alert_recipient_open_id,
                    self._format_alert(alert),
                    key,
                )
            except Exception as exc:
                error_code, retryable = self._delivery_failure(exc)
                if retryable:
                    self.store.mark_delivery_retrying(key, error_code, int(time.time()))
                    self.metrics.delivery_states.labels("alertmanager", "retrying").inc()
                    self.metrics.delivery_attempts.labels("alertmanager", "retryable_failure").inc()
                    self.store.release_event(key)
                else:
                    self.store.mark_delivery_permanent_failed(key, error_code)
                    self.store.mark_processed(key, status="permanent_failed")
                    self.metrics.delivery_states.labels("alertmanager", "permanent_failed").inc()
                    self.metrics.delivery_attempts.labels("alertmanager", "permanent_failure").inc()
                self.metrics.deliveries.labels("alertmanager", "error").inc()
                raise
            self.store.mark_delivery_confirmed(key)
            self.store.mark_processed(key)
            accepted += 1
            self.metrics.deliveries.labels("alertmanager", "success").inc()
            self.metrics.delivery_states.labels("alertmanager", "delivery_confirmed").inc()
            self.metrics.delivery_attempts.labels("alertmanager", "confirmed").inc()
        if accepted:
            self.metrics.last_success.labels("alertmanager").set(time.time())
        return AcceptedResponse(accepted=accepted, deduplicated=deduplicated)

    @staticmethod
    def _delivery_failure(exc: Exception) -> tuple[str, bool]:
        if isinstance(exc, GatewayError):
            return exc.code, exc.status_code in RETRYABLE_DELIVERY_STATUS_CODES
        return "UNEXPECTED_DELIVERY_ERROR", True

    def prepare_synthetic_notification(self) -> SyntheticPrepareResponse:
        event_id = f"synthetic-{uuid.uuid4().hex}"
        entry = self.store.prepare_synthetic_delivery(event_id)
        self.metrics.synthetic_prepares.labels("prepared").inc()
        self.metrics.delivery_states.labels("test", "prepared").inc()
        logger.info(
            "synthetic notification prepared",
            extra={"event": "synthetic_notification_prepared", "source": "test"},
        )
        return SyntheticPrepareResponse(
            eventId=entry.event_id,
            status="prepared",
            transportAccepted=entry.transport_accepted,
            deliveryConfirmed=entry.delivery_confirmed,
            externalSendStarted=False,
            requiresManualConfirmation=True,
        )

    def probe_synthetic_notification(self) -> SyntheticProbeResponse:
        self.metrics.synthetic_probes.labels("ready").inc()
        return SyntheticProbeResponse(
            status="ready",
            syntheticEnabled=True,
            externalSendStarted=False,
            requiresManualConfirmation=True,
        )

    def delivery_ledger(self, event_id: str) -> DeliveryLedgerEntry | None:
        return self.store.delivery_for(event_id)

    def administrative_communication_ledger(
        self, event_id: str
    ) -> AdministrativeCommunicationLedgerEntry | None:
        return self.store.administrative_communication_for(event_id)

    @staticmethod
    def _alert_key(alert: Alert) -> str:
        ending = "" if alert.ends_at is None else alert.ends_at.isoformat()
        material = f"{alert.fingerprint}|{alert.status}|{alert.starts_at.isoformat()}|{ending}"
        return "alert:" + hashlib.sha256(material.encode()).hexdigest()

    @staticmethod
    def _format_alert(alert: Alert) -> str:
        name = alert.labels.get("alertname", "UnnamedAlert")[:128]
        severity = alert.labels.get("severity", "unknown")[:32]
        summary = alert.annotations.get("summary", "")[:1_024]
        description = alert.annotations.get("description", "")[:2_048]
        status = "FIRING" if alert.status == "firing" else "RESOLVED"
        parts = [f"[{status}] {name}", f"severity: {severity}"]
        if summary:
            parts.append(summary)
        if description:
            parts.append(description)
        return "\n".join(parts)

    async def handle_feishu_text(self, event_id: str, open_id: str, text: str) -> None:
        if not hmac.compare_digest(open_id, self.settings.feishu_allowed_open_id):
            self.metrics.deliveries.labels("feishu-chat", "forbidden").inc()
            logger.warning(
                "unauthorized feishu sender",
                extra={
                    "event": "feishu_sender_rejected",
                    "source": "feishu-chat",
                    "result": "forbidden",
                },
            )
            return
        key = "feishu:" + hashlib.sha256(event_id.encode()).hexdigest()
        if not self.store.claim_event(key):
            self.metrics.deliveries.labels("feishu-chat", "deduplicated").inc()
            return
        user_hash = pseudonymous_user(self.settings.user_hmac_key, open_id)
        try:
            reply = await self._command_or_chat(text, user_hash)
            await self._send_chunks(open_id, reply, key)
        except Exception:
            self.store.release_event(key)
            self.metrics.deliveries.labels("feishu-chat", "error").inc()
            raise
        self.store.mark_processed(key)
        self.metrics.deliveries.labels("feishu-chat", "success").inc()
        self.metrics.last_success.labels("chat").set(time.time())

    async def _command_or_chat(self, text: str, user_hash: str) -> str:
        command = text.strip().lower()
        if command == "/help":
            return (
                "可用命令：\n"
                "/help 查看帮助\n"
                "/status 查看只读健康状态\n"
                "/alerts 查看当前告警\n"
                "任意消息：派发任务给 Agent(Codex) 执行\n"
                "/task <描述> 与任意消息等价\n"
                "/cp status 控制平面状态\n"
                "/cp approve <id> 批准修复\n"
                "/cp reject <id> 拒绝修复\n"
                "/cp rollback <id> 回滚修复\n"
                "/cp pause 暂停控制平面\n"
                "/cp resume 恢复控制平面\n"
                "/cp promote <candidate_id> 晋升候选经验\n"
                "/cp policy <fingerprint> auto|manual|ignore 设置告警策略\n"
                "/cp run <fingerprint> 手动策略下执行修复\n"
                "/cp ignore <fingerprint> 忽略该告警\n"
                "/cp evidence 查看沉淀证据与候选\n"
                "/cp dismiss <candidate_id> 归档候选"
            )
        if command == "/status":
            feishu_ok, prometheus_ok = await self.readiness()
            return (
                "只读状态：\n"
                f"Feishu: {'ok' if feishu_ok else 'down'}\n"
                f"Prometheus: {'ok' if prometheus_ok else 'down'}"
            )
        if command == "/alerts":
            alerts = await self.prometheus.active_alerts()
            if not alerts:
                return "当前没有可见的 active alert。"
            lines = [f"- {name}: {state}" for name, state in alerts]
            return "当前告警：\n" + "\n".join(lines)
        if command.startswith("/cp "):
            return await self._control_plane_command(text[4:].strip())
        if command.startswith("/task "):
            text = text[6:].strip()
        if not text.strip():
            return "请描述你要派发的任务。"
        return await self.control_plane.request(
            "POST",
            "/v1/tasks",
            {"prompt": text.strip(), "repo": "", "project": ""},
        )

    async def _control_plane_command(self, text: str) -> str:
        parts = text.split(maxsplit=1)
        action = parts[0].lower() if parts else ""
        arg = parts[1].strip() if len(parts) > 1 else ""
        if action == "status":
            return await self.control_plane.request("GET", "/status")
        if action in {"approve", "reject", "rollback"} and arg:
            return await self.control_plane.request(
                "POST",
                f"/v1/approvals/{arg}/decision",
                {"action": action, "decided_by": "feishu", "note": ""},
            )
        if action in {"pause", "resume"}:
            return await self.control_plane.request(
                "POST",
                f"/v1/control/{action}",
                {"reason": "feishu"},
            )
        if action == "promote" and arg:
            return await self.control_plane.request(
                "POST",
                f"/v1/candidates/{arg}/promote",
                {"decided_by": "feishu", "note": ""},
            )
        if action == "policy" and arg:
            parts = arg.split(maxsplit=1)
            fingerprint = parts[0].strip()
            policy = parts[1].strip().lower() if len(parts) > 1 else ""
            if policy not in {"auto", "manual", "ignore"}:
                return "用法：/cp policy <fingerprint> auto|manual|ignore"
            return await self.control_plane.request(
                "POST",
                f"/v1/alerts/{fingerprint}/policy",
                {"policy": policy, "note": "feishu"},
            )
        if action == "run" and arg:
            return await self.control_plane.request(
                "POST",
                f"/v1/alerts/{arg}/run",
                {},
            )
        if action == "ignore" and arg:
            return await self.control_plane.request(
                "POST",
                f"/v1/alerts/{arg}/policy",
                {"policy": "ignore", "note": "feishu"},
            )
        if action == "evidence":
            return await self.control_plane.request("GET", "/v1/evidence")
        if action == "dismiss" and arg:
            return await self.control_plane.request(
                "POST",
                f"/v1/candidates/{arg}/dismiss",
                {"decided_by": "feishu", "note": ""},
            )
        return (
            "用法：\n"
            "/cp status\n"
            "/cp approve <id> | reject <id> | rollback <id>\n"
            "/cp pause | resume\n"
            "/cp promote <candidate_id>\n"
            "/cp policy <fingerprint> auto|manual|ignore\n"
            "/cp run <fingerprint> | ignore <fingerprint>\n"
            "/cp evidence\n"
            "/cp dismiss <candidate_id>\n"
            "/task <描述> 派发任务给 Agent 执行"
        )

    async def readiness(self) -> tuple[bool, bool]:
        feishu_check: Awaitable[bool] = self.sender.ready()
        prometheus_check: Awaitable[bool] = self.prometheus.ready()
        import asyncio

        feishu_ok, prometheus_ok = await asyncio.gather(feishu_check, prometheus_check)
        return feishu_ok, prometheus_ok

    async def core_readiness(self) -> bool:
        return await self.sender.ready()

    async def close(self) -> None:
        await self.sender.close()
        await self.prometheus.close()
        await self.control_plane.close()
        self.store.close()
