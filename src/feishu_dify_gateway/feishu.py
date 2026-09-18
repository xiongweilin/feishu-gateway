from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import threading
import time
from collections.abc import Awaitable, Callable
from concurrent.futures import Future
from dataclasses import dataclass
from time import perf_counter
from typing import Any

import httpx
from pydantic import ValidationError

from .errors import GatewayError
from .metrics import Metrics
from .models import FeishuMessageResponse, FeishuTokenResponse

logger = logging.getLogger(__name__)
RETRYABLE_HTTP_STATUSES = {429, 500, 502, 503, 504}
ADMINISTRATIVE_INGRESS_PATH = "/v1/intake/feishu/events"


def _provider_status_code(status_code: int) -> int:
    return status_code if status_code >= 400 else 422


def is_administrative_route(text: str, prefix: str) -> bool:
    """Select the Administrative transport lane without interpreting intent."""
    normalized_prefix = prefix.strip().rstrip("/")
    normalized_text = text.strip()
    suffix = normalized_text[len(normalized_prefix) :]
    return bool(
        normalized_prefix
        and (
            normalized_text == normalized_prefix
            or (normalized_text.startswith(normalized_prefix) and suffix[:1].isspace())
        )
    )


class FeishuSender:
    def __init__(
        self,
        base_url: str,
        app_id: str,
        app_secret: str,
        metrics: Metrics,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
        max_attempts: int = 4,
        retry_base_seconds: float = 0.25,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._app_id = app_id
        self._app_secret = app_secret
        self._metrics = metrics
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(15.0), transport=transport)
        self._token = ""
        self._token_expires_at = 0.0
        self._token_lock = asyncio.Lock()
        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        self._max_attempts = max_attempts
        self._retry_base_seconds = retry_base_seconds

    async def _retry_wait(self, attempt: int, response: httpx.Response | None = None) -> None:
        delay = self._retry_base_seconds * (2**attempt)
        if response is not None:
            retry_after = response.headers.get("Retry-After")
            if retry_after is not None:
                with contextlib.suppress(ValueError):
                    delay = float(retry_after)
        await asyncio.sleep(max(0.0, min(delay, 10.0)))

    async def _access_token(self) -> str:
        if self._token and time.time() < self._token_expires_at:
            return self._token
        async with self._token_lock:
            if self._token and time.time() < self._token_expires_at:
                return self._token
            last_error: Exception | None = None
            for attempt in range(self._max_attempts):
                try:
                    response = await self._client.post(
                        f"{self._base_url}/open-apis/auth/v3/tenant_access_token/internal",
                        json={"app_id": self._app_id, "app_secret": self._app_secret},
                    )
                except httpx.RequestError as exc:
                    last_error = exc
                    if attempt + 1 < self._max_attempts:
                        await self._retry_wait(attempt)
                        continue
                    break
                if (
                    response.status_code in RETRYABLE_HTTP_STATUSES
                    and attempt + 1 < self._max_attempts
                ):
                    await self._retry_wait(attempt, response)
                    continue
                try:
                    response.raise_for_status()
                    parsed = FeishuTokenResponse.model_validate(response.json())
                except (httpx.HTTPError, ValueError, ValidationError) as exc:
                    raise GatewayError(
                        "FEISHU_AUTH_FAILED",
                        "Feishu authentication failed",
                        status_code=_provider_status_code(response.status_code),
                    ) from exc
                if parsed.code != 0 or not parsed.tenant_access_token:
                    raise GatewayError(
                        "FEISHU_AUTH_FAILED",
                        "Feishu authentication failed",
                        status_code=_provider_status_code(response.status_code),
                    )
                self._token = parsed.tenant_access_token
                self._token_expires_at = time.time() + max(60, parsed.expire - 60)
                return self._token
            raise GatewayError("FEISHU_AUTH_FAILED", "Feishu authentication failed") from last_error

    async def send_text(
        self, recipient_open_id: str, text: str, idempotency_key: str
    ) -> str | None:
        started = perf_counter()
        try:
            last_error: Exception | None = None
            for attempt in range(self._max_attempts):
                token = await self._access_token()
                try:
                    response = await self._client.post(
                        f"{self._base_url}/open-apis/im/v1/messages",
                        params={"receive_id_type": "open_id"},
                        headers={"Authorization": f"Bearer {token}"},
                        json={
                            "receive_id": recipient_open_id,
                            "msg_type": "text",
                            "content": json.dumps({"text": text}, ensure_ascii=False),
                            "uuid": idempotency_key,
                        },
                    )
                except httpx.RequestError as exc:
                    last_error = exc
                    if attempt + 1 < self._max_attempts:
                        await self._retry_wait(attempt)
                        continue
                    break
                if response.status_code == 401:
                    self._token = ""
                    self._token_expires_at = 0.0
                if (
                    response.status_code in RETRYABLE_HTTP_STATUSES
                    and attempt + 1 < self._max_attempts
                ):
                    await self._retry_wait(attempt, response)
                    continue
                try:
                    response.raise_for_status()
                    parsed = FeishuMessageResponse.model_validate(response.json())
                except (httpx.HTTPError, ValueError, ValidationError) as exc:
                    raise GatewayError(
                        "FEISHU_SEND_FAILED",
                        "Feishu message delivery failed",
                        status_code=_provider_status_code(response.status_code),
                    ) from exc
                if parsed.code != 0:
                    raise GatewayError(
                        "FEISHU_SEND_FAILED",
                        "Feishu rejected the message",
                        status_code=_provider_status_code(response.status_code),
                    )
                last_error = None
                break
            if last_error is not None and attempt + 1 == self._max_attempts:
                raise GatewayError(
                    "FEISHU_SEND_FAILED", "Feishu message delivery failed"
                ) from last_error
        except GatewayError:
            self._metrics.external_requests.labels("feishu", "error").inc()
            raise
        finally:
            self._metrics.external_duration.labels("feishu").observe(perf_counter() - started)
        self._metrics.external_requests.labels("feishu", "success").inc()
        if parsed.data is None:
            return None
        message_id = parsed.data.get("message_id")
        return message_id if isinstance(message_id, str) and message_id else None

    async def ready(self) -> bool:
        try:
            await self._access_token()
            return True
        except GatewayError:
            return False

    async def close(self) -> None:
        await self._client.aclose()


@dataclass(frozen=True, slots=True)
class FeishuEventMetadata:
    """Body-free metadata extracted from one trusted long-connection event."""

    event_id: str
    event_type: str
    tenant_key: str
    message_id: str
    root_id: str | None
    parent_id: str | None
    thread_id: str | None
    sender_open_id: str
    create_time: str
    verification_token: str | None
    message_type: str

    def provider_envelope(self) -> dict[str, object]:
        """Rebuild only the metadata accepted by the administrative ingress."""
        header: dict[str, object] = {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "tenant_key": self.tenant_key,
            "create_time": self.create_time,
        }
        if self.verification_token:
            header["token"] = self.verification_token

        message: dict[str, object] = {
            "message_id": self.message_id,
            "create_time": self.create_time,
            "message_type": self.message_type,
        }
        # The administrative adapter currently derives thread_ref from
        # root_id/parent_id/message_id. Preserve SDK thread_id when root_id is
        # absent without adding a new cross-repository contract.
        if self.root_id or self.thread_id:
            message["root_id"] = self.root_id or self.thread_id
        if self.parent_id:
            message["parent_id"] = self.parent_id
        return {
            "schema": "2.0",
            "header": header,
            "event": {
                "sender": {"sender_id": {"open_id": self.sender_open_id}},
                "message": message,
            },
        }


def _nonblank_text(value: object) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _provider_time(value: object) -> str | None:
    if isinstance(value, int) and value > 0:
        return str(value)
    text = _nonblank_text(value)
    if text is None or not text.isascii() or not text.isdigit() or int(text) < 1:
        return None
    return text


def extract_feishu_event_metadata(data: Any) -> FeishuEventMetadata | None:
    """Extract the fields needed by the durable ingress without reading content."""
    header = getattr(data, "header", None)
    event = getattr(data, "event", None)
    message = getattr(event, "message", None)
    sender = getattr(event, "sender", None)
    sender_id = getattr(sender, "sender_id", None)
    if header is None or message is None or sender_id is None:
        return None

    event_id = _nonblank_text(getattr(header, "event_id", None))
    message_id = _nonblank_text(getattr(message, "message_id", None))
    tenant_key = _nonblank_text(getattr(header, "tenant_key", None)) or _nonblank_text(
        getattr(sender, "tenant_key", None)
    )
    sender_open_id = _nonblank_text(getattr(sender_id, "open_id", None))
    create_time = _provider_time(
        getattr(message, "create_time", None) or getattr(header, "create_time", None)
    )
    if not event_id or not message_id or not tenant_key or not sender_open_id or not create_time:
        return None

    return FeishuEventMetadata(
        event_id=event_id,
        event_type=_nonblank_text(getattr(header, "event_type", None)) or "im.message.receive_v1",
        tenant_key=tenant_key,
        message_id=message_id,
        root_id=_nonblank_text(getattr(message, "root_id", None)),
        parent_id=_nonblank_text(getattr(message, "parent_id", None)),
        thread_id=_nonblank_text(getattr(message, "thread_id", None)),
        sender_open_id=sender_open_id,
        create_time=create_time,
        verification_token=_nonblank_text(getattr(header, "token", None)),
        message_type=_nonblank_text(getattr(message, "message_type", None)) or "text",
    )


class AdministrativeIngressClient:
    """Bounded, metadata-only handoff to the Administrative durable ingress."""

    def __init__(
        self,
        base_url: str,
        *,
        shared_secret: str = "",
        transport: httpx.AsyncBaseTransport | None = None,
        max_attempts: int = 3,
        retry_base_seconds: float = 0.25,
    ) -> None:
        if not base_url.strip():
            raise ValueError("base_url must not be blank")
        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        self._base_url = base_url.rstrip("/")
        self._shared_secret = shared_secret.strip()
        self._max_attempts = max_attempts
        self._retry_base_seconds = retry_base_seconds
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(10.0), transport=transport)

    async def _retry_wait(self, attempt: int, response: httpx.Response | None = None) -> None:
        delay = self._retry_base_seconds * (2**attempt)
        if response is not None:
            retry_after = response.headers.get("Retry-After")
            if retry_after is not None:
                with contextlib.suppress(ValueError):
                    delay = float(retry_after)
        await asyncio.sleep(max(0.0, min(delay, 10.0)))

    async def send_metadata(self, metadata: FeishuEventMetadata) -> None:
        # The long-connection event may not carry the HTTP callback token. The
        # internal handoff therefore uses a dedicated transport credential;
        # never send an unauthenticated reconstructed event.
        if not self._shared_secret:
            raise GatewayError(
                "ADMIN_INGRESS_AUTH_UNAVAILABLE",
                "Administrative ingress transport authentication is unavailable",
                503,
            )

        payload = metadata.provider_envelope()
        last_error: Exception | None = None
        for attempt in range(self._max_attempts):
            try:
                response = await self._client.post(
                    f"{self._base_url}{ADMINISTRATIVE_INGRESS_PATH}",
                    json=payload,
                    headers={
                        "Accept": "application/json",
                        "X-Administrative-Ingress-Token": self._shared_secret,
                    },
                )
            except httpx.RequestError as exc:
                last_error = exc
                if attempt + 1 < self._max_attempts:
                    await self._retry_wait(attempt)
                    continue
                break

            if response.status_code == 202:
                return
            if response.status_code in RETRYABLE_HTTP_STATUSES and attempt + 1 < self._max_attempts:
                await self._retry_wait(attempt, response)
                continue
            raise GatewayError(
                "ADMIN_INGRESS_REJECTED",
                "Administrative ingress rejected the event",
                response.status_code if response.status_code >= 400 else 502,
            )

        raise GatewayError(
            "ADMIN_INGRESS_UNAVAILABLE",
            "Administrative ingress is unavailable",
            503,
        ) from last_error

    async def close(self) -> None:
        await self._client.aclose()


class FeishuLongConnection:
    """Runs the official SDK long connection in a daemon thread."""

    def __init__(
        self,
        app_id: str,
        app_secret: str,
        handler: Callable[[str, str, str], Awaitable[None]],
        metrics: Metrics,
        metadata_handler: Callable[[FeishuEventMetadata], Awaitable[None]] | None = None,
        administrative_route_prefix: str = "",
    ) -> None:
        self._app_id = app_id
        self._app_secret = app_secret
        self._handler = handler
        self._metrics = metrics
        self._metadata_handler = metadata_handler
        self._administrative_route_prefix = administrative_route_prefix.strip().rstrip("/")
        self._thread: threading.Thread | None = None
        self._running = threading.Event()

    @property
    def running(self) -> bool:
        return self._running.is_set()

    def start(self, loop: asyncio.AbstractEventLoop) -> None:
        if self._thread is not None:
            return

        def run() -> None:
            try:
                import lark_oapi as lark  # type: ignore[import-untyped]
                from lark_oapi.api.im.v1 import (  # type: ignore[import-untyped]
                    P2ImMessageReceiveV1,
                )

                def on_message(data: P2ImMessageReceiveV1) -> None:
                    try:
                        event = data.event
                        if event is None or event.message is None or event.sender is None:
                            return
                        sender_id = event.sender.sender_id
                        if sender_id is None or not sender_id.open_id:
                            return
                        metadata = extract_feishu_event_metadata(data)
                        event_id = event.message.message_id or _nonblank_text(
                            getattr(getattr(data, "header", None), "event_id", None)
                        )
                        if not isinstance(event_id, str) or not event_id:
                            return
                        text: str | None = None
                        if event.message.message_type == "text":
                            try:
                                content = json.loads(event.message.content or "{}")
                            except (TypeError, ValueError):
                                logger.warning(
                                    "feishu text event content rejected",
                                    extra={
                                        "event": "feishu_event_rejected",
                                        "error_code": "INVALID_TEXT_CONTENT",
                                    },
                                )
                            else:
                                candidate_text = content.get("text")
                                if isinstance(candidate_text, str) and candidate_text.strip():
                                    text = candidate_text.strip()

                        async def dispatch() -> None:
                            # Exclusive transport routing: an event enters at
                            # most one execution system, selected by transport
                            # facts only. The gateway never interprets
                            # administrative intent here.
                            if text is None:
                                # Non-text events never reach the control
                                # plane; with ingress enabled they cross the
                                # boundary as metadata only.
                                if self._metadata_handler is not None and metadata is not None:
                                    await self._safe_metadata_dispatch(metadata)
                                return
                            if is_administrative_route(text, self._administrative_route_prefix):
                                if self._metadata_handler is not None and metadata is not None:
                                    await self._safe_metadata_dispatch(metadata)
                                return
                            await self._handler(event_id, sender_id.open_id, text)

                        if (not isinstance(text, str) or not text.strip()) and (
                            self._metadata_handler is None or metadata is None
                        ):
                            return

                        future = asyncio.run_coroutine_threadsafe(dispatch(), loop)

                        def completed(result: Future[None]) -> None:
                            try:
                                result.result()
                            except Exception:
                                logger.exception(
                                    "feishu event processing failed",
                                    extra={
                                        "event": "feishu_event_processing_failed",
                                        "error_code": "EVENT_PROCESSING_FAILED",
                                    },
                                )

                        future.add_done_callback(completed)
                    except (AttributeError, TypeError, ValueError, json.JSONDecodeError):
                        logger.warning(
                            "feishu event rejected",
                            extra={"event": "feishu_event_rejected", "error_code": "INVALID_EVENT"},
                        )

                dispatcher = (
                    lark.EventDispatcherHandler.builder("", "")
                    .register_p2_im_message_receive_v1(on_message)
                    .build()
                )
                client = lark.ws.Client(
                    self._app_id,
                    self._app_secret,
                    event_handler=dispatcher,
                    log_level=lark.LogLevel.WARNING,
                )
                self._metrics.long_connection_up.set(1)
                self._running.set()
                client.start()
            except Exception:
                self._running.clear()
                self._metrics.long_connection_up.set(0)
                logger.exception(
                    "feishu long connection stopped",
                    extra={
                        "event": "feishu_long_connection_stopped",
                        "error_code": "WS_STOPPED",
                    },
                )

        self._thread = threading.Thread(target=run, name="feishu-long-connection", daemon=True)
        self._thread.start()

    async def _safe_metadata_dispatch(self, metadata: FeishuEventMetadata) -> None:
        handler = self._metadata_handler
        if handler is None:
            return
        try:
            await handler(metadata)
        except Exception:
            logger.error(
                "feishu administrative metadata handoff failed",
                extra={
                    "event": "feishu_administrative_metadata_handoff_failed",
                    "error_code": "ADMIN_INGRESS_HANDOFF_FAILED",
                },
            )
