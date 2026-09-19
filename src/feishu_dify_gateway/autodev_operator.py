from __future__ import annotations

import hashlib
import hmac
import json
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode

import httpx


class OperatorApiError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


@dataclass(frozen=True, slots=True)
class OperatorRequirement:
    request_id: str
    target_id: str
    source: str
    external_reference_digest: str
    title: str
    normalized_requirement_text: str
    content_sha256: str


@dataclass(frozen=True, slots=True)
class OperatorEvent:
    event_id: str
    sequence: int
    request_id: str | None
    cycle_id: str | None
    event_type: str
    payload: dict[str, Any]


class OperatorClient:
    def __init__(
        self,
        base_url: str,
        secret: str,
        *,
        timeout_seconds: float = 15.0,
        max_attempts: int = 4,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        if not base_url.startswith("http://127.0.0.1:"):
            raise ValueError("operator client must use loopback HTTP")
        if not secret.strip():
            raise ValueError("operator HMAC secret must be configured")
        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        self._base_url = base_url.rstrip("/")
        self._secret = secret
        self._client = httpx.AsyncClient(timeout=timeout_seconds, transport=transport)
        self._max_attempts = max_attempts

    async def close(self) -> None:
        await self._client.aclose()

    async def submit(self, requirement: OperatorRequirement) -> dict[str, Any]:
        body = {
            "requestId": requirement.request_id,
            "targetId": requirement.target_id,
            "source": requirement.source,
            "externalReferenceDigest": requirement.external_reference_digest,
            "title": requirement.title,
            "normalizedRequirementText": requirement.normalized_requirement_text,
            "contentSha256": requirement.content_sha256,
        }
        return await self._json_request(
            "POST",
            "/v1/operator/requirements",
            requirement.request_id,
            body,
        )

    async def status(self, request_id: str) -> dict[str, Any]:
        return await self._json_request(
            "GET", f"/v1/operator/requirements/{request_id}", request_id, None
        )

    async def start(self, request_id: str) -> dict[str, Any]:
        return await self._json_request(
            "POST", f"/v1/operator/requirements/{request_id}/start", request_id, None
        )

    async def cancel(self, request_id: str) -> dict[str, Any]:
        return await self._json_request(
            "POST", f"/v1/operator/requirements/{request_id}/cancel", request_id, None
        )

    async def respond(self, intervention_id: str, response: str) -> dict[str, Any]:
        return await self._json_request(
            "POST",
            f"/v1/operator/interventions/{intervention_id}/responses",
            intervention_id,
            {"response": response},
        )

    async def events(self, after: int, limit: int = 50) -> tuple[OperatorEvent, ...]:
        query = urlencode({"after": after, "limit": limit})
        payload = await self._json_request(
            "GET", f"/v1/operator/events?{query}", f"events:{after}:{limit}", None
        )
        values = payload.get("events")
        if not isinstance(values, list):
            raise OperatorApiError("operator API returned an invalid event list")
        events: list[OperatorEvent] = []
        for value in values:
            if not isinstance(value, dict):
                raise OperatorApiError("operator API returned an invalid event")
            sequence = value.get("sequence")
            event_id = value.get("eventId")
            event_type = value.get("eventType")
            if (
                not isinstance(sequence, int)
                or not isinstance(event_id, str)
                or not isinstance(event_type, str)
            ):
                raise OperatorApiError("operator API returned an invalid event identity")
            payload_value = value.get("payload")
            if not isinstance(payload_value, dict):
                raise OperatorApiError("operator API returned an invalid event payload")
            request_id = value.get("requestId")
            cycle_id = value.get("cycleId")
            events.append(
                OperatorEvent(
                    event_id=event_id,
                    sequence=sequence,
                    request_id=request_id if isinstance(request_id, str) else None,
                    cycle_id=cycle_id if isinstance(cycle_id, str) else None,
                    event_type=event_type,
                    payload=dict(payload_value),
                )
            )
        return tuple(events)

    async def acknowledge(self, event_id: str) -> dict[str, Any]:
        return await self._json_request(
            "POST", f"/v1/operator/events/{event_id}/ack", event_id, None
        )

    async def _json_request(
        self,
        method: str,
        path_with_query: str,
        request_id: str,
        body: dict[str, Any] | None,
    ) -> dict[str, Any]:
        body_bytes = (
            json.dumps(body, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
            if body is not None
            else b""
        )
        timestamp = str(int(time.time()))
        path, _, query = path_with_query.partition("?")
        signed_path = f"{path}?{query}" if query else path
        digest = hashlib.sha256(body_bytes).hexdigest()
        canonical = f"{timestamp}\n{request_id}\n{method}\n{signed_path}\n{digest}".encode()
        signature = hmac.new(self._secret.encode(), canonical, hashlib.sha256).hexdigest()
        headers = {
            "X-Operator-Timestamp": timestamp,
            "X-Operator-Signature": signature,
            "Content-Type": "application/json",
        }
        last_error: Exception | None = None
        for attempt in range(self._max_attempts):
            try:
                response = await self._client.request(
                    method,
                    f"{self._base_url}{path_with_query}",
                    content=body_bytes,
                    headers=headers,
                )
            except httpx.RequestError as exc:
                last_error = exc
                if attempt + 1 < self._max_attempts:
                    await _backoff(attempt)
                    continue
                raise OperatorApiError("operator API transport is unavailable") from exc
            if response.status_code in {502, 503, 504} and attempt + 1 < self._max_attempts:
                await _backoff(attempt)
                continue
            if response.status_code >= 400:
                raise OperatorApiError(
                    "operator API rejected the request",
                    status_code=response.status_code,
                )
            try:
                payload = response.json()
            except ValueError as exc:
                raise OperatorApiError("operator API returned invalid JSON") from exc
            if not isinstance(payload, dict):
                raise OperatorApiError("operator API returned an invalid response")
            return payload
        raise OperatorApiError("operator API transport is unavailable") from last_error


async def _backoff(attempt: int) -> None:
    import asyncio

    await asyncio.sleep(min(0.25 * (2**attempt), 5.0))
