from __future__ import annotations

import json
import time

from fastapi.testclient import TestClient

from feishu_dify_gateway.app import create_app
from feishu_dify_gateway.config import Settings
from feishu_dify_gateway.security import sign_request
from feishu_dify_gateway.service import GatewayService

from .conftest import FakeControlPlane, FakePrometheus, FakeSender


def test_health_and_metrics(
    settings: Settings,
    service: tuple[GatewayService, FakeSender, FakePrometheus, FakeControlPlane],
) -> None:
    gateway, _, _, _ = service
    app = create_app(settings, service=gateway, start_long_connection=False)
    with TestClient(app) as client:
        assert client.get("/healthz").status_code == 200
        assert client.get("/readyz").status_code == 200
        metrics = client.get("/metrics")
        assert metrics.status_code == 200
        assert "feishu_gateway_http_requests_total" in metrics.text


def test_ready_does_not_depend_on_prometheus(
    settings: Settings,
    service: tuple[GatewayService, FakeSender, FakePrometheus, FakeControlPlane],
) -> None:
    gateway, _, prometheus, _ = service
    prometheus.is_ready = False
    app = create_app(settings, service=gateway, start_long_connection=False)
    with TestClient(app) as client:
        assert client.get("/readyz").status_code == 200


def test_alertmanager_route_is_hidden_on_public_listener(
    settings: Settings,
    service: tuple[GatewayService, FakeSender, FakePrometheus, FakeControlPlane],
) -> None:
    gateway, _, _, _ = service
    app = create_app(settings, service=gateway, start_long_connection=False)
    with TestClient(app, base_url=f"http://testserver:{settings.public_port}") as client:
        response = client.post("/v1/alerts/alertmanager", json={})
    assert response.status_code == 404
    assert response.json() == {"error": {"code": "NOT_FOUND", "message": "Resource not found"}}


def test_synthetic_prepare_is_internal_and_does_not_send(
    settings: Settings,
    service: tuple[GatewayService, FakeSender, FakePrometheus, FakeControlPlane],
) -> None:
    gateway, sender, _, _ = service
    app = create_app(settings, service=gateway, start_long_connection=False)
    with TestClient(app, base_url=f"http://testserver:{settings.port}") as client:
        response = client.post("/v1/notifications/synthetic/prepare")
        assert response.status_code == 201
        payload = response.json()
        assert payload["status"] == "prepared"
        assert payload["externalSendStarted"] is False
        assert payload["requiresManualConfirmation"] is True
        event_id = payload["eventId"]
        ledger = client.get(f"/v1/delivery-ledger/{event_id}")
        assert ledger.status_code == 200
        assert ledger.json()["deliveryConfirmed"] is False
        client.base_url = client.base_url.copy_with(port=settings.public_port)
        assert client.post("/v1/notifications/synthetic/prepare").status_code == 404

    assert not sender.messages


def test_synthetic_probe_is_internal_and_does_not_send(
    settings: Settings,
    service: tuple[GatewayService, FakeSender, FakePrometheus, FakeControlPlane],
) -> None:
    gateway, sender, _, _ = service
    app = create_app(settings, service=gateway, start_long_connection=False)
    with TestClient(app, base_url=f"http://testserver:{settings.port}") as client:
        response = client.get("/v1/notifications/synthetic/probe")
        assert response.status_code == 200
        assert response.json() == {
            "status": "ready",
            "syntheticEnabled": True,
            "externalSendStarted": False,
            "requiresManualConfirmation": True,
        }
        client.base_url = client.base_url.copy_with(port=settings.public_port)
        assert client.get("/v1/notifications/synthetic/probe").status_code == 404

    assert not sender.messages


def test_signed_notification_and_replay(
    settings: Settings,
    service: tuple[GatewayService, FakeSender, FakePrometheus, FakeControlPlane],
) -> None:
    gateway, sender, _, _ = service
    app = create_app(settings, service=gateway, start_long_connection=False)
    body = json.dumps(
        {
            "source": "test",
            "severity": "info",
            "title": "title",
            "text": "text",
            "occurredAt": "2026-08-02T00:00:00Z",
        },
        separators=(",", ":"),
    ).encode()
    timestamp = str(int(time.time()))
    headers = {
        "Content-Type": "application/json",
        "X-Event-ID": "event-1",
        "X-Timestamp": timestamp,
        "X-Signature": sign_request(settings.notification_hmac_key, timestamp, "event-1", body),
    }
    with TestClient(app) as client:
        assert client.post("/v1/notifications", content=body, headers=headers).status_code == 202
        replay = client.post("/v1/notifications", content=body, headers=headers)
        assert replay.status_code == 202
        assert replay.json()["deduplicated"] == 1
    assert len(sender.messages) == 1


def test_invalid_signature_is_safe(
    settings: Settings,
    service: tuple[GatewayService, FakeSender, FakePrometheus, FakeControlPlane],
) -> None:
    gateway, sender, _, _ = service
    app = create_app(settings, service=gateway, start_long_connection=False)
    with TestClient(app) as client:
        response = client.post(
            "/v1/notifications",
            content=b"{}",
            headers={
                "X-Event-ID": "event-2",
                "X-Timestamp": str(int(time.time())),
                "X-Signature": "0" * 64,
            },
        )
    assert response.status_code == 401
    assert response.json() == {
        "error": {"code": "AUTHENTICATION_FAILED", "message": "Request authentication failed"}
    }
    assert not sender.messages
