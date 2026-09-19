from __future__ import annotations

import hashlib
import hmac
import json

import httpx
import pytest

from feishu_dify_gateway.autodev_operator import OperatorClient, OperatorRequirement


@pytest.mark.asyncio
async def test_operator_client_binds_body_and_path_to_hmac() -> None:
    observed: dict[str, str] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        body = await request.aread()
        timestamp = request.headers["X-Operator-Timestamp"]
        request_id = "request-1"
        digest = hashlib.sha256(body).hexdigest()
        canonical = f"{timestamp}\n{request_id}\nPOST\n/v1/operator/requirements\n{digest}".encode()
        observed["signature"] = request.headers["X-Operator-Signature"]
        expected = hmac.new(b"operator-secret", canonical, hashlib.sha256).hexdigest()
        assert hmac.compare_digest(observed["signature"], expected)
        assert json.loads(body)["requestId"] == request_id
        return httpx.Response(201, json={"requestId": request_id, "status": "received"})

    client = OperatorClient(
        "http://127.0.0.1:8765",
        "operator-secret",
        transport=httpx.MockTransport(handler),
    )
    try:
        response = await client.submit(
            OperatorRequirement(
                request_id="request-1",
                target_id="target-1",
                source="test",
                external_reference_digest="external-1",
                title="Requirement",
                normalized_requirement_text="Do the thing.",
                content_sha256="a" * 64,
            )
        )
    finally:
        await client.close()
    assert response["status"] == "received"
