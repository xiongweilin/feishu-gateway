# Feishu Gateway

[![CI](https://github.com/xiongweilin/feishu-gateway/actions/workflows/ci.yml/badge.svg)](https://github.com/xiongweilin/feishu-gateway/actions/workflows/ci.yml) [![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=metratio_feishu-dify-gateway&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=metratio_feishu-dify-gateway) [![Coverage](https://sonarcloud.io/api/project_badges/measure?project=metratio_feishu-dify-gateway&metric=coverage)](https://sonarcloud.io/summary/new_code?id=metratio_feishu-dify-gateway) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE) [![Python 3.12](https://img.shields.io/badge/Python-3.12-blue.svg)](pyproject.toml)

> **The repository name is historical. Dify is no longer part of the runtime path.** Dify Chatflow dispatch was removed by ADR-002.

This project is a narrow Feishu transport, notification, and ingress-security gateway for a personal operations stack.

It does not reason about tasks, decide whether work should exist, authorize external effects, or determine whether an objective has been completed.

```text
Feishu transport / command ingress
        |
        v
control-plane / administrative ingress
        |
        v
runtime authority / execution / verification
```

The gateway owns transport. Downstream systems own semantics and authority.

## What it does

The gateway has three responsibilities:

1. Receive authenticated personal Feishu messages and forward supported requests to the appropriate downstream ingress.
2. Receive infrastructure or application notifications and deliver them to the configured Feishu private chat.
3. Expose operational health/readiness/metrics while keeping message bodies, credentials, and raw user identifiers out of durable telemetry.

## What it deliberately does not do

```text
transport accepted      != task accepted
message received        != decision made
command parsed          != effect authorized
notification delivered  != human observed
provider success        != objective verified
```

Task execution and repair governance belong to `xiongweilin/control-plane`. Administrative requests may be forwarded to a separate administrative ingress when explicitly configured. The gateway forwards requests and renders confirmed responses; it does not mint task authority or infer completion from transport success.

## Security and privacy boundary

The public design is intentionally narrow:

- interaction is limited to the configured personal Feishu identity;
- supported commands are explicit and bounded;
- notification ingress is authenticated and replay-resistant;
- downstream responses are treated as untrusted external input;
- idempotency and delivery ledgers store metadata rather than message bodies;
- credentials are mounted or supplied through deployment-owned secret mechanisms rather than committed to Git;
- synthetic notification preparation and capability probes are non-sending paths;
- transport success is recorded separately from downstream semantic completion.

The gateway does not expose unrestricted execution capability and does not hold the authority to broaden downstream effect scope.

## Request paths

Normal personal task messages are forwarded to the control-plane task surface.

A separately configured administrative route can forward a reconstructed metadata envelope to the administrative ingress. The gateway does not interpret the administrative request as domain truth and does not dispatch the same event through the normal control-plane task path.

Notifications use a metadata-only delivery ledger. States describe transport progress only; they do not assert that a human read a message or that the underlying operational objective was completed.

## Internal service surface

The service exposes a small internal surface for:

- monitoring/alert ingress;
- authenticated notification delivery;
- non-sending synthetic preparation and capability probes;
- metadata-only delivery-ledger inspection;
- health, readiness, and metrics.

Exact deployment bindings and secret-management procedures are documented under `deploy/` rather than treated as product semantics in this README.

## Development

```powershell
uv sync
uv run pytest
uv run ruff check .
uv run mypy
```

## Autonomous Development profile

Autonomous Development uses the independent `feishu-autodev-bridge` entrypoint documented in
[docs/autodev-profile.md](docs/autodev-profile.md). It is a new Feishu Bot identity with a new
secret directory, P2P owner allowlist, SQLite state file and operator HMAC. It accepts only
requirements and explicit lifecycle actions for the single registered target; it is not a
general-purpose chat, Dify or operations bot.

The existing gateway profile, its ports, credentials, state and routing remain unchanged. Set up
the new app and run the Windows helper only after following
`D:\agent\autonomous-development\docs\feishu-autodev-app-setup.md`.

## Design decisions

Detailed decisions are recorded in:

- [ADR-001](docs/decisions/0001-use-feishu-long-connection-and-dify.md) — Feishu long-connection transport choice.
- [ADR-002](docs/decisions/0002-dispatch-messages-to-control-plane-codex.md) — removal of Dify Chatflow dispatch and move to the control-plane path.
- [ADR-004](docs/decisions/0004-notification-delivery-ledger.md) — metadata-only delivery ledger and non-sending synthetic paths.
- [Administrative ingress compatibility](docs/administrative-ingress-compatibility.md) — metadata-only Feishu handoff for the administrative route.
