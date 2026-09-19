# ADR-005: independent Autonomous Development Feishu profile

Status: accepted

## Context

The existing gateway already has a Feishu identity, long connection, SQLite state and business
semantics for Dify/operations/administrative traffic. Autonomous Development needs a human-owned
requirement intake UI, explicit start/cancel actions, intervention replies and a durable event
outbox. Combining those meanings would let an old bot identity or permission set authorize a new
workflow and would make recovery state ambiguous.

## Decision

Implement `feishu_dify_gateway.autodev` as a separate process/profile with:

- a new self-built Feishu Bot application;
- separate App ID/App Secret, owner allowlist, SQLite state and operator HMAC;
- P2P-only policy and an explicit target ID;
- a provider-neutral operator API client with timestamp/request-ID/body-digest HMAC;
- durable inbound/card dedup and outbox cursor/ACK state;
- `lark-channel-sdk==1.4.0` for message normalization, WebSocket lifecycle, resource download and
  card actions;
- `lark-oapi` left untouched for the existing profile.

The bridge is an operator UI only. PostgreSQL/DBOS and the Autonomous Development control plane
remain the authority for requirements, cycles, releases, interventions and terminal state.

## Consequences

The deployment has one extra Windows background task and one extra loopback health/metrics port.
There is intentional duplicated transport configuration, but the duplication is a safety
boundary: old-bot regressions and new-bot credential reuse become detectable configuration errors.
An unavailable bridge delays notification; it cannot fail a running development workflow because
the operator event outbox is acknowledged independently after recovery.
