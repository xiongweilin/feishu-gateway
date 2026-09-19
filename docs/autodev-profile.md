# Independent Autonomous Development Feishu profile

The repository now contains two intentionally separate profiles:

| Profile | Entry point | Identity/state | Scope |
|---|---|---|---|
| Existing gateway | existing compose entrypoint | existing App ID, old secret directory and old SQLite state | Dify/operations/notifications and existing ingress behavior |
| Autodev bridge | `feishu-autodev-bridge` | new App ID, new secret files, new SQLite state and new operator HMAC | owner-only P2P Autonomous Development requirements |

The new profile is implemented in `feishu_dify_gateway.autodev` and uses
`lark-channel-sdk==1.4.0` (`lark_channel.FeishuChannel`). `lark-oapi` remains installed for the
existing profile and is not migrated. The profile enables strict Channel SDK security, WebSocket
reconnect, normalized messages, resource download, card actions and explicit P2P allowlisting.

## Runtime configuration

Non-secret settings:

- `AUTODEV_FEISHU_SECRETS_DIR` (default `%ProgramData%\AutonomousDevelopment\secrets`);
- `AUTODEV_FEISHU_STATE_DB` (default `%ProgramData%\AutonomousDevelopment\feishu-autodev\state.db`);
- `AUTODEV_FEISHU_HOST=127.0.0.1`;
- `AUTODEV_FEISHU_PORT=18085` (select another free loopback port only after live inventory);
- `AUTODEV_OPERATOR_BASE_URL=http://127.0.0.1:8765`;
- `AUTODEV_TARGET_ID` for the one registered target;
- `AUTODEV_FEISHU_MAX_FILE_BYTES` (default 10 MiB);
- `AUTODEV_FEISHU_MAX_TEXT_CHARS` (default 100,000).

Secret files are `app_id`, `app_secret`, `owner_open_id` and `operator_hmac_secret`. The latter
must match the control plane's separate `AUTODEV_OPERATOR_HMAC_SECRET_FILE`; it is not any old
gateway HMAC or Feishu App Secret. Use `deploy/windows/set-autodev-feishu-secrets.ps1` to create
the ACL-protected directory without echoing values.

## Local health

The independent process exposes only loopback health/metrics:

```text
GET http://127.0.0.1:18085/healthz
GET http://127.0.0.1:18085/readyz
GET http://127.0.0.1:18085/metrics
```

`readyz` requires the Channel SDK connection and the outbox worker. The control plane remains
ready for a durable development cycle when this bridge is down; its own `/ready` reports the
operator API, event backlog and intervention count.

## Development checks

```powershell
uv sync --locked
uv run ruff check .
uv run ruff format --check .
uv run mypy
uv run pytest
docker compose config -q
```

Complete the manual app installation and real P2P E2E only by following the control-plane
repository's `docs/feishu-autodev-app-setup.md` and `docs/feishu-integration-runbook.md`.
