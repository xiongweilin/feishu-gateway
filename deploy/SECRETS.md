# 凭证录入

凭证不得通过聊天、命令参数、Git、Docker 环境变量或日志传递。实施者只负责提供脚本；用户在交互终端自行运行并粘贴值。

1. 在飞书后台创建仅本人可见的自建应用，启用机器人及长连接事件订阅，获取 App ID 和 App Secret。
2. 在 Bitwarden 生成两个随机 32-byte HMAC key：用户映射 key、通知签名 key。
3. 控制平面共享密钥 `control_plane_key` 与控制平面的 `CONTROL_PLANE_API_KEY` 保持一致，只进入网关 secret 目录与既有安全存储，不进入仓库、日志或命令历史。
4. 网关密钥由 Docker 外部卷 `feishu_secrets` 与 Windows Credential Manager 承担；当前链路不需要 Dify Chatflow API Key。
5. 当前 open_id 存于 `feishu_secrets` 卷与 Credential Manager，不显示其值。
6. 在 Windows 运行 `deploy/windows/set-feishu-gateway-credential.ps1`，录入同一个通知签名 key。
7. 在云服务器交互运行 `sudo deploy/cloud/install-cloud-secrets.sh`，录入同一个通知签名 key，以及 SMTP credential（云端 watchdog 的可逆恢复输入；当前 systemd unit 由 `WATCHDOG_EMAIL_ENABLED=false` 禁用邮件发送）。
8. 只核对文件存在、权限和服务健康；不要打印任何值。

## Autonomous Development bridge

The independent bridge uses a separate ACL-protected directory, by default
`%ProgramData%\AutonomousDevelopment\secrets`, containing only `app_id`, `app_secret`,
`owner_open_id` and `operator_hmac_secret`. Run
`deploy/windows/set-autodev-feishu-secrets.ps1` interactively; it does not echo values. These
files must never be copied into the existing `feishu_secrets` volume, Docker environment values,
`.env`, logs or Git.

`operator_hmac_secret` is shared only with the Autonomous Development control plane's
`AUTODEV_OPERATOR_HMAC_SECRET_FILE`. It is not the existing notification HMAC, administrative
HMAC, user HMAC, control-plane key or either Feishu App Secret.
