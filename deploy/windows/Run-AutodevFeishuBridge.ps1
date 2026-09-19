[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..\..")).Path
$secretRoot = Join-Path $env:ProgramData "AutonomousDevelopment\secrets"
$required = @("app_id", "app_secret", "owner_open_id", "operator_hmac_secret")
foreach ($name in $required) {
    $path = Join-Path $secretRoot $name
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
        throw "Autodev Feishu secret file is missing: $name"
    }
}

$env:AUTODEV_FEISHU_SECRETS_DIR = $secretRoot
$env:AUTODEV_FEISHU_STATE_DB = Join-Path $env:ProgramData "AutonomousDevelopment\feishu-autodev\state.db"
$env:AUTODEV_FEISHU_HOST = "127.0.0.1"
$env:AUTODEV_FEISHU_PORT = "18085"
$env:AUTODEV_OPERATOR_BASE_URL = "http://127.0.0.1:8765"
$env:AUTODEV_TARGET_ID = if (Test-Path Env:AUTODEV_TARGET_ID) {
    $env:AUTODEV_TARGET_ID
} else {
    "primary"
}

Set-Location $repoRoot
& uv run --project $repoRoot feishu-autodev-bridge
exit $LASTEXITCODE
