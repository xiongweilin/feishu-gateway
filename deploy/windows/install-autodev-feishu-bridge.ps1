[CmdletBinding(SupportsShouldProcess)]
param(
    [switch]$Force
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$taskName = "AutonomousDevelopmentFeishuBridge"
$runScript = Join-Path $PSScriptRoot "Run-AutodevFeishuBridge.ps1"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..\..")).Path
$existing = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
if ($existing -and -not $Force) {
    throw "$taskName already exists. Use -Force only to replace this task."
}

$action = New-ScheduledTaskAction `
    -Execute "pwsh.exe" `
    -Argument "-NoLogo -NoProfile -ExecutionPolicy Bypass -File `"$runScript`"" `
    -WorkingDirectory $repoRoot
$trigger = New-ScheduledTaskTrigger -AtLogOn -User "$env:USERDOMAIN\$env:USERNAME"
$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -RestartCount 5 `
    -RestartInterval (New-TimeSpan -Minutes 1) `
    -ExecutionTimeLimit (New-TimeSpan -Days 7)
$principal = New-ScheduledTaskPrincipal `
    -UserId "$env:USERDOMAIN\$env:USERNAME" `
    -LogonType InteractiveToken `
    -RunLevel Limited

if ($PSCmdlet.ShouldProcess($taskName, "Register independent Autodev Feishu bridge task")) {
    Register-ScheduledTask `
        -TaskName $taskName `
        -Action $action `
        -Trigger $trigger `
        -Settings $settings `
        -Principal $principal `
        -Force | Out-Null
    Write-Host "Scheduled task configured: $taskName"
}
