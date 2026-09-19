[CmdletBinding()]
param(
    [string]$SecretsDirectory = $(Join-Path $env:ProgramData "AutonomousDevelopment\secrets")
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

New-Item -ItemType Directory -Force -Path $SecretsDirectory | Out-Null

function Convert-SecureValue {
    param([System.Security.SecureString]$Value)

    $bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($Value)
    try {
        return [Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr)
    }
    finally {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr)
    }
}

function Write-SecretFile {
    param(
        [string]$Name,
        [string]$Value
    )

    $path = Join-Path $SecretsDirectory $Name
    $encoding = New-Object System.Text.UTF8Encoding($false)
    [IO.File]::WriteAllText($path, $Value.Trim(), $encoding)
}

$appId = Read-Host "New Feishu Autodev App ID"
$appSecret = Convert-SecureValue (Read-Host "New Feishu Autodev App Secret" -AsSecureString)
$ownerOpenId = Read-Host "Owner open_id for the new app context"
$operatorHmac = Convert-SecureValue (Read-Host "Autonomous Development Operator HMAC" -AsSecureString)

if ([string]::IsNullOrWhiteSpace($appId) -or
    [string]::IsNullOrWhiteSpace($appSecret) -or
    [string]::IsNullOrWhiteSpace($ownerOpenId) -or
    [string]::IsNullOrWhiteSpace($operatorHmac)) {
    throw "All four values are required. No secret files were accepted as configured."
}

Write-SecretFile "app_id" $appId
Write-SecretFile "app_secret" $appSecret
Write-SecretFile "owner_open_id" $ownerOpenId
Write-SecretFile "operator_hmac_secret" $operatorHmac

icacls $SecretsDirectory /inheritance:r /grant:r `"$env:USERNAME`:(OI)(CI)(F)`" `"SYSTEM:(OI)(CI)(F)`" *> $null
Write-Host "Autodev Feishu secret directory configured: $SecretsDirectory"
Write-Host "Secret values were not displayed or written to the console."
