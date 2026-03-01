param(
  [string]$Project = "",
  [string]$ServiceAccountPath = ""
)

$ErrorActionPreference = 'Stop'

function ConvertFrom-SecureStringToPlainText {
  param(
    [Parameter(Mandatory=$true)][Security.SecureString]$Secure
  )
  $bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($Secure)
  try {
    return [Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr)
  } finally {
    [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr)
  }
}

function Read-SecretText {
  param(
    [Parameter(Mandatory=$true)][string]$Prompt
  )
  $secure = Read-Host $Prompt -AsSecureString
  if (-not $secure) { return "" }
  $plain = ConvertFrom-SecureStringToPlainText -Secure $secure
  return [string]$plain
}

function Resolve-Project {
  if ($Project -and $Project.Trim() -ne "") {
    return $Project.Trim()
  }

  $firebaserc = Join-Path -Path (Resolve-Path (Join-Path $PSScriptRoot "..\..")) -ChildPath ".firebaserc"
  if (Test-Path $firebaserc) {
    $json = Get-Content $firebaserc -Raw | ConvertFrom-Json
    if ($json.projects -and $json.projects.default) {
      return [string]$json.projects.default
    }
  }

  throw "Cannot determine Firebase project. Pass -Project <id>."
}

function Resolve-ServiceAccountPath {
  if ($ServiceAccountPath -and $ServiceAccountPath.Trim() -ne "") {
    return (Resolve-Path $ServiceAccountPath).Path
  }

  $defaultPath = Join-Path $PSScriptRoot "..\serviceAccount.json"
  if (Test-Path $defaultPath) {
    return (Resolve-Path $defaultPath).Path
  }

  throw "serviceAccount.json not found. Pass -ServiceAccountPath <path>."
}

function Set-SecretFromStringFile {
  param(
    [Parameter(Mandatory=$true)][string]$Key,
    [Parameter(Mandatory=$true)][string]$Value,
    [Parameter(Mandatory=$true)][string]$ProjectId
  )

  $tmp = New-TemporaryFile
  try {
    # PowerShell 5.1: Set-Content -Encoding utf8 writes BOM which breaks PEM keys.
    # Write UTF-8 without BOM to keep private key intact.
    $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($tmp, $Value, $utf8NoBom)
    firebase functions:secrets:set $Key --project $ProjectId --data-file $tmp | Out-Host
  } finally {
    Remove-Item $tmp -Force -ErrorAction SilentlyContinue
  }
}

$projectId = Resolve-Project
$saPath = Resolve-ServiceAccountPath

Write-Host "Using Firebase project: $projectId"
Write-Host "Using service account: $saPath"

$sa = Get-Content $saPath -Raw | ConvertFrom-Json
if (-not $sa.client_email -or -not $sa.private_key) {
  throw "serviceAccount.json missing client_email/private_key"
}

Set-SecretFromStringFile -Key "GOOGLE_CLIENT_EMAIL" -Value ([string]$sa.client_email) -ProjectId $projectId
Set-SecretFromStringFile -Key "GOOGLE_PRIVATE_KEY" -Value ([string]$sa.private_key) -ProjectId $projectId

$sheetId = Read-Host "Enter SPREADSHEET_ID (Google Sheet id)"
if ($sheetId -and $sheetId.Trim() -ne "") {
  Set-SecretFromStringFile -Key "SPREADSHEET_ID" -Value $sheetId.Trim() -ProjectId $projectId
}

$tgToken = Read-SecretText "Enter TELEGRAM_TOKEN (leave blank to skip)"
if ($tgToken -and $tgToken.Trim() -ne "") {
  Set-SecretFromStringFile -Key "TELEGRAM_TOKEN" -Value $tgToken.Trim() -ProjectId $projectId
}

$tgChatId = Read-Host "Enter TELEGRAM_CHAT_ID (leave blank to skip)"
if ($tgChatId -and $tgChatId.Trim() -ne "") {
  Set-SecretFromStringFile -Key "TELEGRAM_CHAT_ID" -Value $tgChatId.Trim() -ProjectId $projectId
}

Write-Host "\nSMTP settings (for monthly PDF email reports)" -ForegroundColor Cyan
Write-Host "Tip: หากใช้ Gmail แนะนำสร้าง App Password แล้วใช้ smtp.gmail.com:587" -ForegroundColor DarkGray

$smtpHost = Read-Host "Enter SMTP_HOST (leave blank to skip)"
if ($smtpHost -and $smtpHost.Trim() -ne "") {
  Set-SecretFromStringFile -Key "SMTP_HOST" -Value $smtpHost.Trim() -ProjectId $projectId

  $smtpPort = Read-Host "Enter SMTP_PORT (default 587)"
  if (-not $smtpPort -or $smtpPort.Trim() -eq "") { $smtpPort = "587" }
  Set-SecretFromStringFile -Key "SMTP_PORT" -Value $smtpPort.Trim() -ProjectId $projectId

  $smtpUser = Read-Host "Enter SMTP_USER (email/login)"
  if ($smtpUser -and $smtpUser.Trim() -ne "") {
    Set-SecretFromStringFile -Key "SMTP_USER" -Value $smtpUser.Trim() -ProjectId $projectId
  }

  $smtpPass = Read-SecretText "Enter SMTP_PASS (password/app password)"
  if ($smtpPass -and $smtpPass.Trim() -ne "") {
    Set-SecretFromStringFile -Key "SMTP_PASS" -Value $smtpPass.Trim() -ProjectId $projectId
  }

  $smtpFrom = Read-Host "Enter SMTP_FROM (from address, blank = use SMTP_USER)"
  if ($smtpFrom -and $smtpFrom.Trim() -ne "") {
    Set-SecretFromStringFile -Key "SMTP_FROM" -Value $smtpFrom.Trim() -ProjectId $projectId
  }
}

Write-Host "Done. Next: firebase deploy --only functions --project $projectId"
