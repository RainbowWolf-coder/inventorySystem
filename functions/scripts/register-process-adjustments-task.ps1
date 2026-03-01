param(
  [Parameter(Mandatory = $false)]
  [string]$Project = "inventorysystem-d5ff8",

  [Parameter(Mandatory = $false)]
  [string]$BaseUrl = "",

  [Parameter(Mandatory = $true)]
  [string]$Token,

  [Parameter(Mandatory = $false)]
  [ValidateRange(1, 1440)]
  [int]$IntervalMinutes = 5,

  [Parameter(Mandatory = $false)]
  [ValidateRange(1, 20)]
  [int]$MaxToProcess = 5,

  [Parameter(Mandatory = $false)]
  [string]$TaskName = "InventorySystem-ProcessAdjustments"
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$processScript = Join-Path $scriptDir "process-adjustments.ps1"

if (-not (Test-Path $processScript)) {
  throw "Cannot find process script at: $processScript"
}

function Format-Token([string]$t) {
  if (-not $t) { return "" }
  $x = $t.Trim()
  if ($x.StartsWith('<') -and $x.EndsWith('>') -and $x.Length -ge 2) {
    $x = $x.Substring(1, $x.Length - 2).Trim()
  }
  return $x
}

$effectiveBaseUrl = if ($BaseUrl -and $BaseUrl.Trim() -ne "") { $BaseUrl.Trim() } else { ("https://" + $Project.Trim() + ".web.app") }
$safeToken = Format-Token $Token

# -WindowStyle Hidden prevents a PowerShell window from popping up during scheduled runs.
$actionArgs = "-NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File `"$processScript`" -BaseUrl `"$effectiveBaseUrl`" -Token `"$safeToken`" -MaxToProcess $MaxToProcess"
$action = New-ScheduledTaskAction -Execute "PowerShell.exe" -Argument $actionArgs

$startAt = (Get-Date).AddMinutes(1)
$trigger = New-ScheduledTaskTrigger -Once -At $startAt `
  -RepetitionInterval (New-TimeSpan -Minutes $IntervalMinutes) `
  -RepetitionDuration (New-TimeSpan -Days 3650)

$settings = New-ScheduledTaskSettingsSet -Hidden
Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings -Force | Out-Null

Write-Host "Scheduled task created/updated:" -ForegroundColor Green
Write-Host "  Name: $TaskName"
Write-Host "  Every: $IntervalMinutes minute(s)"
Write-Host "  Batch max: $MaxToProcess"
Write-Host "  Starts: $startAt"

Write-Host "\nTo run immediately:" -ForegroundColor Cyan
Write-Host "  Start-ScheduledTask -TaskName `"$TaskName`""

Write-Host "\nTo remove:" -ForegroundColor Cyan
Write-Host "  Unregister-ScheduledTask -TaskName `"$TaskName`" -Confirm:\$false"
