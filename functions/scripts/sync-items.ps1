param(
  [string]$BaseUrl = "https://inventorysystem-d5ff8.web.app",
  [string]$Token = ""
)

$ErrorActionPreference = 'Stop'

function Read-TokenIfMissing {
  if ($Token -and $Token.Trim() -ne "") {
    $x = $Token.Trim()
    if ($x.StartsWith('<') -and $x.EndsWith('>') -and $x.Length -ge 2) {
      $x = $x.Substring(1, $x.Length - 2).Trim()
    }
    return $x
  }

  if ($env:SYNC_TOKEN -and $env:SYNC_TOKEN.Trim() -ne "") {
    $x = $env:SYNC_TOKEN.Trim()
    if ($x.StartsWith('<') -and $x.EndsWith('>') -and $x.Length -ge 2) {
      $x = $x.Substring(1, $x.Length - 2).Trim()
    }
    return $x
  }

  $t = Read-Host "Enter SYNC_TOKEN (sent as x-sync-token header)"
  if ($null -eq $t) { $t = "" }
  return ([string]$t).Trim()
}

$syncToken = Read-TokenIfMissing
if (-not $syncToken) {
  throw "Missing token. Provide -Token or enter when prompted."
}

$endpoint = ($BaseUrl.TrimEnd('/') + "/api/syncItems")

Write-Host "POST $endpoint"

try {
  $headers = @{ 'x-sync-token' = $syncToken }
  $result = Invoke-RestMethod -Uri $endpoint -Method Post -Headers $headers -ContentType 'application/json' -Body '{}' 
  $result | ConvertTo-Json -Depth 10
  exit 0
} catch {
  Write-Host "Request failed." -ForegroundColor Red

  # PowerShell often puts the response body here for non-2xx
  if ($_.ErrorDetails -and $_.ErrorDetails.Message) {
    Write-Host $_.ErrorDetails.Message
    exit 1
  }

  if ($_.Exception -and $_.Exception.Response) {
    try {
      try {
        $statusCode = $_.Exception.Response.StatusCode
        if ($statusCode) { Write-Host ("HTTP Status: " + $statusCode) }
      } catch {}

      $reader = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
      $body = $reader.ReadToEnd()
      Write-Host $body
      exit 1
    } catch {
      Write-Host $_.Exception.Message
      exit 1
    }
  }
  Write-Host $_.Exception.Message
  exit 1
}
