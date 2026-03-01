param(
  [string]$BaseUrl = "https://inventorysystem-d5ff8.web.app"
)

$ErrorActionPreference = 'Stop'

$itemsUrl = ($BaseUrl.TrimEnd('/') + "/api/items?limit=5&q=")
$namesUrl = ($BaseUrl.TrimEnd('/') + "/api/names?limit=5&q=")

Write-Host "GET $itemsUrl"
Invoke-RestMethod -Uri $itemsUrl -Method Get | ConvertTo-Json -Depth 10

Write-Host "GET $namesUrl"
Invoke-RestMethod -Uri $namesUrl -Method Get | ConvertTo-Json -Depth 10
