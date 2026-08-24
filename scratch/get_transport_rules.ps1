param(
    [string]$TenantId = "a8a95e4b-10a6-4526-a2cb-db7682eba2e6",
    [string]$ClientId = "16e96d46-0e64-481a-872f-c538c1a99311",
    [string]$ClientSecret = "j_L8Q~DsaImyAmDqU4u~kjz~43b15N1PzllASbNW"
)

Import-Module ExchangeOnlineManagement

$body = @{
    grant_type    = "client_credentials"
    client_id     = $ClientId
    client_secret = $ClientSecret
    scope         = "https://outlook.office365.com/.default"
}

try {
    $tokenResponse = Invoke-RestMethod -Method Post -Uri "https://login.microsoftonline.com/$TenantId/oauth2/v2.0/token" -ContentType "application/x-www-form-urlencoded" -Body $body
    $token = $tokenResponse.access_token

    Connect-ExchangeOnline -AccessToken $token -Organization $TenantId -ShowBanner:$false
    
    $rules = Get-TransportRule
    $rules | Select-Object Name, State, Priority, Mode, Description, Conditions, Actions, Exceptions, Comments | ConvertTo-Json -Depth 3
} catch {
    Write-Host "Error: $($_.Exception.Message)"
} finally {
    Disconnect-ExchangeOnline -Confirm:$false -ErrorAction SilentlyContinue
}
