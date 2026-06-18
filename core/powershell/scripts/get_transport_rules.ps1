param(
    [Parameter(Mandatory=$true)]
    [string]$AppId,
    [Parameter(Mandatory=$true)]
    [string]$Organization,
    [Parameter(Mandatory=$true)]
    [string]$ClientSecret
)

$ErrorActionPreference = "Stop"

if (-not (Get-Module -ListAvailable -Name ExchangeOnlineManagement)) {
    throw "ExchangeOnlineManagement PowerShell module is not installed."
}

Import-Module ExchangeOnlineManagement

$body = @{
    grant_type    = "client_credentials"
    client_id     = $AppId
    client_secret = $ClientSecret
    scope         = "https://outlook.office365.com/.default"
}

try {
    $tokenResponse = Invoke-RestMethod -Method Post -Uri "https://login.microsoftonline.com/$Organization/oauth2/v2.0/token" -ContentType "application/x-www-form-urlencoded" -Body $body
    $token = $tokenResponse.access_token

    Connect-ExchangeOnline -AccessToken $token -Organization $Organization -ShowBanner:$false -WarningAction SilentlyContinue
    
    $rules = Get-TransportRule -ErrorAction Stop
    $output = @()
    if ($rules) {
        foreach ($rule in @($rules)) {
            $output += @{
                Name = $rule.Name
                State = $rule.State
                Priority = $rule.Priority
                Mode = $rule.Mode
                Description = $rule.Description
                Conditions = if ($rule.Conditions) { $rule.Conditions -join ", " } else { $null }
                Actions = if ($rule.Actions) { $rule.Actions -join ", " } else { $null }
                Exceptions = if ($rule.Exceptions) { $rule.Exceptions -join ", " } else { $null }
                Comments = $rule.Comments
            }
        }
    }
    
    $result = [PSCustomObject]@{
        TransportRules = $output
        Errors = @{}
    }
    $result | ConvertTo-Json -Depth 4
} catch {
    $result = [PSCustomObject]@{
        TransportRules = @()
        Errors = @{ "TransportRules" = $_.Exception.Message }
    }
    $result | ConvertTo-Json -Depth 4
} finally {
    Disconnect-ExchangeOnline -Confirm:$false -ErrorAction SilentlyContinue
}
