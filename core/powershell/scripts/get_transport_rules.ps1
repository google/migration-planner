param(
    [Parameter(Mandatory=$true)]
    [string]$AppId,
    [Parameter(Mandatory=$true)]
    [string]$Organization,
    [Parameter(Mandatory=$true)]
    [string]$ClientSecret,
    [Parameter(Mandatory=$true)]
    [string]$CsvPath
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
    if ($rules) {
        $rules | Select-Object Name, State, Priority, Mode, Description, 
            @{Name="Conditions"; Expression={if ($_.Conditions) { $_.Conditions -join ", " } else { $null }}},
            @{Name="Actions"; Expression={if ($_.Actions) { $_.Actions -join ", " } else { $null }}},
            @{Name="Exceptions"; Expression={if ($_.Exceptions) { $_.Exceptions -join ", " } else { $null }}},
            Comments | Export-Csv -Path $CsvPath -NoTypeInformation -Encoding UTF8 -Force
    } else {
        # Create an empty CSV with headers
        "" | Select-Object Name, State, Priority, Mode, Description, Conditions, Actions, Exceptions, Comments | ConvertTo-Csv -NoTypeInformation | Select-Object -Skip 1 | Out-File -FilePath $CsvPath -Encoding UTF8 -Force
    }
    
    $result = [PSCustomObject]@{
        Success = $true
        Errors = @{}
    }
    $result | ConvertTo-Json -Depth 4
} catch {
    $result = [PSCustomObject]@{
        Success = $false
        Errors = @{ "TransportRules" = $_.Exception.Message }
    }
    $result | ConvertTo-Json -Depth 4
} finally {
    Disconnect-ExchangeOnline -Confirm:$false -ErrorAction SilentlyContinue
}
