param (
    [Parameter(Mandatory=$true)]
    [string]$AppId,

    [Parameter(Mandatory=$true)]
    [string]$Organization,

    [Parameter(Mandatory=$true)]
    [string]$CertificatePath,

    [Parameter(Mandatory=$true)]
    [string]$CertificatePassword
)

$ErrorActionPreference = "Stop"

try {
    # Convert password to secure string
    $secPassword = ConvertTo-SecureString -String $CertificatePassword -AsPlainText -Force

    # Import module and connect
    Import-Module ExchangeOnlineManagement -ErrorAction Stop
    Connect-ExchangeOnline -CertificateFilePath $CertificatePath -CertificatePassword $secPassword -AppId $AppId -Organization $Organization -ShowBanner:$false -WarningAction SilentlyContinue

    # Get mailboxes on hold
    $mailboxes = Get-Mailbox -ResultSize Unlimited | Where-Object {$_.InPlaceHolds -ne $null} | Select-Object DisplayName, InPlaceHolds
    
    $result = @{
        "value" = @($mailboxes)
    }
    $result | ConvertTo-Json -Depth 5
}
catch {
    Write-Error "Failed to fetch Legal Holds: $_"
    exit 1
}
finally {
    Disconnect-ExchangeOnline -Confirm:$false -WarningAction SilentlyContinue
}
