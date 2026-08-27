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

    # Query all mailboxes on hold (both LitigationHoldEnabled and InPlaceHolds)
    $mailboxes = Get-Mailbox -ResultSize Unlimited | Where-Object {
        $_.LitigationHoldEnabled -eq $true -or ($_.InPlaceHolds -ne $null -and $_.InPlaceHolds.Count -gt 0)
    } | ForEach-Object {
        $holdsList = @()
        if ($_.LitigationHoldEnabled) {
            $holdsList += "Litigation Hold"
        }
        if ($_.InPlaceHolds) {
            foreach ($h in $_.InPlaceHolds) {
                if ($h -and "$h".Trim() -and "$h".Trim() -ne "-") {
                    $holdsList += "$h".Trim()
                }
            }
        }
        [PSCustomObject]@{
            DisplayName           = $_.DisplayName
            UserPrincipalName     = if ($_.UserPrincipalName) { $_.UserPrincipalName } else { $_.PrimarySmtpAddress }
            InPlaceHolds          = $holdsList
            LitigationHoldEnabled = [bool]$_.LitigationHoldEnabled
        }
    }
    
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
