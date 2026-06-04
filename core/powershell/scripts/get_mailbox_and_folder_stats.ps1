param(
    [Parameter(Mandatory=$true)]
    [string]$AppId,
    [Parameter(Mandatory=$true)]
    [string]$Organization,
    [Parameter(Mandatory=$true)]
    [string]$CertificatePath,
    [Parameter(Mandatory=$false)]
    [string]$CertificatePassword
)

$ErrorActionPreference = "Stop"

# Check if ExchangeOnlineManagement module is installed beforehand
if (-not (Get-Module -ListAvailable -Name ExchangeOnlineManagement)) {
    throw "ExchangeOnlineManagement PowerShell module is not installed. Please install it beforehand by running: Install-Module -Name ExchangeOnlineManagement -Scope CurrentUser"
}

Import-Module ExchangeOnlineManagement

# Connect to Exchange Online using App-Only Cert Auth
if ($CertificatePassword) {
    $secPassword = ConvertTo-SecureString -String $CertificatePassword -AsPlainText -Force
    Connect-ExchangeOnline -CertificateFilePath $CertificatePath -CertificatePassword $secPassword -AppId $AppId -Organization $Organization -ShowBanner:$false -WarningAction SilentlyContinue
} else {
    Connect-ExchangeOnline -CertificateFilePath $CertificatePath -AppId $AppId -Organization $Organization -ShowBanner:$false -WarningAction SilentlyContinue
}

try {
    # 1. Query Shared Mailboxes
    $sharedCount = 0
    $sharedTotalBytes = [long]0
    try {
        $sharedMailboxes = Get-Mailbox -RecipientTypeDetails SharedMailbox -ResultSize Unlimited -ErrorAction SilentlyContinue
        if ($sharedMailboxes) {
            $sharedStats = @($sharedMailboxes | Get-MailboxStatistics -ErrorAction SilentlyContinue)
            $sharedCount = @($sharedMailboxes).Count
            foreach ($stat in $sharedStats) {
                if ($stat.TotalItemSize -and $stat.TotalItemSize.Value) {
                    try {
                        $bytes = $stat.TotalItemSize.Value.ToBytes()
                    } catch {
                        $bytesStr = $stat.TotalItemSize.ToString()
                        if ($bytesStr -match '\(([\d,]+) bytes\)') {
                            $bytes = [long]($Matches[1] -replace ',', '')
                        } else {
                            $bytes = [long]0
                        }
                    }
                    $sharedTotalBytes += $bytes
                }
            }
        }
    } catch {
        # Log or handle
    }

    # 2. Query Public Folders
    $pfCount = 0
    try {
        $pfs = Get-PublicFolder -Recurse -ResultSize Unlimited -ErrorAction SilentlyContinue
        if ($pfs) {
            $pfCount = @($pfs).Count
        }
    } catch {
        $pfCount = 0
    }

    $pfTotalBytes = [long]0
    try {
        $pfStats = Get-PublicFolderStatistics -ResultSize Unlimited -ErrorAction SilentlyContinue
        if ($pfStats) {
            foreach ($stat in @($pfStats)) {
                if ($stat.TotalItemSize -and $stat.TotalItemSize.Value) {
                    try {
                        $bytes = $stat.TotalItemSize.Value.ToBytes()
                    } catch {
                        $bytesStr = $stat.TotalItemSize.ToString()
                        if ($bytesStr -match '\(([\d,]+) bytes\)') {
                            $bytes = [long]($Matches[1] -replace ',', '')
                        } else {
                            $bytes = [long]0
                        }
                    }
                    $pfTotalBytes += $bytes
                }
            }
        }
    } catch {
        # Log or handle
    }

    # Output results as JSON
    $result = [PSCustomObject]@{
        SharedMailboxesCount = $sharedCount
        SharedMailboxesTotalBytes = $sharedTotalBytes
        PublicFoldersCount = $pfCount
        PublicFoldersTotalBytes = $pfTotalBytes
    }
    $result | ConvertTo-Json
}
finally {
    Disconnect-ExchangeOnline -Confirm:$false -ErrorAction SilentlyContinue
}
