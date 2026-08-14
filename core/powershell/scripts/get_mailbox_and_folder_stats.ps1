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
    $errors = @{}

    # 1. Query Shared Mailboxes
    $sharedCount = $null
    $sharedTotalBytes = $null
    try {
        $sharedMailboxes = Get-EXOMailbox -RecipientTypeDetails SharedMailbox -ResultSize Unlimited -ErrorAction Stop
        if ($sharedMailboxes) {
            $sharedStats = @($sharedMailboxes | Get-EXOMailboxStatistics -ErrorAction Stop)
            $sharedCount = @($sharedMailboxes).Count
            $sharedTotalBytes = [long]0
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
        } else {
            $sharedCount = 0
            $sharedTotalBytes = 0
        }
    } catch {
        $errors["SharedMailboxes"] = $_.Exception.Message
    }

    # 2. Query Public Folders
    $pfCount = $null
    try {
        $pfs = Get-PublicFolder -Recurse -ResultSize Unlimited -ErrorAction Stop
        if ($pfs) {
            $pfCount = @($pfs).Count
        } else {
            $pfCount = 0
        }
    } catch {
        if ($_.Exception.Message -match "Specify the -Organization parameter") {
            $pfCount = 0
        } else {
            $errors["PublicFolders"] = $_.Exception.Message
        }
    }

    # 2b. Query Mail-Enabled Public Folders
    $mailPfCount = $null
    try {
        $mailPfs = Get-MailPublicFolder -ResultSize Unlimited -ErrorAction Stop
        if ($mailPfs) {
            $mailPfCount = @($mailPfs).Count
        } else {
            $mailPfCount = 0
        }
    } catch {
        if ($_.Exception.Message -match "Specify the -Organization parameter") {
            $mailPfCount = 0
        } else {
            $errors["MailPublicFolders"] = $_.Exception.Message
        }
    }

    # 2c. Query Public Folder Stats (Size)
    $pfTotalBytes = $null
    try {
        $pfStats = Get-PublicFolderStatistics -ResultSize Unlimited -ErrorAction Stop
        if ($pfStats) {
            $pfTotalBytes = [long]0
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
        } else {
            $pfTotalBytes = 0
        }
    } catch {
        if ($_.Exception.Message -match "Specify the -Organization parameter") {
            $pfTotalBytes = 0
        } else {
            $errors["PublicFolderStats"] = $_.Exception.Message
        }
    }

    # Output results as JSON
    $result = [PSCustomObject]@{
        SharedMailboxesCount = $sharedCount
        SharedMailboxesTotalBytes = $sharedTotalBytes
        PublicFoldersCount = $pfCount
        PublicFoldersTotalBytes = $pfTotalBytes
        MailPublicFoldersCount = $mailPfCount
        Errors = $errors
    }
    $result | ConvertTo-Json -Depth 3
}
finally {
    Disconnect-ExchangeOnline -Confirm:$false -ErrorAction SilentlyContinue
}