param (
    [Parameter(Mandatory=$true)][string]$Organization,
    [Parameter(Mandatory=$true)][string]$AppId,
    [Parameter(Mandatory=$true)][string]$CertificateFilePath,
    [Parameter(Mandatory=$true)][string]$CertificatePassword
)

# Force TLS 1.2
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$ErrorActionPreference = "Stop"

if (-not (Get-Module -ListAvailable -Name ExchangeOnlineManagement)) {
    throw "ExchangeOnlineManagement PowerShell module is not installed."
}

Import-Module ExchangeOnlineManagement

try {
    # Convert password to SecureString for macOS/Linux compatibility
    $secPass = ConvertTo-SecureString $CertificatePassword -AsPlainText -Force
    # Connect using Certificate-based App-Only Authentication
    Connect-ExchangeOnline -AppID $AppId -Organization $Organization -CertificateFilePath $CertificateFilePath -CertificatePassword $secPass -ShowBanner:$false -ErrorAction Stop

    $result = @{
        "m365_policies" = @()
        "exchange_deps" = @()
    }

    # Fetch M365 Data at Rest Encryption Policies (Customer Key multi-workload)
    try {
        $m365Policies = Get-M365DataAtRestEncryptionPolicy -ErrorAction SilentlyContinue
        if ($m365Policies) {
            foreach ($pol in $m365Policies) {
                $result["m365_policies"] += @{
                    "Name" = $pol.Name
                    "Description" = $pol.Description
                }
            }
        }
    } catch {
        # Catch unsupported or missing permissions specifically for this cmdlet
    }

    # Fetch legacy Exchange Data Encryption Policies (DEPs)
    try {
        $deps = Get-DataEncryptionPolicy -ErrorAction SilentlyContinue
        if ($deps) {
            foreach ($dep in $deps) {
                $result["exchange_deps"] += @{
                    "Name" = $dep.Name
                    "Description" = $dep.Description
                }
            }
        }
    } catch {
        # Catch unsupported or missing permissions specifically for this cmdlet
    }

    $result | ConvertTo-Json -Depth 5 -Compress

} catch {
    Write-Error $_.Exception.Message
    exit 1
} finally {
    Disconnect-ExchangeOnline -Confirm:$false -ErrorAction SilentlyContinue
}
