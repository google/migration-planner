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

# Connect to Security & Compliance PowerShell using App-Only Cert Auth
if ($CertificatePassword) {
    $secPassword = ConvertTo-SecureString -String $CertificatePassword -AsPlainText -Force
    Connect-IPPSSession -CertificateFilePath $CertificatePath -CertificatePassword $secPassword -AppId $AppId -Organization $Organization -ShowBanner:$false -WarningAction SilentlyContinue
} else {
    Connect-IPPSSession -CertificateFilePath $CertificatePath -AppId $AppId -Organization $Organization -ShowBanner:$false -WarningAction SilentlyContinue
}

try {
    $output = @()
    $standardError = $null
    $premiumError = $null

    # Get Standard eDiscovery cases
    try {
        $standardCases = Get-ComplianceCase -CaseType eDiscovery -WarningAction SilentlyContinue
        if ($standardCases) {
            foreach ($case in @($standardCases)) {
                $caseObj = [PSCustomObject]@{
                    Name = $case.Name
                    Status = $case.Status
                    CaseType = "eDiscovery (Standard)"
                    CreatedBy = $case.CreatedBy
                    WhenCreated = $case.WhenCreated
                }
                $output += $caseObj
            }
        }
    } catch {
        $standardError = $_.Exception.Message
    }

    # Get Premium (Advanced) eDiscovery cases
    try {
        $premiumCases = Get-ComplianceCase -CaseType AdvancedEdiscovery -WarningAction SilentlyContinue
        if ($premiumCases) {
            foreach ($case in @($premiumCases)) {
                $caseObj = [PSCustomObject]@{
                    Name = $case.Name
                    Status = $case.Status
                    CaseType = "eDiscovery (Premium)"
                    CreatedBy = $case.CreatedBy
                    WhenCreated = $case.WhenCreated
                }
                $output += $caseObj
            }
        }
    } catch {
        $premiumError = $_.Exception.Message
    }

    # If both failed, throw a combined exception
    if ($standardError -and $premiumError) {
        throw "Failed to fetch eDiscovery cases. eDiscovery (Standard) error: $standardError; eDiscovery (Premium) error: $premiumError"
    }

    if ($output) {
        $output | ConvertTo-Json -Depth 5
    } else {
        "[]"
    }
}
finally {
    Disconnect-ExchangeOnline -Confirm:$false
}
