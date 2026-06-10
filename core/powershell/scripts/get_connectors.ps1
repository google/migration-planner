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
    $inbound = @()
    $outbound = @()

    try {
        $inboundRaw = Get-InboundConnector -ErrorAction Stop
        if ($inboundRaw) {
            foreach ($conn in @($inboundRaw)) {
                $inbound += @{
                    Name = $conn.Identity
                    Enabled = $conn.Enabled
                    ConnectorType = $conn.ConnectorType
                    SenderDomains = ($conn.SenderDomains -join ", ")
                    RequireTls = $conn.RequireTls
                }
            }
        }
    } catch {
        $errors["InboundConnectors"] = $_.Exception.Message
    }

    try {
        $outboundRaw = Get-OutboundConnector -ErrorAction Stop
        if ($outboundRaw) {
            foreach ($conn in @($outboundRaw)) {
                $outbound += @{
                    Name = $conn.Identity
                    Enabled = $conn.Enabled
                    RecipientDomains = ($conn.RecipientDomains -join ", ")
                    SmartHosts = ($conn.SmartHosts -join ", ")
                    UseMxRecord = $conn.UseMxRecord
                }
            }
        }
    } catch {
        $errors["OutboundConnectors"] = $_.Exception.Message
    }

    $result = [PSCustomObject]@{
        InboundConnectors = $inbound
        OutboundConnectors = $outbound
        Errors = $errors
    }
    $result | ConvertTo-Json -Depth 5
}
finally {
    Disconnect-ExchangeOnline -Confirm:$false -ErrorAction SilentlyContinue
}
