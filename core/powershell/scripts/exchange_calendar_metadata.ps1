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
    throw "ExchangeOnlineManagement PowerShell module is not installed."
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
    $roomsCount = 0
    $roomsNaming = $null
    $roomsError = $null
    try {
        $rooms = @(Get-EXOMailbox -RecipientTypeDetails RoomMailbox -ResultSize Unlimited)
        $roomsCount = $rooms.Count
        $roomsNaming = if ($rooms) { (($rooms | Select-Object -First 5 -ExpandProperty Name) -join ", ") } else { $null }
    } catch {
        $roomsError = $_.Exception.Message
    }

    $equipmentCount = 0
    $equipmentError = $null
    try {
        $equipment = @(Get-EXOMailbox -RecipientTypeDetails EquipmentMailbox -ResultSize Unlimited)
        $equipmentCount = $equipment.Count
    } catch {
        $equipmentError = $_.Exception.Message
    }

    $owaPolicyError = $null
    $canShareAttachments = $true
    try {
        $owaPolicy = Get-OwaMailboxPolicy | Where-Object { $_.IsDefault -eq $true }
        if (-not $owaPolicy) { 
            $owaPolicy = Get-OwaMailboxPolicy | Select-Object -First 1 
        }
        $canShareAttachments = if ($owaPolicy) { $owaPolicy.ClassicAttachmentsEnabled } else { $true }
    } catch {
        $owaPolicyError = $_.Exception.Message
    }

    $orgAppsData = @()
    $appsError = $null
    try {
        $orgApps = @(Get-App -OrganizationApp -ErrorAction Stop)
        if ($orgApps) {
            foreach ($app in $orgApps) {
                $orgAppsData += [PSCustomObject]@{
                    DisplayName = $app.DisplayName
                    AppId = $app.AppId
                    Enabled = $app.Enabled
                }
            }
        }
    } catch {
        $appsError = $_.Exception.Message
    }
    
    $result = [PSCustomObject]@{
        RoomsCount             = $roomsCount
        RoomsError             = $roomsError
        RoomsNaming            = $roomsNaming
        EquipmentCount         = $equipmentCount
        EquipmentError         = $equipmentError
        CanShareAttachments    = $canShareAttachments
        OwaPolicyError         = $owaPolicyError
        OrganizationApps       = $orgAppsData
        AppsError              = $appsError
    }
    $result | ConvertTo-Json
}
finally {
    Disconnect-ExchangeOnline -Confirm:$false -ErrorAction SilentlyContinue
}
