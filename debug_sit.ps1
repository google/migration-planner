param(
    [Parameter(Mandatory=$true)][string]$AppId,
    [Parameter(Mandatory=$true)][string]$Organization,
    [Parameter(Mandatory=$true)][string]$CertificatePath,
    [Parameter(Mandatory=$false)][string]$CertificatePassword
)

Write-Host "Importing ExchangeOnlineManagement module..." -ForegroundColor Cyan
Import-Module ExchangeOnlineManagement -ErrorAction Stop

Write-Host "Authenticating to $Organization..." -ForegroundColor Cyan
if ([string]::IsNullOrEmpty($CertificatePassword)) {
    Connect-IPPSSession -AppId $AppId -Organization $Organization -CertificateFilePath $CertificatePath -ShowBanner:$false
} else {
    $secPassword = ConvertTo-SecureString -String $CertificatePassword -AsPlainText -Force
    Connect-IPPSSession -AppId $AppId -Organization $Organization -CertificateFilePath $CertificatePath -CertificatePassword $secPassword -ShowBanner:$false
}

Write-Host "`n--- Fetching Custom Rule Packages ---" -ForegroundColor Yellow
$customRulePackages = Get-DlpSensitiveInformationTypeRulePackage | Where-Object { $_.IsDefault -eq $false }
if ($null -eq $customRulePackages) {
    Write-Host "Result: `$null (No custom rule packages found matching criteria)" -ForegroundColor Red
} else {
    $count = @($customRulePackages).Count
    Write-Host "Result: Found $count custom rule package(s)" -ForegroundColor Green
    $customRulePackages | Select-Object Name, PublisherName | Format-Table
}

Write-Host "`n--- Fetching EDM Schemas ---" -ForegroundColor Yellow
if (Get-Command "Get-EdmSchema" -ErrorAction SilentlyContinue) {
    $edmSchemas = Get-EdmSchema
    if ($null -eq $edmSchemas) {
        Write-Host "Result: `$null (Get-EdmSchema returned nothing)" -ForegroundColor Red
    } else {
        $count = @($edmSchemas).Count
        Write-Host "Result: Found $count EDM Schema(s)" -ForegroundColor Green
        $edmSchemas | Select-Object Name, DataStoreName | Format-Table
    }
} else {
    Write-Host "Result: Get-EdmSchema command is NOT available in this session." -ForegroundColor Red
    Write-Host "This usually means the authenticated Service Principal is missing required DLP permissions." -ForegroundColor Gray
}

Write-Host "`nDisconnecting session..." -ForegroundColor Cyan
Disconnect-ExchangeOnline -Confirm:$false
Write-Host "Debug complete." -ForegroundColor Green
