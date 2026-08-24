param(
    [Parameter(Mandatory=$true)][string]$AppId,
    [Parameter(Mandatory=$true)][string]$Organization,
    [Parameter(Mandatory=$true)][string]$CertificatePath,
    [Parameter(Mandatory=$false)][string]$CertificatePassword
)

Import-Module ExchangeOnlineManagement -ErrorAction Stop

if ([string]::IsNullOrEmpty($CertificatePassword)) {
    Connect-IPPSSession -AppId $AppId -Organization $Organization -CertificateFilePath $CertificatePath -ShowBanner:$false
} else {
    $secPassword = ConvertTo-SecureString -String $CertificatePassword -AsPlainText -Force
    Connect-IPPSSession -AppId $AppId -Organization $Organization -CertificateFilePath $CertificatePath -CertificatePassword $secPassword -ShowBanner:$false
}

Write-Host "`n--- Fetching All SITs grouped by Publisher ---" -ForegroundColor Yellow
$sits = Get-DlpSensitiveInformationType
$sits | Group-Object PublisherName | Select-Object Count, Name | Format-Table

Write-Host "`n--- Fetching Custom SITs (Publisher != 'Microsoft Corporation') ---" -ForegroundColor Yellow
$customSits = $sits | Where-Object { $_.PublisherName -ne "Microsoft Corporation" }
$customSits | Select-Object Name, PublisherName, IsExactMatch, ContainsData | Format-Table

Disconnect-ExchangeOnline -Confirm:$false
