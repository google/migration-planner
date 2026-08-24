
    param($AppId, $Organization, $CertificatePath, $CertificatePassword)
    Import-Module ExchangeOnlineManagement
    $secPassword = ConvertTo-SecureString -String $CertificatePassword -AsPlainText -Force
    Connect-IPPSSession -CertificateFilePath $CertificatePath -CertificatePassword $secPassword -AppId $AppId -Organization $Organization -ShowBanner:$false -WarningAction SilentlyContinue
    
    $policies = Get-RetentionCompliancePolicy
    $count = @($policies).Count
    Write-Output "TOTAL POLICIES IN POWERSHELL: $count"
    
    foreach ($p in @($policies)) {
        Write-Output $p.Name
    }
    
    Disconnect-ExchangeOnline -Confirm:$false -WarningAction SilentlyContinue
    