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

try {
    # Import the module
    Import-Module ExchangeOnlineManagement -ErrorAction Stop
    
    # Authenticate
    if ([string]::IsNullOrEmpty($CertificatePassword)) {
        Connect-IPPSSession -AppId $AppId -Organization $Organization -CertificateFilePath $CertificatePath -ShowBanner:$false
    } else {
        $secPassword = ConvertTo-SecureString -String $CertificatePassword -AsPlainText -Force
        Connect-IPPSSession -AppId $AppId -Organization $Organization -CertificateFilePath $CertificatePath -CertificatePassword $secPassword -ShowBanner:$false
    }
    
    # Retrieve sensitive info types
    $sitData = Get-DlpSensitiveInformationType | Select-Object Name, Description, PublisherName, RecommendedConfidence, Type, IsExactMatch, ContainsData, IsExactDataMatch, Publisher, IsOutOfBox
    
    # Retrieve Custom SIT Rule Packages (XML)
    $customRulePackages = Get-DlpSensitiveInformationTypeRulePackage | Where-Object { $_.IsDefault -eq $false } | Select-Object Name, Description, PublisherName, ClassificationRuleCollectionXml
    
    # Retrieve EDM Schemas
    $edmSchemas = @()
    # Check if Get-EdmSchema is available (sometimes requires specific permissions/modules)
    if (Get-Command "Get-EdmSchema" -ErrorAction SilentlyContinue) {
        $edmSchemas = Get-EdmSchema
    }

    # Return as JSON
    $result = @{
        "SensitiveInformationTypes" = $sitData
        "CustomRulePackages" = $customRulePackages
        "EdmSchemas" = $edmSchemas
    }
    $result | ConvertTo-Json -Depth 10
} catch {
    Write-Error "Failed to fetch Sensitive Information Types: $_"
    exit 1
} finally {
    Disconnect-ExchangeOnline -Confirm:$false -ErrorAction SilentlyContinue
}
