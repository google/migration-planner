param (
    [Parameter(Mandatory=$true)]
    [string]$SharePointAdminUrl
)

$ErrorActionPreference = "Stop"

try {
    # Requires Microsoft.Online.SharePoint.PowerShell module
    # Assumes environment or previous steps handle authentication for Connect-SPOService
    Connect-SPOService -Url $SharePointAdminUrl
    
    # Export all sites with storage and template details
    $sites = Get-SPOSite -Limit All -Detailed | Select-Object URL, Title, StorageUsageCurrent, StorageQuota, Template, Owner, SensitivityLabel, ItemCount, ViewsRecent, WebsiteInRecycleBin
    
    $result = @{
        "value" = @($sites)
    }
    $result | ConvertTo-Json -Depth 5
}
catch {
    Write-Error "Failed to fetch SharePoint sites: $_"
    exit 1
}
