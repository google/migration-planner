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
    # Retrieve policies
    $policies = Get-DlpCompliancePolicy
    $output = @()

    if ($policies) {
        $policies_list = @($policies)
        
        foreach ($policy in $policies_list) {
            # Retrieve rules for this policy
            $rules = Get-DlpComplianceRule -Policy $policy.Identity.ToString()
            $rulesOutput = @()
            
            if ($rules) {
                foreach ($rule in @($rules)) {
                    # Convert actions and conditions/exceptions to strings for robust inspection
                    $actionsList = @()
                    if ($rule.Actions) {
                        foreach ($action in $rule.Actions) {
                            $actionsList += $action.ToString()
                        }
                    }
                    
                    $ruleObj = [PSCustomObject]@{
                        Name = $rule.Name
                        Comment = $rule.Comment
                        Enabled = $rule.Enabled
                        Actions = $actionsList
                    }
                    $rulesOutput += $ruleObj
                }
            }
            
            # Format locations (can be lists of specific objects or strings, let's convert to arrays/strings)
            $exchangeLoc = $null
            if ($policy.ExchangeLocation) {
                $exchangeLoc = @($policy.ExchangeLocation | ForEach-Object { $_.ToString() })
            }
            
            $sharepointLoc = $null
            if ($policy.SharePointLocation) {
                $sharepointLoc = @($policy.SharePointLocation | ForEach-Object { $_.ToString() })
            }
            
            $onedriveLoc = $null
            if ($policy.OneDriveLocation) {
                $onedriveLoc = @($policy.OneDriveLocation | ForEach-Object { $_.ToString() })
            }
            
            $teamsLoc = $null
            if ($policy.TeamsLocation) {
                $teamsLoc = @($policy.TeamsLocation | ForEach-Object { $_.ToString() })
            }
            
            $devicesLoc = $null
            if ($policy.DevicesLocation) {
                $devicesLoc = @($policy.DevicesLocation | ForEach-Object { $_.ToString() })
            }
            
            $policyObj = [PSCustomObject]@{
                Name = $policy.Name
                Comment = $policy.Comment
                ExchangeLocation = $exchangeLoc
                SharePointLocation = $sharepointLoc
                OneDriveLocation = $onedriveLoc
                TeamsLocation = $teamsLoc
                DevicesLocation = $devicesLoc
                Mode = $policy.Mode.ToString()
                DistributionStatus = $policy.DistributionStatus.ToString()
                Enabled = $policy.Enabled
                Identity = $policy.Identity.ToString()
                WhenCreated = $policy.WhenCreated
                WhenChanged = $policy.WhenChanged
                CreatedBy = $policy.CreatedBy
                LastModifiedBy = $policy.LastModifiedBy
                Rules = $rulesOutput
            }
            $output += $policyObj
        }
        $output | ConvertTo-Json -Depth 5
    } else {
        "[]"
    }
}
finally {
    Disconnect-ExchangeOnline -Confirm:$false
}
