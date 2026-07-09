param (
    [Parameter(Mandatory=$true)]
    [string]$AppId,

    [Parameter(Mandatory=$true)]
    [string]$Organization,

    [Parameter(Mandatory=$true)]
    [string]$CertificatePath,

    [Parameter(Mandatory=$true)]
    [string]$CertificatePassword
)

$ErrorActionPreference = "Stop"

try {
    # Convert password to secure string
    $secPassword = ConvertTo-SecureString -String $CertificatePassword -AsPlainText -Force

    # Import module and connect
    Import-Module ExchangeOnlineManagement -ErrorAction Stop
    Connect-IPPSSession -CertificateFilePath $CertificatePath -CertificatePassword $secPassword -AppId $AppId -Organization $Organization -ShowBanner:$false -WarningAction SilentlyContinue

    # Retrieve policies
    $policies = Get-DlpCompliancePolicy
    $policies_output = @()
    $rules_output = @()

    if ($policies) {
        # Retrieve all compliance rules at once to map them
        $allRules = Get-DlpComplianceRule
        $ruleMap = @{}
        if ($allRules) {
            foreach ($rule in @($allRules)) {
                # Add to rules output
                $rules_output += [PSCustomObject]@{
                    Name = $rule.Name
                    Policy = $rule.Policy.ToString()
                    ContentContainsSensitiveInformation = $rule.ContentContainsSensitiveInformation
                    ExceptIfContentContainsSensitiveInformation = $rule.ExceptIfContentContainsSensitiveInformation
                    AccessScope = $rule.AccessScope
                    BlockAccess = $rule.BlockAccess
                    AdvancedRule = $rule.AdvancedRule
                }

                if ($rule.Policy) {
                    $rawKey = $rule.Policy.ToString()
                    
                    if (-not $ruleMap.ContainsKey($rawKey)) {
                        $ruleMap[$rawKey] = @()
                    }
                    $ruleMap[$rawKey] += $rule
                    
                    # Extract the policy name if the reference is a DistinguishedName
                    if ($rawKey -match "CN=([^,]+)") {
                        $cnKey = $Matches[1]
                        if (-not $ruleMap.ContainsKey($cnKey)) {
                            $ruleMap[$cnKey] = @()
                        }
                        $ruleMap[$cnKey] += $rule
                    }
                }
            }
        }

        # Handle case where $policies is not an array
        $policies_list = @($policies)
        
        foreach ($policy in $policies_list) {
            $actions = "None"
            
            $rules = $null
            if ($ruleMap.ContainsKey($policy.Name)) {
                $rules = $ruleMap[$policy.Name]
            } elseif ($ruleMap.ContainsKey($policy.Identity.ToString())) {
                $rules = $ruleMap[$policy.Identity.ToString()]
            } elseif ($policy.Guid -and $ruleMap.ContainsKey($policy.Guid.ToString())) {
                $rules = $ruleMap[$policy.Guid.ToString()]
            }

            if ($rules) {
                $actionList = @()
                foreach ($rule in $rules) {
                    if ($rule.BlockAccess -eq $true) { $actionList += "BlockAccess" }
                    if ($rule.GenerateIncidentReport -ne $null) { $actionList += "IncidentReport" }
                    if ($rule.NotifyUser -ne $null) { $actionList += "NotifyUser" }
                }
                if ($actionList.Count -gt 0) {
                    # Dedup actions
                    $actions = ($actionList | Select-Object -Unique) -join ", "
                }
            }
            
            # Construct a combined custom object
            $policyObj = [PSCustomObject]@{
                Name = $policy.Name
                Comment = $policy.Comment
                Workload = $policy.Workload
                Mode = $policy.Mode
                State = $policy.State
                ExchangeLocation = $policy.ExchangeLocation
                SharePointLocation = $policy.SharePointLocation
                OneDriveLocation = $policy.OneDriveLocation
                DistributionStatus = $policy.DistributionStatus
                Enabled = $policy.Enabled
                Actions = $actions
                Identity = $policy.Identity.ToString()
                WhenCreated = $policy.WhenCreated
                WhenChanged = $policy.WhenChanged
                CreatedBy = $policy.CreatedBy
                LastModifiedBy = $policy.LastModifiedBy
            }
            $policies_output += $policyObj
        }
        
        $finalOutput = @{
            value = $policies_output
            Rules = $rules_output
        }
        $finalOutput | ConvertTo-Json -Depth 10
    } else {
        "[]"
    }
}
finally {
    Disconnect-ExchangeOnline -Confirm:$false -WarningAction SilentlyContinue
}
