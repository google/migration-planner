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
    $output = @()

    if ($policies) {
        # Retrieve all compliance rules at once to map them
        $allRules = Get-DlpComplianceRule
        $ruleMap = @{}
        if ($allRules) {
            foreach ($rule in @($allRules)) {
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
                DistributionStatus = $policy.DistributionStatus
                Enabled = $policy.Enabled
                Actions = $actions
                Identity = $policy.Identity.ToString()
                WhenCreated = $policy.WhenCreated
                WhenChanged = $policy.WhenChanged
                CreatedBy = $policy.CreatedBy
                LastModifiedBy = $policy.LastModifiedBy
            }
            $output += $policyObj
        }
        $output | ConvertTo-Json -Depth 5
    } else {
        "[]"
    }
}
finally {
    Disconnect-ExchangeOnline -Confirm:$false -WarningAction SilentlyContinue
}
