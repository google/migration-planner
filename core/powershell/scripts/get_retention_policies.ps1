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
    $policies = Get-RetentionCompliancePolicy
    $output = @()

    if ($policies) {
        # Retrieve all compliance rules at once
        $allRules = Get-RetentionComplianceRule
        $ruleMap = @{}
        if ($allRules) {
            foreach ($rule in @($allRules)) {
                if ($rule.Policy) {
                    $rawKey = $rule.Policy.ToString()
                    $ruleMap[$rawKey] = $rule
                    
                    # Extract the policy name if the reference is a DistinguishedName
                    if ($rawKey -match "CN=([^,]+)") {
                        $cnKey = $Matches[1]
                        $ruleMap[$cnKey] = $rule
                    }
                }
            }
        }

        # Handle case where $policies is not an array
        $policies_list = @($policies)
        
        foreach ($policy in $policies_list) {
            $duration = "N/A"
            $action = "N/A"
            $trigger = "N/A"
            
            $rule = $null
            if ($policy.Guid -and $ruleMap.ContainsKey($policy.Guid.ToString())) {
                $rule = $ruleMap[$policy.Guid.ToString()]
            } elseif ($ruleMap.ContainsKey($policy.Name)) {
                $rule = $ruleMap[$policy.Name]
            } elseif ($ruleMap.ContainsKey($policy.Identity.ToString())) {
                $rule = $ruleMap[$policy.Identity.ToString()]
            } elseif ($policy.Guid -and $ruleMap.ContainsKey($policy.Guid.ToString())) {
                $rule = $ruleMap[$policy.Guid.ToString()]
            }

            if ($rule) {
                $duration = $rule.RetentionDuration
                $action = $rule.RetentionAction
                $trigger = $rule.RetentionTrigger
            }
            
            # Construct a combined custom object
            $policyObj = [PSCustomObject]@{
                Name = $policy.Name
                Comment = $policy.Comment
                Workload = $policy.Workload
                Mode = $policy.Mode
                DistributionStatus = $policy.DistributionStatus
                Enabled = $policy.Enabled
                Duration = $duration
                RetentionAction = $action
                RetentionTrigger = $trigger
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
    Disconnect-ExchangeOnline -Confirm:$false
}
