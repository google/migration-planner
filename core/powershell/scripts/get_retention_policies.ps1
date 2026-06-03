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
    Connect-IPPSSession -CertificateFilePath $CertificatePath -CertificatePassword $secPassword -AppId $AppId -Organization $Organization
} else {
    Connect-IPPSSession -CertificateFilePath $CertificatePath -AppId $AppId -Organization $Organization
}

try {
    # Retrieve policies
    $policies = Get-RetentionCompliancePolicy
    $output = @()

    if ($policies) {
        # Handle case where $policies is not an array
        $policies_list = @($policies)
        
        foreach ($policy in $policies_list) {
            # Get the rules associated with this policy
            $rules = Get-RetentionComplianceRule -Policy $policy.Name
            
            $duration = "N/A"
            $action = "N/A"
            $trigger = "N/A"
            if ($rules) {
                $rules_list = @($rules)
                if ($rules_list.Count -gt 0) {
                    $rule = $rules_list[0]
                    $duration = $rule.RetentionDuration
                    $action = $rule.RetentionAction
                    $trigger = $rule.RetentionTrigger
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
                Duration = $duration
                RetentionAction = $action
                RetentionTrigger = $trigger
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
