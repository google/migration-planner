 <#    
.SYNOPSIS    
Automates the creation of a Single-Tenant Entra ID App for Workspace Migration.    
Strictly forces account selection and verifies specific Admin roles.    
#>

# Check if the module is missing
if (-not (Get-Module -ListAvailable -Name Microsoft.Graph.Authentication)) {
    Write-Host "Microsoft Graph module is NOT installed." -ForegroundColor Yellow
    $UserResponse = Read-Host "Would you like to try installing Microsoft Graph? (Y/N)"

    if ($UserResponse -ieq "Y") {
        try {
            # Use only native cmdlets, no .NET property setting
            Install-Module -Name Microsoft.Graph -Scope CurrentUser -Force -AllowClobber
            Write-Host "Installation complete!" -ForegroundColor Green
        }
        catch {
            Write-Error "Policy is blocking installation. Please contact IT to install Microsoft.Graph module."
            Read-Host "Press Enter to exit"; exit
        }
    }
    else {
        exit
    }
} else {
    Write-Host "Microsoft Graph modules detected. Proceeding..." -ForegroundColor Green
}

# --- STEP 0: THE "DEEP" LOGOUT ---
Write-Host "Forcing session cleanup..." -ForegroundColor Gray
Disconnect-MgGraph -ErrorAction SilentlyContinue

# Force clear the local token cache folder if it exists
$CachePath = "$env:USERPROFILE\.mg"
if (Test-Path $CachePath) {
    try { Remove-Item $CachePath -Recurse -Force -ErrorAction SilentlyContinue } catch {}
}

Write-Host "Opening Microsoft Login... (Please select the correct account)" -ForegroundColor Cyan

$RequiredScopes = @(
    "Application.ReadWrite.All", 
    "AppRoleAssignment.ReadWrite.All", 
    "Directory.Read.All", 
    "RoleManagement.ReadWrite.Directory",
    "DelegatedPermissionGrant.ReadWrite.All"
)

try {
    # In v2, -ContextScope Process is the most reliable way to force account selection
    # and prevent the session from saving to the machine permanently.
    Connect-MgGraph -Scopes $RequiredScopes -ContextScope Process

    $Context = Get-MgContext
    if ($null -eq $Context) { throw "Login was cancelled or failed." }

    $UserPrincipal = $Context.Account
    Write-Host "Logged in as: $UserPrincipal" -ForegroundColor Green

    # --- ROLE VALIDATION ---
    Write-Host "Verifying Directory Roles..." -ForegroundColor Gray
    $UserRoles = Get-MgUserMemberOf -UserId $Context.Account -All | Where-Object { $_.AdditionalProperties.displayName -ne $null }
    
    $Authorized = $false
    $RequiredRoles = @("Global Administrator", "Privileged Role Administrator")

    foreach ($role in $UserRoles) {
        $roleName = $role.AdditionalProperties.displayName
        if ($roleName -in $RequiredRoles) {
            $Authorized = $true
            Write-Host "Access Granted: $roleName" -ForegroundColor Green
            break
        }
    }

    if (-not $Authorized) {
        Write-Host "`nCRITICAL ERROR: Insufficient Privileges." -ForegroundColor Red
        Write-Host "Account must be 'Global Administrator' or 'Privileged Role Administrator'." -ForegroundColor Yellow
        Disconnect-MgGraph
        Read-Host "`nPress Enter to exit"; exit
    }

} catch {
    Write-Error "Login failed: $_"
    Read-Host "Press Enter to exit"; exit
}

# --- USER INPUT ---
Write-Host "`n--- APPLICATION SETUP ---" -ForegroundColor Cyan
$InputName = Read-Host "Enter the name for your new Entra ID Application (Default: Workspace Migration App)"
$AppName = if ([string]::IsNullOrWhiteSpace($InputName)) { "Workspace Migration App" } else { $InputName }

# --- CONFIGURATION ---
$GraphAppRoles = @(
    "Reports.Read.All", "Directory.Read.All", "Policy.Read.All", "NetworkAccess.Read.All",
    "DeviceManagementConfiguration.Read.All", "DeviceManagementServiceConfig.Read.All",
    "DeviceManagementApps.Read.All", "DeviceManagementManagedDevices.Read.All",
    "Organization.Read.All", "Place.Read.All", "Calendars.ReadBasic.All", "Sites.Read.All",
    "AuditLog.Read.All", "SensitivityLabels.Read.All", "Application.Read.All", "User.Read.All",
    "Group.Read.All", "Mail.Read", "Contacts.Read", "Calendars.Read", "MailboxFolder.Read.All",
    "MailboxSettings.Read", "Chat.Read.All", "ChannelMessage.Read.All", "ChannelSettings.Read.All",
    "TeamsActivity.Read.All", "TeamMember.Read.All", "Files.Read.All", "LicenseAssignment.Read.All"
)

$GraphDelegatedScopes = @(
    "eDiscovery.Read.All", "Policy.Read.All", "offline_access"
)

$ExchangeAppRoles = @(
    "Exchange.ManageAsApp", "Exchange.ManageAsAppV2"
)

$TenantId = $Context.TenantId
$UserConsentedRoles = $false

try {
    # --- STEP 1: REGISTER APPLICATION ---
    Write-Host "Creating Application: $AppName..." -ForegroundColor Cyan
    $Application = New-MgApplication -BodyParameter @{
        displayName = $AppName
        signInAudience = "AzureADMyOrg"
        web = @{
            redirectUris = @("http://localhost")
        }
    }
    
    # --- STEP 2: PREPARE SERVICE PRINCIPAL ---
    $NewServicePrincipal = New-MgServicePrincipal -BodyParameter @{ appId = $Application.AppId }

    Write-Host "Waiting 10 seconds for replication..." -ForegroundColor DarkGray
    Start-Sleep -Seconds 10

    # --- STEP 3: CONFIGURE & GRANT PERMISSIONS ---
    Write-Host "Configuring API Permissions & Granting Admin Consent..." -ForegroundColor Cyan
    
    $AllRequiredResourceAccess = @()

    # 1. Graph API Permissions
    $GraphSP = Get-MgServicePrincipal -Filter "AppId eq '00000003-0000-0000-c000-000000000000'" | Select-Object -First 1
    $GraphResourceAccess = @()

    foreach ($RoleName in $GraphAppRoles) {
        $Role = $GraphSP.AppRoles | Where-Object { $_.Value -eq $RoleName }
        if ($Role) {
            $GraphResourceAccess += @{ id = $Role.Id; type = "Role" }
            New-MgServicePrincipalAppRoleAssignment -ServicePrincipalId $NewServicePrincipal.Id -BodyParameter @{
                principalId = $NewServicePrincipal.Id; resourceId = $GraphSP.Id; appRoleId = $Role.Id
            } | Out-Null
            Write-Host " - Granted App Role (Graph): $RoleName" -ForegroundColor Gray
        }
    }

    foreach ($ScopeName in $GraphDelegatedScopes) {
        $Scope = $GraphSP.Oauth2PermissionScopes | Where-Object { $_.Value -eq $ScopeName }
        if ($Scope) {
            $GraphResourceAccess += @{ id = $Scope.Id; type = "Scope" }
            Write-Host " - Added Delegated Scope (Graph): $ScopeName" -ForegroundColor Gray
        }
    }

    if ($GraphResourceAccess.Count -gt 0) {
        $AllRequiredResourceAccess += @{ resourceAppId = "00000003-0000-0000-c000-000000000000"; resourceAccess = $GraphResourceAccess }
    }

    # 2. Exchange Online API Permissions
    $ExchangeSP = Get-MgServicePrincipal -Filter "AppId eq '00000002-0000-0ff1-ce00-000000000000'" | Select-Object -First 1
    $ExchangeResourceAccess = @()

    if ($ExchangeSP) {
        foreach ($RoleName in $ExchangeAppRoles) {
            $Role = $ExchangeSP.AppRoles | Where-Object { $_.Value -eq $RoleName }
            if ($Role) {
                $ExchangeResourceAccess += @{ id = $Role.Id; type = "Role" }
                New-MgServicePrincipalAppRoleAssignment -ServicePrincipalId $NewServicePrincipal.Id -BodyParameter @{
                    principalId = $NewServicePrincipal.Id; resourceId = $ExchangeSP.Id; appRoleId = $Role.Id
                } | Out-Null
                Write-Host " - Granted App Role (Exchange): $RoleName" -ForegroundColor Gray
            }
        }
        if ($ExchangeResourceAccess.Count -gt 0) {
            $AllRequiredResourceAccess += @{ resourceAppId = "00000002-0000-0ff1-ce00-000000000000"; resourceAccess = $ExchangeResourceAccess }
        }
    } else {
        Write-Host " - Warning: Exchange Online Service Principal not found. Skipping Exchange permissions." -ForegroundColor Yellow
    }

    # Update the Application Registration with all configured scopes and roles
    Update-MgApplication -ApplicationId $Application.Id -RequiredResourceAccess $AllRequiredResourceAccess

    # 3. Grant Admin Consent for Delegated Scopes
    if ($GraphDelegatedScopes.Count -gt 0) {
        $ScopeString = $GraphDelegatedScopes -join " "
        New-MgOauth2PermissionGrant -BodyParameter @{
            clientId = $NewServicePrincipal.Id
            consentType = "AllPrincipals"
            resourceId = $GraphSP.Id
            scope = $ScopeString
        } | Out-Null
        Write-Host " - Admin Consent Granted for Delegated Scopes" -ForegroundColor Gray
    }

    # --- STEP 4: CREATE CLIENT SECRET ---
    Write-Host "Generating Client Secret..." -ForegroundColor Cyan
    $ExpiryDate = (Get-Date).AddYears(2).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
    $PasswordCred = Add-MgApplicationPassword -ApplicationId $Application.Id -BodyParameter @{
        passwordCredential = @{
            displayName = "MigrationToolSecret"
            endDateTime = $ExpiryDate
        }
    }

    # --- OUTPUT ---
    Write-Host "`n-------------------------------------------------------" -ForegroundColor Yellow
    Write-Host "        SETUP COMPLETE - SAVE THESE DETAILS" -ForegroundColor Yellow
    Write-Host "-------------------------------------------------------" -ForegroundColor Yellow
    Write-Host "Application Name        : $AppName"
    Write-Host "Application (Client) ID : $($Application.AppId)"
    Write-Host "Client Secret Value     : $($PasswordCred.SecretText)"
    Write-Host "Directory (Tenant) ID   : $TenantId"
    Write-Warning "IMPORTANT: Copy the Client Secret Value immediately."

    # --- STEP 5: DIRECTORY ROLE ASSIGNMENTS ---
    Write-Host "`n--- ENTRA ID DIRECTORY ROLE ASSIGNMENTS ---" -ForegroundColor Cyan
    $PromptRoles = Read-Host "Would you like to assign Directory Roles (Compliance Data Administrator, Compliance Administrator, Power Platform Administrator) to this App? (Y/N)"
    
    $UserConsentedRoles = ($PromptRoles -ieq "Y")

    if ($UserConsentedRoles) {
        Write-Host "Assigning Directory Roles to App Service Principal..." -ForegroundColor Cyan
        $AppDirectoryRoles = @(
            "Compliance Data Administrator",
            "Compliance Administrator"
        )

        foreach ($RoleName in $AppDirectoryRoles) {
            try {
                $Role = Get-MgDirectoryRole -Filter "displayName eq '$RoleName'"
                if (-not $Role) {
                    $RoleTemplate = Get-MgDirectoryRoleTemplate -Filter "displayName eq '$RoleName'"
                    if ($RoleTemplate) {
                        $Role = New-MgDirectoryRole -RoleTemplateId $RoleTemplate.Id
                    }
                }
                if ($Role) {
                    New-MgDirectoryRoleMemberByRef -DirectoryRoleId $Role.Id -OdataId "https://graph.microsoft.com/v1.0/directoryObjects/$($NewServicePrincipal.Id)" | Out-Null
                    Write-Host " - Assigned Directory Role: $RoleName" -ForegroundColor Gray
                } else {
                    Write-Warning "Could not find directory role or template: $RoleName"
                }
            } catch {
                Write-Warning "Failed to assign directory role '$RoleName': $_"
            }
        }
    } else {
        Write-Host "Skipping Directory Role assignment." -ForegroundColor Yellow
    }

}
catch {
    Write-Error "Operation failed: $_"
}

# --- STEP 6: POWER PLATFORM MANAGEMENT APP (OPTIONAL) ---
Write-Host "`n--- POWER AUTOMATE & DATAVERSE CONFIGURATION ---" -ForegroundColor Cyan
$PromptPower = Read-Host "Would you like to register this app for Power Automate telemetry? (Y/N)"
if ($PromptPower -ieq "Y") {
    try {
        Write-Host "Installing PowerApps Administration Module..." -ForegroundColor Gray
        Install-Module -Name Microsoft.PowerApps.Administration.PowerShell -Scope CurrentUser -AllowClobber -Force -ErrorAction Stop
        
        Write-Host "Logging into PowerApps... (This may open a browser window)" -ForegroundColor Gray
        Add-PowerAppsAccount -Endpoint prod -TenantID $TenantId
        
        Write-Host "Registering App as Management App..." -ForegroundColor Gray
        New-PowerAppManagementApp -ApplicationId $Application.AppId -ErrorAction Stop
        Write-Host "Power Automate Management App Registration Complete!" -ForegroundColor Green
    } catch {
        Write-Warning "Power Automate registration failed: $_"
        Write-Host "You can safely ignore this error. The main App Registration was created successfully." -ForegroundColor Yellow
        Write-Host "To retry later, follow the Power Platform steps in the README." -ForegroundColor Yellow
    }
}

# --- FINAL DISCONNECT ---
Disconnect-MgGraph
Read-Host "`nPress Enter to close this window"
    