# Telemetry Module

## Setup & Execution

### 1. Entra ID Permissions (Tenant-Wide Cloud Flows)

1. Navigate to the [Microsoft Entra ID Portal](https://www.google.com/search?q=https://entra.microsoft.com/) > **Roles and administrators**.
2. Assign the **Power Platform Administrator** role to your App Registration.

### 2. Dataverse Permissions (Desktop Flows)

*Perform this in each environment where you need to scan Desktop Flows:*

1. Navigate to the [Power Platform Admin Center](https://www.google.com/search?q=https://admin.powerplatform.microsoft.com/) > **Environments** > [Select Environment] > **Settings**.
2. Under **Users + permissions** > **Application users**, click **+ New app user**.
3. Add your App Registration and assign it the **System Administrator** role.

## Logging
Logs are appended to `telemetry/logs/power_automate_log.txt`.

## Troubleshooting
If you encounter a `400 Client Error: Bad Request` when querying the workflows endpoint, please verify:
1. The App is properly allowlisted in the Power Platform Admin Center.
2. The Environment URL is correct and accessible.