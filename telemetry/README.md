# Telemetry Module

## Prerequisites

The certificate authentication flow and reports parsing require the following Python libraries:
* `msal`
* `cryptography`
* `pandas`

You can install them via pip:
```bash
pip install msal cryptography pandas
```


## Setup & Execution

### 1. Entra ID Permissions (Tenant-Wide Cloud Flows)

1. Navigate to the [Microsoft Entra ID Portal](https://www.google.com/search?q=https://entra.microsoft.com/) > **Roles and administrators**.
2. Assign the **Power Platform Administrator** role to your App Registration.

### 2. Dataverse Permissions (Desktop Flows)

*Perform this in each environment where you need to scan Desktop Flows:*

1. Navigate to the [Power Platform Admin Center](https://www.google.com/search?q=https://admin.powerplatform.microsoft.com/) > **Environments** > [Select Environment] > **Settings**.
2. Under **Users + permissions** > **Application users**, click **+ New app user**.
3. Add your App Registration and assign it the **System Administrator** role.

### 3. Certificate Setup (Hybrid Authentication)

The telemetry planner uses local certificate-based authentication for connecting securely to Microsoft APIs:

1. When running the Telemetry tool, it checks for a directory named `certificate` containing `passkey.pfx` at the root of `migration-planner`.
2. If this file does not exist, the app automatically generates a self-signed public certificate (`certificate.pem`) and an encrypted private key bundle (`passkey.pfx`) using the provided Client Secret as the password.
3. You will be prompted in the UI to upload `certificate.pem` to Microsoft Entra ID:
   - Navigate to the **Microsoft Entra ID Portal** > **App registrations** > [Select your Application].
   - Click **Certificates & secrets** > **Certificates** tab > **Upload certificate**.
   - Select and upload the generated `certificate.pem` file.
4. Click **Continue** in the application interface to complete the connection flow.

## Logging
Logs are appended to `telemetry/logs/power_automate_log.txt`.

## Troubleshooting
If you encounter a `400 Client Error: Bad Request` when querying the workflows endpoint, please verify:
1. The App is properly allowlisted in the Power Platform Admin Center.
2. The Environment URL is correct and accessible.