# Telemetry Module

## Prerequisites

The certificate authentication flow, reports parsing, and user interface require the following Python libraries:
* `customtkinter`
* `requests`
* `pandas`
* `psutil`
* `matplotlib`
* `cryptography`
* `msal`
* `google-genai`
* `scikit-learn`

You can install them via pip:
```bash
pip install customtkinter requests pandas psutil matplotlib cryptography msal google-genai scikit-learn
```

## Architecture & Optimizations
For large tenant scopes (e.g., millions of records or 100K+ flows), this module utilizes aggressive disk-caching mechanisms out-of-the-box, ensuring the application remains lightweight on RAM:
- **SQLite UI Pagination**: Data grids are lazily fetched from `sqlite3` temp databases rather than hoarding UI components in Python lists.
- **Disk Streaming Pipelines**: Complex parsing arrays are continuously streamed to local `.jsonl` temp files and pushed natively to `pandas.DataFrame` chunking logic for exports.
- **Lazy Garbage Collection**: Core navigation state handles `gc.collect()` passively between UI tab cycles.


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

1. When running the Telemetry tool, it checks for a directory named `certificate/{tenantId}_{clientId}` containing `passkey.pfx` under the root of `migration-planner`.
2. If this file does not exist, the app automatically generates a self-signed public certificate (`certificate.pem`) and an encrypted private key bundle (`passkey.pfx`) under the dynamic `certificate/{tenantId}_{clientId}` directory using the provided Client Secret as the password.
3. You will be prompted in the UI to upload `certificate.pem` to Microsoft Entra ID:
   - Navigate to the **Microsoft Entra ID Portal** > **App registrations** > [Select your Application].
   - Click **Certificates & secrets** > **Certificates** tab > **Upload certificate**.
   - Select and upload the generated `certificate.pem` file.
4. Click **Continue** in the application interface to complete the connection flow.

### 4. Entra ID App & PowerShell Permissions (Calendar & Mailbox Telemetry)

For the core telemetry scanners (Calendar Telemetry, Active Users, Mailbox/SharePoint Usage, etc.) to query successfully:

#### A. Microsoft Graph API Permissions (Application Scopes)
Ensure the following **Application** API permissions are granted and admin-consented in your App Registration:
- `Place.Read.All`: Used to list meeting rooms and resource device counts.
- `User.Read.All`: Used to read user directory identities to aggregate settings.
- `Calendars.ReadBasic.All`: Used to audit organizational calendar permissions.
- `Reports.Read.All`: Used to retrieve active user trends and mailbox/SharePoint usage reports.
- `Directory.Read.All`: Used to read tenant organization configuration data.

#### B. Exchange Online PowerShell Roles
The certificate-based PowerShell client requires administrative roles to read Exchange policies (OWA, default apps, sharing policies). 
In the **Microsoft Entra ID Portal** > **Roles and administrators**, assign one of the following directory roles to your App Registration:
- **Global Reader** (Recommended, read-only)
- **Exchange Administrator**

## Logging
Logs are appended to `telemetry/logs/power_automate_log.txt`.

## Troubleshooting
If you encounter a `400 Client Error: Bad Request` when querying the workflows endpoint, please verify:
1. The App is properly allowlisted in the Power Platform Admin Center.
2. The Environment URL is correct and accessible.