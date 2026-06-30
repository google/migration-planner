# Deal Assistant - Flet Dashboard UI

This folder contains the Flet-based modern desktop dashboard application for the **Migration Planner Tool / Deal Assistant**. It provides an interactive, beautiful, web-like graphical user interface to collect and analyze Microsoft 365 tenant license adoption, compliance policies, active usage trends, and Power Automate flow telemetry.

---

## Key Features

- **Unified Credentials Connection (`AuthView`):** Enter your Tenant ID, Client ID, and Client Secret. 
- **Certificate-Based Auth Flow (`CertInstructionsView`):** Automatically detects if a security certificate is missing. Generates a new `certificate.pem` file locally and guides you on uploading it to the Microsoft Entra ID portal to enable secure Exchange Online and Retention Policy scans.
- **Interactive Reports Dashboard (`DashboardView`):**
  1. **Subscribed SKUs Inventory Summary:** Lists active license plans, pre-paid quantities, consumed units, and status. Includes a **CSV Export** button.
  2. **O365 Active Users Usage:** Shows active user metrics (30-day, 90-day, 180-day) for Exchange, OneDrive, SharePoint, and Teams.
  3. **O365 30-Day Active User Trend:** Renders a gorgeous visual line chart (using Matplotlib backend rendering) showing historical trends.
  4. **M365 App Usage (180 Days):** Platform/App distribution details for user endpoints.
  5. **Exchange Online Mailbox Usage Telemetry:** Details total mailboxes, collective size, average sizes, and email volumes.
  6. **SharePoint Site Usage Telemetry:** Details total sites, storage consumed, files stored, and percent active files.
  7. **OneDrive Usage Telemetry:** Highlights OneDrive accounts, usage levels, file synchronisation percentages, and active OneNote users.
  8. **Sensitivity Labels:** Displays configured sensitivity labels (with child hierarchies), protection details, priority, and application targets. Supports **Pagination** (Page 1 of N).
  9. **Retention Compliance Policies:** Displays tenant compliance rules, workloads, and duration metrics. Features a quick link to open Microsoft Purview.
  10. **Power Automate Flows:** Lists environment counts, flow types, and premium/custom connector usage. Includes a **CSV Export** button to download complex logic flows.
- **Granular Individual Card Retry/Refresh:** Each card features a Refresh (`ft.Icons.REFRESH`) button on the top-right. You can re-fetch telemetry for an individual section (e.g. just SharePoint or just SKUs) without having to trigger a full master scan of the entire tenant again.

---

## Prerequisites & Installation

To run the Flet application, you need to set up Python and install the required UI and backend libraries.

### 1. Python Environment
Make sure you have **Python 3.10** or newer installed. We highly recommend using a virtual environment:

```bash
# Create a virtual environment
python -m venv venv

# Activate the environment
# On macOS/Linux:
source venv/bin/activate
# On Windows (cmd):
.\venv\Scripts\activate
# On Windows (PowerShell):
.\venv\Scripts\Activate.ps1
```

### 2. Install Required Python Packages
With your virtual environment active, run:

```bash
pip install flet matplotlib pandas requests urllib3 aiohttp certifi psutil Pillow customtkinter
```

*Note: Flet does not require any additional web server setup. Matplotlib is used in headless mode (`matplotlib.use("Agg")`) to render the trend chart into the Flet UI natively.*

### 3. PowerShell Prerequisites (For Retention Policy Scan)
The Retention Compliance Policy scanner uses PowerShell Core and the Exchange Online module.

#### Install PowerShell Core (`pwsh`):
- **macOS (via Homebrew):**
  ```bash
  brew install powershell
  ```
- **Windows (via winget):**
  ```cmd
  winget install --id Microsoft.Powershell --source winget
  ```

#### Install the Exchange Online Module:
Open PowerShell (`pwsh`) and install the module:
```powershell
Install-Module -Name ExchangeOnlineManagement -Scope CurrentUser -Force
```

---

## How to Run

1. Open your terminal or command prompt and navigate to the project root directory (the parent of `flet_app/`):
   ```bash
   cd /path/to/project_root
   ```
2. Activate your virtual environment:
   ```bash
   source venv/bin/activate
   ```
3. Launch the Flet application:
   ```bash
   python flet_app/main.py
   ```
4. Enter your Azure Client/Tenant credentials to connect.
5. If requested, locate the auto-generated certificate (`certificate/certificate.pem`), upload it to the Azure portal under **App Registrations > Certificates & secrets**, and click **Continue** to load the dashboard.
