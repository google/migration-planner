import os
import requests
import concurrent.futures
import time
import csv
import logging
from datetime import datetime, date

# Bind to the async logger initialized in license_usage.py
usage_logger = logging.getLogger("LicenseUsageAsyncLogger")

def get_access_token(client_id, client_secret, tenant_id):
    """Fetches the OAuth 2.0 Access Token from Microsoft Entra ID."""
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    token_data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://graph.microsoft.com/.default"
    }
    
    usage_logger.info("Fetching OAuth token from Microsoft Entra ID for usage reports...")
    token_response = requests.post(token_url, data=token_data)
    token_response.raise_for_status()
    usage_logger.info("Successfully fetched OAuth token for usage reports.")
    return token_response.json().get("access_token")

def download_report(api_url, access_token, output_filename):
    """Calls the Graph API and downloads the CSV report to the 'reports' folder using a streaming download."""
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    
    usage_logger.info(f"Calling API for {output_filename} (intercepting redirect)...")
    report_response = requests.get(api_url, headers=headers, allow_redirects=False, stream=True)
    
    script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
    reports_dir = os.path.join(script_dir, "reports")
    os.makedirs(reports_dir, exist_ok=True)
    
    output_path = os.path.join(reports_dir, output_filename)
    
    if report_response.status_code == 302:
        report_response.close()
        location_url = report_response.headers.get("Location")
        if not location_url:
            usage_logger.error(f"Received a 302 status for {output_filename} but no Location header was found.")
            raise Exception(f"Received a 302 status for {output_filename} but no Location header was found.")
            
        usage_logger.info(f"Pre-authenticated URL retrieved for {output_filename}. Waiting 2 seconds before starting download...")
        time.sleep(2)
        
        max_retries = 5
        retry_interval = 30
        
        for attempt in range(1, max_retries + 1):
            try:
                usage_logger.info(f"[Attempt {attempt}/{max_retries}] Downloading {output_filename}...")
                with requests.get(location_url, stream=True) as csv_response:
                    csv_response.raise_for_status()
                    
                    if os.path.exists(output_path):
                        usage_logger.info(f"Notice: '{output_filename}' already exists in 'reports'. Overwriting...")
                        
                    with open(output_path, "wb") as f:
                        for chunk in csv_response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                break
            except requests.exceptions.RequestException as e:
                if attempt < max_retries:
                    usage_logger.warning(f"[Attempt {attempt}/{max_retries}] Failed to download {output_filename}. Retrying in {retry_interval}s... (Error: {e})")
                    time.sleep(retry_interval)
                else:
                    usage_logger.error(f"Failed to download {output_filename} after {max_retries} attempts.", exc_info=True)
                    raise Exception(f"Failed to download {output_filename} after {max_retries} attempts. Last error: {e}")
                    
        usage_logger.info(f"Success! Saved usage report to: {output_path}")
        
    elif report_response.status_code == 200:
        usage_logger.info(f"Unexpected 200 OK for {output_filename}. Saving payload directly via stream...")
        if os.path.exists(output_path):
            usage_logger.info(f"Notice: '{output_filename}' already exists in 'reports'. Overwriting...")
            
        with open(output_path, "wb") as f:
            for chunk in report_response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        report_response.close()
        usage_logger.info(f"Success! Saved usage report to: {output_path}")
    else:
        usage_logger.error(f"Error fetching {output_filename}. Status Code: {report_response.status_code}")
        report_response.close()
        raise Exception(f"API Error {report_response.status_code} fetching {output_filename}")

def _download_reports_batch(access_token, reports_to_download):
    """Helper to download a specific list of reports in parallel."""
    usage_logger.info(f"Starting parallel downloads for {len(reports_to_download)} reports...")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(reports_to_download)) as executor:
        futures = [executor.submit(download_report, url, access_token, filename) for url, filename in reports_to_download]
        for future in concurrent.futures.as_completed(futures):
            future.result()

def process_active_user_detail(filepath):
    """Streams the downloaded CSV and calculates usage counters over 30, 90, and 180 days."""
    usage_logger.info(f"Processing O365 file: {os.path.basename(filepath)}")
    
    if not os.path.exists(filepath):
        usage_logger.error(f"Error: Could not find the file {filepath} to process.")
        raise FileNotFoundError(f"Report file {os.path.basename(filepath)} not found. Download may have failed.")

    exchange_online_usage = [0, 0, 0] # 30d, 90d, 180d
    onedrive_usage = [0, 0, 0]
    sharepoint_usage = [0, 0, 0]
    teams_usage = [0, 0, 0]
    current_date = date.today()

    with open(filepath, mode="r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Exchange Online
            if row.get("Has Exchange License", "").strip().upper() == "TRUE":
                dt_str = row.get("Exchange Last Activity Date", "").strip()
                if dt_str:
                    try:
                        days_diff = (current_date - datetime.strptime(dt_str, "%Y-%m-%d").date()).days
                        if days_diff < 180: exchange_online_usage[2] += 1
                        if days_diff < 90: exchange_online_usage[1] += 1
                        if days_diff < 30: exchange_online_usage[0] += 1
                    except ValueError: pass

            # OneDrive
            if row.get("Has OneDrive License", "").strip().upper() == "TRUE":
                dt_str = row.get("OneDrive Last Activity Date", "").strip()
                if dt_str:
                    try:
                        days_diff = (current_date - datetime.strptime(dt_str, "%Y-%m-%d").date()).days
                        if days_diff < 180: onedrive_usage[2] += 1
                        if days_diff < 90: onedrive_usage[1] += 1
                        if days_diff < 30: onedrive_usage[0] += 1
                    except ValueError: pass

            # SharePoint
            if row.get("Has SharePoint License", "").strip().upper() == "TRUE":
                dt_str = row.get("SharePoint Last Activity Date", "").strip()
                if dt_str:
                    try:
                        days_diff = (current_date - datetime.strptime(dt_str, "%Y-%m-%d").date()).days
                        if days_diff < 180: sharepoint_usage[2] += 1
                        if days_diff < 90: sharepoint_usage[1] += 1
                        if days_diff < 30: sharepoint_usage[0] += 1
                    except ValueError: pass

            # Teams
            if row.get("Has Teams License", "").strip().upper() == "TRUE":
                dt_str = row.get("Teams Last Activity Date", "").strip()
                if dt_str:
                    try:
                        days_diff = (current_date - datetime.strptime(dt_str, "%Y-%m-%d").date()).days
                        if days_diff < 180: teams_usage[2] += 1
                        if days_diff < 90: teams_usage[1] += 1
                        if days_diff < 30: teams_usage[0] += 1
                    except ValueError: pass
                    
    usage_logger.info("Successfully processed O365 active user data.")
    return [
        ("Exchange Online", exchange_online_usage[0], exchange_online_usage[1], exchange_online_usage[2]),
        ("OneDrive", onedrive_usage[0], onedrive_usage[1], onedrive_usage[2]),
        ("SharePoint", sharepoint_usage[0], sharepoint_usage[1], sharepoint_usage[2]),
        ("Teams", teams_usage[0], teams_usage[1], teams_usage[2])
    ]

def process_active_user_counts(filepath):
    """Parses chronological usage data for plotting."""
    usage_logger.info(f"Processing O365 Counts file: {os.path.basename(filepath)}")
    if not os.path.exists(filepath):
        usage_logger.error(f"Error: Could not find the file {filepath} to process.")
        raise FileNotFoundError(f"Report file {os.path.basename(filepath)} not found.")

    dates, office365, exchange, onedrive, sharepoint, teams = [], [], [], [], [], []

    with open(filepath, mode="r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        # Sort chronologically by "Report Date" to ensure line graph renders left-to-right correctly
        rows = sorted(list(reader), key=lambda x: x.get("Report Date", ""))
        
        for row in rows:
            dt = row.get("Report Date", "").strip()
            if not dt: continue
            
            dates.append(dt)
            
            def get_int(val):
                try: return int(val) if val else 0
                except ValueError: return 0
            
            office365.append(get_int(row.get("Office 365")))
            exchange.append(get_int(row.get("Exchange")))
            onedrive.append(get_int(row.get("OneDrive")))
            sharepoint.append(get_int(row.get("SharePoint")))
            teams.append(get_int(row.get("Teams")))

    usage_logger.info("Successfully processed O365 active user counts data.")
    return {
        "dates": dates,
        "office365": office365,
        "exchange": exchange,
        "onedrive": onedrive,
        "sharepoint": sharepoint,
        "teams": teams
    }

def process_m365_app_user_detail(filepath):
    """Streams the downloaded M365 App User Detail CSV and calculates usage counters."""
    usage_logger.info(f"Processing M365 App file: {os.path.basename(filepath)}")
    
    if not os.path.exists(filepath):
        usage_logger.error(f"Error: Could not find the file {filepath} to process.")
        raise FileNotFoundError(f"Report file {os.path.basename(filepath)} not found. Download may have failed.")

    columns_to_track = [
        "Windows", "Mac", "Mobile", "Web", "Outlook", "Word", "Excel", 
        "PowerPoint", "OneNote", "Teams", "Outlook (Windows)", "Word (Windows)", 
        "Excel (Windows)", "PowerPoint (Windows)", "OneNote (Windows)", 
        "Teams (Windows)", "Outlook (Mac)", "Word (Mac)", "Excel (Mac)", 
        "PowerPoint (Mac)", "OneNote (Mac)", "Teams (Mac)", "Outlook (Mobile)", 
        "Word (Mobile)", "Excel (Mobile)", "PowerPoint (Mobile)", 
        "OneNote (Mobile)", "Teams (Mobile)", "Outlook (Web)", "Word (Web)", 
        "Excel (Web)", "PowerPoint (Web)", "OneNote (Web)", "Teams (Web)"
    ]
    
    counters = {col: 0 for col in columns_to_track}

    with open(filepath, mode="r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            for col in columns_to_track:
                val = row.get(col, "").strip().lower()
                if val in ["yes", "true"]:
                    counters[col] += 1
                    
    usage_logger.info("Successfully processed M365 App user data.")
    return [(col, count) for col, count in counters.items()]

def run_o365_pipeline(client_id, client_secret, tenant_id):
    """Pipeline specifically for O365 Active User Data (Isolated for independent retries)."""
    usage_logger.info("Starting isolated O365 Pipeline...")
    access_token = get_access_token(client_id, client_secret, tenant_id)
    
    reports = [
        ("https://graph.microsoft.com/v1.0/reports/getOffice365ActiveUserDetail(period='D180')", "Office365ActiveUserDetail(180d).csv")
    ]
    
    _download_reports_batch(access_token, reports)
    
    script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
    reports_dir = os.path.join(script_dir, "reports")
    
    return process_active_user_detail(os.path.join(reports_dir, "Office365ActiveUserDetail(180d).csv"))

def run_o365_trend_pipeline(client_id, client_secret, tenant_id):
    """Pipeline specifically for O365 Trend Data (Isolated for independent retries)."""
    try:
        usage_logger.info("Starting isolated O365 Trend Pipeline...")
        access_token = get_access_token(client_id, client_secret, tenant_id)
        reports = [
            ("https://graph.microsoft.com/v1.0/reports/getOffice365ActiveUserCounts(period='D30')", "Office365ActiveUserCounts(30d).csv")
        ]
        _download_reports_batch(access_token, reports)
        
        script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
        reports_dir = os.path.join(script_dir, "reports")
        
        return process_active_user_counts(os.path.join(reports_dir, "Office365ActiveUserCounts(30d).csv"))
    except Exception as e:
        usage_logger.error("O365 Trend pipeline failed.", exc_info=True)
        raise

def run_m365_pipeline(client_id, client_secret, tenant_id):
    """Pipeline specifically for M365 Apps Data (Isolated for independent retries)."""
    usage_logger.info("Starting isolated M365 Apps Pipeline...")
    access_token = get_access_token(client_id, client_secret, tenant_id)
    
    reports = [
        ("https://graph.microsoft.com/v1.0/reports/getM365AppUserDetail(period='D180')", "M365AppUserDetail(180d).csv"),
        ("https://graph.microsoft.com/v1.0/reports/getM365AppUserCounts(period='D180')", "getM365AppUserCounts(180d).csv")
    ]
    
    _download_reports_batch(access_token, reports)
    
    script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
    reports_dir = os.path.join(script_dir, "reports")
    
    return process_m365_app_user_detail(os.path.join(reports_dir, "M365AppUserDetail(180d).csv"))
