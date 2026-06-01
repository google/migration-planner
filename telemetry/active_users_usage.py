# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Aggregations and data pipelines for O365 active user counts and details."""

import os
import csv
import logging
from datetime import datetime, date

# Import unified core service layer
from core.graph.client import GraphClient
from core.graph.reports import ReportsService

# Bind to the async logger initialized in license_usage.py
usage_logger = logging.getLogger("LicenseUsageAsyncLogger")

def _download_reports_via_service(client_id, client_secret, tenant_id, reports) -> None:
    """Helper to instantiate GraphClient/ReportsService and perform concurrent downloads."""
    client = GraphClient(
        tenant_id=tenant_id,
        client_ids=client_id,
        client_secrets=client_secret,
        concurrency=len(reports),
        retries=5,
        backoff=2
    )
    service = ReportsService(client)
    
    script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
    reports_dir = os.path.join(script_dir, "reports")
    
    service.download_reports_batch(reports, reports_dir)
    client.close()

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
    """Streams the downloaded CSV and calculates usage counters."""
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
    reports = [
        ("https://graph.microsoft.com/v1.0/reports/getOffice365ActiveUserDetail(period='D180')", "Office365ActiveUserDetail(180d).csv")
    ]
    _download_reports_via_service(client_id, client_secret, tenant_id, reports)
    
    script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
    reports_dir = os.path.join(script_dir, "reports")
    return process_active_user_detail(os.path.join(reports_dir, "Office365ActiveUserDetail(180d).csv"))

def run_o365_trend_pipeline(client_id, client_secret, tenant_id):
    """Pipeline specifically for O365 Trend Data (Isolated for independent retries)."""
    try:
        usage_logger.info("Starting isolated O365 Trend Pipeline...")
        reports = [
            ("https://graph.microsoft.com/v1.0/reports/getOffice365ActiveUserCounts(period='D30')", "Office365ActiveUserCounts(30d).csv")
        ]
        _download_reports_via_service(client_id, client_secret, tenant_id, reports)
        
        script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
        reports_dir = os.path.join(script_dir, "reports")
        return process_active_user_counts(os.path.join(reports_dir, "Office365ActiveUserCounts(30d).csv"))
    except Exception as e:
        usage_logger.error("O365 Trend pipeline failed.", exc_info=True)
        raise

def run_m365_pipeline(client_id, client_secret, tenant_id):
    """Pipeline specifically for M365 Apps Data (Isolated for independent retries)."""
    usage_logger.info("Starting isolated M365 Apps Pipeline...")
    reports = [
        ("https://graph.microsoft.com/v1.0/reports/getM365AppUserDetail(period='D180')", "M365AppUserDetail(180d).csv"),
        ("https://graph.microsoft.com/v1.0/reports/getM365AppUserCounts(period='D180')", "getM365AppUserCounts(180d).csv")
    ]
    _download_reports_via_service(client_id, client_secret, tenant_id, reports)
    
    script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
    reports_dir = os.path.join(script_dir, "reports")
    return process_m365_app_user_detail(os.path.join(reports_dir, "M365AppUserDetail(180d).csv"))
