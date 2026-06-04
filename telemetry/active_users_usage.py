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
import pandas as pd
import logging
from datetime import datetime, date

# Import unified core service layer
from core.graph.client import GraphClient
from core.graph.reports import ReportsService

# Bind to the async logger initialized in license_usage.py
usage_logger = logging.getLogger("LicenseUsageAsyncLogger")

def _get_reports_service(client_id, client_secret, tenant_id) -> tuple[GraphClient, ReportsService]:
    """Helper to instantiate GraphClient/ReportsService and manage credentials slots."""
    client = GraphClient(
        tenant_id=tenant_id,
        client_ids=client_id,
        client_secrets=client_secret,
        concurrency=2,
        retries=5,
        backoff=2
    )
    client.authenticate()
    return client, ReportsService(client)

def process_active_user_detail(filepath):
    """Streams the downloaded CSV and calculates usage counters over 30, 90, and 180 days."""
    usage_logger.info(f"Processing O365 file: {os.path.basename(filepath)}")
    
    if not os.path.exists(filepath):
        usage_logger.error(f"Error: Could not find the file {filepath} to process.")
        raise FileNotFoundError(f"Report file {os.path.basename(filepath)} not found. Download may have failed.")

    current_date = pd.Timestamp.today().normalize()

    df = pd.read_csv(filepath, encoding="utf-8-sig")

    def aggregate_column(df, has_license_col, date_col):
        if has_license_col not in df.columns or date_col not in df.columns:
            return [0, 0, 0]
        mask = df[has_license_col].astype(str).str.strip().str.upper() == "TRUE"
        dates_series = pd.to_datetime(df.loc[mask, date_col], errors='coerce')
        days_diff = (current_date - dates_series).dt.days
        d180 = int((days_diff < 180).sum())
        d90 = int((days_diff < 90).sum())
        d30 = int((days_diff < 30).sum())
        return [d30, d90, d180]

    exchange_online_usage = aggregate_column(df, "Has Exchange License", "Exchange Last Activity Date")
    onedrive_usage = aggregate_column(df, "Has OneDrive License", "OneDrive Last Activity Date")
    sharepoint_usage = aggregate_column(df, "Has SharePoint License", "SharePoint Last Activity Date")
    teams_usage = aggregate_column(df, "Has Teams License", "Teams Last Activity Date")

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

    df = pd.read_csv(filepath, encoding="utf-8-sig")
    
    if "Report Date" in df.columns:
        df = df.sort_values(by="Report Date").fillna(0)
    
    dates = df["Report Date"].astype(str).tolist() if "Report Date" in df.columns else []
    
    def get_column_list(col_name):
        if col_name in df.columns:
            return pd.to_numeric(df[col_name], errors='coerce').fillna(0).astype(int).tolist()
        return [0] * len(dates)

    office365 = get_column_list("Office 365")
    exchange = get_column_list("Exchange")
    onedrive = get_column_list("OneDrive")
    sharepoint = get_column_list("SharePoint")
    teams = get_column_list("Teams")

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
    
    df = pd.read_csv(filepath, encoding="utf-8-sig")
    
    counters = {}
    for col in columns_to_track:
        if col in df.columns:
            col_series = df[col].astype(str).str.strip().str.lower()
            count = int(col_series.isin(["yes", "true"]).sum())
            counters[col] = count
        else:
            counters[col] = 0
            
    usage_logger.info("Successfully processed M365 App user data.")
    return [(col, count) for col, count in counters.items()]

def run_o365_pipeline(client_id, client_secret, tenant_id):
    """Pipeline specifically for O365 Active User Data (Isolated for independent retries)."""
    usage_logger.info("Starting isolated O365 Pipeline...")
    client, service = _get_reports_service(client_id, client_secret, tenant_id)
    
    script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
    reports_dir = os.path.join(script_dir, "reports", f"{tenant_id}_{client_id}")
    
    service.download_o365_active_user_detail(reports_dir)
    client.close()
    
    return process_active_user_detail(os.path.join(reports_dir, "Office365ActiveUserDetail(180d).csv"))

def run_o365_trend_pipeline(client_id, client_secret, tenant_id):
    """Pipeline specifically for O365 Trend Data (Isolated for independent retries)."""
    try:
        usage_logger.info("Starting isolated O365 Trend Pipeline...")
        client, service = _get_reports_service(client_id, client_secret, tenant_id)
        
        script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
        reports_dir = os.path.join(script_dir, "reports", f"{tenant_id}_{client_id}")
        
        service.download_o365_active_user_counts(reports_dir)
        client.close()
        
        return process_active_user_counts(os.path.join(reports_dir, "Office365ActiveUserCounts(30d).csv"))
    except Exception as e:
        usage_logger.error("O365 Trend pipeline failed.", exc_info=True)
        raise

def run_m365_pipeline(client_id, client_secret, tenant_id):
    """Pipeline specifically for M365 Apps Data (Isolated for independent retries)."""
    usage_logger.info("Starting isolated M365 Apps Pipeline...")
    client, service = _get_reports_service(client_id, client_secret, tenant_id)
    
    script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
    reports_dir = os.path.join(script_dir, "reports", f"{tenant_id}_{client_id}")
    
    service.download_m365_app_details(reports_dir)
    client.close()
    
    return process_m365_app_user_detail(os.path.join(reports_dir, "M365AppUserDetail(180d).csv"))
