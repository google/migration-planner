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

"""Exchange Online Supported Email Clients telemetry scanner data pipeline."""

import os
import logging
import pandas as pd

from core.graph.client import GraphClient
from core.graph.reports import ReportsService

logger = logging.getLogger(__name__)

def parse_email_client_support_csv(filepath: str) -> dict:
    """Parses the Email App Usage Counts CSV to categorize Browser vs Non-Browser client adoption."""
    logger.info(f"Processing Email App Usage Counts file: {os.path.basename(filepath)}")
    if not os.path.exists(filepath):
        logger.warning(f"Email App Usage report {filepath} not found. Skipping client breakdown.")
        return {}

    try:
        df = pd.read_csv(filepath)
        for col in df.columns:
            if col not in ["Report Refresh Date", "User Principal Name", "Display Name", "Last Activity Date", "Is Deleted"]:
                df[col] = df[col].astype(str).str.strip().str.upper().isin(["TRUE", "1", "UNDETERMINED"])
                
        if "Is Deleted" in df.columns:
            df = df[~df["Is Deleted"].astype(str).str.strip().str.upper().isin(["TRUE", "1"])]

        if df.empty:
            logger.warning(f"Email App Usage report {filepath} is empty.")
            return {}

        owa_users = int(df["Outlook For Web"].sum()) if "Outlook For Web" in df.columns else 0

        win_users = int(df["Outlook For Windows"].sum()) if "Outlook For Windows" in df.columns else 0
        mac_users = int(df["Outlook For Mac"].sum()) if "Outlook For Mac" in df.columns else 0
        mail_mac = int(df["Mail For Mac"].sum()) if "Mail For Mac" in df.columns else 0
        
        mobile_users = int(df["Outlook For Mobile"].sum()) if "Outlook For Mobile" in df.columns else 0
        other_mobile = int(df["Other For Mobile"].sum()) if "Other For Mobile" in df.columns else 0
        
        pop3_users = int(df["POP3 App"].sum()) if "POP3 App" in df.columns else 0
        imap4_users = int(df["IMAP4 App"].sum()) if "IMAP4 App" in df.columns else 0
        smtp_users = int(df["SMTP App"].sum()) if "SMTP App" in df.columns else 0

        total_desktop = win_users + mac_users + mail_mac
        total_mobile = mobile_users + other_mobile
        total_protocols = pop3_users + imap4_users + smtp_users
        total_non_browser = total_desktop + total_mobile + total_protocols

        return {
            "browser_users": owa_users,
            "desktop_win": win_users,
            "desktop_mac": mac_users,
            "desktop_mail_mac": mail_mac,
            "mobile_outlook": mobile_users,
            "mobile_other": other_mobile,
            "protocol_pop3": pop3_users,
            "protocol_imap4": imap4_users,
            "protocol_smtp": smtp_users,
            "total_desktop": total_desktop,
            "total_mobile": total_mobile,
            "total_protocols": total_protocols,
            "total_non_browser": total_non_browser
        }
    except Exception as e:
        logger.error(f"Error parsing Email App Usage CSV: {e}")
        return {}

def run_email_client_usage_pipeline(client_id: str, client_secret: str, tenant_id: str) -> dict:
    """Executes the pipeline to download and parse Supported Email Clients usage data."""
    client = GraphClient(tenant_id=tenant_id, client_ids=client_id, client_secrets=client_secret, concurrency=1, retries=3, backoff=2)
    client.authenticate()
    service = ReportsService(client)
    
    script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
    telemetry_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(script_dir))), "telemetry")
    reports_dir = os.path.join(telemetry_dir, "reports", f"{tenant_id}_{client_id}")
    
    client_stats = {}
    client_error = None
    try:
        service.download_email_app_usage_detail(reports_dir)
        client_stats = parse_email_client_support_csv(os.path.join(reports_dir, "EmailAppUsageUserDetail(180d).csv"))
    except Exception as e:
        client_error = str(e)
    client.close()
    return {"client_adoption": client_stats, "client_error": client_error}
