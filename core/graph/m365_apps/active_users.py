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

"""O365 Active Users telemetry data pipeline and processing logic."""

import os
import logging
import pandas as pd
from core.graph.client import GraphClient
from core.graph.reports import ReportsService

logger = logging.getLogger(__name__)

def _get_reports_service(client_id: str, client_secret: str, tenant_id: str) -> tuple[GraphClient, ReportsService]:
    """Helper to instantiate GraphClient/ReportsService."""
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

def process_active_user_detail(filepath: str):
    """Streams the downloaded CSV and calculates usage counters over 30, 90, and 180 days."""
    logger.info(f"Processing O365 file: {os.path.basename(filepath)}")
    
    if not os.path.exists(filepath):
        logger.error(f"Error: Could not find the file {filepath} to process.")
        raise FileNotFoundError(f"Report file {os.path.basename(filepath)} not found. Download may have failed.")

    current_date = pd.Timestamp.today().normalize()

    headers = pd.read_csv(filepath, nrows=0).columns.tolist()
    expected = [
        "Has Exchange License", "Exchange Last Activity Date",
        "Has OneDrive License", "OneDrive Last Activity Date",
        "Has SharePoint License", "SharePoint Last Activity Date",
        "Has Teams License", "Teams Last Activity Date"
    ]
    cols = [c for c in expected if c in headers]

    exchange_online_usage = [0, 0, 0]
    onedrive_usage = [0, 0, 0]
    sharepoint_usage = [0, 0, 0]
    teams_usage = [0, 0, 0]

    def process_chunk_col(chunk, has_license_col, date_col):
        if has_license_col not in chunk.columns or date_col not in chunk.columns:
            return [0, 0, 0]
        mask = chunk[has_license_col].astype(str).str.strip().str.upper() == "TRUE"
        dates_series = pd.to_datetime(chunk.loc[mask, date_col], errors='coerce')
        days_diff = (current_date - dates_series).dt.days
        d180 = int((days_diff < 180).sum())
        d90 = int((days_diff < 90).sum())
        d30 = int((days_diff < 30).sum())
        return [d30, d90, d180]

    for chunk in pd.read_csv(filepath, usecols=cols, chunksize=10000, encoding="utf-8-sig"):
        e_chunk = process_chunk_col(chunk, "Has Exchange License", "Exchange Last Activity Date")
        exchange_online_usage = [x + y for x, y in zip(exchange_online_usage, e_chunk)]

        od_chunk = process_chunk_col(chunk, "Has OneDrive License", "OneDrive Last Activity Date")
        onedrive_usage = [x + y for x, y in zip(onedrive_usage, od_chunk)]

        sp_chunk = process_chunk_col(chunk, "Has SharePoint License", "SharePoint Last Activity Date")
        sharepoint_usage = [x + y for x, y in zip(sharepoint_usage, sp_chunk)]

        t_chunk = process_chunk_col(chunk, "Has Teams License", "Teams Last Activity Date")
        teams_usage = [x + y for x, y in zip(teams_usage, t_chunk)]

    logger.info("Successfully processed O365 active user data in chunks.")
    return [
        ("Exchange Online", exchange_online_usage[0], exchange_online_usage[1], exchange_online_usage[2]),
        ("OneDrive", onedrive_usage[0], onedrive_usage[1], onedrive_usage[2]),
        ("SharePoint", sharepoint_usage[0], sharepoint_usage[1], sharepoint_usage[2]),
        ("Teams", teams_usage[0], teams_usage[1], teams_usage[2])
    ]

def run_o365_pipeline(client_id: str, client_secret: str, tenant_id: str):
    """Pipeline specifically for O365 Active User Data."""
    logger.info("Starting isolated O365 Pipeline...")
    client, service = _get_reports_service(client_id, client_secret, tenant_id)
    
    script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
    # Go up from core/graph/m365_apps to telemetry folder reports
    telemetry_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(script_dir))), "telemetry")
    reports_dir = os.path.join(telemetry_dir, "reports", f"{tenant_id}_{client_id}")
    
    service.download_o365_active_user_detail(reports_dir)
    client.close()
    
    return process_active_user_detail(os.path.join(reports_dir, "Office365ActiveUserDetail(180d).csv"))
