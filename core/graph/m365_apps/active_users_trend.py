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

"""O365 Active Users Trend telemetry data pipeline and processing logic."""

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

def process_active_user_counts(filepath: str):
    """Parses chronological usage data for plotting."""
    logger.info(f"Processing O365 Counts file: {os.path.basename(filepath)}")
    if not os.path.exists(filepath):
        logger.error(f"Error: Could not find the file {filepath} to process.")
        raise FileNotFoundError(f"Report file {os.path.basename(filepath)} not found.")

    headers = pd.read_csv(filepath, nrows=0).columns.tolist()
    expected = ["Report Date", "Office 365", "Exchange", "OneDrive", "SharePoint", "Teams"]
    cols = [c for c in expected if c in headers]
    dates = []
    office365 = []
    exchange = []
    onedrive = []
    sharepoint = []
    teams = []

    for chunk in pd.read_csv(filepath, usecols=cols, chunksize=10000, encoding="utf-8-sig"):
        if "Report Date" in chunk.columns:
            chunk = chunk.sort_values(by="Report Date").fillna(0)
            dates.extend(chunk["Report Date"].astype(str).tolist())
        else:
            dates.extend([""] * len(chunk))
        
        def extract_col(col_name):
            if col_name in chunk.columns:
                return pd.to_numeric(chunk[col_name], errors='coerce').fillna(0).astype(int).tolist()
            return [0] * len(chunk)

        office365.extend(extract_col("Office 365"))
        exchange.extend(extract_col("Exchange"))
        onedrive.extend(extract_col("OneDrive"))
        sharepoint.extend(extract_col("SharePoint"))
        teams.extend(extract_col("Teams"))

    logger.info("Successfully processed O365 active user counts data.")
    return {
        "dates": dates,
        "office365": office365,
        "exchange": exchange,
        "onedrive": onedrive,
        "sharepoint": sharepoint,
        "teams": teams
    }

def run_o365_trend_pipeline(client_id: str, client_secret: str, tenant_id: str):
    """Pipeline specifically for O365 Trend Data."""
    try:
        logger.info("Starting isolated O365 Trend Pipeline...")
        client, service = _get_reports_service(client_id, client_secret, tenant_id)
        
        script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
        telemetry_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(script_dir))), "telemetry")
        reports_dir = os.path.join(telemetry_dir, "reports", f"{tenant_id}_{client_id}")
        
        service.download_o365_active_user_counts(reports_dir)
        client.close()
        
        return process_active_user_counts(os.path.join(reports_dir, "Office365ActiveUserCounts(30d).csv"))
    except Exception as e:
        logger.error("O365 Trend pipeline failed.", exc_info=True)
        raise
