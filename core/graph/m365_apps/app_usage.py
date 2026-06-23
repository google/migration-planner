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

"""M365 App Usage telemetry data pipeline and processing logic."""

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

def process_m365_app_user_detail(filepath: str):
    """Streams the downloaded CSV and calculates usage counters."""
    logger.info(f"Processing M365 App file: {os.path.basename(filepath)}")
    
    if not os.path.exists(filepath):
        logger.error(f"Error: Could not find the file {filepath} to process.")
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
    
    headers = pd.read_csv(filepath, nrows=0).columns.tolist()
    cols = [c for c in columns_to_track if c in headers]
    
    counters = {col: 0 for col in columns_to_track}

    for chunk in pd.read_csv(filepath, usecols=cols, chunksize=10000, encoding="utf-8-sig"):
        for col in columns_to_track:
            if col in chunk.columns:
                col_series = chunk[col].astype(str).str.strip().str.lower()
                count = int(col_series.isin(["yes", "true"]).sum())
                counters[col] += count
            
    logger.info("Successfully processed M365 App user data in chunks.")
    return [(col, count) for col, count in counters.items()]

def run_m365_pipeline(client_id: str, client_secret: str, tenant_id: str):
    """Pipeline specifically for M365 Apps Data."""
    logger.info("Starting isolated M365 Apps Pipeline...")
    client, service = _get_reports_service(client_id, client_secret, tenant_id)
    
    script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
    telemetry_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(script_dir))), "telemetry")
    reports_dir = os.path.join(telemetry_dir, "reports", f"{tenant_id}_{client_id}")
    
    service.download_m365_app_details(reports_dir)
    client.close()
    
    return process_m365_app_user_detail(os.path.join(reports_dir, "M365AppUserDetail(180d).csv"))
