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

"""OneDrive usage telemetry scanner data pipeline."""

import os
import logging
import pandas as pd

from core.graph.client import GraphClient
from core.graph.reports import ReportsService

logger = logging.getLogger(__name__)

def format_bytes(num_bytes: int) -> str:
    """Formats raw byte values into highly readable string equivalents (e.g., GB, TB)."""
    if num_bytes is None:
        return "0.00 Bytes"
    
    for unit in ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB']:
        if num_bytes < 1024.0:
            return f"{num_bytes:,.2f} {unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:,.2f} EB"

def parse_onedrive_csv(filepath):
    """Streams the OneDrive Account Usage Detail CSV and aggregates metrics in chunks."""
    logger.info(f"Processing OneDrive Account Usage file: {os.path.basename(filepath)}")
    if not os.path.exists(filepath):
        logger.error(f"Error: Could not find OneDrive report {filepath}")
        raise FileNotFoundError(f"OneDrive Account Usage report not found.")

    headers = pd.read_csv(filepath, nrows=0).columns.tolist()
    expected = ["Is Deleted", "Storage Used (Byte)", "File Count", "Active File Count"]
    cols = [c for c in expected if c in headers]

    total_accounts = 0
    total_storage = 0
    total_files = 0
    active_files = 0

    for chunk in pd.read_csv(filepath, usecols=cols, chunksize=10000, encoding="utf-8-sig"):
        if "Is Deleted" in chunk.columns:
            mask = chunk["Is Deleted"].astype(str).str.strip().str.upper() != "TRUE"
            active_chunk = chunk[mask]
        else:
            active_chunk = chunk

        total_accounts += len(active_chunk)
        if "Storage Used (Byte)" in active_chunk.columns:
            total_storage += int(pd.to_numeric(active_chunk["Storage Used (Byte)"], errors='coerce').fillna(0).sum())
        if "File Count" in active_chunk.columns:
            total_files += int(pd.to_numeric(active_chunk["File Count"], errors='coerce').fillna(0).sum())
        if "Active File Count" in active_chunk.columns:
            active_files += int(pd.to_numeric(active_chunk["Active File Count"], errors='coerce').fillna(0).sum())

    logger.info(f"OneDrive parsing complete: accounts={total_accounts}, storage={format_bytes(total_storage)}, files={total_files}, active_files={active_files}")
    return {
        "total_accounts": total_accounts,
        "total_storage_bytes": total_storage,
        "total_storage_formatted": format_bytes(total_storage),
        "total_files": total_files,
        "active_files": active_files,
        "active_files_pct": (active_files / total_files * 100) if total_files > 0 else 0.0
    }

def parse_onedrive_activity_csv(filepath):
    """Streams the OneDrive Activity User Detail CSV and aggregates active sync client users in chunks."""
    logger.info(f"Processing OneDrive Activity User Detail file: {os.path.basename(filepath)}")
    if not os.path.exists(filepath):
        logger.error(f"Error: Could not find OneDrive Activity report {filepath}")
        raise FileNotFoundError(f"OneDrive Activity User Detail report not found.")

    headers = pd.read_csv(filepath, nrows=0).columns.tolist()
    expected = ["Is Deleted", "Synced File Count"]
    cols = [c for c in expected if c in headers]

    sync_users = 0

    for chunk in pd.read_csv(filepath, usecols=cols, chunksize=10000, encoding="utf-8-sig"):
        if "Is Deleted" in chunk.columns:
            mask = chunk["Is Deleted"].astype(str).str.strip().str.upper() != "TRUE"
            active_chunk = chunk[mask]
        else:
            active_chunk = chunk

        if "Synced File Count" in active_chunk.columns:
            synced_series = pd.to_numeric(active_chunk["Synced File Count"], errors='coerce').fillna(0)
            sync_users += int((synced_series > 0).sum())

    logger.info(f"OneDrive Activity parsing complete: sync_users={sync_users}")
    return {
        "sync_users": sync_users
    }

def parse_onenote_users_csv(filepath):
    """Streams the M365 App User Detail CSV and counts unique active OneNote users in chunks."""
    logger.info(f"Processing OneNote Users file: {os.path.basename(filepath)}")
    if not os.path.exists(filepath):
        logger.error(f"Error: Could not find M365 App User Detail report {filepath}")
        raise FileNotFoundError(f"M365 App User Detail report not found.")

    headers = pd.read_csv(filepath, nrows=0).columns.tolist()
    expected = ["Is Deleted", "OneNote"]
    cols = [c for c in expected if c in headers]

    onenote_users = 0

    for chunk in pd.read_csv(filepath, usecols=cols, chunksize=10000, encoding="utf-8-sig"):
        if "Is Deleted" in chunk.columns:
            mask = chunk["Is Deleted"].astype(str).str.strip().str.upper() != "TRUE"
            active_chunk = chunk[mask]
        else:
            active_chunk = chunk

        if "OneNote" in active_chunk.columns:
            onenote_series = active_chunk["OneNote"].astype(str).str.strip().str.lower()
            onenote_users += int(onenote_series.isin(["yes", "true"]).sum())

    logger.info(f"OneNote Users parsing complete: onenote_users={onenote_users}")
    return {
        "onenote_users": onenote_users
    }

def run_onedrive_pipeline(client_id, client_secret, tenant_id) -> dict:
    """Pipeline specifically for OneDrive telemetry data collection."""
    logger.info("Starting OneDrive Telemetry Pipeline...")
    
    client = GraphClient(
        tenant_id=tenant_id,
        client_ids=client_id,
        client_secrets=client_secret,
        concurrency=2,
        retries=5,
        backoff=2
    )
    client.authenticate()
    service = ReportsService(client)
    
    script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
    telemetry_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(script_dir))), "telemetry")
    reports_dir = os.path.join(telemetry_dir, "reports", f"{tenant_id}_{client_id}")
    
    service.download_onedrive_details(reports_dir)
    logger.info("OneDrive account, activity, and OneNote CSV downloads completed. Initiating parsers...")
    client.close()
    
    od_data = parse_onedrive_csv(os.path.join(reports_dir, "OneDriveUsageAccountDetail(180d).csv"))
    od_act_data = parse_onedrive_activity_csv(os.path.join(reports_dir, "OneDriveActivityUserDetail(180d).csv"))
    onenote_data = parse_onenote_users_csv(os.path.join(reports_dir, "M365AppUserDetail_sp_od(180d).csv"))
    
    # Merge active sync client user data into od_data
    od_data["sync_users"] = od_act_data["sync_users"]
    od_data["sync_users_pct"] = (od_act_data["sync_users"] / od_data["total_accounts"] * 100) if od_data["total_accounts"] > 0 else 0.0
    
    # Merge OneNote user data
    od_data["onenote_users"] = onenote_data["onenote_users"]
    
    logger.info("OneDrive Telemetry Pipeline completed successfully.")
    return od_data
