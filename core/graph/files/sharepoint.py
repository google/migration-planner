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

"""SharePoint Site usage telemetry scanner data pipeline."""

import os
import logging
import pandas as pd
import requests
import concurrent.futures

from core.graph.client import GraphClient
from core.graph.reports import ReportsService
from core.graph.files.onedrive import format_bytes

logger = logging.getLogger(__name__)

def parse_sharepoint_csv(filepath, client=None):
    """Streams the SharePoint Site Usage Detail CSV and aggregates metrics in chunks."""
    logger.info(f"Processing SharePoint Site Usage file: {os.path.basename(filepath)}")
    if not os.path.exists(filepath):
        logger.error(f"Error: Could not find SharePoint report {filepath}")
        raise FileNotFoundError(f"SharePoint Site Usage report not found.")

    headers = pd.read_csv(filepath, nrows=0).columns.tolist()
    expected = ["Is Deleted", "Storage Used (Byte)", "File Count", "Active File Count"]
    cols = [c for c in expected if c in headers]

    total_sites = 0
    total_storage = 0
    total_files = 0
    active_files = 0

    for chunk in pd.read_csv(filepath, usecols=cols, chunksize=10000, encoding="utf-8-sig"):
        if "Is Deleted" in chunk.columns:
            mask = chunk["Is Deleted"].astype(str).str.strip().str.upper() != "TRUE"
            active_chunk = chunk[mask]
        else:
            active_chunk = chunk

        total_sites += len(active_chunk)
        if "Storage Used (Byte)" in active_chunk.columns:
            total_storage += int(pd.to_numeric(active_chunk["Storage Used (Byte)"], errors='coerce').fillna(0).sum())
        if "File Count" in active_chunk.columns:
            total_files += int(pd.to_numeric(active_chunk["File Count"], errors='coerce').fillna(0).sum())
        if "Active File Count" in active_chunk.columns:
            active_files += int(pd.to_numeric(active_chunk["Active File Count"], errors='coerce').fillna(0).sum())

    logger.info(f"SharePoint parsing complete: sites={total_sites}, storage={format_bytes(total_storage)}, files={total_files}, active_files={active_files}")
    
    # Reload to get heavy sites
    heavy_sites = []
    if "Storage Used (Byte)" in headers:
        usecols = ["Storage Used (Byte)"]
        for col in ["Site URL", "Site Id", "Owner Principal Name", "Owner Display Name"]:
            if col in headers:
                usecols.append(col)
                
        df = pd.read_csv(filepath, usecols=usecols, encoding="utf-8-sig")
        df["Storage Used (Byte)"] = pd.to_numeric(df["Storage Used (Byte)"], errors='coerce').fillna(0)
        df_sorted = df.sort_values(by="Storage Used (Byte)", ascending=False).head(25)
        df_sorted = df_sorted.fillna("")
        
        records = df_sorted.to_dict("records")
        
        def resolve_site(record):
            site_name = record.get("Site URL", "")
            
            # Resolve via Graph API if concealed
            if not site_name and client:
                site_id = record.get("Site Id")
                if site_id:
                    try:
                        token_slot = client.get_active_token()
                        try:
                            headers_req = {"Authorization": f"Bearer {token_slot['token']}", "Accept": "application/json"}
                            resp = requests.get(f"https://graph.microsoft.com/v1.0/sites/{site_id}", headers=headers_req, timeout=5)
                            if resp.status_code == 200:
                                site_name = resp.json().get("webUrl", "")
                        finally:
                            client.release_token(token_slot)
                    except Exception as e:
                        logger.warning(f"Failed to fetch site url for {site_id}: {e}")
                        
            if not site_name:
                site_name = record.get("Owner Display Name") or record.get("Owner Principal Name") or record.get("Site Id") or "Unknown Site"
                site_name = f"Concealed Site ({site_name})"
            
            return {
                "Site URL": site_name,
                "Site Id": record.get("Site Id", ""),
                "Storage Used (Byte)": record.get("Storage Used (Byte)", 0)
            }
            
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            heavy_sites = list(executor.map(resolve_site, records))
        
        # Sort heavy sites by storage again just in case (executor.map preserves order, but to be safe)
        heavy_sites = sorted(heavy_sites, key=lambda x: x.get("Storage Used (Byte)", 0), reverse=True)
        
    return {
        "total_sites": total_sites,
        "total_storage_bytes": total_storage,
        "total_storage_formatted": format_bytes(total_storage),
        "total_files": total_files,
        "active_files": active_files,
        "active_files_pct": (active_files / total_files * 100) if total_files > 0 else 0.0,
        "heavy_sites": heavy_sites
    }

def run_sharepoint_pipeline(client_id, client_secret, tenant_id) -> dict:
    """Pipeline specifically for SharePoint telemetry data collection."""
    logger.info("Starting SharePoint Telemetry Pipeline...")
    
    client = GraphClient(
        tenant_id=tenant_id,
        client_ids=client_id,
        client_secrets=client_secret,
        concurrency=1,
        retries=5,
        backoff=2
    )
    client.authenticate()
    service = ReportsService(client)
    
    script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
    telemetry_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(script_dir))), "telemetry")
    reports_dir = os.path.join(telemetry_dir, "reports", f"{tenant_id}_{client_id}")
    
    service.download_sharepoint_details(reports_dir)
    logger.info("SharePoint Site Usage CSV download completed. Initiating parser...")
    
    sp_data = parse_sharepoint_csv(os.path.join(reports_dir, "SharePointSiteUsageDetail(180d).csv"), client=client)
    client.close()
    
    logger.info("SharePoint Telemetry Pipeline completed successfully.")
    return sp_data
