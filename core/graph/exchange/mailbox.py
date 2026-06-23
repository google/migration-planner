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

"""Exchange Online Mailbox usage telemetry scanner data pipeline."""

import os
import logging
import pandas as pd

from core.graph.client import GraphClient
from core.graph.reports import ReportsService
from core.graph.directory import DirectoryService
from core.powershell.client import PowerShellClient
from core.powershell.mailbox import MailboxStatsService

logger = logging.getLogger(__name__)

def format_bytes(num_bytes: float) -> str:
    """Formats raw byte values into highly readable string equivalents (e.g., GB, TB)."""
    if num_bytes is None:
        return "0.00 Bytes"
    
    for unit in ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB']:
        if num_bytes < 1024.0:
            return f"{num_bytes:,.2f} {unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:,.2f} EB"

def parse_mailbox_usage_csv(filepath: str) -> dict:
    """Streams the Mailbox Usage Detail CSV and aggregates metrics in chunks using pandas."""
    logger.info(f"Processing Mailbox Usage file: {os.path.basename(filepath)}")
    if not os.path.exists(filepath):
        logger.error(f"Error: Could not find Mailbox report {filepath}")
        raise FileNotFoundError("Mailbox Usage report not found.")

    cols = ["Storage Used (Byte)", "Item Count"]
    headers = pd.read_csv(filepath, nrows=0).columns.tolist()
    if "Is Deleted" in headers:
        cols.append("Is Deleted")

    total_mailboxes = 0
    total_bytes = 0
    total_emails = 0

    for chunk in pd.read_csv(filepath, usecols=cols, chunksize=10000):
        if "Is Deleted" in chunk.columns:
            active_chunk = chunk[~chunk["Is Deleted"].astype(str).str.strip().str.upper().isin(["TRUE", "1"])]
        else:
            active_chunk = chunk

        active_chunk = active_chunk.dropna(subset=['Storage Used (Byte)', 'Item Count'])
        
        total_mailboxes += len(active_chunk)
        total_bytes += int(active_chunk['Storage Used (Byte)'].sum())
        total_emails += int(active_chunk['Item Count'].sum())

    avg_bytes = (total_bytes / total_mailboxes) if total_mailboxes > 0 else 0.0
    avg_emails = (total_emails / total_mailboxes) if total_mailboxes > 0 else 0.0

    logger.info(
        f"Mailbox parsing complete: mailboxes={total_mailboxes}, "
        f"storage={format_bytes(total_bytes)}, items={total_emails}"
    )

    return {
        "total_mailboxes": total_mailboxes,
        "total_storage_bytes": total_bytes,
        "total_storage_formatted": format_bytes(total_bytes),
        "average_mailbox_size_bytes": avg_bytes,
        "average_mailbox_size_formatted": format_bytes(avg_bytes),
        "total_emails": total_emails,
        "average_emails": avg_emails
    }

def run_mailbox_usage_pipeline(client_id: str, client_secret: str, tenant_id: str) -> dict:
    """Pipeline specifically for Mailbox Usage telemetry data collection."""
    logger.info("Starting Mailbox Usage Telemetry Pipeline...")
    
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
    
    tenant_domain = tenant_id
    try:
        dir_svc = DirectoryService(client)
        tenant_domain = dir_svc.get_tenant_primary_domain()
        logger.info(f"Retrieved primary tenant domain for Connect-ExchangeOnline: {tenant_domain}")
    except Exception as e:
        logger.warning(f"Could not retrieve tenant domain via Graph. Falling back to Tenant ID Guid: {e}")

    script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
    telemetry_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(script_dir))), "telemetry")
    reports_dir = os.path.join(telemetry_dir, "reports", f"{tenant_id}_{client_id}")
    
    service.download_mailbox_usage_detail(reports_dir)
    logger.info("Mailbox Usage CSV download completed. Initiating parser...")
    client.close()
    
    data = parse_mailbox_usage_csv(os.path.join(reports_dir, "MailboxUsageDetail(180d).csv"))
    
    shared_count = None
    shared_bytes = None
    pf_count = None
    pf_bytes = None
    mail_pf_count = None
    powershell_error = None
    
    try:
        logger.info("Running PowerShell script for Shared Mailboxes and Public Folders stats...")
        ps_client = PowerShellClient(
            tenant_id=tenant_domain,
            client_id=client_id,
            client_secret=client_secret,
            cert_tenant_id=tenant_id
        )
        pb_service = MailboxStatsService(ps_client)
        stats = pb_service.fetch_mailbox_and_folder_stats()
        
        shared_count = stats.get("SharedMailboxesCount")
        shared_bytes = stats.get("SharedMailboxesTotalBytes")
        pf_count = stats.get("PublicFoldersCount")
        pf_bytes = stats.get("PublicFoldersTotalBytes")
        mail_pf_count = stats.get("MailPublicFoldersCount")
        
        errors = stats.get("Errors", {})
        if errors:
            for component, err_msg in errors.items():
                logger.error(f"PowerShell error querying {component}: {err_msg}")
            powershell_error = ", ".join(f"{k}: {v}" for k, v in errors.items())
    except Exception as e:
        logger.error("Failed to fetch Shared Mailbox / Public Folder stats via PowerShell", exc_info=True)
        powershell_error = str(e)

    data.update({
        "shared_mailboxes_count": shared_count,
        "shared_mailboxes_total_bytes": shared_bytes,
        "shared_mailboxes_total_formatted": format_bytes(shared_bytes) if shared_bytes is not None else "Error/Unavailable",
        "public_folders_count": pf_count,
        "public_folders_total_bytes": pf_bytes,
        "public_folders_total_formatted": format_bytes(pf_bytes) if pf_bytes is not None else "Error/Unavailable",
        "mail_public_folders_count": mail_pf_count,
        "powershell_error": powershell_error
    })

    logger.info("Mailbox Usage Telemetry Pipeline completed successfully.")
    return data
