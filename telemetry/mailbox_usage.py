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

"""Modular Exchange Online Mailbox usage telemetry scanners and visual interfaces."""

import os
import logging
import threading
import pandas as pd
import customtkinter as ctk

# Import unified core service layer
from core.graph.client import GraphClient
from core.graph.reports import ReportsService
from core.graph.directory import DirectoryService
from core.powershell.client import PowerShellClient
from core.powershell.mailbox import MailboxStatsService

# Bind to the async logger initialized in m365_telemetry.py
usage_logger = logging.getLogger("M365TelemetryAsyncLogger")

# =================================================================================
# CONSTANTS & STYLES (Imported from shared styles)
# =================================================================================
from telemetry.styles import *

# =================================================================================
# PIPELINE UTILITIES
# =================================================================================

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
    usage_logger.info(f"Processing Mailbox Usage file: {os.path.basename(filepath)}")
    if not os.path.exists(filepath):
        usage_logger.error(f"Error: Could not find Mailbox report {filepath}")
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

    usage_logger.info(
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
    usage_logger.info("Starting Mailbox Usage Telemetry Pipeline...")
    
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
        usage_logger.info(f"Retrieved primary tenant domain for Connect-ExchangeOnline: {tenant_domain}")
    except Exception as e:
        usage_logger.warning(f"Could not retrieve tenant domain via Graph. Falling back to Tenant ID Guid: {e}")

    script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
    reports_dir = os.path.join(script_dir, "reports", f"{tenant_id}_{client_id}")
    
    service.download_mailbox_usage_detail(reports_dir)
    usage_logger.info("Mailbox Usage CSV download completed. Initiating parser...")
    client.close()
    
    data = parse_mailbox_usage_csv(os.path.join(reports_dir, "MailboxUsageDetail(180d).csv"))
    
    shared_count = None
    shared_bytes = None
    pf_count = None
    pf_bytes = None
    mail_pf_count = None
    powershell_error = None
    
    try:
        usage_logger.info("Running PowerShell script for Shared Mailboxes and Public Folders stats...")
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
        
        # Parse and log individual command errors
        errors = stats.get("Errors", {})
        if errors:
            for component, err_msg in errors.items():
                usage_logger.error(f"PowerShell error querying {component}: {err_msg}")
            powershell_error = ", ".join(f"{k}: {v}" for k, v in errors.items())
    except Exception as e:
        usage_logger.error("Failed to fetch Shared Mailbox / Public Folder stats via PowerShell", exc_info=True)
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

    usage_logger.info("Mailbox Usage Telemetry Pipeline completed successfully.")
    return data

# =================================================================================
# MODULAR UI COMPONENTS
# =================================================================================

class MailboxUsageFrame(ctk.CTkFrame):
    """Self-contained customtkinter component wrapping Exchange Online Mailbox Usage UI."""
    
    def __init__(self, master, log_callback, credentials_callback, status_change_callback, **kwargs):
        self.semaphore = kwargs.pop("concurrency_semaphore", None)
        super().__init__(master, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=12, **kwargs)
        self.log_msg = log_callback
        self.get_credentials = credentials_callback
        self.on_status_change = status_change_callback
        self.status = None  # 'loading', 'success', 'error', None
        
        self.build_ui()

    def build_ui(self):
        """Creates card container for the tab."""
        self.pack(fill="x", expand=True, pady=10)
        
        self.inner_pad = ctk.CTkFrame(self, fg_color="transparent")
        self.inner_pad.pack(fill="both", expand=True, padx=20, pady=20)
        
        ctk.CTkLabel(self.inner_pad, text="Exchange Online Mailbox Usage", font=FONT_HEADER_SMALL, text_color=COLOR_TEXT_MAIN).pack(anchor="w", pady=(0, 10))
        
        self.state_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.warning_label = ctk.CTkLabel(self.inner_pad, text="", font=FONT_BODY_MEDIUM, text_color=COLOR_ERROR, justify="left", anchor="w", wraplength=750)
        self.grid_frame = ctk.CTkFrame(self.inner_pad, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)
        
        self.reset_view()

    def reset_view(self):
        """Resets and hides grids."""
        self.pack_forget()
        self.state_frame.pack_forget()
        self.grid_frame.pack_forget()
        if hasattr(self, "warning_label"):
            self.warning_label.pack_forget()
        
        for w in self.state_frame.winfo_children():
            w.destroy()
        for w in self.grid_frame.winfo_children():
            w.destroy()

    def _set_state_loading(self, msg="Loading..."):
        for w in self.state_frame.winfo_children():
            w.destroy()
        ctk.CTkLabel(self.state_frame, text=f"⏳ {msg}", text_color=COLOR_TEXT_SUB, font=FONT_BODY_MEDIUM).pack(pady=20)
        self.state_frame.pack(fill="x", expand=True)

    def _set_state_error(self, error_msg):
        for w in self.state_frame.winfo_children():
            w.destroy()

        display_msg = error_msg
        if "401" in error_msg or "403" in error_msg or "unauthorized" in error_msg.lower() or "forbidden" in error_msg.lower():
            display_msg = "Reports telemetry permission required.\nPlease grant the 'Reports.Read.All' application permission to your App Registration in Microsoft Entra ID."

        ctk.CTkLabel(self.state_frame, text=f"✖ {display_msg}", text_color=COLOR_ERROR, font=FONT_BODY_MEDIUM, justify="center").pack(pady=(20, 10))
        ctk.CTkButton(self.state_frame, text="Try Again", command=self._retry_fetch, width=120, fg_color="transparent", border_width=1, text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER).pack(pady=(0, 20))
        self.state_frame.pack(fill="x", expand=True)

    def _retry_fetch(self):
        tenant, clients, secrets = self.get_credentials()
        if tenant:
            self.trigger_fetch(tenant, clients[0], secrets[0])

    def trigger_fetch(self, tenant, client_id, client_secret):
        """Triggers parallel fetches inside isolated background threads."""
        usage_logger.info("Mailbox Usage trigger_fetch called. Spawning background worker thread...")
        self.status = "loading"
        self.on_status_change()
        
        self.pack(fill="x", expand=True, pady=10)
        self.grid_frame.pack_forget()
        
        self._set_state_loading("Downloading and parsing Mailbox Usage reports...")
        
        threading.Thread(
            target=self._execute_worker,
            args=(tenant, client_id, client_secret),
            daemon=True
        ).start()

    def _execute_worker(self, tenant: str, client_id: str, client_secret: str):
        usage_logger.info("Executing thread: _execute_mailbox_worker")
        if self.semaphore:
            self.semaphore.acquire()
        try:
            data = run_mailbox_usage_pipeline(client_id, client_secret, tenant)
            usage_logger.info("Successfully completed Mailbox Usage telemetry data fetch.")
            self.after(0, self._render_success, data)
        except Exception as e:
            usage_logger.error("Exception caught in Mailbox Usage worker.", exc_info=True)
            self.after(0, self._render_error, str(e))
        finally:
            if self.semaphore:
                self.semaphore.release()

    def _render_success(self, data: dict):
        self.last_data = data
        usage_logger.info("Mailbox Usage data successfully retrieved. Rendering UI grid.")
        self.state_frame.pack_forget()
        for w in self.grid_frame.winfo_children():
            w.destroy()

        if data.get("powershell_error"):
            err_msg = data["powershell_error"]
            if "powershell" in err_msg.lower() or "pwsh" in err_msg.lower():
                friendly_msg = "PowerShell Core ('pwsh') not installed/configured. Cannot retrieve shared mailbox or public folder statistics."
            elif "exchangeonlinemanagement" in err_msg.lower():
                friendly_msg = "ExchangeOnlineManagement PowerShell module is missing. Run: Install-Module -Name ExchangeOnlineManagement -Scope CurrentUser"
            else:
                friendly_msg = f"Failed to retrieve shared mailbox or public folder statistics: {err_msg}"
            
            self.warning_label.configure(text=f"⚠️ Warning: {friendly_msg}")
            self.warning_label.pack(anchor="w", pady=(0, 10))
        else:
            self.warning_label.pack_forget()

        self.grid_frame.pack(fill="x", expand=True)

        self.grid_frame.grid_columnconfigure(0, weight=3)
        self.grid_frame.grid_columnconfigure(1, weight=2)

        headers_sp = ["Mailbox Metric Description", "Value / Measurement"]
        for col_idx, head_text in enumerate(headers_sp):
            cell = ctk.CTkFrame(self.grid_frame, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=1, pady=1)
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        rows_data = [
            ("Total Mailboxes Analyzed", f"{data.get('total_mailboxes', 0):,} Mailboxes"),
            ("Total Size of All Mailboxes", data.get("total_storage_formatted", "0.00 Bytes")),
            ("Average Mailbox Size", data.get("average_mailbox_size_formatted", "0.00 Bytes")),
            ("Total Number of Emails", f"{data.get('total_emails', 0):,} Emails"),
            ("Average Emails per User", f"{data.get('average_emails', 0.0):,.0f} Emails")
        ]

        # PowerShell retrieved stats (with individual error handling)
        s_count = data.get("shared_mailboxes_count")
        s_count_str = f"{s_count:,} Shared Mailboxes" if s_count is not None else "Error/Unavailable"
        s_size_str = data.get("shared_mailboxes_total_formatted", "Error/Unavailable")
        
        pf_count = data.get("public_folders_count")
        pf_count_str = f"{pf_count:,} Public Folders" if pf_count is not None else "Error/Unavailable"
        
        mail_pf_count = data.get("mail_public_folders_count")
        mail_pf_count_str = f"{mail_pf_count:,} Public Folders" if mail_pf_count is not None else "Error/Unavailable"
        
        pf_size_str = data.get("public_folders_total_formatted", "Error/Unavailable")

        rows_data += [
            ("Shared Mailboxes Count", s_count_str),
            ("Total Shared Mailbox Size", s_size_str),
            ("Public Folders Count", pf_count_str),
            ("Mail-enabled Public Folders Count", mail_pf_count_str),
            ("Total Public Folder Size", pf_size_str)
        ]

        for r_idx, (metric_name, val) in enumerate(rows_data, start=1):
            bg_style = "transparent" if r_idx % 2 != 0 else COLOR_SURFACE_VARIANT
            
            c0 = ctk.CTkFrame(self.grid_frame, fg_color=bg_style, corner_radius=0)
            c0.grid(row=r_idx, column=0, sticky="nsew", padx=1, pady=1)
            ctk.CTkLabel(c0, text=metric_name, font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=6, anchor="w")

            c1 = ctk.CTkFrame(self.grid_frame, fg_color=bg_style, corner_radius=0)
            c1.grid(row=r_idx, column=1, sticky="nsew", padx=1, pady=1)
            ctk.CTkLabel(c1, text=val, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=6, anchor="w")

        self.status = "success"
        self.on_status_change()

    def _render_error(self, err_msg):
        usage_logger.warning(f"Mailbox Usage fetch failed: {err_msg}")
        self._set_state_error(err_msg)
        self.status = "error"
        self.on_status_change()
