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

"""Modular SharePoint and OneDrive usage telemetry scanner and visual interface."""

import os
import csv
import logging
import threading
import customtkinter as ctk

# Import unified core service layer
from core.graph.client import GraphClient
from core.graph.reports import ReportsService

# Bind to the async logger initialized in license_usage.py
usage_logger = logging.getLogger("LicenseUsageAsyncLogger")

# =================================================================================
# CONSTANTS & STYLES (Imported from shared styles)
# =================================================================================
from telemetry.styles import *

# =================================================================================
# PIPELINE UTILITIES
# =================================================================================

def format_bytes(num_bytes: int) -> str:
    """Formats raw byte values into highly readable string equivalents (e.g., GB, TB)."""
    if num_bytes is None:
        return "0.00 Bytes"
    
    for unit in ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB']:
        if num_bytes < 1024.0:
            return f"{num_bytes:,.2f} {unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:,.2f} EB"

def parse_sharepoint_csv(filepath):
    """Streams the SharePoint Site Usage Detail CSV and aggregates metrics."""
    usage_logger.info(f"Processing SharePoint Site Usage file: {os.path.basename(filepath)}")
    if not os.path.exists(filepath):
        usage_logger.error(f"Error: Could not find SharePoint report {filepath}")
        raise FileNotFoundError(f"SharePoint Site Usage report not found.")

    total_sites = 0
    total_storage = 0
    total_files = 0
    active_files = 0

    with open(filepath, mode="r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("Is Deleted", "").strip().upper() != "TRUE":
                total_sites += 1
                
                try:
                    total_storage += int(row.get("Storage Used (Byte)", 0) or 0)
                except ValueError:
                    pass
                
                try:
                    total_files += int(row.get("File Count", 0) or 0)
                except ValueError:
                    pass
                
                try:
                    active_files += int(row.get("Active File Count", 0) or 0)
                except ValueError:
                    pass
                    
    return {
        "total_sites": total_sites,
        "total_storage_bytes": total_storage,
        "total_storage_formatted": format_bytes(total_storage),
        "total_files": total_files,
        "active_files": active_files,
        "active_files_pct": (active_files / total_files * 100) if total_files > 0 else 0.0
    }

def parse_onedrive_csv(filepath):
    """Streams the OneDrive Account Usage Detail CSV and aggregates metrics."""
    usage_logger.info(f"Processing OneDrive Account Usage file: {os.path.basename(filepath)}")
    if not os.path.exists(filepath):
        usage_logger.error(f"Error: Could not find OneDrive report {filepath}")
        raise FileNotFoundError(f"OneDrive Account Usage report not found.")

    total_accounts = 0
    total_storage = 0
    total_files = 0
    active_files = 0

    with open(filepath, mode="r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("Is Deleted", "").strip().upper() != "TRUE":
                total_accounts += 1
                
                try:
                    total_storage += int(row.get("Storage Used (Byte)", 0) or 0)
                except ValueError:
                    pass
                
                try:
                    total_files += int(row.get("File Count", 0) or 0)
                except ValueError:
                    pass
                
                try:
                    active_files += int(row.get("Active File Count", 0) or 0)
                except ValueError:
                    pass
                    
    return {
        "total_accounts": total_accounts,
        "total_storage_bytes": total_storage,
        "total_storage_formatted": format_bytes(total_storage),
        "total_files": total_files,
        "active_files": active_files,
        "active_files_pct": (active_files / total_files * 100) if total_files > 0 else 0.0
    }

def parse_onedrive_activity_csv(filepath):
    """Streams the OneDrive Activity User Detail CSV and aggregates active sync client users."""
    usage_logger.info(f"Processing OneDrive Activity User Detail file: {os.path.basename(filepath)}")
    if not os.path.exists(filepath):
        usage_logger.error(f"Error: Could not find OneDrive Activity report {filepath}")
        raise FileNotFoundError(f"OneDrive Activity User Detail report not found.")

    sync_users = 0

    with open(filepath, mode="r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("Is Deleted", "").strip().upper() != "TRUE":
                try:
                    synced_count = int(row.get("Synced File Count", 0) or 0)
                    if synced_count > 0:
                        sync_users += 1
                except ValueError:
                    pass

    return {
        "sync_users": sync_users
    }

def run_sharepoint_onedrive_pipeline(client_id, client_secret, tenant_id):
    """Pipeline specifically for SharePoint and OneDrive telemetry data collection."""
    usage_logger.info("Starting SharePoint & OneDrive Telemetry Pipeline...")
    
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
    reports_dir = os.path.join(script_dir, "reports")
    
    service.download_sharepoint_onedrive_details(reports_dir)
    client.close()
    
    sp_data = parse_sharepoint_csv(os.path.join(reports_dir, "SharePointSiteUsageDetail(180d).csv"))
    od_data = parse_onedrive_csv(os.path.join(reports_dir, "OneDriveUsageAccountDetail(180d).csv"))
    od_act_data = parse_onedrive_activity_csv(os.path.join(reports_dir, "OneDriveActivityUserDetail(180d).csv"))
    
    # Merge active sync client user data into od_data
    od_data["sync_users"] = od_act_data["sync_users"]
    od_data["sync_users_pct"] = (od_act_data["sync_users"] / od_data["total_accounts"] * 100) if od_data["total_accounts"] > 0 else 0.0
    
    return {
        "sharepoint": sp_data,
        "onedrive": od_data
    }

# =================================================================================
# CUSTOM UI COMPONENT CLASS (Preserved identically for visual parity)
# =================================================================================
class SharePointOneDriveUsageFrame(ctk.CTkFrame):
    """Self-contained customtkinter component wrapping SharePoint & OneDrive Telemetry UI."""
    
    def __init__(self, master, log_callback, credentials_callback, status_change_callback, **kwargs):
        super().__init__(master, fg_color="transparent", **kwargs)
        self.log_msg = log_callback
        self.get_credentials = credentials_callback
        self.on_status_change = status_change_callback
        self.status = None  # 'loading', 'success', 'error', None
        
        self.build_ui()

    def build_ui(self):
        """Creates card container for the tab."""
        self.pack(fill="x", expand=True, pady=(20, 10))
        
        ctk.CTkLabel(self, text="SharePoint & OneDrive Telemetry (180 Days)", font=FONT_HEADER_SMALL, text_color=COLOR_TEXT_MAIN).pack(anchor="w", pady=(0, 10))
        
        self.state_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.grid_frame = ctk.CTkFrame(self, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)
        
        self.reset_view()

    def reset_view(self):
        """Resets and hides grids."""
        self.pack_forget()
        self.state_frame.pack_forget()
        self.grid_frame.pack_forget()
        
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
        ctk.CTkLabel(self.state_frame, text=f"✖ {error_msg}", text_color=COLOR_ERROR, font=FONT_BODY_MEDIUM).pack(pady=(20, 10))
        ctk.CTkButton(self.state_frame, text="Try Again", command=self._retry_fetch, width=120, fg_color="transparent", border_width=1, text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER).pack(pady=(0, 20))
        self.state_frame.pack(fill="x", expand=True)

    def _retry_fetch(self):
        tenant, clients, secrets = self.get_credentials()
        if tenant:
            self.trigger_fetch(tenant, clients[0], secrets[0])

    def trigger_fetch(self, tenant, client_id, client_secret):
        """Triggers parallel fetches inside isolated background threads."""
        self.status = "loading"
        self.on_status_change()
        
        self.pack(fill="x", expand=True, pady=(20, 10))
        self.grid_frame.pack_forget()
        
        self._set_state_loading("Downloading and parsing SharePoint & OneDrive reports...")
        
        threading.Thread(
            target=self._execute_worker,
            args=(tenant, client_id, client_secret),
            daemon=True
        ).start()

    def _execute_worker(self, tenant: str, client_id: str, client_secret: str):
        usage_logger.info("Executing thread: _execute_sp_od_worker")
        try:
            data = run_sharepoint_onedrive_pipeline(client_id, client_secret, tenant)
            usage_logger.info("Successfully completed SharePoint & OneDrive telemetry data fetch.")
            self.after(0, self._render_success, data)
        except Exception as e:
            usage_logger.error("Exception caught in SharePoint & OneDrive worker.", exc_info=True)
            self.after(0, self._render_error, str(e))

    def _render_success(self, data: dict):
        self.state_frame.pack_forget()
        for w in self.grid_frame.winfo_children():
            w.destroy()

        self.grid_frame.pack(fill="x", expand=True)

        self.grid_frame.grid_columnconfigure(0, weight=2)
        self.grid_frame.grid_columnconfigure(1, weight=1)
        self.grid_frame.grid_columnconfigure(2, weight=1)

        headers_sp_od = ["Metric Description", "SharePoint Sites", "OneDrive Accounts"]
        for col_idx, head_text in enumerate(headers_sp_od):
            cell = ctk.CTkFrame(self.grid_frame, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=1, pady=1)
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        sp = data.get("sharepoint", {})
        od = data.get("onedrive", {})

        rows_data = [
            ("Total Count", f"{sp.get('total_sites', 0):,} Sites", f"{od.get('total_accounts', 0):,} Accounts"),
            ("Total Storage Used", sp.get("total_storage_formatted", "0.00 Bytes"), od.get("total_storage_formatted", "0.00 Bytes")),
            ("Total File Count", f"{sp.get('total_files', 0):,} Files", f"{od.get('total_files', 0):,} Files"),
            ("Active File Count", 
             f"{sp.get('active_files', 0):,} Files ({sp.get('active_files_pct', 0.0):.1f}%)", 
             f"{od.get('active_files', 0):,} Files ({od.get('active_files_pct', 0.0):.1f}%)"),
            ("Users with Synced Files", 
             "N/A", 
             f"{od.get('sync_users', 0):,} Users ({od.get('sync_users_pct', 0.0):.1f}%)")
        ]

        for r_idx, (metric_name, sp_val, od_val) in enumerate(rows_data, start=1):
            bg_style = "transparent" if r_idx % 2 != 0 else COLOR_SURFACE_VARIANT
            
            c0 = ctk.CTkFrame(self.grid_frame, fg_color=bg_style, corner_radius=0)
            c0.grid(row=r_idx, column=0, sticky="nsew", padx=1, pady=1)
            ctk.CTkLabel(c0, text=metric_name, font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=6, anchor="w")

            c1 = ctk.CTkFrame(self.grid_frame, fg_color=bg_style, corner_radius=0)
            c1.grid(row=r_idx, column=1, sticky="nsew", padx=1, pady=1)
            ctk.CTkLabel(c1, text=sp_val, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=6, anchor="w")

            c2 = ctk.CTkFrame(self.grid_frame, fg_color=bg_style, corner_radius=0)
            c2.grid(row=r_idx, column=2, sticky="nsew", padx=1, pady=1)
            ctk.CTkLabel(c2, text=od_val, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=6, anchor="w")

        self.status = "success"
        self.on_status_change()

    def _render_error(self, err_msg):
        self._set_state_error(err_msg)
        self.status = "error"
        self.on_status_change()
