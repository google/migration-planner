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

"""Modular SharePoint and OneDrive usage telemetry scanners and visual interfaces."""

import os
import pandas as pd
import logging
import threading
import customtkinter as ctk

# Import unified core service layer
from core.graph.client import GraphClient
from core.graph.reports import ReportsService

# Bind to the async logger initialized in m365_telemetry.py
usage_logger = logging.getLogger("M365TelemetryAsyncLogger")

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
    """Streams the SharePoint Site Usage Detail CSV and aggregates metrics in chunks."""
    usage_logger.info(f"Processing SharePoint Site Usage file: {os.path.basename(filepath)}")
    if not os.path.exists(filepath):
        usage_logger.error(f"Error: Could not find SharePoint report {filepath}")
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

    usage_logger.info(f"SharePoint parsing complete: sites={total_sites}, storage={format_bytes(total_storage)}, files={total_files}, active_files={active_files}")
    return {
        "total_sites": total_sites,
        "total_storage_bytes": total_storage,
        "total_storage_formatted": format_bytes(total_storage),
        "total_files": total_files,
        "active_files": active_files,
        "active_files_pct": (active_files / total_files * 100) if total_files > 0 else 0.0
    }

def parse_onedrive_csv(filepath):
    """Streams the OneDrive Account Usage Detail CSV and aggregates metrics in chunks."""
    usage_logger.info(f"Processing OneDrive Account Usage file: {os.path.basename(filepath)}")
    if not os.path.exists(filepath):
        usage_logger.error(f"Error: Could not find OneDrive report {filepath}")
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

    usage_logger.info(f"OneDrive parsing complete: accounts={total_accounts}, storage={format_bytes(total_storage)}, files={total_files}, active_files={active_files}")
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
    usage_logger.info(f"Processing OneDrive Activity User Detail file: {os.path.basename(filepath)}")
    if not os.path.exists(filepath):
        usage_logger.error(f"Error: Could not find OneDrive Activity report {filepath}")
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

    usage_logger.info(f"OneDrive Activity parsing complete: sync_users={sync_users}")
    return {
        "sync_users": sync_users
    }

def parse_onenote_users_csv(filepath):
    """Streams the M365 App User Detail CSV and counts unique active OneNote users in chunks."""
    usage_logger.info(f"Processing OneNote Users file: {os.path.basename(filepath)}")
    if not os.path.exists(filepath):
        usage_logger.error(f"Error: Could not find M365 App User Detail report {filepath}")
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

    usage_logger.info(f"OneNote Users parsing complete: onenote_users={onenote_users}")
    return {
        "onenote_users": onenote_users
    }

# =================================================================================
# INDEPENDENT SCANNING PIPELINES
# =================================================================================

def run_sharepoint_pipeline(client_id, client_secret, tenant_id) -> dict:
    """Pipeline specifically for SharePoint telemetry data collection."""
    usage_logger.info("Starting SharePoint Telemetry Pipeline...")
    
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
    reports_dir = os.path.join(script_dir, "reports", f"{tenant_id}_{client_id}")
    
    service.download_sharepoint_details(reports_dir)
    usage_logger.info("SharePoint Site Usage CSV download completed. Initiating parser...")
    client.close()
    
    sp_data = parse_sharepoint_csv(os.path.join(reports_dir, "SharePointSiteUsageDetail(180d).csv"))
    usage_logger.info("SharePoint Telemetry Pipeline completed successfully.")
    return sp_data

def run_onedrive_pipeline(client_id, client_secret, tenant_id) -> dict:
    """Pipeline specifically for OneDrive telemetry data collection."""
    usage_logger.info("Starting OneDrive Telemetry Pipeline...")
    
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
    reports_dir = os.path.join(script_dir, "reports", f"{tenant_id}_{client_id}")
    
    service.download_onedrive_details(reports_dir)
    usage_logger.info("OneDrive account, activity, and OneNote CSV downloads completed. Initiating parsers...")
    client.close()
    
    od_data = parse_onedrive_csv(os.path.join(reports_dir, "OneDriveUsageAccountDetail(180d).csv"))
    od_act_data = parse_onedrive_activity_csv(os.path.join(reports_dir, "OneDriveActivityUserDetail(180d).csv"))
    onenote_data = parse_onenote_users_csv(os.path.join(reports_dir, "M365AppUserDetail_sp_od(180d).csv"))
    
    # Merge active sync client user data into od_data
    od_data["sync_users"] = od_act_data["sync_users"]
    od_data["sync_users_pct"] = (od_act_data["sync_users"] / od_data["total_accounts"] * 100) if od_data["total_accounts"] > 0 else 0.0
    
    # Merge OneNote user data
    od_data["onenote_users"] = onenote_data["onenote_users"]
    
    usage_logger.info("OneDrive Telemetry Pipeline completed successfully.")
    return od_data

# =================================================================================
# MODULAR UI COMPONENTS
# =================================================================================

class SharePointUsageFrame(ctk.CTkFrame):
    """Self-contained customtkinter component wrapping SharePoint Telemetry UI."""
    
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
        
        ctk.CTkLabel(self.inner_pad, text="SharePoint Site Usage Telemetry (180 Days)", font=FONT_HEADER_SMALL, text_color=COLOR_TEXT_MAIN).pack(anchor="w", pady=(0, 10))
        
        self.state_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.grid_frame = ctk.CTkFrame(self.inner_pad, fg_color=COLOR_OUTLINE_LIGHT, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)
        
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
        usage_logger.info("SharePoint Site Usage trigger_fetch called. Spawning background worker thread...")
        self.status = "loading"
        self.on_status_change()
        
        self.pack(fill="x", expand=True, pady=(20, 10))
        self.grid_frame.pack_forget()
        
        self._set_state_loading("Downloading and parsing SharePoint Site Usage reports...")
        
        threading.Thread(
            target=self._execute_worker,
            args=(tenant, client_id, client_secret),
            daemon=True
        ).start()

    def _execute_worker(self, tenant: str, client_id: str, client_secret: str):
        usage_logger.info("Executing thread: _execute_sharepoint_worker")
        if self.semaphore:
            self.semaphore.acquire()
        try:
            data = run_sharepoint_pipeline(client_id, client_secret, tenant)
            usage_logger.info("Successfully completed SharePoint telemetry data fetch.")
            self.after(0, self._render_success, data)
        except Exception as e:
            usage_logger.error("Exception caught in SharePoint worker.", exc_info=True)
            self.after(0, self._render_error, str(e))
        finally:
            if self.semaphore:
                self.semaphore.release()

    def _render_success(self, data: dict):
        usage_logger.info("SharePoint Site Usage data successfully retrieved. Rendering UI grid.")
        self.state_frame.pack_forget()
        for w in self.grid_frame.winfo_children():
            w.destroy()

        self.grid_frame.pack(fill="x", expand=True)

        self.grid_frame.grid_columnconfigure(0, weight=3)
        self.grid_frame.grid_columnconfigure(1, weight=2)

        headers_sp = ["SharePoint Metric Description", "Value / Measurement"]
        for col_idx, head_text in enumerate(headers_sp):
            cell = ctk.CTkFrame(self.grid_frame, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        rows_data = [
            ("Total Sites Count", f"{data.get('total_sites', 0):,} Sites"),
            ("Total Storage Used", data.get("total_storage_formatted", "0.00 Bytes")),
            ("Total Files Stored", f"{data.get('total_files', 0):,} Files"),
            ("Active Files Count", f"{data.get('active_files', 0):,} Files ({data.get('active_files_pct', 0.0):.1f}%)")
        ]

        for r_idx, (metric_name, val) in enumerate(rows_data, start=1):
            bg_style = COLOR_SURFACE if r_idx % 2 == 0 else COLOR_SURFACE_VARIANT
            
            c0 = ctk.CTkFrame(self.grid_frame, fg_color=bg_style, corner_radius=0)
            c0.grid(row=r_idx, column=0, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c0, text=metric_name, font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=6, anchor="w")

            c1 = ctk.CTkFrame(self.grid_frame, fg_color=bg_style, corner_radius=0)
            c1.grid(row=r_idx, column=1, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c1, text=val, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=6, anchor="w")

        self.status = "success"
        self.on_status_change()

    def _render_error(self, err_msg):
        usage_logger.warning(f"SharePoint Site Usage fetch failed: {err_msg}")
        self._set_state_error(err_msg)
        self.status = "error"
        self.on_status_change()


class OneDriveUsageFrame(ctk.CTkFrame):
    """Self-contained customtkinter component wrapping OneDrive Telemetry UI."""
    
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
        
        ctk.CTkLabel(self.inner_pad, text="OneDrive Usage Telemetry (180 Days)", font=FONT_HEADER_SMALL, text_color=COLOR_TEXT_MAIN).pack(anchor="w", pady=(0, 10))
        
        self.state_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.grid_frame = ctk.CTkFrame(self.inner_pad, fg_color=COLOR_OUTLINE_LIGHT, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)
        
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
        usage_logger.info("OneDrive Usage trigger_fetch called. Spawning background worker thread...")
        self.status = "loading"
        self.on_status_change()
        
        self.pack(fill="x", expand=True, pady=(20, 10))
        self.grid_frame.pack_forget()
        
        self._set_state_loading("Downloading and parsing OneDrive reports...")
        
        threading.Thread(
            target=self._execute_worker,
            args=(tenant, client_id, client_secret),
            daemon=True
        ).start()

    def _execute_worker(self, tenant: str, client_id: str, client_secret: str):
        usage_logger.info("Executing thread: _execute_onedrive_worker")
        if self.semaphore:
            self.semaphore.acquire()
        try:
            data = run_onedrive_pipeline(client_id, client_secret, tenant)
            usage_logger.info("Successfully completed OneDrive telemetry data fetch.")
            self.after(0, self._render_success, data)
        except Exception as e:
            usage_logger.error("Exception caught in OneDrive worker.", exc_info=True)
            self.after(0, self._render_error, str(e))
        finally:
            if self.semaphore:
                self.semaphore.release()

    def _render_success(self, data: dict):
        usage_logger.info("OneDrive Usage data successfully retrieved. Rendering UI grid.")
        self.state_frame.pack_forget()
        for w in self.grid_frame.winfo_children():
            w.destroy()

        self.grid_frame.pack(fill="x", expand=True)

        self.grid_frame.grid_columnconfigure(0, weight=3)
        self.grid_frame.grid_columnconfigure(1, weight=2)

        headers_od = ["OneDrive Metric Description", "Value / Measurement"]
        for col_idx, head_text in enumerate(headers_od):
            cell = ctk.CTkFrame(self.grid_frame, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        rows_data = [
            ("Total Accounts Count", f"{data.get('total_accounts', 0):,} Accounts"),
            ("Total Storage Used", data.get("total_storage_formatted", "0.00 Bytes")),
            ("Total Files Stored", f"{data.get('total_files', 0):,} Files"),
            ("Active Files Count", f"{data.get('active_files', 0):,} Files ({data.get('active_files_pct', 0.0):.1f}%)"),
            ("Users with Synced Files", f"{data.get('sync_users', 0):,} Users ({data.get('sync_users_pct', 0.0):.1f}%)"),
            ("OneNote Active Users", f"{data.get('onenote_users', 0):,} Users")
        ]

        for r_idx, (metric_name, val) in enumerate(rows_data, start=1):
            bg_style = COLOR_SURFACE if r_idx % 2 == 0 else COLOR_SURFACE_VARIANT
            
            c0 = ctk.CTkFrame(self.grid_frame, fg_color=bg_style, corner_radius=0)
            c0.grid(row=r_idx, column=0, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c0, text=metric_name, font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=6, anchor="w")

            c1 = ctk.CTkFrame(self.grid_frame, fg_color=bg_style, corner_radius=0)
            c1.grid(row=r_idx, column=1, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c1, text=val, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=6, anchor="w")

        self.status = "success"
        self.on_status_change()

    def _render_error(self, err_msg):
        usage_logger.warning(f"OneDrive Usage fetch failed: {err_msg}")
        self._set_state_error(err_msg)
        self.status = "error"
        self.on_status_change()
