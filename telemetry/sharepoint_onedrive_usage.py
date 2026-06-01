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
import requests
import concurrent.futures
import time
import logging
import threading
import customtkinter as ctk

# Bind to the async logger initialized in license_usage.py
usage_logger = logging.getLogger("LicenseUsageAsyncLogger")

# =================================================================================
# CONSTANTS & STYLES (Material 3 parity with license_usage.py)
# =================================================================================
COLOR_PRIMARY = "#0B57D0"
COLOR_SURFACE = "#FFFFFF"
COLOR_TEXT_MAIN = "#1F1F1F"
COLOR_TEXT_SUB = "#444746"
COLOR_OUTLINE = "#747775"
COLOR_OUTLINE_LIGHT = "#E0E2E0"
COLOR_TONAL_BG = "#D3E3FD"
COLOR_TONAL_TEXT = "#041E49"
COLOR_SUCCESS = "#188038"
COLOR_ERROR = "#B3261E"
COLOR_PRIMARY_HOVER = "#0842a0"
COLOR_SECONDARY_HOVER = "#F1F3F4"
COLOR_SURFACE_HOVER = "#EFF6FF"
COLOR_SURFACE_VARIANT = "#F8F9FA"

FONT_HEADER_SMALL = ("Roboto", 18, "bold")
FONT_BODY_BOLD = ("Roboto", 14, "bold")
FONT_BODY_MEDIUM = ("Roboto", 12)
FONT_BODY_SMALL = ("Roboto", 11)

# =================================================================================
# NETWORKING & PIPELINE UTILITIES (Isolated)
# =================================================================================

def get_access_token(client_id, client_secret, tenant_id):
    """Fetches the OAuth 2.0 Access Token from Microsoft Entra ID."""
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    token_data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://graph.microsoft.com/.default"
    }
    usage_logger.info("Fetching OAuth token from Microsoft Entra ID for SharePoint & OneDrive...")
    token_response = requests.post(token_url, data=token_data)
    token_response.raise_for_status()
    return token_response.json().get("access_token")

def download_report(api_url, access_token, output_filename):
    """Calls the Graph API and downloads the CSV report to the 'reports' folder using a streaming download."""
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    
    usage_logger.info(f"Calling API for {output_filename} (intercepting redirect)...")
    report_response = requests.get(api_url, headers=headers, allow_redirects=False, stream=True)
    
    script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
    reports_dir = os.path.join(script_dir, "reports")
    os.makedirs(reports_dir, exist_ok=True)
    
    output_path = os.path.join(reports_dir, output_filename)
    
    if report_response.status_code == 302:
        report_response.close()
        location_url = report_response.headers.get("Location")
        if not location_url:
            usage_logger.error(f"Received a 302 status for {output_filename} but no Location header was found.")
            raise Exception(f"Received a 302 status for {output_filename} but no Location header was found.")
            
        usage_logger.info(f"Pre-authenticated URL retrieved for {output_filename}. Waiting 2 seconds before starting download...")
        time.sleep(2)
        
        max_retries = 5
        retry_interval = 30
        
        for attempt in range(1, max_retries + 1):
            try:
                usage_logger.info(f"[Attempt {attempt}/{max_retries}] Downloading {output_filename}...")
                with requests.get(location_url, stream=True) as csv_response:
                    csv_response.raise_for_status()
                    with open(output_path, "wb") as f:
                        for chunk in csv_response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                break
            except requests.exceptions.RequestException as e:
                if attempt < max_retries:
                    usage_logger.warning(f"[Attempt {attempt}/{max_retries}] Failed to download {output_filename}. Retrying in {retry_interval}s... (Error: {e})")
                    time.sleep(retry_interval)
                else:
                    usage_logger.error(f"Failed to download {output_filename} after {max_retries} attempts.", exc_info=True)
                    raise Exception(f"Failed to download {output_filename} after {max_retries} attempts. Last error: {e}")
                    
        usage_logger.info(f"Success! Saved usage report to: {output_path}")
        
    elif report_response.status_code == 200:
        usage_logger.info(f"Unexpected 200 OK for {output_filename}. Saving payload directly via stream...")
        with open(output_path, "wb") as f:
            for chunk in report_response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        report_response.close()
        usage_logger.info(f"Success! Saved usage report to: {output_path}")
    else:
        usage_logger.error(f"Error fetching {output_filename}. Status Code: {report_response.status_code}")
        report_response.close()
        raise Exception(f"API Error {report_response.status_code} fetching {output_filename}")

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

def run_sharepoint_onedrive_pipeline(client_id, client_secret, tenant_id):
    """Pipeline specifically for SharePoint and OneDrive telemetry data collection."""
    usage_logger.info("Starting SharePoint & OneDrive Telemetry Pipeline...")
    access_token = get_access_token(client_id, client_secret, tenant_id)
    
    reports = [
        ("https://graph.microsoft.com/v1.0/reports/getSharePointSiteUsageDetail(period='D180')", "SharePointSiteUsageDetail(180d).csv"),
        ("https://graph.microsoft.com/v1.0/reports/getOneDriveUsageAccountDetail(period='D180')", "OneDriveUsageAccountDetail(180d).csv")
    ]
    
    # Parallel downloads inside isolated thread executor
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(download_report, url, access_token, filename) for url, filename in reports]
        for future in concurrent.futures.as_completed(futures):
            future.result()
    
    script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
    reports_dir = os.path.join(script_dir, "reports")
    
    sp_data = parse_sharepoint_csv(os.path.join(reports_dir, "SharePointSiteUsageDetail(180d).csv"))
    od_data = parse_onedrive_csv(os.path.join(reports_dir, "OneDriveUsageAccountDetail(180d).csv"))
    
    return {
        "sharepoint": sp_data,
        "onedrive": od_data
    }

# =================================================================================
# CUSTOM UI COMPONENT CLASS
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
             f"{od.get('active_files', 0):,} Files ({od.get('active_files_pct', 0.0):.1f}%)")
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
