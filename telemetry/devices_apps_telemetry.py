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

"""Modular Microsoft Entra Data telemetry scanners and visual interfaces."""

import os
import csv
import logging
import threading
from typing import Any, Dict, List, Optional
import customtkinter as ctk

# Bind to the async logger initialized in m365_telemetry.py
usage_logger = logging.getLogger("M365TelemetryAsyncLogger.DevicesAppsUI")

# Import unified core service layer
from core.graph.client import GraphClient
from core.graph.security import SecurityService
from core.graph.reports import ReportsService

# Import shared styles
from telemetry.styles import *


def run_devices_apps_pipeline(
    client_id: str, 
    client_secret: str, 
    tenant_id: str, 
    on_page_callback=None, 
    on_app_signins_page_callback=None,
    on_auth_methods_page_callback=None,
    is_cancelled_callback=None
) -> dict:
    """Pipeline to fetch interactive & non-interactive sign-in logs, app sign-in summaries, 
    and auth methods in parallel, then compile them from local CSV files."""
    usage_logger.info("Starting Microsoft Entra Data Telemetry Pipeline in parallel...")
    
    script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
    reports_dir = os.path.join(script_dir, "reports", f"{tenant_id}_{client_id}")
    os.makedirs(reports_dir, exist_ok=True)
    
    csv_path_interactive = os.path.join(reports_dir, "signin_interactive.csv")
    csv_path_noninteractive = os.path.join(reports_dir, "signin_noninteractive.csv")
    csv_path_app_signins = os.path.join(reports_dir, "entra_app_signins.csv")
    csv_path_auth_methods = os.path.join(reports_dir, "entra_auth_methods.csv")
    
    for path in [csv_path_interactive, csv_path_noninteractive]:
        with open(path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["appDisplayName", "operatingSystem", "browser", "signInEventType"])
            
    with open(csv_path_app_signins, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["appDisplayName", "successSignInCount"])
        
    with open(csv_path_auth_methods, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["authenticationMethod", "successActivityCount"])
            
    client = GraphClient(
        tenant_id=tenant_id,
        client_ids=client_id,
        client_secrets=client_secret,
        concurrency=4,
        retries=5,
        backoff=2
    )
    client.authenticate()
    security_service = SecurityService(client)
    reports_service = ReportsService(client)
    
    errors = []
    
    def run_fetch_signins(event_type, path):
        try:
            security_service.fetch_signin_activities(
                event_type=event_type,
                csv_path=path,
                max_rows=10000,
                on_page_callback=on_page_callback,
                is_cancelled_callback=is_cancelled_callback
            )
        except Exception as thread_err:
            usage_logger.error(f"Error in thread fetching {event_type}: {thread_err}")
            errors.append(thread_err)
            
    def run_fetch_app_signins(path):
        try:
            reports_service.fetch_app_signin_summary(
                csv_path=path,
                max_rows=5000,
                on_page_callback=on_app_signins_page_callback,
                is_cancelled_callback=is_cancelled_callback
            )
        except Exception as thread_err:
            usage_logger.error(f"Error in thread fetching app sign-ins: {thread_err}")
            errors.append(thread_err)

    def run_fetch_auth_methods(path):
        try:
            reports_service.fetch_auth_methods_summary(
                csv_path=path,
                max_rows=5000,
                on_page_callback=on_auth_methods_page_callback,
                is_cancelled_callback=is_cancelled_callback
            )
        except Exception as thread_err:
            usage_logger.error(f"Error in thread fetching auth methods: {thread_err}")
            errors.append(thread_err)
            
    try:
        t1 = threading.Thread(target=run_fetch_signins, args=("nonInteractiveUser", csv_path_noninteractive), daemon=True)
        t2 = threading.Thread(target=run_fetch_signins, args=("interactiveUser", csv_path_interactive), daemon=True)
        t3 = threading.Thread(target=run_fetch_app_signins, args=(csv_path_app_signins,), daemon=True)
        t4 = threading.Thread(target=run_fetch_auth_methods, args=(csv_path_auth_methods,), daemon=True)
        
        t1.start()
        t2.start()
        t3.start()
        t4.start()
        
        t1.join()
        t2.join()
        t3.join()
        t4.join()
        
        if len(errors) == 4:
            raise errors[0]
            
        unique_apps = set()
        unique_os = set()
        unique_browsers = set()
        app_signins = []
        auth_methods = []
        
        for path in [csv_path_noninteractive, csv_path_interactive]:
            if os.path.exists(path):
                with open(path, 'r', encoding='utf-8') as f:
                    reader = csv.reader(f)
                    next(reader, None)
                    for row in reader:
                        if len(row) >= 3:
                            app, os_name, browser = row[0], row[1], row[2]
                            if app:
                                unique_apps.add(app)
                            if os_name:
                                unique_os.add(os_name)
                            if browser:
                                unique_browsers.add(browser)
                                
        if os.path.exists(csv_path_app_signins):
            with open(csv_path_app_signins, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader, None)
                for row in reader:
                    if len(row) >= 2:
                        app_signins.append((row[0], row[1]))
                        
        if os.path.exists(csv_path_auth_methods):
            with open(csv_path_auth_methods, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader, None)
                for row in reader:
                    if len(row) >= 2:
                        auth_methods.append((row[0], row[1]))
                                
        return {
            "apps": sorted(list(unique_apps)),
            "operating_systems": sorted(list(unique_os)),
            "browsers": sorted(list(unique_browsers)),
            "app_signins": app_signins,
            "auth_methods": auth_methods
        }
    finally:
        client.close()


class DevicesAppsTelemetryFrame(ctk.CTkFrame):
    """Self-contained customtkinter component wrapping Microsoft Entra Data UI."""

    def __init__(self, master, log_callback, credentials_callback, status_change_callback, **kwargs):
        self.semaphore = kwargs.pop("concurrency_semaphore", None)
        super().__init__(master, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=12, **kwargs)
        self.log_msg = log_callback
        self.get_credentials = credentials_callback
        self.on_status_change = status_change_callback
        self.status = None
        self.last_data = {}

        self.build_ui()

    def build_ui(self):
        """Creates card container for the tab."""
        self.pack(fill="x", expand=True, pady=10)

        self.inner_pad = ctk.CTkFrame(self, fg_color="transparent")
        self.inner_pad.pack(fill="both", expand=True, padx=20, pady=20)

        ctk.CTkLabel(self.inner_pad, text="Microsoft Entra Data", font=FONT_HEADER_SMALL, text_color=COLOR_TEXT_MAIN).pack(anchor="w", pady=(0, 10))

        self.state_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.grid_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")

        self.reset_view()

    def reset_view(self):
        """Resets and hides grids."""
        self.pack_forget()
        self.state_frame.pack_forget()
        self.grid_frame.pack_forget()
        self.last_data = {}

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
            display_msg = "Reports and Audit Log reading permission required.\nPlease grant 'Reports.Read.All' and 'AuditLog.Read.All' application permissions to your App Registration in Microsoft Entra ID."

        ctk.CTkLabel(self.state_frame, text=f"✖ {display_msg}", text_color=COLOR_ERROR, font=FONT_BODY_MEDIUM, justify="center").pack(pady=(20, 10))
        ctk.CTkButton(self.state_frame, text="Try Again", command=self._retry_fetch, width=120, fg_color="transparent", border_width=1, text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER).pack(pady=(0, 20))
        self.state_frame.pack(fill="x", expand=True)

    def _retry_fetch(self):
        tenant, clients, secrets = self.get_credentials()
        if tenant:
            self.trigger_fetch(tenant, clients[0], secrets[0])

    def trigger_fetch(self, tenant, client_id, client_secret):
        """Triggers background fetch thread."""
        usage_logger.info("Entra Data trigger_fetch called. Spawning background thread...")
        self.status = "loading"
        self.on_status_change()

        self.pack(fill="x", expand=True, pady=10)
        self.grid_frame.pack_forget()

        self._set_state_loading("Downloading and parsing Microsoft Entra activities...")

        threading.Thread(
            target=self._execute_worker,
            args=(tenant, client_id, client_secret),
            daemon=True
        ).start()

    def _execute_worker(self, tenant: str, client_id: str, client_secret: str):
        usage_logger.info("Executing thread: _execute_entra_data_worker")
        if self.semaphore:
            self.semaphore.acquire()
            
        unique_apps = set()
        unique_os = set()
        unique_browsers = set()
        app_signins = []
        auth_methods = []
        
        def handle_signins_page(value_list):
            for log in value_list:
                app = log.get("appDisplayName")
                if app:
                    unique_apps.add(app)
                device = log.get("deviceDetail")
                if device:
                    os_name = device.get("operatingSystem")
                    if os_name:
                        unique_os.add(os_name)
                    browser = device.get("browser")
                    if browser:
                        unique_browsers.add(browser)
            
            data_to_render = {
                "apps": sorted(list(unique_apps)),
                "operating_systems": sorted(list(unique_os)),
                "browsers": sorted(list(unique_browsers)),
                "app_signins": list(app_signins),
                "auth_methods": list(auth_methods)
            }
            self.after(0, self._render_partial_success, data_to_render)
            
        def handle_app_signins_page(value_list):
            for item in value_list:
                app_name = item.get("appDisplayName") or ""
                success = str(item.get("successfulSignInCount") or 0)
                app_signins.append((app_name, success))
                
            data_to_render = {
                "apps": sorted(list(unique_apps)),
                "operating_systems": sorted(list(unique_os)),
                "browsers": sorted(list(unique_browsers)),
                "app_signins": list(app_signins),
                "auth_methods": list(auth_methods)
            }
            self.after(0, self._render_partial_success, data_to_render)

        def handle_auth_methods_page(value_list):
            for item in value_list:
                method = item.get("authenticationMethod") or ""
                activity = str(item.get("successActivityCount") or 0)
                auth_methods.append((method, activity))
                
            data_to_render = {
                "apps": sorted(list(unique_apps)),
                "operating_systems": sorted(list(unique_os)),
                "browsers": sorted(list(unique_browsers)),
                "app_signins": list(app_signins),
                "auth_methods": list(auth_methods)
            }
            self.after(0, self._render_partial_success, data_to_render)
            
        try:
            data = run_devices_apps_pipeline(
                client_id, 
                client_secret, 
                tenant, 
                on_page_callback=handle_signins_page,
                on_app_signins_page_callback=handle_app_signins_page,
                on_auth_methods_page_callback=handle_auth_methods_page,
                is_cancelled_callback=lambda: getattr(self, "is_cancelled", False)
            )
            usage_logger.info("Successfully completed Microsoft Entra telemetry data fetch.")
            self.after(0, self._render_success, data)
        except Exception as e:
            usage_logger.error("Exception caught in Microsoft Entra worker.", exc_info=True)
            self.after(0, self._render_error, str(e))
        finally:
            if self.semaphore:
                self.semaphore.release()

    def _render_partial_success(self, data: dict):
        if self.status == "loading":
            self._update_ui_lists(data)

    def _render_success(self, data: dict):
        self.status = "success"
        self._update_ui_lists(data)
        self.on_status_change()

    def _update_ui_lists(self, data: dict):
        self.last_data = data
        self.state_frame.pack_forget()
        for w in self.grid_frame.winfo_children():
            try:
                w.destroy()
            except Exception:
                pass

        self.grid_frame.pack(fill="x", expand=True)

        if self.status == "loading":
            progress_frame = ctk.CTkFrame(self.grid_frame, fg_color=COLOR_TONAL_BG, height=26, corner_radius=6)
            progress_frame.pack(fill="x", pady=(0, 6))
            ctk.CTkLabel(
                progress_frame, 
                text="⏳ Querying Microsoft Entra activities in the background... UI will auto-refresh.", 
                font=FONT_BODY_SMALL,
                text_color=COLOR_TONAL_TEXT
            ).pack(padx=10, pady=2, anchor="w")
        elif self.status == "cancelled" or getattr(self, "is_cancelled", False):
            progress_frame = ctk.CTkFrame(self.grid_frame, fg_color=COLOR_SURFACE_VARIANT, height=26, corner_radius=6)
            progress_frame.pack(fill="x", pady=(0, 6))
            ctk.CTkLabel(
                progress_frame, 
                text="⚠️ Fetching cancelled by user. Displaying partial data.", 
                font=FONT_BODY_SMALL,
                text_color=COLOR_ERROR
            ).pack(padx=10, pady=2, anchor="w")

        # --- Subheading 1: User Sign Ins ---
        user_signins_header = ctk.CTkFrame(self.grid_frame, fg_color="transparent")
        user_signins_header.pack(fill="x", padx=10, pady=(10, 5))
        ctk.CTkLabel(user_signins_header, text="User Sign Ins", font=FONT_SUBSECTION_HEADER, text_color=COLOR_PRIMARY).pack(anchor="w")

        rows_data = [
            ("📱 Apps Used", data.get("apps", []), "No apps found"),
            ("💻 Operating Systems", data.get("operating_systems", []), "No operating systems found"),
            ("🌐 Browsers Used", data.get("browsers", []), "No browsers found")
        ]

        for title, items, empty_msg in rows_data:
            row_frame = ctk.CTkFrame(self.grid_frame, fg_color="transparent")
            row_frame.pack(fill="x", pady=4, padx=10, anchor="w")
            
            lbl_title = ctk.CTkLabel(
                row_frame, 
                text=f"{title}: ", 
                font=FONT_BODY_BOLD, 
                text_color=COLOR_TEXT_MAIN,
                anchor="w"
            )
            lbl_title.pack(side="left", anchor="nw")
            
            display_text = ", ".join(items) if items else empty_msg
            lbl_content = ctk.CTkLabel(
                row_frame, 
                text=display_text, 
                font=FONT_BODY_MEDIUM, 
                text_color=COLOR_TEXT_MAIN if items else COLOR_TEXT_SUB,
                justify="left",
                anchor="w"
            )
            lbl_content.pack(side="left", fill="x", expand=True, anchor="nw")
            
            def make_configure_handler(lbl=lbl_content):
                def on_configure(event):
                    lbl.configure(wraplength=max(200, event.width - 180))
                return on_configure
            
            row_frame.bind("<Configure>", make_configure_handler())

        # Spacer
        ctk.CTkFrame(self.grid_frame, fg_color=COLOR_OUTLINE_LIGHT, height=1).pack(fill="x", padx=10, pady=15)

        # --- Subheading 2: App Sign Ins ---
        app_signins_header = ctk.CTkFrame(self.grid_frame, fg_color="transparent")
        app_signins_header.pack(fill="x", padx=10, pady=(0, 5))
        ctk.CTkLabel(app_signins_header, text="App Sign Ins", font=FONT_SUBSECTION_HEADER, text_color=COLOR_PRIMARY).pack(anchor="w")

        app_signins_data = data.get("app_signins", [])
        
        metrics_grid1 = ctk.CTkFrame(self.grid_frame, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)
        metrics_grid1.pack(fill="x", padx=10, pady=(5, 10))
        
        headers1 = ["App Name", "Successful Sign Ins"]
        for i in range(2):
            metrics_grid1.grid_columnconfigure(i, weight=1)
            
        for col_idx, head_text in enumerate(headers1):
            cell = ctk.CTkFrame(metrics_grid1, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")
            
        if not app_signins_data:
            c = ctk.CTkFrame(metrics_grid1, fg_color=COLOR_SURFACE, corner_radius=0)
            c.grid(row=1, column=0, columnspan=2, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c, text="No app sign-ins detected.", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="center").pack(padx=10, pady=12)
        else:
            for r_idx, (app, success) in enumerate(app_signins_data[:500], start=1): # Limit display to top 500 for UI clean layout
                bg_style = COLOR_SURFACE if r_idx % 2 != 0 else COLOR_SURFACE_VARIANT
                vals = [app, success]
                for c_idx, val in enumerate(vals):
                    c = ctk.CTkFrame(metrics_grid1, fg_color=bg_style, corner_radius=0)
                    c.grid(row=r_idx, column=c_idx, sticky="nsew", padx=0, pady=(0, 1))
                    ctk.CTkLabel(c, text=val, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="left", wraplength=350).pack(padx=10, pady=8, anchor="nw")

        # Spacer
        ctk.CTkFrame(self.grid_frame, fg_color=COLOR_OUTLINE_LIGHT, height=1).pack(fill="x", padx=10, pady=15)

        # --- Subheading 3: Authentication Methods ---
        auth_header = ctk.CTkFrame(self.grid_frame, fg_color="transparent")
        auth_header.pack(fill="x", padx=10, pady=(0, 5))
        ctk.CTkLabel(auth_header, text="Authentication Methods", font=FONT_SUBSECTION_HEADER, text_color=COLOR_PRIMARY).pack(anchor="w")

        auth_methods_data = data.get("auth_methods", [])
        
        metrics_grid2 = ctk.CTkFrame(self.grid_frame, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)
        metrics_grid2.pack(fill="x", padx=10, pady=(5, 10))
        
        headers2 = ["Authentication Method", "Success Activity Count"]
        for i in range(2):
            metrics_grid2.grid_columnconfigure(i, weight=1)
            
        for col_idx, head_text in enumerate(headers2):
            cell = ctk.CTkFrame(metrics_grid2, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")
            
        if not auth_methods_data:
            c = ctk.CTkFrame(metrics_grid2, fg_color=COLOR_SURFACE, corner_radius=0)
            c.grid(row=1, column=0, columnspan=2, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c, text="No authentication activity detected.", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="center").pack(padx=10, pady=12)
        else:
            for r_idx, (method, activity) in enumerate(auth_methods_data, start=1):
                bg_style = COLOR_SURFACE if r_idx % 2 != 0 else COLOR_SURFACE_VARIANT
                vals = [method, activity]
                for c_idx, val in enumerate(vals):
                    c = ctk.CTkFrame(metrics_grid2, fg_color=bg_style, corner_radius=0)
                    c.grid(row=r_idx, column=c_idx, sticky="nsew", padx=0, pady=(0, 1))
                    ctk.CTkLabel(c, text=val, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="left").pack(padx=10, pady=8, anchor="nw")

    def _render_error(self, err_msg):
        usage_logger.warning(f"Entra Data Telemetry fetch failed: {err_msg}")
        self._set_state_error(err_msg)
        self.status = "error"
        self.on_status_change()

    def cancel(self):
        """Cancels background thread operations."""
        self.status = None
