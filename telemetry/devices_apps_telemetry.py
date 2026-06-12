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

"""Modular Devices & Apps telemetry scanners and visual interfaces."""

import logging
import threading
from typing import Any, Dict, List, Optional
import customtkinter as ctk

# Bind to the async logger initialized in m365_telemetry.py
usage_logger = logging.getLogger("M365TelemetryAsyncLogger")

# Import unified core service layer
from core.graph.client import GraphClient
from core.graph.security import SecurityService

# Import shared styles
from telemetry.styles import *


def run_devices_apps_pipeline(client_id: str, client_secret: str, tenant_id: str, on_page_callback=None, is_cancelled_callback=None) -> dict:
    """Pipeline to fetch interactive and non-interactive sign-in logs in parallel and aggregate unique apps, OS, and browsers."""
    usage_logger.info("Starting Devices & Apps Telemetry Pipeline in parallel...")
    
    import os
    script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
    reports_dir = os.path.join(script_dir, "reports", f"{tenant_id}_{client_id}")
    os.makedirs(reports_dir, exist_ok=True)
    
    csv_path_interactive = os.path.join(reports_dir, "signin_interactive.csv")
    csv_path_noninteractive = os.path.join(reports_dir, "signin_noninteractive.csv")
    
    import csv
    for path in [csv_path_interactive, csv_path_noninteractive]:
        with open(path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["appDisplayName", "operatingSystem", "browser", "signInEventType"])
            
    client = GraphClient(
        tenant_id=tenant_id,
        client_ids=client_id,
        client_secrets=client_secret,
        concurrency=2,
        retries=5,
        backoff=2
    )
    client.authenticate()
    service = SecurityService(client)
    
    errors = []
    import threading
    
    def run_fetch(event_type, path):
        try:
            service.fetch_signin_activities(
                event_type=event_type,
                csv_path=path,
                max_rows=10000,
                on_page_callback=on_page_callback,
                is_cancelled_callback=is_cancelled_callback
            )
        except Exception as thread_err:
            usage_logger.error(f"Error in thread fetching {event_type}: {thread_err}")
            errors.append(thread_err)
            
    try:
        t1 = threading.Thread(target=run_fetch, args=("nonInteractiveUser", csv_path_noninteractive), daemon=True)
        t2 = threading.Thread(target=run_fetch, args=("interactiveUser", csv_path_interactive), daemon=True)
        
        t1.start()
        t2.start()
        
        t1.join()
        t2.join()
        
        if len(errors) == 2:
            raise errors[0]
            
        unique_apps = set()
        unique_os = set()
        unique_browsers = set()
        
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
                                
        return {
            "apps": sorted(list(unique_apps)),
            "operating_systems": sorted(list(unique_os)),
            "browsers": sorted(list(unique_browsers))
        }
    finally:
        client.close()


class DevicesAppsTelemetryFrame(ctk.CTkFrame):
    """Self-contained customtkinter component wrapping Devices & Apps Telemetry UI with a 3-column scrolling list layout."""

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

        ctk.CTkLabel(self.inner_pad, text="Devices & Apps Summary (Sign-in Telemetry)", font=FONT_HEADER_SMALL, text_color=COLOR_TEXT_MAIN).pack(anchor="w", pady=(0, 10))

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
            display_msg = "Audit log reading permission required.\nPlease grant the 'AuditLog.Read.All' application permission to your App Registration in Microsoft Entra ID."

        ctk.CTkLabel(self.state_frame, text=f"✖ {display_msg}", text_color=COLOR_ERROR, font=FONT_BODY_MEDIUM, justify="center").pack(pady=(20, 10))
        ctk.CTkButton(self.state_frame, text="Try Again", command=self._retry_fetch, width=120, fg_color="transparent", border_width=1, text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER).pack(pady=(0, 20))
        self.state_frame.pack(fill="x", expand=True)

    def _retry_fetch(self):
        tenant, clients, secrets = self.get_credentials()
        if tenant:
            self.trigger_fetch(tenant, clients[0], secrets[0])

    def trigger_fetch(self, tenant, client_id, client_secret):
        """Triggers background fetch thread."""
        usage_logger.info("Devices & Apps trigger_fetch called. Spawning background thread...")
        self.status = "loading"
        self.on_status_change()

        self.pack(fill="x", expand=True, pady=10)
        self.grid_frame.pack_forget()

        self._set_state_loading("Downloading and parsing user sign-in activities...")

        threading.Thread(
            target=self._execute_worker,
            args=(tenant, client_id, client_secret),
            daemon=True
        ).start()

    def _execute_worker(self, tenant: str, client_id: str, client_secret: str):
        usage_logger.info("Executing thread: _execute_devices_apps_worker")
        if self.semaphore:
            self.semaphore.acquire()
            
        unique_apps = set()
        unique_os = set()
        unique_browsers = set()
        
        def handle_page(value_list):
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
            
            # Update UI dynamically with sorted lists
            data_to_render = {
                "apps": sorted(list(unique_apps)),
                "operating_systems": sorted(list(unique_os)),
                "browsers": sorted(list(unique_browsers))
            }
            self.after(0, self._render_partial_success, data_to_render)
            
        try:
            data = run_devices_apps_pipeline(
                client_id, 
                client_secret, 
                tenant, 
                on_page_callback=handle_page,
                is_cancelled_callback=lambda: getattr(self, "is_cancelled", False)
            )
            usage_logger.info("Successfully completed Devices & Apps telemetry data fetch.")
            self.after(0, self._render_success, data)
        except Exception as e:
            usage_logger.error("Exception caught in Devices & Apps worker.", exc_info=True)
            self.after(0, self._render_error, str(e))
        finally:
            if self.semaphore:
                self.semaphore.release()

    def _render_partial_success(self, data: dict):
        if self.status == "loading":  # Only update if still loading
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
                text="⏳ Querying Microsoft Graph sign-in activities in the background... UI will auto-refresh.", 
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

        rows_data = [
            ("📱 Apps Used", data.get("apps", []), "No apps found"),
            ("💻 Operating Systems", data.get("operating_systems", []), "No operating systems found"),
            ("🌐 Browsers Used", data.get("browsers", []), "No browsers found")
        ]

        for idx, (title, items, empty_msg) in enumerate(rows_data):
            row_frame = ctk.CTkFrame(self.grid_frame, fg_color="transparent")
            row_frame.pack(fill="x", pady=6, anchor="w")
            
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
            
            # Dynamically adjust wraplength based on parent row frame width changes
            def make_configure_handler(lbl=lbl_content):
                def on_configure(event):
                    lbl.configure(wraplength=max(200, event.width - 180))
                return on_configure
            
            row_frame.bind("<Configure>", make_configure_handler())

    def _render_error(self, err_msg):
        usage_logger.warning(f"Devices & Apps Telemetry fetch failed: {err_msg}")
        self._set_state_error(err_msg)
        self.status = "error"
        self.on_status_change()

    def cancel(self):
        """Cancels background thread operations."""
        self.status = None
