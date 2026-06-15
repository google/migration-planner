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



def run_user_signins_pipeline(client_id: str, client_secret: str, tenant_id: str, on_page_callback=None) -> dict:
    from core.graph.client import GraphClient
    from core.graph.security import SecurityService
    import csv, os
    client = GraphClient(tenant_id=tenant_id, client_ids=client_id, client_secrets=client_secret, concurrency=4, retries=5, backoff=2)
    client.authenticate()
    security_service = SecurityService(client)
    
    script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
    reports_dir = os.path.join(script_dir, "reports", f"{tenant_id}_{client_id}")
    os.makedirs(reports_dir, exist_ok=True)
    
    csv_path_interactive = os.path.join(reports_dir, "signin_interactive.csv")
    csv_path_noninteractive = os.path.join(reports_dir, "signin_noninteractive.csv")
    
    for path in [csv_path_interactive, csv_path_noninteractive]:
        with open(path, 'w', encoding='utf-8', newline='') as f:
            csv.writer(f).writerow(["appDisplayName", "operatingSystem", "browser", "signInEventType"])
            
    import threading
    errors = []
    
    unique_apps = set()
    unique_os = set()
    unique_browsers = set()
    
    def handle_page(value_list):
        for item in value_list:
            app = item.get("appDisplayName")
            device = item.get("deviceDetail") or {}
            os_name = device.get("operatingSystem")
            browser = device.get("browser")
            if app: unique_apps.add(app)
            if os_name: unique_os.add(os_name)
            if browser: unique_browsers.add(browser)
            
        if on_page_callback:
            try:
                on_page_callback({
                    "apps": sorted(list(unique_apps)),
                    "operating_systems": sorted(list(unique_os)),
                    "browsers": sorted(list(unique_browsers))
                })
            except Exception as e:
                errors.append(e)

    def run_fetch_signins(event_type, path):
        try:
            security_service.fetch_signin_activities(event_type=event_type, csv_path=path, max_rows=10000, on_page_callback=handle_page)
        except Exception as e:
            errors.append(e)

    t1 = threading.Thread(target=run_fetch_signins, args=("nonInteractiveUser", csv_path_noninteractive), daemon=True)
    t2 = threading.Thread(target=run_fetch_signins, args=("interactiveUser", csv_path_interactive), daemon=True)
    t1.start(); t2.start()
    t1.join(); t2.join()
    
    client.close()
    
    if errors: raise errors[0]
    
    unique_apps = set()
    unique_os = set()
    unique_browsers = set()
    
    for path in [csv_path_noninteractive, csv_path_interactive]:
        if os.path.exists(path):
            with open(path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                header = next(reader, None)
                for row in reader:
                    if len(row) >= 3:
                        if row[0]: unique_apps.add(row[0])
                        if row[1]: unique_os.add(row[1])
                        if row[2]: unique_browsers.add(row[2])
                        
    return {
        "apps": sorted(list(unique_apps)),
        "operating_systems": sorted(list(unique_os)),
        "browsers": sorted(list(unique_browsers))
    }

def run_app_signins_pipeline(client_id: str, client_secret: str, tenant_id: str) -> dict:
    from core.graph.client import GraphClient
    from core.graph.reports import ReportsService
    import csv, os
    client = GraphClient(tenant_id=tenant_id, client_ids=client_id, client_secrets=client_secret, concurrency=4, retries=5, backoff=2)
    client.authenticate()
    reports_service = ReportsService(client)
    
    script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
    reports_dir = os.path.join(script_dir, "reports", f"{tenant_id}_{client_id}")
    os.makedirs(reports_dir, exist_ok=True)
    
    csv_path_app_signins = os.path.join(reports_dir, "entra_app_signins.csv")
    with open(csv_path_app_signins, 'w', encoding='utf-8', newline='') as f:
        csv.writer(f).writerow(["appDisplayName", "successSignInCount"])
        
    reports_service.fetch_app_signin_summary(csv_path=csv_path_app_signins, max_rows=5000)
    client.close()
    
    app_signins = []
    if os.path.exists(csv_path_app_signins):
        with open(csv_path_app_signins, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader, None)
            for row in reader:
                if len(row) >= 2:
                    app_signins.append((row[0], row[1]))
                    
    return {"app_signins": app_signins}

def run_auth_methods_pipeline(client_id: str, client_secret: str, tenant_id: str) -> dict:
    from core.graph.client import GraphClient
    from core.graph.reports import ReportsService
    import csv, os
    client = GraphClient(tenant_id=tenant_id, client_ids=client_id, client_secrets=client_secret, concurrency=4, retries=5, backoff=2)
    client.authenticate()
    reports_service = ReportsService(client)
    
    script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
    reports_dir = os.path.join(script_dir, "reports", f"{tenant_id}_{client_id}")
    os.makedirs(reports_dir, exist_ok=True)
    
    csv_path_auth_methods = os.path.join(reports_dir, "entra_auth_methods.csv")
    with open(csv_path_auth_methods, 'w', encoding='utf-8', newline='') as f:
        csv.writer(f).writerow(["authenticationMethod", "successActivityCount"])
        
    reports_service.fetch_auth_methods_summary(csv_path=csv_path_auth_methods, max_rows=5000)
    client.close()
    
    auth_methods = []
    if os.path.exists(csv_path_auth_methods):
        with open(csv_path_auth_methods, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader, None)
            for row in reader:
                if len(row) >= 2:
                    auth_methods.append((row[0], row[1]))
                    
    return {"auth_methods": auth_methods}


class DevicesAppsTelemetryFrame(ctk.CTkFrame):
    def __init__(self, master, log_callback, credentials_callback, status_change_callback, **kwargs):
        self.semaphore = kwargs.pop("concurrency_semaphore", None)
        super().__init__(master, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=12, **kwargs)
        self.log_msg = log_callback
        self.get_credentials = credentials_callback
        self.on_status_change = status_change_callback
        self.build_ui()

    def build_ui(self):
        self.pack(fill="x", expand=True, pady=10)
        self.inner_pad = ctk.CTkFrame(self, fg_color="transparent")
        self.inner_pad.pack(fill="both", expand=True, padx=20, pady=20)

        ctk.CTkLabel(self.inner_pad, text="Microsoft Entra Data", font=FONT_HEADER_SMALL, text_color=COLOR_TEXT_MAIN).pack(anchor="w", pady=(0, 20))

        # 1. User Sign Ins
        self.us_header_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.us_header_frame.pack(fill="x", pady=(0, 10))
        ctk.CTkLabel(self.us_header_frame, text="User Sign Ins", font=FONT_HEADER_SMALL, text_color=COLOR_TEXT_MAIN).pack(side="left")
        self.us_reload_btn = ctk.CTkButton(
            self.us_header_frame, text="↻ Reload", width=80, height=24,
            font=__import__("customtkinter").CTkFont(family="Segoe UI", size=12),
            fg_color="transparent", border_width=1, text_color="#2563EB", hover_color="#DBEAFE",
            command=self._retry_us_fetch
        )
        self.us_reload_btn.pack(side="right")
        self.us_grid_frame = ctk.CTkFrame(self.inner_pad, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)
        self.us_grid_frame.pack(fill="x", expand=True, pady=(0, 20))

        # 2. App Sign Ins
        self.as_header_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.as_header_frame.pack(fill="x", pady=(0, 10))
        ctk.CTkLabel(self.as_header_frame, text="App Sign Ins", font=FONT_HEADER_SMALL, text_color=COLOR_TEXT_MAIN).pack(side="left")
        self.as_reload_btn = ctk.CTkButton(
            self.as_header_frame, text="↻ Reload", width=80, height=24,
            font=__import__("customtkinter").CTkFont(family="Segoe UI", size=12),
            fg_color="transparent", border_width=1, text_color="#2563EB", hover_color="#DBEAFE",
            command=self._retry_as_fetch
        )
        self.as_reload_btn.pack(side="right")
        self.as_grid_frame = ctk.CTkFrame(self.inner_pad, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)
        self.as_grid_frame.pack(fill="x", expand=True, pady=(0, 20))

        # 3. Auth Methods
        self.am_header_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.am_header_frame.pack(fill="x", pady=(0, 10))
        ctk.CTkLabel(self.am_header_frame, text="Authentication Methods", font=FONT_HEADER_SMALL, text_color=COLOR_TEXT_MAIN).pack(side="left")
        self.am_reload_btn = ctk.CTkButton(
            self.am_header_frame, text="↻ Reload", width=80, height=24,
            font=__import__("customtkinter").CTkFont(family="Segoe UI", size=12),
            fg_color="transparent", border_width=1, text_color="#2563EB", hover_color="#DBEAFE",
            command=self._retry_am_fetch
        )
        self.am_reload_btn.pack(side="right")
        self.am_grid_frame = ctk.CTkFrame(self.inner_pad, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)
        self.am_grid_frame.pack(fill="x", expand=True)
        self.reset_view()

    def reset_view(self):
        self.pack_forget()
        self.status = None
        for w in self.us_grid_frame.winfo_children(): w.destroy()
        for w in self.as_grid_frame.winfo_children(): w.destroy()
        for w in self.am_grid_frame.winfo_children(): w.destroy()

    def trigger_fetch(self, tenant, client_id, client_secret):
        self.pack(fill="x", expand=True, pady=10)
        self.trigger_us_fetch(tenant, client_id, client_secret)
        self.trigger_as_fetch(tenant, client_id, client_secret)
        self.trigger_am_fetch(tenant, client_id, client_secret)

    def _retry_us_fetch(self):
        tenant, clients, secrets = self.get_credentials()
        if tenant and clients and secrets:
            self.us_reload_btn.configure(state="disabled")
            self.trigger_us_fetch(tenant, clients[0], secrets[0])

    def _retry_as_fetch(self):
        tenant, clients, secrets = self.get_credentials()
        if tenant and clients and secrets:
            self.as_reload_btn.configure(state="disabled")
            self.trigger_as_fetch(tenant, clients[0], secrets[0])

    def _retry_am_fetch(self):
        tenant, clients, secrets = self.get_credentials()
        if tenant and clients and secrets:
            self.am_reload_btn.configure(state="disabled")
            self.trigger_am_fetch(tenant, clients[0], secrets[0])

    def trigger_us_fetch(self, tenant, client_id, client_secret):
        for w in self.us_grid_frame.winfo_children(): w.destroy()
        f = ctk.CTkFrame(self.us_grid_frame, fg_color="transparent")
        ctk.CTkLabel(f, text="⏳ Analyzing User Sign Ins...", text_color=COLOR_TEXT_SUB, font=FONT_BODY_MEDIUM).pack(pady=(20, 5))
        pb = ctk.CTkProgressBar(f, mode="indeterminate", width=250, fg_color=COLOR_SURFACE_VARIANT, progress_color=COLOR_PRIMARY)
        pb.pack(pady=(0, 20))
        pb.start()
        self.us_status = "loading"
        f.pack(fill="x", expand=True)
        threading.Thread(target=self._execute_us_worker, args=(tenant, client_id, client_secret), daemon=True).start()

    def trigger_as_fetch(self, tenant, client_id, client_secret):
        for w in self.as_grid_frame.winfo_children(): w.destroy()
        f = ctk.CTkFrame(self.as_grid_frame, fg_color="transparent")
        ctk.CTkLabel(f, text="⏳ Analyzing App Sign Ins...", text_color=COLOR_TEXT_SUB, font=FONT_BODY_MEDIUM).pack(pady=(20, 5))
        pb = ctk.CTkProgressBar(f, mode="indeterminate", width=250, fg_color=COLOR_SURFACE_VARIANT, progress_color=COLOR_PRIMARY)
        pb.pack(pady=(0, 20))
        pb.start()
        f.pack(fill="x", expand=True)
        threading.Thread(target=self._execute_as_worker, args=(tenant, client_id, client_secret), daemon=True).start()

    def trigger_am_fetch(self, tenant, client_id, client_secret):
        for w in self.am_grid_frame.winfo_children(): w.destroy()
        f = ctk.CTkFrame(self.am_grid_frame, fg_color="transparent")
        ctk.CTkLabel(f, text="⏳ Analyzing Auth Methods...", text_color=COLOR_TEXT_SUB, font=FONT_BODY_MEDIUM).pack(pady=(20, 5))
        pb = ctk.CTkProgressBar(f, mode="indeterminate", width=250, fg_color=COLOR_SURFACE_VARIANT, progress_color=COLOR_PRIMARY)
        pb.pack(pady=(0, 20))
        pb.start()
        f.pack(fill="x", expand=True)
        threading.Thread(target=self._execute_am_worker, args=(tenant, client_id, client_secret), daemon=True).start()

    def _execute_us_worker(self, tenant, client_id, client_secret):
        if self.semaphore: self.semaphore.acquire()
        try:
            data = run_user_signins_pipeline(
                client_id, client_secret, tenant,
                on_page_callback=lambda d: self.after(0, self._render_us_success, d, True)
            )
            self.after(0, self._render_us_success, data, False)
        except Exception as e:
            self.after(0, self._render_us_error, str(e))
        finally:
            if self.semaphore: self.semaphore.release()

    def _execute_as_worker(self, tenant, client_id, client_secret):
        if self.semaphore: self.semaphore.acquire()
        try:
            data = run_app_signins_pipeline(client_id, client_secret, tenant)
            self.after(0, self._render_as_success, data)
        except Exception as e:
            self.after(0, self._render_as_error, str(e))
        finally:
            if self.semaphore: self.semaphore.release()

    def _execute_am_worker(self, tenant, client_id, client_secret):
        if self.semaphore: self.semaphore.acquire()
        try:
            data = run_auth_methods_pipeline(client_id, client_secret, tenant)
            self.after(0, self._render_am_success, data)
        except Exception as e:
            self.after(0, self._render_am_error, str(e))
        finally:
            if self.semaphore: self.semaphore.release()

    def _render_us_error(self, err_msg):
        self.us_reload_btn.configure(state="normal")
        self.us_status = "error"
        for w in self.us_grid_frame.winfo_children(): w.destroy()
        ctk.CTkLabel(self.us_grid_frame, text=f"✖ Error: {err_msg}", text_color=COLOR_ERROR, font=FONT_BODY_MEDIUM).pack(pady=20)
        self.status = "error"
        self.on_status_change()

    def _render_as_error(self, err_msg):
        self.as_reload_btn.configure(state="normal")
        for w in self.as_grid_frame.winfo_children(): w.destroy()
        ctk.CTkLabel(self.as_grid_frame, text=f"✖ Error: {err_msg}", text_color=COLOR_ERROR, font=FONT_BODY_MEDIUM).pack(pady=20)
        self.status = "error"
        self.on_status_change()

    def _render_am_error(self, err_msg):
        self.am_reload_btn.configure(state="normal")
        for w in self.am_grid_frame.winfo_children(): w.destroy()
        ctk.CTkLabel(self.am_grid_frame, text=f"✖ Error: {err_msg}", text_color=COLOR_ERROR, font=FONT_BODY_MEDIUM).pack(pady=20)
        self.status = "error"
        self.on_status_change()

    def _render_us_success(self, data: dict, is_partial=False):
        if not is_partial:
            self.us_reload_btn.configure(state="normal")
            self.us_status = "success"
            
        for w in self.us_grid_frame.winfo_children(): w.destroy()
        
        if is_partial and getattr(self, "us_status", None) == "loading":
            progress_frame = ctk.CTkFrame(self.us_grid_frame, fg_color=COLOR_TONAL_BG, height=26, corner_radius=6)
            progress_frame.pack(fill="x", pady=(0, 6))
            ctk.CTkLabel(
                progress_frame, 
                text="⏳ Querying activities... UI will auto-refresh.", 
                font=FONT_BODY_SMALL,
                text_color=COLOR_TONAL_TEXT
            ).pack(padx=10, pady=2, anchor="w")
            
        rows_data = [
            ("📱 Apps Used", data.get("apps", []), "No apps found"),
            ("💻 Operating Systems", data.get("operating_systems", []), "No operating systems found"),
            ("🌐 Browsers Used", data.get("browsers", []), "No browsers found")
        ]

        for title, items, empty_msg in rows_data:
            row_frame = ctk.CTkFrame(self.us_grid_frame, fg_color="transparent")
            row_frame.pack(fill="x", pady=4, padx=10, anchor="w")
            ctk.CTkLabel(row_frame, text=f"{title}: ", font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN, anchor="w").pack(side="left", anchor="nw")
            display_text = ", ".join(items) if items else empty_msg
            lbl_content = ctk.CTkLabel(row_frame, text=display_text, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN if items else COLOR_TEXT_SUB, justify="left", anchor="w")
            lbl_content.pack(side="left", fill="x", expand=True, anchor="nw")
            
            def make_configure_handler(lbl=lbl_content):
                def on_configure(event):
                    lbl.configure(wraplength=max(200, event.width - 180))
                return on_configure
            row_frame.bind("<Configure>", make_configure_handler())
        self.status = "success"
        self.on_status_change()

    def _render_as_success(self, data: dict):
        self.as_reload_btn.configure(state="normal")
        for w in self.as_grid_frame.winfo_children(): w.destroy()
        
        app_signins_data = data.get("app_signins", [])
        headers1 = ["App Name", "Successful Sign Ins"]
        for i in range(2):
            self.as_grid_frame.grid_columnconfigure(i, weight=1)
            
        for col_idx, head_text in enumerate(headers1):
            cell = ctk.CTkFrame(self.as_grid_frame, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")
            
        if not app_signins_data:
            c = ctk.CTkFrame(self.as_grid_frame, fg_color=COLOR_SURFACE, corner_radius=0)
            c.grid(row=1, column=0, columnspan=2, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c, text="No app sign-ins detected.", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="center").pack(padx=10, pady=12)
        else:
            for r_idx, (app, success) in enumerate(app_signins_data[:500], start=1):
                bg_style = "transparent" if r_idx % 2 != 0 else COLOR_SURFACE_VARIANT
                vals = [app, success]
                for c_idx, val in enumerate(vals):
                    c = ctk.CTkFrame(self.as_grid_frame, fg_color=bg_style, corner_radius=0)
                    c.grid(row=r_idx, column=c_idx, sticky="nsew", padx=0, pady=(0, 1))
                    ctk.CTkLabel(c, text=val, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="left", wraplength=350).pack(padx=10, pady=8, anchor="nw")
        self.status = "success"
        self.on_status_change()

    def _render_am_success(self, data: dict):
        self.am_reload_btn.configure(state="normal")
        for w in self.am_grid_frame.winfo_children(): w.destroy()
        
        auth_methods_data = data.get("auth_methods", [])
        headers2 = ["Authentication Method", "Success Activity Count"]
        for i in range(2):
            self.am_grid_frame.grid_columnconfigure(i, weight=1)
            
        for col_idx, head_text in enumerate(headers2):
            cell = ctk.CTkFrame(self.am_grid_frame, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")
            
        if not auth_methods_data:
            c = ctk.CTkFrame(self.am_grid_frame, fg_color=COLOR_SURFACE, corner_radius=0)
            c.grid(row=1, column=0, columnspan=2, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c, text="No authentication activity detected.", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="center").pack(padx=10, pady=12)
        else:
            for r_idx, (method, activity) in enumerate(auth_methods_data, start=1):
                bg_style = "transparent" if r_idx % 2 != 0 else COLOR_SURFACE_VARIANT
                vals = [method, activity]
                for c_idx, val in enumerate(vals):
                    c = ctk.CTkFrame(self.am_grid_frame, fg_color=bg_style, corner_radius=0)
                    c.grid(row=r_idx, column=c_idx, sticky="nsew", padx=0, pady=(0, 1))
                    ctk.CTkLabel(c, text=val, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="left").pack(padx=10, pady=8, anchor="nw")
        self.status = "success"
        self.on_status_change()

    def cancel(self):
        pass
