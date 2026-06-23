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

"""UI frame for Microsoft Entra User Sign-ins telemetry."""

import os
import csv
import logging
import threading
import customtkinter as ctk

from core.graph.entra.user_signins import run_user_signins_pipeline
from telemetry.styles import *

usage_logger = logging.getLogger("M365TelemetryAsyncLogger.UserSigninsUI")

class UserSigninsSubFrame(ctk.CTkFrame):
    """Sub-frame for Microsoft Entra User Sign Ins with unique set displays."""

    def __init__(self, master, log_callback, credentials_callback, status_change_callback, semaphore=None, **kwargs):
        super().__init__(master, fg_color="transparent", **kwargs)
        self.log_msg = log_callback
        self.get_credentials = credentials_callback
        self.on_status_change = status_change_callback
        self.semaphore = semaphore
        self.status = None
        self.is_cancelled = False
        self.current_request_id = 0
        self.csv_path = None

        self.build_ui()

    def build_ui(self):
        self.header_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.header_frame.pack(fill="x", pady=(0, 5))

        ctk.CTkLabel(self.header_frame, text="User Sign-Ins", font=FONT_SUBSECTION_HEADER, text_color=COLOR_PRIMARY).pack(side="left")

        self.btn_refresh = ctk.CTkButton(
            self.header_frame, state="disabled", text="↻ Reload", width=80, height=26, corner_radius=13,
            font=FONT_BODY_SMALL, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER,
            command=self.trigger_fetch_individual
        )
        self.btn_refresh.pack(side="right", padx=(10, 0))

        self.body_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.body_frame.pack(fill="x")

        self.reset_view()

    def reset_view(self):
        self.status = None
        self.is_cancelled = False
        self.csv_path = None
        for w in self.body_frame.winfo_children():
            w.destroy()

    def _set_state_loading(self, msg="Loading..."):
        for w in self.body_frame.winfo_children():
            w.destroy()
        ctk.CTkLabel(self.body_frame, text=f"⏳ {msg}", text_color=COLOR_TEXT_SUB, font=FONT_BODY_MEDIUM).pack(pady=10)

    def _set_state_error(self, error_msg):
        for w in self.body_frame.winfo_children():
            w.destroy()
        display_msg = error_msg
        if "401" in error_msg or "403" in error_msg or "unauthorized" in error_msg.lower() or "forbidden" in error_msg.lower() or "auditlog.read.all" in error_msg.lower():
            display_msg = "AuditLog.Read.All application permission required.\nPlease grant 'AuditLog.Read.All' to your App Registration in Microsoft Entra ID."
        ctk.CTkLabel(self.body_frame, text=f"✖ {display_msg}", text_color=COLOR_ERROR, font=FONT_BODY_MEDIUM, justify="center").pack(pady=(10, 5))
        ctk.CTkButton(self.body_frame, text="Try Again", command=self.trigger_fetch_individual, width=120, fg_color="transparent", border_width=1, text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER).pack(pady=(0, 10))

    def trigger_fetch_individual(self):
        tenant, clients, secrets = self.get_credentials()
        if tenant:
            self.trigger_fetch(tenant, clients[0], secrets[0])
        else:
            self._set_state_error("Missing credentials. Please submit the credentials above.")

    def trigger_fetch(self, tenant, client_id, client_secret):
        self.status = "loading"
        self.is_cancelled = False

        script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
        if os.path.basename(script_dir) == "entra":
            script_dir = os.path.dirname(script_dir)
        reports_dir = os.path.join(script_dir, "reports", f"{tenant}_{client_id}")
        self.csv_path = os.path.join(reports_dir, "entra_user_signins.csv")

        self._set_state_loading("Downloading and parsing User Sign-Ins...")
        self.on_status_change()
        if hasattr(self, "btn_refresh") and self.btn_refresh.winfo_exists():
            self.btn_refresh.configure(state="disabled")

        threading.Thread(
            target=self._execute_worker,
            args=(tenant, client_id, client_secret, self.current_request_id),
            daemon=True
        ).start()

    def _execute_worker(self, tenant, client_id, client_secret, request_id):
        if self.semaphore:
            self.semaphore.acquire()
        try:
            if self.is_cancelled or request_id != self.current_request_id:
                return

            unique_apps = set()
            unique_os = set()
            unique_browsers = set()
            page_count = 0

            def handle_page(filtered_list):
                nonlocal unique_apps, unique_os, unique_browsers, page_count
                if self.is_cancelled or request_id != self.current_request_id:
                    return
                for item in filtered_list:
                    app_name = item.get("appDisplayName") or ""
                    device = item.get("deviceDetail") or {}
                    os_name = device.get("operatingSystem") or ""
                    browser_name = device.get("browser") or ""
                    
                    if app_name: unique_apps.add(app_name)
                    if os_name: unique_os.add(os_name)
                    if browser_name: unique_browsers.add(browser_name)
                    
                page_count += 1
                if page_count % 3 == 0:
                    current_data = {
                        "apps": sorted(list(unique_apps)),
                        "os": sorted(list(unique_os)),
                        "browsers": sorted(list(unique_browsers))
                    }
                    self.after(0, self._render_partial, current_data, request_id)

            temp_csv_path = self.csv_path + ".tmp"
            script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
            if os.path.basename(script_dir) == "entra":
                script_dir = os.path.dirname(script_dir)
            reports_dir = os.path.join(script_dir, "reports", f"{tenant}_{client_id}")
            os.makedirs(reports_dir, exist_ok=True)
            
            with open(temp_csv_path, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["appDisplayName", "operatingSystem", "browser", "isInteractive"])

            run_user_signins_pipeline(
                client_id=client_id,
                client_secret=client_secret,
                tenant_id=tenant,
                csv_path=temp_csv_path,
                max_rows=20000,
                on_page_callback=handle_page,
                is_cancelled_callback=lambda: self.is_cancelled or request_id != self.current_request_id
            )

            # Compile final list
            final_data = {
                "apps": sorted(list(unique_apps)),
                "os": sorted(list(unique_os)),
                "browsers": sorted(list(unique_browsers))
            }

            if not self.is_cancelled and request_id == self.current_request_id:
                if os.path.exists(temp_csv_path):
                    if os.path.exists(self.csv_path):
                        os.remove(self.csv_path)
                    os.rename(temp_csv_path, self.csv_path)
                self.after(0, self._render_success, final_data, request_id)
        except Exception as e:
            usage_logger.error(f"Error fetching user sign-ins: {e}", exc_info=True)
            if not self.is_cancelled and request_id == self.current_request_id:
                self.after(0, self._render_error, str(e), request_id)
        finally:
            if 'temp_csv_path' in locals() and os.path.exists(temp_csv_path):
                try:
                    os.remove(temp_csv_path)
                except Exception:
                    pass
            if self.semaphore:
                self.semaphore.release()

    def _render_partial(self, data, request_id):
        if self.is_cancelled or request_id != self.current_request_id:
            return
        self._update_ui(data, is_partial=True)

    def _render_success(self, data, request_id):
        if self.is_cancelled or request_id != self.current_request_id:
            return
        self.status = "success"
        self._update_ui(data=None, is_partial=False)
        self.on_status_change()
        if hasattr(self, "btn_refresh") and self.btn_refresh.winfo_exists():
            self.btn_refresh.configure(state="normal")

    def _render_error(self, err_msg, request_id):
        if self.is_cancelled or request_id != self.current_request_id:
            return
        self.status = "error"
        self._set_state_error(err_msg)
        self.on_status_change()
        if hasattr(self, "btn_refresh") and self.btn_refresh.winfo_exists():
            self.btn_refresh.configure(state="normal")

    def _load_data_from_csv(self):
        if not self.csv_path or not os.path.exists(self.csv_path):
            tenant, clients, secrets = self.get_credentials()
            if tenant and clients:
                script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
                if os.path.basename(script_dir) == "entra":
                    script_dir = os.path.dirname(script_dir)
                self.csv_path = os.path.join(script_dir, "reports", f"{tenant}_{clients[0]}", "entra_user_signins.csv")
            else:
                return {"apps": [], "os": [], "browsers": []}
                
        if not os.path.exists(self.csv_path):
            return {"apps": [], "os": [], "browsers": []}
            
        unique_apps = set()
        unique_os = set()
        unique_browsers = set()
        try:
            with open(self.csv_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader, None)  # skip header
                for row in reader:
                    if len(row) >= 3:
                        if row[0]: unique_apps.add(row[0])
                        if row[1]: unique_os.add(row[1])
                        if row[2]: unique_browsers.add(row[2])
        except Exception as e:
            usage_logger.error(f"Error reading CSV for User Sign-ins: {e}", exc_info=True)
            
        return {
            "apps": sorted(list(unique_apps)),
            "os": sorted(list(unique_os)),
            "browsers": sorted(list(unique_browsers))
        }

    def _update_ui(self, data=None, is_partial=False):
        for w in self.body_frame.winfo_children():
            w.destroy()

        if is_partial:
            progress_frame = ctk.CTkFrame(self.body_frame, fg_color=COLOR_TONAL_BG, height=26, corner_radius=6)
            progress_frame.pack(fill="x", pady=(0, 6))
            ctk.CTkLabel(
                progress_frame,
                text="⏳ Querying User Sign-Ins in the background... UI will auto-refresh.",
                font=FONT_BODY_SMALL,
                text_color=COLOR_TONAL_TEXT
            ).pack(padx=10, pady=2, anchor="w")

        if data is None:
            data = self._load_data_from_csv()

        metrics_grid = ctk.CTkFrame(self.body_frame, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)
        metrics_grid.pack(fill="x", pady=(5, 10))

        headers = ["Sign-in Attribute", "Successful Unique Values"]
        for i in range(2):
            metrics_grid.grid_columnconfigure(i, weight=1 if i == 0 else 3)

        for col_idx, head_text in enumerate(headers):
            cell = ctk.CTkFrame(metrics_grid, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        apps_str = ", ".join(data.get("apps", [])) or "None"
        os_str = ", ".join(data.get("os", [])) or "None"
        browsers_str = ", ".join(data.get("browsers", [])) or "None"

        rows = [
            ("Successful App Sign-ins", apps_str),
            ("Successful Client OS", os_str),
            ("Successful Browsers", browsers_str)
        ]

        for r_idx, (attribute, val_str) in enumerate(rows, start=1):
            bg_style = COLOR_SURFACE if r_idx % 2 != 0 else COLOR_SURFACE_VARIANT
            
            c0 = ctk.CTkFrame(metrics_grid, fg_color=bg_style, corner_radius=0)
            c0.grid(row=r_idx, column=0, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c0, text=attribute, font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN, justify="left").pack(padx=10, pady=8, anchor="nw")
            
            c1 = ctk.CTkFrame(metrics_grid, fg_color=bg_style, corner_radius=0)
            c1.grid(row=r_idx, column=1, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c1, text=val_str, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="left", wraplength=650).pack(padx=10, pady=8, anchor="nw")

        # Footnote disclaimer
        ctk.CTkLabel(
            self.body_frame,
            text="* Based on sample data collected from signins.",
            font=FONT_BODY_SMALL,
            text_color=COLOR_TEXT_SUB,
            justify="left"
        ).pack(anchor="w", padx=10, pady=(5, 10))

    @property
    def last_data(self):
        return self._load_data_from_csv()

    def cancel(self):
        self.is_cancelled = True
        self.current_request_id += 1
        if self.status == "loading":
            self.status = "cancelled"
            self._update_ui(data=None, is_partial=False)
        if hasattr(self, "btn_refresh") and self.btn_refresh.winfo_exists():
            self.btn_refresh.configure(state="normal")
