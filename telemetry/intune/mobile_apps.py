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

"""UI component for Managed Mobile Apps."""

import os
import csv
import logging
import threading
import customtkinter as ctk

from core.graph.intune.mobile_apps import run_mobile_apps_pipeline
from telemetry.styles import *

usage_logger = logging.getLogger("M365TelemetryAsyncLogger.MobileAppsUI")

class MobileAppsSubFrame(ctk.CTkFrame):
    """Sub-frame for Managed Mobile Apps telemetry."""

    def __init__(self, master, log_callback, credentials_callback, status_change_callback, semaphore=None, **kwargs):
        super().__init__(master, fg_color="transparent", **kwargs)
        self.log_msg = log_callback
        self.get_credentials = credentials_callback
        self.on_status_change = status_change_callback
        self.semaphore = semaphore
        self.status = None
        self.csv_path = None
        self.is_cancelled = False
        self._cached_apps = []

        self.build_ui()

    def build_ui(self):
        self.inner_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.inner_frame.pack(fill="x", padx=10, pady=(10, 5))
        
        self.lbl_title = ctk.CTkLabel(
            self.inner_frame, 
            text="⚙️ Managed Mobile Apps: ", 
            font=FONT_BODY_BOLD, 
            text_color=COLOR_TEXT_MAIN,
            anchor="w"
        )
        self.lbl_title.pack(side="left", anchor="nw")
        
        self.lbl_content = ctk.CTkLabel(
            self.inner_frame, 
            text="No apps found or scanning...", 
            font=FONT_BODY_MEDIUM, 
            text_color=COLOR_TEXT_SUB,
            justify="left",
            anchor="w"
        )
        self.lbl_content.pack(side="left", fill="x", expand=True, anchor="nw")
        
        def make_configure_handler(lbl=self.lbl_content):
            def on_configure(event):
                lbl.configure(wraplength=max(200, event.width - 200))
            return on_configure
            
        self.inner_frame.bind("<Configure>", make_configure_handler())

    def reset_view(self):
        self.status = None
        self.is_cancelled = False
        self.csv_path = None
        self._cached_apps = []
        self.lbl_content.configure(text="No apps found or scanning...", text_color=COLOR_TEXT_SUB)

    def trigger_fetch_individual(self):
        tenant, clients, secrets = self.get_credentials()
        if tenant:
            self.trigger_fetch(tenant, clients[0], secrets[0])

    def trigger_fetch(self, tenant, client_id, client_secret):
        self.status = "loading"
        self.is_cancelled = False
        
        script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
        if os.path.basename(script_dir) == "intune":
            script_dir = os.path.dirname(script_dir)
        reports_dir = os.path.join(script_dir, "reports", f"{tenant}_{client_id}")
        self.csv_path = os.path.join(reports_dir, "intune_apps.csv")
        
        self.lbl_content.configure(text="⏳ Scanning Managed Mobile Apps in background...", text_color=COLOR_TEXT_SUB)
        self.on_status_change()
        
        threading.Thread(target=self._execute_worker, args=(tenant, client_id, client_secret), daemon=True).start()

    def _execute_worker(self, tenant, client_id, client_secret):
        if self.semaphore: self.semaphore.acquire()
        try:
            temp_csv_path = self.csv_path + ".tmp"
            script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
            if os.path.basename(script_dir) == "intune":
                script_dir = os.path.dirname(script_dir)
            reports_dir = os.path.join(script_dir, "reports", f"{tenant}_{client_id}")
            os.makedirs(reports_dir, exist_ok=True)
            
            with open(temp_csv_path, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["displayName"])

            run_mobile_apps_pipeline(
                client_id=client_id,
                client_secret=client_secret,
                tenant_id=tenant,
                csv_path=temp_csv_path,
                is_cancelled_callback=lambda: self.is_cancelled
            )
            
            if not self.is_cancelled:
                if os.path.exists(temp_csv_path):
                    if os.path.exists(self.csv_path): os.remove(self.csv_path)
                    os.rename(temp_csv_path, self.csv_path)
                self.status = "success"
                self.after(0, self._render_success)
        except Exception as e:
            usage_logger.error(f"Mobile apps fetch error: {e}", exc_info=True)
            self.status = "error"
            self.after(0, self._render_error, str(e))
        finally:
            if 'temp_csv_path' in locals() and os.path.exists(temp_csv_path):
                try: os.remove(temp_csv_path)
                except Exception: pass
            if self.semaphore: self.semaphore.release()
            self.after(0, self.on_status_change)

    def _render_success(self):
        apps = self._load_data_from_csv()
        self._cached_apps = apps
        display_text = ", ".join(apps) if apps else "No managed apps detected."
        self.lbl_content.configure(text=display_text, text_color=COLOR_TEXT_MAIN)

    def _render_error(self, err):
        self.lbl_content.configure(text=f"✖ {err}", text_color=COLOR_ERROR)

    def _load_data_from_csv(self):
        if not self.csv_path or not os.path.exists(self.csv_path): return []
        items = []
        try:
            with open(self.csv_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader, None)
                for row in reader:
                    if row and row[0]: items.append(row[0])
        except Exception as e:
            usage_logger.error(f"Error reading mobile apps CSV: {e}")
        return sorted(items)

    @property
    def last_data(self):
        if self._cached_apps: return self._cached_apps
        return self._load_data_from_csv()

    def cancel(self):
        self.is_cancelled = True
        self.status = None
