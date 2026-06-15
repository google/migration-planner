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

"""Modular Exchange Online Organization-wide Apps telemetry scanners and visual interfaces."""

import logging
import threading
from typing import Any, Dict, List, Optional
import customtkinter as ctk

# Bind to the async logger initialized in m365_telemetry.py
usage_logger = logging.getLogger("M365TelemetryAsyncLogger")

# Import shared styles
from telemetry.styles import *
from telemetry.calendar_telemetry import run_calendar_telemetry_pipeline

class ExchangeAppsFrame(ctk.CTkFrame):
    def update_loading_text(self, text_msg):
        if hasattr(self, 'loading_label') and self.loading_label.winfo_exists():
            self.loading_label.configure(text=f"⏳ {text_msg}")
    """Self-contained customtkinter component wrapping Exchange Online Org-wide Apps UI with side-by-side columns layout."""

    def __init__(self, master, log_callback, credentials_callback, status_change_callback, **kwargs):
        self.semaphore = kwargs.pop("concurrency_semaphore", None)
        super().__init__(master, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=12, **kwargs)
        self.log_msg = log_callback
        self.get_credentials = credentials_callback
        self.on_status_change = status_change_callback
        self.status = None
        self.last_apps = []

        self.build_ui()

    def build_ui(self):
        """Creates card container for the tab."""
        self.pack(fill="x", expand=True, pady=10)

        self.inner_pad = ctk.CTkFrame(self, fg_color="transparent")
        self.inner_pad.pack(fill="both", expand=True, padx=20, pady=20)

        
        self.header = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.header.pack(fill="x", pady=(0, 10))
        ctk.CTkLabel(self.header, text="Integrated Apps", font=FONT_HEADER_SMALL, text_color=COLOR_TEXT_MAIN).pack(side="left")
        self.reload_btn = ctk.CTkButton(
            self.header, 
            state="disabled", text="↻ Reload", 
            width=80, 
            height=24,
            font=__import__("customtkinter").CTkFont(family="Segoe UI", size=12),
            fg_color="transparent", 
            border_width=1, 
            text_color="#2563EB", 
            hover_color="#DBEAFE",
            command=self._retry_fetch
        )
        self.reload_btn.pack(side="right")

        self.state_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.warning_label = ctk.CTkLabel(self.inner_pad, text="", font=FONT_BODY_MEDIUM, text_color=COLOR_ERROR, justify="left", anchor="w", wraplength=750)
        self.grid_frame = ctk.CTkFrame(self.inner_pad, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)

        self.reset_view()

    def reset_view(self):
        """Resets and hides grids."""
        self.pack_forget()
        self.state_frame.pack_forget()
        self.grid_frame.pack_forget()
        self.last_apps = []
        if hasattr(self, "warning_label"):
            self.warning_label.pack_forget()

        for w in self.state_frame.winfo_children():
            w.destroy()
        for w in self.grid_frame.winfo_children():
            w.destroy()

    def _set_state_loading(self, msg="Loading..."):
        for w in self.state_frame.winfo_children():
            w.destroy()
        self.loading_label = __import__("customtkinter").CTkLabel(self.state_frame, text=f"⏳ {msg}", text_color="#6b7280", font=__import__("customtkinter").CTkFont(family="Segoe UI", size=13))
        self.loading_label.pack(pady=(20, 5))
        pb = __import__("customtkinter").CTkProgressBar(self.state_frame, mode="indeterminate", width=250, fg_color="#F3F4F6", progress_color="#2563EB")
        pb.pack(pady=(0, 20))
        pb.start()
        self.state_frame.pack(fill="x", expand=True)

    def _set_state_error(self, error_msg):
        for w in self.state_frame.winfo_children():
            w.destroy()

        ctk.CTkLabel(self.state_frame, text=f"✖ {error_msg}", text_color=COLOR_ERROR, font=FONT_BODY_MEDIUM, justify="center").pack(pady=(20, 5))
        ctk.CTkButton(self.state_frame, text="Try Again", command=self._retry_fetch, width=120, fg_color="transparent", border_width=1, text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER).pack(pady=(0, 20))
        self.state_frame.pack(fill="x", expand=True)

    def _retry_fetch(self):
        if hasattr(self, 'reload_btn') and self.reload_btn.winfo_exists():
            self.reload_btn.configure(state="disabled")
        tenant, clients, secrets = self.get_credentials()
        if tenant:
            self.trigger_fetch(tenant, clients[0], secrets[0])

    def trigger_fetch(self, tenant, client_id, client_secret):
        """Triggers background fetch thread."""
        usage_logger.info("Exchange Apps trigger_fetch called. Spawning background thread...")
        self.status = "loading"
        self.on_status_change()

        self.pack(fill="x", expand=True, pady=10)
        self.grid_frame.pack_forget()
        self.warning_label.pack_forget()

        self._set_state_loading("Downloading Exchange organization-wide apps configurations...")

        threading.Thread(
            target=self._execute_worker,
            args=(tenant, client_id, client_secret),
            daemon=True
        ).start()

    def _execute_worker(self, tenant: str, client_id: str, client_secret: str):
        usage_logger.info("Executing thread: _execute_apps_worker")
        if self.semaphore:
            self.semaphore.acquire()
        try:
            data = run_calendar_telemetry_pipeline(client_id, client_secret, tenant)
            usage_logger.info("Successfully completed Exchange Apps telemetry data fetch.")
            self.after(0, self._render_success, data)
        except Exception as e:
            usage_logger.error("Exception caught in Exchange Apps worker.", exc_info=True)
            self.after(0, self._render_error, str(e))
        finally:
            if self.semaphore:
                self.semaphore.release()

    def _render_success(self, data: dict):
        usage_logger.info("Exchange Apps data successfully retrieved. Rendering UI grid.")
        if hasattr(self, 'reload_btn') and self.reload_btn.winfo_exists():
            self.reload_btn.configure(state="normal")
        self.state_frame.pack_forget()
        for w in self.grid_frame.winfo_children():
            w.destroy()

        if data.get("powershell_error"):
            friendly_msg = f"Exchange PowerShell query failed: {data['powershell_error']}"
            self.warning_label.configure(text=f"⚠️ Warning: {friendly_msg}")
            self.warning_label.pack(anchor="w", pady=(0, 10))
        else:
            self.warning_label.pack_forget()

        self.grid_frame.pack(fill="x", expand=True)

        for i in range(4):
            self.grid_frame.grid_columnconfigure(i, weight=1)

        headers = ["App Display Name", "Status", "App Display Name", "Status"]
        for col_idx, head_text in enumerate(headers):
            cell = ctk.CTkFrame(self.grid_frame, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=1, pady=1)
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        apps_list = data.get("OrganizationApps", [])
        apps_err = data.get("AppsError")
        self.last_apps = apps_list

        if apps_err:
            usage_logger.error(f"Exchange PowerShell query failed for organization apps: {apps_err}")
            empty_cell = ctk.CTkFrame(self.grid_frame, fg_color="transparent")
            empty_cell.grid(row=1, column=0, columnspan=4, sticky="nsew", pady=15)
            ctk.CTkLabel(empty_cell, text=f"Error retrieving apps: {apps_err}", text_color=COLOR_ERROR).pack()
        elif not apps_list:
            empty_cell = ctk.CTkFrame(self.grid_frame, fg_color="transparent")
            empty_cell.grid(row=1, column=0, columnspan=4, sticky="nsew", pady=15)
            ctk.CTkLabel(empty_cell, text="No organization-wide apps found.", text_color=COLOR_TEXT_SUB).pack()
        else:
            half = (len(apps_list) + 1) // 2
            left_col = apps_list[:half]
            right_col = apps_list[half:]

            for r_idx in range(half):
                bg_style = "transparent" if r_idx % 2 != 0 else COLOR_SURFACE_VARIANT
                row_items = []

                if r_idx < len(left_col):
                    app = left_col[r_idx]
                    status_str = "Enabled" if app.get("Enabled") else "Disabled"
                    row_items.extend([app.get("DisplayName", "-"), status_str])
                else:
                    row_items.extend(["", ""])

                if r_idx < len(right_col):
                    app = right_col[r_idx]
                    status_str = "Enabled" if app.get("Enabled") else "Disabled"
                    row_items.extend([app.get("DisplayName", "-"), status_str])
                else:
                    row_items.extend(["", ""])

                for c_idx, val in enumerate(row_items):
                    cell = ctk.CTkFrame(self.grid_frame, fg_color=bg_style, corner_radius=0)
                    cell.grid(row=r_idx + 1, column=c_idx, sticky="nsew", padx=1, pady=1)
                    
                    is_enabled_col = c_idx in [1, 3]
                    fnt = FONT_BODY_MEDIUM if is_enabled_col else FONT_BODY_BOLD
                    
                    text_color = COLOR_TEXT_MAIN
                    if is_enabled_col and val == "Disabled":
                        text_color = COLOR_TEXT_SUB
                        
                    ctk.CTkLabel(cell, text=val, font=fnt, text_color=text_color, wraplength=200, justify="left", anchor="w").pack(padx=10, pady=6, anchor="w")

        self.status = "success"
        self.on_status_change()

    def _render_error(self, err_msg):
        usage_logger.warning(f"Exchange Apps Telemetry fetch failed: {err_msg}")
        if hasattr(self, 'reload_btn') and self.reload_btn.winfo_exists():
            self.reload_btn.configure(state="normal")
        self._set_state_error(err_msg)
        self.status = "error"
        self.on_status_change()
