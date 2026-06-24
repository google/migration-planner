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

"""UI frame for Exchange Online Calendar Environment Telemetry."""

import os
import csv
import logging
import threading
import customtkinter as ctk

from core.graph.exchange.calendar import run_calendar_telemetry_pipeline
from telemetry.styles import *

calendar_logger = logging.getLogger("M365TelemetryAsyncLogger.CalendarTelemetryUI")

class CalendarTelemetryFrame(ctk.CTkFrame):
    """Self-contained customtkinter component wrapping Exchange Online Calendar Environment Telemetry UI."""
    
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
        
        self.header = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.header.pack(fill="x", pady=(0, 10))
        ctk.CTkLabel(self.header, text="Exchange Online Calendar Environment", font=FONT_HEADER_SMALL, text_color=COLOR_TEXT_MAIN).pack(side="left")
        self.reload_btn = ctk.CTkButton(
            self.header, 
            state="disabled", text="↻ Reload", 
            width=80, 
            height=24,
            font=ctk.CTkFont(family="Segoe UI", size=12),
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
        self.state_frame.pack_forget()
        self.grid_frame.pack_forget()
        self.last_data = {}
        if hasattr(self, "warning_label"):
            self.warning_label.pack_forget()
        
        for w in self.state_frame.winfo_children():
            w.destroy()
        for w in self.grid_frame.winfo_children():
            w.destroy()

    def _set_state_loading(self, msg="Loading..."):
        for w in self.state_frame.winfo_children():
            w.destroy()
        ctk.CTkLabel(self.state_frame, text=f"⏳ {msg}", text_color=COLOR_TEXT_SUB, font=FONT_BODY_MEDIUM).pack(pady=(20, 5))
        self.progress = ctk.CTkProgressBar(self.state_frame, mode="indeterminate", width=250, fg_color="#F3F4F6", progress_color="#2563EB")
        self.progress.pack(pady=(0, 20))
        self.progress.start()
        self.state_frame.pack(fill="x", expand=True)

    def _set_state_error(self, error_msg):
        for w in self.state_frame.winfo_children():
            w.destroy()

        ctk.CTkLabel(self.state_frame, text=f"✖ {error_msg}", text_color=COLOR_ERROR, font=FONT_BODY_MEDIUM, justify="center").pack(pady=(20, 10))
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
        calendar_logger.info("Calendar Telemetry trigger_fetch called. Spawning background thread...")
        self.status = "loading"
        self.on_status_change()
        
        self.grid_frame.pack_forget()
        self.warning_label.pack_forget()
        
        self._set_state_loading("Downloading and auditing Exchange Calendar configurations...")
        
        threading.Thread(
            target=self._execute_worker,
            args=(tenant, client_id, client_secret),
            daemon=True
        ).start()

    def _execute_worker(self, tenant: str, client_id: str, client_secret: str):
        if self.semaphore:
            self.semaphore.acquire()
        try:
            data = run_calendar_telemetry_pipeline(client_id, client_secret, tenant)
            
            # Stream to CSV in reports_dir
            if not data.get("powershell_error"):
                script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
                reports_dir = os.path.join(script_dir, "reports", f"{tenant}_{client_id}")
                os.makedirs(reports_dir, exist_ok=True)
                csv_path = os.path.join(reports_dir, "calendar_configurations.csv")
                
                with open(csv_path, 'w', encoding='utf-8', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(["Configuration", "Value"])
                    for k, v in data.items():
                        writer.writerow([k, str(v)])
                calendar_logger.info(f"Successfully streamed Calendar data to {csv_path}")

            calendar_logger.info("Successfully completed Calendar telemetry data fetch.")
            self.after(0, self._render_success, data)
        except Exception as e:
            calendar_logger.error("Exception caught in Calendar worker.", exc_info=True)
            self.after(0, self._render_error, str(e))
        finally:
            if self.semaphore:
                self.semaphore.release()

    def _render_success(self, data: dict):
        if hasattr(self, 'reload_btn') and self.reload_btn.winfo_exists():
            self.reload_btn.configure(state="normal")
        self.last_data = data
        calendar_logger.info("Calendar data successfully retrieved. Rendering UI grid.")
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

        self.grid_frame.grid_columnconfigure(0, weight=3)
        self.grid_frame.grid_columnconfigure(1, weight=2)

        headers_sp = ["Calendar Configuration / Metric", "Value / Configuration"]
        for col_idx, head_text in enumerate(headers_sp):
            cell = ctk.CTkFrame(self.grid_frame, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=1, pady=1)
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        rooms_err = data.get("RoomsError")
        devs_err = data.get("DevicesError")
        rooms_count = data.get("RoomsCount", 0)
        equip_count = data.get("EquipmentCount", 0)
        
        if rooms_err and devs_err:
            res_val = rooms_err
        else:
            r_str = "Error" if rooms_err else str(rooms_count)
            e_str = "Error" if devs_err else str(equip_count)
            tot = "Error" if (rooms_err or devs_err) else str(rooms_count + equip_count)
            res_val = f"Total: {tot} ({r_str} Rooms, {e_str} Equipment)"

        reserve_val = data.get("CanUsersReserveRooms")
        if isinstance(reserve_val, bool):
             reserve_val = "Yes" if reserve_val else "No"

        att_val = data.get("CanShareAttachments")
        if isinstance(att_val, bool):
            attachments_val = "Yes" if att_val else "No"
        else:
            attachments_val = att_val

        rows_data = [
            ("Room & Resource Reservation", reserve_val),
            ("Calendar Resources", res_val),
            ("Resource Naming Convention", data.get("NamingConvention") or "None found"),
            ("Calendar Attachments Enabled", attachments_val),
        ]

        for r_idx, (metric_name, val) in enumerate(rows_data, start=1):
            bg_style = "transparent" if r_idx % 2 != 0 else COLOR_SURFACE_VARIANT
            
            c0 = ctk.CTkFrame(self.grid_frame, fg_color=bg_style, corner_radius=0)
            c0.grid(row=r_idx, column=0, sticky="nsew", padx=1, pady=1)
            ctk.CTkLabel(c0, text=metric_name, font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN, wraplength=450, justify="left", anchor="w").pack(padx=10, pady=6, anchor="w")

            c1 = ctk.CTkFrame(self.grid_frame, fg_color=bg_style, corner_radius=0)
            c1.grid(row=r_idx, column=1, sticky="nsew", padx=1, pady=1)
            ctk.CTkLabel(c1, text=str(val), font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, wraplength=300, justify="left", anchor="w").pack(padx=10, pady=6, anchor="w")

        self.status = "success"
        self.on_status_change()

    def _render_error(self, err_msg):
        calendar_logger.warning(f"Calendar Telemetry fetch failed: {err_msg}")
        if hasattr(self, 'reload_btn') and self.reload_btn.winfo_exists():
            self.reload_btn.configure(state="normal")
        self._set_state_error(err_msg)
        self.status = "error"
        self.on_status_change()

    def cancel(self):
        pass
