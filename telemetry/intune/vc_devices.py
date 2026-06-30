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

"""UI component for VC Devices telemetry."""

import os
import csv
import logging
import threading
import pandas as pd
import customtkinter as ctk
import sqlite3
import asyncio
import shutil
from tkinter import filedialog, messagebox

from core.graph.db import import_csv_to_sqlite
from core.graph.client import GraphClient
from core.graph.directory import DirectoryService
from core.powershell.client import PowerShellClient
from core.powershell.calendar import CalendarStatsService
from telemetry.styles import *

usage_logger = logging.getLogger("M365TelemetryAsyncLogger.VCDevicesUI")

class VCDevicesSubFrame(ctk.CTkFrame):
    """Sub-frame for Room-registered Video Conferencing (VC) Devices."""

    def __init__(self, master, log_callback, credentials_callback, status_change_callback, semaphore=None, **kwargs):
        super().__init__(master, fg_color="transparent", **kwargs)
        self.log_msg = log_callback
        self.get_credentials = credentials_callback
        self.on_status_change = status_change_callback
        self.semaphore = semaphore
        self.status = None
        self.page = 0
        self.ITEMS_PER_PAGE = 5
        self.csv_path = None
        self.is_cancelled = False
        self.rooms_count = 0

        self.build_ui()

    def build_ui(self):
        self.header_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.header_frame.pack(fill="x", pady=(0, 5))
        
        ctk.CTkLabel(self.header_frame, text="VC Devices", font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN).pack(side="left")
        
        self.btn_reload = ctk.CTkButton(
            self.header_frame, text="↻ Reload", width=80, height=24,
            font=ctk.CTkFont(family="Segoe UI", size=12),
            fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color="#2563EB", hover_color="#DBEAFE",
            command=self.trigger_fetch_individual
        )
        self.btn_reload.pack(side="right", padx=(10, 0))
        
        self.btn_export = ctk.CTkButton(
            self.header_frame, text="Export VC Devices", width=180, height=32, corner_radius=16,
            font=FONT_BODY_BOLD, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER,
            command=self._export
        )
        self.btn_export.pack(side="right")

        self.state_frame = ctk.CTkFrame(self, fg_color="transparent")
        
        # Room metric and total counts container
        self.metrics_container = ctk.CTkFrame(self, fg_color="transparent")
        self.rooms_metric_lbl = ctk.CTkLabel(self.metrics_container, text="Room Mailboxes Discovered: 0", font=FONT_BODY_BOLD, text_color=COLOR_SUCCESS)
        self.rooms_metric_lbl.pack(side="left", padx=(10, 20))
        
        self.summary_lbl = ctk.CTkLabel(self.metrics_container, text="Total Extracted VC Devices: 0", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB)
        self.summary_lbl.pack(side="left")
        
        self.grid_frame = ctk.CTkFrame(self, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)
        
        headers = ["User ID", "Device Name", "Operating System", "Management Agent", "Registration State", "Model", "Manufacturer"]
        for col_idx, head_text in enumerate(headers):
            self.grid_frame.grid_columnconfigure(col_idx, weight=1 if col_idx != 0 else 2) # userId gets slightly more weight
            cell = ctk.CTkFrame(self.grid_frame, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=1, pady=1)
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        # Pagination controls frame (stays fixed/centered)
        self.pagination_frame = ctk.CTkFrame(self, fg_color="transparent")

        self.reset_view()

    def reset_view(self):
        self.status = None
        self.is_cancelled = False
        self.csv_path = None
        self.page = 0
        self.rooms_count = 0
        self.state_frame.pack_forget()
        self.metrics_container.pack_forget()
        self.grid_frame.pack_forget()
        self.pagination_frame.pack_forget()
        for w in self.state_frame.winfo_children(): w.destroy()
        for w in self.grid_frame.winfo_children():
            info = w.grid_info()
            if "row" in info and int(info["row"]) > 0: w.destroy()
        for w in self.pagination_frame.winfo_children(): w.destroy()

    def _set_loading_state(self, msg):
        for w in self.state_frame.winfo_children(): w.destroy()
        self.state_frame.pack(fill="x", expand=True)
        ctk.CTkLabel(self.state_frame, text=f"⏳ {msg}", text_color=COLOR_TEXT_SUB, font=FONT_BODY_MEDIUM).pack(pady=(15, 5))
        pb = ctk.CTkProgressBar(self.state_frame, mode="indeterminate", width=200, fg_color=COLOR_SURFACE_VARIANT, progress_color=COLOR_PRIMARY)
        pb.pack(pady=(0, 15))
        pb.start()

    def trigger_fetch_individual(self):
        tenant, clients, secrets = self.get_credentials()
        if tenant:
            self.trigger_fetch(tenant, clients[0], secrets[0])

    def trigger_fetch(self, tenant, client_id, client_secret):
        self.status = "loading"
        self.is_cancelled = False
        self.page = 0
        self.btn_reload.configure(state="disabled")
        self.btn_export.configure(state="disabled")
        self.grid_frame.pack_forget()
        self.pagination_frame.pack_forget()
        
        script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
        if os.path.basename(script_dir) == "intune":
            script_dir = os.path.dirname(script_dir)
        reports_dir = os.path.join(script_dir, "reports", f"{tenant}_{client_id}")
        self.csv_path = os.path.join(reports_dir, "intune_vc_devices.csv")

        self._set_loading_state("Filtering Video Conferencing (VC) Devices...")
        self.on_status_change()
            
        threading.Thread(target=self._execute_worker, args=(tenant, client_id, client_secret), daemon=True).start()

    def _execute_worker(self, tenant, client_id, client_secret):
        if self.semaphore: self.semaphore.acquire()
        try:
            reports_dir = os.path.dirname(self.csv_path)
            
            # Fetch primary domain name for Exchange connection
            tenant_domain = tenant
            client = None
            try:
                client = GraphClient(
                    tenant_id=tenant,
                    client_ids=client_id,
                    client_secrets=client_secret,
                    concurrency=1,
                    retries=5,
                    backoff=2
                )
                client.authenticate()
                dir_svc = DirectoryService(client)
                tenant_domain = dir_svc.get_tenant_primary_domain()
            except Exception as e:
                usage_logger.warning(f"Could not retrieve tenant domain via Graph. Falling back to Tenant ID Guid: {e}")
            finally:
                if client:
                    client.close()

            # Query Room Mailboxes from Exchange Online PowerShell directly
            usage_logger.info("Connecting to Exchange Online PowerShell for room mailboxes...")
            ps_client = PowerShellClient(
                tenant_id=tenant_domain,
                client_id=client_id,
                client_secret=client_secret,
                cert_tenant_id=tenant
            )
            cal_service = CalendarStatsService(ps_client)
            rooms_list = cal_service.fetch_room_mailboxes()
            room_emails = set(r.lower() for r in rooms_list if r)
            
            if not room_emails:
                raise ValueError("No room mailboxes found.")
            self.rooms_count = len(room_emails)

            # Save the discovered room list to CSV
            rooms_csv = os.path.join(reports_dir, "room_mailboxes.csv")
            os.makedirs(reports_dir, exist_ok=True)
            with open(rooms_csv, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["PrimarySmtpAddress"])
                for room in rooms_list:
                    writer.writerow([room])

            # Load managed devices
            devices_csv = os.path.join(reports_dir, "intune_managed_devices.csv")
            if os.path.exists(devices_csv):
                df_devices = pd.read_csv(devices_csv)
            else:
                df_devices = pd.DataFrame(columns=["userId", "deviceName", "operatingSystem", "managementAgent", "deviceRegistrationState", "model", "manufacturer", "userPrincipalName", "emailAddress"])
            
            # Filter where userPrincipalName or emailAddress in room_emails
            upn_match = df_devices["userPrincipalName"].fillna("").str.lower().isin(room_emails) if "userPrincipalName" in df_devices.columns else pd.Series([False]*len(df_devices))
            email_match = df_devices["emailAddress"].fillna("").str.lower().isin(room_emails) if "emailAddress" in df_devices.columns else pd.Series([False]*len(df_devices))
            
            df_vc = df_devices[upn_match | email_match]
            
            # Save to intune_vc_devices.csv
            df_vc.to_csv(self.csv_path, index=False, encoding='utf-8')
            
            # Import to sqlite db cache table vc_devices
            db_path = os.path.join(reports_dir, "telemetry_cache.db")
            if os.path.exists(self.csv_path):
                asyncio.run(import_csv_to_sqlite(self.csv_path, db_path, "vc_devices"))
                
            self.status = "success"
            self.after(0, self._render_success)
        except Exception as e:
            usage_logger.error(f"VC devices filter error: {e}", exc_info=True)
            self.status = "error"
            self.after(0, self._render_error, str(e))
        finally:
            if self.semaphore: self.semaphore.release()
            self.after(0, self.on_status_change)

    def _render_success(self):
        self.btn_reload.configure(state="normal")
        self.btn_export.configure(state="normal")
        self.state_frame.pack_forget()
        self.metrics_container.pack(anchor="w", padx=10, pady=5)
        self.grid_frame.pack(fill="x")
        self.pagination_frame.pack(fill="x", pady=(5, 10))
        
        self.rooms_metric_lbl.configure(text=f"Room Mailboxes Discovered: {self.rooms_count}")
        self._update_grid()

    def _render_error(self, err):
        self.btn_reload.configure(state="normal")
        self.grid_frame.pack_forget()
        self.pagination_frame.pack_forget()
        self.metrics_container.pack_forget()
        for w in self.state_frame.winfo_children(): w.destroy()
        self.state_frame.pack(fill="x", expand=True)
        ctk.CTkLabel(self.state_frame, text=f"✖ {err}", text_color=COLOR_ERROR, font=FONT_BODY_MEDIUM, justify="center", wraplength=700).pack(pady=(15, 5))
        ctk.CTkButton(self.state_frame, text="Try Again", command=self.trigger_fetch_individual, width=100, fg_color="transparent", border_width=1, text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER).pack(pady=(0, 15))

    def _load_page(self):
        if not self.csv_path or not os.path.exists(self.csv_path): return [], 0
        
        reports_dir = os.path.dirname(self.csv_path)
        db_path = os.path.join(reports_dir, "telemetry_cache.db")
        if not os.path.exists(db_path): return [], 0
        
        try:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM vc_devices")
            total = cursor.fetchone()[0]
            
            offset = self.page * self.ITEMS_PER_PAGE
            cursor.execute("SELECT * FROM vc_devices LIMIT ? OFFSET ?", (self.ITEMS_PER_PAGE, offset))
            rows = cursor.fetchall()
            
            conn.close()
            return rows, total
        except Exception as e:
            usage_logger.error(f"Error reading SQLite vc_devices: {e}")
            return [], 0

    def _update_grid(self):
        for w in self.grid_frame.winfo_children():
            info = w.grid_info()
            if "row" in info and int(info["row"]) > 0: w.destroy()

        page_data, total_count = self._load_page()
        total_pages = (total_count - 1) // self.ITEMS_PER_PAGE + 1 if total_count > 0 else 1

        self.summary_lbl.configure(text=f"Total Extracted VC Devices: {total_count}")

        for row_idx, item in enumerate(page_data, start=1):
            bg_style = COLOR_SURFACE if row_idx % 2 == 0 else COLOR_SURFACE_VARIANT
            vals = [
                item.get("userId") or "N/A",
                item.get("deviceName") or "N/A",
                item.get("operatingSystem") or "N/A",
                item.get("managementAgent") or "unknown",
                item.get("deviceRegistrationState") or "unknown",
                item.get("model") or "N/A",
                item.get("manufacturer") or "N/A"
            ]
            for col_idx, val in enumerate(vals):
                cell = ctk.CTkFrame(self.grid_frame, fg_color=bg_style, corner_radius=0)
                cell.grid(row=row_idx, column=col_idx, sticky="nsew", padx=1, pady=1)
                lbl = ctk.CTkLabel(cell, text=str(val), font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, wraplength=250, justify="left", anchor="w")
                lbl.pack(padx=10, pady=8, fill="x")

        # Re-build pagination controls
        for w in self.pagination_frame.winfo_children(): w.destroy()
        
        center_container = ctk.CTkFrame(self.pagination_frame, fg_color="transparent")
        center_container.pack(pady=5)

        prev_state = "normal" if self.page > 0 else "disabled"
        ctk.CTkButton(
            center_container, text="◀ Prev", width=70, height=22, corner_radius=6,
            font=FONT_BODY_SMALL, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER, state=prev_state,
            command=lambda: self._change_page(-1)
        ).pack(side="left", padx=5)

        ctk.CTkLabel(center_container, text=f"Page {self.page + 1} of {total_pages}", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB).pack(side="left", padx=15)

        next_state = "normal" if self.page < total_pages - 1 else "disabled"
        ctk.CTkButton(
            center_container, text="Next ▶", width=70, height=22, corner_radius=6,
            font=FONT_BODY_SMALL, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER, state=next_state,
            command=lambda: self._change_page(1)
        ).pack(side="left", padx=5)

    def _change_page(self, delta):
        self.page += delta
        self._update_grid()

    def _export(self):
        if not self.csv_path or not os.path.exists(self.csv_path):
            messagebox.showerror("Export Error", "No data available to export yet.")
            return

        dest_path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            title="Export VC Devices CSV",
            initialfile="M365_Intune_VC_Devices.csv",
            parent=self
        )
        if not dest_path: return
        
        try:
            shutil.copyfile(self.csv_path, dest_path)
            messagebox.showinfo("Export Successful", f"VC Devices successfully exported to:\n{dest_path}")
        except Exception as e:
            messagebox.showerror("Export Failed", f"Failed to save CSV file: {e}")

    @property
    def last_data(self):
        if not self.csv_path or not os.path.exists(self.csv_path): return []
        try:
            df = pd.read_csv(self.csv_path)
            return df.head(200).fillna("N/A").to_dict('records')
        except Exception:
            return []

    def cancel(self):
        self.is_cancelled = True
        self.status = None
