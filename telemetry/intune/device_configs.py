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

"""UI component for Device Configurations telemetry."""

import os
import csv
import time
import logging
import threading
from collections import defaultdict
import customtkinter as ctk
from tkinter import filedialog, messagebox
import asyncio
import sqlite3
import pandas as pd

from core.graph.db import import_csv_to_sqlite
from core.graph.intune.device_configs import run_device_configs_pipeline
from telemetry.styles import *

usage_logger = logging.getLogger("M365TelemetryAsyncLogger.DeviceConfigsUI")

class DeviceConfigsSubFrame(ctk.CTkFrame):
    """Sub-frame for Device Configurations & Policies."""

    def __init__(self, master, log_callback, credentials_callback, status_change_callback, semaphore=None, **kwargs):
        super().__init__(master, fg_color="transparent", **kwargs)
        self.log_msg = log_callback
        self.get_credentials = credentials_callback
        self.on_status_change = status_change_callback
        self.semaphore = semaphore
        self.status = None
        self.page = 0
        self.ITEMS_PER_PAGE = 5
        self.csv_path_configs = None
        self.csv_path_policies = None
        self.is_cancelled = False

        self.build_ui()

    def build_ui(self):
        self.header_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.header_frame.pack(fill="x", pady=(0, 5))
        
        ctk.CTkLabel(self.header_frame, text="3. Device Configurations", font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN).pack(side="left")
        
        self.btn_reload = ctk.CTkButton(
            self.header_frame, text="↻ Reload", width=80, height=24,
            font=ctk.CTkFont(family="Segoe UI", size=12),
            fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color="#2563EB", hover_color="#DBEAFE",
            command=self.trigger_fetch_individual
        )
        self.btn_reload.pack(side="right", padx=(10, 0))
        
        self.btn_export = ctk.CTkButton(
            self.header_frame, text="Export configurations", width=180, height=32, corner_radius=16,
            font=FONT_BODY_BOLD, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER,
            command=self._export
        )
        self.btn_export.pack(side="right")

        self.state_frame = ctk.CTkFrame(self, fg_color="transparent")
        
        self.summary_lbl = ctk.CTkLabel(self, text="Total Extracted: 0 Device Configurations | 0 Configuration Policies", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB)
        
        self.grid_frame = ctk.CTkFrame(self, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)
        
        headers = ["Platform", "Policy Type", "Number of Policies"]
        for i in range(3):
            self.grid_frame.grid_columnconfigure(i, weight=1 if i == 2 else 2)
            
        for col_idx, head_text in enumerate(headers):
            cell = ctk.CTkFrame(self.grid_frame, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=1, pady=1)
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        self.reset_view()

    def reset_view(self):
        self.status = None
        self.is_cancelled = False
        self.csv_path_configs = None
        self.csv_path_policies = None
        self.page = 0
        self.state_frame.pack_forget()
        self.summary_lbl.pack_forget()
        self.grid_frame.pack_forget()
        for w in self.state_frame.winfo_children(): w.destroy()
        for w in self.grid_frame.winfo_children():
            info = w.grid_info()
            if "row" in info and int(info["row"]) > 0: w.destroy()

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
        
        script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
        if os.path.basename(script_dir) == "intune":
            script_dir = os.path.dirname(script_dir)
        reports_dir = os.path.join(script_dir, "reports", f"{tenant}_{client_id}")
        self.csv_path_configs = os.path.join(reports_dir, "intune_device_configs.csv")
        self.csv_path_policies = os.path.join(reports_dir, "intune_config_policies.csv")

        self._set_loading_state("Scanning Intune device configurations & policies...")
        self.on_status_change()
            
        threading.Thread(target=self._execute_worker, args=(tenant, client_id, client_secret), daemon=True).start()

    def _execute_worker(self, tenant, client_id, client_secret):
        if self.semaphore: self.semaphore.acquire()
        try:
            temp_path_configs = self.csv_path_configs + ".tmp"
            temp_path_policies = self.csv_path_policies + ".tmp"
            
            with open(temp_path_configs, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["displayName", "platform", "policyType"])
            with open(temp_path_policies, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["displayName", "platform", "policyType"])

            # Run in parallel internally or sequentially
            run_device_configs_pipeline(
                client_id=client_id,
                client_secret=client_secret,
                tenant_id=tenant,
                endpoint_name="deviceConfigurations",
                csv_path=temp_path_configs,
                is_cancelled_callback=lambda: self.is_cancelled
            )
            run_device_configs_pipeline(
                client_id=client_id,
                client_secret=client_secret,
                tenant_id=tenant,
                endpoint_name="configurationPolicies",
                csv_path=temp_path_policies,
                is_cancelled_callback=lambda: self.is_cancelled
            )

            if not self.is_cancelled:
                if os.path.exists(temp_path_configs):
                    if os.path.exists(self.csv_path_configs): os.remove(self.csv_path_configs)
                    os.rename(temp_path_configs, self.csv_path_configs)
                if os.path.exists(temp_path_policies):
                    if os.path.exists(self.csv_path_policies): os.remove(self.csv_path_policies)
                    os.rename(temp_path_policies, self.csv_path_policies)
                
                reports_dir = os.path.dirname(self.csv_path_configs)
                db_path = os.path.join(reports_dir, "telemetry_cache.db")
                if os.path.exists(self.csv_path_configs):
                    asyncio.run(import_csv_to_sqlite(self.csv_path_configs, db_path, "device_configs"))
                if os.path.exists(self.csv_path_policies):
                    asyncio.run(import_csv_to_sqlite(self.csv_path_policies, db_path, "device_policies"))

                self.status = "success"
                self.after(0, self._render_success)
        except Exception as e:
            usage_logger.error(f"Device configs fetch error: {e}", exc_info=True)
            self.status = "error"
            self.after(0, self._render_error, str(e))
        finally:
            if 'temp_path_configs' in locals() and os.path.exists(temp_path_configs):
                try: os.remove(temp_path_configs)
                except Exception: pass
            if 'temp_path_policies' in locals() and os.path.exists(temp_path_policies):
                try: os.remove(temp_path_policies)
                except Exception: pass
            if self.semaphore: self.semaphore.release()
            self.after(0, self.on_status_change)

    def _render_success(self):
        self.btn_reload.configure(state="normal")
        self.btn_export.configure(state="normal")
        self.state_frame.pack_forget()
        self.summary_lbl.pack(anchor="w", padx=10, pady=5)
        self.grid_frame.pack(fill="x")
        
        self._update_grid()

    def _render_error(self, err):
        self.btn_reload.configure(state="normal")
        self.grid_frame.pack_forget()
        self.summary_lbl.pack_forget()
        for w in self.state_frame.winfo_children(): w.destroy()
        self.state_frame.pack(fill="x", expand=True)
        ctk.CTkLabel(self.state_frame, text=f"✖ {err}", text_color=COLOR_ERROR, font=FONT_BODY_MEDIUM, justify="center", wraplength=700).pack(pady=(15, 5))
        ctk.CTkButton(self.state_frame, text="Try Again", command=self.trigger_fetch_individual, width=100, fg_color="transparent", border_width=1, text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER).pack(pady=(0, 15))

    def _load_page(self):
        if not self.csv_path_configs or not os.path.exists(self.csv_path_configs): return [], 0, 0, 0
        
        counts = defaultdict(int)
        total_dc = 0
        total_cp = 0
        
        reports_dir = os.path.dirname(self.csv_path_configs)
        db_path = os.path.join(reports_dir, "telemetry_cache.db")
        if not os.path.exists(db_path): return [], 0, 0, 0
        
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            cursor.execute("SELECT platform, policyType FROM device_configs")
            for row in cursor.fetchall():
                if len(row) >= 2:
                    plat, p_type = row[0], row[1]
                    if plat and p_type:
                        counts[(plat, p_type)] += 1
                        total_dc += 1
                        
            try:
                cursor.execute("SELECT platform, policyType FROM device_policies")
                for row in cursor.fetchall():
                    if len(row) >= 2:
                        plat, p_type = row[0], row[1]
                        if plat and p_type:
                            counts[(plat, p_type)] += 1
                            total_cp += 1
            except sqlite3.OperationalError:
                pass
                
            conn.close()
        except Exception as e:
            usage_logger.error(f"Error loading device configurations DB: {e}")
            
        rows_data = []
        for (platform, p_type), count in sorted(counts.items()):
            rows_data.append((platform, p_type, str(count)))
            
        total = len(rows_data)
        start = self.page * self.ITEMS_PER_PAGE
        end = start + self.ITEMS_PER_PAGE
        
        return rows_data[start:end], total, total_dc, total_cp

    def _update_grid(self):
        for w in self.grid_frame.winfo_children():
            info = w.grid_info()
            if "row" in info and int(info["row"]) > 0: w.destroy()

        page_data, total_count, total_dc, total_cp = self._load_page()
        total_pages = (total_count - 1) // self.ITEMS_PER_PAGE + 1 if total_count > 0 else 1

        self.summary_lbl.configure(text=f"Total Extracted: {total_dc} Device Configurations | {total_cp} Configuration Policies")

        for row_idx, (platform, p_type, count) in enumerate(page_data, start=1):
            bg_style = COLOR_SURFACE if row_idx % 2 == 0 else COLOR_SURFACE_VARIANT
            vals = [platform, p_type, count]
            for col_idx, val in enumerate(vals):
                cell = ctk.CTkFrame(self.grid_frame, fg_color=bg_style, corner_radius=0)
                cell.grid(row=row_idx, column=col_idx, sticky="nsew", padx=1, pady=1)
                lbl = ctk.CTkLabel(cell, text=str(val), font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, wraplength=450, justify="left", anchor="w")
                lbl.pack(padx=10, pady=8, fill="x")

        # Pagination controls row
        control_frame = ctk.CTkFrame(self.grid_frame, fg_color="transparent")
        control_frame.grid(row=self.ITEMS_PER_PAGE + 1, column=0, columnspan=3, pady=0, sticky="ew")

        center_container = ctk.CTkFrame(control_frame, fg_color="transparent")
        center_container.pack(pady=(5, 10))

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
        if not self.csv_path_configs or not os.path.exists(self.csv_path_configs):
            messagebox.showerror("Export Error", "No data available to export yet.")
            return

        dest_path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            title="Export Device Configurations CSV",
            initialfile="M365_Intune_Device_Configurations.csv",
            parent=self
        )
        if not dest_path: return
        
        try:
            df = pd.read_csv(self.csv_path_configs)
            df.fillna("N/A").to_csv(dest_path, index=False, encoding="utf-8-sig")
            messagebox.showinfo("Export Successful", f"Device Configurations successfully exported to:\n{dest_path}")
        except Exception as e:
            messagebox.showerror("Export Failed", f"Failed to save CSV file: {e}")

    @property
    def last_data(self):
        return self._load_page()[0]

    def cancel(self):
        self.is_cancelled = True
        self.status = None
