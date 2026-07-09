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

"""UI component for Data Loss Prevention (DLP) Policies."""

import os
import time
import logging
import threading
import csv
import shutil
from datetime import datetime
import customtkinter as ctk
from tkinter import filedialog, messagebox

from core.graph.security.dlp_policies import run_dlp_policies_pipeline
from telemetry.styles import *

usage_logger = logging.getLogger("M365TelemetryAsyncLogger.DLPUI")

class DLPPoliciesSubFrame(ctk.CTkFrame):
    """Sub-frame for Data Loss Prevention Policies."""

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

        self.build_ui()

    def build_ui(self):
        self.header_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.header_frame.pack(fill="x", pady=(0, 10))
        
        ctk.CTkLabel(self.header_frame, text="Data Loss Prevention (DLP) Policies", font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN).pack(side="left")
        
        self.lbl_link = ctk.CTkLabel(
            self.header_frame, text="Open Purview DLP Portal ↗",
            font=FONT_BODY_BOLD, text_color=COLOR_PRIMARY, cursor="hand2"
        )
        self.lbl_link.pack(side="left", anchor="w", padx=(15, 0))
        self.lbl_link.bind("<Button-1>", lambda e: __import__("webbrowser").open("https://purview.microsoft.com/datalossprevention/policies"))
        self.lbl_link.bind("<Enter>", lambda e: self.lbl_link.configure(text_color=COLOR_PRIMARY_HOVER))
        self.lbl_link.bind("<Leave>", lambda e: self.lbl_link.configure(text_color=COLOR_PRIMARY))
        
        self.btn_reload = ctk.CTkButton(
            self.header_frame, state="disabled", text="↻ Reload", width=80, height=24,
            font=ctk.CTkFont(family="Segoe UI", size=12),
            fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color="#2563EB", hover_color="#DBEAFE",
            command=self.trigger_fetch_individual
        )
        self.btn_reload.pack(side="right", padx=(10, 0))
        
        self.btn_export = ctk.CTkButton(
            self.header_frame, text="Export DLP Policies", width=180, height=32, corner_radius=16,
            font=FONT_BODY_BOLD, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER,
            command=self._export, state="disabled"
        )
        self.btn_export.pack(side="right")

        self.state_frame = ctk.CTkFrame(self, fg_color="transparent")
        
        self.grid_frame = ctk.CTkFrame(self, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)
        
        self.grid_frame.grid_columnconfigure(0, weight=2)
        self.grid_frame.grid_columnconfigure(1, weight=1)
        self.grid_frame.grid_columnconfigure(2, weight=2)
        self.grid_frame.grid_columnconfigure(3, weight=1)
        self.grid_frame.grid_columnconfigure(4, weight=2)
        self.grid_frame.grid_columnconfigure(5, weight=2)
        
        headers = ["Policy Name", "Mode", "Workload", "State", "Locations", "Actions"]
        for col_idx, head_text in enumerate(headers):
            cell = ctk.CTkFrame(self.grid_frame, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        self.reset_view()

    def reset_view(self):
        self.status = None
        self.is_cancelled = False
        self.csv_path = None
        self.page = 0
        self.state_frame.pack_forget()
        self.grid_frame.pack_forget()
        self.btn_export.configure(state="disabled")
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
        if os.path.basename(script_dir) == "security":
            script_dir = os.path.dirname(script_dir)
        reports_dir = os.path.join(script_dir, "reports", f"{tenant}_{client_id}")
        self.csv_path = os.path.join(reports_dir, "dlp_policies.csv")

        self._set_loading_state("Scanning DLP policies...")
        self.on_status_change()
            
        threading.Thread(target=self._execute_worker, args=(tenant, client_id, client_secret), daemon=True).start()

    def _execute_worker(self, tenant, client_id, client_secret):
        if self.semaphore: self.semaphore.acquire()
        try:
            policies = run_dlp_policies_pipeline(
                client_id=client_id,
                client_secret=client_secret,
                tenant_id=tenant
            )
            
            # Stream to CSV
            if isinstance(policies, dict) and "value" in policies:
                policies = policies["value"]
            policies_list = policies if isinstance(policies, list) else [policies]
            
            script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
            if os.path.basename(script_dir) == "security":
                script_dir = os.path.dirname(script_dir)
            reports_dir = os.path.join(script_dir, "reports", f"{tenant}_{client_id}")
            os.makedirs(reports_dir, exist_ok=True)
            self.csv_path = os.path.join(reports_dir, "dlp_policies.csv")
            
            with open(self.csv_path, 'w', encoding='utf-8', newline='') as f:
                if policies_list:
                    writer = csv.DictWriter(f, fieldnames=policies_list[0].keys())
                    writer.writeheader()
                    writer.writerows(policies_list)
                    
            self.status = "success"
            self.after(0, self._render_success)
        except Exception as e:
            usage_logger.error(f"DLP policies fetch error: {e}", exc_info=True)
            self.status = "error"
            self.after(0, self._render_error, str(e))
        finally:
            if self.semaphore: self.semaphore.release()
            self.after(0, self.on_status_change)

    def _render_success(self):
        self.btn_reload.configure(state="normal")
        self.btn_export.configure(state="normal")
        self.state_frame.pack_forget()
        self.grid_frame.pack(fill="x")
        
        self._update_grid()

    def _render_error(self, err_msg):
        self.btn_reload.configure(state="normal")
        self.grid_frame.pack_forget()
        for w in self.state_frame.winfo_children(): w.destroy()
        self.state_frame.pack(fill="x", expand=True)
        display_msg = err_msg
        if "401" in err_msg or "403" in err_msg or "permission" in err_msg.lower():
            display_msg = "DLP policies telemetry permission required.\nPlease grant required application permissions to your App Registration in Microsoft Entra ID."
        ctk.CTkLabel(self.state_frame, text=f"✖ {display_msg}", text_color=COLOR_ERROR, font=FONT_BODY_MEDIUM, justify="center", wraplength=700).pack(pady=(15, 5))
        ctk.CTkButton(self.state_frame, text="Try Again", command=self.trigger_fetch_individual, width=100, fg_color="transparent", border_width=1, text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER).pack(pady=(0, 15))

    def _load_page(self):
        if not self.csv_path or not os.path.exists(self.csv_path): return [], 0
        items = []
        total_count = 0
        try:
            with open(self.csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                all_rows = list(reader)
                total_count = len(all_rows)
                start_idx = self.page * self.ITEMS_PER_PAGE
                end_idx = start_idx + self.ITEMS_PER_PAGE
                items = all_rows[start_idx:end_idx]
        except Exception as e:
            usage_logger.error(f"Error reading DLP CSV: {e}")
        return items, total_count

    def _update_grid(self):
        for w in self.grid_frame.winfo_children():
            info = w.grid_info()
            if "row" in info and int(info["row"]) > 0: w.destroy()

        page_data, total_count = self._load_page()
        total_pages = (total_count - 1) // self.ITEMS_PER_PAGE + 1 if total_count > 0 else 1

        for offset, row_item in enumerate(page_data, start=1):
            r_idx = offset
            bg_style = COLOR_SURFACE if r_idx % 2 == 0 else COLOR_SURFACE_VARIANT
            
            name = row_item.get("Name", "N/A")
            mode = row_item.get("Mode", "N/A")
            workloads = row_item.get("Workload", "N/A")
            
            en_val = str(row_item.get("Enabled", "")).lower()
            enabled = "🟢 Enabled" if en_val in ("true", "1", "yes") else "🔴 Disabled"
            
            exc = "EX" if row_item.get("ExchangeLocation") else ""
            spo = "SPO" if row_item.get("SharePointLocation") else ""
            od = "OD" if row_item.get("OneDriveLocation") else ""
            locations = ", ".join([loc for loc in (exc, spo, od) if loc]) or "N/A"
            
            actions = row_item.get("Actions", "None")

            vals = [name, mode, workloads, enabled, locations, actions]
            for col_idx, val in enumerate(vals):
                cell = ctk.CTkFrame(self.grid_frame, fg_color=bg_style, corner_radius=0)
                cell.grid(row=r_idx, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
                lbl = ctk.CTkLabel(cell, text=str(val), font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="left", wraplength=140)
                lbl.pack(padx=10, pady=12, anchor="nw")
                
            # Regex Column
            cell = ctk.CTkFrame(self.grid_frame, fg_color=bg_style, corner_radius=0)
            cell.grid(row=r_idx, column=6, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkButton(cell, text="View", width=50, command=lambda: messagebox.showinfo("Regex View", "Regex rules will be downloaded as a TXT file via Export.")).pack(padx=5, pady=10)

        # Pagination controls row
        control_frame = ctk.CTkFrame(self.grid_frame, fg_color="transparent")
        control_frame.grid(row=self.ITEMS_PER_PAGE + 1, column=0, columnspan=6, pady=0, sticky="ew")

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
        if not self.csv_path or not os.path.exists(self.csv_path):
            messagebox.showinfo("No Data", "There is no DLP policy data to export.", parent=self)
            return
            
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        f = filedialog.asksaveasfilename(
            initialfile=f"dlp_policies_{ts}.csv",
            defaultextension=".csv",
            filetypes=[("CSV Files", "*.csv")],
            parent=self
        )
        if not f: return
        try:
            shutil.copyfile(self.csv_path, f)
            messagebox.showinfo("Export Successful", f"DLP policies exported to:\n{f}", parent=self)
        except Exception as e:
            messagebox.showerror("Export Failed", f"Error: {e}", parent=self)

    @property
    def last_data(self):
        if not self.csv_path or not os.path.exists(self.csv_path): return []
        try:
            with open(self.csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                return list(reader)
        except Exception:
            return []

    def cancel(self):
        self.is_cancelled = True
        self.status = None
