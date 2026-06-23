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

"""UI component for Detected Apps telemetry."""

import os
import time
import logging
import threading
import pandas as pd
import customtkinter as ctk
from tkinter import filedialog, messagebox

from core.graph.intune.detected_apps import run_detected_apps_pipeline
from telemetry.styles import *

usage_logger = logging.getLogger("M365TelemetryAsyncLogger.DetectedAppsUI")

class DetectedAppsSubFrame(ctk.CTkFrame):
    """Sub-frame for Detected Apps."""

    def __init__(self, master, log_callback, credentials_callback, status_change_callback, semaphore=None, **kwargs):
        super().__init__(master, fg_color="transparent", **kwargs)
        self.log_msg = log_callback
        self.get_credentials = credentials_callback
        self.on_status_change = status_change_callback
        self.semaphore = semaphore
        self.status = None
        self.start_time = 0
        self.lbl_timer = None
        self.page = 0
        self.ITEMS_PER_PAGE = 5
        self.csv_path = None
        self.is_cancelled = False

        self.build_ui()

    def build_ui(self):
        self.header_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.header_frame.pack(fill="x", pady=(0, 5))
        
        ctk.CTkLabel(self.header_frame, text="2. Detected Apps", font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN).pack(side="left")
        
        self.btn_reload = ctk.CTkButton(
            self.header_frame, text="↻ Reload", width=80, height=24,
            font=ctk.CTkFont(family="Segoe UI", size=12),
            fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color="#2563EB", hover_color="#DBEAFE",
            command=self.trigger_fetch_individual
        )
        self.btn_reload.pack(side="right", padx=(10, 0))
        
        self.btn_export = ctk.CTkButton(
            self.header_frame, text="Export Detected Apps", width=180, height=32, corner_radius=16,
            font=FONT_BODY_BOLD, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER,
            command=self._export
        )
        self.btn_export.pack(side="right")

        self.state_frame = ctk.CTkFrame(self, fg_color="transparent")
        
        self.grid_frame = ctk.CTkFrame(self, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)
        
        headers = ["App Name", "Version", "Publisher", "Platform"]
        for i in range(4):
            self.grid_frame.grid_columnconfigure(i, weight=2 if i == 0 else 1)
            
        for col_idx, head_text in enumerate(headers):
            cell = ctk.CTkFrame(self.grid_frame, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=1, pady=1)
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        self.reset_view()

    def reset_view(self):
        self.status = None
        self.is_cancelled = False
        self.csv_path = None
        self.page = 0
        self.state_frame.pack_forget()
        self.grid_frame.pack_forget()
        if self.lbl_timer and self.lbl_timer.winfo_exists():
            self.lbl_timer.destroy()
            self.lbl_timer = None
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
        self.start_time = time.time()
        self.btn_reload.configure(state="disabled")
        self.btn_export.configure(state="disabled")
        self.grid_frame.pack_forget()
        
        script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
        if os.path.basename(script_dir) == "intune":
            script_dir = os.path.dirname(script_dir)
        reports_dir = os.path.join(script_dir, "reports", f"{tenant}_{client_id}")
        self.csv_path = os.path.join(reports_dir, "intune_detected_apps.csv")

        self._set_loading_state("Scanning Detected Apps (v1.0)...")
        self.on_status_change()
        
        if self.lbl_timer and self.lbl_timer.winfo_exists():
            self.lbl_timer.destroy()
            self.lbl_timer = None
            
        threading.Thread(target=self._execute_worker, args=(tenant, client_id, client_secret), daemon=True).start()

    def _execute_worker(self, tenant, client_id, client_secret):
        if self.semaphore: self.semaphore.acquire()
        try:
            run_detected_apps_pipeline(
                client_id=client_id,
                client_secret=client_secret,
                tenant_id=tenant,
                csv_path=self.csv_path,
                max_rows=10000,
                is_cancelled_callback=lambda: self.is_cancelled
            )
            self.status = "success"
            self.after(0, self._render_success)
        except Exception as e:
            usage_logger.error(f"Detected apps fetch error: {e}", exc_info=True)
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
        
        if self.start_time > 0:
            elapsed = time.time() - self.start_time
            if self.lbl_timer and self.lbl_timer.winfo_exists(): self.lbl_timer.destroy()
            self.lbl_timer = ctk.CTkLabel(self, text=f"⏱ {elapsed:.2f}s", font=ctk.CTkFont(family="Segoe UI", size=11, weight="bold"), text_color=COLOR_PRIMARY)
            self.lbl_timer.pack(in_=self.header_frame, side="right", padx=(0, 15))

        self._update_grid()

    def _render_error(self, err):
        self.btn_reload.configure(state="normal")
        self.grid_frame.pack_forget()
        for w in self.state_frame.winfo_children(): w.destroy()
        self.state_frame.pack(fill="x", expand=True)
        ctk.CTkLabel(self.state_frame, text=f"✖ {err}", text_color=COLOR_ERROR, font=FONT_BODY_MEDIUM, justify="center", wraplength=700).pack(pady=(15, 5))
        ctk.CTkButton(self.state_frame, text="Try Again", command=self.trigger_fetch_individual, width=100, fg_color="transparent", border_width=1, text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER).pack(pady=(0, 15))

    def _load_page(self):
        if not self.csv_path or not os.path.exists(self.csv_path): return [], 0
        try:
            df = pd.read_csv(self.csv_path).fillna("N/A")
            total = len(df)
            start = self.page * self.ITEMS_PER_PAGE
            end = start + self.ITEMS_PER_PAGE
            return df.iloc[start:end].to_dict('records'), total
        except Exception as e:
            usage_logger.error(f"Error reading CSV {self.csv_path}: {e}")
            return [], 0

    def _update_grid(self):
        for w in self.grid_frame.winfo_children():
            info = w.grid_info()
            if "row" in info and int(info["row"]) > 0: w.destroy()

        page_data, total_count = self._load_page()
        total_pages = (total_count - 1) // self.ITEMS_PER_PAGE + 1 if total_count > 0 else 1

        for row_idx, item in enumerate(page_data, start=1):
            bg_style = COLOR_SURFACE if row_idx % 2 == 0 else COLOR_SURFACE_VARIANT
            vals = [
                item.get("displayName", "N/A"),
                item.get("version", "N/A"),
                item.get("publisher", "N/A"),
                item.get("platform", "unknown")
            ]
            for col_idx, val in enumerate(vals):
                cell = ctk.CTkFrame(self.grid_frame, fg_color=bg_style, corner_radius=0)
                cell.grid(row=row_idx, column=col_idx, sticky="nsew", padx=1, pady=1)
                lbl = ctk.CTkLabel(cell, text=str(val), font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, wraplength=200, justify="left", anchor="w")
                lbl.pack(padx=10, pady=8, fill="x")

        # Pagination controls row
        control_frame = ctk.CTkFrame(self.grid_frame, fg_color="transparent")
        control_frame.grid(row=self.ITEMS_PER_PAGE + 1, column=0, columnspan=4, pady=0, sticky="ew")

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
            messagebox.showerror("Export Error", "No data available to export yet.")
            return

        dest_path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            title="Export Detected Apps CSV",
            initialfile="M365_Intune_Detected_Apps.csv",
            parent=self
        )
        if not dest_path: return
        
        try:
            df = pd.read_csv(self.csv_path)
            df.fillna("N/A").to_csv(dest_path, index=False, encoding="utf-8-sig")
            messagebox.showinfo("Export Successful", f"Detected Apps successfully exported to:\n{dest_path}")
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
