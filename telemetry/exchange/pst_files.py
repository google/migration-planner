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

"""UI frame for Exchange Online PST Files discovery telemetry."""

import os
import csv
import logging
import threading
import asyncio
import sqlite3
import customtkinter as ctk

from core.graph.exchange.pst_files import run_pst_discovery_pipeline
from core.graph.exchange.mailbox import format_bytes
from core.graph.db import import_csv_to_sqlite, query_page_sync
from telemetry.styles import *

usage_logger = logging.getLogger("M365TelemetryAsyncLogger.PstFilesUI")

class PstFilesFrame(ctk.CTkFrame):
    """Self-contained component wrapping Exchange Online PST Files discovery UI."""

    def __init__(self, master, log_callback, credentials_callback, status_change_callback, **kwargs):
        self.semaphore = kwargs.pop("concurrency_semaphore", None)
        super().__init__(master, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=12, **kwargs)
        self.log_msg = log_callback
        self.get_credentials = credentials_callback
        self.on_status_change = status_change_callback
        
        self.status = None
        self._cached_pst_data = {}
        self.pst_disclaimer_lbl = None

        self.build_ui()

    def build_ui(self):
        self.pack(fill="x", expand=True, pady=10)

        self.inner_pad = ctk.CTkFrame(self, fg_color="transparent")
        self.inner_pad.pack(fill="both", expand=True, padx=20, pady=20)

        self.pst_header_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.pst_header_frame.pack(fill="x", pady=(0, 10))
        ctk.CTkLabel(self.pst_header_frame, text="PST Files", font=FONT_HEADER_SMALL, text_color=COLOR_TEXT_MAIN).pack(side="left")
        self.pst_reload_btn = ctk.CTkButton(
            self.pst_header_frame, 
            state="disabled", text="↻ Reload", width=80, height=24,
            font=ctk.CTkFont(family="Segoe UI", size=12),
            fg_color="transparent", border_width=1, text_color="#2563EB", hover_color="#DBEAFE",
            command=self._retry_pst_fetch
        )
        self.pst_reload_btn.pack(side="right")
        
        self.pst_grid_frame = ctk.CTkFrame(self.inner_pad, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)
        self.pst_grid_frame.pack(fill="x", expand=True)

        self.reset_view()

    def reset_view(self):
        self.pack_forget()
        self.status = None
        self._cached_pst_data = {}
        for w in self.pst_grid_frame.winfo_children(): w.destroy()
        if hasattr(self, 'pst_disclaimer_lbl') and self.pst_disclaimer_lbl:
            self.pst_disclaimer_lbl.destroy()
            self.pst_disclaimer_lbl = None

    def _retry_pst_fetch(self):
        tenant, clients, secrets = self.get_credentials()
        if tenant and clients and secrets:
            self.pst_reload_btn.configure(state="disabled")
            self.trigger_fetch(tenant, clients[0], secrets[0])

    def trigger_fetch(self, tenant, client_id, client_secret):
        self.pack(fill="x", expand=True, pady=10)
        self.status = "loading"
        self.on_status_change()
        
        for w in self.pst_grid_frame.winfo_children(): w.destroy()
        f = ctk.CTkFrame(self.pst_grid_frame, fg_color="transparent")
        ctk.CTkLabel(f, text="⏳ Discovering PST Files...", text_color=COLOR_TEXT_SUB, font=FONT_BODY_MEDIUM).pack(pady=(20, 5))
        pb = ctk.CTkProgressBar(f, mode="indeterminate", width=250, fg_color=COLOR_SURFACE_VARIANT, progress_color=COLOR_PRIMARY)
        pb.pack(pady=(0, 20))
        pb.start()
        f.pack(fill="x", expand=True)
        
        threading.Thread(target=self._execute_pst_worker, args=(tenant, client_id, client_secret), daemon=True).start()

    def _execute_pst_worker(self, tenant, client_id, client_secret):
        if self.semaphore: self.semaphore.acquire()
        try:
            data = run_pst_discovery_pipeline(client_id, client_secret, tenant)
            if not data.get("pst_error"):
                script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
                if os.path.basename(script_dir) == "exchange":
                    script_dir = os.path.dirname(script_dir)
                reports_dir = os.path.join(script_dir, "reports", f"{tenant}_{client_id}")
                os.makedirs(reports_dir, exist_ok=True)
                csv_path = os.path.join(reports_dir, "pst_discovery.csv")
                
                pst_cloud = data.get("pst_cloud_data", {})
                cloud_count = 0
                cloud_bytes = 0
                if pst_cloud and "value" in pst_cloud:
                    for item in pst_cloud.get("value", []):
                        for hc in item.get("hitsContainers", []):
                            cloud_count += hc.get("total", 0)
                            for hit in hc.get("hits", []):
                                cloud_bytes += int(hit.get("resource", {}).get("size", 0))
                
                with open(csv_path, 'w', encoding='utf-8', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(["Location", "Discovered File Count", "Total Size (Bytes)"])
                    writer.writerow(["Cloud (SharePoint & OneDrive)", cloud_count, cloud_bytes])

                db_path = os.path.join(reports_dir, "telemetry_cache.db")
                asyncio.run(import_csv_to_sqlite(csv_path, db_path, "pst_files"))

            self.after(0, self._render_pst_success, data)
        except Exception as e:
            self.after(0, self._render_pst_error, str(e))
        finally:
            if self.semaphore: self.semaphore.release()

    def _render_pst_error(self, error_msg):
        self._cached_pst_data = {"pst_error": error_msg}
        self.pst_reload_btn.configure(state="normal")
        for w in self.pst_grid_frame.winfo_children(): w.destroy()
        f = ctk.CTkFrame(self.pst_grid_frame, fg_color="transparent")
        display_msg = error_msg
        if "401" in error_msg or "403" in error_msg or "unauthorized" in error_msg.lower():
            display_msg = "Search permission required. Please grant 'Files.Read.All'."
        ctk.CTkLabel(f, text=f"✖ {display_msg}", text_color=COLOR_ERROR, font=FONT_BODY_MEDIUM, justify="center").pack(pady=(20, 5))
        ctk.CTkButton(f, text="Try Again", command=self._retry_pst_fetch, width=120, fg_color="transparent", border_width=1, text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER).pack(pady=(0, 20))
        f.pack(fill="x", expand=True)
        self.status = "error"
        self.on_status_change()

    def _render_pst_success(self, data: dict):
        self._cached_pst_data = data
        self.pst_reload_btn.configure(state="normal")
        for w in self.pst_grid_frame.winfo_children(): w.destroy()
        
        self.pst_grid_frame.grid_columnconfigure(0, weight=2)
        self.pst_grid_frame.grid_columnconfigure(1, weight=5)

        headers_pst = ["PST Storage Location", "Discovered File Count & Size"]
        for col_idx, head_text in enumerate(headers_pst):
            cell = ctk.CTkFrame(self.pst_grid_frame, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=1, pady=1)
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        pst_err = data.get("pst_error")
        cloud_count = 0
        if pst_err:
            cloud_str = f"✖ Error: {pst_err}"
        else:
            pst_cloud = data.get("pst_cloud_data", {})
            cloud_bytes = 0
            if pst_cloud and "value" in pst_cloud:
                for item in pst_cloud.get("value", []):
                    for hc in item.get("hitsContainers", []):
                        cloud_count += hc.get("total", 0)
                        for hit in hc.get("hits", []):
                            cloud_bytes += int(hit.get("resource", {}).get("size", 0))

            cloud_size_str = f" ({format_bytes(cloud_bytes)})" if cloud_bytes > 0 else ""
            cloud_str = f"{cloud_count:,} Files{cloud_size_str}" if cloud_count > 0 else "None Detected"

        rows_pst = [
            ("Cloud (SharePoint & OneDrive)", cloud_str)
        ]

        for p_idx, (p_name, p_val) in enumerate(rows_pst, start=1):
            bg_p = "transparent" if p_idx % 2 != 0 else COLOR_SURFACE_VARIANT
            
            pp0 = ctk.CTkFrame(self.pst_grid_frame, fg_color=bg_p, corner_radius=0)
            pp0.grid(row=p_idx, column=0, sticky="nsew", padx=1, pady=1)
            ctk.CTkLabel(pp0, text=p_name, font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=10, anchor="nw")

            pp1 = ctk.CTkFrame(self.pst_grid_frame, fg_color=bg_p, corner_radius=0)
            pp1.grid(row=p_idx, column=1, sticky="nsew", padx=1, pady=1)
            ctk.CTkLabel(pp1, text=p_val, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="left").pack(padx=10, pady=10, anchor="nw")

        if not pst_err and cloud_count > 0:
            if hasattr(self, 'pst_disclaimer_lbl') and self.pst_disclaimer_lbl:
                self.pst_disclaimer_lbl.destroy()
            self.pst_disclaimer_lbl = ctk.CTkLabel(
                self.inner_pad, 
                text="* Note: There may be more than 2,000 files in the tenant; this tool only checks up to 2,000 files.",
                font=FONT_BODY_SMALL,
                text_color=COLOR_TEXT_SUB,
                justify="left"
            )
            self.pst_disclaimer_lbl.pack(anchor="w", pady=(10, 0))

        self.status = "success"
        self.on_status_change()

    def cancel(self):
        pass

    def _load_pst_data_from_csv(self):
        tenant, clients, secrets = self.get_credentials()
        if not tenant or not clients:
            return {}
            
        script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
        if os.path.basename(script_dir) == "exchange":
            script_dir = os.path.dirname(script_dir)
        db_path = os.path.join(script_dir, "reports", f"{tenant}_{clients[0]}", "telemetry_cache.db")
        
        if not os.path.exists(db_path):
            return {}
            
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM pst_files")
            rows = cursor.fetchall()
            conn.close()
            if rows:
                row = rows[0]
                if len(row) >= 3:
                    count, size = int(row[1]), int(row[2])
                    return {
                        "pst_cloud_data": {
                            "value": [
                                {
                                    "hitsContainers": [
                                        {
                                            "total": count,
                                            "hits": [
                                                {
                                                    "resource": {
                                                        "size": size
                                                    }
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        },
                        "pst_error": None
                    }
            return {}
        except Exception as e:
            usage_logger.error(f"Error loading PST data from DB: {e}")
            return {"pst_error": str(e)}

    @property
    def last_data(self):
        if hasattr(self, "_cached_pst_data") and self._cached_pst_data:
            return self._cached_pst_data
        return self._load_pst_data_from_csv()
