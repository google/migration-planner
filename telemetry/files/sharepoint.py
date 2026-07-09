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

"""UI frame for SharePoint Online Site Usage telemetry."""

import os
import logging
import threading
import customtkinter as ctk

from core.graph.files.sharepoint import run_sharepoint_pipeline
from core.graph.files.sharepoint_data_types import run_sharepoint_data_types_pipeline
import concurrent.futures
from telemetry.styles import *

usage_logger = logging.getLogger("M365TelemetryAsyncLogger.SharePointUsageUI")

class SharePointUsageFrame(ctk.CTkFrame):
    """Self-contained customtkinter component wrapping SharePoint Telemetry UI."""
    
    def __init__(self, master, log_callback, credentials_callback, status_change_callback, **kwargs):
        self.semaphore = kwargs.pop("concurrency_semaphore", None)
        super().__init__(master, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=12, **kwargs)
        self.log_msg = log_callback
        self.get_credentials = credentials_callback
        self.on_status_change = status_change_callback
        self.status = None  # 'loading', 'success', 'error', None
        self.last_data = {}
        
        self.build_ui()

    def build_ui(self):
        """Creates card container for the tab."""
        self.pack(fill="x", expand=True, pady=10)
        
        self.inner_pad = ctk.CTkFrame(self, fg_color="transparent")
        self.inner_pad.pack(fill="both", expand=True, padx=20, pady=20)
        
        self.header = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.header.pack(fill="x", pady=(0, 10))
        ctk.CTkLabel(self.header, text="SharePoint Overview", font=FONT_HEADER_SMALL, text_color=COLOR_TEXT_MAIN).pack(side="left")
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
        self.grid_frame = ctk.CTkFrame(self.inner_pad, fg_color=COLOR_OUTLINE_LIGHT, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)
        self.heavy_sites_master = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        
        self.reset_view()

    def reset_view(self):
        """Resets and hides grids."""
        self.pack_forget()
        self.state_frame.pack_forget()
        self.grid_frame.pack_forget()
        self.heavy_sites_master.pack_forget()
        self.last_data = {}
        
        for w in self.state_frame.winfo_children():
            w.destroy()
        for w in self.grid_frame.winfo_children():
            w.destroy()
        for w in self.heavy_sites_master.winfo_children():
            w.destroy()

    def _set_state_loading(self, msg="Loading..."):
        for w in self.state_frame.winfo_children():
            w.destroy()
        self.loading_label = ctk.CTkLabel(self.state_frame, text=f"⏳ {msg}", text_color="#6b7280", font=ctk.CTkFont(family="Segoe UI", size=13))
        self.loading_label.pack(pady=(20, 5))
        pb = ctk.CTkProgressBar(self.state_frame, mode="indeterminate", width=250, fg_color="#F3F4F6", progress_color="#2563EB")
        pb.pack(pady=(0, 20))
        pb.start()
        self.state_frame.pack(fill="x", expand=True)

    def _set_state_error(self, error_msg):
        for w in self.state_frame.winfo_children():
            w.destroy()

        display_msg = error_msg
        if "401" in error_msg or "403" in error_msg or "unauthorized" in error_msg.lower() or "forbidden" in error_msg.lower():
            display_msg = "Reports telemetry permission required.\nPlease grant the 'Reports.Read.All' application permission to your App Registration in Microsoft Entra ID."

        ctk.CTkLabel(self.state_frame, text=f"✖ {display_msg}", text_color=COLOR_ERROR, font=FONT_BODY_MEDIUM, justify="center").pack(pady=(20, 5))
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
        usage_logger.info("SharePoint Site Usage trigger_fetch called. Spawning background worker thread...")
        self.status = "loading"
        self.on_status_change()
        
        self.pack(fill="x", expand=True, pady=(20, 5))
        self.grid_frame.pack_forget()
        
        self._set_state_loading("Downloading and parsing SharePoint Site Usage and Data Types...")
        
        threading.Thread(
            target=self._execute_worker,
            args=(tenant, client_id, client_secret),
            daemon=True
        ).start()

    def _execute_worker(self, tenant: str, client_id: str, client_secret: str):
        if self.semaphore:
            self.semaphore.acquire()
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                usage_future = executor.submit(run_sharepoint_pipeline, client_id, client_secret, tenant)
                datatypes_future = executor.submit(run_sharepoint_data_types_pipeline, client_id, client_secret, tenant)
                
                usage_data = usage_future.result()
                datatypes_data = datatypes_future.result()
            
            combined_data = {**usage_data, **datatypes_data}
            
            script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
            if os.path.basename(script_dir) == "files":
                script_dir = os.path.dirname(script_dir)
            reports_dir = os.path.join(script_dir, "reports", f"{tenant}_{client_id}")
            self.spo_csv = os.path.join(reports_dir, "SharePointSiteUsageDetail(180d).csv")
            
            usage_logger.info("Successfully completed SharePoint telemetry data fetch.")
            self.after(0, self._render_success, combined_data)
        except Exception as e:
            usage_logger.error("Exception caught in SharePoint worker.", exc_info=True)
            self.after(0, self._render_error, str(e))
        finally:
            if self.semaphore:
                self.semaphore.release()

    def _render_success(self, data: dict):
        if hasattr(self, 'reload_btn') and self.reload_btn.winfo_exists():
            self.reload_btn.configure(state="normal")
        self.last_data = data
        usage_logger.info("SharePoint Site Usage data successfully retrieved. Rendering UI grid.")
        self.state_frame.pack_forget()
        for w in self.grid_frame.winfo_children():
            w.destroy()

        self.grid_frame.pack(fill="x", expand=True)

        self.grid_frame.grid_columnconfigure(0, weight=3)
        self.grid_frame.grid_columnconfigure(1, weight=2)

        headers_sp = ["SharePoint Metric Description", "Value / Measurement"]
        for col_idx, head_text in enumerate(headers_sp):
            cell = ctk.CTkFrame(self.grid_frame, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        rows_data = [
            ("Total Sites Count", f"{data.get('total_sites', 0):,} Sites"),
            ("Total Storage Used", data.get("total_storage_formatted", "0.00 Bytes")),
            ("Total Files Stored", f"{data.get('total_files', 0):,} Files"),
            ("Active Files Count (180 days)", f"{data.get('active_files', 0):,} Files ({data.get('active_files_pct', 0.0):.1f}%)"),
            ("Document Libraries", f"{data.get('Document Libraries', 0):,}"),
            ("Lists", f"{data.get('Lists', 0):,}"),
            ("Web Pages", f"{data.get('Web Pages', 0):,}")
        ]

        for r_idx, (metric_name, val) in enumerate(rows_data, start=1):
            bg_style = COLOR_SURFACE if r_idx % 2 == 0 else COLOR_SURFACE_VARIANT
            
            c0 = ctk.CTkFrame(self.grid_frame, fg_color=bg_style, corner_radius=0)
            c0.grid(row=r_idx, column=0, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c0, text=metric_name, font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=6, anchor="w")

            c1 = ctk.CTkFrame(self.grid_frame, fg_color=bg_style, corner_radius=0)
            c1.grid(row=r_idx, column=1, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c1, text=val, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=6, anchor="w")

        # --- TENANT INVENTORY & HEAVY SITES ---
        self.heavy_sites = data.get("heavy_sites", [])
        if self.heavy_sites:
            self.current_page = 0
            self.page_size = 10
            
            # Show the separate master frame
            self.heavy_sites_master.pack(fill="x", expand=True, pady=(20, 0))
            
            spo_header = ctk.CTkFrame(self.heavy_sites_master, fg_color="transparent")
            spo_header.pack(fill="x", padx=10, pady=(0, 10))
            
            ctk.CTkLabel(spo_header, text="Heavy Sites Inventory", font=FONT_HEADER_SMALL, text_color=COLOR_TEXT_MAIN).pack(side="left")
            
            self.heavy_sites_controls = ctk.CTkFrame(spo_header, fg_color="transparent")
            self.heavy_sites_controls.pack(side="right")
            
            # Match the reload button style for View Report
            ctk.CTkButton(self.heavy_sites_controls, text="View Report", width=80, height=24,
                          font=ctk.CTkFont(family="Segoe UI", size=12), fg_color="transparent", border_width=1,
                          text_color="#2563EB", hover_color="#DBEAFE",
                          command=lambda: __import__('os').system(f"open '{getattr(self, 'spo_csv', '')}'" if __import__('os').name == "posix" else f"start \"\" \"{getattr(self, 'spo_csv', '')}\"")).pack(side="right", padx=(10, 0))
            
            self.heavy_sites_container = ctk.CTkFrame(self.heavy_sites_master, fg_color=COLOR_OUTLINE_LIGHT, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)
            self.heavy_sites_container.pack(fill="x", expand=True)
            
            self._render_heavy_sites_page()

        self.status = "success"
        self.on_status_change()

    def _render_heavy_sites_page(self):
        # Clear existing controls and container
        for widget in self.heavy_sites_container.winfo_children():
            widget.destroy()
            
        for widget in self.heavy_sites_controls.winfo_children():
            # Only destroy pagination buttons in header (if any leftover from previous logic)
            if getattr(widget, "is_pagination", False):
                widget.destroy()
                
        total_pages = max(1, (len(self.heavy_sites) + self.page_size - 1) // self.page_size)
        
        spo_grid = ctk.CTkFrame(self.heavy_sites_container, fg_color=COLOR_SURFACE, corner_radius=0)
        spo_grid.pack(fill="x", expand=True)
        
        spo_headers = ["URL", "Site ID", "Storage (MB)"]
        spo_grid.grid_columnconfigure(0, weight=3)
        spo_grid.grid_columnconfigure(1, weight=2)
        spo_grid.grid_columnconfigure(2, weight=1)
        
        for c_idx, h_text in enumerate(spo_headers):
            cell = ctk.CTkFrame(spo_grid, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=c_idx, sticky="nsew", padx=0, pady=(0, 1) if c_idx == 0 else (0, 1)) # Keep padding same
            ctk.CTkLabel(cell, text=h_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")
            
        start_idx = self.current_page * self.page_size
        end_idx = min(start_idx + self.page_size, len(self.heavy_sites))
        page_sites = self.heavy_sites[start_idx:end_idx]
        
        for s_idx, site in enumerate(page_sites, start=1):
            bg_style = COLOR_SURFACE if s_idx % 2 != 0 else COLOR_SURFACE_VARIANT
            
            url_c = ctk.CTkFrame(spo_grid, fg_color=bg_style, corner_radius=0)
            url_c.grid(row=s_idx, column=0, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(url_c, text=site.get("Site URL", ""), font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, anchor="w").pack(padx=10, pady=6, anchor="w")
            
            id_c = ctk.CTkFrame(spo_grid, fg_color=bg_style, corner_radius=0)
            id_c.grid(row=s_idx, column=1, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(id_c, text=site.get("Site Id", ""), font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, anchor="w").pack(padx=10, pady=6, anchor="w")
            
            stor_c = ctk.CTkFrame(spo_grid, fg_color=bg_style, corner_radius=0)
            stor_c.grid(row=s_idx, column=2, sticky="nsew", padx=0, pady=(0, 1))
            
            # Convert bytes to MB
            stor_mb = site.get("Storage Used (Byte)", 0) / (1024 * 1024)
            ctk.CTkLabel(stor_c, text=f"{stor_mb:,.2f} MB", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, anchor="w").pack(padx=10, pady=6, anchor="w")

        # Add Pagination Controls at the bottom
        if total_pages > 1:
            control_frame = ctk.CTkFrame(spo_grid, fg_color=COLOR_SURFACE)
            # Use max(1, len(page_sites)) + 1 for row to place it at the very bottom
            control_frame.grid(row=self.page_size + 2, column=0, columnspan=3, pady=0, sticky="ew")

            center_container = ctk.CTkFrame(control_frame, fg_color="transparent")
            center_container.pack(pady=(5, 10))

            btn_prev = ctk.CTkButton(
                center_container, text="◀ Prev", width=70, height=22, corner_radius=6,
                font=FONT_BODY_SMALL, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
                text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER, 
                state="normal" if self.current_page > 0 else "disabled",
                command=lambda: self._change_page(-1)
            )
            btn_prev.pack(side="left", padx=5)

            page_lbl = ctk.CTkLabel(
                center_container, text=f"Page {self.current_page + 1} of {total_pages}",
                font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB
            )
            page_lbl.pack(side="left", padx=15)

            btn_next = ctk.CTkButton(
                center_container, text="Next ▶", width=70, height=22, corner_radius=6,
                font=FONT_BODY_SMALL, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
                text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER, 
                state="normal" if self.current_page < total_pages - 1 else "disabled",
                command=lambda: self._change_page(1)
            )
            btn_next.pack(side="left", padx=5)

    def _change_page(self, delta):
        self.current_page += delta
        self._render_heavy_sites_page()

    def _render_error(self, err_msg):
        usage_logger.warning(f"SharePoint Site Usage fetch failed: {err_msg}")
        if hasattr(self, 'reload_btn') and self.reload_btn.winfo_exists():
            self.reload_btn.configure(state="normal")
        self._set_state_error(err_msg)
        self.status = "error"
        self.on_status_change()

    def cancel(self):
        pass
