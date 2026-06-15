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

"""Modular Subscribed SKUs Inventory Summary telemetry scanners and visual interfaces."""

import os
import logging
import threading
import webbrowser
import pandas as pd
from datetime import datetime
from tkinter import filedialog, messagebox
from typing import Any, Dict, List, Optional
import customtkinter as ctk

# Import unified core service layer
from core.graph.client import GraphClient
from core.graph.directory import DirectoryService

# Bind to the async logger initialized in m365_telemetry.py
usage_logger = logging.getLogger("M365TelemetryAsyncLogger")

# =================================================================================
# CONSTANTS & STYLES (Imported from shared styles)
# =================================================================================
from telemetry.styles import *

class SubscribedSKUsFrame(ctk.CTkFrame):
    """Self-contained customtkinter component wrapping Subscribed SKUs Inventory Summary UI."""
    
    def __init__(self, master, log_callback, credentials_callback, status_change_callback, retries_var=None, backoff_var=None, **kwargs):
        self.semaphore = kwargs.pop("concurrency_semaphore", None)
        super().__init__(master, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=12, **kwargs)
        self.log_msg = log_callback
        self.get_credentials = credentials_callback
        self.on_status_change = status_change_callback
        self.retries = retries_var
        self.backoff = backoff_var
        self.status = None  # 'loading', 'success', 'error', None
        self.last_licenses_items = []
        
        self.build_ui()

    def build_ui(self):
        """Creates card container for the tab."""
        self.pack(fill="x", expand=True, pady=10)
        
        self.inner_pad = ctk.CTkFrame(self, fg_color="transparent")
        self.inner_pad.pack(fill="both", expand=True, padx=20, pady=20)
        
        self.lic_header = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.lic_header.pack(fill="x", pady=(0, 10))
        
        self.header = ctk.CTkFrame(self.lic_header, fg_color="transparent")
        self.header.pack(fill="x", pady=(0, 10))
        ctk.CTkLabel(self.header, text="Subscribed SKUs", font=FONT_HEADER_SMALL, text_color=COLOR_TEXT_MAIN).pack(side="left")
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
        
        self.lic_reference_link = ctk.CTkLabel(
            self.lic_header,
            text="Service Plan Reference ↗",
            font=FONT_BODY_BOLD,
            text_color=COLOR_PRIMARY,
            cursor="hand2"
        )
        self.lic_reference_link.pack(side="left", padx=(15, 0))
        self.lic_reference_link.bind("<Button-1>", lambda e: webbrowser.open("https://learn.microsoft.com/en-us/entra/identity/users/licensing-service-plan-reference"))
        self.lic_reference_link.bind("<Enter>", lambda e: self.lic_reference_link.configure(text_color=COLOR_PRIMARY_HOVER))
        self.lic_reference_link.bind("<Leave>", lambda e: self.lic_reference_link.configure(text_color=COLOR_PRIMARY))

        ctk.CTkLabel(self.lic_header, text="* To view specific services offered, export the spreadsheet.", font=FONT_BODY_SMALL, text_color=COLOR_TEXT_SUB).pack(side="left", padx=(10, 0))

        self.btn_export_lic = ctk.CTkButton(
            self.lic_header, text="Export Spreadsheet", width=140, height=32, corner_radius=16,
            font=FONT_BODY_BOLD, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER,
            command=self.export_licenses_spreadsheet, state="disabled"
        )
        self.btn_export_lic.pack(side="right")

        self.state_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.grid_frame = ctk.CTkFrame(self.inner_pad, fg_color=COLOR_OUTLINE_LIGHT, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)
        
        self.reset_view()

    def reset_view(self):
        """Resets and hides grids."""
        self.pack_forget()
        self.state_frame.pack_forget()
        self.grid_frame.pack_forget()
        self.btn_export_lic.configure(state="disabled")
        self.last_licenses_items = []
        
        for w in self.state_frame.winfo_children():
            w.destroy()
        for w in self.grid_frame.winfo_children():
            w.destroy()

    def _set_state_loading(self, msg="Loading..."):
        for w in self.state_frame.winfo_children():
            w.destroy()
        ctk.CTkLabel(self.state_frame, text=f"⏳ {msg}", text_color=COLOR_TEXT_SUB, font=FONT_BODY_MEDIUM).pack(pady=(20, 5))
        self.progress = __import__("customtkinter").CTkProgressBar(self.state_frame, mode="indeterminate", width=250, fg_color="#F3F4F6", progress_color="#2563EB")
        self.progress.pack(pady=(0, 20))
        self.progress.start()
        self.state_frame.pack(fill="x", expand=True)

    def _set_state_error(self, error_msg):
        for w in self.state_frame.winfo_children():
            w.destroy()

        display_msg = error_msg
        if "401" in error_msg or "403" in error_msg or "unauthorized" in error_msg.lower() or "forbidden" in error_msg.lower():
            display_msg = "Directory/Organization read permission required.\nPlease grant the 'Organization.Read.All' or 'Directory.Read.All' permission to your App Registration in Entra ID."

        ctk.CTkLabel(self.state_frame, text=f"✖ {display_msg}", text_color=COLOR_ERROR, font=FONT_BODY_MEDIUM, justify="center").pack(pady=(20, 10))
        ctk.CTkButton(self.state_frame, text="Try Again", command=self._retry_fetch, width=120, fg_color="transparent", border_width=1, text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER).pack(pady=(0, 20))
        self.state_frame.pack(fill="x", expand=True)

    def _retry_fetch(self):
        if hasattr(self, 'reload_btn') and self.reload_btn.winfo_exists():
            self.reload_btn.configure(state="disabled")
        tenant, clients, secrets = self.get_credentials()
        if tenant:
            self.trigger_fetch(tenant, clients, secrets)

    def trigger_fetch(self, tenant, clients, secrets):
        """Triggers SKU fetch inside background thread."""
        usage_logger.info("SKU fetch trigger_fetch called. Spawning background worker thread...")
        self.status = "loading"
        self.on_status_change()
        
        self.pack(fill="x", expand=True, pady=10)
        self.grid_frame.pack_forget()
        
        self._set_state_loading("Fetching subscribed SKUs...")
        
        retries_val = self.retries.get() if self.retries else 5
        backoff_val = self.backoff.get() if self.backoff else 2
        
        threading.Thread(
            target=self._execute_worker,
            args=(tenant, clients, secrets, retries_val, backoff_val),
            daemon=True
        ).start()

    def _execute_worker(self, tenant: str, clients: List[str], secrets: List[str], retries_val: int, backoff_val: int):
        usage_logger.info("Executing thread: _execute_sku_worker")
        if self.semaphore:
            self.semaphore.acquire()
        try:
            client_id = clients[0]
            client_secret = secrets[0]
            
            self.log_msg(f"Authenticating app {client_id[:5]}...")
            
            client = GraphClient(
                tenant_id=tenant,
                client_ids=client_id,
                client_secrets=client_secret,
                concurrency=1,
                retries=retries_val,
                backoff=backoff_val
            )
            
            required_scopes = ["Organization.Read.All", "Directory.Read.All"]
            client.authenticate(required_scopes=required_scopes)
            
            self.log_msg("Querying Graph API endpoint for SKUs...")
            dir_service = DirectoryService(client)
            sku_data = dir_service.get_subscribed_skus()
            client.close()
            
            usage_logger.info("Successfully fetched SKU data.")
            self.after(0, self._render_success, sku_data)
        except Exception as e:
            usage_logger.error("Exception caught in SubscribedSKUs worker.", exc_info=True)
            self.after(0, self._render_error, str(e))
        finally:
            if self.semaphore:
                self.semaphore.release()

    def _render_success(self, sku_dict: Dict[str, Any]):
        usage_logger.info("Executing UI render for SKU table.")
        if hasattr(self, 'reload_btn') and self.reload_btn.winfo_exists():
            self.reload_btn.configure(state="normal")
        self.state_frame.pack_forget()
        for w in self.grid_frame.winfo_children():
            w.destroy()

        self.grid_frame.pack(fill="x", expand=True)

        self.grid_frame.grid_columnconfigure(0, weight=2)
        self.grid_frame.grid_columnconfigure(1, weight=1)
        self.grid_frame.grid_columnconfigure(2, weight=1)

        headers = ["SKU Part Number", "Units", "Consumed Units"]
        for col_idx, head_text in enumerate(headers):
            cell = ctk.CTkFrame(self.grid_frame, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        items = sku_dict.get("value", [])
        if not items:
            empty_cell = ctk.CTkFrame(self.grid_frame, fg_color="transparent")
            empty_cell.grid(row=1, column=0, columnspan=3, sticky="nsew", pady=15)
            ctk.CTkLabel(empty_cell, text="No subscribed product configurations found in scope.", text_color=COLOR_TEXT_SUB).pack()
        else:
            items.sort(key=lambda x: len(x.get("servicePlans", [])), reverse=True)
            self.last_licenses_items = items
            self.btn_export_lic.configure(state="normal")

            current_row = 1
            for item_idx, item in enumerate(items):
                bg_style = COLOR_SURFACE if item_idx % 2 == 0 else COLOR_SURFACE_VARIANT

                c0 = ctk.CTkFrame(self.grid_frame, fg_color=bg_style, corner_radius=0)
                c0.grid(row=current_row, column=0, sticky="nsew", padx=0, pady=(0, 1))
                ctk.CTkLabel(c0, text=item.get("skuPartNumber", "UNKNOWN_SKU"), font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=8, anchor="nw")

                c1 = ctk.CTkFrame(self.grid_frame, fg_color=bg_style, corner_radius=0)
                c1.grid(row=current_row, column=1, sticky="nsew", padx=0, pady=(0, 1))
                prepaid = item.get("prepaidUnits", {})
                p_str = f"Enabled: {prepaid.get('enabled', 0):,}"
                if prepaid.get('warning', 0) > 0: p_str += f"\nWarn: {prepaid.get('warning'):,}"
                if prepaid.get('suspended', 0) > 0: p_str += f"\nSusp: {prepaid.get('suspended'):,}"
                ctk.CTkLabel(c1, text=p_str, text_color=COLOR_TEXT_MAIN, justify="left").pack(padx=10, pady=8, anchor="nw")

                c2 = ctk.CTkFrame(self.grid_frame, fg_color=bg_style, corner_radius=0)
                c2.grid(row=current_row, column=2, sticky="nsew", padx=0, pady=(0, 1))
                ctk.CTkLabel(c2, text=f"{item.get('consumedUnits', 0):,}", text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=8, anchor="nw")

                current_row += 1

        self.status = "success"
        self.on_status_change()

    def _render_error(self, err_msg):
        usage_logger.warning(f"Rendering SKU table error state: {err_msg}")
        if hasattr(self, 'reload_btn') and self.reload_btn.winfo_exists():
            self.reload_btn.configure(state="normal")
        self._set_state_error(err_msg)
        self.btn_export_lic.configure(state="disabled")
        self.status = "error"
        self.on_status_change()

    def export_licenses_spreadsheet(self):
        """Exports the SKUs inventory to a CSV formatted to mimic merged cells."""
        usage_logger.info("Exporting licenses to local spreadsheet requested.")
        if not self.last_licenses_items:
            usage_logger.warning("Export aborted: No cached license items available to export.")
            return

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        f = filedialog.asksaveasfilename(
            initialfile=f"licenses_inventory_{ts}.csv",
            defaultextension=".csv",
            filetypes=[("CSV Spreadsheet", "*.csv")]
        )

        if not f:
            usage_logger.info("Export aborted by user (dialog cancelled).")
            return

        usage_logger.info(f"Target export path established: {f}")
        headers = ["SKU Part Number", "Units", "Consumed Units", "Included Service Plans", "Applies To"]
        rows = []

        for item in self.last_licenses_items:
            sku_name = item.get("skuPartNumber", "UNKNOWN_SKU")
            prepaid = item.get("prepaidUnits", {})
            enabled_units = prepaid.get("enabled", 0)
            warn_units = prepaid.get("warning", 0)
            susp_units = prepaid.get("suspended", 0)

            prepaid_str = f"Enabled: {enabled_units:,}"
            if warn_units > 0: prepaid_str += f"\nWarn: {warn_units:,}"
            if susp_units > 0: prepaid_str += f"\nSusp: {susp_units:,}"
            consumed_str = f"{item.get('consumedUnits', 0):,}"

            plans = item.get("servicePlans", [])

            if not plans:
                rows.append([sku_name, prepaid_str, consumed_str, "None designated.", "-"])
            else:
                for idx, p in enumerate(plans):
                    p_name = p.get("servicePlanName", "UnnamedPlan")
                    p_scope = p.get("appliesTo", "Unknown")
                    if idx == 0:
                        rows.append([sku_name, prepaid_str, consumed_str, p_name, p_scope])
                    else:
                        rows.append(["", "", "", p_name, p_scope])

        try:
            chunk_size = 1000
            for i in range(0, len(rows), chunk_size):
                chunk = rows[i:i + chunk_size]
                df = pd.DataFrame(chunk, columns=headers)
                df.to_csv(f, mode='a' if i > 0 else 'w', header=(i == 0), index=False, encoding='utf-8')
            usage_logger.info("Spreadsheet exported successfully.")
            messagebox.showinfo("Export Successful", f"Spreadsheet successfully saved to:\n{f}", parent=self)
        except Exception as e:
            usage_logger.error("Failed writing export spreadsheet to disk.", exc_info=True)
            messagebox.showerror("Export Error", f"Failed to save file:\n{e}", parent=self)
