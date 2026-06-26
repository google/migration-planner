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

"""UI frame for Exchange Online Mail Security."""

import os
import csv
import logging
import threading
import concurrent.futures
import customtkinter as ctk

from core.graph.exchange.mail_security import run_mail_security_pipeline
from core.powershell.client import PowerShellClient
from core.powershell.encryption import get_encryption_policies
from telemetry.styles import *

usage_logger = logging.getLogger("M365TelemetryAsyncLogger.MailSecurityUI")

class MailSecurityFrame(ctk.CTkFrame):
    """Self-contained customtkinter component wrapping Exchange Online Mail Security UI."""

    def update_loading_text(self, text_msg):
        if hasattr(self, 'loading_label') and self.loading_label.winfo_exists():
            self.loading_label.configure(text=f"⏳ {text_msg}")

    def __init__(self, master, log_callback, credentials_callback, status_change_callback, **kwargs):
        self.semaphore = kwargs.pop("concurrency_semaphore", None)
        super().__init__(master, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=12, **kwargs)
        self.log_msg = log_callback
        self.get_credentials = credentials_callback
        self.on_status_change_cb = status_change_callback
        
        self.status = None
        self.loading = True
        self.error_msg = None
        
        self.total_eop_users = 0
        self.eop_skus = []
        
        self.total_defender_users = 0
        self.defender_skus = []
        
        self.encryption_data = {"m365_policies": [], "exchange_deps": [], "error": None}
        self.last_data = {}
        
        self.inner_pad = ctk.CTkFrame(self, fg_color="transparent")
        self.inner_pad.pack(fill="both", expand=True, padx=20, pady=20)
        
        self.header = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.header.pack(fill="x", pady=(0, 10))
        ctk.CTkLabel(self.header, text="Mail Security", font=FONT_HEADER_SMALL, text_color=COLOR_TEXT_MAIN).pack(side="left")
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
        
        self.grid_frame = ctk.CTkFrame(self.inner_pad, fg_color=COLOR_SURFACE, corner_radius=12, border_width=1, border_color=COLOR_OUTLINE_LIGHT)
        self.grid_frame.pack(fill="x", expand=True, pady=(0, 10))
        
        self.loading_label = None
        self.progress = None
        self.render_ui_state()
 
    def trigger_fetch(self, tenant, client_id, client_secret):
        self.status = "loading"
        self.loading = True
        self.render_ui_state()
        threading.Thread(target=self._fetch_data, args=(tenant, client_id, client_secret), daemon=True).start()
 
    def _fetch_data(self, tenant, c_id, c_secret):
        if self.semaphore:
            self.semaphore.acquire()
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                skus_future = executor.submit(run_mail_security_pipeline, c_id, c_secret, tenant)
                
                # Fetch encryption policies
                def fetch_encryption():
                    try:
                        ps_client = PowerShellClient(tenant, c_id, c_secret)
                        return get_encryption_policies(ps_client)
                    except Exception as e:
                        usage_logger.warning(f"Failed to fetch encryption policies: {e}")
                        return {"m365_policies": [], "exchange_deps": [], "error": str(e)}
                
                enc_future = executor.submit(fetch_encryption)
                
                result_data = skus_future.result()
                self.encryption_data = enc_future.result()
            
            self.defender_skus = result_data["defender"]["skus"]
            self.total_defender_users = result_data["defender"]["users"]
            
            self.eop_skus = result_data["eop"]["skus"]
            self.total_eop_users = result_data["eop"]["users"]
            
            script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
            if os.path.basename(script_dir) == "exchange":
                script_dir = os.path.dirname(script_dir)
            reports_dir = os.path.join(script_dir, "reports", f"{tenant}_{c_id}")
            os.makedirs(reports_dir, exist_ok=True)
            csv_path = os.path.join(reports_dir, "mail_security_licensing.csv")
            
            with open(csv_path, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["Protection Type", "Associated SKUs", "Covered User Count"])
                writer.writerow(["Defender for Office 365", " | ".join(self.defender_skus) if self.defender_skus else "None", self.total_defender_users])
                writer.writerow(["Exchange Online Protection (EOP)", " | ".join(self.eop_skus) if self.eop_skus else "None", self.total_eop_users])
            
            usage_logger.info(f"Successfully streamed Mail Security data to {csv_path}")
            self.after(0, self._render_success, result_data)
        except Exception as e:
            usage_logger.error(f"Error fetching mail security SKUs: {e}", exc_info=True)
            self.after(0, self._render_error, str(e))
        finally:
            if self.semaphore:
                self.semaphore.release()

    def _render_success(self, data):
        self.last_data = data
        self.status = "success"
        self.loading = False
        self.on_status_change()

    def _render_error(self, err_msg):
        self._set_state_error(err_msg)

    def _set_state_error(self, error_msg):
        self.error_msg = error_msg
        self.status = "error"
        self.loading = False
        self.on_status_change()

    def reset_view(self):
        self.status = None
        self.error_msg = None

    def _retry_fetch(self):
        tenant, clients, secrets = self.get_credentials()
        if tenant and clients and secrets:
            if hasattr(self, 'reload_btn') and self.reload_btn.winfo_exists():
                self.reload_btn.configure(state="disabled")
            self.trigger_fetch(tenant, clients[0], secrets[0])

    def cancel(self):
        self.status = None
        self.loading = False
        self.error_msg = None
        self.reset_view()

    def on_status_change(self):
        self.render_ui_state()
        if hasattr(self, "on_status_change_cb") and self.on_status_change_cb:
            self.on_status_change_cb()

    def render_ui_state(self):
        for widget in self.grid_frame.winfo_children():
            widget.destroy()
            
        if not self.loading and hasattr(self, 'reload_btn') and self.reload_btn.winfo_exists():
            self.reload_btn.configure(state="normal")
            
        if self.loading:
            self.loading_label = ctk.CTkLabel(self.grid_frame, text="⏳ Loading Mail Security Data...", text_color="#6b7280", font=ctk.CTkFont(family="Segoe UI", size=13))
            self.loading_label.pack(pady=(20, 5))
            self.progress = ctk.CTkProgressBar(self.grid_frame, mode="indeterminate", width=250, fg_color=COLOR_SURFACE_VARIANT, progress_color=COLOR_PRIMARY)
            self.progress.pack(pady=(0, 20))
            self.progress.start()
            return
        if self.error_msg:
            f = ctk.CTkFrame(self.grid_frame, fg_color="transparent")
            ctk.CTkLabel(f, text=f"✖ {self.error_msg}", text_color=COLOR_ERROR, font=FONT_BODY_MEDIUM, justify="center").pack(pady=(20, 5))
            ctk.CTkButton(f, text="Try Again", command=self._retry_fetch, width=120, fg_color="transparent", border_width=1, text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER).pack(pady=(0, 20))
            f.pack(fill="x", expand=True)
            return
            
        metrics_grid = ctk.CTkFrame(self.grid_frame, fg_color=COLOR_SURFACE, corner_radius=0)
        metrics_grid.pack(fill="x", padx=10, pady=(5, 5))
        
        headers = ["Mail Security Configuration", "Detected SKUs", "Affected Users"]
        for i in range(3):
            metrics_grid.grid_columnconfigure(i, weight=1 if i == 2 else 2)
            
        for col_idx, head_text in enumerate(headers):
            cell = ctk.CTkFrame(metrics_grid, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")
            
        rows_data = []
        if self.defender_skus:
            rows_data.append(("Microsoft Defender for Office 365", ", ".join(self.defender_skus), str(self.total_defender_users)))
        if self.eop_skus:
            rows_data.append(("Exchange Online Protection (Baseline)", ", ".join(self.eop_skus), str(self.total_eop_users)))
            
        if not rows_data:
            c = ctk.CTkFrame(metrics_grid, fg_color=COLOR_SURFACE, corner_radius=0)
            c.grid(row=1, column=0, columnspan=3, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c, text="No Mail Security SKUs detected.", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="center").pack(padx=10, pady=12)
        else:
            for r_idx, vals in enumerate(rows_data, start=1):
                bg_style = COLOR_SURFACE if r_idx % 2 != 0 else COLOR_SURFACE_VARIANT
                for c_idx, val in enumerate(vals):
                    c = ctk.CTkFrame(metrics_grid, fg_color=bg_style, corner_radius=0)
                    c.grid(row=r_idx, column=c_idx, sticky="nsew", padx=0, pady=(0, 1))
                    ctk.CTkLabel(c, text=val, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="left", wraplength=450).pack(padx=10, pady=12, anchor="nw")
                    
        # --- ENCRYPTION UI ---
        ctk.CTkLabel(self.grid_frame, text="Data at Rest Encryption", font=FONT_HEADER_SMALL, text_color=COLOR_TEXT_MAIN).pack(fill="x", padx=15, pady=(20, 5))
        
        enc_grid = ctk.CTkFrame(self.grid_frame, fg_color=COLOR_SURFACE, corner_radius=0)
        enc_grid.pack(fill="x", padx=10, pady=(0, 10))
        
        enc_headers = ["Encryption Posture", "Tenant-Level Policies (M365)", "Mailbox Policies (Exchange DEPs)"]
        for i in range(3):
            enc_grid.grid_columnconfigure(i, weight=1)
            
        for col_idx, head_text in enumerate(enc_headers):
            cell = ctk.CTkFrame(enc_grid, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        if self.encryption_data.get("error"):
            # Error fetching policies (e.g., missing permissions or script error)
            c = ctk.CTkFrame(enc_grid, fg_color=COLOR_SURFACE, corner_radius=0)
            c.grid(row=1, column=0, columnspan=3, sticky="nsew", padx=0, pady=(0, 1))
            err_lbl = f"Failed to fetch encryption policies: {self.encryption_data['error']}\nNote: Requires Exchange Online PowerShell certificate auth."
            ctk.CTkLabel(c, text=err_lbl, font=FONT_BODY_MEDIUM, text_color=COLOR_ERROR, justify="center").pack(padx=10, pady=12)
        else:
            m365_pols = self.encryption_data.get("m365_policies", [])
            exc_deps = self.encryption_data.get("exchange_deps", [])
            
            posture = "Customer Key (Customer-Managed)" if (m365_pols or exc_deps) else "Microsoft-Managed Keys (Default)"
            
            m365_text = "\n".join([p["Name"] for p in m365_pols]) if m365_pols else "None detected"
            exc_text = "\n".join([p["Name"] for p in exc_deps]) if exc_deps else "None detected"
            
            bg_style = COLOR_SURFACE
            vals = [posture, m365_text, exc_text]
            for c_idx, val in enumerate(vals):
                c = ctk.CTkFrame(enc_grid, fg_color=bg_style, corner_radius=0)
                c.grid(row=1, column=c_idx, sticky="nsew", padx=0, pady=(0, 1))
                ctk.CTkLabel(c, text=val, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="left", wraplength=450).pack(padx=10, pady=12, anchor="nw")

        # --- DISCLAIMER ---
        disclaimer = ctk.CTkLabel(self.grid_frame, text="Note: Users can track inbound connectors displayed below to identify 3rd-party security apps.", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB, anchor="w")
        disclaimer.pack(fill="x", padx=15, pady=(5, 15))
