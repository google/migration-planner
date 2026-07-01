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

"""UI component for Service Principals SSO Configurations telemetry."""

import os
import time
import logging
import threading
import csv
import shutil
from datetime import datetime
import customtkinter as ctk
from tkinter import filedialog, messagebox

from core.graph.security.service_principals_sso import run_service_principals_sso_pipeline
from telemetry.styles import *

usage_logger = logging.getLogger("M365TelemetryAsyncLogger.SSOUI")

class ServicePrincipalsSsoSubFrame(ctk.CTkFrame):
    """Sub-frame for Service Principals SSO modes."""

    def __init__(self, master, log_callback, credentials_callback, status_change_callback, semaphore=None, **kwargs):
        super().__init__(master, fg_color="transparent", **kwargs)
        self.log_msg = log_callback
        self.get_credentials = credentials_callback
        self.on_status_change = status_change_callback
        self.semaphore = semaphore
        self.status = None
        self.csv_path = None
        self.is_cancelled = False

        self.build_ui()

    def build_ui(self):
        self.header_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.header_frame.pack(fill="x", pady=(0, 10))
        
        ctk.CTkLabel(self.header_frame, text="Service Principals Single Sign-On (SSO) Modes", font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN).pack(side="left")
        
        self.lbl_link = ctk.CTkLabel(
            self.header_frame, text="Open Enterprise Applications ↗",
            font=FONT_BODY_BOLD, text_color=COLOR_PRIMARY, cursor="hand2"
        )
        self.lbl_link.pack(side="left", anchor="w", padx=(15, 0))
        self.lbl_link.bind("<Button-1>", lambda e: __import__("webbrowser").open("https://entra.microsoft.com/#view/Microsoft_AAD_IAM/StartboardApplicationsMenuBlade/~/AppAppsPreview"))
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
            self.header_frame, text="Export SSO Data", width=150, height=32, corner_radius=16,
            font=FONT_BODY_BOLD, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER,
            command=self._export, state="disabled"
        )
        self.btn_export.pack(side="right")

        self.state_frame = ctk.CTkFrame(self, fg_color="transparent")
        
        self.grid_frame = ctk.CTkFrame(self, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)
        self.grid_frame.grid_columnconfigure(0, weight=1)
        self.grid_frame.grid_columnconfigure(1, weight=1)
        
        headers = ["SSO Mode", "Application Count"]
        for col_idx, head_text in enumerate(headers):
            cell = ctk.CTkFrame(self.grid_frame, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=1, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        self.reset_view()

    def reset_view(self):
        self.status = None
        self.is_cancelled = False
        self.csv_path = None
        self.state_frame.pack_forget()
        self.grid_frame.pack_forget()
        self.btn_export.configure(state="disabled")
        for w in self.state_frame.winfo_children(): w.destroy()
        for w in self.grid_frame.winfo_children():
            info = w.grid_info()
            if "row" in info and int(info["row"]) > 1: w.destroy()

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
        self.btn_reload.configure(state="disabled")
        self.btn_export.configure(state="disabled")
        self.grid_frame.pack_forget()
        
        script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
        if os.path.basename(script_dir) == "security":
            script_dir = os.path.dirname(script_dir)
        reports_dir = os.path.join(script_dir, "reports", f"{tenant}_{client_id}")
        self.csv_path = os.path.join(reports_dir, "service_principals_sso.csv")

        # Ensure directory exists and headers are initialized
        os.makedirs(reports_dir, exist_ok=True)
        with open(self.csv_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["appDisplayName", "preferredSingleSignOnMode"])

        self._set_loading_state("Scanning Service Principals SSO modes...")
        self.on_status_change()
            
        threading.Thread(target=self._execute_worker, args=(tenant, client_id, client_secret), daemon=True).start()

    def _execute_worker(self, tenant, client_id, client_secret):
        if self.semaphore: self.semaphore.acquire()
        try:
            run_service_principals_sso_pipeline(
                client_id=client_id,
                client_secret=client_secret,
                tenant_id=tenant,
                csv_path=self.csv_path,
                is_cancelled_callback=lambda: self.is_cancelled
            )
            self.status = "success"
            self.after(0, self._render_success)
        except Exception as e:
            usage_logger.error(f"Service Principals SSO fetch error: {e}", exc_info=True)
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
        if "401" in err_msg or "403" in err_msg or "permission" in err_msg.lower() or "unauthorized" in err_msg.lower():
            display_msg = "Service Principals telemetry permission required.\nPlease grant the 'Application.Read.All' application permission to your App Registration in Microsoft Entra ID."
        ctk.CTkLabel(self.state_frame, text=f"✖ {display_msg}", text_color=COLOR_ERROR, font=FONT_BODY_MEDIUM, justify="center", wraplength=700).pack(pady=(15, 5))
        ctk.CTkButton(self.state_frame, text="Try Again", command=self.trigger_fetch_individual, width=100, fg_color="transparent", border_width=1, text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER).pack(pady=(0, 15))

    def _update_grid(self):
        for w in self.grid_frame.winfo_children():
            info = w.grid_info()
            if "row" in info and int(info["row"]) > 1: w.destroy()

        saml = oidc = password = none_count = 0
        if self.csv_path and os.path.exists(self.csv_path):
            try:
                with open(self.csv_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        m = row.get("preferredSingleSignOnMode", "").lower()
                        if m == "saml": saml += 1
                        elif m == "oidc": oidc += 1
                        elif m == "password": password += 1
                        else: none_count += 1
            except Exception as e:
                usage_logger.error(f"Error reading SSO CSV: {e}")

        rows = [
            ("SAML", saml),
            ("OIDC", oidc),
            ("Password", password),
            ("Null / Not Supported", none_count)
        ]

        for idx, (mode_name, count) in enumerate(rows, start=2):
            bg_style = COLOR_SURFACE if idx % 2 != 0 else COLOR_SURFACE_VARIANT
            
            c0 = ctk.CTkFrame(self.grid_frame, fg_color=bg_style, corner_radius=0)
            c0.grid(row=idx, column=0, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c0, text=mode_name, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=12, anchor="w")

            c1 = ctk.CTkFrame(self.grid_frame, fg_color=bg_style, corner_radius=0)
            c1.grid(row=idx, column=1, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c1, text=str(count), font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=12, anchor="w")

    def _export(self):
        if not self.csv_path or not os.path.exists(self.csv_path):
            messagebox.showinfo("No Data", "There is no SSO data to export.", parent=self)
            return
            
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        f = filedialog.asksaveasfilename(
            initialfile=f"service_principals_sso_{ts}.csv",
            defaultextension=".csv",
            filetypes=[("CSV Files", "*.csv")],
            parent=self
        )
        if not f: return
        try:
            shutil.copyfile(self.csv_path, f)
            messagebox.showinfo("Export Successful", f"SSO exported to:\n{f}", parent=self)
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
