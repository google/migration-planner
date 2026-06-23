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

"""UI frame for Exchange Online Supported Email Clients telemetry."""

import os
import csv
import logging
import threading
import customtkinter as ctk

from core.graph.exchange.email_clients import run_email_client_usage_pipeline
from telemetry.styles import *

usage_logger = logging.getLogger("M365TelemetryAsyncLogger.EmailClientSupportUI")

class EmailClientSupportFrame(ctk.CTkFrame):
    """Self-contained component wrapping Exchange Online Supported Email Clients UI."""

    def __init__(self, master, log_callback, credentials_callback, status_change_callback, **kwargs):
        self.semaphore = kwargs.pop("concurrency_semaphore", None)
        super().__init__(master, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=12, **kwargs)
        self.log_msg = log_callback
        self.get_credentials = credentials_callback
        self.on_status_change = status_change_callback
        
        self.status = None
        self._cached_client_data = {}

        self.build_ui()

    def build_ui(self):
        self.pack(fill="x", expand=True, pady=10)

        self.inner_pad = ctk.CTkFrame(self, fg_color="transparent")
        self.inner_pad.pack(fill="both", expand=True, padx=20, pady=20)

        self.client_header_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.client_header_frame.pack(fill="x", pady=(0, 10))
        ctk.CTkLabel(self.client_header_frame, text="Email Client Classification", font=FONT_HEADER_SMALL, text_color=COLOR_TEXT_MAIN).pack(side="left")
        self.client_reload_btn = ctk.CTkButton(
            self.client_header_frame, 
            state="disabled", text="↻ Reload", width=80, height=24,
            font=ctk.CTkFont(family="Segoe UI", size=12),
            fg_color="transparent", border_width=1, text_color="#2563EB", hover_color="#DBEAFE",
            command=self._retry_client_fetch
        )
        self.client_reload_btn.pack(side="right")
        
        self.client_grid_frame = ctk.CTkFrame(self.inner_pad, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)
        self.client_grid_frame.pack(fill="x", expand=True)

        self.reset_view()

    def reset_view(self):
        self.pack_forget()
        self.status = None
        self._cached_client_data = {}
        for w in self.client_grid_frame.winfo_children(): w.destroy()

    def _retry_client_fetch(self):
        tenant, clients, secrets = self.get_credentials()
        if tenant and clients and secrets:
            self.client_reload_btn.configure(state="disabled")
            self.trigger_fetch(tenant, clients[0], secrets[0])

    def trigger_fetch(self, tenant, client_id, client_secret):
        self.pack(fill="x", expand=True, pady=10)
        self.status = "loading"
        self.on_status_change()
        
        for w in self.client_grid_frame.winfo_children(): w.destroy()
        f = ctk.CTkFrame(self.client_grid_frame, fg_color="transparent")
        ctk.CTkLabel(f, text="⏳ Analyzing Email Clients...", text_color=COLOR_TEXT_SUB, font=FONT_BODY_MEDIUM).pack(pady=(20, 5))
        pb = ctk.CTkProgressBar(f, mode="indeterminate", width=250, fg_color=COLOR_SURFACE_VARIANT, progress_color=COLOR_PRIMARY)
        pb.pack(pady=(0, 20))
        pb.start()
        f.pack(fill="x", expand=True)
        
        threading.Thread(target=self._execute_client_worker, args=(tenant, client_id, client_secret), daemon=True).start()

    def _execute_client_worker(self, tenant, client_id, client_secret):
        if self.semaphore: self.semaphore.acquire()
        try:
            data = run_email_client_usage_pipeline(client_id, client_secret, tenant)
            if not data.get("client_error"):
                script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
                if os.path.basename(script_dir) == "exchange":
                    script_dir = os.path.dirname(script_dir)
                reports_dir = os.path.join(script_dir, "reports", f"{tenant}_{client_id}")
                os.makedirs(reports_dir, exist_ok=True)
                csv_path = os.path.join(reports_dir, "email_client_support_metrics.csv")
                
                with open(csv_path, 'w', encoding='utf-8', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(["Client Environment", "Active User Count"])
                    adop = data.get("client_adoption", {})
                    writer.writerow(["Browser Users (Web)", adop.get("browser_users", 0)])
                    writer.writerow(["Desktop Windows (Outlook)", adop.get("desktop_win", 0)])
                    writer.writerow(["Desktop Mac (Outlook)", adop.get("desktop_mac", 0)])
                    writer.writerow(["Desktop Mac (Mail)", adop.get("desktop_mail_mac", 0)])
                    writer.writerow(["Mobile Outlook", adop.get("mobile_outlook", 0)])
                    writer.writerow(["Mobile Native (Exchange ActiveSync)", adop.get("mobile_native", 0)])
                    writer.writerow(["IMAP Users", adop.get("imap_users", 0)])
                    writer.writerow(["POP Users", adop.get("pop_users", 0)])
                    writer.writerow(["SMTP Users", adop.get("smtp_users", 0)])

            self.after(0, self._render_client_success, data)
        except Exception as e:
            self.after(0, self._render_client_error, str(e))
        finally:
            if self.semaphore: self.semaphore.release()

    def _render_client_error(self, error_msg):
        self._cached_client_data = {"client_error": error_msg}
        self.client_reload_btn.configure(state="normal")
        for w in self.client_grid_frame.winfo_children(): w.destroy()
        f = ctk.CTkFrame(self.client_grid_frame, fg_color="transparent")
        display_msg = error_msg
        if "401" in error_msg or "403" in error_msg or "unauthorized" in error_msg.lower():
            display_msg = "Reports permission required. Please grant 'Reports.Read.All'."
        ctk.CTkLabel(f, text=f"✖ {display_msg}", text_color=COLOR_ERROR, font=FONT_BODY_MEDIUM, justify="center").pack(pady=(20, 5))
        ctk.CTkButton(f, text="Try Again", command=self._retry_client_fetch, width=120, fg_color="transparent", border_width=1, text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER).pack(pady=(0, 20))
        f.pack(fill="x", expand=True)
        self.status = "error"
        self.on_status_change()

    def _render_client_success(self, data: dict):
        self._cached_client_data = data
        self.client_reload_btn.configure(state="normal")
        for w in self.client_grid_frame.winfo_children(): w.destroy()
        
        for i in range(2):
            self.client_grid_frame.grid_columnconfigure(i, weight=1)

        headers_client = ["Email Client Classification", "Active User Counts (180-Day Telemetry)"]
        for col_idx, head_text in enumerate(headers_client):
            cell = ctk.CTkFrame(self.client_grid_frame, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=1, pady=1)
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        client_err = data.get("client_error")
        if client_err:
            rows_client = [
                ("Supported Browser-Based Clients", f"✖ Error: {client_err}"),
                ("Supported Non-Browser (Desktop)", f"✖ Error: {client_err}"),
                ("Supported Non-Browser (Mobile)", f"✖ Error: {client_err}"),
                ("Supported Non-Browser (Protocols)", f"✖ Error: {client_err}")
            ]
        else:
            c_adop = data.get("client_adoption", {})
            b_users = c_adop.get("browser_users", 0) if c_adop else 0
            d_win = c_adop.get("desktop_win", 0) if c_adop else 0
            d_mac = c_adop.get("desktop_mac", 0) if c_adop else 0
            m_mac = c_adop.get("desktop_mail_mac", 0) if c_adop else 0
            m_out = c_adop.get("mobile_outlook", 0) if c_adop else 0
            m_oth = c_adop.get("mobile_other", 0) if c_adop else 0
            p_imap = c_adop.get("protocol_imap4", 0) if c_adop else 0
            p_smtp = c_adop.get("protocol_smtp", 0) if c_adop else 0
            p_pop = c_adop.get("protocol_pop3", 0) if c_adop else 0

            d_str = (f"• Outlook for Windows: {d_win:,} Users\n"
                     f"• Outlook for Mac: {d_mac:,} Users\n"
                     f"• Apple Mail (macOS): {m_mac:,} Users")

            m_str = (f"• Outlook Mobile (iOS/Android): {m_out:,} Users\n"
                     f"• Native / Other Mobile Apps: {m_oth:,} Users")

            p_str = (f"• IMAP4 App: {p_imap:,} Users\n"
                     f"• POP3 App: {p_pop:,} Users\n"
                     f"• SMTP App: {p_smtp:,} Accounts")

            rows_client = [
                ("Supported Browser-Based Clients", f"• Outlook on the Web (OWA): {b_users:,} Users"),
                ("Supported Non-Browser (Desktop)", d_str),
                ("Supported Non-Browser (Mobile)", m_str),
                ("Supported Non-Browser (Protocols)", p_str)
            ]

        for cr_idx, (c_name, c_val) in enumerate(rows_client, start=1):
            bg_c = "transparent" if cr_idx % 2 != 0 else COLOR_SURFACE_VARIANT
            
            cc0 = ctk.CTkFrame(self.client_grid_frame, fg_color=bg_c, corner_radius=0)
            cc0.grid(row=cr_idx, column=0, sticky="nsew", padx=1, pady=1)
            ctk.CTkLabel(cc0, text=c_name, font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=10, anchor="nw")

            cc1 = ctk.CTkFrame(self.client_grid_frame, fg_color=bg_c, corner_radius=0)
            cc1.grid(row=cr_idx, column=1, sticky="nsew", padx=1, pady=1)
            ctk.CTkLabel(cc1, text=c_val, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="left").pack(padx=10, pady=10, anchor="nw")

        self.status = "success"
        self.on_status_change()

    def cancel(self):
        pass

    def _load_client_data_from_csv(self):
        tenant, clients, secrets = self.get_credentials()
        if not tenant or not clients:
            return {}
            
        script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
        if os.path.basename(script_dir) == "exchange":
            script_dir = os.path.dirname(script_dir)
        csv_path = os.path.join(script_dir, "reports", f"{tenant}_{clients[0]}", "email_client_support_metrics.csv")
        
        if not os.path.exists(csv_path):
            return {}
            
        adop = {}
        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader, None)  # skip header
                for row in reader:
                    if len(row) >= 2:
                        name, val = row[0], int(row[1])
                        if "Browser Users" in name: adop["browser_users"] = val
                        elif "Desktop Windows" in name: adop["desktop_win"] = val
                        elif "Desktop Mac (Outlook)" in name: adop["desktop_mac"] = val
                        elif "Desktop Mac (Mail)" in name: adop["desktop_mail_mac"] = val
                        elif "Mobile Outlook" in name: adop["mobile_outlook"] = val
                        elif "Mobile Native" in name: adop["mobile_other"] = val
                        elif "IMAP" in name: adop["protocol_imap4"] = val
                        elif "POP" in name: adop["protocol_pop3"] = val
                        elif "SMTP" in name: adop["protocol_smtp"] = val
            return {"client_adoption": adop, "client_error": None}
        except Exception as e:
            usage_logger.error(f"Error loading client data from CSV: {e}")
            return {"client_error": str(e)}

    @property
    def last_data(self):
        if hasattr(self, "_cached_client_data") and self._cached_client_data:
            return self._cached_client_data
        return self._load_client_data_from_csv()
