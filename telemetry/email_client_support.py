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

"""Standalone Exchange Online Supported Email Clients telemetry scanner and UI presentation."""

import os
import logging
import threading
import pandas as pd
import customtkinter as ctk

from core.graph.client import GraphClient
from core.graph.reports import ReportsService
from telemetry.styles import *

usage_logger = logging.getLogger("M365TelemetryAsyncLogger")

def parse_email_client_support_csv(filepath: str) -> dict:
    """Parses the Email App Usage Counts CSV to categorize Browser vs Non-Browser client adoption."""
    usage_logger.info(f"Processing Email App Usage Counts file: {os.path.basename(filepath)}")
    if not os.path.exists(filepath):
        usage_logger.warning(f"Email App Usage report {filepath} not found. Skipping client breakdown.")
        return {}

    try:
        df = pd.read_csv(filepath)
        for col in df.columns:
            if col not in ["Report Refresh Date", "User Principal Name", "Display Name", "Last Activity Date", "Is Deleted"]:
                df[col] = df[col].astype(str).str.strip().str.upper().isin(["TRUE", "1", "UNDETERMINED"])
                
        if "Is Deleted" in df.columns:
            df = df[~df["Is Deleted"].astype(str).str.strip().str.upper().isin(["TRUE", "1"])]

        if df.empty:
            usage_logger.warning(f"Email App Usage report {filepath} is empty.")
            return {}

        owa_users = int(df["Outlook For Web"].sum()) if "Outlook For Web" in df.columns else 0

        win_users = int(df["Outlook For Windows"].sum()) if "Outlook For Windows" in df.columns else 0
        mac_users = int(df["Outlook For Mac"].sum()) if "Outlook For Mac" in df.columns else 0
        mail_mac = int(df["Mail For Mac"].sum()) if "Mail For Mac" in df.columns else 0
        
        mobile_users = int(df["Outlook For Mobile"].sum()) if "Outlook For Mobile" in df.columns else 0
        other_mobile = int(df["Other For Mobile"].sum()) if "Other For Mobile" in df.columns else 0
        
        pop3_users = int(df["POP3 App"].sum()) if "POP3 App" in df.columns else 0
        imap4_users = int(df["IMAP4 App"].sum()) if "IMAP4 App" in df.columns else 0
        smtp_users = int(df["SMTP App"].sum()) if "SMTP App" in df.columns else 0

        total_desktop = win_users + mac_users + mail_mac
        total_mobile = mobile_users + other_mobile
        total_protocols = pop3_users + imap4_users + smtp_users
        total_non_browser = total_desktop + total_mobile + total_protocols

        return {
            "browser_users": owa_users,
            "desktop_win": win_users,
            "desktop_mac": mac_users,
            "desktop_mail_mac": mail_mac,
            "mobile_outlook": mobile_users,
            "mobile_other": other_mobile,
            "protocol_pop3": pop3_users,
            "protocol_imap4": imap4_users,
            "protocol_smtp": smtp_users,
            "total_desktop": total_desktop,
            "total_mobile": total_mobile,
            "total_protocols": total_protocols,
            "total_non_browser": total_non_browser
        }
    except Exception as e:
        usage_logger.error(f"Error parsing Email App Usage CSV: {e}")
        return {}

def run_email_client_pipeline(client_id: str, client_secret: str, tenant_id: str) -> dict:
    """Pipeline specifically downloading getEmailAppUsageAppsUserCounts reports and discovering PSTs."""
    usage_logger.info("Starting Email Environment & PST Discovery Pipeline...")
    client = GraphClient(
        tenant_id=tenant_id,
        client_ids=client_id,
        client_secrets=client_secret,
        concurrency=1,
        retries=3,
        backoff=2
    )
    client.authenticate()
    service = ReportsService(client)

    script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
    reports_dir = os.path.join(script_dir, "reports", f"{tenant_id}_{client_id}")
    
    client_stats = {}
    client_error = None
    try:
        service.download_email_app_usage_detail(reports_dir)
        client_stats = parse_email_client_support_csv(os.path.join(reports_dir, "EmailAppUsageUserDetail(180d).csv"))
    except Exception as e:
        usage_logger.warning(f"Could not retrieve/parse Email App Usage report: {e}")
        client_error = str(e)

    pst_cloud = {}
    pst_error = None
    try:
        pst_cloud = service.search_cloud_pst_files()
    except Exception as e:
        pst_error = str(e)

    client.close()
    return {
        "client_adoption": client_stats,
        "client_error": client_error,
        "pst_cloud_data": pst_cloud,
        "pst_error": pst_error
    }


class EmailClientSupportFrame(ctk.CTkFrame):
    def update_loading_text(self, text_msg):
        if hasattr(self, 'loading_label') and self.loading_label.winfo_exists():
            self.loading_label.configure(text=f"⏳ {text_msg}")
    """Self-contained CustomTkinter component rendering Email Environment UI."""

    def __init__(self, master, log_callback, credentials_callback, status_change_callback, **kwargs):
        self.semaphore = kwargs.pop("concurrency_semaphore", None)
        super().__init__(master, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=12, **kwargs)
        self.log_msg = log_callback
        self.get_credentials = credentials_callback
        self.on_status_change = status_change_callback
        self.status = None

        self.build_ui()

    def build_ui(self):
        self.pack(fill="x", expand=True, pady=10)
        
        self.inner_pad = ctk.CTkFrame(self, fg_color="transparent")
        self.inner_pad.pack(fill="both", expand=True, padx=20, pady=20)

        self.sub_title_1 = ctk.CTkLabel(self.inner_pad, text="Email Client Classification", font=FONT_HEADER_SMALL, text_color=COLOR_TEXT_MAIN)
        self.state_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.grid_frame = ctk.CTkFrame(self.inner_pad, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)

        self.sub_title_2 = ctk.CTkLabel(self.inner_pad, text="PST Files", font=FONT_HEADER_SMALL, text_color=COLOR_TEXT_MAIN)
        self.pst_grid_frame = ctk.CTkFrame(self.inner_pad, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)

        self.reset_view()

    def reset_view(self):
        self.pack_forget()
        self.sub_title_1.pack_forget()
        self.state_frame.pack_forget()
        self.grid_frame.pack_forget()
        self.sub_title_2.pack_forget()
        self.pst_grid_frame.pack_forget()
        if hasattr(self, "pst_disclaimer_lbl") and self.pst_disclaimer_lbl:
            self.pst_disclaimer_lbl.pack_forget()
            self.pst_disclaimer_lbl.destroy()
            self.pst_disclaimer_lbl = None
        
        for w in self.state_frame.winfo_children():
            w.destroy()
        for w in self.grid_frame.winfo_children():
            w.destroy()
        for w in self.pst_grid_frame.winfo_children():
            w.destroy()

    def _set_state_loading(self):
        self.state_frame.pack_forget()
        
        self.sub_title_1.pack(anchor="w", pady=(5, 10))
        self.grid_frame.pack(fill="x", expand=True)
        for w in self.grid_frame.winfo_children():
            w.destroy()
        f1 = ctk.CTkFrame(self.grid_frame, fg_color="transparent")
        ctk.CTkLabel(f1, text="⏳ Analyzing Email Clients...", text_color=COLOR_TEXT_SUB, font=FONT_BODY_MEDIUM).pack(pady=(20, 5))
        pb1 = ctk.CTkProgressBar(f1, mode="indeterminate", width=250, fg_color=COLOR_SURFACE_VARIANT, progress_color=COLOR_PRIMARY)
        pb1.pack(pady=(0, 20))
        pb1.start()
        f1.pack(fill="x", expand=True)

        self.sub_title_2.pack(anchor="w", pady=(20, 5))
        self.pst_grid_frame.pack(fill="x", expand=True)
        for w in self.pst_grid_frame.winfo_children():
            w.destroy()
        f2 = ctk.CTkFrame(self.pst_grid_frame, fg_color="transparent")
        ctk.CTkLabel(f2, text="⏳ Discovering PST Files...", text_color=COLOR_TEXT_SUB, font=FONT_BODY_MEDIUM).pack(pady=(20, 5))
        pb2 = ctk.CTkProgressBar(f2, mode="indeterminate", width=250, fg_color=COLOR_SURFACE_VARIANT, progress_color=COLOR_PRIMARY)
        pb2.pack(pady=(0, 20))
        pb2.start()
        f2.pack(fill="x", expand=True)

    def _set_state_error(self, error_msg):
        self.sub_title_1.pack_forget()
        self.grid_frame.pack_forget()
        self.sub_title_2.pack_forget()
        self.pst_grid_frame.pack_forget()
        
        for w in self.state_frame.winfo_children():
            w.destroy()
        display_msg = error_msg
        if "401" in error_msg or "403" in error_msg or "unauthorized" in error_msg.lower():
            display_msg = "Reports / Search telemetry permission required.\nPlease grant 'Reports.Read.All' and 'Files.Read.All' in Microsoft Entra ID."

        ctk.CTkLabel(self.state_frame, text=f"✖ {display_msg}", text_color=COLOR_ERROR, font=FONT_BODY_MEDIUM, justify="center").pack(pady=(20, 5))
        self.state_frame.pack(fill="x", expand=True)

    def trigger_fetch(self, tenant, client_id, client_secret):
        self.status = "loading"
        self.on_status_change()

        self.pack(fill="x", expand=True, pady=10)
        self._set_state_loading()

        threading.Thread(
            target=self._execute_worker,
            args=(tenant, client_id, client_secret),
            daemon=True
        ).start()

    def _execute_worker(self, tenant: str, client_id: str, client_secret: str):
        if self.semaphore:
            self.semaphore.acquire()
        try:
            data = run_email_client_pipeline(client_id, client_secret, tenant)
            self.after(0, self._render_success, data)
        except Exception as e:
            self.after(0, self._render_error, str(e))
        finally:
            if self.semaphore:
                self.semaphore.release()

    def _render_success(self, data: dict):
        self.state_frame.pack_forget()
        for w in self.grid_frame.winfo_children():
            w.destroy()
        for w in self.pst_grid_frame.winfo_children():
            w.destroy()

        # Render Supported Email Clients
        self.sub_title_1.pack(anchor="w", pady=(5, 10))
        self.grid_frame.pack(fill="x", expand=True)
        for i in range(2):
            self.grid_frame.grid_columnconfigure(i, weight=1)

        headers_client = ["Email Client Classification", "Active User Counts (180-Day Telemetry)"]
        for col_idx, head_text in enumerate(headers_client):
            cell = ctk.CTkFrame(self.grid_frame, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=1, pady=1)
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        client_err = data.get("client_error")
        if client_err:
            d_str = f"✖ Error: {client_err}"
            m_str = f"✖ Error: {client_err}"
            p_str = f"✖ Error: {client_err}"
            rows_client = [
                ("Supported Browser-Based Clients", f"✖ Error: {client_err}"),
                ("Supported Non-Browser (Desktop)", d_str),
                ("Supported Non-Browser (Mobile)", m_str),
                ("Supported Non-Browser (Protocols)", p_str)
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
            
            cc0 = ctk.CTkFrame(self.grid_frame, fg_color=bg_c, corner_radius=0)
            cc0.grid(row=cr_idx, column=0, sticky="nsew", padx=1, pady=1)
            ctk.CTkLabel(cc0, text=c_name, font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=10, anchor="nw")

            cc1 = ctk.CTkFrame(self.grid_frame, fg_color=bg_c, corner_radius=0)
            cc1.grid(row=cr_idx, column=1, sticky="nsew", padx=1, pady=1)
            ctk.CTkLabel(cc1, text=c_val, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="left").pack(padx=10, pady=10, anchor="nw")

        # 3. Subsection 3: PST Files
        self.sub_title_2.pack(anchor="w", pady=(25, 10))
        self.pst_grid_frame.pack(fill="x", expand=True)
        self.pst_grid_frame.grid_columnconfigure(0, weight=2)
        self.pst_grid_frame.grid_columnconfigure(1, weight=5)

        headers_pst = ["PST Storage Location", "Discovered File Count & Size"]
        for col_idx, head_text in enumerate(headers_pst):
            cell = ctk.CTkFrame(self.pst_grid_frame, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=1, pady=1)
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        pst_err = data.get("pst_error")
        if pst_err:
            cloud_str = f"✖ Error: {pst_err}"
        else:
            pst_cloud = data.get("pst_cloud_data", {})
            cloud_count = 0
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

        # Show disclaimer if any cloud PST files are found
        if not pst_err and cloud_count > 0:
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

    def _render_error(self, err_msg):
        self._set_state_error(err_msg)
        self.status = "error"
        self.on_status_change()

