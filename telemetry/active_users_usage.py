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

"""Modular O365 Active Users usage telemetry scanners, aggregation pipelines, and visual interfaces."""

import os
import sys
import logging
import threading
import pandas as pd
from datetime import datetime, date
from typing import Any, List
import customtkinter as ctk

# Import unified core service layer
from core.graph.client import GraphClient
from core.graph.reports import ReportsService

# Safely import matplotlib to embed plots in Tkinter
try:
    from matplotlib.figure import Figure
    from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False

# Bind to the async logger initialized in m365_telemetry.py
usage_logger = logging.getLogger("M365TelemetryAsyncLogger")

# Import shared styles
from telemetry.styles import *


# =================================================================================
# PIPELINE LOGIC / PROCESSORS
# =================================================================================

def _get_reports_service(client_id, client_secret, tenant_id) -> tuple[GraphClient, ReportsService]:
    """Helper to instantiate GraphClient/ReportsService and manage credentials slots."""
    client = GraphClient(
        tenant_id=tenant_id,
        client_ids=client_id,
        client_secrets=client_secret,
        concurrency=2,
        retries=5,
        backoff=2
    )
    client.authenticate()
    return client, ReportsService(client)


def process_active_user_detail(filepath):
    """Streams the downloaded CSV and calculates usage counters over 30, 90, and 180 days."""
    usage_logger.info(f"Processing O365 file: {os.path.basename(filepath)}")
    
    if not os.path.exists(filepath):
        usage_logger.error(f"Error: Could not find the file {filepath} to process.")
        raise FileNotFoundError(f"Report file {os.path.basename(filepath)} not found. Download may have failed.")

    current_date = pd.Timestamp.today().normalize()

    headers = pd.read_csv(filepath, nrows=0).columns.tolist()
    expected = [
        "Has Exchange License", "Exchange Last Activity Date",
        "Has OneDrive License", "OneDrive Last Activity Date",
        "Has SharePoint License", "SharePoint Last Activity Date",
        "Has Teams License", "Teams Last Activity Date"
    ]
    cols = [c for c in expected if c in headers]

    exchange_online_usage = [0, 0, 0]
    onedrive_usage = [0, 0, 0]
    sharepoint_usage = [0, 0, 0]
    teams_usage = [0, 0, 0]

    def process_chunk_col(chunk, has_license_col, date_col):
        if has_license_col not in chunk.columns or date_col not in chunk.columns:
            return [0, 0, 0]
        mask = chunk[has_license_col].astype(str).str.strip().str.upper() == "TRUE"
        dates_series = pd.to_datetime(chunk.loc[mask, date_col], errors='coerce')
        days_diff = (current_date - dates_series).dt.days
        d180 = int((days_diff < 180).sum())
        d90 = int((days_diff < 90).sum())
        d30 = int((days_diff < 30).sum())
        return [d30, d90, d180]

    for chunk in pd.read_csv(filepath, usecols=cols, chunksize=10000, encoding="utf-8-sig"):
        e_chunk = process_chunk_col(chunk, "Has Exchange License", "Exchange Last Activity Date")
        exchange_online_usage = [x + y for x, y in zip(exchange_online_usage, e_chunk)]

        od_chunk = process_chunk_col(chunk, "Has OneDrive License", "OneDrive Last Activity Date")
        onedrive_usage = [x + y for x, y in zip(onedrive_usage, od_chunk)]

        sp_chunk = process_chunk_col(chunk, "Has SharePoint License", "SharePoint Last Activity Date")
        sharepoint_usage = [x + y for x, y in zip(sharepoint_usage, sp_chunk)]

        t_chunk = process_chunk_col(chunk, "Has Teams License", "Teams Last Activity Date")
        teams_usage = [x + y for x, y in zip(teams_usage, t_chunk)]

    usage_logger.info("Successfully processed O365 active user data in chunks.")
    return [
        ("Exchange Online", exchange_online_usage[0], exchange_online_usage[1], exchange_online_usage[2]),
        ("OneDrive", onedrive_usage[0], onedrive_usage[1], onedrive_usage[2]),
        ("SharePoint", sharepoint_usage[0], sharepoint_usage[1], sharepoint_usage[2]),
        ("Teams", teams_usage[0], teams_usage[1], teams_usage[2])
    ]


def process_active_user_counts(filepath):
    """Parses chronological usage data for plotting."""
    usage_logger.info(f"Processing O365 Counts file: {os.path.basename(filepath)}")
    if not os.path.exists(filepath):
        usage_logger.error(f"Error: Could not find the file {filepath} to process.")
        raise FileNotFoundError(f"Report file {os.path.basename(filepath)} not found.")

    headers = pd.read_csv(filepath, nrows=0).columns.tolist()
    expected = ["Report Date", "Office 365", "Exchange", "OneDrive", "SharePoint", "Teams"]
    cols = [c for c in expected if c in headers]
    dates = []
    office365 = []
    exchange = []
    onedrive = []
    sharepoint = []
    teams = []

    for chunk in pd.read_csv(filepath, usecols=cols, chunksize=10000, encoding="utf-8-sig"):
        if "Report Date" in chunk.columns:
            chunk = chunk.sort_values(by="Report Date").fillna(0)
            dates.extend(chunk["Report Date"].astype(str).tolist())
        else:
            dates.extend([""] * len(chunk))
        
        def extract_col(col_name):
            if col_name in chunk.columns:
                return pd.to_numeric(chunk[col_name], errors='coerce').fillna(0).astype(int).tolist()
            return [0] * len(chunk)

        office365.extend(extract_col("Office 365"))
        exchange.extend(extract_col("Exchange"))
        onedrive.extend(extract_col("OneDrive"))
        sharepoint.extend(extract_col("SharePoint"))
        teams.extend(extract_col("Teams"))

    usage_logger.info("Successfully processed O365 active user counts data.")
    return {
        "dates": dates,
        "office365": office365,
        "exchange": exchange,
        "onedrive": onedrive,
        "sharepoint": sharepoint,
        "teams": teams
    }


def process_m365_app_user_detail(filepath):
    """Streams the downloaded CSV and calculates usage counters."""
    usage_logger.info(f"Processing M365 App file: {os.path.basename(filepath)}")
    
    if not os.path.exists(filepath):
        usage_logger.error(f"Error: Could not find the file {filepath} to process.")
        raise FileNotFoundError(f"Report file {os.path.basename(filepath)} not found. Download may have failed.")

    columns_to_track = [
        "Windows", "Mac", "Mobile", "Web", "Outlook", "Word", "Excel", 
        "PowerPoint", "OneNote", "Teams", "Outlook (Windows)", "Word (Windows)", 
        "Excel (Windows)", "PowerPoint (Windows)", "OneNote (Windows)", 
        "Teams (Windows)", "Outlook (Mac)", "Word (Mac)", "Excel (Mac)", 
        "PowerPoint (Mac)", "OneNote (Mac)", "Teams (Mac)", "Outlook (Mobile)", 
        "Word (Mobile)", "Excel (Mobile)", "PowerPoint (Mobile)", 
        "OneNote (Mobile)", "Teams (Mobile)", "Outlook (Web)", "Word (Web)", 
        "Excel (Web)", "PowerPoint (Web)", "OneNote (Web)", "Teams (Web)"
    ]
    
    headers = pd.read_csv(filepath, nrows=0).columns.tolist()
    cols = [c for c in columns_to_track if c in headers]
    
    counters = {col: 0 for col in columns_to_track}

    for chunk in pd.read_csv(filepath, usecols=cols, chunksize=10000, encoding="utf-8-sig"):
        for col in columns_to_track:
            if col in chunk.columns:
                col_series = chunk[col].astype(str).str.strip().str.lower()
                count = int(col_series.isin(["yes", "true"]).sum())
                counters[col] += count
            
    usage_logger.info("Successfully processed M365 App user data in chunks.")
    return [(col, count) for col, count in counters.items()]


def run_o365_pipeline(client_id, client_secret, tenant_id):
    """Pipeline specifically for O365 Active User Data."""
    usage_logger.info("Starting isolated O365 Pipeline...")
    client, service = _get_reports_service(client_id, client_secret, tenant_id)
    
    script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
    reports_dir = os.path.join(script_dir, "reports", f"{tenant_id}_{client_id}")
    
    service.download_o365_active_user_detail(reports_dir)
    client.close()
    
    return process_active_user_detail(os.path.join(reports_dir, "Office365ActiveUserDetail(180d).csv"))


def run_o365_trend_pipeline(client_id, client_secret, tenant_id):
    """Pipeline specifically for O365 Trend Data."""
    try:
        usage_logger.info("Starting isolated O365 Trend Pipeline...")
        client, service = _get_reports_service(client_id, client_secret, tenant_id)
        
        script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
        reports_dir = os.path.join(script_dir, "reports", f"{tenant_id}_{client_id}")
        
        service.download_o365_active_user_counts(reports_dir)
        client.close()
        
        return process_active_user_counts(os.path.join(reports_dir, "Office365ActiveUserCounts(30d).csv"))
    except Exception as e:
        usage_logger.error("O365 Trend pipeline failed.", exc_info=True)
        raise


def run_m365_pipeline(client_id, client_secret, tenant_id):
    """Pipeline specifically for M365 Apps Data."""
    usage_logger.info("Starting isolated M365 Apps Pipeline...")
    client, service = _get_reports_service(client_id, client_secret, tenant_id)
    
    script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
    reports_dir = os.path.join(script_dir, "reports", f"{tenant_id}_{client_id}")
    
    service.download_m365_app_details(reports_dir)
    client.close()
    
    return process_m365_app_user_detail(os.path.join(reports_dir, "M365AppUserDetail(180d).csv"))


# =================================================================================
# MODULAR UI COMPONENTS
# =================================================================================

class ActiveUsersUsageFrame(ctk.CTkFrame):
    def update_loading_text(self, text_msg):
        if hasattr(self, 'loading_label') and self.loading_label.winfo_exists():
            self.loading_label.configure(text=f"⏳ {text_msg}")
    """Self-contained component wrapping O365 Active Users Usage UI."""
    
    def __init__(self, master, log_callback, credentials_callback, status_change_callback, **kwargs):
        self.semaphore = kwargs.pop("concurrency_semaphore", None)
        super().__init__(master, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=12, **kwargs)
        self.log_msg = log_callback
        self.get_credentials = credentials_callback
        self.on_status_change = status_change_callback
        self.status = None  # 'loading', 'success', 'error', None
        self.ITEMS_PER_PAGE = 5
        self.current_page = 0
        self.last_data = None
        
        self.build_ui()

    def build_ui(self):
        self.pack(fill="x", expand=True, pady=10)
        
        self.inner_pad = ctk.CTkFrame(self, fg_color="transparent")
        self.inner_pad.pack(fill="both", expand=True, padx=20, pady=20)
        
        
        self.header = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.header.pack(fill="x", pady=(0, 10))
        ctk.CTkLabel(self.header, text="O365 Active Users Usage", font=FONT_HEADER_SMALL, text_color=COLOR_TEXT_MAIN).pack(side="left")
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
        
        self.state_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.grid_frame = ctk.CTkFrame(self.inner_pad, fg_color=COLOR_OUTLINE_LIGHT, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)
        
        self.reset_view()

    def reset_view(self):
        self.pack_forget()
        self.state_frame.pack_forget()
        self.grid_frame.pack_forget()
        
        for w in self.state_frame.winfo_children():
            w.destroy()
        for w in self.grid_frame.winfo_children():
            w.destroy()
        self.current_page = 0
        self.last_data = None

    def _set_state_loading(self, msg="Loading..."):
        for w in self.state_frame.winfo_children():
            w.destroy()
        self.loading_label = __import__("customtkinter").CTkLabel(self.state_frame, text=f"⏳ {msg}", text_color="#6b7280", font=__import__("customtkinter").CTkFont(family="Segoe UI", size=13))
        self.loading_label.pack(pady=(20, 5))
        pb = __import__("customtkinter").CTkProgressBar(self.state_frame, mode="indeterminate", width=250, fg_color="#F3F4F6", progress_color="#2563EB")
        pb.pack(pady=(0, 20))
        pb.start()
        self.state_frame.pack(fill="x", expand=True)

    def _set_state_error(self, error_msg):
        for w in self.state_frame.winfo_children():
            w.destroy()

        display_msg = error_msg
        if "401" in error_msg or "403" in error_msg or "unauthorized" in error_msg.lower() or "forbidden" in error_msg.lower():
            display_msg = "Reports telemetry permission required.\nPlease grant the 'Reports.Read.All' application permission to your App Registration in Entra ID."

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
        usage_logger.info("Active Users Usage trigger_fetch called. Spawning background worker thread...")
        self.status = "loading"
        self.on_status_change()
        
        self.pack(fill="x", expand=True, pady=10)
        self.grid_frame.pack_forget()
        
        self._set_state_loading("Downloading and parsing O365 Active Users reports...")
        
        threading.Thread(
            target=self._execute_worker,
            args=(tenant, client_id, client_secret),
            daemon=True
        ).start()

    def _execute_worker(self, tenant: str, client_id: str, client_secret: str):
        if self.semaphore:
            self.semaphore.acquire()
        try:
            o365_data = run_o365_pipeline(client_id, client_secret, tenant)
            usage_logger.info("Successfully completed O365 usage data fetch.")
            self.after(0, self._render_success, o365_data)
        except Exception as e:
            usage_logger.error("Exception caught in ActiveUsersUsage worker.", exc_info=True)
            self.after(0, self._render_error, str(e))
        finally:
            if self.semaphore:
                self.semaphore.release()

    def _render_success(self, o365_data: list):
        self.o365_data = o365_data
        if hasattr(self, 'reload_btn') and self.reload_btn.winfo_exists():
            self.reload_btn.configure(state="normal")
        self.state_frame.pack_forget()
        for w in self.grid_frame.winfo_children():
            w.destroy()
        self.current_page = 0
        self.last_data = None

        self.grid_frame.pack(fill="x", expand=True)

        self.grid_frame.grid_columnconfigure(0, weight=2)
        self.grid_frame.grid_columnconfigure(1, weight=1)
        self.grid_frame.grid_columnconfigure(2, weight=1)
        self.grid_frame.grid_columnconfigure(3, weight=1)

        headers_o365 = ["Service", "30 Days", "90 Days", "180 Days"]
        for col_idx, head_text in enumerate(headers_o365):
            cell = ctk.CTkFrame(self.grid_frame, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        if not o365_data:
            empty_cell = ctk.CTkFrame(self.grid_frame, fg_color="transparent")
            empty_cell.grid(row=1, column=0, columnspan=4, sticky="nsew", pady=15)
            ctk.CTkLabel(empty_cell, text="No O365 usage data found.", text_color=COLOR_TEXT_SUB).pack()
        else:
            for r_idx, row_data in enumerate(o365_data, start=1):
                bg_style = COLOR_SURFACE if r_idx % 2 == 0 else COLOR_SURFACE_VARIANT
                for c_idx, val in enumerate(row_data):
                    cell = ctk.CTkFrame(self.grid_frame, fg_color=bg_style, corner_radius=0)
                    cell.grid(row=r_idx, column=c_idx, sticky="nsew", padx=0, pady=(0, 1))
                    fnt = FONT_BODY_BOLD if c_idx == 0 else FONT_BODY_MEDIUM
                    ctk.CTkLabel(cell, text=str(val), font=fnt, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=8, anchor="nw")

        self.status = "success"
        self.on_status_change()

    def _render_error(self, err_msg):
        usage_logger.warning(f"Active Users Usage fetch failed: {err_msg}")
        if hasattr(self, 'reload_btn') and self.reload_btn.winfo_exists():
            self.reload_btn.configure(state="normal")
        self._set_state_error(err_msg)
        self.status = "error"
        self.on_status_change()


class ActiveUsersTrendFrame(ctk.CTkFrame):
    """Self-contained component wrapping O365 Active User Trend Chart and height controls."""
    
    def __init__(self, master, log_callback, credentials_callback, status_change_callback, **kwargs):
        self.semaphore = kwargs.pop("concurrency_semaphore", None)
        super().__init__(master, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=12, **kwargs)
        self.log_msg = log_callback
        self.get_credentials = credentials_callback
        self.on_status_change = status_change_callback
        self.status = None  # 'loading', 'success', 'error', None
        self.ITEMS_PER_PAGE = 5
        self.current_page = 0
        self.last_data = None
        self.trend_data = {}
        
        self.build_ui()

    def build_ui(self):
        self.pack(fill="x", expand=True, pady=10)
        
        self.inner_pad = ctk.CTkFrame(self, fg_color="transparent")
        self.inner_pad.pack(fill="both", expand=True, padx=20, pady=20)
        
        trend_header = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        trend_header.pack(fill="x", pady=(0, 10))

        ctk.CTkLabel(trend_header, text="O365 30-Day Active User Trend", font=FONT_HEADER_SMALL, text_color=COLOR_TEXT_MAIN).pack(side="left")

        self.trend_height_var = ctk.DoubleVar(value=400)
        
        slider_frame = ctk.CTkFrame(trend_header, fg_color="transparent")
        slider_frame.pack(side="right")
        
        self.lbl_trend_height = ctk.CTkLabel(slider_frame, text="Height: 400px", font=FONT_BODY_SMALL, text_color=COLOR_TEXT_SUB)
        self.lbl_trend_height.pack(side="left", padx=(0, 10))
        
        self.slider_trend_height = ctk.CTkSlider(
            slider_frame, from_=200, to=800, number_of_steps=60,
            variable=self.trend_height_var, width=120, height=16,
            command=self._on_trend_height_slider_change
        )
        self.slider_trend_height.pack(side="left")

        self.state_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        
        self.grid_frame = ctk.CTkFrame(
            self.inner_pad, fg_color=COLOR_SURFACE,
            border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8,
            height=400
        )
        self.grid_frame.pack_propagate(False)

        self.reset_view()

    def reset_view(self):
        self.pack_forget()
        self.state_frame.pack_forget()
        self.grid_frame.pack_forget()
        self.trend_data = {}
        
        for w in self.state_frame.winfo_children():
            w.destroy()
        for w in self.grid_frame.winfo_children():
            w.destroy()
        self.current_page = 0
        self.last_data = None

    def _set_state_loading(self, msg="Loading..."):
        for w in self.state_frame.winfo_children():
            w.destroy()
        self.loading_label = __import__("customtkinter").CTkLabel(self.state_frame, text=f"⏳ {msg}", text_color="#6b7280", font=__import__("customtkinter").CTkFont(family="Segoe UI", size=13))
        self.loading_label.pack(pady=(20, 5))
        pb = __import__("customtkinter").CTkProgressBar(self.state_frame, mode="indeterminate", width=250, fg_color="#F3F4F6", progress_color="#2563EB")
        pb.pack(pady=(0, 20))
        pb.start()
        self.state_frame.pack(fill="x", expand=True)

    def _set_state_error(self, error_msg):
        for w in self.state_frame.winfo_children():
            w.destroy()

        display_msg = error_msg
        if "401" in error_msg or "403" in error_msg or "unauthorized" in error_msg.lower() or "forbidden" in error_msg.lower():
            display_msg = "Reports telemetry permission required.\nPlease grant the 'Reports.Read.All' application permission to your App Registration in Entra ID."

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
        usage_logger.info("Active Users Trend trigger_fetch called. Spawning background worker thread...")
        self.status = "loading"
        self.on_status_change()
        
        self.pack(fill="x", expand=True, pady=10)
        self.grid_frame.pack_forget()
        
        self._set_state_loading("Downloading and parsing O365 Trend reports...")
        
        threading.Thread(
            target=self._execute_worker,
            args=(tenant, client_id, client_secret),
            daemon=True
        ).start()

    def _execute_worker(self, tenant: str, client_id: str, client_secret: str):
        if self.semaphore:
            self.semaphore.acquire()
        try:
            trend_data = run_o365_trend_pipeline(client_id, client_secret, tenant)
            usage_logger.info("Successfully completed O365 trend data fetch.")
            self.after(0, self._render_success, trend_data)
        except Exception as e:
            usage_logger.error("Exception caught in ActiveUsersTrend worker.", exc_info=True)
            self.after(0, self._render_error, str(e))
        finally:
            if self.semaphore:
                self.semaphore.release()

    def _render_success(self, trend_data: dict):
        self.trend_data = trend_data
        if hasattr(self, 'reload_btn') and self.reload_btn.winfo_exists():
            self.reload_btn.configure(state="normal")
        self.state_frame.pack_forget()
        for w in self.grid_frame.winfo_children():
            w.destroy()
        self.current_page = 0
        self.last_data = None

        self.grid_frame.pack(fill="both", expand=True)

        if not MATPLOTLIB_AVAILABLE:
            empty_cell = ctk.CTkFrame(self.grid_frame, fg_color="transparent")
            empty_cell.pack(fill="x", expand=True, pady=15)
            ctk.CTkLabel(empty_cell, text="Matplotlib is required to render charts.\nPlease install it using 'pip install matplotlib'.", text_color=COLOR_ERROR).pack()
            self.status = "error"
            self.on_status_change()
            return

        if not trend_data or not trend_data.get("dates"):
            empty_cell = ctk.CTkFrame(self.grid_frame, fg_color="transparent")
            empty_cell.pack(fill="x", expand=True, pady=15)
            ctk.CTkLabel(empty_cell, text="No O365 trend data found.", text_color=COLOR_TEXT_SUB).pack()
        else:
            try:
                fig = Figure(figsize=(8, 4), dpi=100)
                ax = fig.add_subplot(111)
                fig.patch.set_facecolor(COLOR_SURFACE)
                ax.set_facecolor(COLOR_SURFACE)

                dates = trend_data["dates"]

                ax.plot(dates, trend_data["office365"], marker='o', label='Office 365')
                ax.plot(dates, trend_data["exchange"], marker='o', label='Exchange')
                ax.plot(dates, trend_data["onedrive"], marker='o', label='OneDrive')
                ax.plot(dates, trend_data["sharepoint"], marker='o', label='SharePoint')
                ax.plot(dates, trend_data["teams"], marker='o', label='Teams')

                ax.set_xlabel("Date", fontsize=10, color=COLOR_TEXT_SUB)
                ax.set_ylabel("Active Users", fontsize=10, color=COLOR_TEXT_SUB)

                ax.tick_params(axis='x', colors=COLOR_TEXT_SUB, rotation=45, labelsize=8)
                ax.tick_params(axis='y', colors=COLOR_TEXT_SUB)

                if len(dates) > 10:
                    ax.set_xticks(dates[::max(1, len(dates)//10)])

                for spine in ax.spines.values():
                    spine.set_color(COLOR_OUTLINE_LIGHT)

                ax.legend(facecolor=COLOR_SURFACE, edgecolor=COLOR_OUTLINE_LIGHT, labelcolor=COLOR_TEXT_MAIN, fontsize=9)
                fig.tight_layout()

                canvas = FigureCanvasTkAgg(fig, master=self.grid_frame)
                canvas.draw()
                canvas.get_tk_widget().pack(fill="both", expand=True, padx=10, pady=10)
            except Exception as e:
                usage_logger.error(f"Error drawing matplotlib plot: {e}", exc_info=True)
                empty_cell = ctk.CTkFrame(self.grid_frame, fg_color="transparent")
                empty_cell.pack(fill="x", expand=True, pady=15)
                ctk.CTkLabel(empty_cell, text="Failed to render trend graph (Matplotlib constraint).", text_color=COLOR_ERROR).pack()

        self.status = "success"
        self.on_status_change()

    def _render_error(self, err_msg):
        usage_logger.warning(f"Active Users Trend fetch failed: {err_msg}")
        if hasattr(self, 'reload_btn') and self.reload_btn.winfo_exists():
            self.reload_btn.configure(state="normal")
        self._set_state_error(err_msg)
        self.status = "error"
        self.on_status_change()

    def _on_trend_height_slider_change(self, val):
        height_val = int(val)
        self.lbl_trend_height.configure(text=f"Height: {height_val}px")
        self.grid_frame.configure(height=height_val)


class M365AppUsageFrame(ctk.CTkFrame):
    """Self-contained component wrapping M365 App Usage UI."""
    
    def __init__(self, master, log_callback, credentials_callback, status_change_callback, **kwargs):
        self.semaphore = kwargs.pop("concurrency_semaphore", None)
        super().__init__(master, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=12, **kwargs)
        self.log_msg = log_callback
        self.get_credentials = credentials_callback
        self.on_status_change = status_change_callback
        self.status = None  # 'loading', 'success', 'error', None
        self.ITEMS_PER_PAGE = 5
        self.current_page = 0
        self.last_data = None
        
        self.build_ui()

    def build_ui(self):
        self.pack(fill="x", expand=True, pady=10)
        
        self.inner_pad = ctk.CTkFrame(self, fg_color="transparent")
        self.inner_pad.pack(fill="both", expand=True, padx=20, pady=20)
        
        
        self.header = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.header.pack(fill="x", pady=(0, 10))
        ctk.CTkLabel(self.header, text="M365 App Usage (180 Days)", font=FONT_HEADER_SMALL, text_color=COLOR_TEXT_MAIN).pack(side="left")
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
        
        self.state_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.grid_frame = ctk.CTkFrame(self.inner_pad, fg_color=COLOR_OUTLINE_LIGHT, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)
        
        self.reset_view()

    def reset_view(self):
        self.pack_forget()
        self.state_frame.pack_forget()
        self.grid_frame.pack_forget()
        
        for w in self.state_frame.winfo_children():
            w.destroy()
        for w in self.grid_frame.winfo_children():
            w.destroy()
        self.current_page = 0
        self.last_data = None

    def _set_state_loading(self, msg="Loading..."):
        for w in self.state_frame.winfo_children():
            w.destroy()
        self.loading_label = __import__("customtkinter").CTkLabel(self.state_frame, text=f"⏳ {msg}", text_color="#6b7280", font=__import__("customtkinter").CTkFont(family="Segoe UI", size=13))
        self.loading_label.pack(pady=(20, 5))
        pb = __import__("customtkinter").CTkProgressBar(self.state_frame, mode="indeterminate", width=250, fg_color="#F3F4F6", progress_color="#2563EB")
        pb.pack(pady=(0, 20))
        pb.start()
        self.state_frame.pack(fill="x", expand=True)

    def _set_state_error(self, error_msg):
        for w in self.state_frame.winfo_children():
            w.destroy()

        display_msg = error_msg
        if "401" in error_msg or "403" in error_msg or "unauthorized" in error_msg.lower() or "forbidden" in error_msg.lower():
            display_msg = "Reports telemetry permission required.\nPlease grant the 'Reports.Read.All' application permission to your App Registration in Entra ID."

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
        usage_logger.info("M365 App Usage trigger_fetch called. Spawning background worker thread...")
        self.status = "loading"
        self.on_status_change()
        
        self.pack(fill="x", expand=True, pady=10)
        self.grid_frame.pack_forget()
        
        self._set_state_loading("Downloading and parsing M365 App Usage reports...")
        
        threading.Thread(
            target=self._execute_worker,
            args=(tenant, client_id, client_secret),
            daemon=True
        ).start()

    def _execute_worker(self, tenant: str, client_id: str, client_secret: str):
        if self.semaphore:
            self.semaphore.acquire()
        try:
            m365_data = run_m365_pipeline(client_id, client_secret, tenant)
            usage_logger.info("Successfully completed M365 Apps usage data fetch.")
            self.after(0, self._render_success, m365_data)
        except Exception as e:
            usage_logger.error("Exception caught in M365AppUsage worker.", exc_info=True)
            self.after(0, self._render_error, str(e))
        finally:
            if self.semaphore:
                self.semaphore.release()

    def _render_success(self, m365_data: list):
        if hasattr(self, 'reload_btn') and self.reload_btn.winfo_exists():
            self.reload_btn.configure(state="normal")
        self.state_frame.pack_forget()
        self.grid_frame.pack(fill="x")

        self.last_data = m365_data
        self.current_page = 0
        self._update_ui_paginated()

        self.status = "success"
        self.on_status_change()

    def _update_ui_paginated(self, data=None):
        if data is None:
            data = self.last_data

        for w in self.grid_frame.winfo_children():
            info = w.grid_info()
            if "row" in info and int(info["row"]) > 0:
                w.destroy()

        for i in range(2):
            self.grid_frame.grid_columnconfigure(i, weight=1)
        self.grid_frame.grid_columnconfigure(2, weight=0)
        self.grid_frame.grid_columnconfigure(3, weight=0)

        headers = ["App / Platform", "Users Count"]
        
        if not self.grid_frame.winfo_children():
            for col_idx, head_text in enumerate(headers):
                cell = ctk.CTkFrame(self.grid_frame, fg_color=COLOR_TONAL_BG, corner_radius=0)
                cell.grid(row=0, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
                ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        total_count = len(data) if data else 0
        start_idx = self.current_page * self.ITEMS_PER_PAGE
        end_idx = start_idx + self.ITEMS_PER_PAGE
        page_data = data[start_idx:end_idx] if data else []

        if not data:
            empty_cell = ctk.CTkFrame(self.grid_frame, fg_color="transparent")
            empty_cell.grid(row=1, column=0, columnspan=2, sticky="nsew", pady=15)
            ctk.CTkLabel(empty_cell, text="No M365 App usage data found.", text_color=COLOR_TEXT_SUB).pack()
        else:
            for r_idx, (platform, count) in enumerate(page_data, start=1):
                bg_style = COLOR_SURFACE if r_idx % 2 != 0 else COLOR_SURFACE_VARIANT
                vals = [platform, count]
                for c_idx, val in enumerate(vals):
                    cell = ctk.CTkFrame(self.grid_frame, fg_color=bg_style, corner_radius=0)
                    cell.grid(row=r_idx, column=c_idx, sticky="nsew", padx=0, pady=(0, 1))
                    fnt = FONT_BODY_BOLD if c_idx == 0 else FONT_BODY_MEDIUM
                    ctk.CTkLabel(cell, text=str(val), font=fnt, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=6, anchor="nw")

        self._draw_pagination_controls(total_count, data)

    def _draw_pagination_controls(self, total_count, data):
        total_pages = max(1, (total_count + self.ITEMS_PER_PAGE - 1) // self.ITEMS_PER_PAGE)
        
        control_frame = ctk.CTkFrame(self.grid_frame, fg_color=COLOR_SURFACE)
        control_frame.grid(row=self.ITEMS_PER_PAGE + 1, column=0, columnspan=2, pady=0, sticky="ew")
        
        center_container = ctk.CTkFrame(control_frame, fg_color="transparent")
        center_container.pack(pady=(5, 10))

        prev_state = "normal" if self.current_page > 0 else "disabled"
        btn_prev = ctk.CTkButton(
            center_container, text="◀ Prev", width=70, height=26, corner_radius=6,
            font=FONT_BODY_SMALL, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER, state=prev_state,
            command=lambda: self._change_page(-1, data)
        )
        btn_prev.pack(side="left", padx=5)

        page_lbl = ctk.CTkLabel(center_container, text=f"Page {self.current_page + 1} of {total_pages}", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB)
        page_lbl.pack(side="left", padx=15)

        next_state = "normal" if self.current_page < total_pages - 1 else "disabled"
        btn_next = ctk.CTkButton(
            center_container, text="Next ▶", width=70, height=26, corner_radius=6,
            font=FONT_BODY_SMALL, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER, state=next_state,
            command=lambda: self._change_page(1, data)
        )
        btn_next.pack(side="left", padx=5)


    def _change_page(self, delta, data):
        self.current_page += delta
        self._update_ui_paginated(data)

    def _render_error(self, err_msg):
        usage_logger.warning(f"M365 App Usage fetch failed: {err_msg}")
        if hasattr(self, 'reload_btn') and self.reload_btn.winfo_exists():
            self.reload_btn.configure(state="normal")
        self._set_state_error(err_msg)
        self.status = "error"
        self.on_status_change()
