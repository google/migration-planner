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

"""UI frame for O365 Active Users Trend telemetry."""

import os
import logging
import threading
import customtkinter as ctk

# Safely import matplotlib
try:
    from matplotlib.figure import Figure
    from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False

from core.graph.m365_apps.active_users_trend import run_o365_trend_pipeline
from telemetry.styles import *

usage_logger = logging.getLogger("M365TelemetryAsyncLogger.ActiveUsersTrendUI")

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
            display_msg = "Reports telemetry permission required.\nPlease grant the 'Reports.Read.All' application permission to your App Registration in Entra ID."

        ctk.CTkLabel(self.state_frame, text=f"✖ {display_msg}", text_color=COLOR_ERROR, font=FONT_BODY_MEDIUM, justify="center").pack(pady=(20, 5))
        ctk.CTkButton(self.state_frame, text="Try Again", command=self._retry_fetch, width=120, fg_color="transparent", border_width=1, text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER).pack(pady=(0, 20))
        self.state_frame.pack(fill="x", expand=True)

    def _retry_fetch(self):
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
        self.last_data = trend_data
        self.state_frame.pack_forget()
        for w in self.grid_frame.winfo_children():
            w.destroy()
        self.current_page = 0

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
        self._set_state_error(err_msg)
        self.status = "error"
        self.on_status_change()

    def _on_trend_height_slider_change(self, val):
        height_val = int(val)
        self.lbl_trend_height.configure(text=f"Height: {height_val}px")
        self.grid_frame.configure(height=height_val)

    def cancel(self):
        pass
