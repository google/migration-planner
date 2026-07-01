import os
import logging
import threading
import sqlite3
import asyncio
import customtkinter as ctk

from core.graph.files.msteams_overview import run_msteams_pipeline
from core.graph.db import import_csv_to_sqlite
from telemetry.styles import *

logger = logging.getLogger("M365TelemetryAsyncLogger.MsTeamsOverviewUI")

class MsTeamsOverviewFrame(ctk.CTkFrame):
    def __init__(self, master, log_callback, credentials_callback, status_change_callback, semaphore=None, **kwargs):
        self.semaphore = semaphore
        super().__init__(master, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=12, **kwargs)
        self.log_msg = log_callback
        self.get_credentials = credentials_callback
        self.on_status_change = status_change_callback
        
        self.status = None
        self.is_cancelled = False
        self.current_request_id = 0
        self.csv_path = None
        
        self.build_ui()

    def build_ui(self):
        self.pack(fill="x", expand=True, pady=10)
        
        self.inner_pad = ctk.CTkFrame(self, fg_color="transparent")
        self.inner_pad.pack(fill="both", expand=True, padx=20, pady=20)
        
        self.header = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.header.pack(fill="x", pady=(0, 10))
        ctk.CTkLabel(self.header, text="Microsoft Teams Overview", font=FONT_HEADER_SMALL, text_color=COLOR_TEXT_MAIN).pack(side="left")
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
        
        self.reset_view()

    def reset_view(self):
        self.pack_forget()
        self.state_frame.pack_forget()
        self.grid_frame.pack_forget()
        
        for w in self.state_frame.winfo_children():
            w.destroy()
        for w in self.grid_frame.winfo_children():
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
        logger.info("MsTeams Overview trigger_fetch called.")
        self.status = "loading"
        self.is_cancelled = False
        self.on_status_change()
        
        self.pack(fill="x", expand=True, pady=(20, 5))
        self.grid_frame.pack_forget()
        
        self._set_state_loading("Scanning MsTeams details...")
        
        threading.Thread(
            target=self._execute_worker,
            args=(tenant, client_id, client_secret, self.current_request_id),
            daemon=True
        ).start()

    def _execute_worker(self, tenant: str, client_id: str, client_secret: str, request_id: int):
        if self.semaphore:
            self.semaphore.acquire()
        try:
            if self.is_cancelled or request_id != self.current_request_id: return
            
            self.csv_path = run_msteams_pipeline(client_id, client_secret, tenant)
            
            if self.is_cancelled or request_id != self.current_request_id: return
            
            db_path = os.path.join(os.path.dirname(self.csv_path), "telemetry_cache.db")
            asyncio.run(import_csv_to_sqlite(self.csv_path, db_path, "msteams_activity"))
            
            if self.is_cancelled or request_id != self.current_request_id: return
            
            self.status = "success"
            self.after(0, self._render_success, request_id)
        except Exception as e:
            logger.error(f"Error fetching feature telemetry: {e}", exc_info=True)
            self.status = "error"
            self.after(0, self._render_error, str(e), request_id)
        finally:
            if self.semaphore:
                self.semaphore.release()
            self.after(0, self.on_status_change)

    def _load_metrics_from_sqlite(self):
        if not self.csv_path or not os.path.exists(self.csv_path): return {}
        db_path = os.path.join(os.path.dirname(self.csv_path), "telemetry_cache.db")
        if not os.path.exists(db_path): return {}

        conn = sqlite3.connect(db_path)
        metrics = {}
        try:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM msteams_activity WHERE Team_Name IS NOT NULL AND Team_Name != ''")
            row = cursor.fetchone()
            metrics["total_teams"] = row[0] if row else 0

            cursor.execute("SELECT SUM(Active_Users), SUM(Guests), SUM(Active_Channels), SUM(Channel_Messages), SUM(Meetings_Organized) FROM msteams_activity WHERE Team_Name IS NOT NULL AND Team_Name != ''")
            row = cursor.fetchone()
            metrics["active_users"] = int(row[0] or 0) if row else 0
            metrics["guests"] = int(row[1] or 0) if row else 0
            metrics["active_channels"] = int(row[2] or 0) if row else 0
            metrics["channel_messages"] = int(row[3] or 0) if row else 0
            metrics["meetings_organized"] = int(row[4] or 0) if row else 0
            
            return metrics
        except Exception as e:
            logger.error(f"Error reading SQLite: {e}")
            return {}
        finally:
            conn.close()

    def _render_success(self, request_id):
        if self.is_cancelled or request_id != self.current_request_id: return
        if hasattr(self, 'reload_btn') and self.reload_btn.winfo_exists():
            self.reload_btn.configure(state="normal")
            
        self.state_frame.pack_forget()
        self._update_grid()
        self.grid_frame.pack(fill="x", expand=True)
        
    def _render_error(self, err_msg, request_id):
        if self.is_cancelled or request_id != self.current_request_id: return
        logger.warning(f"MsTeams Overview fetch failed: {err_msg}")
        if hasattr(self, 'reload_btn') and self.reload_btn.winfo_exists():
            self.reload_btn.configure(state="normal")
        self._set_state_error(err_msg)

    def _update_grid(self):
        for w in self.grid_frame.winfo_children():
            w.destroy()
            
        metrics = self._load_metrics_from_sqlite()
        
        self.grid_frame.grid_columnconfigure(0, weight=3)
        self.grid_frame.grid_columnconfigure(1, weight=2)
        
        headers = ["MsTeams Metric Description", "Value / Measurement"]
        for col_idx, head_text in enumerate(headers):
            cell = ctk.CTkFrame(self.grid_frame, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")
            
        if not metrics:
            c0 = ctk.CTkFrame(self.grid_frame, fg_color=COLOR_SURFACE, corner_radius=0)
            c0.grid(row=1, column=0, columnspan=len(headers), sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c0, text="No activity data found.", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=6)
            return
            
        users = metrics.get('active_users', 0)
        channels = metrics.get('active_channels', 0)
        avg_users_per_channel = f"{(users / channels):.1f}" if channels > 0 else "0"
            
        rows_data = [
            ("Total Teams Count", f"{metrics.get('total_teams', 0):,} Teams"),
            ("Total Active Channels (180 days)", f"{channels:,} Channels"),
            ("Total Channel Messages", f"{metrics.get('channel_messages', 0):,} Messages"),
            ("Total Active Users(180 days)", f"{users:,} Users"),
            ("Average Users per Channel", avg_users_per_channel),
            ("Total Meetings Organized", f"{metrics.get('meetings_organized', 0):,} Meetings"),
            ("Total Guests", f"{metrics.get('guests', 0):,} Guests")
        ]
        
        for r_idx, (metric_name, val) in enumerate(rows_data, start=1):
            bg_style = COLOR_SURFACE if r_idx % 2 == 0 else COLOR_SURFACE_VARIANT
            
            c0 = ctk.CTkFrame(self.grid_frame, fg_color=bg_style, corner_radius=0)
            c0.grid(row=r_idx, column=0, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c0, text=metric_name, font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=6, anchor="w")

            c1 = ctk.CTkFrame(self.grid_frame, fg_color=bg_style, corner_radius=0)
            c1.grid(row=r_idx, column=1, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c1, text=val, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=6, anchor="w")
