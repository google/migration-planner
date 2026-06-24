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

"""UI frame for Microsoft Entra App Registrations telemetry."""

import os
import csv
import logging
import threading
import asyncio
import sqlite3
import customtkinter as ctk

from core.graph.entra.app_registrations import run_app_registrations_pipeline
from core.graph.db import import_csv_to_sqlite, query_page_sync
from telemetry.styles import *

usage_logger = logging.getLogger("M365TelemetryAsyncLogger.AppRegistrationsUI")

class AppRegistrationsSubFrame(ctk.CTkFrame):
    """Sub-frame for Microsoft Entra App Registrations with UI Pagination."""

    def __init__(self, master, log_callback, credentials_callback, status_change_callback, semaphore=None, **kwargs):
        super().__init__(master, fg_color="transparent", **kwargs)
        self.log_msg = log_callback
        self.get_credentials = credentials_callback
        self.on_status_change = status_change_callback
        self.semaphore = semaphore
        self.status = None
        self._cached_app_registrations = []
        self.is_cancelled = False
        self.current_request_id = 0

        # Pagination variables (5 rows per page)
        self.ITEMS_PER_PAGE = 5
        self.current_page = 0
        self.csv_path = None

        self.build_ui()

    def build_ui(self):
        self.header_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.header_frame.pack(fill="x", pady=(0, 5))

        ctk.CTkLabel(self.header_frame, text="App Registrations", font=FONT_SUBSECTION_HEADER, text_color=COLOR_PRIMARY).pack(side="left")

        self.btn_refresh = ctk.CTkButton(
            self.header_frame, state="disabled", text="↻ Reload", width=80, height=26, corner_radius=13,
            font=FONT_BODY_SMALL, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER,
            command=self.trigger_fetch_individual
        )
        self.btn_refresh.pack(side="right", padx=(10, 0))

        self.body_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.body_frame.pack(fill="x")

        self.reset_view()

    def reset_view(self):
        self.status = None
        self._cached_app_registrations = []
        self.is_cancelled = False
        self.current_page = 0
        self.csv_path = None
        for w in self.body_frame.winfo_children():
            w.destroy()

    def _set_state_loading(self, msg="Loading..."):
        for w in self.body_frame.winfo_children():
            w.destroy()
        ctk.CTkLabel(self.body_frame, text=f"⏳ {msg}", text_color=COLOR_TEXT_SUB, font=FONT_BODY_MEDIUM).pack(pady=10)

    def _set_state_error(self, error_msg):
        for w in self.body_frame.winfo_children():
            w.destroy()
        display_msg = error_msg
        if "401" in error_msg or "403" in error_msg or "unauthorized" in error_msg.lower() or "forbidden" in error_msg.lower() or "application.read" in error_msg.lower():
            display_msg = "Application.Read.All application permission required.\nPlease grant 'Application.Read.All' to your App Registration in Microsoft Entra ID."
        ctk.CTkLabel(self.body_frame, text=f"✖ {display_msg}", text_color=COLOR_ERROR, font=FONT_BODY_MEDIUM, justify="center").pack(pady=(10, 5))
        ctk.CTkButton(self.body_frame, text="Try Again", command=self.trigger_fetch_individual, width=120, fg_color="transparent", border_width=1, text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER).pack(pady=(0, 10))

    def trigger_fetch_individual(self):
        tenant, clients, secrets = self.get_credentials()
        if tenant:
            self.trigger_fetch(tenant, clients[0], secrets[0])
        else:
            self._set_state_error("Missing credentials. Please submit the credentials above.")

    def trigger_fetch(self, tenant, client_id, client_secret):
        self.status = "loading"
        self.is_cancelled = False
        self.current_page = 0

        script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
        if os.path.basename(script_dir) == "entra":
            script_dir = os.path.dirname(script_dir)
        reports_dir = os.path.join(script_dir, "reports", f"{tenant}_{client_id}")
        self.csv_path = os.path.join(reports_dir, "entra_app_registrations.csv")

        self._set_state_loading("Downloading and parsing App Registrations...")
        self.on_status_change()
        if hasattr(self, "btn_refresh") and self.btn_refresh.winfo_exists():
            self.btn_refresh.configure(state="disabled")

        threading.Thread(
            target=self._execute_worker,
            args=(tenant, client_id, client_secret, self.current_request_id),
            daemon=True
        ).start()

    def _execute_worker(self, tenant, client_id, client_secret, request_id):
        if self.semaphore:
            self.semaphore.acquire()
        try:
            if self.is_cancelled or request_id != self.current_request_id:
                return

            app_registrations = []
            page_count = 0
            def handle_page(value_list):
                nonlocal app_registrations, page_count
                if self.is_cancelled or request_id != self.current_request_id:
                    return
                for item in value_list:
                    displayName = item.get("displayName") or ""
                    appId = item.get("appId") or ""
                    createdDateTime = item.get("createdDateTime") or ""
                    signInAudience = item.get("signInAudience") or ""
                    
                    secrets_cnt = len(item.get("passwordCredentials", []))
                    certs_cnt = len(item.get("keyCredentials", []))
                    credentials_str = f"{secrets_cnt} Secrets, {certs_cnt} Certs"
                    
                    app_registrations.append((displayName, appId, createdDateTime, signInAudience, credentials_str))
                page_count += 1
                if page_count % 3 == 0:
                    self.after(0, self._render_partial, list(app_registrations), request_id)

            temp_csv_path = self.csv_path + ".tmp"
            script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
            if os.path.basename(script_dir) == "entra":
                script_dir = os.path.dirname(script_dir)
            reports_dir = os.path.join(script_dir, "reports", f"{tenant}_{client_id}")
            os.makedirs(reports_dir, exist_ok=True)
            
            with open(temp_csv_path, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["displayName", "appId", "createdDateTime", "signInAudience", "credentials"])

            run_app_registrations_pipeline(
                client_id=client_id,
                client_secret=client_secret,
                tenant_id=tenant,
                csv_path=temp_csv_path,
                max_rows=5000,
                on_page_callback=handle_page,
                is_cancelled_callback=lambda: self.is_cancelled or request_id != self.current_request_id
            )

            if not self.is_cancelled and request_id == self.current_request_id:
                if os.path.exists(temp_csv_path):
                    if os.path.exists(self.csv_path):
                        os.remove(self.csv_path)
                    os.rename(temp_csv_path, self.csv_path)
                import asyncio
                db_dir = os.path.dirname(self.csv_path)
                db_path = os.path.join(db_dir, "telemetry_cache.db")
                asyncio.run(import_csv_to_sqlite(self.csv_path, db_path, "app_registrations", "displayName"))
                
                if self.is_cancelled or request_id != self.current_request_id:
                    return
                self.after(0, self._render_success, app_registrations, request_id)
        except Exception as e:
            usage_logger.error(f"Error fetching app registrations: {e}", exc_info=True)
            if not self.is_cancelled and request_id == self.current_request_id:
                self.after(0, self._render_error, str(e), request_id)
        finally:
            if 'temp_csv_path' in locals() and os.path.exists(temp_csv_path):
                try:
                    os.remove(temp_csv_path)
                except Exception:
                    pass
            if self.semaphore:
                self.semaphore.release()

    def _render_partial(self, data, request_id):
        if self.is_cancelled or request_id != self.current_request_id:
            return
        self._update_ui_paginated(data, is_partial=True)

    def _render_success(self, data, request_id):
        if self.is_cancelled or request_id != self.current_request_id:
            return
        self.status = "success"
        self._cached_app_registrations = data
        self._update_ui_paginated(data=None, is_partial=False)
        self.on_status_change()
        if hasattr(self, "btn_refresh") and self.btn_refresh.winfo_exists():
            self.btn_refresh.configure(state="normal")

    def _render_error(self, err_msg, request_id):
        if self.is_cancelled or request_id != self.current_request_id:
            return
        self.status = "error"
        self._set_state_error(err_msg)
        self.on_status_change()
        if hasattr(self, "btn_refresh") and self.btn_refresh.winfo_exists():
            self.btn_refresh.configure(state="normal")

    def _load_page_from_csv(self, page):
        if not self.csv_path or not os.path.exists(self.csv_path):
            return [], 0

        db_dir = os.path.dirname(self.csv_path)
        db_path = os.path.join(db_dir, "telemetry_cache.db")
        try:
            page_data, total_count = query_page_sync(db_path, "app_registrations", page, self.ITEMS_PER_PAGE)
            mapped_data = []
            for r in page_data:
                mapped_data.append((
                    r.get("displayName", ""),
                    r.get("appId", ""),
                    r.get("createdDateTime", ""),
                    r.get("signInAudience", ""),
                    r.get("credentials", "")
                ))
            return mapped_data, total_count
        except Exception as e:
            usage_logger.error(f"Error loading page from SQLite cache: {e}")
            return [], 0

    def _update_ui_paginated(self, data=None, is_partial=False):
        for w in self.body_frame.winfo_children():
            w.destroy()

        if is_partial:
            progress_frame = ctk.CTkFrame(self.body_frame, fg_color=COLOR_TONAL_BG, height=26, corner_radius=6)
            progress_frame.pack(fill="x", pady=(0, 6))
            ctk.CTkLabel(
                progress_frame,
                text="⏳ Querying App Registrations in the background... UI will auto-refresh.",
                font=FONT_BODY_SMALL,
                text_color=COLOR_TONAL_TEXT
            ).pack(padx=10, pady=2, anchor="w")

        # Get the page slice
        if data is not None:
            total_count = len(data)
            start_idx = self.current_page * self.ITEMS_PER_PAGE
            end_idx = start_idx + self.ITEMS_PER_PAGE
            page_data = data[start_idx:end_idx]
        else:
            page_data, total_count = self._load_page_from_csv(self.current_page)

        # Draw the table grid
        metrics_grid = ctk.CTkFrame(self.body_frame, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)
        metrics_grid.pack(fill="x", pady=(5, 10))

        headers = ["App Name", "Application ID", "Created Date", "Sign In Audience", "Credentials"]
        for i in range(5):
            metrics_grid.grid_columnconfigure(i, weight=1)

        for col_idx, head_text in enumerate(headers):
            cell = ctk.CTkFrame(metrics_grid, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        if not page_data:
            c = ctk.CTkFrame(metrics_grid, fg_color=COLOR_SURFACE, corner_radius=0)
            c.grid(row=1, column=0, columnspan=5, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c, text="No app registrations detected.", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="center").pack(padx=10, pady=12)
        else:
            for r_idx, (name, app_id, created, audience, creds) in enumerate(page_data, start=1):
                bg_style = COLOR_SURFACE if r_idx % 2 != 0 else COLOR_SURFACE_VARIANT
                formatted_created = created[:10] if created else ""
                vals = [name, app_id, formatted_created, audience, creds]
                for c_idx, val in enumerate(vals):
                    c = ctk.CTkFrame(metrics_grid, fg_color=bg_style, corner_radius=0)
                    c.grid(row=r_idx, column=c_idx, sticky="nsew", padx=0, pady=(0, 1))
                    ctk.CTkLabel(c, text=val, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="left", wraplength=180).pack(padx=10, pady=8, anchor="nw")

        # Draw the pagination controls if we have items
        if total_count > 0:
            self._draw_pagination_controls(total_count, data, is_partial)

    def _draw_pagination_controls(self, total_count, data, is_partial):
        total_pages = (total_count + self.ITEMS_PER_PAGE - 1) // self.ITEMS_PER_PAGE
        if total_pages <= 1:
            return

        control_frame = ctk.CTkFrame(self.body_frame, fg_color=COLOR_SURFACE)
        control_frame.pack(fill="x", pady=0)

        center_container = ctk.CTkFrame(control_frame, fg_color="transparent")
        center_container.pack(pady=(5, 10))

        prev_state = "normal" if self.current_page > 0 else "disabled"
        btn_prev = ctk.CTkButton(
            center_container, text="◀ Prev", width=70, height=22, corner_radius=6,
            font=FONT_BODY_SMALL, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER,
            state=prev_state,
            command=lambda: self._change_page(-1, data, is_partial)
        )
        btn_prev.pack(side="left", padx=5)

        page_lbl = ctk.CTkLabel(
            center_container,
            text=f"Page {self.current_page + 1} of {total_pages} ({total_count} items)",
            font=FONT_BODY_MEDIUM,
            text_color=COLOR_TEXT_SUB
        )
        page_lbl.pack(side="left", padx=15)

        next_state = "normal" if self.current_page < total_pages - 1 else "disabled"
        btn_next = ctk.CTkButton(
            center_container, text="Next ▶", width=70, height=22, corner_radius=6,
            font=FONT_BODY_SMALL, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER,
            state=next_state,
            command=lambda: self._change_page(1, data, is_partial)
        )
        btn_next.pack(side="left", padx=5)

    def _change_page(self, delta, data, is_partial):
        self.current_page += delta
        self._update_ui_paginated(data, is_partial)

    def cancel(self):
        self.is_cancelled = True
        self.current_request_id += 1
        if self.status == "loading":
            self.status = "cancelled"
            self._update_ui_paginated(self.last_data)
        if hasattr(self, "btn_refresh") and self.btn_refresh.winfo_exists():
            self.btn_refresh.configure(state="normal")

    def _load_data_from_csv(self):
        if not self.csv_path or not os.path.exists(self.csv_path):
            tenant, clients, secrets = self.get_credentials()
            if tenant and clients:
                script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
                if os.path.basename(script_dir) == "entra":
                    script_dir = os.path.dirname(script_dir)
                self.csv_path = os.path.join(script_dir, "reports", f"{tenant}_{clients[0]}", "entra_app_registrations.csv")
            else:
                return []
                
        db_dir = os.path.dirname(self.csv_path)
        db_path = os.path.join(db_dir, "telemetry_cache.db")
        if not os.path.exists(db_path):
            return []
            
        items = []
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT displayName, appId, createdDateTime, signInAudience, credentials FROM app_registrations")
            for r in cursor.fetchall():
                items.append((r[0], r[1], r[2], r[3], r[4]))
            conn.close()
        except Exception as e:
            usage_logger.error(f"Error reading SQLite cache for AppRegistrationsSubFrame: {e}", exc_info=True)
        return items

    @property
    def last_data(self):
        if hasattr(self, "_cached_app_registrations") and self._cached_app_registrations:
            return self._cached_app_registrations
        return self._load_data_from_csv()
