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

"""Modular Microsoft Entra Data telemetry scanners and visual interfaces."""

import os
import csv
import logging
import threading
from typing import Any, Dict, List, Optional
import customtkinter as ctk

# Bind to the async logger initialized in m365_telemetry.py
usage_logger = logging.getLogger("M365TelemetryAsyncLogger.DevicesAppsUI")

# Import unified core service layer
from core.graph.client import GraphClient
from core.graph.security import SecurityService
from core.graph.reports import ReportsService

# Import shared styles
from telemetry.styles import *


class AuthMethodsSubFrame(ctk.CTkFrame):
    """Sub-frame for Microsoft Entra Authentication Methods."""

    def __init__(self, master, log_callback, credentials_callback, status_change_callback, semaphore=None, **kwargs):
        super().__init__(master, fg_color="transparent", **kwargs)
        self.log_msg = log_callback
        self.get_credentials = credentials_callback
        self.on_status_change = status_change_callback
        self.semaphore = semaphore
        self.status = None
        self.last_data = []
        self.is_cancelled = False
        self.current_request_id = 0
        self.current_page = 0
        self.ITEMS_PER_PAGE = 5

        self.build_ui()

    def build_ui(self):
        self.header_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.header_frame.pack(fill="x", pady=(0, 5))

        ctk.CTkLabel(self.header_frame, text="Authentication Methods", font=FONT_SUBSECTION_HEADER, text_color=COLOR_PRIMARY).pack(side="left")

        self.btn_refresh = ctk.CTkButton(
            self.header_frame, state="disabled", text="↻ Reload", width=80, height=26, corner_radius=13,
            font=FONT_BODY_SMALL, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER,
            command=self.trigger_fetch_individual
        )
        self.btn_refresh.pack(side="right", padx=(10, 0))

        self.body_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.body_frame.pack(fill="x", expand=True)

        self.reset_view()

    def reset_view(self):
        self.status = None
        self.last_data = []
        self.is_cancelled = False
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
        if "401" in error_msg or "403" in error_msg or "unauthorized" in error_msg.lower() or "forbidden" in error_msg.lower():
            display_msg = "Reports.Read.All application permission required.\nPlease grant 'Reports.Read.All' to your App Registration in Microsoft Entra ID."
        ctk.CTkLabel(self.body_frame, text=f"✖ {display_msg}", text_color=COLOR_ERROR, font=FONT_BODY_MEDIUM, justify="center").pack(pady=10)

    def trigger_fetch_individual(self):
        tenant, clients, secrets = self.get_credentials()
        if tenant:
            self.trigger_fetch(tenant, clients[0], secrets[0])
        else:
            self._set_state_error("Missing credentials. Please submit the credentials above.")

    def trigger_fetch(self, tenant, client_id, client_secret):
        self.status = "loading"
        self.is_cancelled = False
        self.current_request_id += 1
        self.current_page = 0
        self._set_state_loading("Downloading and parsing Authentication Methods...")
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

            script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
            reports_dir = os.path.join(script_dir, "reports", f"{tenant}_{client_id}")
            os.makedirs(reports_dir, exist_ok=True)
            csv_path = os.path.join(reports_dir, "entra_auth_methods.csv")

            client = GraphClient(
                tenant_id=tenant,
                client_ids=client_id,
                client_secrets=client_secret,
                concurrency=1,
                retries=5,
                backoff=2
            )
            client.authenticate()
            reports_service = ReportsService(client)

            auth_methods = []
            page_count = 0
            def handle_page(value_list):
                nonlocal auth_methods, page_count
                if self.is_cancelled or request_id != self.current_request_id:
                    return
                for item in value_list:
                    method = item.get("authenticationMethod") or ""
                    activity = str(item.get("successActivityCount") or 0)
                    auth_methods.append((method, activity))
                page_count += 1
                if page_count % 3 == 0:
                    self.after(0, self._render_partial, list(auth_methods), request_id)

            with open(csv_path, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["authenticationMethod", "successActivityCount"])

            reports_service.fetch_auth_methods_summary(
                csv_path=csv_path,
                max_rows=5000,
                on_page_callback=handle_page,
                is_cancelled_callback=lambda: self.is_cancelled or request_id != self.current_request_id
            )
            client.close()

            if not self.is_cancelled and request_id == self.current_request_id:
                self.after(0, self._render_success, auth_methods, request_id)
        except Exception as e:
            usage_logger.error(f"Error fetching auth methods: {e}", exc_info=True)
            if not self.is_cancelled and request_id == self.current_request_id:
                self.after(0, self._render_error, str(e), request_id)
        finally:
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
        self.last_data = data
        self._update_ui_paginated(data, is_partial=False)
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

    def _update_ui_paginated(self, data, is_partial=False):
        for w in self.body_frame.winfo_children():
            w.destroy()

        if is_partial:
            progress_frame = ctk.CTkFrame(self.body_frame, fg_color=COLOR_TONAL_BG, height=26, corner_radius=6)
            progress_frame.pack(fill="x", pady=(0, 6))
            ctk.CTkLabel(
                progress_frame,
                text="⏳ Querying Authentication Methods in the background... UI will auto-refresh.",
                font=FONT_BODY_SMALL,
                text_color=COLOR_TONAL_TEXT
            ).pack(padx=10, pady=2, anchor="w")

        metrics_grid = ctk.CTkFrame(self.body_frame, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)
        metrics_grid.pack(fill="x", pady=(5, 10))

        headers = ["Authentication Method", "Success Activity Count"]
        for i in range(2):
            metrics_grid.grid_columnconfigure(i, weight=1)

        for col_idx, head_text in enumerate(headers):
            cell = ctk.CTkFrame(metrics_grid, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        if not data:
            c = ctk.CTkFrame(metrics_grid, fg_color=COLOR_SURFACE, corner_radius=0)
            c.grid(row=1, column=0, columnspan=2, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c, text="No authentication activity detected.", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="center").pack(padx=10, pady=12)
        else:
            total_count = len(data)
            start_idx = self.current_page * self.ITEMS_PER_PAGE
            end_idx = start_idx + self.ITEMS_PER_PAGE
            page_data = data[start_idx:end_idx]

            for r_idx, (method, activity) in enumerate(page_data, start=1):
                bg_style = COLOR_SURFACE if r_idx % 2 != 0 else COLOR_SURFACE_VARIANT
                vals = [method, activity]
                for c_idx, val in enumerate(vals):
                    c = ctk.CTkFrame(metrics_grid, fg_color=bg_style, corner_radius=0)
                    c.grid(row=r_idx, column=c_idx, sticky="nsew", padx=0, pady=(0, 1))
                    ctk.CTkLabel(c, text=val, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="left").pack(padx=10, pady=8, anchor="nw")

            if total_count > self.ITEMS_PER_PAGE:
                self._draw_pagination_controls(total_count, data, is_partial)

    def _draw_pagination_controls(self, total_count, data, is_partial):
        total_pages = (total_count + self.ITEMS_PER_PAGE - 1) // self.ITEMS_PER_PAGE
        
        control_frame = ctk.CTkFrame(self.body_frame, fg_color="transparent")
        control_frame.pack(fill="x", pady=(5, 10))

        left_spacer = ctk.CTkFrame(control_frame, fg_color="transparent")
        left_spacer.pack(side="left", fill="x", expand=True)

        center_container = ctk.CTkFrame(control_frame, fg_color="transparent")
        center_container.pack(side="left")

        prev_state = "normal" if self.current_page > 0 else "disabled"
        btn_prev = ctk.CTkButton(
            center_container, text="◀ Prev", width=70, height=26, corner_radius=6,
            font=FONT_BODY_SMALL, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER, state=prev_state,
            command=lambda: self._change_page(-1, data, is_partial)
        )
        btn_prev.pack(side="left", padx=5)

        page_lbl = ctk.CTkLabel(
            center_container, text=f"Page {self.current_page + 1} of {total_pages}",
            font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB
        )
        page_lbl.pack(side="left", padx=15)

        next_state = "normal" if self.current_page < total_pages - 1 else "disabled"
        btn_next = ctk.CTkButton(
            center_container, text="Next ▶", width=70, height=26, corner_radius=6,
            font=FONT_BODY_SMALL, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER, state=next_state,
            command=lambda: self._change_page(1, data, is_partial)
        )
        btn_next.pack(side="left", padx=5)

        right_spacer = ctk.CTkFrame(control_frame, fg_color="transparent")
        right_spacer.pack(side="right", fill="x", expand=True)

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


class AppSigninsSubFrame(ctk.CTkFrame):
    """Sub-frame for Microsoft Entra App Sign Ins with UI Pagination."""

    def __init__(self, master, log_callback, credentials_callback, status_change_callback, semaphore=None, **kwargs):
        super().__init__(master, fg_color="transparent", **kwargs)
        self.log_msg = log_callback
        self.get_credentials = credentials_callback
        self.on_status_change = status_change_callback
        self.semaphore = semaphore
        self.status = None
        self.last_data = []
        self.is_cancelled = False
        self.current_request_id = 0

        # Pagination variables
        self.ITEMS_PER_PAGE = 10
        self.current_page = 0
        self.csv_path = None

        self.build_ui()

    def build_ui(self):
        self.header_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.header_frame.pack(fill="x", pady=(0, 5))

        ctk.CTkLabel(self.header_frame, text="App Sign Ins", font=FONT_SUBSECTION_HEADER, text_color=COLOR_PRIMARY).pack(side="left")

        self.btn_refresh = ctk.CTkButton(
            self.header_frame, state="disabled", text="↻ Reload", width=80, height=26, corner_radius=13,
            font=FONT_BODY_SMALL, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER,
            command=self.trigger_fetch_individual
        )
        self.btn_refresh.pack(side="right", padx=(10, 0))

        self.body_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.body_frame.pack(fill="x", expand=True)

        self.reset_view()

    def reset_view(self):
        self.status = None
        self.last_data = []
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
        if "401" in error_msg or "403" in error_msg or "unauthorized" in error_msg.lower() or "forbidden" in error_msg.lower():
            display_msg = "Reports.Read.All application permission required.\nPlease grant 'Reports.Read.All' to your App Registration in Microsoft Entra ID."
        ctk.CTkLabel(self.body_frame, text=f"✖ {display_msg}", text_color=COLOR_ERROR, font=FONT_BODY_MEDIUM, justify="center").pack(pady=10)

    def trigger_fetch_individual(self):
        tenant, clients, secrets = self.get_credentials()
        if tenant:
            self.trigger_fetch(tenant, clients[0], secrets[0])
        else:
            self._set_state_error("Missing credentials. Please submit the credentials above.")

    def trigger_fetch(self, tenant, client_id, client_secret):
        self.status = "loading"
        self.is_cancelled = False
        self.current_request_id += 1
        self.current_page = 0
        self.current_page = 0  # Reset to page 0

        script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
        reports_dir = os.path.join(script_dir, "reports", f"{tenant}_{client_id}")
        self.csv_path = os.path.join(reports_dir, "entra_app_signins.csv")

        self._set_state_loading("Downloading and parsing App Sign Ins...")
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

            client = GraphClient(
                tenant_id=tenant,
                client_ids=client_id,
                client_secrets=client_secret,
                concurrency=1,
                retries=5,
                backoff=2
            )
            client.authenticate()
            reports_service = ReportsService(client)

            app_signins = []
            page_count = 0
            def handle_page(value_list):
                nonlocal app_signins, page_count
                if self.is_cancelled or request_id != self.current_request_id:
                    return
                for item in value_list:
                    app_name = item.get("appDisplayName") or ""
                    success = str(item.get("successfulSignInCount") or 0)
                    app_signins.append((app_name, success))
                page_count += 1
                if page_count % 3 == 0:
                    self.after(0, self._render_partial, list(app_signins), request_id)

            with open(self.csv_path, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["appDisplayName", "successSignInCount"])

            reports_service.fetch_app_signin_summary(
                csv_path=self.csv_path,
                max_rows=5000,
                on_page_callback=handle_page,
                is_cancelled_callback=lambda: self.is_cancelled or request_id != self.current_request_id
            )
            client.close()

            if not self.is_cancelled and request_id == self.current_request_id:
                self.after(0, self._render_success, app_signins, request_id)
        except Exception as e:
            usage_logger.error(f"Error fetching app sign-ins: {e}", exc_info=True)
            if not self.is_cancelled and request_id == self.current_request_id:
                self.after(0, self._render_error, str(e), request_id)
        finally:
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
        self.last_data = data
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

        items = []
        total_count = 0
        try:
            with open(self.csv_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader, None)  # skip header
                all_rows = list(reader)
                total_count = len(all_rows)
                start_idx = page * self.ITEMS_PER_PAGE
                end_idx = start_idx + self.ITEMS_PER_PAGE
                items = all_rows[start_idx:end_idx]
        except Exception as e:
            usage_logger.error(f"Error reading CSV for pagination: {e}")
        return items, total_count

    def _update_ui_paginated(self, data=None, is_partial=False):
        for w in self.body_frame.winfo_children():
            w.destroy()

        if is_partial:
            progress_frame = ctk.CTkFrame(self.body_frame, fg_color=COLOR_TONAL_BG, height=26, corner_radius=6)
            progress_frame.pack(fill="x", pady=(0, 6))
            ctk.CTkLabel(
                progress_frame,
                text="⏳ Querying App Sign Ins in the background... UI will auto-refresh.",
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

        headers = ["App Name", "Successful Sign Ins"]
        for i in range(2):
            metrics_grid.grid_columnconfigure(i, weight=1)

        for col_idx, head_text in enumerate(headers):
            cell = ctk.CTkFrame(metrics_grid, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        if not page_data:
            c = ctk.CTkFrame(metrics_grid, fg_color=COLOR_SURFACE, corner_radius=0)
            c.grid(row=1, column=0, columnspan=2, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c, text="No app sign-ins detected.", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="center").pack(padx=10, pady=12)
        else:
            for r_idx, (app, success) in enumerate(page_data, start=1):
                bg_style = COLOR_SURFACE if r_idx % 2 != 0 else COLOR_SURFACE_VARIANT
                vals = [app, success]
                for c_idx, val in enumerate(vals):
                    c = ctk.CTkFrame(metrics_grid, fg_color=bg_style, corner_radius=0)
                    c.grid(row=r_idx, column=c_idx, sticky="nsew", padx=0, pady=(0, 1))
                    ctk.CTkLabel(c, text=val, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="left", wraplength=350).pack(padx=10, pady=8, anchor="nw")

        # Draw the pagination controls if we have items
        if total_count > 0:
            self._draw_pagination_controls(total_count, data, is_partial)

    def _draw_pagination_controls(self, total_count, data, is_partial):
        total_pages = (total_count + self.ITEMS_PER_PAGE - 1) // self.ITEMS_PER_PAGE
        if total_pages <= 1:
            return

        control_frame = ctk.CTkFrame(self.body_frame, fg_color="transparent")
        control_frame.pack(fill="x", pady=(5, 10))

        left_spacer = ctk.CTkFrame(control_frame, fg_color="transparent")
        left_spacer.pack(side="left", fill="x", expand=True)

        center_container = ctk.CTkFrame(control_frame, fg_color="transparent")
        center_container.pack(side="left")

        prev_state = "normal" if self.current_page > 0 else "disabled"
        btn_prev = ctk.CTkButton(
            center_container, text="◀ Prev", width=70, height=26, corner_radius=6,
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
            center_container, text="Next ▶", width=70, height=26, corner_radius=6,
            font=FONT_BODY_SMALL, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER,
            state=next_state,
            command=lambda: self._change_page(1, data, is_partial)
        )
        btn_next.pack(side="left", padx=5)

        right_spacer = ctk.CTkFrame(control_frame, fg_color="transparent")
        right_spacer.pack(side="right", fill="x", expand=True)

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


class UserSigninsSubFrame(ctk.CTkFrame):
    """Sub-frame for Microsoft Entra User Sign Ins."""

    def __init__(self, master, log_callback, credentials_callback, status_change_callback, semaphore=None, **kwargs):
        super().__init__(master, fg_color="transparent", **kwargs)
        self.log_msg = log_callback
        self.get_credentials = credentials_callback
        self.on_status_change = status_change_callback
        self.semaphore = semaphore
        self.status = None
        self.last_data = {
            "apps": [],
            "operating_systems": [],
            "browsers": []
        }
        self.is_cancelled = False
        self.current_request_id = 0

        self.build_ui()

    def build_ui(self):
        self.header_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.header_frame.pack(fill="x", pady=(0, 5))

        ctk.CTkLabel(self.header_frame, text="User Sign Ins", font=FONT_SUBSECTION_HEADER, text_color=COLOR_PRIMARY).pack(side="left")

        self.btn_refresh = ctk.CTkButton(
            self.header_frame, state="disabled", text="↻ Reload", width=80, height=26, corner_radius=13,
            font=FONT_BODY_SMALL, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER,
            command=self.trigger_fetch_individual
        )
        self.btn_refresh.pack(side="right", padx=(10, 0))

        self.body_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.body_frame.pack(fill="x", expand=True)

        self.reset_view()

    def reset_view(self):
        self.status = None
        self.last_data = {
            "apps": [],
            "operating_systems": [],
            "browsers": []
        }
        self.is_cancelled = False
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
        if "401" in error_msg or "403" in error_msg or "unauthorized" in error_msg.lower() or "forbidden" in error_msg.lower():
            display_msg = "AuditLog.Read.All application permission required.\nPlease grant 'AuditLog.Read.All' to your App Registration in Microsoft Entra ID."
        ctk.CTkLabel(self.body_frame, text=f"✖ {display_msg}", text_color=COLOR_ERROR, font=FONT_BODY_MEDIUM, justify="center").pack(pady=10)

    def trigger_fetch_individual(self):
        tenant, clients, secrets = self.get_credentials()
        if tenant:
            self.trigger_fetch(tenant, clients[0], secrets[0])
        else:
            self._set_state_error("Missing credentials. Please submit the credentials above.")

    def trigger_fetch(self, tenant, client_id, client_secret):
        self.status = "loading"
        self.is_cancelled = False
        self.current_request_id += 1
        self.current_page = 0
        self._set_state_loading("Downloading and parsing User Sign Ins...")
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

            script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
            reports_dir = os.path.join(script_dir, "reports", f"{tenant}_{client_id}")
            os.makedirs(reports_dir, exist_ok=True)
            csv_path_interactive = os.path.join(reports_dir, "signin_interactive.csv")
            csv_path_noninteractive = os.path.join(reports_dir, "signin_noninteractive.csv")

            for path in [csv_path_interactive, csv_path_noninteractive]:
                with open(path, 'w', encoding='utf-8', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(["appDisplayName", "operatingSystem", "browser", "signInEventType"])

            client = GraphClient(
                tenant_id=tenant,
                client_ids=client_id,
                client_secrets=client_secret,
                concurrency=2,
                retries=5,
                backoff=2
            )
            client.authenticate()
            security_service = SecurityService(client)

            unique_apps = set()
            unique_os = set()
            unique_browsers = set()
            page_count = 0
 
            def handle_signins_page(value_list):
                nonlocal page_count
                if self.is_cancelled or request_id != self.current_request_id:
                    return
                for log in value_list:
                    app = log.get("appDisplayName")
                    if app:
                        unique_apps.add(app)
                    device = log.get("deviceDetail")
                    if device:
                        os_name = device.get("operatingSystem")
                        if os_name:
                            unique_os.add(os_name)
                        browser = device.get("browser")
                        if browser:
                            unique_browsers.add(browser)
                
                page_count += 1
                if page_count % 3 == 0:
                    partial_data = {
                        "apps": sorted(list(unique_apps)),
                        "operating_systems": sorted(list(unique_os)),
                        "browsers": sorted(list(unique_browsers))
                    }
                    self.after(0, self._render_partial, partial_data, request_id)

            errors = []
            def run_fetch_signins(event_type, path):
                try:
                    security_service.fetch_signin_activities(
                        event_type=event_type,
                        csv_path=path,
                        max_rows=10000,
                        on_page_callback=handle_signins_page,
                        is_cancelled_callback=lambda: self.is_cancelled or request_id != self.current_request_id
                    )
                except Exception as thread_err:
                    usage_logger.error(f"Error fetching {event_type}: {thread_err}")
                    errors.append(thread_err)

            t1 = threading.Thread(target=run_fetch_signins, args=("nonInteractiveUser", csv_path_noninteractive), daemon=True)
            t2 = threading.Thread(target=run_fetch_signins, args=("interactiveUser", csv_path_interactive), daemon=True)
            t1.start()
            t2.start()
            t1.join()
            t2.join()

            client.close()

            if errors and len(errors) == 2:
                raise errors[0]

            if not self.is_cancelled and request_id == self.current_request_id:
                final_data = {
                    "apps": sorted(list(unique_apps)),
                    "operating_systems": sorted(list(unique_os)),
                    "browsers": sorted(list(unique_browsers))
                }
                self.after(0, self._render_success, final_data, request_id)
        except Exception as e:
            usage_logger.error(f"Error fetching user sign-ins: {e}", exc_info=True)
            if not self.is_cancelled and request_id == self.current_request_id:
                self.after(0, self._render_error, str(e), request_id)
        finally:
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
        self.last_data = data
        self._update_ui_paginated(data, is_partial=False)
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

    def _update_ui(self, data, is_partial=False):
        for w in self.body_frame.winfo_children():
            w.destroy()

        if is_partial:
            progress_frame = ctk.CTkFrame(self.body_frame, fg_color=COLOR_TONAL_BG, height=26, corner_radius=6)
            progress_frame.pack(fill="x", pady=(0, 6))
            ctk.CTkLabel(
                progress_frame,
                text="⏳ Querying User Sign Ins in the background... UI will auto-refresh.",
                font=FONT_BODY_SMALL,
                text_color=COLOR_TONAL_TEXT
            ).pack(padx=10, pady=2, anchor="w")

        rows_data = [
            ("📱 Apps Used", data.get("apps", []), "No apps found"),
            ("💻 Operating Systems", data.get("operating_systems", []), "No operating systems found"),
            ("🌐 Browsers Used", data.get("browsers", []), "No browsers found")
        ]

        for title, items, empty_msg in rows_data:
            row_frame = ctk.CTkFrame(self.body_frame, fg_color="transparent")
            row_frame.pack(fill="x", pady=4, anchor="w")

            lbl_title = ctk.CTkLabel(
                row_frame,
                text=f"{title}: ",
                font=FONT_BODY_BOLD,
                text_color=COLOR_TEXT_MAIN,
                anchor="w"
            )
            lbl_title.pack(side="left", anchor="nw")

            display_text = ", ".join(items) if items else empty_msg
            lbl_content = ctk.CTkLabel(
                row_frame,
                text=display_text,
                font=FONT_BODY_MEDIUM,
                text_color=COLOR_TEXT_MAIN if items else COLOR_TEXT_SUB,
                justify="left",
                anchor="w"
            )
            lbl_content.pack(side="left", fill="x", expand=True, anchor="nw")

            def make_configure_handler(lbl=lbl_content):
                def on_configure(event):
                    lbl.configure(wraplength=max(200, event.width - 180))
                return on_configure

            row_frame.bind("<Configure>", make_configure_handler())

    def cancel(self):
        self.is_cancelled = True
        self.current_request_id += 1
        if self.status == "loading":
            self.status = "cancelled"
            self._update_ui_paginated(self.last_data)
        if hasattr(self, "btn_refresh") and self.btn_refresh.winfo_exists():
            self.btn_refresh.configure(state="normal")


class DevicesAppsTelemetryFrame(ctk.CTkFrame):
    """Self-contained component wrapping Microsoft Entra Data UI with independent sub-sections."""

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

        ctk.CTkLabel(self.inner_pad, text="Microsoft Entra Data", font=FONT_HEADER_SMALL, text_color=COLOR_TEXT_MAIN).pack(anchor="w", pady=(0, 10))

        # 1. Authentication Methods at the top
        self.auth_methods_subframe = AuthMethodsSubFrame(
            self.inner_pad,
            log_callback=self.log_msg,
            credentials_callback=self.get_credentials,
            status_change_callback=self._subframe_status_changed,
            semaphore=self.semaphore
        )
        self.auth_methods_subframe.pack(fill="x", pady=(10, 15))

        # Divider 1
        self.divider1 = ctk.CTkFrame(self.inner_pad, fg_color=COLOR_OUTLINE_LIGHT, height=1)
        self.divider1.pack(fill="x", pady=15)

        # 2. App Sign Ins in the middle
        self.app_signins_subframe = AppSigninsSubFrame(
            self.inner_pad,
            log_callback=self.log_msg,
            credentials_callback=self.get_credentials,
            status_change_callback=self._subframe_status_changed,
            semaphore=self.semaphore
        )
        self.app_signins_subframe.pack(fill="x", pady=(0, 15))

        # Divider 2
        self.divider2 = ctk.CTkFrame(self.inner_pad, fg_color=COLOR_OUTLINE_LIGHT, height=1)
        self.divider2.pack(fill="x", pady=15)

        # 3. User Sign Ins at the bottom
        self.user_signins_subframe = UserSigninsSubFrame(
            self.inner_pad,
            log_callback=self.log_msg,
            credentials_callback=self.get_credentials,
            status_change_callback=self._subframe_status_changed,
            semaphore=self.semaphore
        )
        self.user_signins_subframe.pack(fill="x", pady=(0, 10))

    def _subframe_status_changed(self):
        statuses = [
            self.auth_methods_subframe.status,
            self.app_signins_subframe.status,
            self.user_signins_subframe.status
        ]
        if "loading" in statuses:
            self.status = "loading"
        elif "error" in statuses:
            self.status = "error"
        elif "success" in statuses:
            self.status = "success"
        else:
            self.status = None
        self.on_status_change()

    def reset_view(self):
        self.pack_forget()
        self.status = None
        self.auth_methods_subframe.reset_view()
        self.app_signins_subframe.reset_view()
        self.user_signins_subframe.reset_view()

    def trigger_fetch(self, tenant, client_id, client_secret):
        usage_logger.info("Entra Data trigger_fetch called. Propagating to independent sub-sections...")
        self.pack(fill="x", expand=True, pady=10)
        self.auth_methods_subframe.trigger_fetch(tenant, client_id, client_secret)
        self.app_signins_subframe.trigger_fetch(tenant, client_id, client_secret)
        self.user_signins_subframe.trigger_fetch(tenant, client_id, client_secret)

    def cancel(self):
        usage_logger.info("Entra Data cancel called. Propagating to independent sub-sections...")
        self.auth_methods_subframe.cancel()
        self.app_signins_subframe.cancel()
        self.user_signins_subframe.cancel()

    @property
    def last_data(self):
        return {
            "apps": self.user_signins_subframe.last_data.get("apps", []),
            "operating_systems": self.user_signins_subframe.last_data.get("operating_systems", []),
            "browsers": self.user_signins_subframe.last_data.get("browsers", []),
            "app_signins": self.app_signins_subframe.last_data,
            "auth_methods": self.auth_methods_subframe.last_data
        }


def run_devices_apps_pipeline(
    client_id: str, 
    client_secret: str, 
    tenant_id: str, 
    on_page_callback=None, 
    on_app_signins_page_callback=None,
    on_auth_methods_page_callback=None,
    is_cancelled_callback=None
) -> dict:
    """Pipeline to fetch interactive & non-interactive sign-in logs, app sign-in summaries, 
    and auth methods in parallel, then compile them from local CSV files."""
    usage_logger.info("Starting Microsoft Entra Data Telemetry Pipeline in parallel...")
    
    script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
    reports_dir = os.path.join(script_dir, "reports", f"{tenant_id}_{client_id}")
    os.makedirs(reports_dir, exist_ok=True)
    
    csv_path_interactive = os.path.join(reports_dir, "signin_interactive.csv")
    csv_path_noninteractive = os.path.join(reports_dir, "signin_noninteractive.csv")
    csv_path_app_signins = os.path.join(reports_dir, "entra_app_signins.csv")
    csv_path_auth_methods = os.path.join(reports_dir, "entra_auth_methods.csv")
    
    for path in [csv_path_interactive, csv_path_noninteractive]:
        with open(path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["appDisplayName", "operatingSystem", "browser", "signInEventType"])
            
    with open(csv_path_app_signins, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["appDisplayName", "successSignInCount"])
        
    with open(csv_path_auth_methods, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["authenticationMethod", "successActivityCount"])
            
    client = GraphClient(
        tenant_id=tenant_id,
        client_ids=client_id,
        client_secrets=client_secret,
        concurrency=4,
        retries=5,
        backoff=2
    )
    client.authenticate()
    security_service = SecurityService(client)
    reports_service = ReportsService(client)
    
    errors = []
    
    def run_fetch_signins(event_type, path):
        try:
            security_service.fetch_signin_activities(
                event_type=event_type,
                csv_path=path,
                max_rows=10000,
                on_page_callback=on_page_callback,
                is_cancelled_callback=is_cancelled_callback
            )
        except Exception as thread_err:
            usage_logger.error(f"Error in thread fetching {event_type}: {thread_err}")
            errors.append(thread_err)
            
    def run_fetch_app_signins(path):
        try:
            reports_service.fetch_app_signin_summary(
                csv_path=path,
                max_rows=5000,
                on_page_callback=on_app_signins_page_callback,
                is_cancelled_callback=is_cancelled_callback
            )
        except Exception as thread_err:
            usage_logger.error(f"Error in thread fetching app sign-ins: {thread_err}")
            errors.append(thread_err)

    def run_fetch_auth_methods(path):
        try:
            reports_service.fetch_auth_methods_summary(
                csv_path=path,
                max_rows=5000,
                on_page_callback=on_auth_methods_page_callback,
                is_cancelled_callback=is_cancelled_callback
            )
        except Exception as thread_err:
            usage_logger.error(f"Error in thread fetching auth methods: {thread_err}")
            errors.append(thread_err)
            
    try:
        t1 = threading.Thread(target=run_fetch_signins, args=("nonInteractiveUser", csv_path_noninteractive), daemon=True)
        t2 = threading.Thread(target=run_fetch_signins, args=("interactiveUser", csv_path_interactive), daemon=True)
        t3 = threading.Thread(target=run_fetch_app_signins, args=(csv_path_app_signins,), daemon=True)
        t4 = threading.Thread(target=run_fetch_auth_methods, args=(csv_path_auth_methods,), daemon=True)
        
        t1.start()
        t2.start()
        t3.start()
        t4.start()
        
        t1.join()
        t2.join()
        t3.join()
        t4.join()
        
        if len(errors) == 4:
            raise errors[0]
            
        unique_apps = set()
        unique_os = set()
        unique_browsers = set()
        app_signins = []
        auth_methods = []
        
        for path in [csv_path_noninteractive, csv_path_interactive]:
            if os.path.exists(path):
                with open(path, 'r', encoding='utf-8') as f:
                    reader = csv.reader(f)
                    next(reader, None)
                    for row in reader:
                        if len(row) >= 3:
                            app, os_name, browser = row[0], row[1], row[2]
                            if app:
                                unique_apps.add(app)
                            if os_name:
                                unique_os.add(os_name)
                            if browser:
                                unique_browsers.add(browser)
                                
        if os.path.exists(csv_path_app_signins):
            with open(csv_path_app_signins, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader, None)
                for row in reader:
                    if len(row) >= 2:
                        app_signins.append((row[0], row[1]))
                        
        if os.path.exists(csv_path_auth_methods):
            with open(csv_path_auth_methods, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader, None)
                for row in reader:
                    if len(row) >= 2:
                        auth_methods.append((row[0], row[1]))
                                
        return {
            "apps": sorted(list(unique_apps)),
            "operating_systems": sorted(list(unique_os)),
            "browsers": sorted(list(unique_browsers)),
            "app_signins": app_signins,
            "auth_methods": auth_methods
        }
    finally:
        client.close()

