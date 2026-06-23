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

"""UI frame for Microsoft Entra ID Provisioning Logs telemetry."""

import os
import csv
import logging
import threading
import webbrowser
from typing import Optional
import customtkinter as ctk

from core.graph.client import GraphClient
from core.graph.directory.provisioning_logs import ProvisioningLogsService
from telemetry.styles import *

usage_logger = logging.getLogger("M365TelemetryAsyncLogger.DirectoryProvisioningLogsUI")

class DirectoryProvisioningLogsFrame(ctk.CTkFrame):
    """Sub-frame showing Provisioning logs with pagination."""

    def __init__(self, master, log_callback, credentials_callback, status_change_callback, semaphore=None, **kwargs):
        super().__init__(master, fg_color="transparent", **kwargs)
        self.log_msg = log_callback
        self.get_credentials = credentials_callback
        self.on_status_change = status_change_callback
        self.semaphore = semaphore
        self.status = None
        self.is_cancelled = False
        self.current_request_id = 0
        
        self.ITEMS_PER_PAGE = 10
        self.current_page = 0
        self._cached_logs = []
        self.csv_path = None

        self.build_ui()

    def build_ui(self):
        self.header_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.header_frame.pack(fill="x", pady=(0, 5))

        self.title_lbl = ctk.CTkLabel(self.header_frame, text="Provisioning Logs", font=FONT_HEADER_SMALL, text_color=COLOR_TEXT_MAIN)
        self.title_lbl.pack(side="left")

        self.reference_link = ctk.CTkLabel(
            self.header_frame,
            text="Provisioning Logs API Reference ↗",
            font=FONT_BODY_SMALL_UNDERLINED,
            text_color=COLOR_PRIMARY,
            cursor="hand2"
        )
        self.reference_link.pack(side="left", padx=(15, 0))
        self.reference_link.bind("<Button-1>", lambda e: webbrowser.open("https://learn.microsoft.com/en-us/graph/api/resources/provisioningobjectsummary?view=graph-rest-1.0"))
        self.reference_link.bind("<Enter>", lambda e: self.reference_link.configure(text_color=COLOR_PRIMARY_HOVER))
        self.reference_link.bind("<Leave>", lambda e: self.reference_link.configure(text_color=COLOR_PRIMARY))

        self.btn_refresh = ctk.CTkButton(
            self.header_frame, state="disabled", text="↻ Reload", width=80, height=24,
            font=ctk.CTkFont(family="Segoe UI", size=12),
            fg_color="transparent", border_width=1, text_color="#2563EB", hover_color="#DBEAFE",
            command=self.trigger_fetch_individual
        )
        self.btn_refresh.pack(side="right")

        self.body_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.body_frame.pack(fill="x")

        self.reset_view()

    def reset_view(self):
        self.status = None
        self.is_cancelled = False
        self.csv_path = None
        self.current_page = 0
        self._cached_logs = []
        for w in self.body_frame.winfo_children():
            w.destroy()

    def _set_state_loading(self, msg="Loading..."):
        for w in self.body_frame.winfo_children():
            w.destroy()
        loading_lbl = ctk.CTkLabel(self.body_frame, text=f"⏳ {msg}", text_color="#6b7280", font=ctk.CTkFont(family="Segoe UI", size=13))
        loading_lbl.pack(pady=(15, 5))
        pb = ctk.CTkProgressBar(self.body_frame, mode="indeterminate", width=250, fg_color="#F3F4F6", progress_color="#2563EB")
        pb.pack(pady=(0, 15))
        pb.start()

    def _set_state_error(self, error_msg):
        for w in self.body_frame.winfo_children():
            w.destroy()
        display_msg = error_msg
        if "401" in error_msg or "403" in error_msg or "unauthorized" in error_msg.lower() or "forbidden" in error_msg.lower():
            display_msg = "AuditLog.Read.All application permission required.\nPlease grant the 'AuditLog.Read.All' permission to your App Registration in Entra ID."
        ctk.CTkLabel(self.body_frame, text=f"✖ {display_msg}", text_color=COLOR_ERROR, font=FONT_BODY_MEDIUM, justify="center").pack(pady=(15, 5))
        ctk.CTkButton(self.body_frame, text="Try Again", command=self.trigger_fetch_individual, width=120, fg_color="transparent", border_width=1, text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER).pack(pady=(0, 15))

    def trigger_fetch_individual(self):
        tenant, clients, secrets = self.get_credentials()
        if tenant:
            self.trigger_fetch(tenant, clients[0], secrets[0])
        else:
            self._set_state_error("Missing connection credentials.")

    def trigger_fetch(self, tenant, client_id, client_secret):
        self.status = "loading"
        self.is_cancelled = False
        self.current_page = 0
        
        script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
        if os.path.basename(script_dir) == "directory":
            script_dir = os.path.dirname(script_dir)
        reports_dir = os.path.join(script_dir, "reports", f"{tenant}_{client_id}")
        self.csv_path = os.path.join(reports_dir, "directory_provisioning_logs.csv")

        self._set_state_loading("Fetching Provisioning Audit Logs...")
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
            
            prov_service = ProvisioningLogsService(client)
            
            provisioning_logs = []
            def handle_provisioning_page(page_rows):
                provisioning_logs.extend(page_rows)

            prov_service.fetch_provisioning_logs(
                csv_path=self.csv_path,
                max_rows=200,
                on_page_callback=handle_provisioning_page,
                is_cancelled_callback=lambda: self.is_cancelled or request_id != self.current_request_id
            )
            client.close()

            if self.is_cancelled or request_id != self.current_request_id:
                return

            self.after(0, self._render_success, provisioning_logs, request_id)
        except Exception as e:
            usage_logger.error(f"Error fetching Provisioning logs: {e}", exc_info=True)
            if not self.is_cancelled and request_id == self.current_request_id:
                self.after(0, self._render_error, str(e), request_id)
        finally:
            if self.semaphore:
                self.semaphore.release()

    def _render_success(self, logs, request_id):
        if self.is_cancelled or request_id != self.current_request_id:
            return
        self.status = "success"
        self._cached_logs = logs
        self._update_provisioning_ui_paginated()
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

    def _load_provisioning_page_from_csv(self, page):
        if not self.csv_path or not os.path.exists(self.csv_path):
            return [], 0

        logs = []
        try:
            with open(self.csv_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader, None)  # skip header
                for row in reader:
                    if row:
                        logs.append({
                            "initiatedBy": row[0],
                            "provisioningAction": row[1],
                            "provisioningSteps": row[2],
                            "servicePrincipal": row[3],
                            "sourceSystem": row[4],
                            "targetSystem": row[5],
                            "tenantId": row[6],
                            "provisioningStatusInfo": row[7]
                        })
        except Exception as e:
            usage_logger.error(f"Error reading CSV for Provisioning logs pagination: {e}")
            
        total_count = len(logs)
        start_idx = page * self.ITEMS_PER_PAGE
        end_idx = start_idx + self.ITEMS_PER_PAGE
        page_data = logs[start_idx:end_idx]
        
        return page_data, total_count

    def _update_provisioning_ui_paginated(self):
        for w in self.body_frame.winfo_children():
            w.destroy()

        provisioning_grid = ctk.CTkFrame(self.body_frame, fg_color=COLOR_OUTLINE_LIGHT, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)
        provisioning_grid.pack(fill="x", expand=True, pady=(5, 10))

        provisioning_grid.grid_columnconfigure(0, weight=2)
        provisioning_grid.grid_columnconfigure(1, weight=2)
        provisioning_grid.grid_columnconfigure(2, weight=3)
        provisioning_grid.grid_columnconfigure(3, weight=2)
        provisioning_grid.grid_columnconfigure(4, weight=1)
        provisioning_grid.grid_columnconfigure(5, weight=1)
        provisioning_grid.grid_columnconfigure(6, weight=1)
        provisioning_grid.grid_columnconfigure(7, weight=2)

        headers = ["Initiated By", "Action", "Steps", "Service Principal", "Source System", "Target System", "Tenant ID", "Status Info"]
        for col_idx, head_text in enumerate(headers):
            cell = ctk.CTkFrame(provisioning_grid, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=8, pady=8, anchor="w")

        page_data, total_count = self._load_provisioning_page_from_csv(self.current_page)

        if not page_data:
            empty_cell = ctk.CTkFrame(provisioning_grid, fg_color="transparent")
            empty_cell.grid(row=1, column=0, columnspan=8, sticky="nsew", pady=15)
            ctk.CTkLabel(empty_cell, text="No provisioning logs found.", text_color=COLOR_TEXT_SUB).pack()
        elif page_data[0].get("initiatedBy") == "ERROR":
            err_msg = page_data[0].get("provisioningAction")
            error_cell = ctk.CTkFrame(provisioning_grid, fg_color="transparent")
            error_cell.grid(row=1, column=0, columnspan=8, sticky="nsew", pady=15)
            ctk.CTkLabel(error_cell, text=f"⚠️ {err_msg}", font=FONT_BODY_MEDIUM, text_color="#DC2626", justify="left", wraplength=1000).pack(padx=10, pady=5)
        else:
            for item_idx, log in enumerate(page_data, start=1):
                bg_style = COLOR_SURFACE if item_idx % 2 == 0 else COLOR_SURFACE_VARIANT

                vals = [
                    log.get("initiatedBy", "-"),
                    log.get("provisioningAction", "-"),
                    log.get("provisioningSteps", "-"),
                    log.get("servicePrincipal", "-"),
                    log.get("sourceSystem", "-"),
                    log.get("targetSystem", "-"),
                    log.get("tenantId", "-"),
                    log.get("provisioningStatusInfo", "-")
                ]

                for col_idx, val in enumerate(vals):
                    c = ctk.CTkFrame(provisioning_grid, fg_color=bg_style, corner_radius=0)
                    c.grid(row=item_idx, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
                    
                    wraplen = 220 if col_idx in [0, 1, 2, 3, 7] else 100
                    ctk.CTkLabel(c, text=val, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="left", wraplength=wraplen).pack(padx=8, pady=8, anchor="nw")

        # Draw pagination controls if we have multiple pages
        if total_count > 0:
            self._draw_pagination_controls(total_count)

        provisioning_footnote = ctk.CTkLabel(
            self.body_frame,
            text="* Based on sampled data collected from audit logs.",
            font=FONT_BODY_SMALL,
            text_color=COLOR_TEXT_SUB,
            anchor="w",
            justify="left",
            wraplength=1100
        )
        provisioning_footnote.pack(fill="x", padx=10, pady=(0, 5))

    def _draw_pagination_controls(self, total_count):
        total_pages = (total_count + self.ITEMS_PER_PAGE - 1) // self.ITEMS_PER_PAGE
        if total_pages <= 1:
            return

        pagination_frame = ctk.CTkFrame(self.body_frame, fg_color="transparent")
        pagination_frame.pack(fill="x", pady=(2, 5))

        left_spacer = ctk.CTkFrame(pagination_frame, fg_color="transparent")
        left_spacer.pack(side="left", fill="x", expand=True)

        center_container = ctk.CTkFrame(pagination_frame, fg_color="transparent")
        center_container.pack(side="left")

        prev_state = "normal" if self.current_page > 0 else "disabled"
        btn_prev = ctk.CTkButton(
            center_container, text="◀ Prev", width=70, height=22, corner_radius=6,
            font=FONT_BODY_SMALL, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER,
            state=prev_state,
            command=lambda: self._change_page(-1)
        )
        btn_prev.pack(side="left", padx=5)

        page_lbl = ctk.CTkLabel(
            center_container,
            text=f"Page {self.current_page + 1} of {total_pages} ({total_count} logs)",
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
            command=lambda: self._change_page(1)
        )
        btn_next.pack(side="left", padx=5)

        right_spacer = ctk.CTkFrame(pagination_frame, fg_color="transparent")
        right_spacer.pack(side="right", fill="x", expand=True)

    def _change_page(self, delta):
        self.current_page += delta
        self._update_provisioning_ui_paginated()

    def cancel(self):
        self.is_cancelled = True
        self.current_request_id += 1
        if self.status == "loading":
            self.status = "cancelled"
            self._update_provisioning_ui_paginated()
        if hasattr(self, "btn_refresh") and self.btn_refresh.winfo_exists():
            self.btn_refresh.configure(state="normal")

    @property
    def last_data(self):
        if hasattr(self, "_cached_logs") and self._cached_logs:
            return self._cached_logs
        page_data, _ = self._load_provisioning_page_from_csv(0)
        return page_data
