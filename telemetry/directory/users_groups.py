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

"""UI frame for Microsoft Entra ID Users & Groups counts telemetry."""

import os
import csv
import logging
import threading
from typing import Optional
import customtkinter as ctk

from core.graph.client import GraphClient
from core.graph.directory.users_groups import UsersGroupsService
from telemetry.styles import *

usage_logger = logging.getLogger("M365TelemetryAsyncLogger.DirectoryUsersGroupsUI")

class DirectoryUsersGroupsFrame(ctk.CTkFrame):
    """Sub-frame showing Users & Groups counts table."""

    def __init__(self, master, log_callback, credentials_callback, status_change_callback, semaphore=None, **kwargs):
        super().__init__(master, fg_color="transparent", **kwargs)
        self.log_msg = log_callback
        self.get_credentials = credentials_callback
        self.on_status_change = status_change_callback
        self.semaphore = semaphore
        self.status = None
        self.is_cancelled = False
        self.current_request_id = 0
        
        self.last_group_counts = {}
        self.last_user_counts = {}
        self.csv_path = None

        self.build_ui()

    def build_ui(self):
        self.header_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.header_frame.pack(fill="x", pady=(0, 5))

        self.title_lbl = ctk.CTkLabel(self.header_frame, text="Groups & Users", font=FONT_HEADER_SMALL, text_color=COLOR_TEXT_MAIN)
        self.title_lbl.pack(side="left")

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
        self.last_group_counts = {}
        self.last_user_counts = {}
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
            display_msg = "Directory read permissions required.\nPlease grant the 'Directory.Read.All' permission to your App Registration in Entra ID."
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
        
        script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
        if os.path.basename(script_dir) == "directory":
            script_dir = os.path.dirname(script_dir)
        reports_dir = os.path.join(script_dir, "reports", f"{tenant}_{client_id}")
        self.csv_path = os.path.join(reports_dir, "directory_users_groups.csv")

        self._set_state_loading("Fetching Users & Groups Counts...")
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
            client.authenticate(required_scopes=["Directory.Read.All"])
            
            ug_service = UsersGroupsService(client)
            counts_dict = ug_service.get_users_groups_counts(self.log_msg)
            client.close()

            if self.is_cancelled or request_id != self.current_request_id:
                return

            # Write Users/Groups CSV
            script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
            if os.path.basename(script_dir) == "directory":
                script_dir = os.path.dirname(script_dir)
            reports_dir = os.path.join(script_dir, "reports", f"{tenant}_{client_id}")
            os.makedirs(reports_dir, exist_ok=True)
            
            user_c = counts_dict.get("user_counts", {})
            group_c = counts_dict.get("group_counts", {})
            
            rows = [
                ("Total Users", user_c.get("total", 0)),
                ("Enabled Users", user_c.get("enabled", 0)),
                ("Disabled Users", user_c.get("disabled", 0)),
                ("Member Users", user_c.get("member", 0)),
                ("Guest Users", user_c.get("guest", 0)),
                ("Total Groups", group_c.get("total", 0)),
                ("Microsoft 365 Groups (Unified)", group_c.get("m365", 0)),
                ("Security Groups (Static, non-mail-enabled)", group_c.get("security", 0)),
                ("Mail-enabled Security Groups", group_c.get("mail_enabled_security", 0)),
                ("Distribution Groups", group_c.get("distribution", 0)),
                ("Dynamic Groups (Dynamic Membership)", group_c.get("dynamic", 0)),
            ]
            
            with open(self.csv_path, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["Category", "Count"])
                for cat, count in rows:
                    writer.writerow([cat, count])

            self.after(0, self._render_success, user_c, group_c, request_id)
        except Exception as e:
            usage_logger.error(f"Error fetching Users & Groups: {e}", exc_info=True)
            if not self.is_cancelled and request_id == self.current_request_id:
                self.after(0, self._render_error, str(e), request_id)
        finally:
            if self.semaphore:
                self.semaphore.release()

    def _render_success(self, user_c, group_c, request_id):
        if self.is_cancelled or request_id != self.current_request_id:
            return
        self.status = "success"
        self.last_user_counts = user_c
        self.last_group_counts = group_c
        self._update_ui()
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

    def _update_ui(self):
        for w in self.body_frame.winfo_children():
            w.destroy()

        groups_users_grid = ctk.CTkFrame(self.body_frame, fg_color=COLOR_OUTLINE_LIGHT, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)
        groups_users_grid.pack(fill="x", expand=True, pady=(5, 10))

        groups_users_grid.grid_columnconfigure(0, weight=3)
        groups_users_grid.grid_columnconfigure(1, weight=1)

        groups_users_headers = ["Category", "Count"]
        for col_idx, head_text in enumerate(groups_users_headers):
            cell = ctk.CTkFrame(groups_users_grid, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        rows_data = [
            ("Total Users", self.last_user_counts.get("total", 0), True),
            ("Enabled Users", self.last_user_counts.get("enabled", 0), False),
            ("Disabled Users", self.last_user_counts.get("disabled", 0), False),
            ("Member Users", self.last_user_counts.get("member", 0), False),
            ("Guest Users", self.last_user_counts.get("guest", 0), False),
            (None, None, False),
            ("Total Groups", self.last_group_counts.get("total", 0), True),
            ("Microsoft 365 Groups (Unified)", self.last_group_counts.get("m365", 0), False),
            ("Security Groups (Static, non-mail-enabled)", self.last_group_counts.get("security", 0), False),
            ("Mail-enabled Security Groups", self.last_group_counts.get("mail_enabled_security", 0), False),
            ("Distribution Groups", self.last_group_counts.get("distribution", 0), False),
            ("Dynamic Groups (Dynamic Membership)", self.last_group_counts.get("dynamic", 0), False)
        ]

        current_row = 1
        for item in rows_data:
            metric_name, val, is_bold = item
            if metric_name is None:
                for c_idx in range(2):
                    c = ctk.CTkFrame(groups_users_grid, fg_color=COLOR_OUTLINE_LIGHT, corner_radius=0, height=2)
                    c.grid(row=current_row, column=c_idx, sticky="nsew", padx=0, pady=(0, 1))
                current_row += 1
                continue

            bg_style = COLOR_SURFACE if current_row % 2 == 0 else COLOR_SURFACE_VARIANT
            font_style = FONT_BODY_BOLD if is_bold else FONT_BODY_MEDIUM

            c0 = ctk.CTkFrame(groups_users_grid, fg_color=bg_style, corner_radius=0)
            c0.grid(row=current_row, column=0, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c0, text=metric_name, font=font_style, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=8, anchor="nw")

            c1 = ctk.CTkFrame(groups_users_grid, fg_color=bg_style, corner_radius=0)
            c1.grid(row=current_row, column=1, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c1, text=f"{val:,}", font=font_style, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=8, anchor="nw")

            current_row += 1

    def cancel(self):
        self.is_cancelled = True
        self.current_request_id += 1
        if self.status == "loading":
            self.status = "cancelled"
            self._update_ui()
        if hasattr(self, "btn_refresh") and self.btn_refresh.winfo_exists():
            self.btn_refresh.configure(state="normal")

    @property
    def last_data(self):
        # We can reconstruct group_counts and user_counts from self.last_group_counts / self.last_user_counts,
        # or load from CSV if empty
        if self.last_group_counts or self.last_user_counts:
            return {"group_counts": self.last_group_counts, "user_counts": self.last_user_counts}
        
        # Load from CSV fallback
        if not self.csv_path or not os.path.exists(self.csv_path):
            return {"group_counts": {}, "user_counts": {}}
            
        group_c = {}
        user_c = {}
        
        try:
            with open(self.csv_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader, None)
                for row in reader:
                    if len(row) >= 2:
                        cat, val_str = row[0], row[1]
                        val = int(val_str)
                        if cat == "Total Users": user_c["total"] = val
                        elif cat == "Enabled Users": user_c["enabled"] = val
                        elif cat == "Disabled Users": user_c["disabled"] = val
                        elif cat == "Member Users": user_c["member"] = val
                        elif cat == "Guest Users": user_c["guest"] = val
                        elif cat == "Total Groups": group_c["total"] = val
                        elif cat == "Microsoft 365 Groups (Unified)": group_c["m365"] = val
                        elif cat == "Security Groups (Static, non-mail-enabled)": group_c["security"] = val
                        elif cat == "Mail-enabled Security Groups": group_c["mail_enabled_security"] = val
                        elif cat == "Distribution Groups": group_c["distribution"] = val
                        elif cat == "Dynamic Groups (Dynamic Membership)": group_c["dynamic"] = val
        except Exception:
            pass
            
        return {"group_counts": group_c, "user_counts": user_c}
