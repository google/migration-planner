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

"""Modular Directory Domains, Users & Groups summary telemetry scanners and visual interfaces."""

import logging
import threading
from typing import Any, Dict, List, Optional
import customtkinter as ctk

# Import unified core service layer
from core.graph.client import GraphClient
from core.graph.directory import DirectoryService

# Bind to the async logger initialized in m365_telemetry.py
usage_logger = logging.getLogger("M365TelemetryAsyncLogger")

# Import shared styles
from telemetry.styles import *

class DirectoryFrame(ctk.CTkFrame):
    """Self-contained customtkinter component wrapping Directory summary (e.g. Domains, Users & Groups) UI."""
    
    def __init__(self, master, log_callback, credentials_callback, status_change_callback, retries_var=None, backoff_var=None, **kwargs):
        self.semaphore = kwargs.pop("concurrency_semaphore", None)
        super().__init__(master, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=12, **kwargs)
        self.log_msg = log_callback
        self.get_credentials = credentials_callback
        self.on_status_change = status_change_callback
        self.retries = retries_var
        self.backoff = backoff_var
        self.status = None  # 'loading', 'success', 'error', None
        self.last_group_counts = {}
        self.last_user_counts = {}
        self.last_domains = []
        
        self.build_ui()

    def build_ui(self):
        """Creates card container for the tab."""
        self.pack(fill="x", expand=True, pady=10)
        
        self.inner_pad = ctk.CTkFrame(self, fg_color="transparent")
        self.inner_pad.pack(fill="both", expand=True, padx=20, pady=20)
        
        # Uber Title Heading
        ctk.CTkLabel(
            self.inner_pad,
            text="Directory Summary",
            font=FONT_HEADER_SMALL,
            text_color=COLOR_TEXT_MAIN
        ).pack(anchor="w", pady=(0, 5))
        
        self.state_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        
        # Domains sub-heading & grid
        self.domains_title = ctk.CTkLabel(
            self.inner_pad,
            text="Domains",
            font=FONT_HEADER_SMALL,
            text_color=COLOR_TEXT_MAIN
        )
        self.domains_grid = ctk.CTkFrame(self.inner_pad, fg_color=COLOR_OUTLINE_LIGHT, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)
        
        # Divider between sections
        self.divider = ctk.CTkFrame(self.inner_pad, height=1, fg_color=COLOR_OUTLINE_LIGHT)
        
        # Groups & Users sub-heading & grid
        self.groups_users_title = ctk.CTkLabel(
            self.inner_pad,
            text="Groups & Users",
            font=FONT_HEADER_SMALL,
            text_color=COLOR_TEXT_MAIN
        )
        self.groups_users_grid = ctk.CTkFrame(self.inner_pad, fg_color=COLOR_OUTLINE_LIGHT, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)
        
        self.reset_view()

    def reset_view(self):
        """Resets and hides grids."""
        self.pack_forget()
        self.state_frame.pack_forget()
        self.domains_title.pack_forget()
        self.domains_grid.pack_forget()
        self.divider.pack_forget()
        self.groups_users_title.pack_forget()
        self.groups_users_grid.pack_forget()
        self.last_group_counts = {}
        self.last_user_counts = {}
        self.last_domains = []
        
        for w in self.state_frame.winfo_children():
            w.destroy()
        for w in self.domains_grid.winfo_children():
            w.destroy()
        for w in self.groups_users_grid.winfo_children():
            w.destroy()

    def _set_state_loading(self, msg="Loading..."):
        for w in self.state_frame.winfo_children():
            w.destroy()
        ctk.CTkLabel(self.state_frame, text=f"⏳ {msg}", text_color=COLOR_TEXT_SUB, font=FONT_BODY_MEDIUM).pack(pady=20)
        self.state_frame.pack(fill="x", expand=True)

    def _set_state_error(self, error_msg):
        for w in self.state_frame.winfo_children():
            w.destroy()

        display_msg = error_msg
        if "401" in error_msg or "403" in error_msg or "unauthorized" in error_msg.lower() or "forbidden" in error_msg.lower():
            display_msg = "Directory read permissions required.\nPlease grant the 'Directory.Read.All' permission to your App Registration in Entra ID."

        ctk.CTkLabel(self.state_frame, text=f"✖ {display_msg}", text_color=COLOR_ERROR, font=FONT_BODY_MEDIUM, justify="center").pack(pady=(20, 10))
        ctk.CTkButton(self.state_frame, text="Try Again", command=self._retry_fetch, width=120, fg_color="transparent", border_width=1, text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER).pack(pady=(0, 20))
        self.state_frame.pack(fill="x", expand=True)

    def _retry_fetch(self):
        tenant, clients, secrets = self.get_credentials()
        if tenant:
            self.trigger_fetch(tenant, clients[0], secrets[0])

    def trigger_fetch(self, tenant, client_id, client_secret):
        """Triggers Directory Telemetry fetch inside background thread."""
        usage_logger.info("Directory telemetry trigger_fetch called. Spawning background worker thread...")
        self.status = "loading"
        self.on_status_change()
        
        self.pack(fill="x", expand=True, pady=10)
        self.domains_grid.pack_forget()
        self.divider.pack_forget()
        self.groups_users_grid.pack_forget()
        
        self._set_state_loading("Fetching directory domains, users, and group counts...")
        
        retries_val = self.retries.get() if self.retries else 5
        backoff_val = self.backoff.get() if self.backoff else 2
        
        threading.Thread(
            target=self._execute_worker,
            args=(tenant, client_id, client_secret, retries_val, backoff_val),
            daemon=True
        ).start()

    def _execute_worker(self, tenant: str, client_id: str, client_secret: str, retries_val: int, backoff_val: int):
        usage_logger.info("Executing thread: _execute_directory_worker")
        if self.semaphore:
            self.semaphore.acquire()
        try:
            self.log_msg("Authenticating app for directory query...")
            
            client = GraphClient(
                tenant_id=tenant,
                client_ids=client_id,
                client_secrets=client_secret,
                concurrency=1,
                retries=retries_val,
                backoff=backoff_val
            )
            
            required_scopes = ["Directory.Read.All"]
            client.authenticate(required_scopes=required_scopes)
            
            self.log_msg("Querying directory domains, users, and group counts from Microsoft Graph...")
            dir_service = DirectoryService(client)
            telemetry_data = dir_service.get_directory_telemetry(self.log_msg)
            client.close()
            
            usage_logger.info("Successfully fetched directory telemetry data.")
            self.after(0, self._render_success, telemetry_data)
        except Exception as e:
            usage_logger.error("Exception caught in Directory worker.", exc_info=True)
            self.after(0, self._render_error, str(e))
        finally:
            if self.semaphore:
                self.semaphore.release()

    def _render_success(self, telemetry_dict: Dict[str, Any]):
        usage_logger.info("Executing UI render for Directory domains, users & groups tables.")
        self.state_frame.pack_forget()
        
        for w in self.domains_grid.winfo_children():
            w.destroy()
        for w in self.groups_users_grid.winfo_children():
            w.destroy()

        # Display UI titles and tables
        self.domains_title.pack(anchor="w", pady=(10, 10))
        self.domains_grid.pack(fill="x", expand=True, pady=(0, 10))
        
        self.divider.pack(fill="x", pady=15)
        
        self.groups_users_title.pack(anchor="w", pady=(10, 10))
        self.groups_users_grid.pack(fill="x", expand=True, pady=(0, 10))

        # ----------------------------------------------------
        # RENDER DOMAINS GRID
        # ----------------------------------------------------
        self.domains_grid.grid_columnconfigure((0, 4), weight=3)
        self.domains_grid.grid_columnconfigure((1, 2, 3), weight=2)

        domains_headers = ["Domain ID", "Admin Managed", "Default", "Verified", "Supported Services"]
        for col_idx, head_text in enumerate(domains_headers):
            cell = ctk.CTkFrame(self.domains_grid, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        domains_list = telemetry_dict.get("domains", [])
        self.last_domains = domains_list

        if not domains_list:
            empty_cell = ctk.CTkFrame(self.domains_grid, fg_color="transparent")
            empty_cell.grid(row=1, column=0, columnspan=5, sticky="nsew", pady=15)
            ctk.CTkLabel(empty_cell, text="No domains found under the organization.", text_color=COLOR_TEXT_SUB).pack()
        else:
            for item_idx, domain in enumerate(domains_list, start=1):
                bg_style = COLOR_SURFACE if item_idx % 2 == 0 else COLOR_SURFACE_VARIANT

                admin_managed = "Yes" if domain.get("isAdminManaged") else "No"
                is_default = "Yes" if domain.get("isDefault") else "No"
                is_verified = "Yes" if domain.get("isVerified") else "No"
                services = domain.get("supportedServices", [])
                services_str = ", ".join(services) if services else "-"

                # Domain ID
                c0 = ctk.CTkFrame(self.domains_grid, fg_color=bg_style, corner_radius=0)
                c0.grid(row=item_idx, column=0, sticky="nsew", padx=0, pady=(0, 1))
                ctk.CTkLabel(c0, text=domain.get("id", "-"), font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=8, anchor="nw")

                # Admin Managed
                c1 = ctk.CTkFrame(self.domains_grid, fg_color=bg_style, corner_radius=0)
                c1.grid(row=item_idx, column=1, sticky="nsew", padx=0, pady=(0, 1))
                ctk.CTkLabel(c1, text=admin_managed, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=8, anchor="nw")

                # Default
                c2 = ctk.CTkFrame(self.domains_grid, fg_color=bg_style, corner_radius=0)
                c2.grid(row=item_idx, column=2, sticky="nsew", padx=0, pady=(0, 1))
                ctk.CTkLabel(c2, text=is_default, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=8, anchor="nw")

                # Verified
                c3 = ctk.CTkFrame(self.domains_grid, fg_color=bg_style, corner_radius=0)
                c3.grid(row=item_idx, column=3, sticky="nsew", padx=0, pady=(0, 1))
                ctk.CTkLabel(c3, text=is_verified, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=8, anchor="nw")

                # Supported Services
                c4 = ctk.CTkFrame(self.domains_grid, fg_color=bg_style, corner_radius=0)
                c4.grid(row=item_idx, column=4, sticky="nsew", padx=0, pady=(0, 1))
                ctk.CTkLabel(c4, text=services_str, text_color=COLOR_TEXT_MAIN, justify="left", wraplength=250).pack(padx=10, pady=8, anchor="nw")

        # ----------------------------------------------------
        # RENDER GROUPS & USERS GRID
        # ----------------------------------------------------
        self.groups_users_grid.grid_columnconfigure(0, weight=3)
        self.groups_users_grid.grid_columnconfigure(1, weight=1)

        groups_users_headers = ["Category", "Count"]
        for col_idx, head_text in enumerate(groups_users_headers):
            cell = ctk.CTkFrame(self.groups_users_grid, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        counts_dict = telemetry_dict.get("group_counts", {})
        self.last_group_counts = counts_dict

        user_counts_dict = telemetry_dict.get("user_counts", {})
        self.last_user_counts = user_counts_dict

        rows_data = [
            # User statistics
            ("Total Users", user_counts_dict.get("total", 0), True),
            ("Enabled Users", user_counts_dict.get("enabled", 0), False),
            ("Disabled Users", user_counts_dict.get("disabled", 0), False),
            ("Member Users", user_counts_dict.get("member", 0), False),
            ("Guest Users", user_counts_dict.get("guest", 0), False),
            # Divider separator row
            (None, None, False),
            # Group statistics
            ("Total Groups", counts_dict.get("total", 0), True),
            ("Microsoft 365 Groups (Unified)", counts_dict.get("m365", 0), False),
            ("Security Groups (Static, non-mail-enabled)", counts_dict.get("security", 0), False),
            ("Mail-enabled Security Groups", counts_dict.get("mail_enabled_security", 0), False),
            ("Distribution Groups", counts_dict.get("distribution", 0), False),
            ("Dynamic Groups (Dynamic Membership)", counts_dict.get("dynamic", 0), False)
        ]

        current_row = 1
        for item in rows_data:
            metric_name, val, is_bold = item
            if metric_name is None:
                for c_idx in range(2):
                    c = ctk.CTkFrame(self.groups_users_grid, fg_color=COLOR_OUTLINE_LIGHT, corner_radius=0, height=2)
                    c.grid(row=current_row, column=c_idx, sticky="nsew", padx=0, pady=(0, 1))
                current_row += 1
                continue

            bg_style = COLOR_SURFACE if current_row % 2 == 0 else COLOR_SURFACE_VARIANT
            font_style = FONT_BODY_BOLD if is_bold else FONT_BODY_MEDIUM

            c0 = ctk.CTkFrame(self.groups_users_grid, fg_color=bg_style, corner_radius=0)
            c0.grid(row=current_row, column=0, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c0, text=metric_name, font=font_style, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=8, anchor="nw")

            c1 = ctk.CTkFrame(self.groups_users_grid, fg_color=bg_style, corner_radius=0)
            c1.grid(row=current_row, column=1, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c1, text=f"{val:,}", font=font_style, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=8, anchor="nw")

            current_row += 1

        self.status = "success"
        self.on_status_change()

    def _render_error(self, err_msg):
        usage_logger.warning(f"Rendering Directory error state: {err_msg}")
        self._set_state_error(err_msg)
        self.status = "error"
        self.on_status_change()
