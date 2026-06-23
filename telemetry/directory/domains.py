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

"""UI frame for Microsoft Entra ID Domains telemetry."""

import os
import csv
import logging
import threading
import asyncio
import webbrowser
from typing import Optional
import customtkinter as ctk

from core.graph.client import GraphClient
from core.graph.directory.domains import DomainsService
from core.graph.db import import_csv_to_sqlite, query_page_sync
from telemetry.styles import *

usage_logger = logging.getLogger("M365TelemetryAsyncLogger.DirectoryDomainsUI")

class DirectoryDomainsFrame(ctk.CTkFrame):
    """Sub-frame showing Domains list with pagination."""

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
        self._cached_domains = []
        self.csv_path = None

        self.build_ui()

    def build_ui(self):
        self.header_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.header_frame.pack(fill="x", pady=(0, 5))

        self.title_lbl = ctk.CTkLabel(self.header_frame, text="Domains", font=FONT_HEADER_SMALL, text_color=COLOR_TEXT_MAIN)
        self.title_lbl.pack(side="left")

        self.reference_link = ctk.CTkLabel(
            self.header_frame,
            text="Domain API Reference ↗",
            font=FONT_BODY_BOLD,
            text_color=COLOR_PRIMARY,
            cursor="hand2"
        )
        self.reference_link.pack(side="left", padx=(15, 0))
        self.reference_link.bind("<Button-1>", lambda e: webbrowser.open("https://learn.microsoft.com/en-us/graph/api/resources/domain?view=graph-rest-1.0#properties"))
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
        self._cached_domains = []
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
        self.current_page = 0
        
        script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
        if os.path.basename(script_dir) == "directory":
            script_dir = os.path.dirname(script_dir)
        reports_dir = os.path.join(script_dir, "reports", f"{tenant}_{client_id}")
        self.csv_path = os.path.join(reports_dir, "directory_domains.csv")

        self._set_state_loading("Fetching directory domains list...")
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
            
            domains_service = DomainsService(client)
            domains_list = domains_service.get_domains(self.log_msg)
            client.close()

            if self.is_cancelled or request_id != self.current_request_id:
                return

            # Write Domains CSV
            script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
            if os.path.basename(script_dir) == "directory":
                script_dir = os.path.dirname(script_dir)
            reports_dir = os.path.join(script_dir, "reports", f"{tenant}_{client_id}")
            os.makedirs(reports_dir, exist_ok=True)
            
            headers = ["Domain ID", "Authentication Type", "Admin Managed", "Default", "Verified", "Supported Services", "Federation Display Name", "Federation Issuer URI"]
            rows = []
            for domain in domains_list:
                auth_type = domain.get("authenticationType", "N/A") or "N/A"
                admin_managed = "Yes" if domain.get("isAdminManaged") else "No"
                is_default = "Yes" if domain.get("isDefault") else "No"
                is_verified = "Yes" if domain.get("isVerified") else "No"
                services = domain.get("supportedServices", [])
                services_str = ", ".join(services) if services else "-"
                fed_idp = domain.get("federationDisplayName") or "-"
                fed_issuer = domain.get("federationIssuerUri") or "-"
                rows.append([domain.get("id", "-"), auth_type, admin_managed, is_default, is_verified, services_str, fed_idp, fed_issuer])

            with open(self.csv_path, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                writer.writerows(rows)

            db_path = os.path.join(reports_dir, "telemetry_cache.db")
            asyncio.run(import_csv_to_sqlite(self.csv_path, db_path, "directory_domains"))

            self.after(0, self._render_success, domains_list, request_id)
        except Exception as e:
            usage_logger.error(f"Error fetching Domains list: {e}", exc_info=True)
            if not self.is_cancelled and request_id == self.current_request_id:
                self.after(0, self._render_error, str(e), request_id)
        finally:
            if self.semaphore:
                self.semaphore.release()

    def _render_success(self, domains_list, request_id):
        if self.is_cancelled or request_id != self.current_request_id:
            return
        self.status = "success"
        self._cached_domains = domains_list
        self._update_domains_ui_paginated()
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

    def _load_page_from_sqlite(self, page):
        if not self.csv_path:
            return [], 0
            
        reports_dir = os.path.dirname(self.csv_path)
        db_path = os.path.join(reports_dir, "telemetry_cache.db")
        if not os.path.exists(db_path):
            return [], 0

        try:
            rows, total_count = query_page_sync(db_path, "directory_domains", page, self.ITEMS_PER_PAGE)
            domains = []
            for row in rows:
                services_str = row.get("Supported_Services", "")
                domains.append({
                    "id": row.get("Domain_ID", "-"),
                    "authenticationType": row.get("Authentication_Type", "-"),
                    "isAdminManaged": row.get("Admin_Managed") == "Yes",
                    "isDefault": row.get("Default") == "Yes",
                    "isVerified": row.get("Verified") == "Yes",
                    "supportedServices": [s.strip() for s in services_str.split(",")] if services_str and services_str != "-" else [],
                    "federationDisplayName": row.get("Federation_Display_Name", "-"),
                    "federationIssuerUri": row.get("Federation_Issuer_URI", "-")
                })
            return domains, total_count
        except Exception as e:
            usage_logger.error(f"Error reading SQLite for Domains pagination: {e}")
            return [], 0

    def _update_domains_ui_paginated(self):
        for w in self.body_frame.winfo_children():
            w.destroy()

        domains_grid = ctk.CTkFrame(self.body_frame, fg_color=COLOR_OUTLINE_LIGHT, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)
        domains_grid.pack(fill="x", expand=True, pady=(5, 10))

        domains_grid.grid_columnconfigure((0, 5, 6, 7), weight=3)
        domains_grid.grid_columnconfigure((1, 2, 3, 4), weight=2)

        domains_headers = ["Domain ID", "Auth Type", "Admin Managed", "Default", "Verified", "Supported Services", "Federation Display Name", "Federation Issuer URI"]
        for col_idx, head_text in enumerate(domains_headers):
            cell = ctk.CTkFrame(domains_grid, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        page_data, total_count = self._load_page_from_sqlite(self.current_page)

        if not page_data:
            empty_cell = ctk.CTkFrame(domains_grid, fg_color="transparent")
            empty_cell.grid(row=1, column=0, columnspan=8, sticky="nsew", pady=15)
            ctk.CTkLabel(empty_cell, text="No domains found under the organization.", text_color=COLOR_TEXT_SUB).pack()
        else:
            for item_idx, domain in enumerate(page_data, start=1):
                bg_style = COLOR_SURFACE if item_idx % 2 == 0 else COLOR_SURFACE_VARIANT

                auth_type = domain.get("authenticationType", "N/A")
                admin_managed = "Yes" if domain.get("isAdminManaged") else "No"
                is_default = "Yes" if domain.get("isDefault") else "No"
                is_verified = "Yes" if domain.get("isVerified") else "No"
                services = domain.get("supportedServices", [])
                services_str = ", ".join(services) if services else "-"

                # Domain ID
                c0 = ctk.CTkFrame(domains_grid, fg_color=bg_style, corner_radius=0)
                c0.grid(row=item_idx, column=0, sticky="nsew", padx=0, pady=(0, 1))
                ctk.CTkLabel(c0, text=domain.get("id", "-"), font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=8, anchor="nw")

                # Auth Type
                c1 = ctk.CTkFrame(domains_grid, fg_color=bg_style, corner_radius=0)
                c1.grid(row=item_idx, column=1, sticky="nsew", padx=0, pady=(0, 1))
                ctk.CTkLabel(c1, text=auth_type, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=8, anchor="nw")

                # Admin Managed
                c2 = ctk.CTkFrame(domains_grid, fg_color=bg_style, corner_radius=0)
                c2.grid(row=item_idx, column=2, sticky="nsew", padx=0, pady=(0, 1))
                ctk.CTkLabel(c2, text=admin_managed, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=8, anchor="nw")

                # Default
                c3 = ctk.CTkFrame(domains_grid, fg_color=bg_style, corner_radius=0)
                c3.grid(row=item_idx, column=3, sticky="nsew", padx=0, pady=(0, 1))
                ctk.CTkLabel(c3, text=is_default, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=8, anchor="nw")

                # Verified
                c4 = ctk.CTkFrame(domains_grid, fg_color=bg_style, corner_radius=0)
                c4.grid(row=item_idx, column=4, sticky="nsew", padx=0, pady=(0, 1))
                ctk.CTkLabel(c4, text=is_verified, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=8, anchor="nw")

                # Supported Services
                c5 = ctk.CTkFrame(domains_grid, fg_color=bg_style, corner_radius=0)
                c5.grid(row=item_idx, column=5, sticky="nsew", padx=0, pady=(0, 1))
                ctk.CTkLabel(c5, text=services_str, text_color=COLOR_TEXT_MAIN, justify="left", wraplength=180).pack(padx=10, pady=8, anchor="nw")

                # Federated IdP Name
                c6 = ctk.CTkFrame(domains_grid, fg_color=bg_style, corner_radius=0)
                c6.grid(row=item_idx, column=6, sticky="nsew", padx=0, pady=(0, 1))
                ctk.CTkLabel(c6, text=domain.get("federationDisplayName", "-"), text_color=COLOR_TEXT_MAIN, justify="left", wraplength=180).pack(padx=10, pady=8, anchor="nw")

                # Federated Issuer URI
                c7 = ctk.CTkFrame(domains_grid, fg_color=bg_style, corner_radius=0)
                c7.grid(row=item_idx, column=7, sticky="nsew", padx=0, pady=(0, 1))
                ctk.CTkLabel(c7, text=domain.get("federationIssuerUri", "-"), text_color=COLOR_TEXT_MAIN, justify="left", wraplength=200).pack(padx=10, pady=8, anchor="nw")

        # Draw pagination controls if we have multiple pages
        if total_count > 0:
            self._draw_pagination_controls(total_count)

        domains_footnote = ctk.CTkLabel(
            self.body_frame,
            text="* AuthenticationType=Managed indicates a cloud managed domain where Microsoft Entra ID performs user authentication. Federated indicates authentication is federated with an identity provider (eg. AD FS, Okta etc.)",
            font=FONT_BODY_SMALL,
            text_color=COLOR_TEXT_SUB,
            anchor="w",
            justify="left",
            wraplength=1100
        )
        domains_footnote.pack(fill="x", padx=10, pady=(0, 5))

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
            text=f"Page {self.current_page + 1} of {total_pages} ({total_count} domains)",
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
        self._update_domains_ui_paginated()

    def cancel(self):
        self.is_cancelled = True
        self.current_request_id += 1
        if self.status == "loading":
            self.status = "cancelled"
            self._update_domains_ui_paginated()
        if hasattr(self, "btn_refresh") and self.btn_refresh.winfo_exists():
            self.btn_refresh.configure(state="normal")

    @property
    def last_data(self):
        if hasattr(self, "_cached_domains") and self._cached_domains:
            return self._cached_domains
        # Fallback load from SQLite
        page_data, _ = self._load_page_from_sqlite(0)
        return page_data
