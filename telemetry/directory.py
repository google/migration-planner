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

import os
import csv
import logging
import threading
import webbrowser
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
    
    def update_loading_text(self, text_msg):
        if hasattr(self, 'loading_label') and self.loading_label.winfo_exists():
            self.loading_label.configure(text=f"⏳ {text_msg}")
            
    def __init__(self, master, log_callback, credentials_callback, status_change_callback, retries_var=None, backoff_var=None, **kwargs):
        self.semaphore = kwargs.pop("concurrency_semaphore", None)
        super().__init__(master, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=12, **kwargs)
        self.log_msg = log_callback
        self.get_credentials = credentials_callback
        self.on_status_change = status_change_callback
        self.retries = retries_var
        self.backoff = backoff_var
        self.status = None  # 'loading', 'success', 'error', None
        self.last_organization = []
        self.last_group_counts = {}
        self.last_user_counts = {}
        self.last_domains = []
        self.last_user_creation_logs = []
        
        # Pagination variables for Domains and User Creation logs
        self.ITEMS_PER_PAGE = 10
        self.current_page = 0
        self.user_creation_current_page = 0
        self.csv_path = None
        self.org_csv_path = None
        self.user_creation_csv_path = None
        
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
        
        # Organization sub-heading & grid
        self.org_header_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.org_title = ctk.CTkLabel(
            self.org_header_frame,
            text="Organization",
            font=FONT_HEADER_SMALL,
            text_color=COLOR_TEXT_MAIN
        )
        self.org_title.pack(side="left")
        
        self.org_reference_link = ctk.CTkLabel(
            self.org_header_frame,
            text="Organization API Reference ↗",
            font=FONT_BODY_BOLD,
            text_color=COLOR_PRIMARY,
            cursor="hand2"
        )
        self.org_reference_link.pack(side="left", padx=(15, 0))
        self.org_reference_link.bind("<Button-1>", lambda e: webbrowser.open("https://learn.microsoft.com/en-us/graph/api/resources/organization?view=graph-rest-1.0#properties"))
        self.org_reference_link.bind("<Enter>", lambda e: self.org_reference_link.configure(text_color=COLOR_PRIMARY_HOVER))
        self.org_reference_link.bind("<Leave>", lambda e: self.org_reference_link.configure(text_color=COLOR_PRIMARY))
        
        self.org_reload_btn = ctk.CTkButton(
            self.org_header_frame, 
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
        self.org_reload_btn.pack(side="right")
        self.org_grid = ctk.CTkFrame(self.inner_pad, fg_color=COLOR_OUTLINE_LIGHT, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)
        
        # Divider between Organization and Domains
        self.divider_org_domains = ctk.CTkFrame(self.inner_pad, height=1, fg_color=COLOR_OUTLINE_LIGHT)
        
        # Domains sub-heading & grid
        self.domains_header_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.domains_title = ctk.CTkLabel(
            self.domains_header_frame,
            text="Domains",
            font=FONT_HEADER_SMALL,
            text_color=COLOR_TEXT_MAIN
        )
        self.domains_title.pack(side="left")
        
        self.domains_reference_link = ctk.CTkLabel(
            self.domains_header_frame,
            text="Domain API Reference ↗",
            font=FONT_BODY_BOLD,
            text_color=COLOR_PRIMARY,
            cursor="hand2"
        )
        self.domains_reference_link.pack(side="left", padx=(15, 0))
        self.domains_reference_link.bind("<Button-1>", lambda e: webbrowser.open("https://learn.microsoft.com/en-us/graph/api/resources/domain?view=graph-rest-1.0#properties"))
        self.domains_reference_link.bind("<Enter>", lambda e: self.domains_reference_link.configure(text_color=COLOR_PRIMARY_HOVER))
        self.domains_reference_link.bind("<Leave>", lambda e: self.domains_reference_link.configure(text_color=COLOR_PRIMARY))
        self.domains_reload_btn = ctk.CTkButton(
            self.domains_header_frame, 
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
        self.domains_reload_btn.pack(side="right")
        self.domains_grid = ctk.CTkFrame(self.inner_pad, fg_color=COLOR_OUTLINE_LIGHT, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)
        
        # Divider between Domains and User Creation logs
        self.divider_domains_user_creation = ctk.CTkFrame(self.inner_pad, height=1, fg_color=COLOR_OUTLINE_LIGHT)
        
        # User Creation logs sub-heading & grid
        self.user_creation_header_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.user_creation_title = ctk.CTkLabel(
            self.user_creation_header_frame,
            text="User Creation/Deletion logs",
            font=FONT_HEADER_SMALL,
            text_color=COLOR_TEXT_MAIN
        )
        self.user_creation_title.pack(side="left")
        
        self.user_creation_reference_link = ctk.CTkLabel(
            self.user_creation_header_frame,
            text="Directory Audit API Reference ↗",
            font=FONT_BODY_BOLD,
            text_color=COLOR_PRIMARY,
            cursor="hand2"
        )
        self.user_creation_reference_link.pack(side="left", padx=(15, 0))
        self.user_creation_reference_link.bind("<Button-1>", lambda e: webbrowser.open("https://learn.microsoft.com/en-us/graph/api/resources/directoryaudit?view=graph-rest-1.0"))
        self.user_creation_reference_link.bind("<Enter>", lambda e: self.user_creation_reference_link.configure(text_color=COLOR_PRIMARY_HOVER))
        self.user_creation_reference_link.bind("<Leave>", lambda e: self.user_creation_reference_link.configure(text_color=COLOR_PRIMARY))
        
        self.user_creation_reload_btn = ctk.CTkButton(
            self.user_creation_header_frame, 
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
        self.user_creation_reload_btn.pack(side="right")
        self.user_creation_grid = ctk.CTkFrame(self.inner_pad, fg_color=COLOR_OUTLINE_LIGHT, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)
        
        # Divider between User Creation logs and Provisioning logs
        self.divider_user_creation_provisioning = ctk.CTkFrame(self.inner_pad, height=1, fg_color=COLOR_OUTLINE_LIGHT)
        
        # Provisioning logs sub-heading & grid
        self.provisioning_header_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.provisioning_title = ctk.CTkLabel(
            self.provisioning_header_frame,
            text="Provisioning Logs",
            font=FONT_HEADER_SMALL,
            text_color=COLOR_TEXT_MAIN
        )
        self.provisioning_title.pack(side="left")
        
        self.provisioning_reference_link = ctk.CTkLabel(
            self.provisioning_header_frame,
            text="Provisioning Logs API Reference ↗",
            font=FONT_BODY_SMALL_UNDERLINED,
            text_color=COLOR_PRIMARY,
            cursor="hand2"
        )
        self.provisioning_reference_link.pack(side="left", padx=(15, 0))
        self.provisioning_reference_link.bind("<Button-1>", lambda e: webbrowser.open("https://learn.microsoft.com/en-us/graph/api/resources/provisioningobjectsummary?view=graph-rest-1.0"))
        self.provisioning_reference_link.bind("<Enter>", lambda e: self.provisioning_reference_link.configure(text_color=COLOR_PRIMARY_HOVER))
        self.provisioning_reference_link.bind("<Leave>", lambda e: self.provisioning_reference_link.configure(text_color=COLOR_PRIMARY))
        
        self.provisioning_reload_btn = ctk.CTkButton(
            self.provisioning_header_frame, 
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
        self.provisioning_reload_btn.pack(side="right")
        self.provisioning_grid = ctk.CTkFrame(self.inner_pad, fg_color=COLOR_OUTLINE_LIGHT, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)
        
        # Divider between Provisioning logs and Groups & Users
        self.divider_provisioning_groups = ctk.CTkFrame(self.inner_pad, height=1, fg_color=COLOR_OUTLINE_LIGHT)
        
        # Groups & Users sub-heading & grid
        self.groups_users_header_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.groups_users_title = ctk.CTkLabel(
            self.groups_users_header_frame,
            text="Groups & Users",
            font=FONT_HEADER_SMALL,
            text_color=COLOR_TEXT_MAIN
        )
        self.groups_users_title.pack(side="left")
        self.groups_users_reload_btn = ctk.CTkButton(
            self.groups_users_header_frame, 
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
        self.groups_users_reload_btn.pack(side="right")
        self.groups_users_grid = ctk.CTkFrame(self.inner_pad, fg_color=COLOR_OUTLINE_LIGHT, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)
        
        self.reset_view()

    def reset_view(self):
        """Resets and hides grids."""
        self.pack_forget()
        self.state_frame.pack_forget()
        
        self.org_header_frame.pack_forget()
        self.org_grid.pack_forget()
        if hasattr(self, "org_footnote") and self.org_footnote.winfo_exists():
            self.org_footnote.pack_forget()
        self.divider_org_domains.pack_forget()
        
        self.domains_header_frame.pack_forget()
        self.domains_grid.pack_forget()
        if hasattr(self, "domains_footnote") and self.domains_footnote.winfo_exists():
            self.domains_footnote.pack_forget()
        self.divider_domains_user_creation.pack_forget()
        
        self.user_creation_header_frame.pack_forget()
        self.user_creation_grid.pack_forget()
        if hasattr(self, "user_creation_footnote") and self.user_creation_footnote.winfo_exists():
            self.user_creation_footnote.pack_forget()
        self.divider_user_creation_provisioning.pack_forget()
        
        self.provisioning_header_frame.pack_forget()
        self.provisioning_grid.pack_forget()
        if hasattr(self, "provisioning_footnote") and self.provisioning_footnote.winfo_exists():
            self.provisioning_footnote.pack_forget()
        self.divider_provisioning_groups.pack_forget()
        
        self.groups_users_header_frame.pack_forget()
        self.groups_users_grid.pack_forget()
        if hasattr(self, "pagination_frame") and self.pagination_frame.winfo_exists():
            self.pagination_frame.destroy()
        if hasattr(self, "user_creation_pagination_frame") and self.user_creation_pagination_frame.winfo_exists():
            self.user_creation_pagination_frame.destroy()
        if hasattr(self, "provisioning_pagination_frame") and self.provisioning_pagination_frame.winfo_exists():
            self.provisioning_pagination_frame.destroy()
 
        self.last_organization = []
        self.last_group_counts = {}
        self.last_user_counts = {}
        self.last_domains = []
        self.last_user_creation_logs = []
        self.current_page = 0
        self.user_creation_current_page = 0
        self.csv_path = None
        self.org_csv_path = None
        self.user_creation_csv_path = None
        
        for w in self.state_frame.winfo_children():
            w.destroy()
        for w in self.org_grid.winfo_children():
            w.destroy()
        for w in self.domains_grid.winfo_children():
            w.destroy()
        for w in self.user_creation_grid.winfo_children():
            w.destroy()
        for w in self.groups_users_grid.winfo_children():
            w.destroy()

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
            display_msg = "Directory read permissions required.\nPlease grant the 'Directory.Read.All' permission to your App Registration in Entra ID."

        ctk.CTkLabel(self.state_frame, text=f"✖ {display_msg}", text_color=COLOR_ERROR, font=FONT_BODY_MEDIUM, justify="center").pack(pady=(20, 5))
        ctk.CTkButton(self.state_frame, text="Try Again", command=self._retry_fetch, width=120, fg_color="transparent", border_width=1, text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER).pack(pady=(0, 20))
        self.state_frame.pack(fill="x", expand=True)

    def _retry_fetch(self):
        for btn in ['org_reload_btn', 'domains_reload_btn', 'user_creation_reload_btn', 'groups_users_reload_btn']:
            if hasattr(self, btn) and getattr(self, btn).winfo_exists():
                getattr(self, btn).configure(state="disabled")
        tenant, clients, secrets = self.get_credentials()
        if tenant:
            self.trigger_fetch(tenant, clients[0], secrets[0])

    def trigger_fetch(self, tenant, client_id, client_secret):
        """Triggers Directory Telemetry fetch inside background thread."""
        usage_logger.info("Directory telemetry trigger_fetch called. Spawning background worker thread...")
        self.status = "loading"
        self.on_status_change()
        
        self.current_page = 0
        self.user_creation_current_page = 0
        script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
        reports_dir = os.path.join(script_dir, "reports", f"{tenant}_{client_id}")
        self.csv_path = os.path.join(reports_dir, "directory_domains.csv")
        self.org_csv_path = os.path.join(reports_dir, "directory_organization.csv")
        self.user_creation_csv_path = os.path.join(reports_dir, "directory_user_creation_logs.csv")
        
        self.pack(fill="x", expand=True, pady=10)
        self.org_header_frame.pack_forget()
        self.org_grid.pack_forget()
        if hasattr(self, "org_footnote") and self.org_footnote.winfo_exists():
            self.org_footnote.pack_forget()
        self.divider_org_domains.pack_forget()
        
        self.domains_header_frame.pack_forget()
        self.domains_grid.pack_forget()
        if hasattr(self, "domains_footnote") and self.domains_footnote.winfo_exists():
            self.domains_footnote.pack_forget()
        self.divider_domains_user_creation.pack_forget()
        
        self.user_creation_header_frame.pack_forget()
        self.user_creation_grid.pack_forget()
        if hasattr(self, "user_creation_footnote") and self.user_creation_footnote.winfo_exists():
            self.user_creation_footnote.pack_forget()
        self.divider_user_creation_provisioning.pack_forget()
        
        self.provisioning_header_frame.pack_forget()
        self.provisioning_grid.pack_forget()
        if hasattr(self, "provisioning_footnote") and self.provisioning_footnote.winfo_exists():
            self.provisioning_footnote.pack_forget()
        self.divider_provisioning_groups.pack_forget()
        
        self.groups_users_header_frame.pack_forget()
        self.groups_users_grid.pack_forget()
        if hasattr(self, "pagination_frame") and self.pagination_frame.winfo_exists():
            self.pagination_frame.destroy()
        if hasattr(self, "user_creation_pagination_frame") and self.user_creation_pagination_frame.winfo_exists():
            self.user_creation_pagination_frame.destroy()
        if hasattr(self, "provisioning_pagination_frame") and self.provisioning_pagination_frame.winfo_exists():
            self.provisioning_pagination_frame.destroy()
        
        self._set_state_loading("Fetching directory organization, domains, user creation logs, and group counts...")
        
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
            
            script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
            reports_dir = os.path.join(script_dir, "reports", f"{tenant}_{client_id}")
            os.makedirs(reports_dir, exist_ok=True)

            self.log_msg("Querying directory organization, domains, users, and group counts from Microsoft Graph...")
            dir_service = DirectoryService(client)
            telemetry_data = dir_service.get_directory_telemetry(self.log_msg)

            self.log_msg("Querying User Creation logs from Microsoft Graph...")
            user_creation_csv_path = os.path.join(reports_dir, "directory_user_creation_logs.csv")
            user_creation_logs = []
            
            def handle_user_creation_page(page_rows):
                user_creation_logs.extend(page_rows)
                
            dir_service.fetch_user_creation_logs(
                csv_path=user_creation_csv_path,
                max_rows=50,
                on_page_callback=handle_user_creation_page,
                is_cancelled_callback=lambda: getattr(self, "is_cancelled", False)
            )
            telemetry_data["user_creation_logs"] = user_creation_logs
            
            self.log_msg("Querying Provisioning logs from Microsoft Graph...")
            provisioning_csv_path = os.path.join(reports_dir, "directory_provisioning_logs.csv")
            provisioning_logs = []
            
            def handle_provisioning_page(page_rows):
                provisioning_logs.extend(page_rows)
                
            dir_service.fetch_provisioning_logs(
                csv_path=provisioning_csv_path,
                max_rows=200,
                on_page_callback=handle_provisioning_page,
                is_cancelled_callback=lambda: getattr(self, "is_cancelled", False)
            )
            telemetry_data["provisioning_logs"] = provisioning_logs
            client.close()
            
            usage_logger.info("Successfully fetched directory telemetry data. Writing to disk...")
            
            # 1. Write domains CSV
            csv_path = os.path.join(reports_dir, "directory_domains.csv")
            headers = ["Domain ID", "Authentication Type", "Admin Managed", "Default", "Verified", "Supported Services", "Federation Display Name", "Federation Issuer URI"]
            rows = []
            for domain in telemetry_data.get("domains", []):
                auth_type = domain.get("authenticationType", "N/A") or "N/A"
                admin_managed = "Yes" if domain.get("isAdminManaged") else "No"
                is_default = "Yes" if domain.get("isDefault") else "No"
                is_verified = "Yes" if domain.get("isVerified") else "No"
                services = domain.get("supportedServices", [])
                services_str = ", ".join(services) if services else "-"
                fed_idp = domain.get("federationDisplayName") or "-"
                fed_issuer = domain.get("federationIssuerUri") or "-"
                rows.append([domain.get("id", "-"), auth_type, admin_managed, is_default, is_verified, services_str, fed_idp, fed_issuer])

            with open(csv_path, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                writer.writerows(rows)

            # 2. Write organization CSV
            org_csv_path = os.path.join(reports_dir, "directory_organization.csv")
            org_headers = [
                "displayName", 
                "isMultipleDataLocationsForServicesEnabled", "onPremisesSyncEnabled", 
                "onPremisesLastSyncDateTime", "partnerTenantType", "tenantType",
                "provisionedPlans_service", "provisionedPlans_capabilityStatus", "provisionedPlans_provisioningStatus"
            ]
            org_rows = []
            
            def format_csv_val(v):
                return "null" if v is None else str(v)

            for org in telemetry_data.get("organization", []):
                disp_name = format_csv_val(org.get("displayName"))
                multi_loc = format_csv_val(org.get("isMultipleDataLocationsForServicesEnabled"))
                sync_enabled = format_csv_val(org.get("onPremisesSyncEnabled"))
                last_sync = format_csv_val(org.get("onPremisesLastSyncDateTime"))
                partner_type = format_csv_val(org.get("partnerTenantType"))
                tenant_type = format_csv_val(org.get("tenantType"))
                
                plans = org.get("provisionedPlans", [])
                if not plans:
                    org_rows.append([disp_name, multi_loc, sync_enabled, last_sync, partner_type, tenant_type, "null", "null", "null"])
                else:
                    for plan in plans:
                        service = format_csv_val(plan.get("service"))
                        cap_status = format_csv_val(plan.get("capabilityStatus"))
                        prov_status = format_csv_val(plan.get("provisioningStatus"))
                        org_rows.append([disp_name, multi_loc, sync_enabled, last_sync, partner_type, tenant_type, service, cap_status, prov_status])

            with open(org_csv_path, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(org_headers)
                writer.writerows(org_rows)
                
            usage_logger.info(f"Successfully wrote Domains and Organization data to disk.")
            
            self.after(0, self._render_success, telemetry_data)
        except Exception as e:
            usage_logger.error("Exception caught in Directory worker.", exc_info=True)
            self.after(0, self._render_error, str(e))
        finally:
            if self.semaphore:
                self.semaphore.release()

    def _render_success(self, telemetry_dict: Dict[str, Any]):
        usage_logger.info("Executing UI render for Directory domains, users & groups tables.")
        for btn in ['org_reload_btn', 'domains_reload_btn', 'user_creation_reload_btn', 'provisioning_reload_btn', 'groups_users_reload_btn']:
            if hasattr(self, btn) and getattr(self, btn).winfo_exists():
                getattr(self, btn).configure(state="normal")
        self.state_frame.pack_forget()
        
        self.last_organization = telemetry_dict.get("organization", [])
        self.last_domains = telemetry_dict.get("domains", [])
        self.last_user_creation_logs = telemetry_dict.get("user_creation_logs", [])
        self.last_provisioning_logs = telemetry_dict.get("provisioning_logs", [])
        self.last_group_counts = telemetry_dict.get("group_counts", {})
        self.last_user_counts = telemetry_dict.get("user_counts", {})

        for w in self.org_grid.winfo_children():
            w.destroy()
        for w in self.domains_grid.winfo_children():
            w.destroy()
        for w in self.user_creation_grid.winfo_children():
            w.destroy()
        for w in self.provisioning_grid.winfo_children():
            w.destroy()
        for w in self.groups_users_grid.winfo_children():
            w.destroy()

        # Display UI titles and tables
        self.org_header_frame.pack(fill="x", pady=(10, 10))
        self.org_grid.pack(fill="x", expand=True, pady=(0, 10))
        
        if hasattr(self, "org_footnote") and self.org_footnote.winfo_exists():
            self.org_footnote.destroy()
            
        self.org_footnote = ctk.CTkLabel(
            self.inner_pad,
            text="* If OnPremisesSyncEnabled returns True, on-premises Active Directory is a primary source of truth. If it returns Null or False, the directory is cloud-managed or driven by a 3rd-party application.",
            font=FONT_BODY_SMALL,
            text_color=COLOR_TEXT_SUB,
            anchor="w",
            justify="left",
            wraplength=1100
        )
        self.org_footnote.pack(fill="x", padx=10, pady=(0, 10))
        
        self.divider_org_domains.pack(fill="x", pady=15)
        
        self.domains_header_frame.pack_forget()
        self.domains_header_frame.pack(fill="x", pady=(10, 10))
        self.domains_grid.pack(fill="x", expand=True, pady=(0, 10))
        
        if hasattr(self, "domains_footnote") and self.domains_footnote.winfo_exists():
            self.domains_footnote.destroy()
            
        self.domains_footnote = ctk.CTkLabel(
            self.inner_pad,
            text="* AuthenticationType=Managed indicates a cloud managed domain where Microsoft Entra ID performs user authentication. Federated indicates authentication is federated with an identity provider (eg. AD FS, Okta etc.)",
            font=FONT_BODY_SMALL,
            text_color=COLOR_TEXT_SUB,
            anchor="w",
            justify="left",
            wraplength=1100
        )
        self.domains_footnote.pack(fill="x", padx=10, pady=(0, 10))
        
        self.divider_domains_user_creation.pack(fill="x", pady=15)
        
        self.user_creation_header_frame.pack_forget()
        self.user_creation_header_frame.pack(fill="x", pady=(10, 10))
        self.user_creation_grid.pack(fill="x", expand=True, pady=(0, 10))
        
        if hasattr(self, "user_creation_footnote") and self.user_creation_footnote.winfo_exists():
            self.user_creation_footnote.destroy()
            
        self.user_creation_footnote = ctk.CTkLabel(
            self.inner_pad,
            text="* Based on sampled data collected from audit logs.",
            font=FONT_BODY_SMALL,
            text_color=COLOR_TEXT_SUB,
            anchor="w",
            justify="left",
            wraplength=1100
        )
        self.user_creation_footnote.pack(fill="x", padx=10, pady=(0, 10))
        
        self.divider_user_creation_provisioning.pack(fill="x", pady=15)
        
        self.provisioning_header_frame.pack_forget()
        self.provisioning_header_frame.pack(fill="x", pady=(10, 10))
        self.provisioning_grid.pack(fill="x", expand=True, pady=(0, 10))
        
        if hasattr(self, "provisioning_footnote") and self.provisioning_footnote.winfo_exists():
            self.provisioning_footnote.destroy()
            
        self.provisioning_footnote = ctk.CTkLabel(
            self.inner_pad,
            text="* Based on sampled data collected from audit logs.",
            font=FONT_BODY_SMALL,
            text_color=COLOR_TEXT_SUB,
            anchor="w",
            justify="left",
            wraplength=1100
        )
        self.provisioning_footnote.pack(fill="x", padx=10, pady=(0, 10))
        
        self.divider_provisioning_groups.pack(fill="x", pady=15)
        
        self.groups_users_header_frame.pack(fill="x", pady=(10, 10))
        self.groups_users_grid.pack(fill="x", expand=True, pady=(0, 10))

        self._render_org_grid()
        self._update_domains_ui_paginated()
        self._update_user_creation_ui_paginated()
        self._update_provisioning_ui_paginated()
        self._render_groups_users_grid()

        self.status = "success"
        self.on_status_change()


    def _render_org_grid(self):
        self.org_grid.grid_columnconfigure(0, weight=1)
        self.org_grid.grid_columnconfigure(1, weight=3)

        org_headers = ["Property", "Value"]
        for col_idx, head_text in enumerate(org_headers):
            cell = ctk.CTkFrame(self.org_grid, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        org = self.last_organization[0] if self.last_organization else {}
        
        # Unique provisioned plans
        plans = org.get("provisionedPlans", [])
        plan_services = sorted(list(set(
            plan.get("service") for plan in plans 
            if plan.get("service") and str(plan.get("capabilityStatus")).lower() in ["enabled", "warning"]
        )))
        plan_services_str = ", ".join(plan_services) if plan_services else "null"

        def format_ui_val(v):
            return "null" if v is None else str(v)

        rows_data = [
            ("displayName", format_ui_val(org.get("displayName"))),
            ("isMultipleDataLocationsForServicesEnabled", format_ui_val(org.get("isMultipleDataLocationsForServicesEnabled"))),
            ("onPremisesSyncEnabled", format_ui_val(org.get("onPremisesSyncEnabled"))),
            ("onPremisesLastSyncDateTime", format_ui_val(org.get("onPremisesLastSyncDateTime"))),
            ("partnerTenantType", format_ui_val(org.get("partnerTenantType"))),
            ("tenantType", format_ui_val(org.get("tenantType"))),
            ("provisionedPlans", plan_services_str)
        ]

        for r_idx, (prop_name, val) in enumerate(rows_data, start=1):
            bg_style = COLOR_SURFACE if r_idx % 2 == 0 else COLOR_SURFACE_VARIANT

            c0 = ctk.CTkFrame(self.org_grid, fg_color=bg_style, corner_radius=0)
            c0.grid(row=r_idx, column=0, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c0, text=prop_name, font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=8, anchor="nw")

            c1 = ctk.CTkFrame(self.org_grid, fg_color=bg_style, corner_radius=0)
            c1.grid(row=r_idx, column=1, sticky="nsew", padx=0, pady=(0, 1))
            
            wraplen = 600 if prop_name == "provisionedPlans" else None
            lbl = ctk.CTkLabel(c1, text=str(val), font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="left")
            if wraplen:
                lbl.configure(wraplength=wraplen)
            lbl.pack(padx=10, pady=8, anchor="nw")

    def _load_page_from_csv(self, page):
        if not self.csv_path or not os.path.exists(self.csv_path):
            return [], 0

        domains = []
        try:
            with open(self.csv_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader, None)  # skip header
                for row in reader:
                    if row:
                        domains.append({
                            "id": row[0],
                            "authenticationType": row[1],
                            "isAdminManaged": row[2] == "Yes",
                            "isDefault": row[3] == "Yes",
                            "isVerified": row[4] == "Yes",
                            "supportedServices": [s.strip() for s in row[5].split(",")] if row[5] != "-" else [],
                            "federationDisplayName": row[6] if len(row) > 6 else "-",
                            "federationIssuerUri": row[7] if len(row) > 7 else "-"
                        })
        except Exception as e:
            usage_logger.error(f"Error reading CSV for Domains pagination: {e}")
            
        total_count = len(domains)
        start_idx = page * self.ITEMS_PER_PAGE
        end_idx = start_idx + self.ITEMS_PER_PAGE
        page_data = domains[start_idx:end_idx]
        
        return page_data, total_count

    def _update_domains_ui_paginated(self):
        for w in self.domains_grid.winfo_children():
            w.destroy()

        self.domains_grid.grid_columnconfigure((0, 5, 6, 7), weight=3)
        self.domains_grid.grid_columnconfigure((1, 2, 3, 4), weight=2)

        domains_headers = ["Domain ID", "Auth Type", "Admin Managed", "Default", "Verified", "Supported Services", "Federation Display Name", "Federation Issuer URI"]
        for col_idx, head_text in enumerate(domains_headers):
            cell = ctk.CTkFrame(self.domains_grid, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        page_data, total_count = self._load_page_from_csv(self.current_page)

        if not page_data:
            empty_cell = ctk.CTkFrame(self.domains_grid, fg_color="transparent")
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
                c0 = ctk.CTkFrame(self.domains_grid, fg_color=bg_style, corner_radius=0)
                c0.grid(row=item_idx, column=0, sticky="nsew", padx=0, pady=(0, 1))
                ctk.CTkLabel(c0, text=domain.get("id", "-"), font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=8, anchor="nw")

                # Auth Type
                c1 = ctk.CTkFrame(self.domains_grid, fg_color=bg_style, corner_radius=0)
                c1.grid(row=item_idx, column=1, sticky="nsew", padx=0, pady=(0, 1))
                ctk.CTkLabel(c1, text=auth_type, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=8, anchor="nw")

                # Admin Managed
                c2 = ctk.CTkFrame(self.domains_grid, fg_color=bg_style, corner_radius=0)
                c2.grid(row=item_idx, column=2, sticky="nsew", padx=0, pady=(0, 1))
                ctk.CTkLabel(c2, text=admin_managed, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=8, anchor="nw")

                # Default
                c3 = ctk.CTkFrame(self.domains_grid, fg_color=bg_style, corner_radius=0)
                c3.grid(row=item_idx, column=3, sticky="nsew", padx=0, pady=(0, 1))
                ctk.CTkLabel(c3, text=is_default, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=8, anchor="nw")

                # Verified
                c4 = ctk.CTkFrame(self.domains_grid, fg_color=bg_style, corner_radius=0)
                c4.grid(row=item_idx, column=4, sticky="nsew", padx=0, pady=(0, 1))
                ctk.CTkLabel(c4, text=is_verified, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=8, anchor="nw")

                # Supported Services
                c5 = ctk.CTkFrame(self.domains_grid, fg_color=bg_style, corner_radius=0)
                c5.grid(row=item_idx, column=5, sticky="nsew", padx=0, pady=(0, 1))
                ctk.CTkLabel(c5, text=services_str, text_color=COLOR_TEXT_MAIN, justify="left", wraplength=180).pack(padx=10, pady=8, anchor="nw")

                # Federated IdP Name
                c6 = ctk.CTkFrame(self.domains_grid, fg_color=bg_style, corner_radius=0)
                c6.grid(row=item_idx, column=6, sticky="nsew", padx=0, pady=(0, 1))
                ctk.CTkLabel(c6, text=domain.get("federationDisplayName", "-"), text_color=COLOR_TEXT_MAIN, justify="left", wraplength=180).pack(padx=10, pady=8, anchor="nw")

                # Federated Issuer URI
                c7 = ctk.CTkFrame(self.domains_grid, fg_color=bg_style, corner_radius=0)
                c7.grid(row=item_idx, column=7, sticky="nsew", padx=0, pady=(0, 1))
                ctk.CTkLabel(c7, text=domain.get("federationIssuerUri", "-"), text_color=COLOR_TEXT_MAIN, justify="left", wraplength=200).pack(padx=10, pady=8, anchor="nw")

        # Draw pagination controls if we have multiple pages
        if total_count > 0:
            self._draw_pagination_controls(total_count)

    def _draw_pagination_controls(self, total_count):
        total_pages = (total_count + self.ITEMS_PER_PAGE - 1) // self.ITEMS_PER_PAGE
        if total_pages <= 1:
            return

        if hasattr(self, "pagination_frame") and self.pagination_frame.winfo_exists():
            self.pagination_frame.destroy()

        self.pagination_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.pagination_frame.pack(fill="x", pady=(2, 0), after=self.domains_grid)

        left_spacer = ctk.CTkFrame(self.pagination_frame, fg_color="transparent")
        left_spacer.pack(side="left", fill="x", expand=True)

        center_container = ctk.CTkFrame(self.pagination_frame, fg_color="transparent")
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

        right_spacer = ctk.CTkFrame(self.pagination_frame, fg_color="transparent")
        right_spacer.pack(side="right", fill="x", expand=True)

    def _change_page(self, delta):
        self.current_page += delta
        self._update_domains_ui_paginated()

    def _render_groups_users_grid(self):
        self.groups_users_grid.grid_columnconfigure(0, weight=3)
        self.groups_users_grid.grid_columnconfigure(1, weight=1)

        groups_users_headers = ["Category", "Count"]
        for col_idx, head_text in enumerate(groups_users_headers):
            cell = ctk.CTkFrame(self.groups_users_grid, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        rows_data = [
            # User statistics
            ("Total Users", self.last_user_counts.get("total", 0), True),
            ("Enabled Users", self.last_user_counts.get("enabled", 0), False),
            ("Disabled Users", self.last_user_counts.get("disabled", 0), False),
            ("Member Users", self.last_user_counts.get("member", 0), False),
            ("Guest Users", self.last_user_counts.get("guest", 0), False),
            # Divider separator row
            (None, None, False),
            # Group statistics
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

    def _load_user_creation_page_from_csv(self, page):
        if not self.user_creation_csv_path or not os.path.exists(self.user_creation_csv_path):
            return [], 0

        logs = []
        try:
            with open(self.user_creation_csv_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader, None)  # skip header
                for row in reader:
                    if row:
                        logs.append({
                            "activity": row[0],
                            "initiatedBy": row[1]
                        })
        except Exception as e:
            usage_logger.error(f"Error reading CSV for User Creation logs pagination: {e}")
            
        total_count = len(logs)
        start_idx = page * self.ITEMS_PER_PAGE
        end_idx = start_idx + self.ITEMS_PER_PAGE
        page_data = logs[start_idx:end_idx]
        
        return page_data, total_count

    def _update_user_creation_ui_paginated(self):
        for w in self.user_creation_grid.winfo_children():
            w.destroy()

        self.user_creation_grid.grid_columnconfigure(0, weight=1)
        self.user_creation_grid.grid_columnconfigure(1, weight=3)

        headers = ["Activity", "Initiated By"]
        for col_idx, head_text in enumerate(headers):
            cell = ctk.CTkFrame(self.user_creation_grid, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        page_data, total_count = self._load_user_creation_page_from_csv(self.user_creation_current_page)

        if not page_data:
            empty_cell = ctk.CTkFrame(self.user_creation_grid, fg_color="transparent")
            empty_cell.grid(row=1, column=0, columnspan=2, sticky="nsew", pady=15)
            ctk.CTkLabel(empty_cell, text="No user creation/deletion logs found.", text_color=COLOR_TEXT_SUB).pack()
        elif page_data[0].get("activity") == "ERROR":
            err_msg = page_data[0].get("initiatedBy")
            error_cell = ctk.CTkFrame(self.user_creation_grid, fg_color="transparent")
            error_cell.grid(row=1, column=0, columnspan=2, sticky="nsew", pady=15)
            ctk.CTkLabel(error_cell, text=f"⚠️ {err_msg}", font=FONT_BODY_MEDIUM, text_color="#DC2626", justify="left", wraplength=1000).pack(padx=10, pady=5)
        else:
            for item_idx, log in enumerate(page_data, start=1):
                bg_style = COLOR_SURFACE if item_idx % 2 == 0 else COLOR_SURFACE_VARIANT

                vals = [
                    log.get("activity", "-"),
                    log.get("initiatedBy", "-")
                ]

                for col_idx, val in enumerate(vals):
                    c = ctk.CTkFrame(self.user_creation_grid, fg_color=bg_style, corner_radius=0)
                    c.grid(row=item_idx, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
                    
                    wraplen = 600 if col_idx == 1 else 180
                    ctk.CTkLabel(c, text=val, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="left", wraplength=wraplen).pack(padx=10, pady=8, anchor="nw")

        # Draw pagination controls if we have multiple pages
        if total_count > 0:
            self._draw_user_creation_pagination_controls(total_count)

    def _draw_user_creation_pagination_controls(self, total_count):
        total_pages = (total_count + self.ITEMS_PER_PAGE - 1) // self.ITEMS_PER_PAGE
        if total_pages <= 1:
            return

        if hasattr(self, "user_creation_pagination_frame") and self.user_creation_pagination_frame.winfo_exists():
            self.user_creation_pagination_frame.destroy()

        self.user_creation_pagination_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.user_creation_pagination_frame.pack(fill="x", pady=(2, 0), after=self.user_creation_grid)

        left_spacer = ctk.CTkFrame(self.user_creation_pagination_frame, fg_color="transparent")
        left_spacer.pack(side="left", fill="x", expand=True)

        center_container = ctk.CTkFrame(self.user_creation_pagination_frame, fg_color="transparent")
        center_container.pack(side="left")

        prev_state = "normal" if self.user_creation_current_page > 0 else "disabled"
        btn_prev = ctk.CTkButton(
            center_container, text="◀ Prev", width=70, height=22, corner_radius=6,
            font=FONT_BODY_SMALL, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER,
            state=prev_state,
            command=lambda: self._change_user_creation_page(-1)
        )
        btn_prev.pack(side="left", padx=5)

        page_lbl = ctk.CTkLabel(
            center_container,
            text=f"Page {self.user_creation_current_page + 1} of {total_pages} ({total_count} logs)",
            font=FONT_BODY_MEDIUM,
            text_color=COLOR_TEXT_SUB
        )
        page_lbl.pack(side="left", padx=15)

        next_state = "normal" if self.user_creation_current_page < total_pages - 1 else "disabled"
        btn_next = ctk.CTkButton(
            center_container, text="Next ▶", width=70, height=22, corner_radius=6,
            font=FONT_BODY_SMALL, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER,
            state=next_state,
            command=lambda: self._change_user_creation_page(1)
        )
        btn_next.pack(side="left", padx=5)

        right_spacer = ctk.CTkFrame(self.user_creation_pagination_frame, fg_color="transparent")
        right_spacer.pack(side="right", fill="x", expand=True)

    def _change_user_creation_page(self, delta):
        self.user_creation_current_page += delta
        self._update_user_creation_ui_paginated()

    def _render_error(self, err_msg):
        usage_logger.warning(f"Rendering Directory error state: {err_msg}")
        for btn in ['org_reload_btn', 'domains_reload_btn', 'user_creation_reload_btn', 'provisioning_reload_btn', 'groups_users_reload_btn']:
            if hasattr(self, btn) and getattr(self, btn).winfo_exists():
                getattr(self, btn).configure(state="normal")
        self._set_state_error(err_msg)
        self.status = "error"
        self.on_status_change()

    def _load_provisioning_page_from_csv(self, page):
        if not self.provisioning_csv_path or not os.path.exists(self.provisioning_csv_path):
            return [], 0

        logs = []
        try:
            with open(self.provisioning_csv_path, 'r', encoding='utf-8') as f:
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
        for w in self.provisioning_grid.winfo_children():
            w.destroy()

        self.provisioning_grid.grid_columnconfigure(0, weight=2)
        self.provisioning_grid.grid_columnconfigure(1, weight=2)
        self.provisioning_grid.grid_columnconfigure(2, weight=3)
        self.provisioning_grid.grid_columnconfigure(3, weight=2)
        self.provisioning_grid.grid_columnconfigure(4, weight=1)
        self.provisioning_grid.grid_columnconfigure(5, weight=1)
        self.provisioning_grid.grid_columnconfigure(6, weight=1)
        self.provisioning_grid.grid_columnconfigure(7, weight=2)

        headers = ["Initiated By", "Action", "Steps", "Service Principal", "Source System", "Target System", "Tenant ID", "Status Info"]
        for col_idx, head_text in enumerate(headers):
            cell = ctk.CTkFrame(self.provisioning_grid, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=8, pady=8, anchor="w")

        page_data, total_count = self._load_provisioning_page_from_csv(self.provisioning_current_page)

        if not page_data:
            empty_cell = ctk.CTkFrame(self.provisioning_grid, fg_color="transparent")
            empty_cell.grid(row=1, column=0, columnspan=8, sticky="nsew", pady=15)
            ctk.CTkLabel(empty_cell, text="No provisioning logs found.", text_color=COLOR_TEXT_SUB).pack()
        elif page_data[0].get("initiatedBy") == "ERROR":
            err_msg = page_data[0].get("provisioningAction")
            error_cell = ctk.CTkFrame(self.provisioning_grid, fg_color="transparent")
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
                    c = ctk.CTkFrame(self.provisioning_grid, fg_color=bg_style, corner_radius=0)
                    c.grid(row=item_idx, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
                    
                    wraplen = 220 if col_idx in [0, 1, 2, 3, 7] else 100
                    ctk.CTkLabel(c, text=val, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="left", wraplength=wraplen).pack(padx=8, pady=8, anchor="nw")

        # Draw pagination controls if we have multiple pages
        if total_count > 0:
            self._draw_provisioning_pagination_controls(total_count)

    def _draw_provisioning_pagination_controls(self, total_count):
        total_pages = (total_count + self.ITEMS_PER_PAGE - 1) // self.ITEMS_PER_PAGE
        if total_pages <= 1:
            return

        if hasattr(self, "provisioning_pagination_frame") and self.provisioning_pagination_frame.winfo_exists():
            self.provisioning_pagination_frame.destroy()

        self.provisioning_pagination_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.provisioning_pagination_frame.pack(fill="x", pady=(2, 0), after=self.provisioning_grid)

        left_spacer = ctk.CTkFrame(self.provisioning_pagination_frame, fg_color="transparent")
        left_spacer.pack(side="left", fill="x", expand=True)

        center_container = ctk.CTkFrame(self.provisioning_pagination_frame, fg_color="transparent")
        center_container.pack(side="left")

        prev_state = "normal" if self.provisioning_current_page > 0 else "disabled"
        btn_prev = ctk.CTkButton(
            center_container, text="◀ Prev", width=70, height=22, corner_radius=6,
            font=FONT_BODY_SMALL, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER,
            state=prev_state,
            command=lambda: self._change_provisioning_page(-1)
        )
        btn_prev.pack(side="left", padx=5)

        page_lbl = ctk.CTkLabel(
            center_container,
            text=f"Page {self.provisioning_current_page + 1} of {total_pages} ({total_count} logs)",
            font=FONT_BODY_MEDIUM,
            text_color=COLOR_TEXT_SUB
        )
        page_lbl.pack(side="left", padx=15)

        next_state = "normal" if self.provisioning_current_page < total_pages - 1 else "disabled"
        btn_next = ctk.CTkButton(
            center_container, text="Next ▶", width=70, height=22, corner_radius=6,
            font=FONT_BODY_SMALL, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER,
            state=next_state,
            command=lambda: self._change_provisioning_page(1)
        )
        btn_next.pack(side="left", padx=5)

        right_spacer = ctk.CTkFrame(self.provisioning_pagination_frame, fg_color="transparent")
        right_spacer.pack(side="right", fill="x", expand=True)

    def _change_provisioning_page(self, delta):
        self.provisioning_current_page += delta
        self._update_provisioning_ui_paginated()
