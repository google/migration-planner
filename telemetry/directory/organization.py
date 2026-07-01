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

"""UI frame for Microsoft Entra ID Organization telemetry."""

import os
import csv
import logging
import threading
import asyncio
import sqlite3
import webbrowser
from typing import Optional
import customtkinter as ctk

from core.graph.client import GraphClient
from core.graph.directory.organization import OrganizationService
from core.graph.db import import_csv_to_sqlite
from telemetry.styles import *

usage_logger = logging.getLogger("M365TelemetryAsyncLogger.DirectoryOrganizationUI")

class DirectoryOrganizationFrame(ctk.CTkFrame):
    """Sub-frame showing Organization properties."""

    def __init__(self, master, log_callback, credentials_callback, status_change_callback, semaphore=None, **kwargs):
        super().__init__(master, fg_color="transparent", **kwargs)
        self.log_msg = log_callback
        self.get_credentials = credentials_callback
        self.on_status_change = status_change_callback
        self.semaphore = semaphore
        self.status = None
        self.is_cancelled = False
        self.current_request_id = 0
        self._cached_org_data = []
        self.csv_path = None

        self.build_ui()

    def build_ui(self):
        self.header_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.header_frame.pack(fill="x", pady=(0, 5))

        self.title_lbl = ctk.CTkLabel(self.header_frame, text="Organization", font=FONT_HEADER_SMALL, text_color=COLOR_TEXT_MAIN)
        self.title_lbl.pack(side="left")

        self.reference_link = ctk.CTkLabel(
            self.header_frame,
            text="Organization API Reference ↗",
            font=FONT_BODY_BOLD,
            text_color=COLOR_PRIMARY,
            cursor="hand2"
        )
        self.reference_link.pack(side="left", padx=(15, 0))
        self.reference_link.bind("<Button-1>", lambda e: webbrowser.open("https://learn.microsoft.com/en-us/graph/api/resources/organization?view=graph-rest-1.0#properties"))
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
        self._cached_org_data = []
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
        # Navigate up to telemetry parent folder if inside telemetry/directory/
        if os.path.basename(script_dir) == "directory":
            script_dir = os.path.dirname(script_dir)
        reports_dir = os.path.join(script_dir, "reports", f"{tenant}_{client_id}")
        self.csv_path = os.path.join(reports_dir, "directory_organization.csv")

        self._set_state_loading("Fetching directory organization configuration...")
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
            
            org_service = OrganizationService(client)
            org_list = org_service.get_organization_info(self.log_msg)
            client.close()

            if self.is_cancelled or request_id != self.current_request_id:
                return

            # Write Organization CSV
            script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
            if os.path.basename(script_dir) == "directory":
                script_dir = os.path.dirname(script_dir)
            reports_dir = os.path.join(script_dir, "reports", f"{tenant}_{client_id}")
            os.makedirs(reports_dir, exist_ok=True)
            
            org_headers = [
                "displayName", 
                "isMultipleDataLocationsForServicesEnabled", "onPremisesSyncEnabled", 
                "onPremisesLastSyncDateTime", "partnerTenantType", "tenantType",
                "provisionedPlans_service", "provisionedPlans_capabilityStatus", "provisionedPlans_provisioningStatus"
            ]
            org_rows = []
            
            def format_csv_val(v):
                return "null" if v is None else str(v)

            for org in org_list:
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

            with open(self.csv_path, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(org_headers)
                writer.writerows(org_rows)

            db_path = os.path.join(reports_dir, "telemetry_cache.db")
            asyncio.run(import_csv_to_sqlite(self.csv_path, db_path, "directory_organization"))

            self.after(0, self._render_success, org_list, request_id)
        except Exception as e:
            usage_logger.error(f"Error fetching Organization info: {e}", exc_info=True)
            if not self.is_cancelled and request_id == self.current_request_id:
                self.after(0, self._render_error, str(e), request_id)
        finally:
            if self.semaphore:
                self.semaphore.release()

    def _render_success(self, org_list, request_id):
        if self.is_cancelled or request_id != self.current_request_id:
            return
        self.status = "success"
        self._cached_org_data = org_list
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

        org_grid = ctk.CTkFrame(self.body_frame, fg_color=COLOR_OUTLINE_LIGHT, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)
        org_grid.pack(fill="x", expand=True, pady=(5, 10))

        org_grid.grid_columnconfigure(0, weight=1)
        org_grid.grid_columnconfigure(1, weight=3)

        org_headers = ["Property", "Value"]
        for col_idx, head_text in enumerate(org_headers):
            cell = ctk.CTkFrame(org_grid, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        org = self._cached_org_data[0] if self._cached_org_data else {}
        
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

            c0 = ctk.CTkFrame(org_grid, fg_color=bg_style, corner_radius=0)
            c0.grid(row=r_idx, column=0, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c0, text=prop_name, font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=8, anchor="nw")

            c1 = ctk.CTkFrame(org_grid, fg_color=bg_style, corner_radius=0)
            c1.grid(row=r_idx, column=1, sticky="nsew", padx=0, pady=(0, 1))
            
            wraplen = 600 if prop_name == "provisionedPlans" else None
            lbl = ctk.CTkLabel(c1, text=str(val), font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="left")
            if wraplen:
                lbl.configure(wraplength=wraplen)
            lbl.pack(padx=10, pady=8, anchor="nw")

        org_footnote = ctk.CTkLabel(
            self.body_frame,
            text="* If OnPremisesSyncEnabled returns True, on-premises Active Directory is a primary source of truth. If it returns Null or False, the directory is cloud-managed or driven by a 3rd-party application.",
            font=FONT_BODY_SMALL,
            text_color=COLOR_TEXT_SUB,
            anchor="w",
            justify="left",
            wraplength=1100
        )
        org_footnote.pack(fill="x", padx=10, pady=(0, 5))

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
        if hasattr(self, "_cached_org_data") and self._cached_org_data:
            return self._cached_org_data
        # Fallback load from SQLite
        if not self.csv_path:
            return []
            
        reports_dir = os.path.dirname(self.csv_path)
        db_path = os.path.join(reports_dir, "telemetry_cache.db")
        if not os.path.exists(db_path):
            return []
            
        items = []
        try:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM directory_organization")
            
            org_map = {}
            for row in cursor.fetchall():
                disp_name = row["displayName"]
                if disp_name not in org_map:
                    org_map[disp_name] = {
                        "displayName": disp_name,
                        "isMultipleDataLocationsForServicesEnabled": row["isMultipleDataLocationsForServicesEnabled"] if row["isMultipleDataLocationsForServicesEnabled"] != "null" else None,
                        "onPremisesSyncEnabled": row["onPremisesSyncEnabled"] if row["onPremisesSyncEnabled"] != "null" else None,
                        "onPremisesLastSyncDateTime": row["onPremisesLastSyncDateTime"] if row["onPremisesLastSyncDateTime"] != "null" else None,
                        "partnerTenantType": row["partnerTenantType"] if row["partnerTenantType"] != "null" else None,
                        "tenantType": row["tenantType"] if row["tenantType"] != "null" else None,
                        "provisionedPlans": []
                    }
                if row["provisionedPlans_service"] != "null":
                    org_map[disp_name]["provisionedPlans"].append({
                        "service": row["provisionedPlans_service"],
                        "capabilityStatus": row["provisionedPlans_capabilityStatus"],
                        "provisioningStatus": row["provisionedPlans_provisioningStatus"]
                    })
            items = list(org_map.values())
            conn.close()
        except Exception:
            pass
        return items
