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

"""Modular Data Security and Governance telemetry scanner and visual interface."""

import os
import logging
import threading
import customtkinter as ctk
import webbrowser

from core.graph.client import GraphClient
from core.graph.security import SecurityService

# Bind to the async logger initialized in license_usage.py
usage_logger = logging.getLogger("LicenseUsageAsyncLogger")

# Import shared styles
from telemetry.styles import *

def format_dlp_workloads(policy) -> str:
    """Formats active workloads/locations for a DLP policy as a friendly string."""
    active = []
    if policy.get("ExchangeLocation"):
        active.append("Exchange")
    if policy.get("SharePointLocation"):
        active.append("SharePoint")
    if policy.get("OneDriveLocation"):
        active.append("OneDrive")
    if policy.get("TeamsLocation"):
        active.append("Teams")
    if policy.get("DevicesLocation"):
        active.append("Devices (Endpoints)")
    return ", ".join(active) if active else "None"

def analyze_thick_client_enforcement(policy) -> str:
    """Analyzes the rules of a DLP policy to determine thick client (desktop app) enforcement details."""
    devices = policy.get("DevicesLocation")
    if not devices:
        return "Not Configured (Devices workload inactive)"
    
    rules = policy.get("Rules", [])
    if not rules:
        return "Devices Workload Active (No rules configured)"
        
    has_restricted_apps = False
    has_other_endpoint_restrictions = False
    
    for rule in rules:
        if not rule.get("Enabled", True):
            continue
        actions = rule.get("Actions", [])
        for act in actions:
            act_str = str(act).lower()
            if "restrictedapp" in act_str:
                has_restricted_apps = True
            elif any(keyword in act_str for keyword in ["clipboard", "print", "removablemedia", "usb", "networkshare", "cloudlookup", "bluetooth"]):
                has_other_endpoint_restrictions = True
                
    if has_restricted_apps and has_other_endpoint_restrictions:
        return "Purview Restricted Apps & Device Restrictions (Clipboard/USB/Print)"
    elif has_restricted_apps:
        return "Purview Restricted Apps"
    elif has_other_endpoint_restrictions:
        return "Purview Device Restrictions (Clipboard/USB/Print)"
    else:
        return "Devices Workload Active (Audit/Alert Only)"

def run_security_governance_pipeline(client_id, client_secret, tenant_id) -> dict:
    """Pipeline specifically for security and governance policy data collection."""
    usage_logger.info("Starting Data Security & Governance Pipeline...")
    
    client = GraphClient(
        tenant_id=tenant_id,
        client_ids=client_id,
        client_secrets=client_secret,
        concurrency=1,
        retries=3,
        backoff=2
    )
    client.authenticate()
    service = SecurityService(client)
    
    labels = None
    labels_error = None
    
    # Fetch Sensitivity Labels
    try:
        labels = service.fetch_sensitivity_labels()
        # Sort labels by priority descending
        labels.sort(key=lambda x: x.get("priority", 0), reverse=True)
    except Exception as e:
        usage_logger.error("Failed to fetch sensitivity labels", exc_info=True)
        labels_error = str(e)
        
    # Fetch tenant primary domain name from organization endpoint
    tenant_domain = tenant_id
    try:
        from core.graph.directory import DirectoryService
        dir_svc = DirectoryService(client)
        tenant_domain = dir_svc.get_tenant_primary_domain()
        usage_logger.info(f"Retrieved primary tenant domain for Connect-IPPSSession: {tenant_domain}")
    except Exception as e:
        usage_logger.warning(f"Could not retrieve tenant domain via Graph. Falling back to Tenant ID Guid: {e}")

    client.close()
    
    # Fetch Retention Policies via PowerShell client
    policies = None
    policies_error = None
    try:
        from core.powershell.client import PowerShellClient
        from core.powershell.retention import RetentionService
        
        ps_client = PowerShellClient(tenant_id=tenant_domain, client_id=client_id, client_secret=client_secret)
        retention_service = RetentionService(ps_client)
        policies = retention_service.fetch_retention_policies()
    except Exception as e:
        usage_logger.error("Failed to fetch retention policies via PowerShell", exc_info=True)
        policies_error = str(e)
        
    # Fetch DLP Policies via PowerShell client
    dlp_policies = None
    dlp_policies_error = None
    try:
        from core.powershell.client import PowerShellClient
        from core.powershell.dlp import DlpService
        
        ps_client = PowerShellClient(tenant_id=tenant_domain, client_id=client_id, client_secret=client_secret)
        dlp_service = DlpService(ps_client)
        dlp_policies = dlp_service.fetch_dlp_policies()
    except Exception as e:
        usage_logger.error("Failed to fetch DLP compliance policies via PowerShell", exc_info=True)
        dlp_policies_error = str(e)
        
    # Raise ConnectionError only if ALL failed
    if labels_error and policies_error and dlp_policies_error:
        raise ConnectionError(
            f"Security governance fetch failed.\n"
            f"Labels Error: {labels_error}\n"
            f"Policies Error: {policies_error}\n"
            f"DLP Error: {dlp_policies_error}"
        )
        
    usage_logger.info("Data Security & Governance Pipeline completed successfully.")
    return {
        "labels": labels,
        "labels_error": labels_error,
        "policies": policies,
        "policies_error": policies_error,
        "dlp_policies": dlp_policies,
        "dlp_policies_error": dlp_policies_error
    }


class DataSecurityGovernanceFrame(ctk.CTkFrame):
    """Self-contained customtkinter component wrapping Data Security & Governance UI."""
    
    def __init__(self, master, log_callback, credentials_callback, status_change_callback, **kwargs):
        super().__init__(master, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=12, **kwargs)
        self.log_msg = log_callback
        self.get_credentials = credentials_callback
        self.on_status_change = status_change_callback
        self.status = None  # 'loading', 'success', 'error', None
        
        self.flattened_rows = []
        self.current_page = 0
        self.ITEMS_PER_PAGE = 8
        self.last_labels_data = None
        self.last_policies_data = None
        self.last_dlp_policies_data = None
        
        self.build_ui()

    def build_ui(self):
        """Creates card container for the tab."""
        self.pack(fill="x", expand=True, pady=10)
        
        self.inner_pad = ctk.CTkFrame(self, fg_color="transparent")
        self.inner_pad.pack(fill="both", expand=True, padx=20, pady=20)
        
        # Permanent section heading visible during loading and error states
        self.main_title = ctk.CTkLabel(self.inner_pad, text="Data Security & Governance", font=FONT_HEADER_SMALL, text_color=COLOR_TEXT_MAIN)
        self.main_title.pack(anchor="w", pady=(0, 10))
        
        self.state_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        
        # Sensitivity Labels section header
        self.labels_header_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.labels_title = ctk.CTkLabel(
            self.labels_header_frame,
            text="Sensitivity Labels",
            font=FONT_HEADER_SMALL,
            text_color=COLOR_TEXT_MAIN
        )
        self.labels_title.pack(side="left", anchor="w")
        
        self.btn_export_labels = ctk.CTkButton(
            self.labels_header_frame,
            text="Export Sensitivity Labels",
            font=FONT_BODY_BOLD,
            fg_color="transparent",
            text_color=COLOR_PRIMARY,
            border_width=1,
            border_color=COLOR_OUTLINE,
            hover_color=COLOR_SECONDARY_HOVER,
            width=180,
            height=32,
            corner_radius=16,
            command=self.export_labels_csv,
            state="disabled"
        )
        self.btn_export_labels.pack(side="right", anchor="e")

        # Grid for Sensitivity Labels
        self.labels_grid = ctk.CTkFrame(
            self.inner_pad,
            fg_color=COLOR_OUTLINE_LIGHT,
            border_color=COLOR_OUTLINE_LIGHT,
            border_width=1,
            corner_radius=8
        )
        
        # Pagination controls frame (centered below the grid)
        self.pagination_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        
        self.btn_prev = ctk.CTkButton(
            self.pagination_frame,
            text="◀ Prev",
            command=self._prev_page,
            width=80,
            fg_color="transparent",
            border_width=1,
            text_color=COLOR_PRIMARY,
            border_color=COLOR_PRIMARY,
            hover_color=COLOR_SECONDARY_HOVER
        )
        self.btn_prev.pack(side="left", padx=10)
        
        self.lbl_page_info = ctk.CTkLabel(
            self.pagination_frame,
            text="Page 1 of 1",
            font=FONT_BODY_MEDIUM,
            text_color=COLOR_TEXT_MAIN
        )
        self.lbl_page_info.pack(side="left", padx=10)
        
        self.btn_next = ctk.CTkButton(
            self.pagination_frame,
            text="Next ▶",
            command=self._next_page,
            width=80,
            fg_color="transparent",
            border_width=1,
            text_color=COLOR_PRIMARY,
            border_color=COLOR_PRIMARY,
            hover_color=COLOR_SECONDARY_HOVER
        )
        self.btn_next.pack(side="left", padx=10)
        
        # Retention Policies section
        self.retention_header_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.retention_title = ctk.CTkLabel(
            self.retention_header_frame,
            text="Retention Compliance Policies",
            font=FONT_HEADER_SMALL,
            text_color=COLOR_TEXT_MAIN
        )
        self.retention_title.pack(side="left", anchor="w")
        
        self.retention_link = ctk.CTkLabel(
            self.retention_header_frame,
            text="Open Microsoft Purview Portal ↗",
            font=FONT_BODY_BOLD,
            text_color=COLOR_PRIMARY,
            cursor="hand2"
        )
        self.retention_link.pack(side="left", anchor="w", padx=(15, 0))
        self.retention_link.bind("<Button-1>", lambda e: webbrowser.open("https://purview.microsoft.com/datalifecyclemanagement/retention"))
        self.retention_link.bind("<Enter>", lambda e: self.retention_link.configure(text_color=COLOR_PRIMARY_HOVER))
        self.retention_link.bind("<Leave>", lambda e: self.retention_link.configure(text_color=COLOR_PRIMARY))

        self.btn_export_retention = ctk.CTkButton(
            self.retention_header_frame,
            text="Export Retention Policies",
            font=FONT_BODY_BOLD,
            fg_color="transparent",
            text_color=COLOR_PRIMARY,
            border_width=1,
            border_color=COLOR_OUTLINE,
            hover_color=COLOR_SECONDARY_HOVER,
            width=180,
            height=32,
            corner_radius=16,
            command=self.export_retention_csv,
            state="disabled"
        )
        self.btn_export_retention.pack(side="right", anchor="e")
        
        self.retention_grid = ctk.CTkFrame(
            self.inner_pad,
            fg_color=COLOR_SURFACE,
            border_color=COLOR_OUTLINE_LIGHT,
            border_width=1,
            corner_radius=8
        )

        # DLP Compliance Policies section
        self.dlp_header_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.dlp_title = ctk.CTkLabel(
            self.dlp_header_frame,
            text="DLP Compliance Policies",
            font=FONT_HEADER_SMALL,
            text_color=COLOR_TEXT_MAIN
        )
        self.dlp_title.pack(side="left", anchor="w")
        
        self.dlp_link = ctk.CTkLabel(
            self.dlp_header_frame,
            text="Open Microsoft Purview Portal ↗",
            font=FONT_BODY_BOLD,
            text_color=COLOR_PRIMARY,
            cursor="hand2"
        )
        self.dlp_link.pack(side="left", anchor="w", padx=(15, 0))
        self.dlp_link.bind("<Button-1>", lambda e: webbrowser.open("https://purview.microsoft.com/datalossprevention/policies"))
        self.dlp_link.bind("<Enter>", lambda e: self.dlp_link.configure(text_color=COLOR_PRIMARY_HOVER))
        self.dlp_link.bind("<Leave>", lambda e: self.dlp_link.configure(text_color=COLOR_PRIMARY))

        self.btn_export_dlp = ctk.CTkButton(
            self.dlp_header_frame,
            text="Export DLP Policies",
            font=FONT_BODY_BOLD,
            fg_color="transparent",
            text_color=COLOR_PRIMARY,
            border_width=1,
            border_color=COLOR_OUTLINE,
            hover_color=COLOR_SECONDARY_HOVER,
            width=180,
            height=32,
            corner_radius=16,
            command=self.export_dlp_csv,
            state="disabled"
        )
        self.btn_export_dlp.pack(side="right", anchor="e")
        
        self.dlp_grid = ctk.CTkFrame(
            self.inner_pad,
            fg_color=COLOR_SURFACE,
            border_color=COLOR_OUTLINE_LIGHT,
            border_width=1,
            corner_radius=8
        )
        
        self.reset_view()

    def reset_view(self):
        """Resets and hides grids."""
        self.pack_forget()
        self.state_frame.pack_forget()
        self.labels_grid.pack_forget()
        self.pagination_frame.pack_forget()
        self.labels_header_frame.pack_forget()
        self.retention_header_frame.pack_forget()
        self.retention_grid.pack_forget()
        self.dlp_header_frame.pack_forget()
        self.dlp_grid.pack_forget()
        
        for w in self.state_frame.winfo_children():
            w.destroy()
        for w in self.labels_grid.winfo_children():
            w.destroy()
        for w in self.retention_grid.winfo_children():
            w.destroy()
        for w in self.dlp_grid.winfo_children():
            w.destroy()
            
        self.flattened_rows = []
        self.current_page = 0
        
        if hasattr(self, "btn_export_labels"):
            self.btn_export_labels.configure(state="disabled")
        if hasattr(self, "btn_export_retention"):
            self.btn_export_retention.configure(state="disabled")
        if hasattr(self, "btn_export_dlp"):
            self.btn_export_dlp.configure(state="disabled")

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
            display_msg = "Information Protection permission required.\nPlease grant the 'SensitivityLabels.Read.All' application permission to your App Registration in Microsoft Entra ID."

        ctk.CTkLabel(self.state_frame, text=f"✖ {display_msg}", text_color=COLOR_ERROR, font=FONT_BODY_MEDIUM, justify="center").pack(pady=(20, 10))
        ctk.CTkButton(self.state_frame, text="Try Again", command=self._retry_fetch, width=120, fg_color="transparent", border_width=1, text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER).pack(pady=(0, 20))
        self.state_frame.pack(fill="x", expand=True)

    def _retry_fetch(self):
        tenant, clients, secrets = self.get_credentials()
        if tenant:
            self.trigger_fetch(tenant, clients[0], secrets[0])

    def trigger_fetch(self, tenant, client_id, client_secret):
        """Triggers parallel fetches inside isolated background threads."""
        usage_logger.info("Data Security & Governance trigger_fetch called. Spawning background worker thread...")
        self.status = "loading"
        self.on_status_change()
        
        self.pack(fill="x", expand=True, pady=(20, 10))
        self.labels_grid.pack_forget()
        self.labels_header_frame.pack_forget()
        self.retention_header_frame.pack_forget()
        self.retention_grid.pack_forget()
        self.dlp_header_frame.pack_forget()
        self.dlp_grid.pack_forget()
        
        self._set_state_loading("Retrieving tenant Security & Compliance policies...")
        
        self.btn_export_labels.configure(state="disabled")
        self.btn_export_retention.configure(state="disabled")
        self.btn_export_dlp.configure(state="disabled")
        
        threading.Thread(
            target=self._execute_worker,
            args=(tenant, client_id, client_secret),
            daemon=True
        ).start()

    def _execute_worker(self, tenant: str, client_id: str, client_secret: str):
        usage_logger.info("Executing thread: _execute_security_governance_worker")
        try:
            data = run_security_governance_pipeline(client_id, client_secret, tenant)
            usage_logger.info("Successfully completed Data Security & Governance policy fetch.")
            self.after(0, self._render_success, data)
        except Exception as e:
            usage_logger.error("Exception caught in Data Security & Governance worker.", exc_info=True)
            self.after(0, self._render_error, str(e))

    def _render_success(self, data: dict):
        self.state_frame.pack_forget()
        for w in self.labels_grid.winfo_children():
            w.destroy()
        for w in self.retention_grid.winfo_children():
            w.destroy()
        for w in self.dlp_grid.winfo_children():
            w.destroy()

        labels = data.get("labels")
        labels_error = data.get("labels_error")
        policies = data.get("policies")
        policies_error = data.get("policies_error")
        dlp_policies = data.get("dlp_policies")
        dlp_policies_error = data.get("dlp_policies_error")

        self.last_labels_data = labels
        self.last_policies_data = policies
        self.last_dlp_policies_data = dlp_policies

        usage_logger.info(f"Sensitivity Labels fetched successfully. Total labels to render: {len(labels) if labels else 0}")
        self.status = "success"

        # 1. Render Sensitivity Labels Grid
        self.labels_header_frame.pack(fill="x", pady=(0, 10))
        self.labels_grid.pack(fill="x", expand=True, pady=(0, 15))

        if labels_error:
            ctk.CTkLabel(self.labels_grid, text=f"✖ Failed to load Sensitivity Labels: {labels_error}", font=FONT_BODY_MEDIUM, text_color=COLOR_ERROR).pack(padx=20, pady=20)
            self.pagination_frame.pack_forget()
            self.btn_export_labels.configure(state="disabled")
        elif labels is None or not labels:
            ctk.CTkLabel(self.labels_grid, text="No Sensitivity Labels configured in this tenant.", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB).pack(padx=20, pady=20)
            self.pagination_frame.pack_forget()
            self.btn_export_labels.configure(state="disabled")
        else:
            self.btn_export_labels.configure(state="normal")
            # Define column weights for proper proportional spacing
            self.labels_grid.grid_columnconfigure(0, weight=2)  # Label Name
            self.labels_grid.grid_columnconfigure(1, weight=3)  # Description
            self.labels_grid.grid_columnconfigure(2, weight=1)  # Protection
            self.labels_grid.grid_columnconfigure(3, weight=1)  # Mode
            self.labels_grid.grid_columnconfigure(4, weight=1)  # Priority
            self.labels_grid.grid_columnconfigure(5, weight=2)  # Applicable To
            self.labels_grid.grid_columnconfigure(6, weight=1)  # Status

            headers = ["Sensitivity Label", "Description", "Protection", "Mode", "Priority", "Applicable Targets", "Status"]
            for col_idx, head_text in enumerate(headers):
                cell = ctk.CTkFrame(self.labels_grid, fg_color=COLOR_TONAL_BG, corner_radius=0)
                cell.grid(row=0, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
                ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

            # Flatten parent labels and their sorted sublabels
            self.flattened_rows = []
            for parent in labels:
                self.flattened_rows.append({
                    "name": parent.get("name", "N/A"),
                    "description": parent.get("description", "") or parent.get("toolTip", "") or "N/A",
                    "hasProtection": parent.get("hasProtection", False),
                    "applicationMode": parent.get("applicationMode", "N/A") or "N/A",
                    "priority": parent.get("priority", 0),
                    "applicableTo": parent.get("applicableTo", ""),
                    "isEnabled": parent.get("isEnabled", True),
                    "is_sublabel": False
                })
                
                sublabels = parent.get("sublabels", [])
                if sublabels:
                    # Sort sublabels by priority descending
                    sublabels_sorted = sorted(sublabels, key=lambda x: x.get("priority", 0), reverse=True)
                    for sub in sublabels_sorted:
                        self.flattened_rows.append({
                            "name": f"    ↳  {sub.get('name', 'N/A')}",
                            "description": sub.get("description", "") or sub.get("toolTip", "") or "N/A",
                            "hasProtection": sub.get("hasProtection", False),
                            "applicationMode": sub.get("applicationMode", "N/A") or "N/A",
                            "priority": sub.get("priority", 0),
                            "applicableTo": sub.get("applicableTo", ""),
                            "isEnabled": sub.get("isEnabled", True),
                            "is_sublabel": True
                        })

            self.current_page = 0
            self._display_current_page()

            if len(self.flattened_rows) > self.ITEMS_PER_PAGE:
                self.pagination_frame.pack(pady=(5, 10))
            else:
                self.pagination_frame.pack_forget()

        # 2. Render Retention Policies Grid
        self.retention_header_frame.pack(fill="x", pady=(20, 10))
        self.retention_grid.pack(fill="x", expand=True, pady=(0, 15))
        self._render_retention_policies(policies, policies_error)

        # 3. Render DLP Policies Grid
        self.dlp_header_frame.pack(fill="x", pady=(20, 10))
        self.dlp_grid.pack(fill="x", expand=True, pady=(0, 15))
        self._render_dlp_policies(dlp_policies, dlp_policies_error)

        self.on_status_change()

    def _display_current_page(self):
        # Destroy existing data rows (row > 0)
        for w in self.labels_grid.winfo_children():
            info = w.grid_info()
            if "row" in info and int(info["row"]) > 0:
                w.destroy()

        usage_logger.info(f"Displaying page {self.current_page + 1} of Sensitivity Labels.")

        total_items = len(self.flattened_rows)
        total_pages = (total_items + self.ITEMS_PER_PAGE - 1) // self.ITEMS_PER_PAGE
        if total_pages < 1:
            total_pages = 1

        # Bounds safety check
        if self.current_page >= total_pages:
            self.current_page = total_pages - 1
        if self.current_page < 0:
            self.current_page = 0

        start_idx = self.current_page * self.ITEMS_PER_PAGE
        end_idx = min(start_idx + self.ITEMS_PER_PAGE, total_items)

        page_items = self.flattened_rows[start_idx:end_idx]

        for offset, row_item in enumerate(page_items, start=1):
            r_idx = offset
            bg_style = COLOR_SURFACE if r_idx % 2 == 0 else COLOR_SURFACE_VARIANT
            
            name = row_item["name"]
            desc = row_item["description"]
            protection = "🛡️ Yes" if row_item["hasProtection"] else "🔓 No"
            mode = str(row_item["applicationMode"]).capitalize()
            priority = str(row_item["priority"])
            applicable = ", ".join([x.capitalize() for x in row_item["applicableTo"].split(",") if x.strip()]) or "N/A"
            status = "🟢 Enabled" if row_item["isEnabled"] else "🔴 Disabled"
            is_sublabel = row_item["is_sublabel"]

            name_color = COLOR_TEXT_MAIN if not is_sublabel else COLOR_TEXT_SUB
            name_font = FONT_BODY_BOLD if not is_sublabel else FONT_BODY_MEDIUM

            c0 = ctk.CTkFrame(self.labels_grid, fg_color=bg_style, corner_radius=0)
            c0.grid(row=r_idx, column=0, sticky="nsew", padx=0, pady=(0, 1))
            lbl_name = ctk.CTkLabel(c0, text=name, font=name_font, text_color=name_color)
            lbl_name.pack(padx=10, pady=6, anchor="w")
            c0.bind("<Configure>", lambda e, l=lbl_name: l.configure(wraplength=e.width - 20))

            c1 = ctk.CTkFrame(self.labels_grid, fg_color=bg_style, corner_radius=0)
            c1.grid(row=r_idx, column=1, sticky="nsew", padx=0, pady=(0, 1))
            lbl_desc = ctk.CTkLabel(c1, text=desc, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN)
            lbl_desc.pack(padx=10, pady=6, anchor="w")
            c1.bind("<Configure>", lambda e, l=lbl_desc: l.configure(wraplength=e.width - 20))

            c2 = ctk.CTkFrame(self.labels_grid, fg_color=bg_style, corner_radius=0)
            c2.grid(row=r_idx, column=2, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c2, text=protection, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=6, anchor="w")

            c3 = ctk.CTkFrame(self.labels_grid, fg_color=bg_style, corner_radius=0)
            c3.grid(row=r_idx, column=3, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c3, text=mode, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=6, anchor="w")

            c4 = ctk.CTkFrame(self.labels_grid, fg_color=bg_style, corner_radius=0)
            c4.grid(row=r_idx, column=4, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c4, text=priority, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=6, anchor="w")

            c5 = ctk.CTkFrame(self.labels_grid, fg_color=bg_style, corner_radius=0)
            c5.grid(row=r_idx, column=5, sticky="nsew", padx=0, pady=(0, 1))
            lbl_app = ctk.CTkLabel(c5, text=applicable, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN)
            lbl_app.pack(padx=10, pady=6, anchor="w")
            c5.bind("<Configure>", lambda e, l=lbl_app: l.configure(wraplength=e.width - 20))

            c6 = ctk.CTkFrame(self.labels_grid, fg_color=bg_style, corner_radius=0)
            c6.grid(row=r_idx, column=6, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c6, text=status, font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=6, anchor="w")

        # Update page info label
        self.lbl_page_info.configure(text=f"Page {self.current_page + 1} of {total_pages}")

        # Update navigation button states
        if self.current_page <= 0:
            self.btn_prev.configure(state="disabled", text_color=COLOR_TEXT_SUB, border_color=COLOR_OUTLINE_LIGHT)
        else:
            self.btn_prev.configure(state="normal", text_color=COLOR_PRIMARY, border_color=COLOR_PRIMARY)

        if self.current_page >= total_pages - 1:
            self.btn_next.configure(state="disabled", text_color=COLOR_TEXT_SUB, border_color=COLOR_OUTLINE_LIGHT)
        else:
            self.btn_next.configure(state="normal", text_color=COLOR_PRIMARY, border_color=COLOR_PRIMARY)

    def _prev_page(self):
        if self.current_page > 0:
            self.current_page -= 1
            self._display_current_page()

    def _next_page(self):
        total_items = len(self.flattened_rows)
        total_pages = (total_items + self.ITEMS_PER_PAGE - 1) // self.ITEMS_PER_PAGE
        if self.current_page < total_pages - 1:
            self.current_page += 1
            self._display_current_page()

    def _render_error(self, err_msg):
        usage_logger.warning(f"Data Security & Governance fetch failed: {err_msg}")
        self._set_state_error(err_msg)
        self.status = "error"
        self.on_status_change()
        self.btn_export_labels.configure(state="disabled")
        self.btn_export_retention.configure(state="disabled")
        self.btn_export_dlp.configure(state="disabled")

    def _render_retention_policies(self, policies, policies_error):
        if policies_error:
            msg = policies_error
            # Provide helpful, friendly advice if pwsh or dependency issue
            if "powershell" in policies_error.lower() or "pwsh" in policies_error.lower():
                msg = "PowerShell Core ('pwsh') is not installed or configured on this machine.\nPlease refer to the Prerequisites in the README to configure it."
            elif "exchangeonlinemanagement" in policies_error.lower():
                msg = "ExchangeOnlineManagement PowerShell module is missing.\nPlease run: Install-Module -Name ExchangeOnlineManagement -Scope CurrentUser"
                
            ctk.CTkLabel(
                self.retention_grid, 
                text=f"✖ {msg}", 
                font=FONT_BODY_MEDIUM, 
                text_color=COLOR_ERROR,
                justify="center"
            ).pack(padx=20, pady=20)
            self.btn_export_retention.configure(state="disabled")
        elif policies is None or not policies:
            ctk.CTkLabel(
                self.retention_grid, 
                text="No Retention Compliance Policies found in this tenant.", 
                font=FONT_BODY_MEDIUM, 
                text_color=COLOR_TEXT_SUB
            ).pack(padx=20, pady=20)
            self.btn_export_retention.configure(state="disabled")
        else:
            self.btn_export_retention.configure(state="normal")
            # Configure grid columns
            self.retention_grid.grid_columnconfigure(0, weight=3)  # Policy Name
            self.retention_grid.grid_columnconfigure(1, weight=3)  # Workloads
            self.retention_grid.grid_columnconfigure(2, weight=2)  # Duration & Trigger
            self.retention_grid.grid_columnconfigure(3, weight=1)  # Mode
            self.retention_grid.grid_columnconfigure(4, weight=1)  # Distribution
            self.retention_grid.grid_columnconfigure(5, weight=1)  # Status

            headers = ["Policy Name", "Workloads", "Duration", "Mode", "Distribution", "Status"]
            for col_idx, head_text in enumerate(headers):
                cell = ctk.CTkFrame(self.retention_grid, fg_color=COLOR_TONAL_BG, corner_radius=0)
                cell.grid(row=0, column=col_idx, sticky="nsew", padx=1, pady=1)
                ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

            # Handle case where policies is a single dict rather than a list
            policies_list = policies if isinstance(policies, list) else [policies]

            for r_idx, policy in enumerate(policies_list, start=1):
                bg_style = "transparent" if r_idx % 2 != 0 else COLOR_SURFACE_VARIANT

                name = policy.get("Name", "N/A")
                comment = policy.get("Comment", "")
                workload = policy.get("Workload", "N/A")
                duration_val = str(policy.get("Duration", "N/A"))
                trigger_val = policy.get("RetentionTrigger", "N/A")
                mode = policy.get("Mode", "Enforce")
                dist_status = policy.get("DistributionStatus", "Success")
                
                # Format Duration nicely
                duration_str = duration_val
                if duration_val.lower() == "unlimited":
                    duration_str = "Keep Forever"
                elif duration_val.isdigit():
                    days = int(duration_val)
                    if days >= 365:
                        years = days / 365.0
                        if years.is_integer():
                            duration_str = f"{int(years)} Years ({days} days)"
                        else:
                            duration_str = f"{years:.1f} Years ({days} days)"
                    else:
                        duration_str = f"{days} days"
                
                # Append trigger details to duration string if present and not N/A
                if trigger_val and trigger_val != "N/A":
                    trigger_map = {
                        "DateCreated": "created date",
                        "DateModified": "last modified date",
                        "DateLabeled": "labeled date"
                    }
                    friendly_trigger = trigger_map.get(trigger_val, trigger_val)
                    duration_str += f"\n(from {friendly_trigger})"

                # Enabled can be boolean or string
                enabled_val = policy.get("Enabled", True)
                if isinstance(enabled_val, str):
                    is_enabled = enabled_val.lower() == "true"
                else:
                    is_enabled = bool(enabled_val)
                    
                status = "🟢 Enabled" if is_enabled else "🔴 Disabled"

                c0 = ctk.CTkFrame(self.retention_grid, fg_color=bg_style, corner_radius=0)
                c0.grid(row=r_idx, column=0, sticky="nsew", padx=1, pady=1)
                
                has_comment = bool(comment and comment != name)
                lbl_name = ctk.CTkLabel(c0, text=name, font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN)
                lbl_name.pack(padx=10, pady=(6, 2) if has_comment else 6, anchor="w")
                
                if has_comment:
                    lbl_comment = ctk.CTkLabel(c0, text=comment, font=FONT_BODY_SMALL, text_color=COLOR_TEXT_SUB)
                    lbl_comment.pack(padx=10, pady=(0, 6), anchor="w")
                    c0.bind("<Configure>", lambda e, l1=lbl_name, l2=lbl_comment: (l1.configure(wraplength=e.width - 20), l2.configure(wraplength=e.width - 20)))
                else:
                    c0.bind("<Configure>", lambda e, l=lbl_name: l.configure(wraplength=e.width - 20))

                c1 = ctk.CTkFrame(self.retention_grid, fg_color=bg_style, corner_radius=0)
                c1.grid(row=r_idx, column=1, sticky="nsew", padx=1, pady=1)
                lbl_workload = ctk.CTkLabel(c1, text=workload, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN)
                lbl_workload.pack(padx=10, pady=6, anchor="w")
                c1.bind("<Configure>", lambda e, l=lbl_workload: l.configure(wraplength=e.width - 20))

                c2 = ctk.CTkFrame(self.retention_grid, fg_color=bg_style, corner_radius=0)
                c2.grid(row=r_idx, column=2, sticky="nsew", padx=1, pady=1)
                lbl_duration = ctk.CTkLabel(c2, text=duration_str, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="left")
                lbl_duration.pack(padx=10, pady=6, anchor="w")
                c2.bind("<Configure>", lambda e, l=lbl_duration: l.configure(wraplength=e.width - 20))

                c3 = ctk.CTkFrame(self.retention_grid, fg_color=bg_style, corner_radius=0)
                c3.grid(row=r_idx, column=3, sticky="nsew", padx=1, pady=1)
                ctk.CTkLabel(c3, text=mode, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=6, anchor="w")

                c4 = ctk.CTkFrame(self.retention_grid, fg_color=bg_style, corner_radius=0)
                c4.grid(row=r_idx, column=4, sticky="nsew", padx=1, pady=1)
                ctk.CTkLabel(c4, text=dist_status, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=6, anchor="w")

                c5 = ctk.CTkFrame(self.retention_grid, fg_color=bg_style, corner_radius=0)
                c5.grid(row=r_idx, column=5, sticky="nsew", padx=1, pady=1)
                ctk.CTkLabel(c5, text=status, font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=6, anchor="w")

    def _render_dlp_policies(self, policies, policies_error):
        if policies_error:
            msg = policies_error
            # Provide helpful, friendly advice if pwsh or dependency issue
            if "powershell" in policies_error.lower() or "pwsh" in policies_error.lower():
                msg = "PowerShell Core ('pwsh') is not installed or configured on this machine.\nPlease refer to the Prerequisites in the README to configure it."
            elif "exchangeonlinemanagement" in policies_error.lower():
                msg = "ExchangeOnlineManagement PowerShell module is missing.\nPlease run: Install-Module -Name ExchangeOnlineManagement -Scope CurrentUser"
                
            ctk.CTkLabel(
                self.dlp_grid, 
                text=f"✖ {msg}", 
                font=FONT_BODY_MEDIUM, 
                text_color=COLOR_ERROR,
                justify="center"
            ).pack(padx=20, pady=20)
            self.btn_export_dlp.configure(state="disabled")
        elif policies is None or not policies:
            ctk.CTkLabel(
                self.dlp_grid, 
                text="No DLP Compliance Policies found in this tenant.", 
                font=FONT_BODY_MEDIUM, 
                text_color=COLOR_TEXT_SUB
            ).pack(padx=20, pady=20)
            self.btn_export_dlp.configure(state="disabled")
        else:
            self.btn_export_dlp.configure(state="normal")
            # Configure grid columns
            self.dlp_grid.grid_columnconfigure(0, weight=3)  # Policy Name
            self.dlp_grid.grid_columnconfigure(1, weight=3)  # Workloads
            self.dlp_grid.grid_columnconfigure(2, weight=1)  # Mode
            self.dlp_grid.grid_columnconfigure(3, weight=4)  # Thick Client Enforcement
            self.dlp_grid.grid_columnconfigure(4, weight=1)  # Status

            headers = ["Policy Name", "Workloads", "Mode", "Thick Client DLP Enforcement", "Status"]
            for col_idx, head_text in enumerate(headers):
                cell = ctk.CTkFrame(self.dlp_grid, fg_color=COLOR_TONAL_BG, corner_radius=0)
                cell.grid(row=0, column=col_idx, sticky="nsew", padx=1, pady=1)
                ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

            # Handle case where policies is a single dict rather than a list
            policies_list = policies if isinstance(policies, list) else [policies]

            for r_idx, policy in enumerate(policies_list, start=1):
                bg_style = "transparent" if r_idx % 2 != 0 else COLOR_SURFACE_VARIANT

                name = policy.get("Name", "N/A")
                comment = policy.get("Comment", "")
                workloads_str = format_dlp_workloads(policy)
                mode = policy.get("Mode", "Enforce")
                thick_client_status = analyze_thick_client_enforcement(policy)

                # Enabled can be boolean or string
                enabled_val = policy.get("Enabled", True)
                if isinstance(enabled_val, str):
                    is_enabled = enabled_val.lower() == "true"
                else:
                    is_enabled = bool(enabled_val)
                    
                status = "🟢 Enabled" if is_enabled else "🔴 Disabled"

                c0 = ctk.CTkFrame(self.dlp_grid, fg_color=bg_style, corner_radius=0)
                c0.grid(row=r_idx, column=0, sticky="nsew", padx=1, pady=1)
                
                has_comment = bool(comment and comment != name)
                lbl_name = ctk.CTkLabel(c0, text=name, font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN)
                lbl_name.pack(padx=10, pady=(6, 2) if has_comment else 6, anchor="w")
                
                if has_comment:
                    lbl_comment = ctk.CTkLabel(c0, text=comment, font=FONT_BODY_SMALL, text_color=COLOR_TEXT_SUB)
                    lbl_comment.pack(padx=10, pady=(0, 6), anchor="w")
                    c0.bind("<Configure>", lambda e, l1=lbl_name, l2=lbl_comment: (l1.configure(wraplength=e.width - 20), l2.configure(wraplength=e.width - 20)))
                else:
                    c0.bind("<Configure>", lambda e, l=lbl_name: l.configure(wraplength=e.width - 20))

                c1 = ctk.CTkFrame(self.dlp_grid, fg_color=bg_style, corner_radius=0)
                c1.grid(row=r_idx, column=1, sticky="nsew", padx=1, pady=1)
                lbl_workload = ctk.CTkLabel(c1, text=workloads_str, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN)
                lbl_workload.pack(padx=10, pady=6, anchor="w")
                c1.bind("<Configure>", lambda e, l=lbl_workload: l.configure(wraplength=e.width - 20))

                c2 = ctk.CTkFrame(self.dlp_grid, fg_color=bg_style, corner_radius=0)
                c2.grid(row=r_idx, column=2, sticky="nsew", padx=1, pady=1)
                ctk.CTkLabel(c2, text=mode, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=6, anchor="w")

                c3 = ctk.CTkFrame(self.dlp_grid, fg_color=bg_style, corner_radius=0)
                c3.grid(row=r_idx, column=3, sticky="nsew", padx=1, pady=1)
                lbl_thick = ctk.CTkLabel(c3, text=thick_client_status, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="left")
                lbl_thick.pack(padx=10, pady=6, anchor="w")
                c3.bind("<Configure>", lambda e, l=lbl_thick: l.configure(wraplength=e.width - 20))

                c4 = ctk.CTkFrame(self.dlp_grid, fg_color=bg_style, corner_radius=0)
                c4.grid(row=r_idx, column=4, sticky="nsew", padx=1, pady=1)
                ctk.CTkLabel(c4, text=status, font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=6, anchor="w")

    def export_labels_csv(self):
        """Prompts the user to save sensitivity labels as a detailed CSV file."""
        if not hasattr(self, "last_labels_data") or not self.last_labels_data:
            from tkinter import messagebox
            messagebox.showinfo("No Data", "There is no sensitivity labels data to export. Please run a scan first.", parent=self)
            return
            
        from tkinter import filedialog, messagebox
        from datetime import datetime
        import pandas as pd
        
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        f = filedialog.asksaveasfilename(
            initialfile=f"sensitivity_labels_{ts}.csv",
            defaultextension=".csv",
            filetypes=[("CSV Files", "*.csv"), ("All Files", "*.*")],
            parent=self
        )
        if not f:
            return
            
        # Flatten the labels (including sublabels) to export detailed records
        rows = []
        for parent in self.last_labels_data:
            parent_id = parent.get("id", "N/A")
            parent_name = parent.get("name", "N/A")
            
            rows.append({
                "Label ID": parent_id,
                "Label Name": parent_name,
                "Display Name": parent.get("displayName", "N/A"),
                "Description": parent.get("description", "") or parent.get("toolTip", "") or "N/A",
                "Priority": parent.get("priority", 0),
                "Applicable Targets": parent.get("applicableTo", "N/A"),
                "Is Enabled": parent.get("isEnabled", True),
                "Is Sublabel": False,
                "Parent Label ID": "",
                "Parent Label Name": ""
            })
            
            for sub in parent.get("sublabels", []):
                rows.append({
                    "Label ID": sub.get("id", "N/A"),
                    "Label Name": sub.get("name", "N/A"),
                    "Display Name": sub.get("displayName", "N/A"),
                    "Description": sub.get("description", "") or sub.get("toolTip", "") or "N/A",
                    "Priority": sub.get("priority", 0),
                    "Applicable Targets": sub.get("applicableTo", "N/A"),
                    "Is Enabled": sub.get("isEnabled", True),
                    "Is Sublabel": True,
                    "Parent Label ID": parent_id,
                    "Parent Label Name": parent_name
                })
                
        df = pd.DataFrame(rows)
        try:
            df.to_csv(f, index=False)
            messagebox.showinfo("Export Successful", f"Sensitivity labels exported successfully to:\n{f}", parent=self)
        except Exception as e:
            messagebox.showerror("Export Failed", f"Failed to export CSV: {e}", parent=self)

    def export_retention_csv(self):
        """Prompts the user to save retention policies as a detailed CSV file."""
        if not hasattr(self, "last_policies_data") or not self.last_policies_data:
            from tkinter import messagebox
            messagebox.showinfo("No Data", "There is no retention policies data to export. Please run a scan first.", parent=self)
            return
            
        from tkinter import filedialog, messagebox
        from datetime import datetime
        import pandas as pd
        
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        f = filedialog.asksaveasfilename(
            initialfile=f"retention_policies_{ts}.csv",
            defaultextension=".csv",
            filetypes=[("CSV Files", "*.csv"), ("All Files", "*.*")],
            parent=self
        )
        if not f:
            return
            
        policies_list = self.last_policies_data if isinstance(self.last_policies_data, list) else [self.last_policies_data]
        
        rows = []
        for policy in policies_list:
            duration_val = str(policy.get("Duration", "N/A"))
            duration_str = duration_val
            if duration_val.lower() == "unlimited":
                duration_str = "Keep Forever"
            elif duration_val.isdigit():
                days = int(duration_val)
                if days >= 365:
                    years = days / 365.0
                    duration_str = f"{int(years)} Years ({days} days)" if years.is_integer() else f"{years:.1f} Years ({days} days)"
                else:
                    duration_str = f"{days} days"
                    
            rows.append({
                "Policy Name": policy.get("Name", "N/A"),
                "Identity": policy.get("Identity", "N/A"),
                "Description / Comment": policy.get("Comment", "N/A"),
                "Workloads": policy.get("Workload", "N/A"),
                "Mode": policy.get("Mode", "N/A"),
                "Distribution Status": policy.get("DistributionStatus", "N/A"),
                "Is Enabled": policy.get("Enabled", True),
                "Duration Days": duration_val,
                "Duration Description": duration_str,
                "Retention Action": policy.get("RetentionAction", "N/A"),
                "Retention Trigger Basis": policy.get("RetentionTrigger", "N/A"),
                "When Created": policy.get("WhenCreated", "N/A"),
                "When Changed": policy.get("WhenChanged", "N/A"),
                "Created By": policy.get("CreatedBy", "N/A"),
                "Last Modified By": policy.get("LastModifiedBy", "N/A")
            })
            
        df = pd.DataFrame(rows)
        try:
            df.to_csv(f, index=False)
            messagebox.showinfo("Export Successful", f"Retention policies exported successfully to:\n{f}", parent=self)
        except Exception as e:
            messagebox.showerror("Export Failed", f"Failed to export CSV: {e}", parent=self)

    def export_dlp_csv(self):
        """Prompts the user to save DLP compliance policies as a detailed CSV file."""
        if not hasattr(self, "last_dlp_policies_data") or not self.last_dlp_policies_data:
            from tkinter import messagebox
            messagebox.showinfo("No Data", "There is no DLP policies data to export. Please run a scan first.", parent=self)
            return
            
        from tkinter import filedialog, messagebox
        from datetime import datetime
        import pandas as pd
        
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        f = filedialog.asksaveasfilename(
            initialfile=f"dlp_compliance_policies_{ts}.csv",
            defaultextension=".csv",
            filetypes=[("CSV Files", "*.csv"), ("All Files", "*.*")],
            parent=self
        )
        if not f:
            return
            
        policies_list = self.last_dlp_policies_data if isinstance(self.last_dlp_policies_data, list) else [self.last_dlp_policies_data]
        
        rows = []
        for policy in policies_list:
            workloads_str = format_dlp_workloads(policy)
            thick_client_status = analyze_thick_client_enforcement(policy)
            
            # Format rules details
            rules = policy.get("Rules", [])
            rules_details = []
            for r in rules:
                actions_str = ", ".join(r.get("Actions", []))
                rules_details.append(f"Rule: {r.get('Name')} (Enabled: {r.get('Enabled')}) - Actions: [{actions_str}]")
            rules_str = "\n".join(rules_details)
            
            rows.append({
                "Policy Name": policy.get("Name", "N/A"),
                "Identity": policy.get("Identity", "N/A"),
                "Description / Comment": policy.get("Comment", "N/A"),
                "Workloads": workloads_str,
                "Mode": policy.get("Mode", "N/A"),
                "Distribution Status": policy.get("DistributionStatus", "N/A"),
                "Is Enabled": policy.get("Enabled", True),
                "Thick Client DLP Enforcement": thick_client_status,
                "Exchange Locations": ", ".join(policy.get("ExchangeLocation") or []),
                "SharePoint Locations": ", ".join(policy.get("SharePointLocation") or []),
                "OneDrive Locations": ", ".join(policy.get("OneDriveLocation") or []),
                "Teams Locations": ", ".join(policy.get("TeamsLocation") or []),
                "Devices Locations": ", ".join(policy.get("DevicesLocation") or []),
                "Rules Configured": rules_str,
                "When Created": policy.get("WhenCreated", "N/A"),
                "When Changed": policy.get("WhenChanged", "N/A"),
                "Created By": policy.get("CreatedBy", "N/A"),
                "Last Modified By": policy.get("LastModifiedBy", "N/A")
            })
            
        df = pd.DataFrame(rows)
        try:
            df.to_csv(f, index=False)
            messagebox.showinfo("Export Successful", f"DLP compliance policies exported successfully to:\n{f}", parent=self)
        except Exception as e:
            messagebox.showerror("Export Failed", f"Failed to export CSV: {e}", parent=self)
