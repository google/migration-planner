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
from tkinter import messagebox, filedialog
from datetime import datetime
import pandas as pd

from core.graph.client import GraphClient
from core.graph.security import SecurityService
from core.graph.directory import DirectoryService
from core.powershell.client import PowerShellClient
from core.powershell.retention import RetentionService

# Bind to the async logger initialized in m365_telemetry.py
usage_logger = logging.getLogger("M365TelemetryAsyncLogger")

# Import shared styles
from telemetry.styles import *

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
        ps_client = PowerShellClient(tenant_id=tenant_domain, client_id=client_id, client_secret=client_secret, cert_tenant_id=tenant_id)
        retention_service = RetentionService(ps_client)
        policies = retention_service.fetch_retention_policies()
    except Exception as e:
        usage_logger.error("Failed to fetch retention policies via PowerShell", exc_info=True)
        policies_error = str(e)
        
    # Raise ConnectionError only if BOTH failed
    if labels_error and policies_error:
        raise ConnectionError(f"Security governance fetch failed.\nLabels Error: {labels_error}\nPolicies Error: {policies_error}")
        
    usage_logger.info("Data Security & Governance Pipeline completed successfully.")
    return {
        "labels": labels,
        "labels_error": labels_error,
        "policies": policies,
        "policies_error": policies_error
    }

def fetch_sensitivity_labels_data(client_id, client_secret, tenant_id) -> dict:
    """Fetch sensitivity labels and sort them."""
    usage_logger.info("Starting Sensitivity Labels fetch...")
    client = GraphClient(
        tenant_id=tenant_id,
        client_ids=client_id,
        client_secrets=client_secret,
        concurrency=1,
        retries=3,
        backoff=2
    )
    try:
        client.authenticate()
        service = SecurityService(client)
        labels = service.fetch_sensitivity_labels()
        # Sort labels by priority descending
        labels.sort(key=lambda x: x.get("priority", 0), reverse=True)
        return {"labels": labels, "error": None}
    except Exception as e:
        usage_logger.error("Failed to fetch sensitivity labels", exc_info=True)
        return {"labels": None, "error": str(e)}
    finally:
        try:
            client.close()
        except Exception:
            pass

def fetch_retention_policies_data(client_id, client_secret, tenant_id) -> dict:
    """Fetch retention policies via PowerShell client."""
    usage_logger.info("Starting Retention Policies fetch...")
    client = GraphClient(
        tenant_id=tenant_id,
        client_ids=client_id,
        client_secrets=client_secret,
        concurrency=1,
        retries=3,
        backoff=2
    )
    tenant_domain = tenant_id
    try:
        client.authenticate()
        from core.graph.directory import DirectoryService
        dir_svc = DirectoryService(client)
        tenant_domain = dir_svc.get_tenant_primary_domain()
        usage_logger.info(f"Retrieved primary tenant domain: {tenant_domain}")
    except Exception as e:
        usage_logger.warning(f"Could not retrieve tenant domain. Falling back to Tenant ID Guid: {e}")
    finally:
        try:
            client.close()
        except Exception:
            pass
            
    try:
        from core.powershell.client import PowerShellClient
        from core.powershell.retention import RetentionService
        
        ps_client = PowerShellClient(tenant_id=tenant_domain, client_id=client_id, client_secret=client_secret, cert_tenant_id=tenant_id)
        retention_service = RetentionService(ps_client)
        policies = retention_service.fetch_retention_policies()
        return {"policies": policies, "error": None}
    except Exception as e:
        usage_logger.error("Failed to fetch retention policies via PowerShell", exc_info=True)
        return {"policies": None, "error": str(e)}

def fetch_authentication_data(client_id, client_secret, tenant_id) -> dict:
    """Fetch Entra ID Conditional Access authentication mechanics and Enterprise SSO modes."""
    usage_logger.info("Starting Authentication & Conditional Access fetch...")
    client = GraphClient(
        tenant_id=tenant_id,
        client_ids=client_id,
        client_secrets=client_secret,
        concurrency=1,
        retries=2,
        backoff=1
    )
    try:
        client.authenticate()
        service = SecurityService(client)
        policies = service.fetch_conditional_access_policies()
        ca_policies = []
        
        for p in policies:
            name = p.get("displayName", "N/A")
            state = p.get("state", "N/A")
            
            cond = p.get("conditions") or {}
            grants = p.get("grantControls") or {}
            sessions = p.get("sessionControls") or {}

            users_obj = cond.get("users") or {}
            users_arr = users_obj.get("includeUsers") or ["N/A"]
            
            apps_arr = cond.get("clientAppTypes") or ["N/A"]

            controls_arr = grants.get("builtInControls") or ["N/A"]
            
            session_keys = list(sessions.keys()) if sessions else ["N/A"]
            
            ca_policies.append({
                "name": name,
                "state": state,
                "users": ", ".join(users_arr),
                "apps": ", ".join(apps_arr),
                "controls": ", ".join(controls_arr)
            })
            
        return {
            "auth_data": {
                "ca_policies": ca_policies
            },
            "error": None
        }
    except PermissionError as pe:
        msg = str(pe)
        if not msg or msg == "Policy.Read.All or Policy.Read permission required.":
            msg = "Conditional Access telemetry permission required.\nPlease grant the 'Policy.Read.All' (or 'Policy.Read') application permission to your App Registration in Microsoft Entra ID."
        return {"auth_data": None, "error": msg}
    except Exception as e:
        err_str = str(e)
        if "401" in err_str or "403" in err_str or "permission" in err_str.lower() or "unauthorized" in err_str.lower():
            err_str = "Conditional Access / Application telemetry permission required.\nPlease grant the 'Policy.Read.All' and 'Application.Read.All' application permissions to your App Registration in Microsoft Entra ID."
        return {"auth_data": None, "error": err_str}
    finally:
        try:
            client.close()
        except Exception:
            pass



class DataSecurityGovernanceFrame(ctk.CTkFrame):
    def update_loading_text(self, text_msg):
        if hasattr(self, 'loading_label') and self.loading_label.winfo_exists():
            self.loading_label.configure(text=f"⏳ {text_msg}")
    """Self-contained customtkinter component wrapping Data Security & Governance UI."""
    
    def __init__(self, master, log_callback, credentials_callback, status_change_callback, **kwargs):
        self.semaphore = kwargs.pop("concurrency_semaphore", None)
        super().__init__(master, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=12, **kwargs)
        self.log_msg = log_callback
        self.get_credentials = credentials_callback
        self.on_status_change = status_change_callback
        self.status = None  # 'loading', 'success', 'error', None
        
        self.current_page = 0
        self.ITEMS_PER_PAGE = 8
        self.last_labels_data = None
        self.last_policies_data = None
        
        import tempfile
        import sqlite3
        import atexit
        self.db_fd, self.db_path = tempfile.mkstemp(suffix=".db")
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.cursor = self.conn.cursor()
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS labels 
                               (id INTEGER PRIMARY KEY AUTOINCREMENT, 
                                name TEXT, description TEXT, hasProtection INTEGER, 
                                applicationMode TEXT, priority INTEGER, 
                                applicableTo TEXT, isEnabled INTEGER, is_sublabel INTEGER)''')
        self.conn.commit()
        
        def cleanup_db():
            try:
                self.conn.close()
                import os
                os.close(self.db_fd)
                os.remove(self.db_path)
            except Exception:
                pass
        atexit.register(cleanup_db)
        
        self.build_ui()

    def build_ui(self):
        """Creates card container for the tab."""
        self.pack(fill="x", expand=True, pady=10)
        
        self.inner_pad = ctk.CTkFrame(self, fg_color="transparent")
        self.inner_pad.pack(fill="both", expand=True, padx=20, pady=20)
        
        # Permanent section heading visible during loading and error states
        self.header = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.header.pack(fill="x", pady=(0, 10))
        self.main_title = ctk.CTkLabel(self.header, text="Data Security & Governance", font=FONT_HEADER_SMALL, text_color=COLOR_TEXT_MAIN)
        self.main_title.pack(side="left")
        
        # Sensitivity Labels section header
        self.labels_header_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.labels_title = ctk.CTkLabel(
            self.labels_header_frame,
            text="Sensitivity Labels",
            font=FONT_HEADER_SMALL,
            text_color=COLOR_TEXT_MAIN
        )
        self.labels_title.pack(side="left", anchor="w")
        
        self.labels_link = ctk.CTkLabel(
            self.labels_header_frame,
            text="Open Purview Sensitivity Label Portal ↗",
            font=FONT_BODY_BOLD,
            text_color=COLOR_PRIMARY,
            cursor="hand2"
        )
        self.labels_link.pack(side="left", anchor="w", padx=(15, 0))
        self.labels_link.bind("<Button-1>", lambda e: webbrowser.open("https://purview.microsoft.com/informationprotection/informationprotectionlabels/sensitivitylabels"))
        self.labels_link.bind("<Enter>", lambda e: self.labels_link.configure(text_color=COLOR_PRIMARY_HOVER))
        self.labels_link.bind("<Leave>", lambda e: self.labels_link.configure(text_color=COLOR_PRIMARY))
        
        self.labels_reload_btn = ctk.CTkButton(
            self.labels_header_frame, 
            state="disabled", text="↻ Reload", 
            width=80, 
            height=24,
            font=__import__("customtkinter").CTkFont(family="Segoe UI", size=12),
            fg_color="transparent", 
            border_width=1, 
            text_color="#2563EB", 
            hover_color="#DBEAFE",
            command=self._retry_labels_fetch
        )
        self.labels_reload_btn.pack(side="right", padx=(0, 15))
        
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
            fg_color=COLOR_SURFACE,
            border_color=COLOR_OUTLINE_LIGHT,
            border_width=1,
            corner_radius=8
        )
        
        # Pagination controls frame (centered below the grid)
        
        # Pagination controls frame (centered below the grid)
        self.labels_pagination_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        
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
            text="Open Purview Retention Policy Portal ↗",
            font=FONT_BODY_BOLD,
            text_color=COLOR_PRIMARY,
            cursor="hand2"
        )
        self.retention_link.pack(side="left", anchor="w", padx=(15, 0))
        self.retention_link.bind("<Button-1>", lambda e: webbrowser.open("https://purview.microsoft.com/datalifecyclemanagement/retention"))
        self.retention_link.bind("<Enter>", lambda e: self.retention_link.configure(text_color=COLOR_PRIMARY_HOVER))
        self.retention_link.bind("<Leave>", lambda e: self.retention_link.configure(text_color=COLOR_PRIMARY))

        self.retention_reload_btn = ctk.CTkButton(
            self.retention_header_frame, 
            state="disabled", text="↻ Reload", 
            width=80, 
            height=24,
            font=__import__("customtkinter").CTkFont(family="Segoe UI", size=12),
            fg_color="transparent", 
            border_width=1, 
            text_color="#2563EB", 
            hover_color="#DBEAFE",
            command=self._retry_retention_fetch
        )
        self.retention_reload_btn.pack(side="right", padx=(0, 15))

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
        
        self.retention_pagination_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        
        # eDiscovery Cases section (Instructional Guidance)
        
        # eDiscovery Cases section (Instructional Guidance)
        self.ediscovery_header_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.ediscovery_title = ctk.CTkLabel(
            self.ediscovery_header_frame,
            text="eDiscovery Cases",
            font=FONT_HEADER_SMALL,
            text_color=COLOR_TEXT_MAIN
        )
        self.ediscovery_title.pack(side="left", anchor="w")
        
        self.ediscovery_body_frame = ctk.CTkFrame(
            self.inner_pad,
            fg_color=COLOR_SURFACE,
            border_color=COLOR_OUTLINE_LIGHT,
            border_width=1,
            corner_radius=8
        )
        
        self.ediscovery_content = ctk.CTkFrame(self.ediscovery_body_frame, fg_color="transparent")
        self.ediscovery_content.pack(fill="x", padx=20, pady=20)
        
        lbl_inst1 = ctk.CTkLabel(
            self.ediscovery_content,
            text="eDiscovery cases cannot be scanned directly under standard Application permissions. To view your active cases, please navigate to Microsoft Purview:",
            font=FONT_BODY_MEDIUM,
            text_color=COLOR_TEXT_MAIN,
            justify="left",
            wraplength=700
        )
        lbl_inst1.pack(anchor="w", pady=(0, 8))
        
        lbl_cases_link = ctk.CTkLabel(
            self.ediscovery_content,
            text="🔗 Open Purview eDiscovery Cases Portal",
            font=FONT_BODY_BOLD,
            text_color=COLOR_PRIMARY,
            cursor="hand2"
        )
        lbl_cases_link.pack(anchor="w", pady=(0, 15))
        lbl_cases_link.bind("<Button-1>", lambda e: webbrowser.open("https://purview.microsoft.com/ediscovery/casespage"))
        lbl_cases_link.bind("<Enter>", lambda e: lbl_cases_link.configure(text_color=COLOR_PRIMARY_HOVER))
        lbl_cases_link.bind("<Leave>", lambda e: lbl_cases_link.configure(text_color=COLOR_PRIMARY))
        
        lbl_inst2 = ctk.CTkLabel(
            self.ediscovery_content,
            text="Note: Accessing eDiscovery cases requires your administrator account to have the eDiscovery Manager role assigned in the tenant permissions page:",
            font=FONT_BODY_MEDIUM,
            text_color=COLOR_TEXT_SUB,
            justify="left",
            wraplength=700
        )
        lbl_inst2.pack(anchor="w", pady=(0, 8))
        
        lbl_roles_link = ctk.CTkLabel(
            self.ediscovery_content,
            text="🔗 Assign eDiscovery Manager Role in Purview Settings",
            font=FONT_BODY_BOLD,
            text_color=COLOR_PRIMARY,
            cursor="hand2"
        )
        lbl_roles_link.pack(anchor="w")
        lbl_roles_link.bind("<Button-1>", lambda e: webbrowser.open("https://purview.microsoft.com/settings/purviewpermissions"))
        lbl_roles_link.bind("<Enter>", lambda e: lbl_roles_link.configure(text_color=COLOR_PRIMARY_HOVER))
        lbl_roles_link.bind("<Leave>", lambda e: lbl_roles_link.configure(text_color=COLOR_PRIMARY))
        
        # Authentication Mechanics Section
        self.auth_header_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.auth_title = ctk.CTkLabel(
            self.auth_header_frame,
            text="Authentication Mechanics (Conditional Access)",
            font=FONT_HEADER_SMALL,
            text_color=COLOR_TEXT_MAIN
        )
        self.auth_title.pack(side="left", anchor="w")
        
        self.auth_link = ctk.CTkLabel(
            self.auth_header_frame,
            text="Open Microsoft Entra Conditional Access Portal ↗",
            font=FONT_BODY_BOLD,
            text_color=COLOR_PRIMARY,
            cursor="hand2"
        )
        self.auth_link.pack(side="left", anchor="w", padx=(15, 0))
        self.auth_link.bind("<Button-1>", lambda e: webbrowser.open("https://portal.azure.com/#view/Microsoft_AAD_IAM/ConditionalAccessBlade/~/Policies"))
        self.auth_link.bind("<Enter>", lambda e: self.auth_link.configure(text_color=COLOR_PRIMARY_HOVER))
        self.auth_link.bind("<Leave>", lambda e: self.auth_link.configure(text_color=COLOR_PRIMARY))

        self.auth_reload_btn = ctk.CTkButton(
            self.auth_header_frame, 
            state="disabled", text="↻ Reload", 
            width=80, 
            height=24,
            font=__import__("customtkinter").CTkFont(family="Segoe UI", size=12),
            fg_color="transparent", 
            border_width=1, 
            text_color="#2563EB", 
            hover_color="#DBEAFE",
            command=self._retry_auth_fetch
        )
        self.auth_reload_btn.pack(side="right", padx=(0, 15))

        self.auth_grid = ctk.CTkFrame(
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
        self.labels_header_frame.pack_forget()
        self.labels_grid.pack_forget()
        self.labels_pagination_frame.pack_forget()
        
        self.retention_header_frame.pack_forget()
        self.retention_grid.pack_forget()
        
        self.ediscovery_header_frame.pack_forget()
        self.ediscovery_body_frame.pack_forget()
        
        self.auth_header_frame.pack_forget()
        self.auth_grid.pack_forget()
        

        for w in self.labels_grid.winfo_children():
            w.destroy()
        for w in self.retention_grid.winfo_children():
            w.destroy()
        for w in self.auth_grid.winfo_children():
            w.destroy()

            
        self.labels_current_page = 0
        self.retention_current_page = 0
        self.last_labels_data = None
        self.last_policies_data = None
        
        self.btn_export_labels.configure(state="disabled")
        self.btn_export_retention.configure(state="disabled")

    def _set_labels_loading(self, msg="Loading..."):
        for w in self.labels_grid.winfo_children():
            w.destroy()
        self.labels_state_frame = ctk.CTkFrame(self.labels_grid, fg_color="transparent")
        self.loading_label = __import__("customtkinter").CTkLabel(self.labels_state_frame, text=f"⏳ {msg}", text_color="#6b7280", font=__import__("customtkinter").CTkFont(family="Segoe UI", size=13))
        self.loading_label.pack(pady=(20, 5))
        pb = __import__("customtkinter").CTkProgressBar(self.labels_state_frame, mode="indeterminate", width=250, fg_color="#F3F4F6", progress_color="#2563EB")
        pb.pack(pady=(0, 20))
        pb.start()
        self.labels_state_frame.pack(fill="x", expand=True)

    def _set_labels_error(self, error_msg):
        for w in self.labels_grid.winfo_children():
            w.destroy()
        self.labels_state_frame = ctk.CTkFrame(self.labels_grid, fg_color="transparent")
        display_msg = error_msg
        if "401" in error_msg or "403" in error_msg or "unauthorized" in error_msg.lower() or "forbidden" in error_msg.lower():
            display_msg = "Information Protection permission required.\nPlease grant the 'SensitivityLabels.Read.All' application permission to your App Registration in Microsoft Entra ID."
        ctk.CTkLabel(self.labels_state_frame, text=f"✖ {display_msg}", text_color=COLOR_ERROR, font=FONT_BODY_MEDIUM, justify="center").pack(pady=20)
        self.labels_state_frame.pack(fill="x", expand=True)

    def _set_retention_loading(self, msg="Loading..."):
        for w in self.retention_grid.winfo_children():
            w.destroy()
        self.retention_state_frame = ctk.CTkFrame(self.retention_grid, fg_color="transparent")
        self.loading_label = __import__("customtkinter").CTkLabel(self.retention_state_frame, text=f"⏳ {msg}", text_color="#6b7280", font=__import__("customtkinter").CTkFont(family="Segoe UI", size=13))
        self.loading_label.pack(pady=(20, 5))
        pb = __import__("customtkinter").CTkProgressBar(self.retention_state_frame, mode="indeterminate", width=250, fg_color="#F3F4F6", progress_color="#2563EB")
        pb.pack(pady=(0, 20))
        pb.start()
        self.retention_state_frame.pack(fill="x", expand=True)

    def _set_retention_error(self, error_msg):
        for w in self.retention_grid.winfo_children():
            w.destroy()
        self.retention_state_frame = ctk.CTkFrame(self.retention_grid, fg_color="transparent")
        display_msg = error_msg
        if "is not installed or not in PATH" in error_msg.lower() or "pwsh" in error_msg.lower():
            display_msg = "PowerShell Core ('pwsh') is not installed or configured on this machine."
        elif "exchangeonlinemanagement" in error_msg.lower():
            display_msg = "ExchangeOnlineManagement PowerShell module is missing."
        ctk.CTkLabel(self.retention_state_frame, text=f"✖ {display_msg}", text_color=COLOR_ERROR, font=FONT_BODY_MEDIUM, justify="center").pack(pady=20)
        self.retention_state_frame.pack(fill="x", expand=True)

    def _retry_labels_fetch(self):
        if hasattr(self, 'labels_reload_btn') and self.labels_reload_btn.winfo_exists():
            self.labels_reload_btn.configure(state="disabled")
        tenant, clients, secrets = self.get_credentials()
        if tenant:
            self.labels_status = "loading"
            self.labels_grid.pack(fill="x", expand=True, pady=(0, 15))
            if hasattr(self, 'labels_pagination_frame'): self.labels_pagination_frame.pack_forget()
            self._set_labels_loading("Retrieving Sensitivity labels...")
            threading.Thread(target=self._execute_labels_worker, args=(tenant, clients[0], secrets[0]), daemon=True).start()

    def _retry_retention_fetch(self):
        if hasattr(self, 'retention_reload_btn') and self.retention_reload_btn.winfo_exists():
            self.retention_reload_btn.configure(state="disabled")
        tenant, clients, secrets = self.get_credentials()
        if tenant:
            self.retention_status = "loading"
            self.retention_grid.pack(fill="x", expand=True, pady=(0, 15))
            self._set_retention_loading("Retrieving Retention policies...")
            threading.Thread(target=self._execute_retention_worker, args=(tenant, clients[0], secrets[0]), daemon=True).start()

    def trigger_fetch(self, tenant, client_id, client_secret):
        """Triggers parallel fetches inside isolated background threads."""
        usage_logger.info("Data Security & Governance trigger_fetch called. Spawning background worker threads...")
        self.status = "loading"
        self.on_status_change()
        
        self.pack(fill="x", expand=True, pady=(20, 5))
        
        # Pack Sensitivity Labels Section
        self.labels_header_frame.pack(fill="x", pady=(0, 10))
        self.labels_grid.pack(fill="x", expand=True, pady=(0, 15))
        if hasattr(self, 'labels_pagination_frame'): self.labels_pagination_frame.pack_forget()
        self._set_labels_loading("Retrieving Sensitivity labels...")
        
        # Pack Retention Policies Section
        self.retention_header_frame.pack(fill="x", pady=(20, 5))
        self.retention_grid.pack(fill="x", expand=True, pady=(0, 15))
        self._set_retention_loading("Retrieving Retention policies...")
        
        # Pack Authentication Section
        self.auth_header_frame.pack(fill="x", pady=(20, 5))
        self.auth_grid.pack(fill="x", expand=True, pady=(0, 15))
        self._set_auth_loading("Retrieving Conditional Access authentication mechanics...")

        
        # Pack eDiscovery Cases Section (static, show immediately)
        self.ediscovery_header_frame.pack(fill="x", pady=(20, 5))
        self.ediscovery_body_frame.pack(fill="x", expand=True, pady=(0, 15))
        
        self.btn_export_labels.configure(state="disabled")
        self.btn_export_retention.configure(state="disabled")
        
        self.labels_status = "loading"
        self.retention_status = "loading"
        self.auth_status = "loading"
        
        threading.Thread(
            target=self._execute_labels_worker,
            args=(tenant, client_id, client_secret),
            daemon=True
        ).start()
        
        threading.Thread(
            target=self._execute_retention_worker,
            args=(tenant, client_id, client_secret),
            daemon=True
        ).start()

        threading.Thread(
            target=self._execute_auth_worker,
            args=(tenant, client_id, client_secret),
            daemon=True
        ).start()


    def _execute_labels_worker(self, tenant: str, client_id: str, client_secret: str):
        usage_logger.info("Executing thread: _execute_labels_worker")
        if self.semaphore:
            self.semaphore.acquire()
        try:
            res = fetch_sensitivity_labels_data(client_id, client_secret, tenant)
            self.after(0, self._handle_labels_result, res)
        finally:
            if self.semaphore:
                self.semaphore.release()

    def _execute_retention_worker(self, tenant: str, client_id: str, client_secret: str):
        usage_logger.info("Executing thread: _execute_retention_worker")
        if self.semaphore:
            self.semaphore.acquire()
        try:
            res = fetch_retention_policies_data(client_id, client_secret, tenant)
            self.after(0, self._handle_retention_result, res)
        finally:
            if self.semaphore:
                self.semaphore.release()

    def _execute_auth_worker(self, tenant: str, client_id: str, client_secret: str):
        usage_logger.info("Executing thread: _execute_auth_worker")
        if self.semaphore:
            self.semaphore.acquire()
        try:
            res = fetch_authentication_data(client_id, client_secret, tenant)
            self.after(0, self._handle_auth_result, res)
        finally:
            if self.semaphore:
                self.semaphore.release()


    def _set_auth_loading(self, msg="Loading..."):
        for w in self.auth_grid.winfo_children():
            w.destroy()
        self.auth_state_frame = ctk.CTkFrame(self.auth_grid, fg_color="transparent")
        self.loading_label = __import__("customtkinter").CTkLabel(self.auth_state_frame, text=f"⏳ {msg}", text_color="#6b7280", font=__import__("customtkinter").CTkFont(family="Segoe UI", size=13))
        self.loading_label.pack(pady=(20, 5))
        pb = __import__("customtkinter").CTkProgressBar(self.auth_state_frame, mode="indeterminate", width=250, fg_color="#F3F4F6", progress_color="#2563EB")
        pb.pack(pady=(0, 20))
        pb.start()
        self.auth_state_frame.pack(fill="x", expand=True)

    def _set_auth_error(self, error_msg):
        for w in self.auth_grid.winfo_children():
            w.destroy()
        self.auth_state_frame = ctk.CTkFrame(self.auth_grid, fg_color="transparent")
        display_msg = error_msg
        if "401" in error_msg or "403" in error_msg or "permission" in error_msg.lower() or "unauthorized" in error_msg.lower() or "forbidden" in error_msg.lower() or "policy.read" in error_msg.lower():
            display_msg = "Conditional Access telemetry permission required.\nPlease grant the 'Policy.Read.All' (or 'Policy.Read') application permission to your App Registration in Microsoft Entra ID."
        ctk.CTkLabel(self.auth_state_frame, text=f"✖ {display_msg}", text_color=COLOR_ERROR, font=FONT_BODY_MEDIUM, justify="center").pack(pady=(20, 5))
        ctk.CTkButton(self.auth_state_frame, text="Try Again", command=self._retry_auth_fetch, width=120, fg_color="transparent", border_width=1, text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER).pack(pady=(0, 20))
        self.auth_state_frame.pack(fill="x", expand=True)

    def _retry_auth_fetch(self):
        if hasattr(self, 'auth_reload_btn') and self.auth_reload_btn.winfo_exists():
            self.auth_reload_btn.configure(state="disabled")
        tenant, clients, secrets = self.get_credentials()
        if tenant:
            self.auth_status = "loading"
            self.auth_grid.pack(fill="x", expand=True, pady=(0, 15))
            self._set_auth_loading("Retrieving Conditional Access authentication mechanics...")
            threading.Thread(target=self._execute_auth_worker, args=(tenant, clients[0], secrets[0]), daemon=True).start()
            if self.semaphore:
                self.semaphore.release()

    def _handle_labels_result(self, result: dict):
        if hasattr(self, 'labels_reload_btn') and self.labels_reload_btn.winfo_exists():
            self.labels_reload_btn.configure(state="normal")
        for w in self.labels_grid.winfo_children():
            w.destroy()
            
        labels = result.get("labels")
        err = result.get("error")
        self.last_labels_data = labels
        
        if err:
            self.labels_status = "error"
            self._set_labels_error(err)
            self.btn_export_labels.configure(state="disabled")
        else:
            self.labels_status = "success"
            
            if not labels:
                ctk.CTkLabel(self.labels_grid, text="No Sensitivity Labels configured in this tenant.", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB).pack(padx=20, pady=20)
                if hasattr(self, 'labels_pagination_frame'): self.labels_pagination_frame.pack_forget()
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
                    
                import sqlite3
                self.cursor.execute("DELETE FROM labels")
                for parent in labels:
                    self.cursor.execute("INSERT INTO labels (name, description, hasProtection, applicationMode, priority, applicableTo, isEnabled, is_sublabel) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (parent.get("name", "N/A"),
                         parent.get("description", "") or parent.get("toolTip", "") or "N/A",
                         1 if parent.get("hasProtection", False) else 0,
                         parent.get("applicationMode", "N/A") or "N/A",
                         parent.get("priority", 0),
                         parent.get("applicableTo", ""),
                         1 if parent.get("isEnabled", True) else 0,
                         0))
                    sublabels = parent.get("sublabels", [])
                    if sublabels:
                        sublabels_sorted = sorted(sublabels, key=lambda x: x.get("priority", 0), reverse=True)
                        for sub in sublabels_sorted:
                            self.cursor.execute("INSERT INTO labels (name, description, hasProtection, applicationMode, priority, applicableTo, isEnabled, is_sublabel) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                                (f"    ↳  {sub.get('name', 'N/A')}",
                                 sub.get("description", "") or sub.get("toolTip", "") or "N/A",
                                 1 if sub.get("hasProtection", False) else 0,
                                 sub.get("applicationMode", "N/A") or "N/A",
                                 sub.get("priority", 0),
                                 sub.get("applicableTo", ""),
                                 1 if sub.get("isEnabled", True) else 0,
                                 1))
                self.conn.commit()
                self.current_page = 0
                self._display_current_page()
                
                self.cursor.execute("SELECT COUNT(*) FROM labels")
                total_items = self.cursor.fetchone()[0]
                if total_items > self.ITEMS_PER_PAGE:
                    self.pagination_frame.pack(pady=(5, 10))
                else:
                    if hasattr(self, 'labels_pagination_frame'): self.labels_pagination_frame.pack_forget()
                    
        self._check_overall_status()

    def _handle_retention_result(self, result: dict):
        if hasattr(self, 'retention_reload_btn') and self.retention_reload_btn.winfo_exists():
            self.retention_reload_btn.configure(state="normal")
        for w in self.retention_grid.winfo_children():
            w.destroy()
            
        policies = result.get("policies")
        err = result.get("error")
        self.last_policies_data = policies
        
        if err:
            self.retention_status = "error"
            self._set_retention_error(err)
            self.btn_export_retention.configure(state="disabled")
        else:
            self.retention_status = "success"
            self._render_retention_policies(policies, None)
            
        self._check_overall_status()

    def _handle_auth_result(self, result: dict):
        if hasattr(self, 'auth_reload_btn') and self.auth_reload_btn.winfo_exists():
            self.auth_reload_btn.configure(state="normal")
        for w in self.auth_grid.winfo_children():
            w.destroy()

        auth_data = result.get("auth_data")
        err = result.get("error")

        if err:
            self.auth_status = "error"
            self._set_auth_error(err)
        else:
            self.auth_status = "success"
            self._render_authentication_card(auth_data)

        self._check_overall_status()

    def _render_authentication_card(self, auth_data: dict):
        self.auth_grid.configure(fg_color=COLOR_SURFACE, border_width=1, border_color=COLOR_OUTLINE_LIGHT, corner_radius=8)
        
        ca_policies = auth_data.get("ca_policies", [])
        
        headers = ["Policy Name", "State", "Target Users", "Target Apps", "Enforced Controls"]
        for i in range(5):
            self.auth_grid.grid_columnconfigure(i, weight=1)
            
        for col_idx, head_text in enumerate(headers):
            cell = ctk.CTkFrame(self.auth_grid, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        if not ca_policies:
            c0 = ctk.CTkFrame(self.auth_grid, fg_color=COLOR_SURFACE, corner_radius=0)
            c0.grid(row=1, column=0, columnspan=5, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c0, text="N/A (No Conditional Access Policies configured)", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=12, anchor="w")
        else:
            for r_idx, policy in enumerate(ca_policies, start=1):
                bg_style = COLOR_SURFACE if r_idx % 2 != 0 else COLOR_SURFACE_VARIANT
                
                vals = [
                    policy.get("name", "N/A"),
                    policy.get("state", "N/A"),
                    policy.get("users", "N/A"),
                    policy.get("apps", "N/A"),
                    policy.get("controls", "N/A")
                ]
                
                for c_idx, val in enumerate(vals):
                    c = ctk.CTkFrame(self.auth_grid, fg_color=bg_style, corner_radius=0)
                    c.grid(row=r_idx, column=c_idx, sticky="nsew", padx=0, pady=(0, 1))
                    ctk.CTkLabel(c, text=val, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="left", wraplength=180).pack(padx=10, pady=12, anchor="nw")





    def _check_overall_status(self):
        if self.labels_status == "loading" or self.retention_status == "loading" or getattr(self, "auth_status", None) == "loading":
            self.status = "loading"
        elif self.labels_status == "error" and self.retention_status == "error" and getattr(self, "auth_status", None) == "error":
            self.status = "error"
        else:
            self.status = "success"
        self.on_status_change()

    def _update_labels_ui_paginated(self, data):
        for w in self.labels_grid.winfo_children():
            info = w.grid_info()
            if "row" in info and int(info["row"]) > 0:
                w.destroy()
        
        for w in self.labels_pagination_frame.winfo_children():
            w.destroy()

        if not data:
            return

        total_count = len(data)
        start_idx = self.labels_current_page * self.ITEMS_PER_PAGE
        end_idx = start_idx + self.ITEMS_PER_PAGE
        page_data = data[start_idx:end_idx]

        for offset, row_item in enumerate(page_data, start=1):
            r_idx = offset
            bg_style = COLOR_SURFACE if r_idx % 2 == 0 else COLOR_SURFACE_VARIANT
            
            name = row_item["name"]
            desc = row_item["description"]
            protection = "🛡️ Yes" if row_item["hasProtection"] else "🔓 No"
            mode = str(row_item["applicationMode"]).capitalize()
            priority = str(row_item["priority"])
            applicable = ", ".join([x.capitalize() for x in row_item["applicableTo"].split(",") if x.strip()]) or "N/A"
            status = "🟢 Enabled" if row_item["isEnabled"] else "🔴 Disabled"
            is_sublabel = bool(row_item["is_sublabel"])

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

        if total_count > self.ITEMS_PER_PAGE:
            self._draw_labels_pagination_controls(total_count, data)
            self.labels_pagination_frame.pack(fill="x", pady=(5, 10))
        else:
            self.labels_pagination_frame.pack_forget()

    def _draw_labels_pagination_controls(self, total_count, data):
        total_pages = (total_count + self.ITEMS_PER_PAGE - 1) // self.ITEMS_PER_PAGE
        
        left_spacer = ctk.CTkFrame(self.labels_pagination_frame, fg_color="transparent")
        left_spacer.pack(side="left", fill="x", expand=True)
        center_container = ctk.CTkFrame(self.labels_pagination_frame, fg_color="transparent")
        center_container.pack(side="left")

        prev_state = "normal" if self.labels_current_page > 0 else "disabled"
        btn_prev = ctk.CTkButton(
            center_container, text="◀ Prev", width=70, height=26, corner_radius=6,
            font=FONT_BODY_SMALL, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER, state=prev_state,
            command=lambda: self._change_labels_page(-1, data)
        )
        btn_prev.pack(side="left", padx=5)

        page_lbl = ctk.CTkLabel(center_container, text=f"Page {self.labels_current_page + 1} of {total_pages}", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB)
        page_lbl.pack(side="left", padx=15)

        next_state = "normal" if self.labels_current_page < total_pages - 1 else "disabled"
        btn_next = ctk.CTkButton(
            center_container, text="Next ▶", width=70, height=26, corner_radius=6,
            font=FONT_BODY_SMALL, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER, state=next_state,
            command=lambda: self._change_labels_page(1, data)
        )
        btn_next.pack(side="left", padx=5)

        right_spacer = ctk.CTkFrame(self.labels_pagination_frame, fg_color="transparent")
        right_spacer.pack(side="right", fill="x", expand=True)

    def _change_labels_page(self, delta, data):
        self.labels_current_page += delta
        self._update_labels_ui_paginated(data)

    def _render_error(self, err_msg):
        usage_logger.warning(f"Data Security & Governance fetch failed: {err_msg}")
        if hasattr(self, 'reload_btn') and self.reload_btn.winfo_exists():
            self.reload_btn.configure(state="normal")
        self._set_state_error(err_msg)
        self.status = "error"
        self.on_status_change()
        self.btn_export_labels.configure(state="disabled")
        self.btn_export_retention.configure(state="disabled")

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
            policies_list = policies if isinstance(policies, list) else [policies]
            self.last_policies_data = policies_list
            self.retention_current_page = 0
            self._update_retention_ui_paginated(self.last_policies_data)
            
    def _update_retention_ui_paginated(self, data):
        for w in self.retention_grid.winfo_children():
            w.destroy()
        for w in self.retention_pagination_frame.winfo_children():
            w.destroy()

        self.retention_grid.grid_columnconfigure(0, weight=3)  # Policy Name
        self.retention_grid.grid_columnconfigure(1, weight=3)  # Workloads
        self.retention_grid.grid_columnconfigure(2, weight=2)  # Duration & Trigger
        self.retention_grid.grid_columnconfigure(3, weight=1)  # Distribution
        self.retention_grid.grid_columnconfigure(4, weight=1)  # Status

        headers = ["Policy Name", "Workloads", "Duration", "Distribution", "Status"]
        for col_idx, head_text in enumerate(headers):
            cell = ctk.CTkFrame(self.retention_grid, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=1, pady=1)
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        total_count = len(data)
        start_idx = self.retention_current_page * self.ITEMS_PER_PAGE
        end_idx = start_idx + self.ITEMS_PER_PAGE
        page_data = data[start_idx:end_idx]

        for offset, policy in enumerate(page_data, start=1):
            r_idx = offset
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
            ctk.CTkLabel(c3, text=dist_status, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=6, anchor="w")

            c4 = ctk.CTkFrame(self.retention_grid, fg_color=bg_style, corner_radius=0)
            c4.grid(row=r_idx, column=4, sticky="nsew", padx=1, pady=1)
            ctk.CTkLabel(c4, text=status, font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=6, anchor="w")

    def export_labels_csv(self):
        """Prompts the user to save sensitivity labels as a detailed CSV file."""
        if not hasattr(self, "last_labels_data") or not self.last_labels_data:
            messagebox.showinfo("No Data", "There is no sensitivity labels data to export. Please run a scan first.", parent=self)
            return
            
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
                
        try:
            chunk_size = 1000
            for i in range(0, len(rows), chunk_size):
                chunk = rows[i:i + chunk_size]
                df = pd.DataFrame(chunk)
                df.to_csv(f, mode='a' if i > 0 else 'w', header=(i == 0), index=False)
            messagebox.showinfo("Export Successful", f"Sensitivity labels exported successfully to:\n{f}", parent=self)
        except Exception as e:
            messagebox.showerror("Export Failed", f"Failed to export CSV: {e}", parent=self)

    def export_retention_csv(self):
        """Prompts the user to save retention policies as a detailed CSV file."""
        if not hasattr(self, "last_policies_data") or not self.last_policies_data:
            messagebox.showinfo("No Data", "There is no retention policies data to export. Please run a scan first.", parent=self)
            return
            
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
            
        try:
            chunk_size = 1000
            for i in range(0, len(rows), chunk_size):
                chunk = rows[i:i + chunk_size]
                df = pd.DataFrame(chunk)
                df.to_csv(f, mode='a' if i > 0 else 'w', header=(i == 0), index=False)
            messagebox.showinfo("Export Successful", f"Retention policies exported successfully to:\n{f}", parent=self)
        except Exception as e:
            messagebox.showerror("Export Failed", f"Failed to export CSV: {e}", parent=self)


