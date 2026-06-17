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

def fetch_sensitivity_labels_data(client_id, client_secret, tenant_id, csv_path=None, on_page_callback=None, is_cancelled_callback=None) -> dict:
    """Fetch sensitivity labels and stream them."""
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
        service.fetch_sensitivity_labels(csv_path=csv_path, on_page_callback=on_page_callback, is_cancelled_callback=is_cancelled_callback)
        return {"labels": [], "error": None}
    except Exception as e:
        usage_logger.error("Failed to fetch sensitivity labels", exc_info=True)
        return {"labels": None, "error": str(e)}
    finally:
        try:
            client.close()
        except Exception:
            pass

def fetch_service_principals_sso_data(client_id, client_secret, tenant_id, csv_path=None, on_page_callback=None, is_cancelled_callback=None) -> dict:
    """Fetch Service Principals SSO modes via Graph API."""
    usage_logger.info("Starting Service Principals SSO fetch...")
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
        from core.graph.directory import DirectoryService
        dir_svc = DirectoryService(client)
        dir_svc.fetch_service_principals_sso(csv_path=csv_path, on_page_callback=on_page_callback, is_cancelled_callback=is_cancelled_callback)
        return {"sso": [], "error": None}
    except Exception as e:
        usage_logger.error("Failed to fetch Service Principals SSO", exc_info=True)
        return {"sso": None, "error": str(e)}
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

def fetch_dlp_policies_data(client_id, client_secret, tenant_id) -> dict:
    """Fetch DLP policies via PowerShell client."""
    usage_logger.info("Starting DLP Policies fetch...")
    from core.graph.client import GraphClient
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
        usage_logger.info(f"Retrieved primary tenant domain for DLP fetch: {tenant_domain}")
    except Exception as e:
        usage_logger.warning(f"Could not retrieve tenant domain for DLP fetch. Falling back to Tenant ID Guid: {e}")
    finally:
        try:
            client.close()
        except Exception:
            pass
            
    try:
        from core.powershell.client import PowerShellClient
        from core.powershell.dlp import DLPService
        
        ps_client = PowerShellClient(tenant_id=tenant_domain, client_id=client_id, client_secret=client_secret, cert_tenant_id=tenant_id)
        dlp_service = DLPService(ps_client)
        policies = dlp_service.fetch_dlp_policies()
        return {"policies": policies, "error": None}
    except Exception as e:
        usage_logger.error("Failed to fetch DLP policies via PowerShell", exc_info=True)
        return {"policies": None, "error": str(e)}

def fetch_sensitive_info_types_data(client_id, client_secret, tenant_id) -> dict:
    """Fetch Sensitive Information Types using Security & Compliance PowerShell module."""
    usage_logger.info("Starting Sensitive Information Types fetch...")
    
    # We must resolve the tenant domain since Organization cannot be a GUID for IPPSSession
    from core.graph.client import GraphClient
    from core.graph.directory import DirectoryService
    
    tenant_domain = tenant_id
    client = GraphClient(tenant_id=tenant_id, client_ids=client_id, client_secrets=client_secret, concurrency=1)
    try:
        client.authenticate()
        dir_svc = DirectoryService(client)
        tenant_domain = dir_svc.get_tenant_primary_domain()
        usage_logger.info(f"Retrieved primary tenant domain for SIT fetch: {tenant_domain}")
    except Exception as e:
        usage_logger.warning(f"Could not retrieve tenant domain for SIT fetch. Falling back to Tenant ID Guid: {e}")
    finally:
        try:
            client.close()
        except Exception:
            pass
            
    try:
        from core.powershell.client import PowerShellClient
        from core.powershell.dlp import DLPService
        
        ps_client = PowerShellClient(tenant_id=tenant_domain, client_id=client_id, client_secret=client_secret, cert_tenant_id=tenant_id)
        dlp_service = DLPService(ps_client)
        types_data = dlp_service.fetch_sensitive_info_types()
        return {"sit_data": types_data, "error": None}
    except Exception as e:
        usage_logger.error("Failed to fetch Sensitive Info Types via PowerShell", exc_info=True)
        return {"sit_data": None, "error": str(e)}

def fetch_authentication_data(client_id, client_secret, tenant_id, csv_path=None, on_page_callback=None, is_cancelled_callback=None) -> dict:
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
        service.fetch_conditional_access_policies(csv_path=csv_path, on_page_callback=on_page_callback, is_cancelled_callback=is_cancelled_callback)
        
        return {"auth_data": {"ca_policies": []}, "error": None}
    except Exception as e:
        usage_logger.error("Failed to fetch Conditional Access policies", exc_info=True)
        return {"auth_data": None, "error": str(e)}
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
        
        self.labels_current_page = 0
        self.retention_current_page = 0
        self.dlp_current_page = 0
        self.ITEMS_PER_PAGE = 5
        self.last_labels_data = None
        self.last_policies_data = None
        self.last_dlp_data = None
        
        self.build_ui()

    def _stream_to_csv(self, filename, data):
        if not data: return
        try:
            import os, csv
            script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
            tenant, clients, _ = self.get_credentials()
            if not tenant or not clients: return
            reports_dir = os.path.join(script_dir, "reports", f"{tenant}_{clients[0]}")
            os.makedirs(reports_dir, exist_ok=True)
            csv_path = os.path.join(reports_dir, filename)
            
            with open(csv_path, 'w', encoding='utf-8', newline='') as f:
                if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
                    writer = csv.DictWriter(f, fieldnames=data[0].keys())
                    writer.writeheader()
                    writer.writerows(data)
                elif isinstance(data, dict):
                    writer = csv.DictWriter(f, fieldnames=data.keys())
                    writer.writeheader()
                    writer.writerow(data)
            
            usage_logger.info(f"Successfully streamed {filename} to {csv_path}")
        except Exception as e:
            usage_logger.error(f"Failed to stream {filename}: {e}", exc_info=True)

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
        self.labels_reload_btn.pack(side="right", padx=(10, 15))
        
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
        self.retention_reload_btn.pack(side="right", padx=(10, 15))

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
        
        # DLP Policies section
        self.dlp_header_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.dlp_title = ctk.CTkLabel(
            self.dlp_header_frame,
            text="Data Loss Prevention (DLP) Policies",
            font=FONT_HEADER_SMALL,
            text_color=COLOR_TEXT_MAIN
        )
        self.dlp_title.pack(side="left", anchor="w")
        
        self.dlp_link = ctk.CTkLabel(
            self.dlp_header_frame,
            text="Open Purview DLP Portal ↗",
            font=FONT_BODY_BOLD,
            text_color=COLOR_PRIMARY,
            cursor="hand2"
        )
        self.dlp_link.pack(side="left", anchor="w", padx=(15, 0))
        self.dlp_link.bind("<Button-1>", lambda e: webbrowser.open("https://purview.microsoft.com/datalossprevention/policies"))
        self.dlp_link.bind("<Enter>", lambda e: self.dlp_link.configure(text_color=COLOR_PRIMARY_HOVER))
        self.dlp_link.bind("<Leave>", lambda e: self.dlp_link.configure(text_color=COLOR_PRIMARY))

        self.dlp_reload_btn = ctk.CTkButton(
            self.dlp_header_frame, 
            state="disabled", text="↻ Reload", 
            width=80, 
            height=24,
            font=__import__("customtkinter").CTkFont(family="Segoe UI", size=12),
            fg_color="transparent", 
            border_width=1, 
            text_color="#2563EB", 
            hover_color="#DBEAFE",
            command=self._retry_dlp_fetch
        )
        self.dlp_reload_btn.pack(side="right", padx=(10, 15))

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

        # Sensitive Information Types Section
        self.sit_header_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.sit_title = ctk.CTkLabel(
            self.sit_header_frame,
            text="Sensitive Information Types (SIT)",
            font=FONT_HEADER_SMALL,
            text_color=COLOR_TEXT_MAIN
        )
        self.sit_title.pack(side="left", anchor="w")

        self.sit_reload_btn = ctk.CTkButton(
            self.sit_header_frame,
            state="disabled",
            text="↻ Reload",
            width=80,
            height=24,
            font=__import__("customtkinter").CTkFont(family="Segoe UI", size=12),
            fg_color="transparent",
            border_width=1,
            text_color="#2563EB",
            hover_color="#DBEAFE",
            command=self._retry_sit_fetch
        )
        self.sit_reload_btn.pack(side="right", padx=(10, 15))

        self.btn_export_sit = ctk.CTkButton(
            self.sit_header_frame,
            text="Export SIT Data",
            font=FONT_BODY_BOLD,
            fg_color="transparent",
            text_color=COLOR_PRIMARY,
            border_width=1,
            border_color=COLOR_OUTLINE,
            hover_color=COLOR_SECONDARY_HOVER,
            width=150,
            height=32,
            corner_radius=16,
            command=self.export_sit_csv,
            state="disabled"
        )
        self.btn_export_sit.pack(side="right", anchor="e")
        self.sit_grid = ctk.CTkFrame(
            self.inner_pad,
            fg_color=COLOR_SURFACE,
            border_color=COLOR_OUTLINE_LIGHT,
            border_width=1,
            corner_radius=8
        )
        
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
        
        # Service Principals SSO Section
        self.sso_header_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.sso_title = ctk.CTkLabel(
            self.sso_header_frame,
            text="Service Principals Single Sign-On (SSO) Modes",
            font=FONT_HEADER_SMALL,
            text_color=COLOR_TEXT_MAIN
        )
        self.sso_title.pack(side="left", anchor="w")
        
        self.sso_link = ctk.CTkLabel(
            self.sso_header_frame,
            text="Open Enterprise Applications ↗",
            font=FONT_BODY_BOLD,
            text_color=COLOR_PRIMARY,
            cursor="hand2"
        )
        self.sso_link.pack(side="left", anchor="w", padx=(15, 0))
        self.sso_link.bind("<Button-1>", lambda e: webbrowser.open("https://entra.microsoft.com/#view/Microsoft_AAD_IAM/StartboardApplicationsMenuBlade/~/AppAppsPreview"))
        self.sso_link.bind("<Enter>", lambda e: self.sso_link.configure(text_color=COLOR_PRIMARY_HOVER))
        self.sso_link.bind("<Leave>", lambda e: self.sso_link.configure(text_color=COLOR_PRIMARY))

        self.sso_reload_btn = ctk.CTkButton(
            self.sso_header_frame, 
            state="disabled", text="↻ Reload", 
            width=80, 
            height=24,
            font=__import__("customtkinter").CTkFont(family="Segoe UI", size=12),
            fg_color="transparent", 
            border_width=1, 
            text_color="#2563EB", 
            hover_color="#DBEAFE",
            command=self._retry_sso_fetch
        )
        self.sso_reload_btn.pack(side="right", padx=(10, 15))

        self.btn_export_sso = ctk.CTkButton(
            self.sso_header_frame,
            text="Export SSO Data",
            font=FONT_BODY_BOLD,
            fg_color="transparent",
            text_color=COLOR_PRIMARY,
            border_width=1,
            border_color=COLOR_OUTLINE,
            hover_color=COLOR_SECONDARY_HOVER,
            width=150,
            height=32,
            corner_radius=16,
            command=self.export_sso_csv,
            state="disabled"
        )
        self.btn_export_sso.pack(side="right", anchor="e")

        self.sso_grid = ctk.CTkFrame(
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
        
        self.retention_header_frame.pack_forget()
        self.retention_grid.pack_forget()
        
        self.dlp_header_frame.pack_forget()
        self.dlp_grid.pack_forget()
        
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
        self.auth_current_page = 0
        self.last_labels_data = None
        self.last_policies_data = None
        self.last_auth_data = None
        
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
        ctk.CTkLabel(self.labels_state_frame, text=f"✖ {display_msg}", text_color=COLOR_ERROR, font=FONT_BODY_MEDIUM, justify="center").pack(pady=(20, 5))
        ctk.CTkButton(self.labels_state_frame, text="Try Again", command=self._retry_labels_fetch, width=120, fg_color="transparent", border_width=1, text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER).pack(pady=(0, 20))
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
        ctk.CTkLabel(self.retention_state_frame, text=f"✖ {display_msg}", text_color=COLOR_ERROR, font=FONT_BODY_MEDIUM, justify="center").pack(pady=(20, 5))
        ctk.CTkButton(self.retention_state_frame, text="Try Again", command=self._retry_retention_fetch, width=120, fg_color="transparent", border_width=1, text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER).pack(pady=(0, 20))
        self.retention_state_frame.pack(fill="x", expand=True)

    def _retry_labels_fetch(self):
        if hasattr(self, 'labels_reload_btn') and self.labels_reload_btn.winfo_exists():
            self.labels_reload_btn.configure(state="disabled")
        if hasattr(self, "sub_section_start_times"):
            import time
            self.sub_section_start_times["labels"] = time.time()
        tenant, clients, secrets = self.get_credentials()
        if tenant:
            self.labels_status = "loading"
            self.labels_grid.pack(fill="x", pady=(0, 15))
            if hasattr(self, 'labels_pagination_frame'): self.labels_pagination_frame.pack_forget()
            self._set_labels_loading("Retrieving Sensitivity labels...")
            threading.Thread(target=self._execute_labels_worker, args=(tenant, clients[0], secrets[0]), daemon=True).start()

    def _retry_retention_fetch(self):
        if hasattr(self, 'retention_reload_btn') and self.retention_reload_btn.winfo_exists():
            self.retention_reload_btn.configure(state="disabled")
        if hasattr(self, "sub_section_start_times"):
            import time
            self.sub_section_start_times["retention"] = time.time()
        tenant, clients, secrets = self.get_credentials()
        if tenant:
            self.retention_status = "loading"
            self.retention_grid.pack(fill="x", pady=(0, 15))
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
        self.labels_grid.pack(fill="x", pady=(0, 15))
        if hasattr(self, 'labels_pagination_frame'): self.labels_pagination_frame.pack_forget()
        self._set_labels_loading("Retrieving Sensitivity labels...")
        
        # Pack Retention Policies Section
        self.retention_header_frame.pack(fill="x", pady=(20, 5))
        self.retention_grid.pack(fill="x", pady=(0, 15))
        self._set_retention_loading("Retrieving Retention policies...")
        
        # Pack DLP Policies Section
        self.dlp_header_frame.pack(fill="x", pady=(20, 5))
        self.dlp_grid.pack(fill="x", pady=(0, 15))
        self._set_dlp_loading("Retrieving DLP policies...")
        
        # Pack Sensitive Information Types Section
        self.sit_header_frame.pack(fill="x", pady=(20, 5))
        self.sit_grid.pack(fill="x", pady=(0, 15))
        self._set_sit_loading("Retrieving Sensitive Information Types...")
        
        # Pack Authentication Section
        self.auth_header_frame.pack(fill="x", pady=(20, 5))
        self.auth_grid.pack(fill="x", pady=(0, 15))
        self._set_auth_loading("Retrieving Conditional Access authentication mechanics...")

        # Pack SSO Section
        self.sso_header_frame.pack(fill="x", pady=(20, 5))
        self.sso_grid.pack(fill="x", pady=(0, 15))
        self._set_sso_loading("Retrieving Service Principals SSO modes...")

        
        # Pack eDiscovery Cases Section (static, show immediately)
        self.ediscovery_header_frame.pack(fill="x", pady=(20, 5))
        self.ediscovery_body_frame.pack(fill="x", pady=(0, 15))
        
        self.btn_export_labels.configure(state="disabled")
        self.btn_export_retention.configure(state="disabled")
        self.btn_export_dlp.configure(state="disabled")
        self.btn_export_sit.configure(state="disabled")
        
        self.labels_status = "loading"
        self.retention_status = "loading"
        self.dlp_status = "loading"
        self.sit_status = "loading"
        self.auth_status = "loading"
        self.sso_status = "loading"
        
        self.btn_export_sso.configure(state="disabled")
        
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
            target=self._execute_dlp_worker,
            args=(tenant, client_id, client_secret),
            daemon=True
        ).start()

        threading.Thread(
            target=self._execute_sit_worker,
            args=(tenant, client_id, client_secret),
            daemon=True
        ).start()

        threading.Thread(
            target=self._execute_auth_worker,
            args=(tenant, client_id, client_secret),
            daemon=True
        ).start()

        threading.Thread(
            target=self._execute_sso_worker,
            args=(tenant, client_id, client_secret),
            daemon=True
        ).start()


    def _execute_labels_worker(self, tenant: str, client_id: str, client_secret: str):
        usage_logger.info("Executing thread: _execute_labels_worker")
        if self.semaphore:
            self.semaphore.acquire()
        try:
            import os
            script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
            reports_dir = os.path.join(script_dir, "reports", f"{tenant}_{client_id}")
            os.makedirs(reports_dir, exist_ok=True)
            csv_path = os.path.join(reports_dir, "sensitivity_labels.csv")
            
            res = fetch_sensitivity_labels_data(
                client_id, client_secret, tenant,
                csv_path=csv_path,
                is_cancelled_callback=lambda: hasattr(self, 'is_cancelled') and self.is_cancelled
            )
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

    def _execute_dlp_worker(self, tenant: str, client_id: str, client_secret: str):
        usage_logger.info("Executing thread: _execute_dlp_worker")
        if self.semaphore:
            self.semaphore.acquire()
        try:
            res = fetch_dlp_policies_data(client_id, client_secret, tenant)
            self.after(0, self._handle_dlp_result, res)
        finally:
            if self.semaphore:
                self.semaphore.release()



    def _execute_auth_worker(self, tenant: str, client_id: str, client_secret: str):
        usage_logger.info("Executing thread: _execute_auth_worker")
        if self.semaphore:
            self.semaphore.acquire()
        try:
            import os
            script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
            reports_dir = os.path.join(script_dir, "reports", f"{tenant}_{client_id}")
            os.makedirs(reports_dir, exist_ok=True)
            csv_path = os.path.join(reports_dir, "auth_policies.csv")
            
            res = fetch_authentication_data(
                client_id, client_secret, tenant,
                csv_path=csv_path,
                is_cancelled_callback=lambda: hasattr(self, 'is_cancelled') and self.is_cancelled
            )
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

    def _retry_dlp_fetch(self):
        """Manually trigger a re-fetch of DLP policies."""
        if hasattr(self, 'dlp_reload_btn') and self.dlp_reload_btn.winfo_exists():
            self.dlp_reload_btn.configure(state="disabled")
        if hasattr(self, 'btn_export_dlp') and self.btn_export_dlp.winfo_exists():
            self.btn_export_dlp.configure(state="disabled")
            
        if hasattr(self, "sub_section_start_times"):
            import time
            self.sub_section_start_times["dlp"] = time.time()
            
        tenant, clients, secrets = self.get_credentials()
        if tenant:
            self.dlp_status = "loading"
            self.dlp_grid.pack(fill="x", pady=(0, 15))
            self._set_dlp_loading("Retrieving DLP policies...")
            threading.Thread(target=self._execute_dlp_worker, args=(tenant, clients[0], secrets[0]), daemon=True).start()

    def export_dlp_csv(self):
        """Prompts the user to save DLP policies as a detailed CSV file."""
        import shutil, os
        tenant, clients, _ = self.get_credentials()
        script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
        csv_path = os.path.join(script_dir, "reports", f"{tenant}_{clients[0]}", "dlp_policies.csv")

        if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
            messagebox.showinfo("No Data", "There is no DLP policy data to export.", parent=self)
            return
            
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        f = filedialog.asksaveasfilename(
            initialfile=f"dlp_policies_{ts}.csv",
            defaultextension=".csv",
            filetypes=[("CSV Files", "*.csv")],
            parent=self
        )
        if not f: return
        try:
            shutil.copyfile(csv_path, f)
            messagebox.showinfo("Export Successful", f"DLP policies exported to:\n{f}", parent=self)
        except Exception as e:
            messagebox.showerror("Export Failed", f"Error: {e}", parent=self)

    def _set_dlp_loading(self, msg="Loading..."):
        for w in self.dlp_grid.winfo_children():
            w.destroy()
        self.dlp_state_frame = ctk.CTkFrame(self.dlp_grid, fg_color="transparent")
        self.loading_label = __import__("customtkinter").CTkLabel(self.dlp_state_frame, text=f"⏳ {msg}", text_color="#6b7280", font=__import__("customtkinter").CTkFont(family="Segoe UI", size=13))
        self.loading_label.pack(pady=(20, 5))
        pb = __import__("customtkinter").CTkProgressBar(self.dlp_state_frame, mode="indeterminate", width=250, fg_color="#F3F4F6", progress_color="#2563EB")
        pb.pack(pady=(0, 20))
        pb.start()
        self.dlp_state_frame.pack(fill="x", expand=True)

    def _set_dlp_error(self, error_msg):
        for w in self.dlp_grid.winfo_children():
            w.destroy()
        self.dlp_state_frame = ctk.CTkFrame(self.dlp_grid, fg_color="transparent")
        display_msg = error_msg
        if "401" in error_msg or "403" in error_msg or "permission" in error_msg.lower() or "unauthorized" in error_msg.lower() or "forbidden" in error_msg.lower():
            display_msg = "DLP policies telemetry permission required.\nPlease grant required application permissions to your App Registration in Microsoft Entra ID."
        ctk.CTkLabel(self.dlp_state_frame, text=f"✖ {display_msg}", text_color=COLOR_ERROR, font=FONT_BODY_MEDIUM, justify="center").pack(pady=(20, 5))
        ctk.CTkButton(self.dlp_state_frame, text="Try Again", command=self._retry_dlp_fetch, width=120, fg_color="transparent", border_width=1, text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER).pack(pady=(0, 20))
        self.dlp_state_frame.pack(fill="x", expand=True)

    def _handle_dlp_result(self, result: dict):
        if hasattr(self, 'dlp_reload_btn') and self.dlp_reload_btn.winfo_exists():
            self.dlp_reload_btn.configure(state="normal")
        for w in self.dlp_grid.winfo_children():
            w.destroy()
            
        policies = result.get("policies")
        err = result.get("error")
        
        if isinstance(policies, dict) and "value" in policies:
            policies = policies["value"]
            
        if err:
            self.dlp_status = "error"
            self._set_dlp_error(err)
            self.btn_export_dlp.configure(state="disabled")
        else:
            self.dlp_status = "success"
            self._stream_to_csv("dlp_policies.csv", policies)
            
            if not policies:
                ctk.CTkLabel(self.dlp_grid, text="No Data Loss Prevention (DLP) Policies configured in this tenant.", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB).pack(padx=20, pady=20)
                self.btn_export_dlp.configure(state="disabled")
            else:
                self.btn_export_dlp.configure(state="normal")
                self.dlp_grid.grid_columnconfigure(0, weight=3)  # Policy Name
                self.dlp_grid.grid_columnconfigure(1, weight=1)  # Mode
                self.dlp_grid.grid_columnconfigure(2, weight=2)  # Workload
                self.dlp_grid.grid_columnconfigure(3, weight=1)  # Enabled
                self.dlp_grid.grid_columnconfigure(4, weight=2)  # Action
                self.dlp_grid.grid_columnconfigure(5, weight=2)  # Created By
                
                headers = ["Policy Name", "Mode", "Workload", "State", "Actions", "Created By"]
                for col_idx, head_text in enumerate(headers):
                    cell = ctk.CTkFrame(self.dlp_grid, fg_color=COLOR_TONAL_BG, corner_radius=0)
                    cell.grid(row=0, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
                    ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")
                
                self.dlp_current_page = 0
                self._update_dlp_ui_paginated(None)
                
        self._check_overall_status()

    def _update_dlp_ui_paginated(self, data=None):
        for w in self.dlp_grid.winfo_children():
            info = w.grid_info()
            if "row" in info and int(info["row"]) > 0:
                w.destroy()

        if data is not None:
            total_count = len(data)
            start_idx = self.dlp_current_page * self.ITEMS_PER_PAGE
            end_idx = start_idx + self.ITEMS_PER_PAGE
            page_data = data[start_idx:end_idx]
        else:
            page_data, total_count = self._load_page_from_csv("dlp_policies.csv", self.dlp_current_page)

        if not page_data:
            return

        for offset, row_item in enumerate(page_data, start=1):
            r_idx = offset
            bg_style = COLOR_SURFACE if r_idx % 2 == 0 else COLOR_SURFACE_VARIANT
            
            name = row_item.get("Name", "N/A")
            mode = row_item.get("Mode", "N/A")
            workloads = row_item.get("Workload", "N/A")
            
            en_val = str(row_item.get("Enabled", "")).lower()
            enabled = "🟢 Enabled" if en_val in ("true", "1", "yes") else "🔴 Disabled"
            
            actions = row_item.get("Actions", "None")
            created_by = row_item.get("CreatedBy", "N/A")

            vals = [name, mode, workloads, enabled, actions, created_by]
            
            for c_idx, val in enumerate(vals):
                c = ctk.CTkFrame(self.dlp_grid, fg_color=bg_style, corner_radius=0)
                c.grid(row=r_idx, column=c_idx, sticky="nsew", padx=0, pady=(0, 1))
                ctk.CTkLabel(c, text=str(val), font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="left", wraplength=180).pack(padx=10, pady=12, anchor="nw")

        self._draw_dlp_pagination_controls(total_count, data)

    def _draw_dlp_pagination_controls(self, total_count, data):
        total_pages = max(1, (total_count + self.ITEMS_PER_PAGE - 1) // self.ITEMS_PER_PAGE)
        
        control_frame = ctk.CTkFrame(self.dlp_grid, fg_color="transparent")
        control_frame.grid(row=self.ITEMS_PER_PAGE + 1, column=0, columnspan=6, pady=0, sticky="ew")
        
        center_container = ctk.CTkFrame(control_frame, fg_color="transparent")
        center_container.pack(pady=(5, 10))

        prev_state = "normal" if self.dlp_current_page > 0 else "disabled"
        btn_prev = ctk.CTkButton(
            center_container, text="◀ Prev", width=70, height=22, corner_radius=6,
            font=FONT_BODY_SMALL, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER, state=prev_state,
            command=lambda d=data: self._change_dlp_page(-1, d)
        )
        btn_prev.pack(side="left", padx=5)

        page_lbl = ctk.CTkLabel(center_container, text=f"Page {self.dlp_current_page + 1} of {total_pages}", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB)
        page_lbl.pack(side="left", padx=15)

        next_state = "normal" if self.dlp_current_page < total_pages - 1 else "disabled"
        btn_next = ctk.CTkButton(
            center_container, text="Next ▶", width=70, height=22, corner_radius=6,
            font=FONT_BODY_SMALL, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER, state=next_state,
            command=lambda d=data: self._change_dlp_page(1, d)
        )
        btn_next.pack(side="left", padx=5)

    def _change_dlp_page(self, delta, data):
        self.dlp_current_page += delta
        self._update_dlp_ui_paginated(data)

    def _retry_auth_fetch(self):
        if hasattr(self, 'auth_reload_btn') and self.auth_reload_btn.winfo_exists():
            self.auth_reload_btn.configure(state="disabled")
        if hasattr(self, "sub_section_start_times"):
            import time
            self.sub_section_start_times["auth"] = time.time()
        tenant, clients, secrets = self.get_credentials()
        if tenant:
            self.auth_status = "loading"
            self.auth_grid.pack(fill="x", pady=(0, 15))
            self._set_auth_loading("Retrieving Conditional Access authentication mechanics...")
            threading.Thread(target=self._execute_auth_worker, args=(tenant, clients[0], secrets[0]), daemon=True).start()
            if self.semaphore:
                self.semaphore.release()

    def _handle_labels_result(self, result: dict):
        if hasattr(self, 'labels_reload_btn') and self.labels_reload_btn.winfo_exists():
            self.labels_reload_btn.configure(state="normal")
        for w in self.labels_grid.winfo_children():
            w.destroy()
            
        err = result.get("error")
        
        if err:
            self.labels_status = "error"
            self._set_labels_error(err)
            self.btn_export_labels.configure(state="disabled")
        else:
            self.labels_status = "success"
            page_data, total_count = self._load_page_from_csv("sensitivity_labels.csv", 0)
            
            if total_count == 0:
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
                    
                self.labels_current_page = 0
                self._update_labels_ui_paginated(None)
                    
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
        self.auth_current_page = 0
        self._update_auth_ui_paginated(None)

    def _update_auth_ui_paginated(self, data=None):
        for w in self.auth_grid.winfo_children():
            info = w.grid_info()
            if "row" in info and int(info["row"]) > 0:
                w.destroy()

        if data is not None:
            total_count = len(data)
            start_idx = self.auth_current_page * self.ITEMS_PER_PAGE
            end_idx = start_idx + self.ITEMS_PER_PAGE
            page_data = data[start_idx:end_idx]
        else:
            page_data, total_count = self._load_page_from_csv("auth_policies.csv", self.auth_current_page)

        headers = ["Policy Name", "State", "Target Users", "Target Apps", "Enforced Controls"]
        for i in range(5):
            self.auth_grid.grid_columnconfigure(i, weight=1)
            
        if not self.auth_grid.winfo_children():
            for col_idx, head_text in enumerate(headers):
                cell = ctk.CTkFrame(self.auth_grid, fg_color=COLOR_TONAL_BG, corner_radius=0)
                cell.grid(row=0, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
                ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        if not page_data:
            c0 = ctk.CTkFrame(self.auth_grid, fg_color=COLOR_SURFACE, corner_radius=0)
            c0.grid(row=1, column=0, columnspan=5, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c0, text="N/A (No Conditional Access Policies configured)", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=12, anchor="w")
        else:
            for r_idx, policy in enumerate(page_data, start=1):
                bg_style = COLOR_SURFACE if r_idx % 2 != 0 else COLOR_SURFACE_VARIANT
                
                vals = [
                    policy.get("name", "N/A"),
                    policy.get("state", "N/A"),
                    policy.get("target_users", "N/A"),
                    policy.get("target_apps", "N/A"),
                    policy.get("controls", "N/A")
                ]
                
                for c_idx, val in enumerate(vals):
                    c = ctk.CTkFrame(self.auth_grid, fg_color=bg_style, corner_radius=0)
                    c.grid(row=r_idx, column=c_idx, sticky="nsew", padx=0, pady=(0, 1))
                    ctk.CTkLabel(c, text=val, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="left", wraplength=180).pack(padx=10, pady=12, anchor="nw")

        self._draw_auth_pagination_controls(total_count, data)

    def _draw_auth_pagination_controls(self, total_count, data):
        total_pages = max(1, (total_count + self.ITEMS_PER_PAGE - 1) // self.ITEMS_PER_PAGE)
        
        control_frame = ctk.CTkFrame(self.auth_grid, fg_color="transparent")
        control_frame.grid(row=self.ITEMS_PER_PAGE + 1, column=0, columnspan=5, pady=0, sticky="ew")
        
        center_container = ctk.CTkFrame(control_frame, fg_color="transparent")
        center_container.pack(pady=(5, 10))

        prev_state = "normal" if self.auth_current_page > 0 else "disabled"
        btn_prev = ctk.CTkButton(
            center_container, text="◀ Prev", width=70, height=22, corner_radius=6,
            font=FONT_BODY_SMALL, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER, state=prev_state,
            command=lambda: self._change_auth_page(-1, data)
        )
        btn_prev.pack(side="left", padx=5)

        page_lbl = ctk.CTkLabel(center_container, text=f"Page {self.auth_current_page + 1} of {total_pages}", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB)
        page_lbl.pack(side="left", padx=15)

        next_state = "normal" if self.auth_current_page < total_pages - 1 else "disabled"
        btn_next = ctk.CTkButton(
            center_container, text="Next ▶", width=70, height=22, corner_radius=6,
            font=FONT_BODY_SMALL, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER, state=next_state,
            command=lambda: self._change_auth_page(1, data)
        )
        btn_next.pack(side="left", padx=5)


    def _change_auth_page(self, delta, data):
        self.auth_current_page += delta
        self._update_auth_ui_paginated(data)





    def _check_overall_status(self):
        if self.labels_status == "loading" or self.retention_status == "loading" or getattr(self, "dlp_status", None) == "loading" or getattr(self, "sit_status", None) == "loading" or getattr(self, "auth_status", None) == "loading" or getattr(self, "sso_status", None) == "loading":
            self.status = "loading"
        elif self.labels_status == "error" and self.retention_status == "error" and getattr(self, "dlp_status", None) == "error" and getattr(self, "sit_status", None) == "error" and getattr(self, "auth_status", None) == "error" and getattr(self, "sso_status", None) == "error":
            self.status = "error"
        else:
            self.status = "success"
        self.on_status_change()

    def _set_state_error(self, error_msg):
        if getattr(self, "labels_status", None) == "loading":
            self.labels_status = "error"
            self._set_labels_error(error_msg)
            if hasattr(self, 'labels_reload_btn') and self.labels_reload_btn.winfo_exists():
                self.labels_reload_btn.configure(state="normal")
                
        if getattr(self, "retention_status", None) == "loading":
            self.retention_status = "error"
            self._set_retention_error(error_msg)
            if hasattr(self, 'retention_reload_btn') and self.retention_reload_btn.winfo_exists():
                self.retention_reload_btn.configure(state="normal")
                
        if getattr(self, "dlp_status", None) == "loading":
            self.dlp_status = "error"
            self._set_dlp_error(error_msg)
            if hasattr(self, 'dlp_reload_btn') and self.dlp_reload_btn.winfo_exists():
                self.dlp_reload_btn.configure(state="normal")
                
        if getattr(self, "sit_status", None) == "loading":
            self.sit_status = "error"
            self._set_sit_error(error_msg)
            if hasattr(self, 'sit_reload_btn') and self.sit_reload_btn.winfo_exists():
                self.sit_reload_btn.configure(state="normal")
                
        if getattr(self, "auth_status", None) == "loading":
            self.auth_status = "error"
            self._set_auth_error(error_msg)
            if hasattr(self, 'auth_reload_btn') and self.auth_reload_btn.winfo_exists():
                self.auth_reload_btn.configure(state="normal")
                
        if getattr(self, "sso_status", None) == "loading":
            self.sso_status = "error"
            self._set_sso_error(error_msg)
            if hasattr(self, 'sso_reload_btn') and self.sso_reload_btn.winfo_exists():
                self.sso_reload_btn.configure(state="normal")

    def _load_page_from_csv(self, filename, page):
        script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
        tenant, clients, _ = self.get_credentials()
        if not tenant or not clients: return [], 0
        csv_path = os.path.join(script_dir, "reports", f"{tenant}_{clients[0]}", filename)
        if not os.path.exists(csv_path): return [], 0

        items = []
        total_count = 0
        import csv
        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                all_rows = list(reader)
                total_count = len(all_rows)
                start_idx = page * self.ITEMS_PER_PAGE
                end_idx = start_idx + self.ITEMS_PER_PAGE
                items = all_rows[start_idx:end_idx]
        except Exception as e:
            usage_logger.error(f"Error reading CSV for pagination: {e}")
        return items, total_count

    def _update_labels_ui_paginated(self, data=None):
        for w in self.labels_grid.winfo_children():
            info = w.grid_info()
            if "row" in info and int(info["row"]) > 0:
                w.destroy()

        if data is not None:
            total_count = len(data)
            start_idx = self.labels_current_page * self.ITEMS_PER_PAGE
            end_idx = start_idx + self.ITEMS_PER_PAGE
            page_data = data[start_idx:end_idx]
        else:
            page_data, total_count = self._load_page_from_csv("sensitivity_labels.csv", self.labels_current_page)

        if not page_data:
            return

        for offset, row_item in enumerate(page_data, start=1):
            r_idx = offset
            bg_style = COLOR_SURFACE if r_idx % 2 == 0 else COLOR_SURFACE_VARIANT
            
            name = row_item["name"]
            desc = row_item["description"]
            protection = "🛡️ Yes" if str(row_item["hasProtection"]) == "1" else "🔓 No"
            mode = str(row_item["applicationMode"]).capitalize()
            priority = str(row_item["priority"])
            applicable = ", ".join([x.capitalize() for x in str(row_item["applicableTo"]).split(",") if x.strip()]) or "N/A"
            status = "🟢 Enabled" if str(row_item["isEnabled"]) == "1" else "🔴 Disabled"
            is_sublabel = str(row_item["is_sublabel"]) == "1"

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

        self._draw_labels_pagination_controls(total_count, data)

    def _draw_labels_pagination_controls(self, total_count, data):
        total_pages = max(1, (total_count + self.ITEMS_PER_PAGE - 1) // self.ITEMS_PER_PAGE)
        
        control_frame = ctk.CTkFrame(self.labels_grid, fg_color="transparent")
        control_frame.grid(row=self.ITEMS_PER_PAGE + 1, column=0, columnspan=7, pady=0, sticky="ew")
        
        center_container = ctk.CTkFrame(control_frame, fg_color="transparent")
        center_container.pack(pady=(5, 10))

        prev_state = "normal" if self.labels_current_page > 0 else "disabled"
        btn_prev = ctk.CTkButton(
            center_container, text="◀ Prev", width=70, height=22, corner_radius=6,
            font=FONT_BODY_SMALL, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER, state=prev_state,
            command=lambda d=data: self._change_labels_page(-1, d)
        )
        btn_prev.pack(side="left", padx=5)

        page_lbl = ctk.CTkLabel(center_container, text=f"Page {self.labels_current_page + 1} of {total_pages}", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB)
        page_lbl.pack(side="left", padx=15)

        next_state = "normal" if self.labels_current_page < total_pages - 1 else "disabled"
        btn_next = ctk.CTkButton(
            center_container, text="Next ▶", width=70, height=22, corner_radius=6,
            font=FONT_BODY_SMALL, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER, state=next_state,
            command=lambda d=data: self._change_labels_page(1, d)
        )
        btn_next.pack(side="left", padx=5)


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
            if isinstance(policies, dict) and "value" in policies and isinstance(policies["value"], list):
                policies_list = policies["value"]
            else:
                policies_list = policies if isinstance(policies, list) else [policies]
            self._stream_to_csv("retention_policies.csv", policies_list)
            self.retention_current_page = 0
            self._update_retention_ui_paginated(None)
    def _update_retention_ui_paginated(self, data=None):
        for w in self.retention_grid.winfo_children():
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

        if data is not None:
            total_count = len(data)
            start_idx = self.retention_current_page * self.ITEMS_PER_PAGE
            end_idx = start_idx + self.ITEMS_PER_PAGE
            page_data = data[start_idx:end_idx]
        else:
            page_data, total_count = self._load_page_from_csv("retention_policies.csv", self.retention_current_page)

        if not page_data:
            return

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
                is_enabled = enabled_val.lower() in ("true", "1", "yes")
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

        self._draw_retention_pagination_controls(total_count, data)

    def _draw_retention_pagination_controls(self, total_count, data):
        total_pages = max(1, (total_count + self.ITEMS_PER_PAGE - 1) // self.ITEMS_PER_PAGE)
        
        control_frame = ctk.CTkFrame(self.retention_grid, fg_color="transparent")
        control_frame.grid(row=self.ITEMS_PER_PAGE + 1, column=0, columnspan=5, pady=0, sticky="ew")
        
        center_container = ctk.CTkFrame(control_frame, fg_color="transparent")
        center_container.pack(pady=(5, 10))

        prev_state = "normal" if self.retention_current_page > 0 else "disabled"
        btn_prev = ctk.CTkButton(
            center_container, text="◀ Prev", width=70, height=22, corner_radius=6,
            font=FONT_BODY_SMALL, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER, state=prev_state,
            command=lambda d=data: self._change_retention_page(-1, d)
        )
        btn_prev.pack(side="left", padx=5)

        page_lbl = ctk.CTkLabel(center_container, text=f"Page {self.retention_current_page + 1} of {total_pages}", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB)
        page_lbl.pack(side="left", padx=15)

        next_state = "normal" if self.retention_current_page < total_pages - 1 else "disabled"
        btn_next = ctk.CTkButton(
            center_container, text="Next ▶", width=70, height=22, corner_radius=6,
            font=FONT_BODY_SMALL, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER, state=next_state,
            command=lambda d=data: self._change_retention_page(1, d)
        )
        btn_next.pack(side="left", padx=5)


    def _change_retention_page(self, delta, data):
        self.retention_current_page += delta
        self._update_retention_ui_paginated(data)

    def export_labels_csv(self):
        """Prompts the user to save sensitivity labels as a detailed CSV file."""
        import shutil, os
        tenant, clients, _ = self.get_credentials()
        script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
        csv_path = os.path.join(script_dir, "reports", f"{tenant}_{clients[0]}", "sensitivity_labels.csv")

        if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
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
            
        try:
            shutil.copyfile(csv_path, f)
            messagebox.showinfo("Export Successful", f"Sensitivity labels exported successfully to:\n{f}", parent=self)
        except Exception as e:
            messagebox.showerror("Export Failed", f"Failed to export CSV: {e}", parent=self)

    def export_retention_csv(self):
        """Prompts the user to save retention policies as a detailed CSV file."""
        import shutil, os
        tenant, clients, _ = self.get_credentials()
        script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
        csv_path = os.path.join(script_dir, "reports", f"{tenant}_{clients[0]}", "retention_policies.csv")

        if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
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
            
        try:
            shutil.copyfile(csv_path, f)
            messagebox.showinfo("Export Successful", f"Retention policies exported successfully to:\n{f}", parent=self)
        except Exception as e:
            messagebox.showerror("Export Failed", f"Failed to export CSV: {e}", parent=self)

    def _retry_sit_fetch(self):
        """Manually trigger a re-fetch of Sensitive Information Types."""
        if hasattr(self, 'sit_reload_btn') and self.sit_reload_btn.winfo_exists():
            self.sit_reload_btn.configure(state="disabled")
        if hasattr(self, 'btn_export_sit') and self.btn_export_sit.winfo_exists():
            self.btn_export_sit.configure(state="disabled")
            
        if hasattr(self, "sub_section_start_times"):
            import time
            self.sub_section_start_times["sit"] = time.time()
            
        tenant, clients, secrets = self.get_credentials()
        if tenant:
            self.sit_status = "loading"
            self.sit_grid.pack(fill="x", pady=(0, 15))
            self._set_sit_loading("Retrieving Sensitive Information Types...")
            threading.Thread(target=self._execute_sit_worker, args=(tenant, clients[0], secrets[0]), daemon=True).start()

    def _execute_sit_worker(self, tenant: str, client_id: str, client_secret: str):
        usage_logger.info("Executing thread: _execute_sit_worker")
        if self.semaphore:
            self.semaphore.acquire()
        try:
            res = fetch_sensitive_info_types_data(client_id, client_secret, tenant)
            self.after(0, self._handle_sit_result, res)
        finally:
            if self.semaphore:
                self.semaphore.release()

    def _set_sit_loading(self, msg="Loading..."):
        for w in self.sit_grid.winfo_children():
            w.destroy()
        self.sit_state_frame = ctk.CTkFrame(self.sit_grid, fg_color="transparent")
        self.loading_label = __import__("customtkinter").CTkLabel(self.sit_state_frame, text=f"⏳ {msg}", text_color="#6b7280", font=__import__("customtkinter").CTkFont(family="Segoe UI", size=13))
        self.loading_label.pack(pady=(20, 5))
        pb = __import__("customtkinter").CTkProgressBar(self.sit_state_frame, mode="indeterminate", width=250, fg_color="#F3F4F6", progress_color="#2563EB")
        pb.pack(pady=(0, 20))
        pb.start()
        self.sit_state_frame.pack(fill="x", expand=True)

    def _set_sit_error(self, error_msg):
        for w in self.sit_grid.winfo_children():
            w.destroy()
        self.sit_state_frame = ctk.CTkFrame(self.sit_grid, fg_color="transparent")
        display_msg = error_msg
        if "401" in error_msg or "403" in error_msg or "permission" in error_msg.lower() or "unauthorized" in error_msg.lower() or "forbidden" in error_msg.lower():
            display_msg = "SIT telemetry permission required.\nPlease grant required application permissions to your App Registration in Microsoft Entra ID."
        ctk.CTkLabel(self.sit_state_frame, text=f"✖ {display_msg}", text_color=COLOR_ERROR, font=FONT_BODY_MEDIUM, justify="center").pack(pady=(20, 5))
        ctk.CTkButton(self.sit_state_frame, text="Try Again", command=self._retry_sit_fetch, width=120, fg_color="transparent", border_width=1, text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER).pack(pady=(0, 20))
        self.sit_state_frame.pack(fill="x", expand=True)

    def _handle_sit_result(self, result: dict):
        if hasattr(self, 'sit_reload_btn') and self.sit_reload_btn.winfo_exists():
            self.sit_reload_btn.configure(state="normal")
        for w in self.sit_grid.winfo_children():
            w.destroy()
            
        sit_data = result.get("sit_data")
        err = result.get("error")
        
        if isinstance(sit_data, dict) and "value" in sit_data:
            sit_data = sit_data["value"]
            
        self.last_sit_data = sit_data
        self._stream_to_csv("sensitive_info_types.csv", self.last_sit_data)
        
        if err:
            self.sit_status = "error"
            self._set_sit_error(err)
            self.btn_export_sit.configure(state="disabled")
        else:
            self.sit_status = "success"
            self.btn_export_sit.configure(state="normal")
            self._render_sit_types(sit_data)
            
        self._check_overall_status()

    def _render_sit_types(self, sit_data):
        self.sit_grid.configure(fg_color=COLOR_SURFACE, border_width=1, border_color=COLOR_OUTLINE_LIGHT, corner_radius=8)
        self.sit_current_page = 0
        if not isinstance(sit_data, list):
            sit_data = [sit_data] if sit_data else []
        self._update_sit_ui_paginated(sit_data)

    def _update_sit_ui_paginated(self, data=None):
        if data is None:
            data = self.last_sit_data
            
        for w in self.sit_grid.winfo_children():
            info = w.grid_info()
            if "row" in info and int(info["row"]) > 0:
                w.destroy()

        headers = ["SIT Name", "Type", "Confidence", "Description"]
        for i in range(4):
            self.sit_grid.grid_columnconfigure(i, weight=1 if i != 3 else 3)
            
        if not self.sit_grid.winfo_children():
            for col_idx, head_text in enumerate(headers):
                cell = ctk.CTkFrame(self.sit_grid, fg_color=COLOR_TONAL_BG, corner_radius=0)
                cell.grid(row=0, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
                ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        total_count = len(data) if data else 0
        start_idx = self.sit_current_page * self.ITEMS_PER_PAGE
        end_idx = start_idx + self.ITEMS_PER_PAGE
        page_data = data[start_idx:end_idx] if data else []

        if not data:
            c0 = ctk.CTkFrame(self.sit_grid, fg_color=COLOR_SURFACE, corner_radius=0)
            c0.grid(row=1, column=0, columnspan=4, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c0, text="No Sensitive Information Types found.", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB).pack(pady=20)
            return

        for idx, sit in enumerate(page_data):
            r_idx = idx + 1
            bg_style = COLOR_SURFACE if r_idx % 2 != 0 else COLOR_SURFACE_VARIANT
            
            name = sit.get("Name", "N/A")
            desc = sit.get("Description", "N/A")
            sit_type = sit.get("Type", "N/A")
            conf = str(sit.get("RecommendedConfidence", "N/A"))

            c0 = ctk.CTkFrame(self.sit_grid, fg_color=bg_style, corner_radius=0)
            c0.grid(row=r_idx, column=0, sticky="nsew", padx=1, pady=1)
            lbl_name = ctk.CTkLabel(c0, text=name, font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN)
            lbl_name.pack(padx=10, pady=6, anchor="w")
            c0.bind("<Configure>", lambda e, l=lbl_name: l.configure(wraplength=e.width - 20))

            c1 = ctk.CTkFrame(self.sit_grid, fg_color=bg_style, corner_radius=0)
            c1.grid(row=r_idx, column=1, sticky="nsew", padx=1, pady=1)
            lbl_type = ctk.CTkLabel(c1, text=sit_type, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN)
            lbl_type.pack(padx=10, pady=6, anchor="w")
            
            c2 = ctk.CTkFrame(self.sit_grid, fg_color=bg_style, corner_radius=0)
            c2.grid(row=r_idx, column=2, sticky="nsew", padx=1, pady=1)
            lbl_conf = ctk.CTkLabel(c2, text=f"{conf}%" if conf.isdigit() else conf, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN)
            lbl_conf.pack(padx=10, pady=6, anchor="w")

            c3 = ctk.CTkFrame(self.sit_grid, fg_color=bg_style, corner_radius=0)
            c3.grid(row=r_idx, column=3, sticky="nsew", padx=1, pady=1)
            lbl_desc = ctk.CTkLabel(c3, text=desc, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB, justify="left")
            lbl_desc.pack(padx=10, pady=6, anchor="w")
            c3.bind("<Configure>", lambda e, l=lbl_desc: l.configure(wraplength=e.width - 20))

        self._draw_sit_pagination_controls(total_count, data)

    def _draw_sit_pagination_controls(self, total_count, data):
        total_pages = max(1, (total_count + self.ITEMS_PER_PAGE - 1) // self.ITEMS_PER_PAGE)
        
        control_frame = ctk.CTkFrame(self.sit_grid, fg_color="transparent")
        control_frame.grid(row=self.ITEMS_PER_PAGE + 1, column=0, columnspan=4, pady=0, sticky="ew")
        
        center_container = ctk.CTkFrame(control_frame, fg_color="transparent")
        center_container.pack(pady=(5, 10))

        prev_state = "normal" if self.sit_current_page > 0 else "disabled"
        btn_prev = ctk.CTkButton(
            center_container, text="◀ Prev", width=70, height=22, corner_radius=6,
            font=FONT_BODY_SMALL, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER, state=prev_state,
            command=lambda d=data: self._change_sit_page(-1, d)
        )
        btn_prev.pack(side="left", padx=5)

        page_lbl = ctk.CTkLabel(center_container, text=f"Page {self.sit_current_page + 1} of {total_pages}", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB)
        page_lbl.pack(side="left", padx=15)

        next_state = "normal" if self.sit_current_page < total_pages - 1 else "disabled"
        btn_next = ctk.CTkButton(
            center_container, text="Next ▶", width=70, height=22, corner_radius=6,
            font=FONT_BODY_SMALL, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER, state=next_state,
            command=lambda d=data: self._change_sit_page(1, d)
        )
        btn_next.pack(side="left", padx=5)

    def _change_sit_page(self, delta, data):
        self.sit_current_page += delta
        self._update_sit_ui_paginated(data)

    def export_sit_csv(self):
        """Prompts the user to save Sensitive Information Types as a detailed CSV file."""
        import shutil, os
        tenant, clients, _ = self.get_credentials()
        script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
        csv_path = os.path.join(script_dir, "reports", f"{tenant}_{clients[0]}", "sensitive_info_types.csv")

        if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
            messagebox.showinfo("No Data", "There is no SIT data to export.", parent=self)
            return
            
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        f = filedialog.asksaveasfilename(
            initialfile=f"sensitive_info_types_{ts}.csv",
            defaultextension=".csv",
            filetypes=[("CSV Files", "*.csv")],
            parent=self
        )
        if not f: return
        try:
            shutil.copyfile(csv_path, f)
            messagebox.showinfo("Export Successful", f"SIT exported to:\n{f}", parent=self)
        except Exception as e:
            messagebox.showerror("Export Failed", f"Error: {e}", parent=self)

    def _execute_sso_worker(self, tenant: str, client_id: str, client_secret: str):
        usage_logger.info("Executing thread: _execute_sso_worker")
        if self.semaphore:
            self.semaphore.acquire()
        try:
            import os
            script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
            reports_dir = os.path.join(script_dir, "reports", f"{tenant}_{client_id}")
            os.makedirs(reports_dir, exist_ok=True)
            csv_path = os.path.join(reports_dir, "service_principals_sso.csv")
            
            res = fetch_service_principals_sso_data(
                client_id, client_secret, tenant,
                csv_path=csv_path,
                is_cancelled_callback=lambda: hasattr(self, 'is_cancelled') and self.is_cancelled
            )
            self.after(0, self._handle_sso_result, res)
        finally:
            if self.semaphore:
                self.semaphore.release()

    def _handle_sso_result(self, res: dict):
        if res.get("error"):
            self.sso_status = "error"
            self._set_sso_error(res["error"])
        else:
            self.sso_status = "success"
            self.btn_export_sso.configure(state="normal")
            self._render_sso_counts(None)
        
        self.sso_reload_btn.configure(state="normal")
        self._check_overall_status()

    def _set_sso_loading(self, message="Retrieving data..."):
        for w in self.sso_grid.winfo_children():
            w.destroy()
        self.sso_state_frame = ctk.CTkFrame(self.sso_grid, fg_color="transparent")
        self.loading_label = __import__("customtkinter").CTkLabel(
            self.sso_state_frame, 
            text=f"⏳ {message}", 
            text_color="#6b7280", 
            font=__import__("customtkinter").CTkFont(family="Segoe UI", size=13)
        )
        self.loading_label.pack(pady=(20, 5))
        pb = __import__("customtkinter").CTkProgressBar(
            self.sso_state_frame, 
            mode="indeterminate", 
            width=250, 
            fg_color="#F3F4F6", 
            progress_color="#2563EB"
        )
        pb.pack(pady=(0, 20))
        pb.start()
        self.sso_state_frame.pack(fill="x", expand=True)
        self.sso_reload_btn.configure(state="disabled")

    def _set_sso_error(self, err_msg):
        for w in self.sso_grid.winfo_children():
            w.destroy()
        self.sso_state_frame = ctk.CTkFrame(self.sso_grid, fg_color="transparent")
        display_msg = f"Failed to load SSO data: {err_msg}" if err_msg else "SSO telemetry fetch failed."
        ctk.CTkLabel(self.sso_state_frame, text=f"✖ {display_msg}", text_color=COLOR_ERROR, font=FONT_BODY_MEDIUM, justify="center").pack(pady=(20, 5))
        ctk.CTkButton(self.sso_state_frame, text="Try Again", command=self._retry_sso_fetch, width=120, fg_color="transparent", border_width=1, text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER).pack(pady=(0, 20))
        self.sso_state_frame.pack(fill="x", expand=True)

    def _retry_sso_fetch(self):
        self.sso_status = "loading"
        self._set_sso_loading("Retrying SSO modes fetch...")
        self.btn_export_sso.configure(state="disabled")
        threading.Thread(target=self._execute_sso_worker, args=(self.tenant, self.client_id, self.client_secret), daemon=True).start()
        self._check_overall_status()

    def _render_sso_counts(self, data):
        for w in self.sso_grid.winfo_children():
            w.destroy()

        self.sso_grid.grid_columnconfigure(0, weight=1)
        self.sso_grid.grid_columnconfigure(1, weight=1)
        
        saml_count = 0
        oidc_count = 0
        pwd_count = 0
        null_count = 0

        script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
        tenant, clients, _ = self.get_credentials()
        csv_path = os.path.join(script_dir, "reports", f"{tenant}_{clients[0]}", "service_principals_sso.csv") if tenant and clients else None
        
        if csv_path and os.path.exists(csv_path):
            import csv
            try:
                with open(csv_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        mode = row.get("preferredSingleSignOnMode", "")
                        if mode == "saml": saml_count += 1
                        elif mode == "oidc": oidc_count += 1
                        elif mode == "password": pwd_count += 1
                        else: null_count += 1
            except Exception as e:
                usage_logger.error(f"Error reading SSO CSV: {e}")

        headers = ["SSO Mode", "Application Count"]
        
        # Build Grid Header (Row 1)
        for col_idx, head_text in enumerate(headers):
            cell = ctk.CTkFrame(self.sso_grid, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=1, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        rows = [
            ("SAML", saml_count),
            ("OIDC", oidc_count),
            ("Password", pwd_count),
            ("Null / Not Supported", null_count)
        ]

        # Draw Grid Rows (Row 2+)
        for r_idx, (mode_name, count) in enumerate(rows, start=2):
            bg_style = COLOR_SURFACE if r_idx % 2 != 0 else COLOR_SURFACE_VARIANT
            
            # Column 0: Mode Name
            c0 = ctk.CTkFrame(self.sso_grid, fg_color=bg_style, corner_radius=0)
            c0.grid(row=r_idx, column=0, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c0, text=mode_name, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=12, anchor="w")

            # Column 1: Count
            c1 = ctk.CTkFrame(self.sso_grid, fg_color=bg_style, corner_radius=0)
            c1.grid(row=r_idx, column=1, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c1, text=str(count), font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=12, anchor="w")

    def export_sso_csv(self):
        """Prompts the user to save Service Principals SSO as a detailed CSV file."""
        import shutil, os
        tenant, clients, _ = self.get_credentials()
        script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
        csv_path = os.path.join(script_dir, "reports", f"{tenant}_{clients[0]}", "service_principals_sso.csv")

        if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
            messagebox.showinfo("No Data", "There is no SSO data to export.", parent=self)
            return
            
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        f = filedialog.asksaveasfilename(
            initialfile=f"service_principals_sso_{ts}.csv",
            defaultextension=".csv",
            filetypes=[("CSV Files", "*.csv")],
            parent=self
        )
        if not f: return
        try:
            shutil.copyfile(csv_path, f)
            messagebox.showinfo("Export Successful", f"SSO exported to:\n{f}", parent=self)
        except Exception as e:
            messagebox.showerror("Export Failed", f"Error: {e}", parent=self)
