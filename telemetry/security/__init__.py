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

"""Consolidated UI container for Data Security & Governance telemetry."""

import logging
import customtkinter as ctk
import webbrowser

from telemetry.styles import *
from telemetry.ediscovery_ui import EDiscoveryFrame
from telemetry.security.sensitivity_labels import SensitivityLabelsSubFrame
from telemetry.security.retention_policies import RetentionPoliciesSubFrame
from telemetry.security.dlp_policies import DLPPoliciesSubFrame
from telemetry.security.sensitive_info_types import SensitiveInfoTypesSubFrame
from telemetry.security.authentication import AuthenticationSubFrame
from telemetry.security.service_principals_sso import ServicePrincipalsSsoSubFrame

class DataSecurityGovernanceFrame(ctk.CTkFrame):
    """Card container enclosing all Data Security & Governance subframes."""

    def __init__(self, master, log_callback, credentials_callback, status_change_callback, **kwargs):
        self.semaphore = kwargs.pop("concurrency_semaphore", None)
        self.get_delegated_auth = kwargs.pop("delegated_auth_callback", None)
        
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
        
        # Main Title Header
        self.header = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.header.pack(fill="x", pady=(0, 10))
        ctk.CTkLabel(self.header, text="Data Security & Governance", font=FONT_HEADER_SMALL, text_color=COLOR_TEXT_MAIN).pack(side="left")

        # 1. Sensitivity Labels SubFrame
        self.sensitivity_frame = SensitivityLabelsSubFrame(
            self.inner_pad, self.log_msg, self.get_credentials, self._check_overall_status, semaphore=self.semaphore
        )
        
        # 2. Retention Compliance Policies SubFrame
        self.retention_frame = RetentionPoliciesSubFrame(
            self.inner_pad, self.log_msg, self.get_credentials, self._check_overall_status, semaphore=self.semaphore
        )
        
        # 3. DLP Policies SubFrame
        self.dlp_frame = DLPPoliciesSubFrame(
            self.inner_pad, self.log_msg, self.get_credentials, self._check_overall_status, semaphore=self.semaphore
        )

        # 4. Sensitive Information Types (SIT) SubFrame
        self.sit_frame = SensitiveInfoTypesSubFrame(
            self.inner_pad, self.log_msg, self.get_credentials, self._check_overall_status, semaphore=self.semaphore
        )

        # 5. Authentication Mechanics (Conditional Access) SubFrame
        self.auth_frame = AuthenticationSubFrame(
            self.inner_pad, self.log_msg, self.get_credentials, self._check_overall_status, semaphore=self.semaphore
        )

        # 6. Service Principals SSO Modes SubFrame
        self.sso_frame = ServicePrincipalsSsoSubFrame(
            self.inner_pad, self.log_msg, self.get_credentials, self._check_overall_status, semaphore=self.semaphore
        )

        # 7. eDiscovery Cases Section (Conditional based on Delegated Auth setting)
        self.ediscovery_header_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        ctk.CTkLabel(
            self.ediscovery_header_frame, text="eDiscovery Cases", font=FONT_HEADER_SMALL, text_color=COLOR_TEXT_MAIN
        ).pack(side="left", anchor="w")
        
        self.ediscovery_body_frame = ctk.CTkFrame(
            self.inner_pad, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8
        )
        self.ediscovery_content = ctk.CTkFrame(self.ediscovery_body_frame, fg_color="transparent")
        
        lbl_inst1 = ctk.CTkLabel(
            self.ediscovery_content,
            text="eDiscovery cases cannot be scanned directly under standard Application permissions. To view your active cases, please navigate to Microsoft Purview, or enable Delegated Authentication on the Connection screen to view them directly here.",
            font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="left", wraplength=700
        )
        lbl_inst1.pack(anchor="w", pady=(0, 8))
        
        lbl_cases_link = ctk.CTkLabel(
            self.ediscovery_content, text="🔗 Open Purview eDiscovery Cases Portal",
            font=FONT_BODY_BOLD, text_color=COLOR_PRIMARY, cursor="hand2"
        )
        lbl_cases_link.pack(anchor="w", pady=(0, 15))
        lbl_cases_link.bind("<Button-1>", lambda e: webbrowser.open("https://purview.microsoft.com/ediscovery/casespage"))
        lbl_cases_link.bind("<Enter>", lambda e: lbl_cases_link.configure(text_color=COLOR_PRIMARY_HOVER))
        lbl_cases_link.bind("<Leave>", lambda e: lbl_cases_link.configure(text_color=COLOR_PRIMARY))
        
        lbl_inst2 = ctk.CTkLabel(
            self.ediscovery_content,
            text="Note: Accessing eDiscovery cases requires your administrator account to have the eDiscovery Manager role assigned in the tenant permissions page:",
            font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB, justify="left", wraplength=700
        )
        lbl_inst2.pack(anchor="w", pady=(0, 8))
        
        lbl_roles_link = ctk.CTkLabel(
            self.ediscovery_content, text="🔗 Assign eDiscovery Manager Role in Purview Settings",
            font=FONT_BODY_BOLD, text_color=COLOR_PRIMARY, cursor="hand2"
        )
        lbl_roles_link.pack(anchor="w")
        lbl_roles_link.bind("<Button-1>", lambda e: webbrowser.open("https://purview.microsoft.com/settings/purviewpermissions"))
        lbl_roles_link.bind("<Enter>", lambda e: lbl_roles_link.configure(text_color=COLOR_PRIMARY_HOVER))
        lbl_roles_link.bind("<Leave>", lambda e: lbl_roles_link.configure(text_color=COLOR_PRIMARY))
        
        self.ediscovery_ui_view = EDiscoveryFrame(
            master=self.ediscovery_body_frame,
            log_callback=self.log_msg,
            credentials_callback=self.get_credentials,
            status_change_callback=self._check_overall_status,
            concurrency_semaphore=self.semaphore,
            delegated_auth_callback=self.get_delegated_auth
        )

        self.reset_view()

    def reset_view(self):
        self.pack_forget()
        self.sensitivity_frame.pack_forget()
        self.retention_frame.pack_forget()
        self.dlp_frame.pack_forget()
        self.sit_frame.pack_forget()
        self.auth_frame.pack_forget()
        self.sso_frame.pack_forget()
        self.ediscovery_header_frame.pack_forget()
        self.ediscovery_body_frame.pack_forget()
        
        self.sensitivity_frame.reset_view()
        self.retention_frame.reset_view()
        self.dlp_frame.reset_view()
        self.sit_frame.reset_view()
        self.auth_frame.reset_view()
        self.sso_frame.reset_view()
        self.ediscovery_ui_view.reset_view()
        self.status = None

    def trigger_fetch(self, tenant, client_id, client_secret):
        self.status = "loading"
        self.on_status_change()
        
        self.pack(fill="x", expand=True, pady=(20, 5))
        
        # Pack and trigger each subframe
        self.sensitivity_frame.pack(fill="x", pady=(0, 15))
        self.sensitivity_frame.trigger_fetch(tenant, client_id, client_secret)
        
        self.retention_frame.pack(fill="x", pady=(20, 15))
        self.retention_frame.trigger_fetch(tenant, client_id, client_secret)
        
        self.dlp_frame.pack(fill="x", pady=(20, 15))
        self.dlp_frame.trigger_fetch(tenant, client_id, client_secret)

        self.sit_frame.pack(fill="x", pady=(20, 15))
        self.sit_frame.trigger_fetch(tenant, client_id, client_secret)

        self.auth_frame.pack(fill="x", pady=(20, 15))
        self.auth_frame.trigger_fetch(tenant, client_id, client_secret)

        self.sso_frame.pack(fill="x", pady=(20, 15))
        self.sso_frame.trigger_fetch(tenant, client_id, client_secret)

        # Draw eDiscovery block
        self.ediscovery_header_frame.pack(fill="x", pady=(20, 5))
        self.ediscovery_body_frame.pack(fill="x", pady=(0, 15))
        
        use_delegated = self.get_delegated_auth() if self.get_delegated_auth else False
        if use_delegated:
            self.ediscovery_content.pack_forget()
            self.ediscovery_ui_view.pack(fill="x", expand=True)
            self.ediscovery_ui_view.trigger_fetch(tenant, client_id, client_secret, use_delegated_auth=True)
        else:
            self.ediscovery_ui_view.pack_forget()
            self.ediscovery_content.pack(fill="x", padx=20, pady=20)

    def _check_overall_status(self):
        statuses = [
            self.sensitivity_frame.status,
            self.retention_frame.status,
            self.dlp_frame.status,
            self.sit_frame.status,
            self.auth_frame.status,
            self.sso_frame.status,
            self.ediscovery_ui_view.status
        ]
        
        if "loading" in statuses:
            self.status = "loading"
        elif all(s == "error" for s in statuses if s is not None):
            self.status = "error"
        else:
            self.status = "success"
        self.on_status_change()

    def cancel(self):
        self.sensitivity_frame.cancel()
        self.retention_frame.cancel()
        self.dlp_frame.cancel()
        self.sit_frame.cancel()
        self.auth_frame.cancel()
        self.sso_frame.cancel()
        self.ediscovery_ui_view.reset_view()
        self.status = None
