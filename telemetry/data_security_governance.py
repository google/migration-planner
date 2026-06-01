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

from core.graph.client import GraphClient
from core.graph.security import SecurityService

# Bind to the async logger initialized in license_usage.py
usage_logger = logging.getLogger("LicenseUsageAsyncLogger")

# Import shared styles
from telemetry.styles import *

def run_security_governance_pipeline(client_id, client_secret, tenant_id) -> list[dict]:
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
    
    try:
        labels = service.fetch_sensitivity_labels()
        # Sort labels by priority descending
        labels.sort(key=lambda x: x.get("priority", 0), reverse=True)
        return labels
    finally:
        client.close()

class DataSecurityGovernanceFrame(ctk.CTkFrame):
    """Self-contained customtkinter component wrapping Data Security & Governance UI."""
    
    def __init__(self, master, log_callback, credentials_callback, status_change_callback, **kwargs):
        super().__init__(master, fg_color="transparent", **kwargs)
        self.log_msg = log_callback
        self.get_credentials = credentials_callback
        self.on_status_change = status_change_callback
        self.status = None  # 'loading', 'success', 'error', None
        
        self.build_ui()

    def build_ui(self):
        """Creates card container for the tab."""
        self.pack(fill="x", expand=True, pady=(20, 10))
        
        ctk.CTkLabel(self, text="Data Security & Governance (Sensitivity Labels)", font=FONT_HEADER_SMALL, text_color=COLOR_TEXT_MAIN).pack(anchor="w", pady=(0, 10))
        
        self.state_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.grid_frame = ctk.CTkFrame(self, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)
        
        self.reset_view()

    def reset_view(self):
        """Resets and hides grids."""
        self.pack_forget()
        self.state_frame.pack_forget()
        self.grid_frame.pack_forget()
        
        for w in self.state_frame.winfo_children():
            w.destroy()
        for w in self.grid_frame.winfo_children():
            w.destroy()

    def _set_state_loading(self, msg="Loading..."):
        for w in self.state_frame.winfo_children():
            w.destroy()
        ctk.CTkLabel(self.state_frame, text=f"⏳ {msg}", text_color=COLOR_TEXT_SUB, font=FONT_BODY_MEDIUM).pack(pady=20)
        self.state_frame.pack(fill="x", expand=True)

    def _set_state_error(self, error_msg):
        for w in self.state_frame.winfo_children():
            w.destroy()
        ctk.CTkLabel(self.state_frame, text=f"✖ {error_msg}", text_color=COLOR_ERROR, font=FONT_BODY_MEDIUM).pack(pady=(20, 10))
        ctk.CTkButton(self.state_frame, text="Try Again", command=self._retry_fetch, width=120, fg_color="transparent", border_width=1, text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER).pack(pady=(0, 20))
        self.state_frame.pack(fill="x", expand=True)

    def _retry_fetch(self):
        tenant, clients, secrets = self.get_credentials()
        if tenant:
            self.trigger_fetch(tenant, clients[0], secrets[0])

    def trigger_fetch(self, tenant, client_id, client_secret):
        """Triggers parallel fetches inside isolated background threads."""
        self.status = "loading"
        self.on_status_change()
        
        self.pack(fill="x", expand=True, pady=(20, 10))
        self.grid_frame.pack_forget()
        
        self._set_state_loading("Retrieving tenant Sensitivity Labels configuration...")
        
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

    def _render_success(self, data: list[dict]):
        self.state_frame.pack_forget()
        for w in self.grid_frame.winfo_children():
            w.destroy()

        if not data:
            ctk.CTkLabel(self.grid_frame, text="No Sensitivity Labels configured in this tenant.", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB).pack(padx=20, pady=20)
            self.grid_frame.pack(fill="x", expand=True)
            self.status = "success"
            self.on_status_change()
            return

        self.grid_frame.pack(fill="x", expand=True)

        # Define column weights for proper proportional spacing
        self.grid_frame.grid_columnconfigure(0, weight=2)  # Label Name
        self.grid_frame.grid_columnconfigure(1, weight=3)  # Description
        self.grid_frame.grid_columnconfigure(2, weight=1)  # Priority
        self.grid_frame.grid_columnconfigure(3, weight=2)  # Applicable To
        self.grid_frame.grid_columnconfigure(4, weight=1)  # Status

        headers = ["Sensitivity Label", "Description", "Priority", "Applicable Targets", "Status"]
        for col_idx, head_text in enumerate(headers):
            cell = ctk.CTkFrame(self.grid_frame, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=1, pady=1)
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        for r_idx, label in enumerate(data, start=1):
            bg_style = "transparent" if r_idx % 2 != 0 else COLOR_SURFACE_VARIANT
            
            name = label.get("name", "N/A")
            desc = label.get("description", "") or label.get("toolTip", "") or "N/A"
            priority = str(label.get("priority", 0))
            applicable = ", ".join([x.capitalize() for x in label.get("applicableTo", "").split(",") if x.strip()]) or "N/A"
            status = "🟢 Enabled" if label.get("isEnabled", True) else "🔴 Disabled"

            c0 = ctk.CTkFrame(self.grid_frame, fg_color=bg_style, corner_radius=0)
            c0.grid(row=r_idx, column=0, sticky="nsew", padx=1, pady=1)
            lbl_name = ctk.CTkLabel(c0, text=name, font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN)
            lbl_name.pack(padx=10, pady=6, anchor="w")
            c0.bind("<Configure>", lambda e, l=lbl_name: l.configure(wraplength=e.width - 20))

            c1 = ctk.CTkFrame(self.grid_frame, fg_color=bg_style, corner_radius=0)
            c1.grid(row=r_idx, column=1, sticky="nsew", padx=1, pady=1)
            lbl_desc = ctk.CTkLabel(c1, text=desc, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN)
            lbl_desc.pack(padx=10, pady=6, anchor="w")
            c1.bind("<Configure>", lambda e, l=lbl_desc: l.configure(wraplength=e.width - 20))

            c2 = ctk.CTkFrame(self.grid_frame, fg_color=bg_style, corner_radius=0)
            c2.grid(row=r_idx, column=2, sticky="nsew", padx=1, pady=1)
            ctk.CTkLabel(c2, text=priority, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=6, anchor="w")

            c3 = ctk.CTkFrame(self.grid_frame, fg_color=bg_style, corner_radius=0)
            c3.grid(row=r_idx, column=3, sticky="nsew", padx=1, pady=1)
            lbl_app = ctk.CTkLabel(c3, text=applicable, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN)
            lbl_app.pack(padx=10, pady=6, anchor="w")
            c3.bind("<Configure>", lambda e, l=lbl_app: l.configure(wraplength=e.width - 20))

            c4 = ctk.CTkFrame(self.grid_frame, fg_color=bg_style, corner_radius=0)
            c4.grid(row=r_idx, column=4, sticky="nsew", padx=1, pady=1)
            ctk.CTkLabel(c4, text=status, font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=6, anchor="w")

        self.status = "success"
        self.on_status_change()

    def _render_error(self, err_msg):
        self._set_state_error(err_msg)
        self.status = "error"
        self.on_status_change()
