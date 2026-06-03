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
        
    client.close()
    
    if labels_error:
        raise ConnectionError(f"Security governance fetch failed.\nLabels Error: {labels_error}")
        
    return {
        "labels": labels,
        "labels_error": labels_error
    }


class DataSecurityGovernanceFrame(ctk.CTkFrame):
    """Self-contained customtkinter component wrapping Data Security & Governance UI."""
    
    def __init__(self, master, log_callback, credentials_callback, status_change_callback, **kwargs):
        super().__init__(master, fg_color="transparent", **kwargs)
        self.log_msg = log_callback
        self.get_credentials = credentials_callback
        self.on_status_change = status_change_callback
        self.status = None  # 'loading', 'success', 'error', None
        
        self.flattened_rows = []
        self.current_page = 0
        self.ITEMS_PER_PAGE = 8
        
        self.build_ui()

    def build_ui(self):
        """Creates card container for the tab."""
        self.pack(fill="x", expand=True, pady=(20, 10))
        
        # Permanent section heading visible during loading and error states
        self.main_title = ctk.CTkLabel(self, text="Data Security & Governance", font=FONT_HEADER_SMALL, text_color=COLOR_TEXT_MAIN)
        self.main_title.pack(anchor="w", pady=(0, 10))
        
        self.state_frame = ctk.CTkFrame(self, fg_color="transparent")
        
        # Grid for Sensitivity Labels
        self.labels_grid = ctk.CTkFrame(
            self,
            fg_color=COLOR_SURFACE,
            border_color=COLOR_OUTLINE_LIGHT,
            border_width=1,
            corner_radius=8
        )
        
        # Pagination controls frame (centered below the grid)
        self.pagination_frame = ctk.CTkFrame(self, fg_color="transparent")
        
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
        
        self.reset_view()

    def reset_view(self):
        """Resets and hides grids."""
        self.pack_forget()
        self.state_frame.pack_forget()
        self.labels_grid.pack_forget()
        self.pagination_frame.pack_forget()
        
        for w in self.state_frame.winfo_children():
            w.destroy()
        for w in self.labels_grid.winfo_children():
            w.destroy()
            
        self.flattened_rows = []
        self.current_page = 0

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
        self.status = "loading"
        self.on_status_change()
        
        self.pack(fill="x", expand=True, pady=(20, 10))
        self.labels_grid.pack_forget()
        
        self._set_state_loading("Retrieving tenant Sensitivity Labels...")
        
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

        labels = data.get("labels")
        labels_error = data.get("labels_error")

        self.status = "success"

        # 1. Render Sensitivity Labels Grid
        self.labels_grid.pack(fill="x", expand=True, pady=(0, 15))

        if labels_error:
            ctk.CTkLabel(self.labels_grid, text=f"✖ Failed to load Sensitivity Labels: {labels_error}", font=FONT_BODY_MEDIUM, text_color=COLOR_ERROR).pack(padx=20, pady=20)
            self.pagination_frame.pack_forget()
        elif labels is None or not labels:
            ctk.CTkLabel(self.labels_grid, text="No Sensitivity Labels configured in this tenant.", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB).pack(padx=20, pady=20)
            self.pagination_frame.pack_forget()
        else:
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
                cell.grid(row=0, column=col_idx, sticky="nsew", padx=1, pady=1)
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

        self.on_status_change()

    def _display_current_page(self):
        # Destroy existing data rows (row > 0)
        for w in self.labels_grid.winfo_children():
            info = w.grid_info()
            if "row" in info and int(info["row"]) > 0:
                w.destroy()

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
            bg_style = "transparent" if r_idx % 2 != 0 else COLOR_SURFACE_VARIANT
            
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
            c0.grid(row=r_idx, column=0, sticky="nsew", padx=1, pady=1)
            lbl_name = ctk.CTkLabel(c0, text=name, font=name_font, text_color=name_color)
            lbl_name.pack(padx=10, pady=6, anchor="w")
            c0.bind("<Configure>", lambda e, l=lbl_name: l.configure(wraplength=e.width - 20))

            c1 = ctk.CTkFrame(self.labels_grid, fg_color=bg_style, corner_radius=0)
            c1.grid(row=r_idx, column=1, sticky="nsew", padx=1, pady=1)
            lbl_desc = ctk.CTkLabel(c1, text=desc, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN)
            lbl_desc.pack(padx=10, pady=6, anchor="w")
            c1.bind("<Configure>", lambda e, l=lbl_desc: l.configure(wraplength=e.width - 20))

            c2 = ctk.CTkFrame(self.labels_grid, fg_color=bg_style, corner_radius=0)
            c2.grid(row=r_idx, column=2, sticky="nsew", padx=1, pady=1)
            ctk.CTkLabel(c2, text=protection, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=6, anchor="w")

            c3 = ctk.CTkFrame(self.labels_grid, fg_color=bg_style, corner_radius=0)
            c3.grid(row=r_idx, column=3, sticky="nsew", padx=1, pady=1)
            ctk.CTkLabel(c3, text=mode, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=6, anchor="w")

            c4 = ctk.CTkFrame(self.labels_grid, fg_color=bg_style, corner_radius=0)
            c4.grid(row=r_idx, column=4, sticky="nsew", padx=1, pady=1)
            ctk.CTkLabel(c4, text=priority, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=6, anchor="w")

            c5 = ctk.CTkFrame(self.labels_grid, fg_color=bg_style, corner_radius=0)
            c5.grid(row=r_idx, column=5, sticky="nsew", padx=1, pady=1)
            lbl_app = ctk.CTkLabel(c5, text=applicable, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN)
            lbl_app.pack(padx=10, pady=6, anchor="w")
            c5.bind("<Configure>", lambda e, l=lbl_app: l.configure(wraplength=e.width - 20))

            c6 = ctk.CTkFrame(self.labels_grid, fg_color=bg_style, corner_radius=0)
            c6.grid(row=r_idx, column=6, sticky="nsew", padx=1, pady=1)
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
        self._set_state_error(err_msg)
        self.status = "error"
        self.on_status_change()

