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

"""Master coordinator tab and logging setup for Microsoft 365 tenant telemetry dashboard."""

import os
import sys
import time
import queue
import logging
import csv
import psutil
import threading
from typing import Any, Dict, List, Optional
import customtkinter as ctk
from tkinter import messagebox
from logging.handlers import QueueHandler, QueueListener

# Import modular view frames from their consolidated code/view modules
from telemetry.subscribed_skus import SubscribedSKUsFrame
from telemetry.directory import DirectoryFrame
from telemetry.m365_apps_telemetry import M365AppsTelemetryFrame
from telemetry.power_automate import PowerAutomateUsageFrame


# Import existing modular views
from telemetry.files_telemetry import FilesTelemetryFrame
from telemetry.devices_apps_telemetry import DevicesAppsTelemetryFrame
from telemetry.email_client_support import EmailClientSupportFrame
from telemetry.exchange_online import ExchangeOnlineFrame
from telemetry.data_security_governance import DataSecurityGovernanceFrame
from telemetry.intune_policies import IntunePoliciesFrame

from telemetry.styles import *


# =================================================================================
# ASYNC FILE LOGGING SETUP
# =================================================================================

_current_dir = os.path.dirname(os.path.abspath(__file__))
_log_dir = os.path.join(_current_dir, 'logs')
os.makedirs(_log_dir, exist_ok=True)

_log_queue = queue.Queue(-1)
_log_file_path = os.path.join(_log_dir, 'telemetry_log.txt')
_file_handler = logging.FileHandler(_log_file_path, mode='a', encoding='utf-8')
_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
_file_handler.setFormatter(_formatter)

_queue_listener = QueueListener(_log_queue, _file_handler)
_queue_listener.start()

# Configure the root logger to send all records to the log queue and clear other handlers (stdout/stderr)
root_logger = logging.getLogger()
root_logger.setLevel(logging.WARNING)
for h in list(root_logger.handlers):
    root_logger.removeHandler(h)
root_logger.addHandler(QueueHandler(_log_queue))

# Set logger level to INFO for our own application packages/loggers
async_logger = logging.getLogger("M365TelemetryAsyncLogger")
async_logger.setLevel(logging.INFO)
async_logger.propagate = True

logging.getLogger("core").setLevel(logging.INFO)
logging.getLogger("util").setLevel(logging.INFO)
logging.getLogger("chat").setLevel(logging.INFO)
logging.getLogger("PowerShellClient").setLevel(logging.INFO)


def update_log_directory(tenant_id: Optional[str] = None, client_id: Optional[str] = None) -> None:
    """Updates the log directory dynamically once tenant and client ID are known, or reverts to default."""
    global _file_handler, _queue_listener

    try:
        _queue_listener.stop()
    except Exception:
        pass
        
    try:
        _file_handler.close()
    except Exception:
        pass

    if tenant_id and client_id:
        new_log_dir = os.path.join(_current_dir, 'logs', f"{tenant_id}_{client_id}")
    else:
        new_log_dir = os.path.join(_current_dir, 'logs')

    os.makedirs(new_log_dir, exist_ok=True)
    new_log_file_path = os.path.join(new_log_dir, 'telemetry_log.txt')

    _file_handler = logging.FileHandler(new_log_file_path, mode='a', encoding='utf-8')
    _file_handler.setFormatter(_formatter)

    _queue_listener = QueueListener(_log_queue, _file_handler)
    _queue_listener.start()


# =================================================================================
# MAIN TAB COORDINATOR
# =================================================================================

class M365TelemetryTab(ctk.CTkFrame):
    """Encapsulates the UI coordinator for the Microsoft 365 Telemetry & Audit dashboard tab."""

    def __init__(self, master, log_callback, retries_var, backoff_var, **kwargs):
        super().__init__(
            master,
            fg_color="transparent",
            **kwargs
        )
        async_logger.info("Initializing M365TelemetryTab instance.")

        self.log_msg = log_callback
        self.retries = retries_var
        self.backoff = backoff_var

        self.lic_tenant_id = ctk.StringVar()
        self.lic_client_ids = ctk.StringVar()
        self.lic_client_secrets = ctk.StringVar()

        self.on_all_done_callback = None
        self.on_fetch_started_callback = None
        self.telemetry_semaphore = threading.Semaphore(3)
        self.is_fetching = False

        self.tabs = [
            ("tenant_identity", "Tenant & Identity"),
            ("comm_storage", "Communication & Storage"),
            ("apps_automation", "Apps & Automation"),
            ("security_compliance", "Security & Compliance")
        ]

        self.tab_status = {
            "tenant_identity": "pending",
            "comm_storage": "pending",
            "apps_automation": "pending",
            "security_compliance": "pending"
        }

        self.tab_descriptions = {
            "tenant_identity": "Directory Summary, Subscribed SKUs, and Microsoft Entra Data.",
            "comm_storage": "Email mailboxes, Exchange Calendar/contacts telemetry, and Files storage (SharePoint & OneDrive).",
            "apps_automation": "Microsoft 365 Apps active users usage and Power Automate workflows.",
            "security_compliance": "Data Security & Governance (Sensitivity labels, retention/DLP policies) and Microsoft Intune policies."
        }

        self.tab_frames = {}
        self.tab_labels = {}
        self.tab_reload_buttons = {}
        self.tab_containers = {}
        self.tab_prefetch_frames = {}
        self.tab_scroll_frames = {}
        self.active_tab = "tenant_identity"

        self.build_ui()

        self.tab_views_mapping = {
            "tenant_identity": [self.subscribed_skus_view, self.directory_view, self.devices_apps_view],
            "comm_storage": [self.exchange_online_view, self.files_view],
            "apps_automation": [self.m365_apps_view, self.power_automate_view],
            "security_compliance": [self.security_gov_view, self.intune_policies_view]
        }



        # Start a background daemon thread to monitor memory consumption every 30s
        self.mem_monitor_active = True
        self.mem_monitor_thread = threading.Thread(target=self._monitor_memory_loop, daemon=True)
        self.mem_monitor_thread.start()

    def _create_entry(self, parent, label, var, show=None):
        f = ctk.CTkFrame(parent, fg_color="transparent")
        f.pack(fill="x", pady=5)
        ctk.CTkLabel(f, text=label, width=100, anchor="w", text_color=COLOR_TEXT_SUB).pack(side="left")
        ctk.CTkEntry(
            f, textvariable=var, show=show, height=40, corner_radius=4,
            border_width=1, border_color=COLOR_OUTLINE, fg_color="transparent",
            text_color=COLOR_TEXT_MAIN,
        ).pack(side="left", fill="x", expand=True)

    def _create_prefetch_view(self, parent, tab_key, tab_title, description):
        f = ctk.CTkFrame(parent, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=12)
        
        # Center the box
        inner = ctk.CTkFrame(f, fg_color="transparent")
        inner.place(relx=0.5, rely=0.5, anchor="center")
        
        ctk.CTkLabel(inner, text=f"{tab_title} Report", font=ctk.CTkFont(family="Segoe UI", size=16, weight="bold"), text_color=COLOR_TEXT_MAIN).pack(pady=(0, 10))
        
        desc_lbl = ctk.CTkLabel(inner, text=description, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB, justify="center", wraplength=450)
        desc_lbl.pack(pady=(0, 20))
        
        btn = ctk.CTkButton(
            inner,
            text="Fetch Report",
            width=200,
            height=40,
            corner_radius=8,
            fg_color=COLOR_PRIMARY,
            hover_color=COLOR_PRIMARY_HOVER,
            font=FONT_BODY_BOLD,
            command=lambda k=tab_key: self.fetch_tab_data(k)
        )
        btn.pack(pady=(0, 10))
        
        return f

    def build_ui(self):
        async_logger.info("Building graphical UI elements for M365 Telemetry Tab.")

        # ----------------------------------------------------
        # HORIZONTAL TAB STRIP
        # ----------------------------------------------------
        self.tab_strip_frame = ctk.CTkFrame(self, fg_color="transparent", height=40)
        self.tab_strip_frame.pack(fill="x", pady=(0, 0))

        for tab_key, tab_title in self.tabs:
            # Container box styled to look like a modern button
            btn_frame = ctk.CTkFrame(self.tab_strip_frame, height=40, corner_radius=8, fg_color="transparent")
            btn_frame.pack(side="left", fill="x", expand=True, padx=2)
            self.tab_frames[tab_key] = btn_frame

            # Centered inner container hosting label & reload icon
            inner_content = ctk.CTkFrame(btn_frame, fg_color="transparent")
            inner_content.place(relx=0.5, rely=0.5, anchor="center")

            # Main text label
            lbl = ctk.CTkLabel(
                inner_content,
                text=tab_title,
                font=ctk.CTkFont(family="Segoe UI", size=14, weight="bold"),
                text_color=COLOR_TEXT_SUB
            )
            lbl.pack(side="left")
            self.tab_labels[tab_key] = lbl

            # Nested reload button (larger icon, placed at the right edge)
            reload_btn = ctk.CTkButton(
                btn_frame,
                text="↻",
                width=28,
                height=28,
                corner_radius=14,
                fg_color="transparent",
                text_color=COLOR_TEXT_SUB,
                hover_color=COLOR_SECONDARY_HOVER,
                font=ctk.CTkFont(family="Segoe UI", size=16, weight="bold"),
                command=lambda k=tab_key: self.refetch_tab(k)
            )
            self.tab_reload_buttons[tab_key] = reload_btn
            ToolTip(reload_btn, "Refetch Tab Report")

            # Click & Hover bindings to simulate native CTkButton behavior
            def make_click_handler(k=tab_key):
                return lambda event: self.select_tab(k)

            def make_enter_handler(f=btn_frame, k=tab_key):
                return lambda event: f.configure(fg_color=COLOR_SECONDARY_HOVER) if self.active_tab != k else None

            def make_leave_handler(f=btn_frame, k=tab_key):
                return lambda event: f.configure(fg_color="transparent") if self.active_tab != k else None

            click_handler = make_click_handler()
            enter_handler = make_enter_handler()
            leave_handler = make_leave_handler()

            btn_frame.bind("<Button-1>", click_handler)
            lbl.bind("<Button-1>", click_handler)
            inner_content.bind("<Button-1>", click_handler)

            btn_frame.bind("<Enter>", enter_handler)
            btn_frame.bind("<Leave>", leave_handler)
            lbl.bind("<Enter>", enter_handler)
            lbl.bind("<Leave>", leave_handler)
            inner_content.bind("<Enter>", enter_handler)
            inner_content.bind("<Leave>", leave_handler)

        # Status indicator packed directly below the tabs
        self.lbl_lic_status = ctk.CTkLabel(self, text="", font=FONT_BODY_MEDIUM)
        self.lbl_lic_status.pack(anchor="w", pady=(2, 2))

        # Hidden elements (will be hidden programmatically by adapt_embedded_view)
        self.top_desc_lbl = ctk.CTkLabel(self, text="Connect your Microsoft Azure account to authenticate and audit tenant licensing bundle inventories and usage.", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB)
        self.top_desc_lbl.pack(anchor="w", pady=(0, 15))

        self.inputs_frame = ctk.CTkFrame(self, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)
        self.inputs_frame.pack(fill="x", pady=5)

        inner_pad = ctk.CTkFrame(self.inputs_frame, fg_color="transparent")
        inner_pad.pack(fill="x", padx=15, pady=15)

        self._create_entry(inner_pad, "Tenant ID", self.lic_tenant_id)
        self._create_entry(inner_pad, "Client ID", self.lic_client_ids)
        self._create_entry(inner_pad, "Client Secret", self.lic_client_secrets, show="*")

        self.actions_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.actions_frame.pack(fill="x", pady=(20, 5))

        self.btn_lic_submit = ctk.CTkButton(
            self.actions_frame, text="Submit", width=160, height=40, corner_radius=20,
            font=FONT_BODY_BOLD, fg_color=COLOR_PRIMARY, hover_color=COLOR_PRIMARY_HOVER,
            command=self.authenticate_licenses_tab,
        )
        self.btn_lic_submit.pack(side="left")



        # ----------------------------------------------------
        # TAB CONTENT HOST FRAME
        # ----------------------------------------------------
        self.tab_content_area = ctk.CTkFrame(self, fg_color="transparent")
        self.tab_content_area.pack(fill="both", expand=True)

        for tab_key, tab_title in self.tabs:
            tab_container = ctk.CTkFrame(self.tab_content_area, fg_color="transparent")
            self.tab_containers[tab_key] = tab_container

            # 1. Pre-fetch view
            desc = self.tab_descriptions[tab_key]
            prefetch = self._create_prefetch_view(tab_container, tab_key, tab_title, desc)
            prefetch.pack(fill="both", expand=True)
            self.tab_prefetch_frames[tab_key] = prefetch

            # 2. Report view (scrollable frame)
            scroll = ctk.CTkScrollableFrame(
                tab_container,
                fg_color="transparent",
                scrollbar_button_color="white",
                scrollbar_button_hover_color=COLOR_SECONDARY_HOVER
            )
            self.tab_scroll_frames[tab_key] = scroll

        # ----------------------------------------------------
        # MODULAR UI SECTIONS
        # ----------------------------------------------------

        # 1. Subscribed SKUs Section
        self.subscribed_skus_view = SubscribedSKUsFrame(
            master=self.tab_scroll_frames["tenant_identity"],
            log_callback=self.log_msg,
            credentials_callback=self._get_credentials,
            status_change_callback=self._check_all_done,
            retries_var=self.retries,
            backoff_var=self.backoff,
            concurrency_semaphore=self.telemetry_semaphore
        )

        # 5d. Devices & Apps Section (Microsoft Entra Data)
        self.devices_apps_view = DevicesAppsTelemetryFrame(
            master=self.tab_scroll_frames["tenant_identity"],
            log_callback=self.log_msg,
            credentials_callback=self._get_credentials,
            status_change_callback=self._check_all_done,
            concurrency_semaphore=self.telemetry_semaphore
        )

        # 1b. Directory Groups Section
        self.directory_view = DirectoryFrame(
            master=self.tab_scroll_frames["tenant_identity"],
            log_callback=self.log_msg,
            credentials_callback=self._get_credentials,
            status_change_callback=self._check_all_done,
            retries_var=self.retries,
            backoff_var=self.backoff,
            concurrency_semaphore=self.telemetry_semaphore
        )

        # 2. M365 Apps Section (Uber Container)
        self.m365_apps_view = M365AppsTelemetryFrame(
            master=self.tab_scroll_frames["apps_automation"],
            log_callback=self.log_msg,
            credentials_callback=self._get_credentials,
            status_change_callback=self._check_all_done,
            concurrency_semaphore=self.telemetry_semaphore
        )

        # 5. Exchange Online Usage Section
        self.exchange_online_view = ExchangeOnlineFrame(
            master=self.tab_scroll_frames["comm_storage"],
            log_callback=self.log_msg,
            credentials_callback=self._get_credentials,
            status_change_callback=self._check_all_done,
            concurrency_semaphore=self.telemetry_semaphore
        )

        # 5c. Files (SharePoint & OneDrive) Section
        self.files_view = FilesTelemetryFrame(
            master=self.tab_scroll_frames["comm_storage"],
            log_callback=self.log_msg,
            credentials_callback=self._get_credentials,
            status_change_callback=self._check_all_done,
            concurrency_semaphore=self.telemetry_semaphore
        )

        # 6. Data Security & Governance Section
        self.security_gov_view = DataSecurityGovernanceFrame(
            master=self.tab_scroll_frames["security_compliance"],
            log_callback=self.log_msg,
            credentials_callback=self._get_credentials,
            status_change_callback=self._check_all_done,
            concurrency_semaphore=self.telemetry_semaphore
        )

        # 6.5. Intune Policies Section
        self.intune_policies_view = IntunePoliciesFrame(
            master=self.tab_scroll_frames["security_compliance"],
            log_callback=self.log_msg,
            credentials_callback=self._get_credentials,
            status_change_callback=self._check_all_done,
            concurrency_semaphore=self.telemetry_semaphore
        )

        # 7. Power Automate Section
        self.power_automate_view = PowerAutomateUsageFrame(
            master=self.tab_scroll_frames["apps_automation"],
            log_callback=self.log_msg,
            credentials_callback=self._get_credentials,
            status_change_callback=self._check_all_done,
            concurrency_semaphore=self.telemetry_semaphore
        )

        self._hide_all_grids()

        # Wrap all leaf views to support cancellation and prevent race conditions
        for leaf in self._get_all_leaf_views():
            self._wrap_view_for_cancellation(leaf)

        # Select the default active tab
        self.select_tab("tenant_identity")

    def select_tab(self, tab_key):
        self.active_tab = tab_key
        for key in self.tab_frames:
            frame = self.tab_frames[key]
            lbl = self.tab_labels[key]
            reload_btn = self.tab_reload_buttons.get(key)
            if key == tab_key:
                frame.configure(fg_color=COLOR_PRIMARY)
                lbl.configure(text_color="white")
                if reload_btn:
                    reload_btn.configure(text_color="white", hover_color="#2563EB")
            else:
                frame.configure(fg_color="transparent")
                lbl.configure(text_color=COLOR_TEXT_SUB)
                if reload_btn:
                    reload_btn.configure(text_color=COLOR_TEXT_SUB, hover_color=COLOR_SECONDARY_HOVER)

        for key, container in self.tab_containers.items():
            if key == tab_key:
                container.pack(fill="both", expand=True)
            else:
                container.pack_forget()

        self._update_refetch_buttons_visibility()

    def _update_refetch_buttons_visibility(self):
        """Shows or hides each tab's nested reload button based on its scan completion status."""
        for tab_key, reload_btn in self.tab_reload_buttons.items():
            status = self.tab_status.get(tab_key)
            if status in ["success", "error"] and not getattr(self, "is_fetching", False):
                reload_btn.place(relx=1.0, x=-22, rely=0.5, anchor="center")
            else:
                reload_btn.place_forget()

    def refetch_tab(self, tab_key):
        """Triggers a refetch of a specific tab."""
        self.reset_single_tab(tab_key)
        self.fetch_tab_data(tab_key)

    def reset_single_tab(self, tab_key):
        """Resets a single tab back to pending (prefetch) state."""
        self.tab_status[tab_key] = "pending"
        self.tab_scroll_frames[tab_key].pack_forget()
        self.tab_prefetch_frames[tab_key].pack(fill="both", expand=True)
        # Clear specific grids/data for this tab
        for view in self.tab_views_mapping[tab_key]:
            if hasattr(view, "reset_view"):
                view.reset_view()

    def fetch_tab_data(self, tab_key):
        tenant, clients, secrets = self._get_credentials()
        if not tenant:
            messagebox.showerror("Credential Error", "Please provide complete Tenant ID, Client ID, and Client Secret strings.", parent=self)
            return

        self.tab_prefetch_frames[tab_key].pack_forget()
        self.tab_scroll_frames[tab_key].pack(fill="both", expand=True)

        self.tab_status[tab_key] = "loading"
        self.is_fetching = True
        if hasattr(self, "on_fetch_started_callback") and self.on_fetch_started_callback:
            self.on_fetch_started_callback()

        self.lbl_lic_status.configure(text=f"Scanning {self.tab_labels[tab_key].cget('text')} tab...", text_color=COLOR_TEXT_SUB)
        self._update_refetch_buttons_visibility()

        views = self.tab_views_mapping[tab_key]
        for view in views:
            if isinstance(view, SubscribedSKUsFrame):
                view.trigger_fetch(tenant, clients, secrets)
            else:
                view.trigger_fetch(tenant, clients[0], secrets[0])

    def _hide_all_grids(self):
        views = [
            self.subscribed_skus_view,
            self.directory_view,
            self.m365_apps_view,
            self.exchange_online_view,
            self.files_view,
            self.devices_apps_view,
            self.security_gov_view,
            self.intune_policies_view,
            self.power_automate_view
        ]
        for view in views:
            view.reset_view()
            view.status = None

    def reset_tab(self):
        """Resets the coordinator status, credentials variables, submission button, and hides all grids."""
        if getattr(self, "is_fetching", False):
            self.cancel_fetching()
            
        async_logger.info("Resetting M365TelemetryTab coordinator and hiding all sub-grids.")
        self.lic_tenant_id.set("")
        self.lic_client_ids.set("")
        self.lic_client_secrets.set("")

        for tab_key in self.tab_status:
            self.tab_status[tab_key] = "pending"
            self.tab_scroll_frames[tab_key].pack_forget()
            self.tab_prefetch_frames[tab_key].pack(fill="both", expand=True)

        self._hide_all_grids()
        self.btn_lic_submit.configure(state="normal", text="Submit")
        self.lbl_lic_status.configure(text="")
        self._update_refetch_buttons_visibility()
        self.select_tab("tenant_identity")

    def _get_credentials(self):
        tenant = self.lic_tenant_id.get().strip()
        client_str = self.lic_client_ids.get().strip()
        secret_str = self.lic_client_secrets.get().strip()

        if not tenant or not client_str or not secret_str:
            return None, None, None

        clients = [x.strip() for x in client_str.split(",") if x.strip()]
        secrets = [x.strip() for x in secret_str.split(",") if x.strip()]
        return tenant, clients, secrets

    def _check_all_done(self):
        """Checks the completion status of the sub-views in the currently loading tabs."""
        if not getattr(self, "is_fetching", False):
            return

        for tab_key, views in self.tab_views_mapping.items():
            if self.tab_status[tab_key] == "loading":
                statuses = [v.status for v in views]
                if "loading" in statuses:
                    continue
                elif all(s == "success" for s in statuses):
                    self.tab_status[tab_key] = "success"
                elif "error" in statuses:
                    self.tab_status[tab_key] = "error"
                else:
                    self.tab_status[tab_key] = None

        loading_tabs = [k for k, status in self.tab_status.items() if status == "loading"]
        
        if not loading_tabs:
            self.is_fetching = False
            self.btn_lic_submit.configure(state="normal", text="Submit")
            
            any_failed = any(s == "error" for s in self.tab_status.values())
            any_succeeded = any(s == "success" for s in self.tab_status.values())
            
            if not any_failed and any_succeeded:
                self.lbl_lic_status.configure(text="✔ Telemetry tab scan completed successfully!", text_color=COLOR_SUCCESS)
            elif any_succeeded:
                self.lbl_lic_status.configure(text="⚠ Some tab scans completed with errors.", text_color=COLOR_ERROR)
            else:
                self.lbl_lic_status.configure(text="✖ Scans failed or cancelled.", text_color=COLOR_ERROR)

            self._update_refetch_buttons_visibility()

            if hasattr(self, "on_all_done_callback") and self.on_all_done_callback:
                self.on_all_done_callback(any_succeeded)

    def authenticate_licenses_tab(self):
        """Master full sequential fetch of sections, or cancel if already fetching."""
        if getattr(self, "is_fetching", False):
            self.cancel_fetching()
            return

        async_logger.info("Master Submit triggered. Restarting all tab fetches.")

        tenant, clients, secrets = self._get_credentials()
        if not tenant:
            async_logger.warning("Authentication aborted: Missing credential parameters.")
            messagebox.showerror("Credential Error", "Please provide complete Tenant ID, Client ID, and Client Secret strings.", parent=self)
            return

        self.is_fetching = True
        if hasattr(self, "on_fetch_started_callback") and self.on_fetch_started_callback:
            self.on_fetch_started_callback()

        self.btn_lic_submit.configure(state="normal", text="Cancel")
        self.lbl_lic_status.configure(text="Querying Microsoft Graph APIs and Reports...", text_color=COLOR_TEXT_SUB)

        # Trigger fetch on all tabs
        for tab_key in ["tenant_identity", "comm_storage", "apps_automation", "security_compliance"]:
            self.fetch_tab_data(tab_key)

    def cancel_fetching(self):
        """Cancels the current fetching process and stops all subsequent batches."""
        async_logger.info("Cancellation triggered by user.")
        self.is_fetching = False
        
        self.btn_lic_submit.configure(state="normal", text="Submit")
        self.lbl_lic_status.configure(text="✖ Query cancelled by user.", text_color=COLOR_ERROR)
        
        for leaf in self._get_all_leaf_views():
            if hasattr(leaf, "cancel"):
                leaf.cancel()

        for tab_key in self.tab_status:
            if self.tab_status[tab_key] == "loading":
                self.tab_status[tab_key] = "error"

        self._update_refetch_buttons_visibility()

        if hasattr(self, "on_all_done_callback") and self.on_all_done_callback:
            self.on_all_done_callback(False)

    def _get_all_leaf_views(self):
        """Returns a list of all active leaf/base telemetry views across all cards."""
        return [
            self.subscribed_skus_view,
            self.devices_apps_view.auth_methods_subframe,
            self.devices_apps_view.app_signins_subframe,
            self.directory_view,
            self.m365_apps_view.active_users_view,
            self.m365_apps_view.active_users_trend_view,
            self.m365_apps_view.m365_apps_view,
            self.exchange_online_view.mailbox_view,
            self.exchange_online_view.calendar_view,
            self.exchange_online_view.apps_view,
            self.exchange_online_view.mail_security_view,
            self.exchange_online_view.connectors_view,
            self.exchange_online_view.email_clients_view,
            self.files_view.sharepoint_view,
            self.files_view.onedrive_view,
            self.security_gov_view,
            self.intune_policies_view,
            self.power_automate_view
        ]

    def _find_main_title_label(self, view):
        """Recursively searches the widget tree to identify the exact header/title label of a leaf view."""
        known_titles = {
            "SubscribedSKUsFrame": "Subscribed SKUs",
            "DirectoryFrame": "Directory Summary",
            "ActiveUsersUsageFrame": "Active Users Usage",
            "ActiveUsersTrendFrame": "Active Users Trend",
            "M365AppUsageFrame": "M365 App Usage",
            "MailboxUsageFrame": "Mailbox Usage Summary",
            "CalendarTelemetryFrame": "Calendar & Room Resource Telemetry",
            "ExchangeAppsFrame": "Exchange Integrated Apps",
            "ExchangeConnectorsFrame": "Exchange Connectors (Inbound & Outbound Routing)",
            "MailSecurityFrame": "Mail Security",
            "EmailClientSupportFrame": "Email Client Classification",
            "SharePointUsageFrame": "SharePoint Online Sites & Files Summary",
            "OneDriveUsageFrame": "OneDrive for Business Personal Accounts Summary",
            "DevicesAppsTelemetryFrame": "Devices & Apps Summary (Sign-in Telemetry)",
            "DataSecurityGovernanceFrame": "Data Security & Governance",
            "IntunePoliciesFrame": "Intune Policies (Device Configurations)",
            "PowerAutomateUsageFrame": "Power Automate (Workflows & Flows)"
        }
        
        target_text = known_titles.get(view.__class__.__name__, "")
        
        def search(widget):
            if isinstance(widget, ctk.CTkLabel):
                try:
                    text = widget.cget("text")
                    if text == target_text or (target_text and target_text in text):
                        return widget
                except Exception:
                    pass
            if hasattr(widget, "winfo_children"):
                for child in widget.winfo_children():
                    res = search(child)
                    if res:
                        return res
            return None
            
        lbl = search(view)
        if lbl:
            return lbl
            
        # Fallback: search for the first CTkLabel
        def first_label(widget):
            if isinstance(widget, ctk.CTkLabel):
                return widget
            if hasattr(widget, "winfo_children"):
                for child in widget.winfo_children():
                    res = first_label(child)
                    if res:
                        return res
            return None
        return first_label(view)

    def _wrap_view_for_cancellation(self, view):
        """Wraps a leaf view's trigger, render/handle, reset and after methods dynamically to enforce thread safety, cancellation, stale thread filtering, and precise execution time tracking."""
        view.current_request_id = 0
        view.is_cancelled = False
        view.fetch_time_lbl = None
        view.fetch_start_time = 0.0
        
        orig_trigger = view.trigger_fetch
        orig_reset = view.reset_view
        orig_after = view.after
        
        # Wrap the semaphore if present to capture the exact start time after acquisition
        if getattr(view, "semaphore", None):
            orig_sem = view.semaphore
            
            class WrappedSemaphore:
                def __init__(self, sem):
                    self._sem = sem
                    self._acquired_threads = set()
                def acquire(self, *args, **kwargs):
                    res = self._sem.acquire(*args, **kwargs)
                    cur_thread = threading.current_thread()
                    thread_req_id = getattr(cur_thread, "request_id", None)
                    if thread_req_id is not None and thread_req_id < view.current_request_id:
                        self._sem.release()
                        raise InterruptedError("Thread execution cancelled (stale request).")
                    self._acquired_threads.add(cur_thread.ident)
                    sub_sec = getattr(cur_thread, "sub_section", None)
                    if sub_sec:
                        view.sub_section_start_times[sub_sec] = time.time()
                    else:
                        view.fetch_start_time = time.time()
                    return res
                def release(self, *args, **kwargs):
                    cur_thread = threading.current_thread()
                    if cur_thread.ident in self._acquired_threads:
                        self._acquired_threads.remove(cur_thread.ident)
                        return self._sem.release(*args, **kwargs)
                    return None
                def __getattr__(self, name):
                    return getattr(self._sem, name)
                    
            view.semaphore = WrappedSemaphore(orig_sem)
        
        def display_fetch_time(elapsed):
            # Destroy old label if exists
            if hasattr(view, "fetch_time_lbl") and view.fetch_time_lbl:
                try:
                    view.fetch_time_lbl.destroy()
                except Exception:
                    pass
                view.fetch_time_lbl = None
            
            # Create a new floating label next to the title
            view.fetch_time_lbl = ctk.CTkLabel(
                view,
                text=f"⏱ {elapsed:.2f}s",
                font=ctk.CTkFont(family="Segoe UI", size=11, weight="bold"),
                text_color=COLOR_PRIMARY
            )
            
            # Automatically pack the timer into the header so Tkinter resolves layout clashes with buttons
            header_target = getattr(view, "lic_header", getattr(view, "pa_header", getattr(view, "header", getattr(view, "header_frame", None))))
            if header_target:
                view.fetch_time_lbl.pack(
                    in_=header_target,
                    side="right",
                    padx=(0, 15)
                )
            else:
                # Place at top right of the card container inline with text (for views with no buttons)
                view.fetch_time_lbl.place(relx=0.98, rely=0.0, anchor="ne", y=20)

        def display_sub_section_time(sub_sec, header_frame, elapsed):
            # Destroy old label if exists
            if hasattr(view, "sub_section_timer_labels") and sub_sec in view.sub_section_timer_labels:
                lbl = view.sub_section_timer_labels[sub_sec]
                if lbl:
                    try:
                        lbl.destroy()
                    except Exception:
                        pass
                view.sub_section_timer_labels[sub_sec] = None
            
            # Create a new floating label, parented to the view to avoid clipping
            lbl = ctk.CTkLabel(
                view,
                text=f"⏱ {elapsed:.2f}s",
                font=ctk.CTkFont(family="Segoe UI", size=11, weight="bold"),
                text_color=COLOR_PRIMARY
            )
            
            # Pack safely to the right side of the header frame (auto-avoids export buttons)
            lbl.pack(
                in_=header_frame,
                side="right",
                padx=(0, 15)
            )
                
            view.sub_section_timer_labels[sub_sec] = lbl

        # Determine which rendering method is present
        has_render = hasattr(view, "_render_success") and hasattr(view, "_render_error")
        has_handle = hasattr(view, "_handle_result")
        is_security_gov = hasattr(view, "_handle_labels_result")
        
        if is_security_gov:
            view.sub_section_start_times = {}
            view.sub_section_timer_labels = {}
            
            orig_labels_handle = view._handle_labels_result
            orig_retention_handle = view._handle_retention_result
            orig_dlp_handle = view._handle_dlp_result
            orig_sit_handle = view._handle_sit_result
            orig_auth_handle = view._handle_auth_result
            orig_sso_handle = view._handle_sso_result
            
            def new_labels_handle(*args, **kwargs):
                if view.is_cancelled:
                    return
                elapsed = time.time() - view.sub_section_start_times.get("labels", time.time())
                display_sub_section_time("labels", view.labels_header_frame, elapsed)
                orig_labels_handle(*args, **kwargs)
                
            def new_retention_handle(*args, **kwargs):
                if view.is_cancelled:
                    return
                elapsed = time.time() - view.sub_section_start_times.get("retention", time.time())
                display_sub_section_time("retention", view.retention_header_frame, elapsed)
                orig_retention_handle(*args, **kwargs)
                
            def new_dlp_handle(*args, **kwargs):
                if view.is_cancelled:
                    return
                elapsed = time.time() - view.sub_section_start_times.get("dlp", time.time())
                display_sub_section_time("dlp", view.dlp_header_frame, elapsed)
                orig_dlp_handle(*args, **kwargs)
                
            def new_sit_handle(*args, **kwargs):
                if view.is_cancelled:
                    return
                elapsed = time.time() - view.sub_section_start_times.get("sit", time.time())
                display_sub_section_time("sit", view.sit_header_frame, elapsed)
                orig_sit_handle(*args, **kwargs)
                
            def new_auth_handle(*args, **kwargs):
                if view.is_cancelled:
                    return
                elapsed = time.time() - view.sub_section_start_times.get("auth", time.time())
                display_sub_section_time("auth", view.auth_header_frame, elapsed)
                orig_auth_handle(*args, **kwargs)
                
            def new_sso_handle(*args, **kwargs):
                if view.is_cancelled:
                    return
                elapsed = time.time() - view.sub_section_start_times.get("sso", time.time())
                display_sub_section_time("sso", view.sso_header_frame, elapsed)
                orig_sso_handle(*args, **kwargs)
                
            view._handle_labels_result = new_labels_handle
            view._handle_retention_result = new_retention_handle
            view._handle_dlp_result = new_dlp_handle
            view._handle_sit_result = new_sit_handle
            view._handle_auth_result = new_auth_handle
            view._handle_sso_result = new_sso_handle
            
        if has_render:
            orig_success = view._render_success
            orig_error = view._render_error
            
            def new_success(*args, **kwargs):
                if view.is_cancelled:
                    async_logger.info(f"Ignored _render_success for {view.__class__.__name__} because it was cancelled.")
                    return
                elapsed = time.time() - getattr(view, "fetch_start_time", time.time())
                display_fetch_time(elapsed)
                orig_success(*args, **kwargs)
                
            def new_error(*args, **kwargs):
                if view.is_cancelled:
                    async_logger.info(f"Ignored _render_error for {view.__class__.__name__} because it was cancelled.")
                    return
                elapsed = time.time() - getattr(view, "fetch_start_time", time.time())
                display_fetch_time(elapsed)
                orig_error(*args, **kwargs)
                
            view._render_success = new_success
            view._render_error = new_error
            
        elif has_handle:
            orig_handle = view._handle_result
            
            def new_handle(*args, **kwargs):
                if view.is_cancelled:
                    async_logger.info(f"Ignored _handle_result for {view.__class__.__name__} because it was cancelled.")
                    return
                elapsed = time.time() - getattr(view, "fetch_start_time", time.time())
                display_fetch_time(elapsed)
                orig_handle(*args, **kwargs)
                
            view._handle_result = new_handle
            
        def new_trigger(*args, **kwargs):
            if hasattr(view, "fetch_time_lbl") and view.fetch_time_lbl:
                try:
                    view.fetch_time_lbl.destroy()
                except Exception:
                    pass
                view.fetch_time_lbl = None
                
            if hasattr(view, "sub_section_timer_labels"):
                for lbl in view.sub_section_timer_labels.values():
                    if lbl:
                        try:
                            lbl.destroy()
                        except Exception:
                            pass
                view.sub_section_timer_labels.clear()
            
            if is_security_gov:
                view.sub_section_start_times.clear()
                
            # Only set fetch_start_time here if there is no semaphore (otherwise WrappedSemaphore handles it)
            if not getattr(view, "semaphore", None):
                view.fetch_start_time = time.time()
                
            view.current_request_id += 1
            view.is_cancelled = False
            
            # Temporarily tag spawned threads with the current request ID and sub-section
            orig_thread_init = threading.Thread.__init__
            req_id = view.current_request_id
            
            def new_thread_init(thread_self, *t_args, **t_kwargs):
                orig_thread_init(thread_self, *t_args, **t_kwargs)
                thread_self.request_id = req_id
                
                # Tag with sub-section name based on target method name
                target = t_kwargs.get("target") or (t_args[0] if t_args else None)
                if target:
                    target_name = getattr(target, "__name__", "")
                    if "labels" in target_name:
                        thread_self.sub_section = "labels"
                    elif "retention" in target_name:
                        thread_self.sub_section = "retention"
                    elif "dlp" in target_name:
                        thread_self.sub_section = "dlp"
                    elif "sit" in target_name:
                        thread_self.sub_section = "sit"
                    elif "auth" in target_name:
                        thread_self.sub_section = "auth"
                
            threading.Thread.__init__ = new_thread_init
            try:
                orig_trigger(*args, **kwargs)
            finally:
                threading.Thread.__init__ = orig_thread_init
            
        def new_reset(*args, **kwargs):
            view.is_cancelled = True
            if hasattr(view, "fetch_time_lbl") and view.fetch_time_lbl:
                try:
                    view.fetch_time_lbl.destroy()
                except Exception:
                    pass
                view.fetch_time_lbl = None
                
            if hasattr(view, "sub_section_timer_labels"):
                for lbl in view.sub_section_timer_labels.values():
                    if lbl:
                        try:
                            lbl.destroy()
                        except Exception:
                            pass
                view.sub_section_timer_labels.clear()
            orig_reset(*args, **kwargs)
            
        def new_after(ms, callback, *args, **kwargs):
            # Identify the calling thread
            cur_thread = threading.current_thread()
            thread_req_id = getattr(cur_thread, "request_id", None)
            
            # If the calling thread has a request_id and it doesn't match the current one, discard it
            if thread_req_id is not None and thread_req_id != view.current_request_id:
                async_logger.warning(
                    f"Discarded after() callback for {view.__class__.__name__} from stale thread "
                    f"(thread req_id: {thread_req_id}, current req_id: {view.current_request_id})"
                )
                return None
            return orig_after(ms, callback, *args, **kwargs)
            
        def cancel_method():
            view.is_cancelled = True
            view.current_request_id += 1
            if view.status == "loading":
                view.status = "cancelled"
                if hasattr(view, "_update_ui_lists"):
                    data = getattr(view, "last_data", {}) or {}
                    view._update_ui_lists(data)
                elif hasattr(view, "_set_state_error"):
                    view._set_state_error("⚠️ Telemetry fetch cancelled by user.")
                
        view.trigger_fetch = new_trigger
        view.reset_view = new_reset
        view.after = new_after
        view.cancel = cancel_method

    def get_all_telemetry_data(self) -> dict:
        """Retrieves cached telemetry data and charts from all sub-views."""
        
        script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
        tenant = self.lic_tenant_id.get().strip()
        client_str = self.lic_client_ids.get().strip()
        client_ids = [x.strip() for x in client_str.split(",") if x.strip()]
        client_id = client_ids[0] if client_ids else ""
        reports_dir = os.path.join(script_dir, "reports", f"{tenant}_{client_id}")

        def load_csv(filename):
            path = os.path.join(reports_dir, filename)
            if not os.path.exists(path):
                return []
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    return list(csv.DictReader(f))
            except Exception:
                return []

        return {
            "tenant_id": tenant,
            "fetched_tabs": [k for k, s in self.tab_status.items() if s == "success"],
            "skus": getattr(self.subscribed_skus_view, "last_licenses_items", []),
            "directory": {
                "organization": getattr(self.directory_view, "last_organization", []),
                "domains": getattr(self.directory_view, "last_domains", []),
                "group_counts": getattr(self.directory_view, "last_group_counts", {}),
                "user_counts": getattr(self.directory_view, "last_user_counts", {})
            },
            "o365_usage": getattr(self.m365_apps_view.active_users_view, "o365_data", []),
            "o365_trend": getattr(self.m365_apps_view.active_users_trend_view, "trend_data", {}),
            "m365_apps": getattr(self.m365_apps_view.m365_apps_view, "last_data", []),
            "mailbox": getattr(self.exchange_online_view.mailbox_view, "last_data", {}),
            "calendar": getattr(self.exchange_online_view.calendar_view, "last_data", {}),
            "email_clients": getattr(self.exchange_online_view.email_clients_view, "last_client_data", {}),
            "pst_files": getattr(self.exchange_online_view.email_clients_view, "last_pst_data", {}),
            "exchange_connectors": getattr(self.exchange_online_view.connectors_view, "last_connectors_data", []),
            "mail_security": getattr(self.exchange_online_view.mail_security_view, "last_data", {}),
            "sharepoint": getattr(self.files_view.sharepoint_view, "last_data", {}),
            "onedrive": getattr(self.files_view.onedrive_view, "last_data", {}),
            "devices_apps": getattr(self.devices_apps_view, "last_data", {}),
            "intune": getattr(self.intune_policies_view, "last_data", {}),
            "security_labels": load_csv("sensitivity_labels.csv"),
            "retention_policies": load_csv("retention_policies.csv"),
            "dlp_policies": load_csv("dlp_policies.csv"),
            "sensitive_info_types": load_csv("sensitive_info_types.csv"),
            "service_principals_sso": load_csv("service_principals_sso.csv"),
            "conditional_access": load_csv("auth_policies.csv"),
            "power_automate": getattr(self.power_automate_view, "last_results", {})
        }



    def _monitor_memory_loop(self):
        """Periodically measures current resident set size (RSS) RAM usage of the python process and logs it."""
        
        try:
            process = psutil.Process(os.getpid())
        except Exception as err:
            async_logger.warning(f"Could not initialize psutil Process: {err}")
            process = None

        while getattr(self, "mem_monitor_active", False):
            mem_mb = 0.0
            if process:
                try:
                    mem_bytes = process.memory_info().rss
                    mem_mb = float(mem_bytes) / (1024.0 * 1024.0)
                except Exception as get_err:
                    async_logger.warning(f"Error reading memory info via psutil: {get_err}")
            
            async_logger.info(f"💾 Application Current Memory Usage (RSS): {mem_mb:.2f} MB")
            time.sleep(30)


import tkinter as tk

class ToolTip:
    """Standard lightweight Tkinter tooltip overlay helper class."""
    
    def __init__(self, widget, text):
        self.widget = widget
        self.text = text
        self.tip_label = None
        self.id = None
        self.widget.bind("<Enter>", self.enter)
        self.widget.bind("<Leave>", self.leave)

    def enter(self, event=None):
        self.schedule()

    def leave(self, event=None):
        self.unschedule()
        self.hidetip()

    def schedule(self):
        self.unschedule()
        self.id = self.widget.after(400, self.showtip)

    def unschedule(self):
        id_ = self.id
        self.id = None
        if id_:
            self.widget.after_cancel(id_)

    def showtip(self, event=None):
        if self.tip_label or not self.text:
            return
            
        root = self.widget.winfo_toplevel()
        
        # Calculate screen coordinates relative to the main application window
        rx = self.widget.winfo_rootx() - root.winfo_rootx()
        ry = self.widget.winfo_rooty() - root.winfo_rooty()
        
        x = rx + (self.widget.winfo_width() / 2) - 55
        y = ry + self.widget.winfo_height() + 4
        
        self.tip_label = ctk.CTkLabel(
            root,
            text=self.text,
            fg_color="#1E293B",
            text_color="white",
            corner_radius=4,
            font=ctk.CTkFont(family="Segoe UI", size=10, weight="normal"),
            padx=6,
            pady=3
        )
        self.tip_label.place(x=x, y=y)
        self.tip_label.lift()

    def hidetip(self):
        lbl = self.tip_label
        self.tip_label = None
        if lbl:
            try:
                lbl.place_forget()
                lbl.destroy()
            except Exception:
                pass
