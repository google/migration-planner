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

    _queue_listener.stop()
    _file_handler.close()

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

class M365TelemetryTab(ctk.CTkScrollableFrame):
    """Encapsulates the UI coordinator for the Microsoft 365 Telemetry & Audit dashboard tab."""

    def __init__(self, master, log_callback, retries_var, backoff_var, **kwargs):
        super().__init__(
            master,
            fg_color="transparent",
            scrollbar_button_color="white",
            scrollbar_button_hover_color=COLOR_SECONDARY_HOVER,
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
        self.telemetry_semaphore = threading.Semaphore(1)
        self.is_fetching = False

        self.build_ui()

        self.batches = [
            [self.subscribed_skus_view],
            [self.directory_view],
            [self.m365_apps_view],
            [self.exchange_online_view],
            [self.files_view],
            [self.devices_apps_view, self.intune_policies_view],
            [self.security_gov_view],
            [self.power_automate_view]
        ]
        self.current_batch_index = 0

        # Bind mouse wheel globally to scroll this tab when hovered
        self.bind_all("<MouseWheel>", self._handle_global_mousewheel, add="+")
        self.bind_all("<Button-4>", self._handle_global_mousewheel, add="+")
        self.bind_all("<Button-5>", self._handle_global_mousewheel, add="+")

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

    def build_ui(self):
        async_logger.info("Building graphical UI elements for M365 Telemetry Tab.")

        ctk.CTkLabel(self, text="Connect your Microsoft Azure account to authenticate and audit tenant licensing bundle inventories and usage.", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB).pack(anchor="w", pady=(0, 15))

        self.inputs_frame = ctk.CTkFrame(self, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)
        self.inputs_frame.pack(fill="x", pady=5)

        inner_pad = ctk.CTkFrame(self.inputs_frame, fg_color="transparent")
        inner_pad.pack(fill="x", padx=15, pady=15)

        self._create_entry(inner_pad, "Tenant ID", self.lic_tenant_id)
        self._create_entry(inner_pad, "Client ID", self.lic_client_ids)
        self._create_entry(inner_pad, "Client Secret", self.lic_client_secrets, show="*")

        actions_frame = ctk.CTkFrame(self, fg_color="transparent")
        actions_frame.pack(fill="x", pady=(20, 5))

        self.btn_lic_submit = ctk.CTkButton(
            actions_frame, text="Submit", width=160, height=40, corner_radius=20,
            font=FONT_BODY_BOLD, fg_color=COLOR_PRIMARY, hover_color=COLOR_PRIMARY_HOVER,
            command=self.authenticate_licenses_tab,
        )
        self.btn_lic_submit.pack(side="left")

        self.lbl_lic_status = ctk.CTkLabel(actions_frame, text="", font=FONT_BODY_MEDIUM)
        self.lbl_lic_status.pack(side="left", padx=20)

        # ----------------------------------------------------
        # MODULAR UI SECTIONS
        # ----------------------------------------------------

        # 1. Subscribed SKUs Section
        self.subscribed_skus_view = SubscribedSKUsFrame(
            master=self,
            log_callback=self.log_msg,
            credentials_callback=self._get_credentials,
            status_change_callback=self._check_all_done,
            retries_var=self.retries,
            backoff_var=self.backoff,
            concurrency_semaphore=self.telemetry_semaphore
        )

        # 1b. Directory Groups Section
        self.directory_view = DirectoryFrame(
            master=self,
            log_callback=self.log_msg,
            credentials_callback=self._get_credentials,
            status_change_callback=self._check_all_done,
            retries_var=self.retries,
            backoff_var=self.backoff,
            concurrency_semaphore=self.telemetry_semaphore
        )

        # 2. M365 Apps Section (Uber Container)
        self.m365_apps_view = M365AppsTelemetryFrame(
            master=self,
            log_callback=self.log_msg,
            credentials_callback=self._get_credentials,
            status_change_callback=self._check_all_done,
            concurrency_semaphore=self.telemetry_semaphore
        )

        # 5. Exchange Online Usage Section
        self.exchange_online_view = ExchangeOnlineFrame(
            master=self,
            log_callback=self.log_msg,
            credentials_callback=self._get_credentials,
            status_change_callback=self._check_all_done,
            concurrency_semaphore=self.telemetry_semaphore
        )



        # 5c. Files (SharePoint & OneDrive) Section
        self.files_view = FilesTelemetryFrame(
            master=self,
            log_callback=self.log_msg,
            credentials_callback=self._get_credentials,
            status_change_callback=self._check_all_done,
            concurrency_semaphore=self.telemetry_semaphore
        )

        # 5d. Devices & Apps Section
        self.devices_apps_view = DevicesAppsTelemetryFrame(
            master=self,
            log_callback=self.log_msg,
            credentials_callback=self._get_credentials,
            status_change_callback=self._check_all_done,
            concurrency_semaphore=self.telemetry_semaphore
        )

        # 6. Data Security & Governance Section
        self.security_gov_view = DataSecurityGovernanceFrame(
            master=self,
            log_callback=self.log_msg,
            credentials_callback=self._get_credentials,
            status_change_callback=self._check_all_done,
            concurrency_semaphore=self.telemetry_semaphore
        )

        # 6.5. Intune Policies Section
        self.intune_policies_view = IntunePoliciesFrame(
            master=self,
            log_callback=self.log_msg,
            credentials_callback=self._get_credentials,
            status_change_callback=self._check_all_done,
            concurrency_semaphore=self.telemetry_semaphore
        )

        # 7. Power Automate Section
        self.power_automate_view = PowerAutomateUsageFrame(
            master=self,
            log_callback=self.log_msg,
            credentials_callback=self._get_credentials,
            status_change_callback=self._check_all_done,
            concurrency_semaphore=self.telemetry_semaphore
        )

        self._hide_all_grids()

        # Wrap all leaf views to support cancellation and prevent race conditions
        for leaf in self._get_all_leaf_views():
            self._wrap_view_for_cancellation(leaf)

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
        async_logger.info("Resetting M365TelemetryTab coordinator and hiding all sub-grids.")
        self.lic_tenant_id.set("")
        self.lic_client_ids.set("")
        self.lic_client_secrets.set("")
        self.btn_lic_submit.configure(state="normal", text="Submit")
        self.lbl_lic_status.configure(text="")
        self.current_batch_index = 0
        self._hide_all_grids()

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
        """Checks if all sections of the current batch have resolved, then triggers next batch or finishes."""
        if not getattr(self, "is_fetching", False):
            # If fetching was cancelled, ignore any further background thread completions
            return

        if not hasattr(self, "batches"):
            return

        current_views = self.batches[self.current_batch_index]
        batch_states = [view.status for view in current_views if view not in [self.devices_apps_view, self.intune_policies_view]]

        if "loading" in batch_states:
            return

        # Current batch completed. Check if there are more batches to run
        if self.current_batch_index < len(self.batches) - 1:
            self.current_batch_index += 1
            self.trigger_current_batch()
            return

        # All batches have finished. Make sure no individual retries are still loading
        all_views = [view for batch in self.batches for view in batch]
        global_states = [v.status for v in all_views if v not in [self.devices_apps_view, self.intune_policies_view]]
        if "loading" in global_states:
            return

        # Re-enable the submit button
        self.is_fetching = False
        self.btn_lic_submit.configure(state="normal", text="Submit")

        success = all(s == "success" for s in global_states)
        if success:
            self.lbl_lic_status.configure(text="✔ All Inventory and Usage Reports Pulled Successfully!", text_color=COLOR_SUCCESS)
        else:
            self.lbl_lic_status.configure(text="⚠ Some reports failed. Please retry individually.", text_color=COLOR_ERROR)

        if hasattr(self, "on_all_done_callback") and self.on_all_done_callback:
            self.on_all_done_callback(success)

    def trigger_current_batch(self):
        """Triggers the fetches for the current batch of sections."""
        tenant, clients, secrets = self._get_credentials()
        if not tenant:
            return

        current_views = self.batches[self.current_batch_index]
        async_logger.info(f"Triggering batch {self.current_batch_index + 1} with {len(current_views)} views.")

        for view in current_views:
            if isinstance(view, SubscribedSKUsFrame):
                view.trigger_fetch(tenant, clients, secrets)
            else:
                view.trigger_fetch(tenant, clients[0], secrets[0])

    def authenticate_licenses_tab(self):
        """Master full sequential fetch of sections, or cancel if already fetching."""
        if getattr(self, "is_fetching", False):
            self.cancel_fetching()
            return

        async_logger.info("Master Submit triggered. Restarting all fetches sequentially.")

        tenant, clients, secrets = self._get_credentials()
        if not tenant:
            async_logger.warning("Authentication aborted: Missing credential parameters.")
            messagebox.showerror("Credential Error", "Please provide complete Tenant ID, Client ID, and Client Secret strings.", parent=self)
            return

        self.is_fetching = True
        self.btn_lic_submit.configure(state="normal", text="Cancel")
        self.lbl_lic_status.configure(text="Querying Microsoft Graph APIs and Reports sequentially...", text_color=COLOR_TEXT_SUB)

        # Reset all views first
        self._hide_all_grids()

        self.current_batch_index = 0
        self.trigger_current_batch()

    def cancel_fetching(self):
        """Cancels the current fetching process and stops all subsequent batches."""
        async_logger.info("Cancellation triggered by user.")
        self.is_fetching = False
        
        self.btn_lic_submit.configure(state="normal", text="Submit")
        self.lbl_lic_status.configure(text="✖ Query cancelled by user.", text_color=COLOR_ERROR)
        
        # Propagate cancel to all top-level views
        for batch in self.batches:
            for view in batch:
                if hasattr(view, "cancel"):
                    view.cancel()

        if hasattr(self, "on_all_done_callback") and self.on_all_done_callback:
            self.on_all_done_callback(False)

    def _get_all_leaf_views(self):
        """Returns a list of all active leaf/base telemetry views across all cards."""
        return [
            self.subscribed_skus_view,
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
            self.devices_apps_view,
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
                def acquire(self, *args, **kwargs):
                    res = self._sem.acquire(*args, **kwargs)
                    cur_thread = threading.current_thread()
                    sub_sec = getattr(cur_thread, "sub_section", None)
                    if sub_sec:
                        view.sub_section_start_times[sub_sec] = time.time()
                    else:
                        view.fetch_start_time = time.time()
                    return res
                def release(self, *args, **kwargs):
                    return self._sem.release(*args, **kwargs)
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
            header_target = getattr(view, "lic_header", getattr(view, "pa_header", getattr(view, "header", None)))
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
            orig_auth_handle = view._handle_auth_result
            
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
                
            def new_auth_handle(*args, **kwargs):
                if view.is_cancelled:
                    return
                elapsed = time.time() - view.sub_section_start_times.get("auth", time.time())
                display_sub_section_time("auth", view.auth_header_frame, elapsed)
                orig_auth_handle(*args, **kwargs)
                
            view._handle_labels_result = new_labels_handle
            view._handle_retention_result = new_retention_handle
            view._handle_auth_result = new_auth_handle
            
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
        return {
            "tenant_id": self.lic_tenant_id.get().strip(),
            "skus": getattr(self.subscribed_skus_view, "last_licenses_items", []),
            "directory": {
                "domains": getattr(self.directory_view, "last_domains", []),
                "group_counts": getattr(self.directory_view, "last_group_counts", {}),
                "user_counts": getattr(self.directory_view, "last_user_counts", {})
            },
            "o365_usage": getattr(self.m365_apps_view.active_users_view, "o365_data", []),
            "o365_trend": getattr(self.m365_apps_view.active_users_trend_view, "trend_data", {}),
            "m365_apps": getattr(self.m365_apps_view.m365_apps_view, "m365_data", []),
            "mailbox": getattr(self.exchange_online_view.mailbox_view, "last_data", {}),
            "calendar": getattr(self.exchange_online_view.calendar_view, "last_data", {}),
            "sharepoint": getattr(self.files_view.sharepoint_view, "last_data", {}),
            "onedrive": getattr(self.files_view.onedrive_view, "last_data", {}),
            "devices_apps": getattr(self.devices_apps_view, "last_data", {}),
            "security_labels": getattr(self.security_gov_view, "last_labels_data", []),
            "retention_policies": getattr(self.security_gov_view, "last_policies_data", []),
            "power_automate": getattr(self.power_automate_view, "last_results", {})
        }

    def is_descendant(self, parent, widget) -> bool:
        """Recursively checks if a widget (or its Tkinter path name) is a descendant of parent."""
        if not widget:
            return False
        if isinstance(widget, str):
            try:
                widget = self.nametowidget(widget)
            except Exception:
                return False
        if widget == parent:
            return True
        if hasattr(widget, "master") and widget.master is not None:
            return self.is_descendant(parent, widget.master)
        return False

    def _handle_global_mousewheel(self, event):
        """Redirects mousewheel scrolling to the tab's parent canvas if hovered."""
        try:
            widget = self.winfo_containing(event.x_root, event.y_root)
        except Exception:
            return

        if self.is_descendant(self, widget):
            if event.num == 4:  # Linux scroll up
                self._parent_canvas.yview("scroll", -1, "units")
            elif event.num == 5:  # Linux scroll down
                self._parent_canvas.yview("scroll", 1, "units")
            else:  # Windows / macOS
                if sys.platform == "darwin":
                    # macOS trackpad/mouse delta
                    self._parent_canvas.yview("scroll", -event.delta, "units")
                else:
                    # Windows delta (usually multiple of 120)
                    self._parent_canvas.yview("scroll", -int(event.delta / 120), "units")

    def _monitor_memory_loop(self):
        """Periodically measures current resident set size (RSS) RAM usage of the python process and logs it."""
        import time
        import os
        import psutil
        
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
