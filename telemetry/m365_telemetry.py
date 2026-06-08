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
import queue
import logging
import threading
from typing import Any, Dict, List, Optional
import customtkinter as ctk
from tkinter import messagebox
from logging.handlers import QueueHandler, QueueListener

# Import modular view frames from their consolidated code/view modules
from telemetry.subscribed_skus import SubscribedSKUsFrame
from telemetry.active_users_usage import ActiveUsersUsageFrame, ActiveUsersTrendFrame, M365AppUsageFrame
from telemetry.power_automate import PowerAutomateUsageFrame

# Import existing modular views
from telemetry.files_telemetry import FilesTelemetryFrame
from telemetry.exchange_online import ExchangeOnlineFrame
from telemetry.data_security_governance import DataSecurityGovernanceFrame

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
        self.telemetry_semaphore = threading.Semaphore(3)

        self.build_ui()

        # Batching orchestration for sequential loading of sections in groups of 3
        self.batches = [
            [self.subscribed_skus_view, self.active_users_view, self.active_users_trend_view],
            [self.m365_apps_view, self.exchange_online_view, self.files_view],
            [self.security_gov_view, self.power_automate_view]
        ]
        self.current_batch_index = 0


        # Bind mouse wheel globally to scroll this tab when hovered
        self.bind_all("<MouseWheel>", self._handle_global_mousewheel, add="+")
        self.bind_all("<Button-4>", self._handle_global_mousewheel, add="+")
        self.bind_all("<Button-5>", self._handle_global_mousewheel, add="+")

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
        actions_frame.pack(fill="x", pady=(20, 10))

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

        # 2. O365 Active Users Section
        self.active_users_view = ActiveUsersUsageFrame(
            master=self,
            log_callback=self.log_msg,
            credentials_callback=self._get_credentials,
            status_change_callback=self._check_all_done,
            concurrency_semaphore=self.telemetry_semaphore
        )

        # 3. O365 Usage Trend Graph Section
        self.active_users_trend_view = ActiveUsersTrendFrame(
            master=self,
            log_callback=self.log_msg,
            credentials_callback=self._get_credentials,
            status_change_callback=self._check_all_done,
            concurrency_semaphore=self.telemetry_semaphore
        )

        # 4. M365 Apps Usage Section
        self.m365_apps_view = M365AppUsageFrame(
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

        # 6. Data Security & Governance Section
        self.security_gov_view = DataSecurityGovernanceFrame(
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

    def _hide_all_grids(self):
        views = [
            self.subscribed_skus_view,
            self.active_users_view,
            self.active_users_trend_view,
            self.m365_apps_view,
            self.exchange_online_view,
            self.files_view,
            self.security_gov_view,
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
        if not hasattr(self, "batches"):
            return

        current_views = self.batches[self.current_batch_index]
        batch_states = [view.status for view in current_views]

        if "loading" in batch_states:
            return

        # Current batch completed. Check if there are more batches to run
        if self.current_batch_index < len(self.batches) - 1:
            self.current_batch_index += 1
            self.trigger_current_batch()
            return

        # All batches have finished. Make sure no individual retries are still loading
        all_views = [view for batch in self.batches for view in batch]
        global_states = [v.status for v in all_views]
        if "loading" in global_states:
            return

        # Re-enable the submit button
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
        """Master full parallel fetch in sequential batches of 3."""
        async_logger.info("Master Submit triggered. Restarting all fetches in sequential batches of 3.")

        tenant, clients, secrets = self._get_credentials()
        if not tenant:
            async_logger.warning("Authentication aborted: Missing credential parameters.")
            messagebox.showerror("Credential Error", "Please provide complete Tenant ID, Client ID, and Client Secret strings.", parent=self)
            return

        self.btn_lic_submit.configure(state="disabled", text="Submitting...")
        self.lbl_lic_status.configure(text="Querying Microsoft Graph APIs and Reports in sequential batches of 3...", text_color=COLOR_TEXT_SUB)

        # Reset all views first
        self._hide_all_grids()

        self.current_batch_index = 0
        self.trigger_current_batch()

    def get_all_telemetry_data(self) -> dict:
        """Retrieves cached telemetry data and charts from all sub-views."""
        return {
            "tenant_id": self.lic_tenant_id.get().strip(),
            "skus": getattr(self.subscribed_skus_view, "last_licenses_items", []),
            "o365_usage": getattr(self.active_users_view, "o365_data", []),
            "o365_trend": getattr(self.active_users_trend_view, "trend_data", {}),
            "m365_apps": getattr(self.m365_apps_view, "m365_data", []),
            "mailbox": getattr(self.exchange_online_view.mailbox_view, "last_data", {}),
            "calendar": getattr(self.exchange_online_view.calendar_view, "last_data", {}),
            "sharepoint": getattr(self.files_view.sharepoint_view, "last_data", {}),
            "onedrive": getattr(self.files_view.onedrive_view, "last_data", {}),
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
