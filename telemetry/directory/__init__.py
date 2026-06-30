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

"""Consolidated Directory Telemetry Orchestrator Container."""

import logging
import customtkinter as ctk

from telemetry.directory.organization import DirectoryOrganizationFrame
from telemetry.directory.domains import DirectoryDomainsFrame
from telemetry.directory.user_logs import DirectoryUserLogsFrame
from telemetry.directory.provisioning_logs import DirectoryProvisioningLogsFrame
from telemetry.directory.users_groups import DirectoryUsersGroupsFrame
from telemetry.styles import *

usage_logger = logging.getLogger("M365TelemetryAsyncLogger.DirectoryUI")

class DirectoryFrame(ctk.CTkFrame):
    """Self-contained container wrapping the 5 independent Directory telemetry sub-frames."""

    def __init__(self, master, log_callback, credentials_callback, status_change_callback, retries_var=None, backoff_var=None, **kwargs):
        self.semaphore = kwargs.pop("concurrency_semaphore", None)
        super().__init__(master, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=12, **kwargs)
        self.log_msg = log_callback
        self.get_credentials = credentials_callback
        self.on_status_change = status_change_callback
        self.retries = retries_var
        self.backoff = backoff_var
        self.status = None  # 'loading', 'success', 'error', None

        self.build_ui()

    def build_ui(self):
        self.pack(fill="x", expand=True, pady=10)
        
        self.inner_pad = ctk.CTkFrame(self, fg_color="transparent")
        self.inner_pad.pack(fill="both", expand=True, padx=20, pady=20)
        
        # Main Title Header
        self.header_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.header_frame.pack(fill="x", pady=(0, 15))
        ctk.CTkLabel(
            self.header_frame,
            text="Directory Summary",
            font=FONT_HEADER_SMALL,
            text_color=COLOR_TEXT_MAIN
        ).pack(side="left")

        # 1. Organization Subframe
        self.organization_frame = DirectoryOrganizationFrame(
            self.inner_pad,
            log_callback=self.log_msg,
            credentials_callback=self.get_credentials,
            status_change_callback=self._subframe_status_changed,
            semaphore=self.semaphore
        )
        self.organization_frame.pack(fill="x", pady=(0, 10))

        # Divider 1
        self.divider1 = ctk.CTkFrame(self.inner_pad, height=1, fg_color=COLOR_OUTLINE_LIGHT)
        self.divider1.pack(fill="x", pady=15)

        # 2. Domains Subframe
        self.domains_frame = DirectoryDomainsFrame(
            self.inner_pad,
            log_callback=self.log_msg,
            credentials_callback=self.get_credentials,
            status_change_callback=self._subframe_status_changed,
            semaphore=self.semaphore
        )
        self.domains_frame.pack(fill="x", pady=(0, 10))

        # Divider 2
        self.divider2 = ctk.CTkFrame(self.inner_pad, height=1, fg_color=COLOR_OUTLINE_LIGHT)
        self.divider2.pack(fill="x", pady=15)

        # 3. User Logs Subframe
        self.user_logs_frame = DirectoryUserLogsFrame(
            self.inner_pad,
            log_callback=self.log_msg,
            credentials_callback=self.get_credentials,
            status_change_callback=self._subframe_status_changed,
            semaphore=self.semaphore
        )
        self.user_logs_frame.pack(fill="x", pady=(0, 10))

        # Divider 3
        self.divider3 = ctk.CTkFrame(self.inner_pad, height=1, fg_color=COLOR_OUTLINE_LIGHT)
        self.divider3.pack(fill="x", pady=15)

        # 4. Provisioning Logs Subframe
        self.provisioning_logs_frame = DirectoryProvisioningLogsFrame(
            self.inner_pad,
            log_callback=self.log_msg,
            credentials_callback=self.get_credentials,
            status_change_callback=self._subframe_status_changed,
            semaphore=self.semaphore
        )
        self.provisioning_logs_frame.pack(fill="x", pady=(0, 10))

        # Divider 4
        self.divider4 = ctk.CTkFrame(self.inner_pad, height=1, fg_color=COLOR_OUTLINE_LIGHT)
        self.divider4.pack(fill="x", pady=15)

        # 5. Users & Groups Subframe
        self.users_groups_frame = DirectoryUsersGroupsFrame(
            self.inner_pad,
            log_callback=self.log_msg,
            credentials_callback=self.get_credentials,
            status_change_callback=self._subframe_status_changed,
            semaphore=self.semaphore
        )
        self.users_groups_frame.pack(fill="x", pady=(0, 10))

    def _subframe_status_changed(self):
        statuses = [
            self.organization_frame.status,
            self.domains_frame.status,
            self.user_logs_frame.status,
            self.provisioning_logs_frame.status,
            self.users_groups_frame.status
        ]
        if "loading" in statuses:
            self.status = "loading"
        elif "error" in statuses:
            self.status = "error"
        elif "success" in statuses:
            self.status = "success"
        else:
            self.status = None
        self.on_status_change()

    def reset_view(self):
        self.pack_forget()
        self.status = None
        self.organization_frame.reset_view()
        self.domains_frame.reset_view()
        self.user_logs_frame.reset_view()
        self.provisioning_logs_frame.reset_view()
        self.users_groups_frame.reset_view()

    def trigger_fetch(self, tenant, client_id, client_secret):
        usage_logger.info("DirectoryFrame trigger_fetch: propagating to subframes...")
        self.pack(fill="x", expand=True, pady=10)
        self.organization_frame.trigger_fetch(tenant, client_id, client_secret)
        self.domains_frame.trigger_fetch(tenant, client_id, client_secret)
        self.user_logs_frame.trigger_fetch(tenant, client_id, client_secret)
        self.provisioning_logs_frame.trigger_fetch(tenant, client_id, client_secret)
        self.users_groups_frame.trigger_fetch(tenant, client_id, client_secret)

    def cancel(self):
        usage_logger.info("DirectoryFrame cancel: propagating to subframes...")
        self.organization_frame.cancel()
        self.domains_frame.cancel()
        self.user_logs_frame.cancel()
        self.provisioning_logs_frame.cancel()
        self.users_groups_frame.cancel()

    # Properties maintained for exact backward compatibility with reporting and export logic
    @property
    def last_organization(self):
        return self.organization_frame.last_data

    @property
    def last_domains(self):
        return self.domains_frame.last_data

    @property
    def last_user_creation_logs(self):
        return self.user_logs_frame.last_data

    @property
    def last_provisioning_logs(self):
        return self.provisioning_logs_frame.last_data

    @property
    def last_group_counts(self):
        return self.users_groups_frame.last_data.get("group_counts", {})

    @property
    def last_user_counts(self):
        return self.users_groups_frame.last_data.get("user_counts", {})
