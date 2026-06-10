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

"""Uber container section wrapping Active Users Usage, Active Users Trend, and M365 App Usage frames under a single M365 Apps card."""

import customtkinter as ctk
from telemetry.styles import *
from telemetry.active_users_usage import ActiveUsersUsageFrame, ActiveUsersTrendFrame, M365AppUsageFrame

class M365AppsTelemetryFrame(ctk.CTkFrame):
    """Uber section container hosting Active Users Usage, Trend, and M365 App Usage frames vertically stacked."""

    def __init__(self, master, log_callback, credentials_callback, status_change_callback, **kwargs):
        self.semaphore = kwargs.pop("concurrency_semaphore", None)
        super().__init__(master, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=12, **kwargs)
        self.log_msg = log_callback
        self.get_credentials = credentials_callback
        self.on_status_change = status_change_callback
        self.status = None

        self.build_ui()

    def build_ui(self):
        """Instantiates and stacks existing usage sub-frames."""
        self.pack(fill="x", expand=True, pady=10)

        self.inner_pad = ctk.CTkFrame(self, fg_color="transparent")
        self.inner_pad.pack(fill="both", expand=True, padx=20, pady=20)

        # Uber Title Heading
        ctk.CTkLabel(
            self.inner_pad,
            text="M365 Apps",
            font=FONT_HEADER_SMALL,
            text_color=COLOR_TEXT_MAIN
        ).pack(anchor="w", pady=(0, 5))

        # Active Users Usage Sub-frame
        self.active_users_view = ActiveUsersUsageFrame(
            master=self.inner_pad,
            log_callback=self.log_msg,
            credentials_callback=self.get_credentials,
            status_change_callback=self._check_overall_status,
            concurrency_semaphore=self.semaphore
        )
        self.active_users_view.configure(fg_color="transparent", border_width=0)
        self.active_users_view.pack(fill="x", expand=True, pady=(0, 5))

        # Divider 1
        self.divider1 = ctk.CTkFrame(self.inner_pad, height=1, fg_color=COLOR_OUTLINE_LIGHT)
        self.divider1.pack(fill="x", pady=10)

        # Active Users Trend Sub-frame
        self.active_users_trend_view = ActiveUsersTrendFrame(
            master=self.inner_pad,
            log_callback=self.log_msg,
            credentials_callback=self.get_credentials,
            status_change_callback=self._check_overall_status,
            concurrency_semaphore=self.semaphore
        )
        self.active_users_trend_view.configure(fg_color="transparent", border_width=0)
        self.active_users_trend_view.pack(fill="x", expand=True, pady=(0, 5))

        # Divider 2
        self.divider2 = ctk.CTkFrame(self.inner_pad, height=1, fg_color=COLOR_OUTLINE_LIGHT)
        self.divider2.pack(fill="x", pady=10)

        # M365 App Usage Sub-frame
        self.m365_apps_view = M365AppUsageFrame(
            master=self.inner_pad,
            log_callback=self.log_msg,
            credentials_callback=self.get_credentials,
            status_change_callback=self._check_overall_status,
            concurrency_semaphore=self.semaphore
        )
        self.m365_apps_view.configure(fg_color="transparent", border_width=0)
        self.m365_apps_view.pack(fill="x", expand=True, pady=(0, 5))

        self.reset_view()

    def reset_view(self):
        """Resets all sub-views and hides container."""
        self.pack_forget()
        self.active_users_view.reset_view()
        self.active_users_trend_view.reset_view()
        self.m365_apps_view.reset_view()
        self.divider1.pack_forget()
        self.divider2.pack_forget()

    def trigger_fetch(self, tenant, client_id, client_secret):
        """Displays container and delegates fetches to all sub-views."""
        self.status = "loading"
        self.on_status_change()

        self.pack(fill="x", expand=True, pady=10)
        self.divider1.pack(fill="x", pady=10)
        self.divider2.pack(fill="x", pady=10)

        self.active_users_view.trigger_fetch(tenant, client_id, client_secret)
        self.active_users_trend_view.trigger_fetch(tenant, client_id, client_secret)
        self.m365_apps_view.trigger_fetch(tenant, client_id, client_secret)

    def _check_overall_status(self):
        """Updates main container status based on sub-frame statuses."""
        sub_statuses = [self.active_users_view.status, self.active_users_trend_view.status, self.m365_apps_view.status]
        if "loading" in sub_statuses:
            self.status = "loading"
        elif "success" in sub_statuses:
            self.status = "success"
        else:
            self.status = "error"
        self.on_status_change()
