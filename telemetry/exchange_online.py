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

"""Uber container section wrapping Mailbox and Calendar telemetry frames under a single Exchange Online card."""

import customtkinter as ctk
from telemetry.styles import *
from telemetry.mailbox_usage import MailboxUsageFrame
from telemetry.calendar_telemetry import CalendarTelemetryFrame

class ExchangeOnlineFrame(ctk.CTkFrame):
    """Uber section container hosting Mailbox and Calendar telemetry frames vertically stacked."""

    def __init__(self, master, log_callback, credentials_callback, status_change_callback, **kwargs):
        self.semaphore = kwargs.pop("concurrency_semaphore", None)
        super().__init__(master, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=12, **kwargs)
        self.log_msg = log_callback
        self.get_credentials = credentials_callback
        self.on_status_change = status_change_callback
        self.status = None

        self.build_ui()

    def build_ui(self):
        """Instantiates and stacks existing mailbox and calendar frames."""
        self.pack(fill="x", expand=True, pady=10)

        self.inner_pad = ctk.CTkFrame(self, fg_color="transparent")
        self.inner_pad.pack(fill="both", expand=True, padx=20, pady=20)

        # Uber Title Heading
        ctk.CTkLabel(
            self.inner_pad,
            text="Exchange Online",
            font=FONT_HEADER_SMALL,
            text_color=COLOR_TEXT_MAIN
        ).pack(anchor="w", pady=(0, 5))

        # Mailbox Usage Sub-frame
        self.mailbox_view = MailboxUsageFrame(
            master=self.inner_pad,
            log_callback=self.log_msg,
            credentials_callback=self.get_credentials,
            status_change_callback=self._check_overall_status,
            concurrency_semaphore=self.semaphore
        )
        self.mailbox_view.configure(fg_color="transparent", border_width=0)
        self.mailbox_view.pack(fill="x", expand=True, pady=(0, 5))

        # Separator line
        self.divider = ctk.CTkFrame(self.inner_pad, height=1, fg_color=COLOR_OUTLINE_LIGHT)
        self.divider.pack(fill="x", pady=10)

        # Calendar Telemetry Sub-frame
        self.calendar_view = CalendarTelemetryFrame(
            master=self.inner_pad,
            log_callback=self.log_msg,
            credentials_callback=self.get_credentials,
            status_change_callback=self._check_overall_status,
            concurrency_semaphore=self.semaphore
        )
        self.calendar_view.configure(fg_color="transparent", border_width=0)
        self.calendar_view.pack(fill="x", expand=True, pady=(0, 5))

        self.reset_view()

    def reset_view(self):
        """Resets both sub-views and hides container."""
        self.pack_forget()
        self.mailbox_view.reset_view()
        self.calendar_view.reset_view()
        self.divider.pack_forget()

    def trigger_fetch(self, tenant, client_id, client_secret):
        """Displays container and delegates fetches to both sub-views."""
        self.status = "loading"
        self.on_status_change()

        self.pack(fill="x", expand=True, pady=10)
        self.divider.pack(fill="x", pady=10)

        self.mailbox_view.trigger_fetch(tenant, client_id, client_secret)
        self.calendar_view.trigger_fetch(tenant, client_id, client_secret)

    def _check_overall_status(self):
        """Updates main container status based on sub-frame statuses."""
        if self.mailbox_view.status == "loading" or self.calendar_view.status == "loading":
            self.status = "loading"
        elif self.mailbox_view.status == "success" or self.calendar_view.status == "success":
            self.status = "success"
        else:
            self.status = "error"
        self.on_status_change()
