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

"""Consolidated Exchange Online Telemetry Orchestrator Container."""

import logging
import customtkinter as ctk

from telemetry.exchange.mailbox import MailboxUsageFrame
from telemetry.exchange.calendar import CalendarTelemetryFrame
from telemetry.exchange.integrated_apps import ExchangeAppsFrame
from telemetry.exchange.mail_security import MailSecurityFrame
from telemetry.exchange.transport_rules import TransportRulesFrame
from telemetry.exchange.connectors import ExchangeConnectorsFrame
from telemetry.exchange.email_clients import EmailClientSupportFrame
from telemetry.exchange.pst_files import PstFilesFrame
from telemetry.styles import *

usage_logger = logging.getLogger("M365TelemetryAsyncLogger.ExchangeUI")

class ExchangeOnlineFrame(ctk.CTkFrame):
    """Uber section container hosting Mailbox, Calendar, Apps, Mail Security, Rules, Connectors, Email Clients, and PST files UI frames vertically stacked."""

    def __init__(self, master, log_callback, credentials_callback, status_change_callback, **kwargs):
        self.semaphore = kwargs.pop("concurrency_semaphore", None)
        super().__init__(master, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=12, **kwargs)
        self.log_msg = log_callback
        self.get_credentials = credentials_callback
        self.on_status_change = status_change_callback
        self.status = None

        self.build_ui()

    def build_ui(self):
        """Instantiates and stacks the 8 decoupled Exchange telemetry sub-frames."""
        self.pack(fill="x", expand=True, pady=10)

        self.inner_pad = ctk.CTkFrame(self, fg_color="transparent")
        self.inner_pad.pack(fill="both", expand=True, padx=20, pady=20)

        # Main Header Title
        ctk.CTkLabel(
            self.inner_pad,
            text="Email, Calendar and Contacts",
            font=FONT_HEADER_SMALL,
            text_color=COLOR_TEXT_MAIN
        ).pack(anchor="w", pady=(0, 5))

        # 1. Mailbox Usage Sub-frame
        self.mailbox_view = MailboxUsageFrame(
            master=self.inner_pad,
            log_callback=self.log_msg,
            credentials_callback=self.get_credentials,
            status_change_callback=self._subframe_status_changed,
            concurrency_semaphore=self.semaphore
        )
        self.mailbox_view.configure(fg_color="transparent", border_width=0)
        self.mailbox_view.pack(fill="x", expand=True, pady=(0, 5))

        # Divider 1
        self.divider1 = ctk.CTkFrame(self.inner_pad, height=1, fg_color=COLOR_OUTLINE_LIGHT)
        self.divider1.pack(fill="x", pady=10)

        # 2. Calendar Telemetry Sub-frame
        self.calendar_view = CalendarTelemetryFrame(
            master=self.inner_pad,
            log_callback=self.log_msg,
            credentials_callback=self.get_credentials,
            status_change_callback=self._subframe_status_changed,
            concurrency_semaphore=self.semaphore
        )
        self.calendar_view.configure(fg_color="transparent", border_width=0)
        self.calendar_view.pack(fill="x", expand=True, pady=(0, 5))

        # Divider 2
        self.divider2 = ctk.CTkFrame(self.inner_pad, height=1, fg_color=COLOR_OUTLINE_LIGHT)
        self.divider2.pack(fill="x", pady=10)

        # 3. Organization-wide Apps Sub-frame
        self.apps_view = ExchangeAppsFrame(
            master=self.inner_pad,
            log_callback=self.log_msg,
            credentials_callback=self.get_credentials,
            status_change_callback=self._subframe_status_changed,
            concurrency_semaphore=self.semaphore
        )
        self.apps_view.configure(fg_color="transparent", border_width=0)
        self.apps_view.pack(fill="x", expand=True, pady=(0, 5))

        # Divider 3
        self.divider3 = ctk.CTkFrame(self.inner_pad, height=1, fg_color=COLOR_OUTLINE_LIGHT)
        self.divider3.pack(fill="x", pady=10)

        # 4. Mail Security Sub-frame
        self.mail_security_view = MailSecurityFrame(
            master=self.inner_pad,
            log_callback=self.log_msg,
            credentials_callback=self.get_credentials,
            status_change_callback=self._subframe_status_changed,
            concurrency_semaphore=self.semaphore
        )
        self.mail_security_view.configure(fg_color="transparent", border_width=0)
        self.mail_security_view.pack(fill="x", expand=True, pady=(0, 5))

        # Divider 4
        self.divider4 = ctk.CTkFrame(self.inner_pad, height=1, fg_color=COLOR_OUTLINE_LIGHT)
        self.divider4.pack(fill="x", pady=10)

        # 5. Transport Rules Sub-frame
        self.transport_rules_view = TransportRulesFrame(
            master=self.inner_pad,
            log_callback=self.log_msg,
            credentials_callback=self.get_credentials,
            status_change_callback=self._subframe_status_changed,
            concurrency_semaphore=self.semaphore
        )
        self.transport_rules_view.configure(fg_color="transparent", border_width=0)
        self.transport_rules_view.pack(fill="x", expand=True, pady=(0, 5))

        # Divider 5
        self.divider5 = ctk.CTkFrame(self.inner_pad, height=1, fg_color=COLOR_OUTLINE_LIGHT)
        self.divider5.pack(fill="x", pady=10)

        # 6. Exchange Connectors Sub-frame
        self.connectors_view = ExchangeConnectorsFrame(
            master=self.inner_pad,
            log_callback=self.log_msg,
            credentials_callback=self.get_credentials,
            status_change_callback=self._subframe_status_changed,
            concurrency_semaphore=self.semaphore
        )
        self.connectors_view.configure(fg_color="transparent", border_width=0)
        self.connectors_view.pack(fill="x", expand=True, pady=(0, 5))

        # Divider 6
        self.divider6 = ctk.CTkFrame(self.inner_pad, height=1, fg_color=COLOR_OUTLINE_LIGHT)
        self.divider6.pack(fill="x", pady=10)

        # 7. Email Client Support Sub-frame
        self.email_clients_view = EmailClientSupportFrame(
            master=self.inner_pad,
            log_callback=self.log_msg,
            credentials_callback=self.get_credentials,
            status_change_callback=self._subframe_status_changed,
            concurrency_semaphore=self.semaphore
        )
        self.email_clients_view.configure(fg_color="transparent", border_width=0)
        self.email_clients_view.pack(fill="x", expand=True, pady=(0, 5))

        # Divider 7
        self.divider7 = ctk.CTkFrame(self.inner_pad, height=1, fg_color=COLOR_OUTLINE_LIGHT)
        self.divider7.pack(fill="x", pady=10)

        # 8. PST Files Discovery Sub-frame
        self.pst_files_view = PstFilesFrame(
            master=self.inner_pad,
            log_callback=self.log_msg,
            credentials_callback=self.get_credentials,
            status_change_callback=self._subframe_status_changed,
            concurrency_semaphore=self.semaphore
        )
        self.pst_files_view.configure(fg_color="transparent", border_width=0)
        self.pst_files_view.pack(fill="x", expand=True, pady=(0, 5))

        self.reset_view()

    def _subframe_status_changed(self):
        statuses = [
            self.mailbox_view.status,
            self.calendar_view.status,
            self.apps_view.status,
            self.mail_security_view.status,
            self.transport_rules_view.status,
            self.connectors_view.status,
            self.email_clients_view.status,
            self.pst_files_view.status
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
        """Resets all sub-views and hides container."""
        self.pack_forget()
        self.status = None
        self.mailbox_view.reset_view()
        self.calendar_view.reset_view()
        self.apps_view.reset_view()
        self.mail_security_view.reset_view()
        self.transport_rules_view.reset_view()
        self.connectors_view.reset_view()
        self.email_clients_view.reset_view()
        self.pst_files_view.reset_view()

    def trigger_fetch(self, tenant, client_id, client_secret):
        """Displays container and delegates fetches to all sub-views."""
        self.status = "loading"
        self.on_status_change()

        self.pack(fill="x", expand=True, pady=10)

        self.mailbox_view.trigger_fetch(tenant, client_id, client_secret)
        self.calendar_view.trigger_fetch(tenant, client_id, client_secret)
        self.apps_view.trigger_fetch(tenant, client_id, client_secret)
        self.mail_security_view.trigger_fetch(tenant, client_id, client_secret)
        self.transport_rules_view.trigger_fetch(tenant, client_id, client_secret)
        self.connectors_view.trigger_fetch(tenant, client_id, client_secret)
        self.email_clients_view.trigger_fetch(tenant, client_id, client_secret)
        self.pst_files_view.trigger_fetch(tenant, client_id, client_secret)

    def cancel(self):
        """Cancels all child views in this container."""
        self.mailbox_view.cancel()
        self.calendar_view.cancel()
        self.apps_view.cancel()
        self.mail_security_view.cancel()
        self.transport_rules_view.cancel()
        self.connectors_view.cancel()
        self.email_clients_view.cancel()
        self.pst_files_view.cancel()
        self.status = None
