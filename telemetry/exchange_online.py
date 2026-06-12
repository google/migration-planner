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

"""Uber container section wrapping Mailbox, Calendar, Organization Apps, and Supported Email Clients/PSTs telemetry frames under a single Emails & Calendar card."""

import customtkinter as ctk
from telemetry.styles import *
from telemetry.mailbox_usage import MailboxUsageFrame
from telemetry.calendar_telemetry import CalendarTelemetryFrame
from telemetry.exchange_apps import ExchangeAppsFrame
from telemetry.email_client_support import EmailClientSupportFrame
from telemetry.exchange_connectors_ui import ExchangeConnectorsFrame
from telemetry.mail_security import MailSecurityFrame

class ExchangeOnlineFrame(ctk.CTkFrame):
    def update_loading_text(self, text_msg):
        if hasattr(self, 'loading_label') and self.loading_label.winfo_exists():
            self.loading_label.configure(text=f"⏳ {text_msg}")
    """Uber section container hosting Mailbox, Calendar, Apps, and Email Client telemetry frames vertically stacked."""

    def __init__(self, master, log_callback, credentials_callback, status_change_callback, **kwargs):
        self.semaphore = kwargs.pop("concurrency_semaphore", None)
        super().__init__(master, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=12, **kwargs)
        self.log_msg = log_callback
        self.get_credentials = credentials_callback
        self.on_status_change = status_change_callback
        self.status = None

        self.build_ui()

    def build_ui(self):
        """Instantiates and stacks existing mailbox, calendar, apps, and email client support frames."""
        self.pack(fill="x", expand=True, pady=10)

        self.inner_pad = ctk.CTkFrame(self, fg_color="transparent")
        self.inner_pad.pack(fill="both", expand=True, padx=20, pady=20)

        # Uber Title Heading
        ctk.CTkLabel(
            self.inner_pad,
            text="Email, Calendar and Contacts",
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

        # Organization-wide Apps Sub-frame
        self.apps_view = ExchangeAppsFrame(
            master=self.inner_pad,
            log_callback=self.log_msg,
            credentials_callback=self.get_credentials,
            status_change_callback=self._check_overall_status,
            concurrency_semaphore=self.semaphore
        )
        self.apps_view.configure(fg_color="transparent", border_width=0)
        self.apps_view.pack(fill="x", expand=True, pady=(0, 5))

        # Mail Security Sub-frame
        self.mail_security_view = MailSecurityFrame(
            master=self.inner_pad,
            log_callback=self.log_msg,
            credentials_callback=self.get_credentials,
            status_change_callback=self._check_overall_status,
            concurrency_semaphore=self.semaphore
        )
        self.mail_security_view.configure(fg_color="transparent", border_width=0)
        self.mail_security_view.pack(fill="x", expand=True, pady=(0, 5))

        # Exchange Connectors Sub-frame
        self.connectors_view = ExchangeConnectorsFrame(
            master=self.inner_pad,
            log_callback=self.log_msg,
            credentials_callback=self.get_credentials,
            status_change_callback=self._check_overall_status,
            concurrency_semaphore=self.semaphore
        )
        self.connectors_view.configure(fg_color="transparent", border_width=0)
        self.connectors_view.pack(fill="x", expand=True, pady=(0, 5))

        # Email Client Support Sub-frame (combining supported email clients and PST files)
        self.email_clients_view = EmailClientSupportFrame(
            master=self.inner_pad,
            log_callback=self.log_msg,
            credentials_callback=self.get_credentials,
            status_change_callback=self._check_overall_status,
            concurrency_semaphore=self.semaphore
        )
        self.email_clients_view.configure(fg_color="transparent", border_width=0)
        self.email_clients_view.pack(fill="x", expand=True, pady=(0, 5))

        self.reset_view()

    def reset_view(self):
        """Resets all sub-views and hides container."""
        self.pack_forget()
        self.mailbox_view.reset_view()
        self.calendar_view.reset_view()
        self.apps_view.reset_view()
        
        if hasattr(self.mail_security_view, 'reset_view'):
            self.mail_security_view.reset_view()
        else:
            self.mail_security_view.pack_forget()
            
        self.connectors_view.reset_view()
        self.email_clients_view.reset_view()

    def trigger_fetch(self, tenant, client_id, client_secret):
        """Displays container and delegates fetches to all sub-views."""
        self.status = "loading"
        self.on_status_change()

        self.pack(fill="x", expand=True, pady=10)

        self.mailbox_view.trigger_fetch(tenant, client_id, client_secret)
        self.calendar_view.trigger_fetch(tenant, client_id, client_secret)
        self.apps_view.trigger_fetch(tenant, client_id, client_secret)
        
        if hasattr(self.mail_security_view, 'trigger_fetch'):
            self.mail_security_view.trigger_fetch(tenant, client_id, client_secret)
            
        self.connectors_view.trigger_fetch(tenant, client_id, client_secret)
        self.email_clients_view.trigger_fetch(tenant, client_id, client_secret)

    def _check_overall_status(self):
        """Updates main container status based on sub-frame statuses."""
        sub_statuses = [
            self.mailbox_view.status,
            self.calendar_view.status,
            self.apps_view.status,
            getattr(self.mail_security_view, 'status', None),
            self.connectors_view.status,
            self.email_clients_view.status
        ]
        if "loading" in sub_statuses or getattr(self.mail_security_view, 'loading', False):
            self.status = "loading"
        else:
            if "success" in sub_statuses:
                self.status = "success"
            else:
                self.status = "error"
        self.on_status_change()

    def cancel(self):
        """Cancels all child views in this container."""
        self.mailbox_view.cancel()
        self.calendar_view.cancel()
        self.apps_view.cancel()
        self.connectors_view.cancel()
        self.email_clients_view.cancel()
        if hasattr(self.mail_security_view, 'cancel'):
            self.mail_security_view.cancel()
        self.status = None

