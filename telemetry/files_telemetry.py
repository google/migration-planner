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

"""Uber container section wrapping SharePoint and OneDrive telemetry frames under a single Files card."""

import customtkinter as ctk
from telemetry.styles import *
from telemetry.sharepoint_onedrive_usage import SharePointUsageFrame, OneDriveUsageFrame

class FilesTelemetryFrame(ctk.CTkFrame):
    def update_loading_text(self, text_msg):
        if hasattr(self, 'loading_label') and self.loading_label.winfo_exists():
            self.loading_label.configure(text=f"⏳ {text_msg}")
    """Uber section container hosting SharePoint and OneDrive telemetry frames vertically stacked."""

    def __init__(self, master, log_callback, credentials_callback, status_change_callback, **kwargs):
        self.semaphore = kwargs.pop("concurrency_semaphore", None)
        super().__init__(master, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=12, **kwargs)
        self.log_msg = log_callback
        self.get_credentials = credentials_callback
        self.on_status_change = status_change_callback
        self.status = None

        self.build_ui()

    def build_ui(self):
        """Instantiates and stacks existing SharePoint and OneDrive frames."""
        self.pack(fill="x", expand=True, pady=10)

        self.inner_pad = ctk.CTkFrame(self, fg_color="transparent")
        self.inner_pad.pack(fill="both", expand=True, padx=20, pady=20)

        # Uber Title Heading
        ctk.CTkLabel(
            self.inner_pad,
            text="Files",
            font=FONT_HEADER_SMALL,
            text_color=COLOR_TEXT_MAIN
        ).pack(anchor="w", pady=(0, 5))

        # SharePoint Usage Sub-frame
        self.sharepoint_view = SharePointUsageFrame(
            master=self.inner_pad,
            log_callback=self.log_msg,
            credentials_callback=self.get_credentials,
            status_change_callback=self._check_overall_status,
            concurrency_semaphore=self.semaphore
        )
        self.sharepoint_view.configure(fg_color="transparent", border_width=0)
        self.sharepoint_view.pack(fill="x", expand=True, pady=(0, 5))

        # Separator line
        self.divider = ctk.CTkFrame(self.inner_pad, height=1, fg_color=COLOR_OUTLINE_LIGHT)
        self.divider.pack(fill="x", pady=10)

        # OneDrive Usage Sub-frame
        self.onedrive_view = OneDriveUsageFrame(
            master=self.inner_pad,
            log_callback=self.log_msg,
            credentials_callback=self.get_credentials,
            status_change_callback=self._check_overall_status,
            concurrency_semaphore=self.semaphore
        )
        self.onedrive_view.configure(fg_color="transparent", border_width=0)
        self.onedrive_view.pack(fill="x", expand=True, pady=(0, 5))

        self.reset_view()

    def reset_view(self):
        """Resets both sub-views and hides container."""
        self.pack_forget()
        self.sharepoint_view.reset_view()
        self.onedrive_view.reset_view()
        self.divider.pack_forget()

    def trigger_fetch(self, tenant, client_id, client_secret):
        """Displays container and delegates fetches to both sub-views."""
        self.status = "loading"
        self.on_status_change()

        self.pack(fill="x", expand=True, pady=10)
        self.divider.pack(fill="x", pady=10)

        self.sharepoint_view.trigger_fetch(tenant, client_id, client_secret)
        self.onedrive_view.trigger_fetch(tenant, client_id, client_secret)

    def _check_overall_status(self):
        """Updates main container status based on sub-frame statuses."""
        if self.sharepoint_view.status == "loading" or self.onedrive_view.status == "loading":
            self.status = "loading"
        elif self.sharepoint_view.status == "success" or self.onedrive_view.status == "success":
            self.status = "success"
        else:
            self.status = "error"
        self.on_status_change()

    def cancel(self):
        """Cancels all child views in this container."""
        self.sharepoint_view.cancel()
        self.onedrive_view.cancel()
        
        sub_statuses = [
            self.sharepoint_view.status,
            self.onedrive_view.status
        ]
        if all(s is None for s in sub_statuses):
            self.status = None
            self.reset_view()
        else:
            self._check_overall_status()
