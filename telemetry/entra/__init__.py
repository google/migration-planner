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

"""Consolidated Microsoft Entra Data (Sign-in & Authentication Methods) Telemetry Orchestrator Container."""

import logging
import customtkinter as ctk

from telemetry.styles import *
from telemetry.entra.auth_methods import AuthMethodsSubFrame
from telemetry.entra.app_signins import AppSigninsSubFrame
from telemetry.entra.user_signins import UserSigninsSubFrame

usage_logger = logging.getLogger("M365TelemetryAsyncLogger.DevicesAppsUI")

class DevicesAppsTelemetryFrame(ctk.CTkFrame):
    """Self-contained component wrapping Microsoft Entra Data UI with independent sub-sections."""

    def __init__(self, master, log_callback, credentials_callback, status_change_callback, **kwargs):
        self.semaphore = kwargs.pop("concurrency_semaphore", None)
        super().__init__(master, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=12, **kwargs)
        self.log_msg = log_callback
        self.get_credentials = credentials_callback
        self.on_status_change = status_change_callback
        self.status = None

        self.build_ui()

    def build_ui(self):
        self.pack(fill="x", expand=True, pady=10)

        self.inner_pad = ctk.CTkFrame(self, fg_color="transparent")
        self.inner_pad.pack(fill="both", expand=True, padx=20, pady=20)

        ctk.CTkLabel(self.inner_pad, text="Microsoft Entra Data", font=FONT_HEADER_SMALL, text_color=COLOR_TEXT_MAIN).pack(anchor="w", pady=(0, 10))

        # 1. Authentication Methods at the top
        self.auth_methods_subframe = AuthMethodsSubFrame(
            self.inner_pad,
            log_callback=self.log_msg,
            credentials_callback=self.get_credentials,
            status_change_callback=self._subframe_status_changed,
            semaphore=self.semaphore
        )
        self.auth_methods_subframe.pack(fill="x", pady=(10, 15))

        # Divider 1
        self.divider1 = ctk.CTkFrame(self.inner_pad, fg_color=COLOR_OUTLINE_LIGHT, height=1)
        self.divider1.pack(fill="x", pady=15)

        # 2. App Sign Ins in the middle
        self.app_signins_subframe = AppSigninsSubFrame(
            self.inner_pad,
            log_callback=self.log_msg,
            credentials_callback=self.get_credentials,
            status_change_callback=self._subframe_status_changed,
            semaphore=self.semaphore
        )
        self.app_signins_subframe.pack(fill="x", pady=(0, 15))

        # Divider 2
        self.divider2 = ctk.CTkFrame(self.inner_pad, fg_color=COLOR_OUTLINE_LIGHT, height=1)
        self.divider2.pack(fill="x", pady=15)

        # 3. User Sign-Ins below App Sign Ins
        self.user_signins_subframe = UserSigninsSubFrame(
            self.inner_pad,
            log_callback=self.log_msg,
            credentials_callback=self.get_credentials,
            status_change_callback=self._subframe_status_changed,
            semaphore=self.semaphore
        )
        self.user_signins_subframe.pack(fill="x", pady=(0, 15))

    def _subframe_status_changed(self):
        statuses = [
            self.auth_methods_subframe.status,
            self.app_signins_subframe.status,
            self.user_signins_subframe.status
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
        self.auth_methods_subframe.reset_view()
        self.app_signins_subframe.reset_view()
        self.user_signins_subframe.reset_view()

    def trigger_fetch(self, tenant, client_id, client_secret):
        usage_logger.info("Entra Data trigger_fetch called. Propagating to independent sub-sections...")
        self.pack(fill="x", expand=True, pady=10)
        self.auth_methods_subframe.trigger_fetch(tenant, client_id, client_secret)
        self.app_signins_subframe.trigger_fetch(tenant, client_id, client_secret)
        self.user_signins_subframe.trigger_fetch(tenant, client_id, client_secret)

    def cancel(self):
        usage_logger.info("Entra Data cancel called. Propagating to independent sub-sections...")
        self.auth_methods_subframe.cancel()
        self.app_signins_subframe.cancel()
        self.user_signins_subframe.cancel()

    @property
    def last_data(self):
        return {
            "app_signins": self.app_signins_subframe.last_data,
            "auth_methods": self.auth_methods_subframe.last_data,
            "auth_methods_period": getattr(self.auth_methods_subframe, "period", "D7"),
            "user_signins": self.user_signins_subframe.last_data
        }
