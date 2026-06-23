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

"""Consolidated Microsoft Intune Telemetry Orchestrator Container."""

import logging
import webbrowser
import customtkinter as ctk

from telemetry.styles import *
from telemetry.intune.mobile_apps import MobileAppsSubFrame
from telemetry.intune.detected_apps import DetectedAppsSubFrame
from telemetry.intune.device_configs import DeviceConfigsSubFrame

usage_logger = logging.getLogger("M365TelemetryAsyncLogger.IntuneUI")

class IntunePoliciesFrame(ctk.CTkFrame):
    """Component for rendering Intune Policies and Detected Apps data inside modular subframes."""

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
        
        self.header_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.header_frame.pack(fill="x", pady=(0, 10))
        
        self.title_lbl = ctk.CTkLabel(
            self.header_frame,
            text="Microsoft Intune Data",
            font=FONT_HEADER_SMALL,
            text_color=COLOR_TEXT_MAIN
        )
        self.title_lbl.pack(side="left", anchor="w")
        
        self.link_lbl = ctk.CTkLabel(
            self.header_frame,
            text="Open Intune Admin Center ↗",
            font=FONT_BODY_BOLD,
            text_color=COLOR_PRIMARY,
            cursor="hand2"
        )
        self.link_lbl.pack(side="left", anchor="w", padx=(15, 0))
        self.link_lbl.bind("<Button-1>", lambda e: webbrowser.open("https://intune.microsoft.com/#view/Microsoft_Intune_DeviceSettings/DevicesMenu/~/configuration"))
        self.link_lbl.bind("<Enter>", lambda e: self.link_lbl.configure(text_color=COLOR_PRIMARY_HOVER))
        self.link_lbl.bind("<Leave>", lambda e: self.link_lbl.configure(text_color=COLOR_PRIMARY))
        
        self.body_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.body_frame.pack(fill="x", expand=True)

        # 1. Managed Mobile Apps
        self.mobile_apps_view = MobileAppsSubFrame(
            self.body_frame,
            log_callback=self.log_msg,
            credentials_callback=self.get_credentials,
            status_change_callback=self._subframe_status_changed,
            semaphore=self.semaphore
        )
        self.mobile_apps_view.pack(fill="x", pady=(10, 15))

        # Divider 1
        self.divider1 = ctk.CTkFrame(self.body_frame, fg_color=COLOR_OUTLINE_LIGHT, height=1)
        self.divider1.pack(fill="x", pady=15)

        # 2. Detected Apps
        self.detected_apps_view = DetectedAppsSubFrame(
            self.body_frame,
            log_callback=self.log_msg,
            credentials_callback=self.get_credentials,
            status_change_callback=self._subframe_status_changed,
            semaphore=self.semaphore
        )
        self.detected_apps_view.pack(fill="x", pady=(0, 15))

        # Divider 2
        self.divider2 = ctk.CTkFrame(self.body_frame, fg_color=COLOR_OUTLINE_LIGHT, height=1)
        self.divider2.pack(fill="x", pady=15)

        # 3. Device Configurations
        self.device_configs_view = DeviceConfigsSubFrame(
            self.body_frame,
            log_callback=self.log_msg,
            credentials_callback=self.get_credentials,
            status_change_callback=self._subframe_status_changed,
            semaphore=self.semaphore
        )
        self.device_configs_view.pack(fill="x", pady=(0, 15))

        self.reset_view()

    def _subframe_status_changed(self):
        statuses = [
            self.mobile_apps_view.status,
            self.detected_apps_view.status,
            self.device_configs_view.status
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
        self.mobile_apps_view.reset_view()
        self.detected_apps_view.reset_view()
        self.device_configs_view.reset_view()

    def trigger_fetch(self, tenant, client_id, client_secret):
        usage_logger.info("Intune trigger_fetch called.")
        self.pack(fill="x", expand=True, pady=10)
        self.mobile_apps_view.trigger_fetch(tenant, client_id, client_secret)
        self.detected_apps_view.trigger_fetch(tenant, client_id, client_secret)
        self.device_configs_view.trigger_fetch(tenant, client_id, client_secret)

    def cancel(self):
        usage_logger.info("Intune cancel called.")
        self.mobile_apps_view.cancel()
        self.detected_apps_view.cancel()
        self.device_configs_view.cancel()

    @property
    def last_data(self):
        return {
            "total_device_configs": getattr(self.device_configs_view, "total_device_configs", 0),
            "total_config_policies": getattr(self.device_configs_view, "total_config_policies", 0),
            "table_rows": self.device_configs_view.last_data,
            "mobile_apps": self.mobile_apps_view.last_data,
            "detected_apps": self.detected_apps_view.last_data
        }
