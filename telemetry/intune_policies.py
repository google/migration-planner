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

import os
import csv
import logging
import threading
import webbrowser
import customtkinter as ctk
from telemetry.styles import *
from core.graph.client import GraphClient
from core.graph.intune import IntuneService

usage_logger = logging.getLogger("IntunePoliciesUI")

def run_intune_policies_pipeline(client_id: str, client_secret: str, tenant_id: str, on_page_callback=None, is_cancelled_callback=None) -> dict:
    """Pipeline to fetch Intune configuration policies in parallel and aggregate unique policies by platform."""
    usage_logger.info("Starting Intune Policies Pipeline in parallel...")
    
    script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
    reports_dir = os.path.join(script_dir, "reports", f"{tenant_id}_{client_id}")
    os.makedirs(reports_dir, exist_ok=True)
    
    csv_path_device_configs = os.path.join(reports_dir, "intune_device_configs.csv")
    csv_path_config_policies = os.path.join(reports_dir, "intune_config_policies.csv")
    
    for path in [csv_path_device_configs, csv_path_config_policies]:
        with open(path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["id", "displayName", "platform", "policyType"])
            
    client = GraphClient(
        tenant_id=tenant_id,
        client_ids=client_id,
        client_secrets=client_secret,
        concurrency=2,
        retries=5,
        backoff=2
    )
    client.authenticate()
    service = IntuneService(client)
    
    errors = []
    
    def run_fetch(endpoint, path):
        try:
            service.fetch_configuration_records(
                endpoint_name=endpoint,
                csv_path=path,
                max_rows=10000,
                on_page_callback=on_page_callback,
                is_cancelled_callback=is_cancelled_callback
            )
        except Exception as thread_err:
            usage_logger.error(f"Error in thread fetching Intune {endpoint}: {thread_err}")
            errors.append(thread_err)
            
    try:
        t1 = threading.Thread(target=run_fetch, args=("deviceConfigurations", csv_path_device_configs), daemon=True)
        t2 = threading.Thread(target=run_fetch, args=("configurationPolicies", csv_path_config_policies), daemon=True)
        
        t1.start()
        t2.start()
        
        t1.join()
        t2.join()
        
        if len(errors) == 2:
            raise errors[0]
            
        policies_by_platform = {}
        
        for path in [csv_path_device_configs, csv_path_config_policies]:
            if os.path.exists(path):
                with open(path, 'r', encoding='utf-8') as f:
                    reader = csv.reader(f)
                    next(reader, None)
                    for row in reader:
                        if len(row) >= 4:
                            platform, policy_name = row[2], row[1]
                            if platform and policy_name:
                                if platform not in policies_by_platform:
                                    policies_by_platform[platform] = set()
                                policies_by_platform[platform].add(policy_name)
                                
        return {
            platform: sorted(list(names))
            for platform, names in policies_by_platform.items()
        }
    finally:
        client.close()


class IntunePoliciesFrame(ctk.CTkFrame):
    """Component for rendering Intune Policies data in wrapped dynamic horizontal list format."""

    def __init__(self, master, log_callback, credentials_callback, status_change_callback, **kwargs):
        self.semaphore = kwargs.pop("concurrency_semaphore", None)
        super().__init__(master, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=12, **kwargs)
        self.log_msg = log_callback
        self.get_credentials = credentials_callback
        self.on_status_change = status_change_callback
        self.status = None
        self.last_data = {}

        self.build_ui()

    def build_ui(self):
        self.pack(fill="x", expand=True, pady=10)
        
        self.inner_pad = ctk.CTkFrame(self, fg_color="transparent")
        self.inner_pad.pack(fill="both", expand=True, padx=20, pady=20)
        
        self.header_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.header_frame.pack(fill="x", pady=(0, 10))
        
        self.title_lbl = ctk.CTkLabel(
            self.header_frame,
            text="Intune Policies (Device Configurations)",
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
        
        self.state_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.grid_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        
        self.reset_view()

    def reset_view(self):
        self.pack_forget()
        self.state_frame.pack_forget()
        self.grid_frame.pack_forget()
        self.last_data = {}
        
        for w in self.state_frame.winfo_children():
            w.destroy()
        for w in self.grid_frame.winfo_children():
            w.destroy()

    def _set_state_loading(self, msg="Loading..."):
        for w in self.state_frame.winfo_children():
            w.destroy()
        ctk.CTkLabel(self.state_frame, text=f"⏳ {msg}", text_color=COLOR_TEXT_SUB, font=FONT_BODY_MEDIUM).pack(pady=20)
        self.state_frame.pack(fill="x", expand=True)

    def _set_state_error(self, error_msg):
        for w in self.state_frame.winfo_children():
            w.destroy()
        ctk.CTkLabel(self.state_frame, text=f"✖ {error_msg}", text_color=COLOR_ERROR, font=FONT_BODY_MEDIUM, justify="center").pack(pady=(20, 5))
        ctk.CTkButton(self.state_frame, text="Try Again", command=self._retry_fetch, width=120, fg_color="transparent", border_width=1, text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER).pack(pady=(0, 20))
        self.state_frame.pack(fill="x", expand=True)

    def _retry_fetch(self):
        tenant, clients, secrets = self.get_credentials()
        if tenant:
            self.trigger_fetch(tenant, clients[0], secrets[0])

    def trigger_fetch(self, tenant, client_id, client_secret):
        usage_logger.info("Intune Policies trigger_fetch called.")
        self.status = "loading"
        self.on_status_change()
        
        self.pack(fill="x", expand=True, pady=10)
        self.grid_frame.pack_forget()
        
        self._set_state_loading("Scanning Microsoft Intune Device Configurations and Policies...")
        
        threading.Thread(
            target=self._execute_worker,
            args=(tenant, client_id, client_secret),
            daemon=True
        ).start()

    def _execute_worker(self, tenant: str, client_id: str, client_secret: str):
        if self.semaphore:
            self.semaphore.acquire()
            
        platform_policies = {}
        
        def handle_page(parsed_records):
            for item in parsed_records:
                platform = item.get("platform")
                name = item.get("displayName")
                if platform and name:
                    if platform not in platform_policies:
                        platform_policies[platform] = set()
                    platform_policies[platform].add(name)
            
            data_to_render = {
                plat: sorted(list(names))
                for plat, names in platform_policies.items()
            }
            self.after(0, self._render_partial_success, data_to_render)
            
        try:
            data = run_intune_policies_pipeline(
                client_id, 
                client_secret, 
                tenant, 
                on_page_callback=handle_page,
                is_cancelled_callback=lambda: getattr(self, "is_cancelled", False)
            )
            usage_logger.info("Successfully completed Intune Policies fetch.")
            self.after(0, self._render_success, data)
        except Exception as e:
            usage_logger.error("Exception caught in Intune worker.", exc_info=True)
            self.after(0, self._render_error, str(e))
        finally:
            if self.semaphore:
                self.semaphore.release()

    def _render_partial_success(self, data: dict):
        if self.status == "loading":
            self._update_ui_lists(data)

    def _render_success(self, data: dict):
        self.status = "success"
        self._update_ui_lists(data)
        self.on_status_change()

    def _update_ui_lists(self, data: dict):
        self.last_data = data
        self.state_frame.pack_forget()
        for w in self.grid_frame.winfo_children():
            try:
                w.destroy()
            except Exception:
                pass

        self.grid_frame.pack(fill="x", expand=True)

        if self.status == "loading":
            progress_frame = ctk.CTkFrame(self.grid_frame, fg_color=COLOR_TONAL_BG, height=26, corner_radius=6)
            progress_frame.pack(fill="x", pady=(0, 6))
            ctk.CTkLabel(
                progress_frame, 
                text="⏳ Querying Microsoft Intune configuration policies in the background... UI will auto-refresh.", 
                font=FONT_BODY_SMALL,
                text_color=COLOR_TONAL_TEXT
            ).pack(padx=10, pady=2, anchor="w")
        elif self.status == "cancelled" or getattr(self, "is_cancelled", False):
            progress_frame = ctk.CTkFrame(self.grid_frame, fg_color=COLOR_SURFACE_VARIANT, height=26, corner_radius=6)
            progress_frame.pack(fill="x", pady=(0, 6))
            ctk.CTkLabel(
                progress_frame, 
                text="⚠️ Fetching cancelled by user. Displaying partial data.", 
                font=FONT_BODY_SMALL,
                text_color=COLOR_ERROR
            ).pack(padx=10, pady=2, anchor="w")

        # Map to render platforms
        for platform, policies in sorted(data.items()):
            row_frame = ctk.CTkFrame(self.grid_frame, fg_color="transparent")
            row_frame.pack(fill="x", pady=6, anchor="w")
            
            lbl_title = ctk.CTkLabel(
                row_frame, 
                text=f"⚙️ {platform} Policies: ", 
                font=FONT_BODY_BOLD, 
                text_color=COLOR_TEXT_MAIN,
                anchor="w"
            )
            lbl_title.pack(side="left", anchor="nw")
            
            display_text = ", ".join(policies) if policies else "No policies found"
            lbl_content = ctk.CTkLabel(
                row_frame, 
                text=display_text, 
                font=FONT_BODY_MEDIUM, 
                text_color=COLOR_TEXT_MAIN if policies else COLOR_TEXT_SUB,
                justify="left",
                anchor="w"
            )
            lbl_content.pack(side="left", fill="x", expand=True, anchor="nw")
            
            def make_configure_handler(lbl=lbl_content):
                def on_configure(event):
                    lbl.configure(wraplength=max(200, event.width - 200))
                return on_configure
                
            row_frame.bind("<Configure>", make_configure_handler())

    def _render_error(self, err_msg):
        usage_logger.warning(f"Intune Policies Telemetry fetch failed: {err_msg}")
        self._set_state_error(err_msg)
        self.status = "error"
        self.on_status_change()

    def cancel(self):
        """Cancels background thread operations."""
        self.status = None
