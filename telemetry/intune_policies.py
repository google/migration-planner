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

"""Modular Intune Policies & Detected Apps telemetry scanner and visual interface."""

import os
import csv
import time
import logging
import threading
import webbrowser
from collections import defaultdict
import pandas as pd
import customtkinter as ctk
from tkinter import filedialog, messagebox
from telemetry.styles import *
from core.graph.intune import IntuneService
from core.graph.client import GraphClient

usage_logger = logging.getLogger("M365TelemetryAsyncLogger.IntunePoliciesUI")

def run_intune_policies_pipeline(client_id: str, client_secret: str, tenant_id: str, on_page_callback=None, on_apps_page_callback=None, is_cancelled_callback=None) -> dict:
    """Pipeline to fetch Intune configuration policies, mobile apps, and detected apps (v1.0) in parallel and save outputs to CSV."""
    usage_logger.info("Starting Intune Policies Pipeline in parallel...")
    
    script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
    reports_dir = os.path.join(script_dir, "reports", f"{tenant_id}_{client_id}")
    os.makedirs(reports_dir, exist_ok=True)
    csv_path_device_configs = os.path.join(reports_dir, "intune_device_configs.csv")
    csv_path_config_policies = os.path.join(reports_dir, "intune_config_policies.csv")
    csv_path_apps = os.path.join(reports_dir, "intune_apps.csv")
    csv_path_detected_apps = os.path.join(reports_dir, "intune_detected_apps.csv")
    
    temp_path_device_configs = csv_path_device_configs + ".tmp"
    temp_path_config_policies = csv_path_config_policies + ".tmp"
    temp_path_apps = csv_path_apps + ".tmp"
    temp_path_detected_apps = csv_path_detected_apps + ".tmp"

    for path in [temp_path_device_configs, temp_path_config_policies]:
        with open(path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["displayName", "platform", "policyType"])
            
    with open(temp_path_apps, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["displayName"])
            
    client = GraphClient(
        tenant_id=tenant_id,
        client_ids=client_id,
        client_secrets=client_secret,
        concurrency=3,
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
                max_rows=5000,
                on_page_callback=on_page_callback,
                is_cancelled_callback=is_cancelled_callback
            )
        except Exception as thread_err:
            usage_logger.error(f"Error in thread fetching Intune {endpoint}: {thread_err}", exc_info=True)
            errors.append(thread_err)
            
    def run_fetch_apps(path):
        try:
            service.fetch_mobile_apps(
                csv_path=path,
                max_rows=5000,
                on_page_callback=on_apps_page_callback,
                is_cancelled_callback=is_cancelled_callback
            )
        except Exception as thread_err:
            usage_logger.error(f"Error in thread fetching Intune apps: {thread_err}", exc_info=True)
            errors.append(thread_err)

    def run_fetch_detected(path):
        try:
            # Enforce 10k maximum limit for query and export
            service.fetch_detected_apps(csv_path=path, max_rows=10000, is_cancelled_callback=is_cancelled_callback)
        except Exception as thread_err:
            usage_logger.error(f"Error fetching detected apps: {thread_err}", exc_info=True)
            errors.append(thread_err)
            
    try:
        t1 = threading.Thread(target=run_fetch, args=("deviceConfigurations", temp_path_device_configs), daemon=True)
        t2 = threading.Thread(target=run_fetch, args=("configurationPolicies", temp_path_config_policies), daemon=True)
        t3 = threading.Thread(target=run_fetch_apps, args=(temp_path_apps,), daemon=True)
        t4 = threading.Thread(target=run_fetch_detected, args=(temp_path_detected_apps,), daemon=True)
        
        t1.start()
        t2.start()
        t3.start()
        t4.start()
        
        t1.join()
        t2.join()
        t3.join()
        t4.join()
        
        if len(errors) == 4:
            raise errors[0]

        # Rename successful temp files to final paths
        for temp, final in [
            (temp_path_device_configs, csv_path_device_configs),
            (temp_path_config_policies, csv_path_config_policies),
            (temp_path_apps, csv_path_apps),
            (temp_path_detected_apps, csv_path_detected_apps)
        ]:
            if os.path.exists(temp):
                if os.path.exists(final):
                    os.remove(final)
                os.rename(temp, final)

        counts = defaultdict(int)
        total_dc = 0
        total_cp = 0
        unique_apps = set()
        
        if os.path.exists(csv_path_device_configs):
            with open(csv_path_device_configs, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader, None)
                for row in reader:
                    if len(row) >= 3:
                        platform, policy_type = row[1], row[2]
                        if platform and policy_type:
                            counts[(platform, policy_type)] += 1
                            total_dc += 1
                            
        if os.path.exists(csv_path_config_policies):
            with open(csv_path_config_policies, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader, None)
                for row in reader:
                    if len(row) >= 3:
                        platform, policy_type = row[1], row[2]
                        if platform and policy_type:
                            counts[(platform, policy_type)] += 1
                            total_cp += 1
                            
        if os.path.exists(csv_path_apps):
            with open(csv_path_apps, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader, None)
                for row in reader:
                    if len(row) >= 1:
                        app_name = row[0]
                        if app_name:
                            unique_apps.add(app_name)
                            
        rows = []
        for (platform, p_type), count in sorted(counts.items()):
            rows.append((platform, p_type, str(count)))
            
        # Slicing up to 200 detected apps for UI rendering
        detected_rows_for_ui = []
        if os.path.exists(csv_path_detected_apps):
            df_detected = pd.read_csv(csv_path_detected_apps)
            df_slice = df_detected.head(200).fillna("N/A")
            detected_rows_for_ui = df_slice.to_dict('records')
            
        return {
            "total_device_configs": total_dc,
            "total_config_policies": total_cp,
            "table_rows": rows,
            "mobile_apps": sorted(list(unique_apps)),
            "detected_apps": detected_rows_for_ui
        }
    finally:
        client.close()
        for temp in [temp_path_device_configs, temp_path_config_policies, temp_path_apps, temp_path_detected_apps]:
            if 'temp' in locals() and os.path.exists(temp):
                try:
                    os.remove(temp)
                except Exception:
                    pass


class IntunePoliciesFrame(ctk.CTkFrame):
    """Component for rendering Intune Policies and Detected Apps data inside metrics tables."""

    def __init__(self, master, log_callback, credentials_callback, status_change_callback, **kwargs):
        self.semaphore = kwargs.pop("concurrency_semaphore", None)
        super().__init__(master, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=12, **kwargs)
        self.log_msg = log_callback
        self.get_credentials = credentials_callback
        self.on_status_change = status_change_callback
        
        self.status = None
        self.last_data = {}
        self.current_page = 0
        self.detected_current_page = 0
        self.ITEMS_PER_PAGE = 5
        
        # Duration timers
        self.configs_start_time = 0
        self.lbl_timer_configs = None
        self.detected_start_time = 0
        self.lbl_timer_detected = None

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
        
        self.state_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.grid_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        
        self.reset_view()

    def reset_view(self):
        self.pack_forget()
        self.state_frame.pack_forget()
        self.grid_frame.pack_forget()
        self.last_data = {}
        self.lbl_timer_configs = None
        self.lbl_timer_detected = None
        self.current_page = 0
        self.detected_current_page = 0
        
        for w in self.state_frame.winfo_children():
            w.destroy()
        for w in self.grid_frame.winfo_children():
            w.destroy()

    def _set_state_loading(self, msg="Loading..."):
        for w in self.state_frame.winfo_children():
            w.destroy()
        ctk.CTkLabel(self.state_frame, text=f"⏳ {msg}", text_color=COLOR_TEXT_SUB, font=FONT_BODY_MEDIUM).pack(pady=(20, 5))
        pb = ctk.CTkProgressBar(self.state_frame, mode="indeterminate", width=250, fg_color=COLOR_SURFACE_VARIANT, progress_color=COLOR_PRIMARY)
        pb.pack(pady=(0, 20))
        pb.start()
        self.state_frame.pack(fill="x", expand=True)

    def _set_state_error(self, error_msg):
        for w in self.state_frame.winfo_children():
            w.destroy()
        ctk.CTkLabel(self.state_frame, text=f"✖ {error_msg}", text_color=COLOR_ERROR, font=FONT_BODY_MEDIUM, justify="center").pack(pady=(20, 5))
        ctk.CTkButton(self.state_frame, text="Try Again", command=self._retry_all_fetch, width=120, fg_color="transparent", border_width=1, text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER).pack(pady=(0, 20))
        self.state_frame.pack(fill="x", expand=True)

    def _retry_all_fetch(self):
        tenant, clients, secrets = self.get_credentials()
        if tenant:
            self.trigger_fetch(tenant, clients[0], secrets[0])

    def trigger_fetch(self, tenant, client_id, client_secret):
        usage_logger.info("Intune Policies trigger_fetch called.")
        self.status = "loading"
        self.on_status_change()
        
        self.pack(fill="x", expand=True, pady=10)
        self.grid_frame.pack_forget()
        
        self._set_state_loading("Scanning Microsoft Intune Device Configurations and Detected Apps...")
        
        # Track start time for timers
        self.configs_start_time = time.time()
        self.detected_start_time = time.time()
        
        threading.Thread(
            target=self._execute_worker,
            args=(tenant, client_id, client_secret),
            daemon=True
        ).start()

    def _execute_worker(self, tenant: str, client_id: str, client_secret: str):
        if self.semaphore:
            self.semaphore.acquire()
        platform_policy_counts = defaultdict(int)
        tot_dc = [0]
        tot_cp = [0]
        unique_apps = set()
        
        def handle_page(parsed_records):
            for item in parsed_records:
                platform = item.get("platform")
                policy_type = item.get("policyType")
                if platform and policy_type:
                    platform_policy_counts[(platform, policy_type)] += 1
                    if policy_type == "Settings Catalog":
                        tot_cp[0] += 1
                    else:
                        tot_dc[0] += 1
            
            rows = []
            for (plat, p_type), count in sorted(platform_policy_counts.items()):
                rows.append((plat, p_type, str(count)))
                
            data_to_render = {
                "total_device_configs": tot_dc[0],
                "total_config_policies": tot_cp[0],
                "table_rows": rows,
                "mobile_apps": sorted(list(unique_apps)),
                "detected_apps": self.last_data.get("detected_apps", [])
            }
            self.after(0, self._render_partial_success, data_to_render)
            
        def handle_apps_page(parsed_records):
            for item in parsed_records:
                name = item.get("displayName")
                if name:
                    unique_apps.add(name)
                    
            rows = []
            for (plat, p_type), count in sorted(platform_policy_counts.items()):
                rows.append((plat, p_type, str(count)))
                
            data_to_render = {
                "total_device_configs": tot_dc[0],
                "total_config_policies": tot_cp[0],
                "table_rows": rows,
                "mobile_apps": sorted(list(unique_apps)),
                "detected_apps": self.last_data.get("detected_apps", [])
            }
            self.after(0, self._render_partial_success, data_to_render)
            
        try:
            data = run_intune_policies_pipeline(
                client_id, 
                client_secret, 
                tenant, 
                on_page_callback=handle_page,
                on_apps_page_callback=handle_apps_page,
                is_cancelled_callback=lambda: getattr(self, "is_cancelled", False)
            )
            usage_logger.info("Successfully completed Intune Policies & Detected Apps fetch.")
            self.after(0, self._render_success, data)
        except Exception as e:
            usage_logger.error("Exception caught in IntunePolicies worker.", exc_info=True)
            self.after(0, self._render_error, str(e))
        finally:
            if self.semaphore:
                self.semaphore.release()

    def _render_partial_success(self, data):
        self._update_ui_lists_paginated(data)

    def _render_success(self, data: dict):
        self.status = "success"
        self._update_ui_lists_paginated(data)
        
        # Display timers next to subheadings
        self._show_timer("configs", self.configs_start_time, self.configs_sub_header)
        self._show_timer("detected", self.detected_start_time, self.detected_sub_header)
        
        self.on_status_change()

    def _show_timer(self, mode, start_time, header_frame):
        if start_time > 0:
            elapsed = time.time() - start_time
            old_timer = getattr(self, f"lbl_timer_{mode}")
            if old_timer and old_timer.winfo_exists():
                old_timer.destroy()
            
            lbl = ctk.CTkLabel(
                self,
                text=f"⏱ {elapsed:.2f}s",
                font=ctk.CTkFont(family="Segoe UI", size=11, weight="bold"),
                text_color=COLOR_PRIMARY
            )
            lbl.pack(in_=header_frame, side="right", padx=(0, 15))
            setattr(self, f"lbl_timer_{mode}", lbl)

    def _update_ui_lists_paginated(self, data: dict):
        self.last_data = data
        self.state_frame.pack_forget()
        for w in self.grid_frame.winfo_children():
            try:
                w.destroy()
            except Exception:
                pass

        self.grid_frame.pack(fill="x")

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

        # 1. Mobile Apps section
        apps_frame = ctk.CTkFrame(self.grid_frame, fg_color="transparent")
        apps_frame.pack(fill="x", padx=10, pady=(10, 5))
        
        apps_title = ctk.CTkLabel(
            apps_frame, 
            text="⚙️ Managed Mobile Apps: ", 
            font=FONT_BODY_BOLD, 
            text_color=COLOR_TEXT_MAIN,
            anchor="w"
        )
        apps_title.pack(side="left", anchor="nw")
        
        apps_list = data.get("mobile_apps", [])
        display_text = ", ".join(apps_list) if apps_list else "No apps found or scanning..."
        apps_content = ctk.CTkLabel(
            apps_frame, 
            text=display_text, 
            font=FONT_BODY_MEDIUM, 
            text_color=COLOR_TEXT_MAIN if apps_list else COLOR_TEXT_SUB,
            justify="left",
            anchor="w"
        )
        apps_content.pack(side="left", fill="x", expand=True, anchor="nw")
        
        def make_configure_handler(lbl=apps_content):
            def on_configure(event):
                lbl.configure(wraplength=max(200, event.width - 200))
            return on_configure
            
        apps_frame.bind("<Configure>", make_configure_handler())
        
        # Spacer separator line
        ctk.CTkFrame(self.grid_frame, fg_color=COLOR_OUTLINE_LIGHT, height=1).pack(fill="x", padx=10, pady=10)

        # 2. Detected Apps Section
        self.detected_sub_header = ctk.CTkFrame(self.grid_frame, fg_color="transparent")
        self.detected_sub_header.pack(fill="x", padx=10, pady=(5, 5))
        
        ctk.CTkLabel(self.detected_sub_header, text="Detected Apps", font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN).pack(side="left")
        
        btn_reload_detected = ctk.CTkButton(
            self.detected_sub_header, text="↻ Reload", width=80, height=24,
            font=ctk.CTkFont(family="Segoe UI", size=12),
            fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color="#2563EB", hover_color="#DBEAFE",
            command=self._retry_all_fetch
        )
        btn_reload_detected.pack(side="right", padx=(10, 0))
        
        btn_export_detected = ctk.CTkButton(
            self.detected_sub_header, text="Export Detected Apps", width=180, height=32, corner_radius=16,
            font=FONT_BODY_BOLD, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER,
            command=self._export_detected_apps
        )
        btn_export_detected.pack(side="right")
        
        detected_grid = ctk.CTkFrame(self.grid_frame, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)
        detected_grid.pack(fill="x", padx=10, pady=(5, 10))
        
        detected_headers = ["App Name", "Version", "Publisher", "Platform"]
        for i in range(4):
            detected_grid.grid_columnconfigure(i, weight=2 if i == 0 else 1)
            
        for col_idx, head_text in enumerate(detected_headers):
            cell = ctk.CTkFrame(detected_grid, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=1, pady=1)
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")
            
        detected_list = data.get("detected_apps", [])
        
        # Paginate detected apps list
        total_det_count = len(detected_list)
        
        if not detected_list:
            empty_cell = ctk.CTkFrame(detected_grid, fg_color="transparent")
            empty_cell.grid(row=1, column=0, columnspan=4, sticky="nsew", pady=15)
            ctk.CTkLabel(empty_cell, text="No detected apps found.", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB).pack()
        else:
            start_det_idx = self.detected_current_page * self.ITEMS_PER_PAGE
            end_det_idx = start_det_idx + self.ITEMS_PER_PAGE
            page_det_data = detected_list[start_det_idx:end_det_idx]
            
            for r_idx, app in enumerate(page_det_data, start=1):
                bg_style = COLOR_SURFACE if r_idx % 2 != 0 else COLOR_SURFACE_VARIANT
                vals = [
                    app.get("displayName", "N/A"),
                    app.get("version", "N/A"),
                    app.get("publisher", "N/A"),
                    app.get("platform", "unknown")
                ]
                for c_idx, val in enumerate(vals):
                    c = ctk.CTkFrame(detected_grid, fg_color=bg_style, corner_radius=0)
                    c.grid(row=r_idx, column=c_idx, sticky="nsew", padx=1, pady=1)
                    ctk.CTkLabel(c, text=str(val), font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="left", wraplength=200, anchor="w").pack(padx=10, pady=12, anchor="nw")
                
        # Draw detected apps pagination
        self._draw_detected_pagination_controls(total_det_count, data)
        
        # Disclaimer detailing limitations
        ctk.CTkLabel(
            self.grid_frame, 
            text="* Displaying up to 200 detected apps in the UI. The exported CSV file contains up to 10,000 rows.", 
            font=FONT_BODY_SMALL, 
            text_color=COLOR_TEXT_SUB
        ).pack(anchor="w", padx=10, pady=(2, 10))

        # Spacer separator line
        ctk.CTkFrame(self.grid_frame, fg_color=COLOR_OUTLINE_LIGHT, height=1).pack(fill="x", padx=10, pady=15)

        # 3. Device Configurations Section
        self.configs_sub_header = ctk.CTkFrame(self.grid_frame, fg_color="transparent")
        self.configs_sub_header.pack(fill="x", padx=10, pady=(5, 5))
        
        ctk.CTkLabel(self.configs_sub_header, text="Device Configurations", font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN).pack(side="left")
        
        btn_reload_configs = ctk.CTkButton(
            self.configs_sub_header, text="↻ Reload", width=80, height=24,
            font=ctk.CTkFont(family="Segoe UI", size=12),
            fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color="#2563EB", hover_color="#DBEAFE",
            command=self._retry_all_fetch
        )
        btn_reload_configs.pack(side="right", padx=(10, 0))
        
        btn_export_configs = ctk.CTkButton(
            self.configs_sub_header, text="Export Device Configurations", width=200, height=32, corner_radius=16,
            font=FONT_BODY_BOLD, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER,
            command=self._export_device_configs
        )
        btn_export_configs.pack(side="right")
        
        tot_dc = data.get("total_device_configs", 0)
        tot_cp = data.get("total_config_policies", 0)
        
        summary_frame = ctk.CTkFrame(self.grid_frame, fg_color="transparent")
        summary_frame.pack(fill="x", padx=10, pady=5)
        ctk.CTkLabel(summary_frame, text=f"Total Extracted: {tot_dc} Device Configurations | {tot_cp} Configuration Policies", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB).pack(anchor="w")

        metrics_grid = ctk.CTkFrame(self.grid_frame, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)
        metrics_grid.pack(fill="x", padx=10, pady=(5, 10))
        
        headers = ["Platform", "Policy Type", "Number of Policies"]
        for i in range(3):
            metrics_grid.grid_columnconfigure(i, weight=1 if i == 2 else 2)
            
        for col_idx, head_text in enumerate(headers):
            cell = ctk.CTkFrame(metrics_grid, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=1, pady=1)
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")
            
        rows_data = data.get("table_rows", [])
        total_count = len(rows_data)
        
        if not rows_data:
            empty_cell = ctk.CTkFrame(metrics_grid, fg_color="transparent")
            empty_cell.grid(row=1, column=0, columnspan=3, sticky="nsew", pady=15)
            ctk.CTkLabel(empty_cell, text="No device configuration policies found.", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB).pack()
        else:
            start_idx = self.current_page * self.ITEMS_PER_PAGE
            end_idx = start_idx + self.ITEMS_PER_PAGE
            page_data = rows_data[start_idx:end_idx]

            for r_idx, (platform, p_type, count) in enumerate(page_data, start=1):
                bg_style = COLOR_SURFACE if r_idx % 2 != 0 else COLOR_SURFACE_VARIANT
                
                vals = [platform, p_type, count]
                for c_idx, val in enumerate(vals):
                    c = ctk.CTkFrame(metrics_grid, fg_color=bg_style, corner_radius=0)
                    c.grid(row=r_idx, column=c_idx, sticky="nsew", padx=1, pady=1)
                    ctk.CTkLabel(c, text=val, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="left", wraplength=450).pack(padx=10, pady=12, anchor="nw")

        self._draw_pagination_controls(total_count, data)

        ctk.CTkLabel(self.grid_frame, text="* Based on sample data collected from Intune.", font=FONT_BODY_SMALL, text_color=COLOR_TEXT_SUB).pack(anchor="w", padx=10, pady=(2, 15))

    def _draw_pagination_controls(self, total_count, data):
        total_pages = max(1, (total_count + self.ITEMS_PER_PAGE - 1) // self.ITEMS_PER_PAGE)
        
        control_frame = ctk.CTkFrame(self.grid_frame, fg_color="transparent")
        control_frame.pack(fill="x", pady=(5, 10))

        center_container = ctk.CTkFrame(control_frame, fg_color="transparent")
        center_container.pack(pady=(5, 10))

        prev_state = "normal" if self.current_page > 0 else "disabled"
        btn_prev = ctk.CTkButton(
            center_container, text="◀ Prev", width=70, height=22, corner_radius=6,
            font=FONT_BODY_SMALL, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER, state=prev_state,
            command=lambda: self._change_page(-1, data)
        )
        btn_prev.pack(side="left", padx=5)

        page_lbl = ctk.CTkLabel(
            center_container, text=f"Page {self.current_page + 1} of {total_pages}",
            font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB
        )
        page_lbl.pack(side="left", padx=15)

        next_state = "normal" if self.current_page < total_pages - 1 else "disabled"
        btn_next = ctk.CTkButton(
            center_container, text="Next ▶", width=70, height=22, corner_radius=6,
            font=FONT_BODY_SMALL, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER, state=next_state,
            command=lambda: self._change_page(1, data)
        )
        btn_next.pack(side="left", padx=5)

    def _draw_detected_pagination_controls(self, total_count, data):
        total_pages = max(1, (total_count + self.ITEMS_PER_PAGE - 1) // self.ITEMS_PER_PAGE)
        
        control_frame = ctk.CTkFrame(self.grid_frame, fg_color="transparent")
        control_frame.pack(fill="x", pady=(5, 10))

        center_container = ctk.CTkFrame(control_frame, fg_color="transparent")
        center_container.pack(pady=(5, 10))

        prev_state = "normal" if self.detected_current_page > 0 else "disabled"
        btn_prev = ctk.CTkButton(
            center_container, text="◀ Prev", width=70, height=22, corner_radius=6,
            font=FONT_BODY_SMALL, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER, state=prev_state,
            command=lambda: self._change_detected_page(-1, data)
        )
        btn_prev.pack(side="left", padx=5)

        page_lbl = ctk.CTkLabel(
            center_container, text=f"Page {self.detected_current_page + 1} of {total_pages}",
            font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB
        )
        page_lbl.pack(side="left", padx=15)

        next_state = "normal" if self.detected_current_page < total_pages - 1 else "disabled"
        btn_next = ctk.CTkButton(
            center_container, text="Next ▶", width=70, height=22, corner_radius=6,
            font=FONT_BODY_SMALL, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER, state=next_state,
            command=lambda: self._change_detected_page(1, data)
        )
        btn_next.pack(side="left", padx=5)

    def _change_page(self, delta, data):
        self.current_page += delta
        self._update_ui_lists_paginated(data)

    def _change_detected_page(self, delta, data):
        self.detected_current_page += delta
        self._update_ui_lists_paginated(data)

    def _render_error(self, err_msg):
        usage_logger.warning(f"Intune Policies Telemetry fetch failed: {err_msg}")
        self._set_state_error(err_msg)
        self.status = "error"
        self.on_status_change()

    def _export_detected_apps(self):
        self._export_table_csv("intune_detected_apps.csv", "M365_Intune_Detected_Apps.csv", "Detected Apps")

    def _export_device_configs(self):
        self._export_table_csv("intune_device_configs.csv", "M365_Intune_Device_Configurations.csv", "Device Configurations")

    def _export_table_csv(self, filename, default_name, title):
        script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
        tenant, clients, _ = self.get_credentials()
        if not tenant or not clients: return
        src_path = os.path.join(script_dir, "reports", f"{tenant}_{clients[0]}", filename)
        if not os.path.exists(src_path):
            messagebox.showerror("Export Error", "No data available to export yet.")
            return

        dest_path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            title=f"Export {title} CSV",
            initialfile=default_name,
            parent=self
        )
        if not dest_path: return
        
        try:
            df = pd.read_csv(src_path)
            df.fillna("N/A").to_csv(dest_path, index=False, encoding="utf-8-sig")
            messagebox.showinfo("Export Successful", f"{title} successfully exported to:\n{dest_path}")
        except Exception as e:
            messagebox.showerror("Export Failed", f"Failed to save CSV file: {e}")

    def cancel(self):
        """Cancels background thread operations."""
        self.status = None
