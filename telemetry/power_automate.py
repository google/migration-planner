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

"""Modular Power Automate telemetry scanner, analysis pipelines, and visual interfaces."""

import os
import time
import json
import logging
import threading
import requests
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tkinter import filedialog, messagebox
from typing import Any, Dict, List
import customtkinter as ctk

# Safely import matplotlib to embed plots in Tkinter
try:
    from matplotlib.figure import Figure
    from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False

# Import shared styles
from telemetry.styles import *

# Bind to the async logger initialized in m365_telemetry.py
usage_logger = logging.getLogger("M365TelemetryAsyncLogger")


# =================================================================================
# SCANNER LOGIC / PIPELINE
# =================================================================================

class PowerAutomateScanner:
    def __init__(self, tenant_id, client_id, client_secret):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        
        self.log_dir = os.path.join("telemetry", "logs")
        if not os.path.exists(self.log_dir):
            os.makedirs(self.log_dir)
            
        self.log_file = os.path.join(self.log_dir, "power_automate_log.txt")
        self._setup_logger()
        self.access_token = None

    def _setup_logger(self):
        """Configures logging to propagate to the central M365TelemetryAsyncLogger."""
        self.logger = logging.getLogger("M365TelemetryAsyncLogger.PowerAutomateScanner")

    def _get_access_token(self, scope):
        """Fetches OAuth 2.0 token for a given scope."""
        self.logger.info(f"Step Start: Fetching access token for scope {scope} from Microsoft Identity Platform.")
        url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        payload = {
            'grant_type': 'client_credentials',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'scope': scope
        }
        
        try:
            response = requests.post(url, headers=headers, data=payload)
            response.raise_for_status()
            token = response.json().get('access_token')
            self.logger.info(f"Step End: Successfully retrieved access token for {scope}.")
            return token
        except Exception as e:
            self.logger.error(f"Step Error: Failed to fetch access token for {scope}. Error: {str(e)}")
            return None

    def fetch_all_pages(self, url, headers, context_name="API"):
        """Helper to cleanly handle Power Platform API pagination & Throttling limits."""
        results = []
        while url:
            res = requests.get(url, headers=headers)
            
            if res.status_code == 429:
                retry_after = int(res.headers.get("Retry-After", 2))
                self.logger.warning(f"[!] Rate limited on {context_name}. Waiting {retry_after} seconds...")
                time.sleep(retry_after)
                continue
                
            if res.status_code == 200:
                data = res.json()
                results.extend(data.get("value", []))
                url = data.get("nextLink") or data.get("@odata.nextLink")
            else:
                self.logger.error(f"[X] {context_name} Request Failed | HTTP {res.status_code}: {res.text}")
                break
                
        return results

    def fetch_single_resource(self, url, headers, context_name="API"):
        """Helper to cleanly fetch a single resource handling Throttling limits for loop logic."""
        while True:
            res = requests.get(url, headers=headers)
            if res.status_code == 429:
                retry_after = int(res.headers.get("Retry-After", 2))
                self.logger.warning(f"[!] Rate limited on {context_name}. Waiting {retry_after} seconds...")
                time.sleep(retry_after)
                continue
            if res.status_code == 200:
                return res.json()
            self.logger.error(f"[X] {context_name} Request Failed | HTTP {res.status_code}: {res.text}")
            return None

    def scan_flows(self):
        """Scans Power Automate flows across all environments in the tenant."""
        self.logger.info("Main Process Start: Initiating Power Automate Flow Scan.")
        
        try:
            bap_token = self._get_access_token("https://api.bap.microsoft.com/.default")
            flow_token = self._get_access_token("https://service.flow.microsoft.com/.default")
        except Exception as e:
            self.logger.error(f"Auth Error: {e}")
            return None

        if not bap_token or not flow_token:
            self.logger.error("Main Process Failure: Aborting scan due to missing access tokens.")
            return None

        bap_headers = {"Authorization": f"Bearer {bap_token}", "Accept": "application/json"}
        flow_headers = {"Authorization": f"Bearer {flow_token}", "Accept": "application/json"}

        self.logger.info("Step Start: Fetching all environments in the tenant.")
        env_api_url = "https://api.bap.microsoft.com/providers/Microsoft.BusinessAppPlatform/scopes/admin/environments?api-version=2023-06-01"
        environments = self.fetch_all_pages(env_api_url, bap_headers, context_name="Environment Discovery")
        
        if not environments:
            self.logger.error("[X] No environments found or failed to fetch.")
            return None

        self.logger.info(f"[+] Successfully retrieved {len(environments)} environments.")

        counts = {"Cloud Flows": 0, "Desktop Flows": 0}
        active_counts = {"Cloud Flows": 0, "Desktop Flows": 0}
        tier_counts = {"Personal Productivity": 0, "Enterprise/Departmental": 0}
        active_tier_counts = {"Personal Productivity": 0, "Enterprise/Departmental": 0}
        premium_connectors_found = set()
        custom_connectors_found = set()
        import tempfile
        import threading
        cf_fd, complex_logic_flows_path = tempfile.mkstemp(suffix=".jsonl")
        os.close(cf_fd)
        complex_flows_lock = threading.Lock()
        PREMIUM_KEYWORDS = ['shared_sql', 'shared_httpaction', 'shared_salesforce', 'shared_oracle', 'shared_sap']

        for env in environments:
            env_name = env.get("name")
            env_props = env.get("properties", {})
            env_display = env_props.get("displayName", env_name)
            is_default = env_props.get("isDefault", False)
            
            self.logger.info(f"[*] Scanning Environment: {env_display}")
            
            # ==========================================
            # 1. FETCH CLOUD FLOWS
            # ==========================================
            flows_url = f"https://api.flow.microsoft.com/providers/Microsoft.ProcessSimple/scopes/admin/environments/{env_name}/v2/flows?api-version=2016-11-01"
            cloud_flows = self.fetch_all_pages(flows_url, flow_headers, context_name="Cloud Flows Admin API (V2)")
            
            counts["Cloud Flows"] += len(cloud_flows)
            active_cloud_flows = [f for f in cloud_flows if f.get("properties", {}).get("state") == "Started"]
            active_counts["Cloud Flows"] += len(active_cloud_flows)
            
            self.logger.info(f"    -> Cloud Flows: {len(cloud_flows)} total found, {len(active_cloud_flows)} are currently active.")
            
            # Fetch details in parallel using ThreadPoolExecutor
            
            def fetch_and_process_flow_detail(flow_summary):
                flow_id = flow_summary.get("name")
                state = flow_summary.get("properties", {}).get("state")
                is_active = (state == "Started")
                
                detail_url = f"https://api.flow.microsoft.com/providers/Microsoft.ProcessSimple/scopes/admin/environments/{env_name}/flows/{flow_id}?api-version=2016-11-01"
                flow_detail = self.fetch_single_resource(detail_url, flow_headers, context_name=f"Get Flow Details ({flow_id})")
                if not flow_detail:
                    return None
                return (flow_summary, flow_detail, is_active)
            
            with ThreadPoolExecutor(max_workers=15) as executor:
                futures = {executor.submit(fetch_and_process_flow_detail, f): f for f in cloud_flows}
                for future in as_completed(futures):
                    res = future.result()
                    if not res:
                        continue
                    
                    flow_summary, flow_detail, is_active = res
                    props = flow_detail.get("properties", {})
                    name = props.get("displayName", "Unnamed Flow")
                    
                    is_managed = "workflowEntityId" in props or not is_default
                    if is_managed:
                        tier_counts["Enterprise/Departmental"] += 1
                        tier = "Enterprise"
                        if is_active:
                            active_tier_counts["Enterprise/Departmental"] += 1
                    else:
                        tier_counts["Personal Productivity"] += 1
                        tier = "Personal"
                        if is_active:
                            active_tier_counts["Personal Productivity"] += 1

                    conn_refs = props.get("connectionReferences", {})
                    for conn_key, conn_val in conn_refs.items():
                        api_obj = conn_val.get("api", {})
                        api_id = api_obj.get("id", "")
                        conn_name = api_id.split("/")[-1] if "/" in api_id else api_id
                        
                        if api_obj.get("tier") == "Premium" or any(kw in conn_name.lower() for kw in PREMIUM_KEYWORDS):
                            premium_connectors_found.add(conn_name)
                            self.logger.info(f"      [!] Premium connector found: {conn_name} in flow {name}")
                        if "custom" in api_id.lower() or api_obj.get("type") == "Microsoft.PowerApps/apis/custom":
                            custom_connectors_found.add(conn_name)
                            self.logger.info(f"      [!] Custom connector found: {conn_name} in flow {name}")

                    actions_str = json.dumps(props)
                    has_nested_loops = actions_str.count('"type": "Foreach"') > 0 or actions_str.count('"type": "Until"') > 0
                    has_multi_approvals = "shared_approvals" in actions_str or "Approval" in actions_str
                    has_advanced_expressions = "@" in actions_str and any(exp in actions_str for exp in ["concat(", "split(", "base64("])

                    if has_nested_loops or has_multi_approvals or has_advanced_expressions:
                        self.logger.info(f"      [!] Complex logic detected in Cloud Flow: {name}")
                        reasons = []
                        if has_nested_loops: reasons.append("Nested Loops")
                        if has_multi_approvals: reasons.append("Multi Approvals")
                        if has_advanced_expressions: reasons.append("Advanced Expressions")
                        
                        flow_dict = {
                            "Environment": env_display, "Name": name, "Type": "Cloud Flow", "Tier": tier,
                            "Active": "Yes" if is_active else "No",
                            "Reason": ", ".join(reasons)
                        }
                        with complex_flows_lock:
                            with open(complex_logic_flows_path, 'a', encoding='utf-8') as cf_f:
                                cf_f.write(json.dumps(flow_dict) + '\n')
                    
                    del flow_detail
                    del res

            # ==========================================
            # 2. FETCH DESKTOP FLOWS
            # ==========================================
            instance_url = env_props.get("linkedEnvironmentMetadata", {}).get("instanceApiUrl")
            
            if instance_url:
                dv_url = instance_url.rstrip("/")
                try:
                    dv_token = self._get_access_token(f"{dv_url}/.default")
                    if not dv_token:
                         self.logger.warning(f"      [X] Failed to acquire token for Dataverse instance {dv_url}")
                         continue
                         
                    headers_dv = {
                        "Authorization": f"Bearer {dv_token}",
                        "Accept": "application/json",
                        "OData-MaxVersion": "4.0", "OData-Version": "4.0"
                    }
                    
                    dv_api_url = f"{dv_url}/api/data/v9.2/workflows?$filter=category eq 6&$select=name,clientdata,ismanaged,statecode,_ownerid_value&$expand=ownerid"
                    desktop_flows = self.fetch_all_pages(dv_api_url, headers_dv, context_name="Dataverse Desktop Flows API")
                    
                    counts["Desktop Flows"] += len(desktop_flows)
                    active_desktop_flows = [f for f in desktop_flows if f.get("statecode") == 1]
                    active_counts["Desktop Flows"] += len(active_desktop_flows)
                    
                    self.logger.info(f"    -> Desktop Flows: {len(desktop_flows)} total found, {len(active_desktop_flows)} are active.")
                    
                    for flow in desktop_flows:
                        name = flow.get("name", "Unnamed Desktop Flow")
                        is_managed = flow.get("ismanaged", False)
                        owner_name = flow.get("ownerid", {}).get("fullname", "Unknown / System")
                        statecode = flow.get("statecode")
                        is_active = (statecode == 1)
                        
                        if is_managed or "system" in owner_name.lower() or not is_default:
                            tier_counts["Enterprise/Departmental"] += 1
                            tier = "Enterprise"
                            if is_active:
                                active_tier_counts["Enterprise/Departmental"] += 1
                        else:
                            tier_counts["Personal Productivity"] += 1
                            tier = "Personal"
                            if is_active:
                                active_tier_counts["Personal Productivity"] += 1
                            
                        client_data_str = flow.get("clientdata", "")
                        if client_data_str:
                            try:
                                has_nested_loops = client_data_str.lower().count("foreach") > 1
                                has_multi_approvals = client_data_str.count("shared_approvals") > 1
                                has_advanced_expressions = "@" in client_data_str and any(exp in client_data_str for exp in ["concat(", "split(", "base64("])

                                if has_nested_loops or has_multi_approvals or has_advanced_expressions:
                                    self.logger.info(f"      [!] Complex logic detected in Desktop Flow: {name}")
                                    reasons = []
                                    if has_nested_loops: reasons.append("Nested Loops")
                                    if has_multi_approvals: reasons.append("Multi Approvals")
                                    if has_advanced_expressions: reasons.append("Advanced Expressions")
                                    
                                    flow_dict = {
                                        "Environment": env_display, "Name": name, "Type": "Desktop Flow", "Tier": tier,
                                        "Active": "Yes" if is_active else "No",
                                        "Reason": ", ".join(reasons)
                                    }
                                    with complex_flows_lock:
                                        with open(complex_logic_flows_path, 'a', encoding='utf-8') as cf_f:
                                            cf_f.write(json.dumps(flow_dict) + '\n')
                            except Exception:
                                pass
                except Exception as e:
                    self.logger.error(f"      [X] Failed to authenticate against Dataverse instance {dv_url}: {e}")

        complex_active_count = 0
        complex_inactive_count = 0
        with open(complex_logic_flows_path, 'r', encoding='utf-8') as f_cf:
            for line in f_cf:
                if json.loads(line).get("Active") == "Yes":
                    complex_active_count += 1
                else:
                    complex_inactive_count += 1
                    
        results = {
            "total_environments": len(environments),
            "counts": counts,
            "active_counts": active_counts,
            "tier_counts": tier_counts,
            "active_tier_counts": active_tier_counts,
            "premium_connectors": list(premium_connectors_found),
            "custom_connectors": list(custom_connectors_found),
            "complex_logic_flows_path": complex_logic_flows_path,
            "complex_active_count": complex_active_count,
            "complex_inactive_count": complex_inactive_count
        }

        self.logger.info("Step End: Analysis complete.")
        self.logger.info("Main Process End: Power Automate Telemetry scan finished.")
        return results


# =================================================================================
# MODULAR UI COMPONENT
# =================================================================================

class PowerAutomateUsageFrame(ctk.CTkFrame):
    def update_loading_text(self, text_msg):
        if hasattr(self, 'loading_label') and self.loading_label.winfo_exists():
            self.loading_label.configure(text=f"⏳ {text_msg}")
    """Self-contained component wrapping Power Automate UI and export controls."""
    
    def __init__(self, master, log_callback, credentials_callback, status_change_callback, **kwargs):
        self.semaphore = kwargs.pop("concurrency_semaphore", None)
        super().__init__(master, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=12, **kwargs)
        self.log_msg = log_callback
        self.get_credentials = credentials_callback
        self.on_status_change = status_change_callback
        self.status = None  # 'loading', 'success', 'error', None
        self.last_complex_flows = []
        self.last_results = {}
        
        self.build_ui()

    def build_ui(self):
        self.pack(fill="x", expand=True, pady=10)
        
        self.inner_pad = ctk.CTkFrame(self, fg_color="transparent")
        self.inner_pad.pack(fill="both", expand=True, padx=20, pady=20)
        
        self.pa_header = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.pa_header.pack(fill="x", pady=(0, 10))
        
        self.header = ctk.CTkFrame(self.pa_header, fg_color="transparent")
        self.header.pack(fill="x", pady=(0, 10))
        ctk.CTkLabel(self.header, text="Power Automate", font=FONT_HEADER_SMALL, text_color=COLOR_TEXT_MAIN).pack(side="left")
        self.reload_btn = ctk.CTkButton(
            self.header, 
            text="↻ Reload", 
            width=80, 
            height=24,
            font=__import__("customtkinter").CTkFont(family="Segoe UI", size=12),
            fg_color="transparent", 
            border_width=1, 
            text_color="#2563EB", 
            hover_color="#DBEAFE",
            command=self._retry_fetch
        )
        self.reload_btn.pack(side="right")
        
        self.btn_export_pa = ctk.CTkButton(
            self.pa_header, text="Export Complex Flows", width=160, height=32, corner_radius=16,
            font=FONT_BODY_BOLD, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER,
            command=self.export_complex_flows, state="disabled"
        )
        self.btn_export_pa.pack(side="right")

        self.state_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.grid_frame = ctk.CTkFrame(self.inner_pad, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)

        # Height control slider for Power Automate Chart (packed above the chart dynamically)
        self.pa_height_var = ctk.DoubleVar(value=400)
        self.pa_slider_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")

        self.slider_pa_height = ctk.CTkSlider(
            self.pa_slider_frame, from_=200, to=800, number_of_steps=60,
            variable=self.pa_height_var, width=120, height=16,
            command=self._on_pa_height_slider_change
        )
        self.slider_pa_height.pack(side="right")

        self.lbl_pa_height = ctk.CTkLabel(self.pa_slider_frame, text="Height: 400px", font=FONT_BODY_SMALL, text_color=COLOR_TEXT_SUB)
        self.lbl_pa_height.pack(side="right", padx=(0, 10))

        self.pa_chart_container = ctk.CTkFrame(
            self.inner_pad, fg_color=COLOR_SURFACE,
            border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8,
            height=400
        )
        self.pa_chart_container.pack_propagate(False)

        self.reset_view()

    def reset_view(self):
        self.pack_forget()
        self.state_frame.pack_forget()
        self.grid_frame.pack_forget()
        self.pa_slider_frame.pack_forget()
        self.pa_chart_container.pack_forget()
        self.btn_export_pa.configure(state="disabled")
        self.last_complex_flows = []
        self.last_results = {}
        
        for w in self.state_frame.winfo_children():
            w.destroy()
        for w in self.grid_frame.winfo_children():
            w.destroy()
        for w in self.pa_chart_container.winfo_children():
            w.destroy()

    def _set_state_loading(self, msg="Loading..."):
        for w in self.state_frame.winfo_children():
            w.destroy()
        self.loading_label = __import__("customtkinter").CTkLabel(self.state_frame, text=f"⏳ {msg}", text_color="#6b7280", font=__import__("customtkinter").CTkFont(family="Segoe UI", size=13))
        self.loading_label.pack(pady=(20, 5))
        pb = __import__("customtkinter").CTkProgressBar(self.state_frame, mode="indeterminate", width=250, fg_color="#F3F4F6", progress_color="#2563EB")
        pb.pack(pady=(0, 20))
        pb.start()
        self.state_frame.pack(fill="x", expand=True)

    def _set_state_error(self, error_msg):
        for w in self.state_frame.winfo_children():
            w.destroy()

        display_msg = error_msg
        if "401" in error_msg or "403" in error_msg or "unauthorized" in error_msg.lower() or "forbidden" in error_msg.lower():
            display_msg = "Management Link / Flow read permissions required."

        ctk.CTkLabel(self.state_frame, text=f"✖ {display_msg}", text_color=COLOR_ERROR, font=FONT_BODY_MEDIUM, justify="center").pack(pady=(20, 5))
        ctk.CTkButton(self.state_frame, text="Try Again", command=self._retry_fetch, width=120, fg_color="transparent", border_width=1, text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER).pack(pady=(0, 20))
        self.state_frame.pack(fill="x", expand=True)

    def _retry_fetch(self):
        if hasattr(self, 'reload_btn') and self.reload_btn.winfo_exists():
            self.reload_btn.configure(state="disabled")
        tenant, clients, secrets = self.get_credentials()
        if tenant:
            self.trigger_fetch(tenant, clients[0], secrets[0])

    def trigger_fetch(self, tenant, client_id, client_secret):
        usage_logger.info("Power Automate trigger_fetch called. Spawning background worker thread...")
        self.status = "loading"
        self.on_status_change()
        
        self.pack(fill="x", expand=True, pady=10)
        self.grid_frame.pack_forget()
        self.pa_slider_frame.pack_forget()
        self.pa_chart_container.pack_forget()
        
        self._set_state_loading("Scanning Power Automate flows...")
        
        threading.Thread(
            target=self._execute_worker,
            args=(tenant, client_id, client_secret),
            daemon=True
        ).start()

    def _execute_worker(self, tenant: str, client_id: str, client_secret: str):
        if self.semaphore:
            self.semaphore.acquire()
        try:
            scanner = PowerAutomateScanner(tenant, client_id, client_secret)
            results = scanner.scan_flows()
            usage_logger.info("Successfully completed Power Automate scan.")
            self.after(0, self._render_success, results)
        except Exception as e:
            usage_logger.error("Exception caught in PowerAutomateUsage worker.", exc_info=True)
            self.after(0, self._render_error, str(e))
        finally:
            if self.semaphore:
                self.semaphore.release()

    def _render_success(self, results: dict):
        self.last_results = results
        if hasattr(self, 'reload_btn') and self.reload_btn.winfo_exists():
            self.reload_btn.configure(state="normal")
        self.state_frame.pack_forget()
        for w in self.grid_frame.winfo_children():
            w.destroy()

        self.grid_frame.pack(fill="x", expand=True)

        if not results:
            empty_cell = ctk.CTkFrame(self.grid_frame, fg_color="transparent")
            empty_cell.pack(fill="x", expand=True, pady=15)
            ctk.CTkLabel(empty_cell, text="No Power Automate data found.", text_color=COLOR_TEXT_SUB).pack()
            self.status = "success"
            self.on_status_change()
            return

        total_envs = results.get("total_environments", 0)
        counts = results.get("counts", {})
        active_counts = results.get("active_counts", {})
        tier_counts = results.get("tier_counts", {})
        active_tier_counts = results.get("active_tier_counts", {})
        premium_conns = results.get("premium_connectors", [])
        custom_conns = results.get("custom_connectors", [])
        complex_flows = results.get("complex_logic_flows", [])

        total_flows = counts.get("Cloud Flows", 0) + counts.get("Desktop Flows", 0)

        summary_frame = ctk.CTkFrame(self.grid_frame, fg_color=COLOR_OUTLINE_LIGHT, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)
        summary_frame.pack(fill="x", pady=20)
        
        for i in range(2):
            summary_frame.grid_columnconfigure(i, weight=1)

        headers_pa = ["Metric", "Value"]
        for col_idx, head_text in enumerate(headers_pa):
            cell = ctk.CTkFrame(summary_frame, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        prem_str = ", ".join(premium_conns) if premium_conns else "0"
        cust_str = ", ".join(custom_conns) if custom_conns else "0"

        mapping = [
            ("Total Environments Scanned", total_envs),
            ("Total Flows (Active + Inactive)", total_flows),
            ("Premium Connectors In Use", prem_str),
            ("Custom Connectors In Use", cust_str),
        ]

        r_idx = 1
        for label, val in mapping:
            bg_style = COLOR_SURFACE if r_idx % 2 == 0 else COLOR_SURFACE_VARIANT
            
            c0 = ctk.CTkFrame(summary_frame, fg_color=bg_style, corner_radius=0)
            c0.grid(row=r_idx, column=0, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c0, text=label, font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=6, anchor="nw")

            c1 = ctk.CTkFrame(summary_frame, fg_color=bg_style, corner_radius=0)
            c1.grid(row=r_idx, column=1, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c1, text=str(val), font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, wraplength=400).pack(padx=10, pady=6, anchor="nw")
            
            r_idx += 1

        complex_flows_path = results.get("complex_logic_flows_path")
        self.last_complex_flows_path = complex_flows_path
        if complex_flows_path and os.path.exists(complex_flows_path) and os.path.getsize(complex_flows_path) > 0:
            self.btn_export_pa.configure(state="normal")
        else:
            self.btn_export_pa.configure(state="disabled")

        if total_flows > 0:
            self.pa_slider_frame.pack(fill="x", pady=(10, 0))
            self.pa_chart_container.pack(fill="x", pady=(5, 20))
            for w in self.pa_chart_container.winfo_children():
                w.destroy()
            
            if not MATPLOTLIB_AVAILABLE:
                ctk.CTkLabel(self.pa_chart_container, text="Matplotlib is required to render charts.\nPlease install it using 'pip install matplotlib'.", text_color=COLOR_ERROR).pack(pady=15)
            else:
                try:
                    fig = Figure(figsize=(10, 4), dpi=100)
                    ax = fig.add_subplot(111)
                    fig.patch.set_facecolor(COLOR_SURFACE)
                    ax.set_facecolor(COLOR_SURFACE)
                    
                    categories = ['Cloud Flows', 'Desktop Flows', 'Personal Flows', 'Enterprise Flows', 'Complex Flows']
                    
                    c_total = counts.get("Cloud Flows", 0)
                    c_active = active_counts.get("Cloud Flows", 0)
                    c_inactive = c_total - c_active
                    
                    d_total = counts.get("Desktop Flows", 0)
                    d_active = active_counts.get("Desktop Flows", 0)
                    d_inactive = d_total - d_active
                    
                    p_total = tier_counts.get("Personal Productivity", 0)
                    p_active = active_tier_counts.get("Personal Productivity", 0)
                    p_inactive = p_total - p_active
                    
                    e_total = tier_counts.get("Enterprise/Departmental", 0)
                    e_active = active_tier_counts.get("Enterprise/Departmental", 0)
                    e_inactive = e_total - e_active
                    
                    complex_active = results.get("complex_active_count", 0)
                    complex_inactive = results.get("complex_inactive_count", 0)
                    
                    actives = [c_active, d_active, p_active, e_active, complex_active]
                    inactives = [c_inactive, d_inactive, p_inactive, e_inactive, complex_inactive]
                    
                    x = range(len(categories))
                    width = 0.15
                    
                    color_active = COLOR_PRIMARY
                    color_inactive = COLOR_TONAL_BG
                    
                    rects1 = ax.bar(x, actives, width, label='Active', color=color_active)
                    rects2 = ax.bar([i + width for i in x], inactives, width, label='Inactive', color=color_inactive)
                    
                    ax.set_ylabel('Count', color=COLOR_TEXT_MAIN, fontsize=10, fontweight='bold')
                    ax.set_title('Power Automate Flows Breakdown', color=COLOR_TEXT_MAIN, fontsize=12, fontweight='bold')
                    ax.set_xticks([i + width/2 for i in x])
                    ax.set_xticklabels(categories, color=COLOR_TEXT_MAIN, fontsize=10, fontweight='bold')
                    ax.legend(facecolor=COLOR_SURFACE, edgecolor=COLOR_OUTLINE_LIGHT, labelcolor=COLOR_TEXT_MAIN, prop={'weight':'bold', 'size':9})
                    
                    ax.bar_label(rects1, padding=3, color=COLOR_TEXT_MAIN, fontsize=9, fontweight='bold')
                    ax.bar_label(rects2, padding=3, color=COLOR_TEXT_MAIN, fontsize=9, fontweight='bold')
                    
                    for spine in ax.spines.values():
                        spine.set_color(COLOR_OUTLINE_LIGHT)
                    
                    ax.tick_params(axis='y', colors=COLOR_TEXT_MAIN, labelsize=9)
                    for label in ax.get_yticklabels():
                        label.set_fontweight('bold')
                    
                    max_val = max(max(actives), max(inactives))
                    ax.set_ylim(0, max(max_val + 3, int(max_val * 1.3)))
                    
                    fig.tight_layout()

                    canvas = FigureCanvasTkAgg(fig, master=self.pa_chart_container)
                    canvas.draw()
                    canvas.get_tk_widget().pack(fill="both", expand=True, padx=20, pady=10)
                    
                except Exception as e:
                    usage_logger.error(f"Error drawing Power Automate charts: {e}", exc_info=True)

        self.status = "success"
        self.on_status_change()

    def _render_error(self, err_msg):
        usage_logger.warning(f"Power Automate fetch failed: {err_msg}")
        if hasattr(self, 'reload_btn') and self.reload_btn.winfo_exists():
            self.reload_btn.configure(state="normal")
        self._set_state_error(err_msg)
        self.status = "error"
        self.on_status_change()

    def export_complex_flows(self):
        usage_logger.info("Exporting complex flows to local spreadsheet requested.")
        if not hasattr(self, "last_complex_flows_path") or not self.last_complex_flows_path:
            return

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        f = filedialog.asksaveasfilename(
            initialfile=f"complex_flows_{ts}.csv",
            defaultextension=".csv",
            filetypes=[("CSV Spreadsheet", "*.csv")]
        )

        if not f:
            return

        headers = ["Environment", "Name", "Type", "Tier", "Active", "Reason"]

        try:
            import pandas as pd
            df_iter = pd.read_json(self.last_complex_flows_path, lines=True, chunksize=1000)
            for i, chunk in enumerate(df_iter):
                chunk = chunk[headers]
                chunk.to_csv(f, mode='a' if i > 0 else 'w', header=(i == 0), index=False, encoding='utf-8')
            usage_logger.info("Complex flows exported successfully.")
            messagebox.showinfo("Export Successful", f"Complex flows successfully saved to:\n{f}", parent=self)
        except Exception as e:
            usage_logger.error("Failed writing export spreadsheet to disk.", exc_info=True)
            messagebox.showerror("Export Error", f"Failed to save file:\n{e}", parent=self)

    def _on_pa_height_slider_change(self, val):
        height_val = int(val)
        self.lbl_pa_height.configure(text=f"Height: {height_val}px")
        self.pa_chart_container.configure(height=height_val)
