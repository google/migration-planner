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

"""Modular Network Security telemetry scanner and visual interface."""

import os
import time
import logging
import threading
import pandas as pd
import customtkinter as ctk
from tkinter import filedialog, messagebox
from telemetry.styles import *
from core.graph.network_security import NetworkSecurityService
from core.graph.client import GraphClient

usage_logger = logging.getLogger("M365TelemetryAsyncLogger.NetworkSecurityUI")

class NetworkSecurityFrame(ctk.CTkFrame):
    """CustomTkinter component wrapping Network Security UI, individual retries, and exports."""

    def __init__(self, master, log_callback, credentials_callback, status_change_callback, **kwargs):
        self.semaphore = kwargs.pop("concurrency_semaphore", None)
        super().__init__(master, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=12, **kwargs)
        self.log_msg = log_callback
        self.get_credentials = credentials_callback
        self.on_status_change = status_change_callback
        self.status = None # Global status tracking for coordinator batch flow
        
        # Sub-sections status
        self.filtering_status = None
        self.ca_status = None
        self.fw_status = None
        
        # Sub-sections start times
        self.filtering_start_time = 0
        self.ca_start_time = 0
        self.fw_start_time = 0
        
        # Timer labels
        self.lbl_timer_filtering = None
        self.lbl_timer_ca = None
        self.lbl_timer_fw = None
        
        # Pagination states
        self.filtering_page = 0
        self.ca_page = 0
        self.fw_page = 0
        self.ITEMS_PER_PAGE = 5
        
        self.build_ui()

    def build_ui(self):
        self.pack(fill="x", expand=True, pady=10)
        
        self.inner_pad = ctk.CTkFrame(self, fg_color="transparent")
        self.inner_pad.pack(fill="both", expand=True, padx=20, pady=20)
        
        # Global Section Title
        self.header_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.header_frame.pack(fill="x", pady=(0, 15))
        
        self.title_lbl = ctk.CTkLabel(
            self.header_frame,
            text="Network Security Policies & Access Controls",
            font=FONT_HEADER_SMALL,
            text_color=COLOR_TEXT_MAIN
        )
        self.title_lbl.pack(side="left", anchor="w")
        
        self.body_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.body_frame.pack(fill="x", expand=True)
        
        self.reset_view()

    def reset_view(self):
        self.pack_forget()
        self.body_frame.pack_forget()
        for w in self.body_frame.winfo_children():
            w.destroy()
        
        self.filtering_status = None
        self.ca_status = None
        self.fw_status = None
        self.status = None
        
        # Reset timer references
        self.lbl_timer_filtering = None
        self.lbl_timer_ca = None
        self.lbl_timer_fw = None

    def trigger_fetch(self, tenant, client_id, client_secret):
        """Sequential/Parallel initiator for coordinator batch run."""
        usage_logger.info("Network Security trigger_fetch called.")
        self.status = "loading"
        self.on_status_change()
        
        self.pack(fill="x", expand=True, pady=10)
        self.body_frame.pack(fill="x", expand=True)
        
        # Setup layouts for each subsection
        self._build_sections_layout()
        
        # Trigger background execution for all 3 tables
        self._retry_filtering_fetch()
        self._retry_ca_fetch()
        self._retry_fw_fetch()

    def _check_subsections_done(self):
        """Checks if all subsections are finished loading to notify the coordinator batch flow."""
        states = [self.filtering_status, self.ca_status, self.fw_status]
        if "loading" in states:
            self.status = "loading"
        elif "error" in states:
            self.status = "error"
            self.on_status_change()
        else:
            self.status = "success"
            self.on_status_change()

    # =========================================================================
    # RE-FETCH & WORKERS PER TABLE
    # =========================================================================
    def _retry_filtering_fetch(self):
        tenant, clients, secrets = self.get_credentials()
        if not tenant: return
        self.filtering_status = "loading"
        self.filtering_start_time = time.time()
        self.btn_reload_filtering.configure(state="disabled")
        self.btn_export_filtering.configure(state="disabled")
        self._set_loading_state(self.filtering_state_frame, "Scanning Entra filtering policies...")
        self._check_subsections_done()
        
        # Clear previous timer if exists
        if self.lbl_timer_filtering and self.lbl_timer_filtering.winfo_exists():
            self.lbl_timer_filtering.destroy()
            self.lbl_timer_filtering = None
            
        def run():
            if self.semaphore: self.semaphore.acquire()
            try:
                client = GraphClient(tenant_id=tenant, client_ids=clients[0], client_secrets=secrets[0])
                client.authenticate()
                service = NetworkSecurityService(client)
                
                script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
                csv_path = os.path.join(script_dir, "reports", f"{tenant}_{clients[0]}", "network_filtering_policies.csv")
                service.fetch_filtering_policies(csv_path)
                
                self.filtering_status = "success"
                self.after(0, lambda: self._render_subsection_success("filtering"))
            except Exception as e:
                usage_logger.error(f"Filtering fetch error: {e}", exc_info=True)
                self.filtering_status = "error"
                self.after(0, lambda: self._render_subsection_error("filtering", str(e)))
            finally:
                if self.semaphore: self.semaphore.release()
                self.after(0, self._check_subsections_done)
                
        threading.Thread(target=run, daemon=True).start()

    def _retry_ca_fetch(self):
        tenant, clients, secrets = self.get_credentials()
        if not tenant: return
        self.ca_status = "loading"
        self.ca_start_time = time.time()
        self.btn_reload_ca.configure(state="disabled")
        self.btn_export_ca.configure(state="disabled")
        self._set_loading_state(self.ca_state_frame, "Scanning Conditional Access network scope...")
        self._check_subsections_done()
        
        # Clear previous timer if exists
        if self.lbl_timer_ca and self.lbl_timer_ca.winfo_exists():
            self.lbl_timer_ca.destroy()
            self.lbl_timer_ca = None
            
        def run():
            if self.semaphore: self.semaphore.acquire()
            try:
                client = GraphClient(tenant_id=tenant, client_ids=clients[0], client_secrets=secrets[0])
                client.authenticate()
                service = NetworkSecurityService(client)
                
                script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
                csv_path = os.path.join(script_dir, "reports", f"{tenant}_{clients[0]}", "network_conditional_access.csv")
                service.fetch_conditional_access_policies(csv_path)
                
                self.ca_status = "success"
                self.after(0, lambda: self._render_subsection_success("ca"))
            except Exception as e:
                usage_logger.error(f"CA fetch error: {e}", exc_info=True)
                self.ca_status = "error"
                self.after(0, lambda: self._render_subsection_error("ca", str(e)))
            finally:
                if self.semaphore: self.semaphore.release()
                self.after(0, self._check_subsections_done)
                
        threading.Thread(target=run, daemon=True).start()

    def _retry_fw_fetch(self):
        tenant, clients, secrets = self.get_credentials()
        if not tenant: return
        self.fw_status = "loading"
        self.fw_start_time = time.time()
        self.btn_reload_fw.configure(state="disabled")
        self.btn_export_fw.configure(state="disabled")
        self._set_loading_state(self.fw_state_frame, "Scanning Intune Firewall and Proxy configurations...")
        self._check_subsections_done()
        
        # Clear previous timer if exists
        if self.lbl_timer_fw and self.lbl_timer_fw.winfo_exists():
            self.lbl_timer_fw.destroy()
            self.lbl_timer_fw = None
            
        def run():
            if self.semaphore: self.semaphore.acquire()
            try:
                client = GraphClient(tenant_id=tenant, client_ids=clients[0], client_secrets=secrets[0])
                client.authenticate()
                service = NetworkSecurityService(client)
                
                script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
                csv_path = os.path.join(script_dir, "reports", f"{tenant}_{clients[0]}", "network_firewall_policies.csv")
                service.fetch_firewall_policies(csv_path)
                
                self.fw_status = "success"
                self.after(0, lambda: self._render_subsection_success("fw"))
            except Exception as e:
                usage_logger.error(f"Firewall fetch error: {e}", exc_info=True)
                self.fw_status = "error"
                self.after(0, lambda: self._render_subsection_error("fw", str(e)))
            finally:
                if self.semaphore: self.semaphore.release()
                self.after(0, self._check_subsections_done)
                
        threading.Thread(target=run, daemon=True).start()

    # =========================================================================
    # RENDER SECTION STATE HANDLERS
    # =========================================================================
    def _set_loading_state(self, parent_frame, msg):
        for w in parent_frame.winfo_children():
            w.destroy()
        parent_frame.pack(fill="x", expand=True)
        ctk.CTkLabel(parent_frame, text=f"⏳ {msg}", text_color=COLOR_TEXT_SUB, font=FONT_BODY_MEDIUM).pack(pady=(15, 5))
        pb = ctk.CTkProgressBar(parent_frame, mode="indeterminate", width=200, fg_color=COLOR_SURFACE_VARIANT, progress_color=COLOR_PRIMARY)
        pb.pack(pady=(0, 15))
        pb.start()

    def _render_subsection_success(self, mode):
        # Draw subsection timer
        start_time = getattr(self, f"{mode}_start_time")
        if start_time > 0:
            elapsed = time.time() - start_time
            header_frame = getattr(self, f"{mode}_header_frame")
            
            # Destroy old if exists
            old_timer = getattr(self, f"lbl_timer_{mode}")
            if old_timer and old_timer.winfo_exists():
                old_timer.destroy()
                
            timer_lbl = ctk.CTkLabel(
                self,
                text=f"⏱ {elapsed:.2f}s",
                font=ctk.CTkFont(family="Segoe UI", size=11, weight="bold"),
                text_color=COLOR_PRIMARY
            )
            timer_lbl.pack(in_=header_frame, side="right", padx=(0, 15))
            setattr(self, f"lbl_timer_{mode}", timer_lbl)

        if mode == "filtering":
            self.filtering_state_frame.pack_forget()
            self.filtering_grid.pack(fill="x")
            self.btn_reload_filtering.configure(state="normal")
            self.btn_export_filtering.configure(state="normal")
            self.filtering_page = 0
            self._update_grid_generic("network_filtering_policies.csv", 0, self.filtering_grid, 
                                      ["Policy Name", "Description", "Version", "Action", "Rules Count"],
                                      [2, 3, 1, 1, 1], ["name", "description", "version", "action", "rules_count"],
                                      "filtering_page")
        elif mode == "ca":
            self.ca_state_frame.pack_forget()
            self.ca_grid.pack(fill="x")
            self.btn_reload_ca.configure(state="normal")
            self.btn_export_ca.configure(state="normal")
            self.ca_page = 0
            self._update_grid_generic("network_conditional_access.csv", 0, self.ca_grid,
                                      ["Policy Name", "State", "Target Users", "Target Apps", "Grant Controls"],
                                      [3, 1, 2, 2, 2], ["name", "state", "target_users", "target_apps", "controls"],
                                      "ca_page")
        elif mode == "fw":
            self.fw_state_frame.pack_forget()
            self.fw_grid.pack(fill="x")
            self.btn_reload_fw.configure(state="normal")
            self.btn_export_fw.configure(state="normal")
            self.fw_page = 0
            self._update_grid_generic("network_firewall_policies.csv", 0, self.fw_grid,
                                      ["Configuration Name", "Policy Type", "Firewall Status", "Proxy Status"],
                                      [3, 2, 1, 1], ["name", "policy_type", "firewall_status", "proxy_status"],
                                      "fw_page")

    def _render_subsection_error(self, mode, err_msg):
        frame = getattr(self, f"{mode}_state_frame")
        grid = getattr(self, f"{mode}_grid")
        reload_btn = getattr(self, f"btn_reload_{mode}")
        
        grid.pack_forget()
        reload_btn.configure(state="normal")
        
        for w in frame.winfo_children():
            w.destroy()
        frame.pack(fill="x", expand=True)
        
        ctk.CTkLabel(frame, text=f"✖ {err_msg}", text_color=COLOR_ERROR, font=FONT_BODY_MEDIUM, justify="center", wraplength=700).pack(pady=(15, 5))
        retry_fn = getattr(self, f"_retry_{mode}_fetch")
        ctk.CTkButton(frame, text="Try Again", command=retry_fn, width=100, fg_color="transparent", border_width=1, text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER).pack(pady=(0, 15))

    # =========================================================================
    # EXPORT LOGIC PER TABLE
    # =========================================================================
    def _export_filtering(self):
        self._export_table_csv("network_filtering_policies.csv", "M365_Secure_Access_Filtering.csv", "Filtering Policies")

    def _export_ca(self):
        self._export_table_csv("network_conditional_access.csv", "M365_CA_Network_Access.csv", "Conditional Access Policies")

    def _export_fw(self):
        self._export_table_csv("network_firewall_policies.csv", "M365_Firewall_Proxy_Configs.csv", "Firewall & Proxy Configurations")

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

    # =========================================================================
    # BUILD SUBSECTIONS UI LAYOUTS
    # =========================================================================
    def _build_sections_layout(self):
        for w in self.body_frame.winfo_children():
            w.destroy()
            
        # 1. Filtering policies layouts
        self.filtering_header_frame, self.filtering_state_frame, self.filtering_grid, self.btn_reload_filtering, self.btn_export_filtering = self._create_subsection_scaffold(
            parent=self.body_frame,
            title="1. Filtering Policies",
            export_text="Export Filtering Policies",
            headers=["Policy Name", "Description", "Version", "Action", "Rules Count"],
            weights=[2, 3, 1, 1, 1],
            reload_cmd=self._retry_filtering_fetch,
            export_cmd=self._export_filtering
        )

        # 2. CA policies layouts
        self.ca_header_frame, self.ca_state_frame, self.ca_grid, self.btn_reload_ca, self.btn_export_ca = self._create_subsection_scaffold(
            parent=self.body_frame,
            title="2. Conditional Access Policies",
            export_text="Export Conditional Access",
            headers=["Policy Name", "State", "Target Users", "Target Apps", "Grant Controls"],
            weights=[3, 1, 2, 2, 2],
            reload_cmd=self._retry_ca_fetch,
            export_cmd=self._export_ca
        )

        # 3. Firewall/Proxy layouts
        self.fw_header_frame, self.fw_state_frame, self.fw_grid, self.btn_reload_fw, self.btn_export_fw = self._create_subsection_scaffold(
            parent=self.body_frame,
            title="3. Firewall and Proxy Configurations",
            export_text="Export Firewall & Proxy Configs",
            headers=["Configuration Name", "Policy Type", "Firewall Status", "Proxy Status"],
            weights=[3, 2, 1, 1],
            reload_cmd=self._retry_fw_fetch,
            export_cmd=self._export_fw
        )

    def _create_subsection_scaffold(self, parent, title, export_text, headers, weights, reload_cmd, export_cmd):
        sec_frame = ctk.CTkFrame(parent, fg_color="transparent")
        sec_frame.pack(fill="x", pady=(10, 15))
        
        # Sub-header Row
        sub_header = ctk.CTkFrame(sec_frame, fg_color="transparent")
        sub_header.pack(fill="x", pady=(0, 10))
        
        title_lbl = ctk.CTkLabel(sub_header, text=title, font=FONT_HEADER_SMALL, text_color=COLOR_TEXT_MAIN)
        title_lbl.pack(side="left")
        
        # Reload Button (↻ Reload) - Sized smaller to match original dashboards: width=80, height=24, square-ish corner radius
        reload_btn = ctk.CTkButton(
            sub_header, text="↻ Reload", width=80, height=24,
            font=ctk.CTkFont(family="Segoe UI", size=12),
            fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color="#2563EB", hover_color="#DBEAFE",
            command=reload_cmd
        )
        reload_btn.pack(side="right", padx=(10, 0))
        
        # Export Button - Sized pill-like: width=180 or length-specific, height=32, corner_radius=16, FONT_BODY_BOLD
        export_btn = ctk.CTkButton(
            sub_header, text=export_text, width=200, height=32, corner_radius=16,
            font=FONT_BODY_BOLD, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER,
            command=export_cmd
        )
        export_btn.pack(side="right")

        # Scaffold Frame elements
        state_frame = ctk.CTkFrame(sec_frame, fg_color="transparent")
        
        grid_frame = ctk.CTkFrame(sec_frame, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)
        
        for col_idx, weight in enumerate(weights):
            grid_frame.grid_columnconfigure(col_idx, weight=weight)
            
        for col_idx, head_text in enumerate(headers):
            cell = ctk.CTkFrame(grid_frame, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=1, pady=1)
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        return sub_header, state_frame, grid_frame, reload_btn, export_btn

    # =========================================================================
    # PAGINATION GENERAL POPULATION (Centered inside Grid row)
    # =========================================================================
    def _load_page_from_csv(self, filename, page):
        script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
        tenant, clients, _ = self.get_credentials()
        if not tenant or not clients: return [], 0
        csv_path = os.path.join(script_dir, "reports", f"{tenant}_{clients[0]}", filename)
        if not os.path.exists(csv_path): return [], 0

        try:
            df = pd.read_csv(csv_path)
            df = df.fillna("N/A")
            total_count = len(df)
            start_idx = page * self.ITEMS_PER_PAGE
            end_idx = start_idx + self.ITEMS_PER_PAGE
            page_data = df.iloc[start_idx:end_idx].to_dict('records')
            return page_data, total_count
        except Exception as e:
            usage_logger.error(f"Error reading CSV {filename} via Pandas: {e}")
            return [], 0

    def _update_grid_generic(self, filename, page, grid_frame, headers, col_weights, col_keys, page_attr):
        # Clear previous data rows and controls (rows > 0)
        for w in grid_frame.winfo_children():
            info = w.grid_info()
            if "row" in info and int(info["row"]) > 0:
                w.destroy()

        page_data, total_count = self._load_page_from_csv(filename, page)
        total_pages = (total_count - 1) // self.ITEMS_PER_PAGE + 1 if total_count > 0 else 1
        
        # Draw rows
        for row_idx, item in enumerate(page_data, start=1):
            for col_idx, key in enumerate(col_keys):
                cell = ctk.CTkFrame(grid_frame, fg_color="transparent", corner_radius=0)
                cell.grid(row=row_idx, column=col_idx, sticky="nsew", padx=1, pady=1)
                val = item.get(key, "N/A")
                lbl = ctk.CTkLabel(cell, text=str(val), font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, wraplength=220, justify="left", anchor="w")
                lbl.pack(padx=10, pady=6, fill="x")

        # Draw pagination controls centered inside a final spanning grid row
        control_frame = ctk.CTkFrame(grid_frame, fg_color="transparent")
        control_frame.grid(row=self.ITEMS_PER_PAGE + 1, column=0, columnspan=len(col_keys), pady=0, sticky="ew")

        center_container = ctk.CTkFrame(control_frame, fg_color="transparent")
        center_container.pack(pady=(5, 10))

        prev_state = "normal" if page > 0 else "disabled"
        btn_prev = ctk.CTkButton(
            center_container, text="◀ Prev", width=70, height=22, corner_radius=6,
            font=FONT_BODY_SMALL, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER, state=prev_state,
            command=lambda: self._change_page(page_attr, -1, filename, grid_frame, headers, col_weights, col_keys)
        )
        btn_prev.pack(side="left", padx=5)

        page_lbl = ctk.CTkLabel(
            center_container, 
            text=f"Page {page + 1} of {total_pages}", 
            font=FONT_BODY_MEDIUM, 
            text_color=COLOR_TEXT_SUB
        )
        page_lbl.pack(side="left", padx=15)

        next_state = "normal" if page < total_pages - 1 else "disabled"
        btn_next = ctk.CTkButton(
            center_container, text="Next ▶", width=70, height=22, corner_radius=6,
            font=FONT_BODY_SMALL, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER, state=next_state,
            command=lambda: self._change_page(page_attr, 1, filename, grid_frame, headers, col_weights, col_keys)
        )
        btn_next.pack(side="left", padx=5)

    def _change_page(self, page_attr, delta, filename, grid_frame, headers, col_weights, col_keys):
        current_page = getattr(self, page_attr)
        new_page = current_page + delta
        setattr(self, page_attr, new_page)
        self._update_grid_generic(filename, new_page, grid_frame, headers, col_weights, col_keys, page_attr)
