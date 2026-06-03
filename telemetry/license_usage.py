import json
import csv
import threading
import os
import queue
import logging
import base64
import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from logging.handlers import QueueHandler, QueueListener
from datetime import datetime
from tkinter import filedialog, messagebox
from typing import Any, Dict, List, Optional
import customtkinter as ctk

from telemetry import active_users_usage as usage
from telemetry.sharepoint_onedrive_usage import SharePointUsageFrame, OneDriveUsageFrame
from telemetry.data_security_governance import DataSecurityGovernanceFrame
from telemetry.power_automate import PowerAutomateScanner

# Import unified core service layer
from core.graph.client import GraphClient
from core.graph.directory import DirectoryService

# Safely import matplotlib to embed plots in Tkinter
try:
    import matplotlib.pyplot as plt
    from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False


# =================================================================================
# ASYNC FILE LOGGING SETUP
# =================================================================================

_current_dir = os.path.dirname(os.path.abspath(__file__))
_log_dir = os.path.join(_current_dir, 'logs')
os.makedirs(_log_dir, exist_ok=True)
_log_file_path = os.path.join(_log_dir, 'license_log.txt')

_log_queue = queue.Queue()
_file_handler = logging.FileHandler(_log_file_path, mode='a', encoding='utf-8')
_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
_file_handler.setFormatter(_formatter)

_queue_listener = QueueListener(_log_queue, _file_handler)
_queue_listener.start()

# Configure the root logger to send all records to the log queue and clear other handlers (stdout/stderr)
root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)
for h in list(root_logger.handlers):
    root_logger.removeHandler(h)
root_logger.addHandler(QueueHandler(_log_queue))

async_logger = logging.getLogger("LicenseUsageAsyncLogger")
async_logger.setLevel(logging.DEBUG)
# Note: Since the root logger now routes everything to queue, async_logger can simply propagate.
async_logger.propagate = True


# =================================================================================
# CONSTANTS
# =================================================================================

GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"

from telemetry.styles import *


class LicenseUsageTab(ctk.CTkScrollableFrame):
    """Encapsulates the UI and API logic for the Licenses & Usage tab."""

    def __init__(self, master, log_callback, retries_var, backoff_var, **kwargs):
        super().__init__(
            master,
            fg_color="transparent",
            scrollbar_button_color="white",
            scrollbar_button_hover_color=COLOR_SECONDARY_HOVER,
            **kwargs
        )
        async_logger.info("Initializing LicenseUsageTab instance.")

        self.log_msg = log_callback
        self.retries = retries_var
        self.backoff = backoff_var

        self.lic_tenant_id = ctk.StringVar()
        self.lic_client_ids = ctk.StringVar()
        self.lic_client_secrets = ctk.StringVar()

        self.last_licenses_items = []
        self.on_all_done_callback = None

        # Track individual section statuses ('loading', 'success', 'error', None)
        self.status_sku = None
        self.status_o365 = None
        self.status_o365_trend = None
        self.status_m365 = None
        self.status_pa = None
        self.status_sharepoint = None
        self.status_onedrive = None

        self.build_ui()

        # Bind mouse wheel globally to scroll this tab when hovered
        self.bind_all("<MouseWheel>", self._handle_global_mousewheel, add="+")
        self.bind_all("<Button-4>", self._handle_global_mousewheel, add="+")
        self.bind_all("<Button-5>", self._handle_global_mousewheel, add="+")

    def _create_entry(self, parent, label, var, show=None):
        f = ctk.CTkFrame(parent, fg_color="transparent")
        f.pack(fill="x", pady=5)
        ctk.CTkLabel(f, text=label, width=100, anchor="w", text_color=COLOR_TEXT_SUB).pack(side="left")
        ctk.CTkEntry(
            f, textvariable=var, show=show, height=40, corner_radius=4,
            border_width=1, border_color=COLOR_OUTLINE, fg_color="transparent",
            text_color=COLOR_TEXT_MAIN,
        ).pack(side="left", fill="x", expand=True)

    def build_ui(self):
        async_logger.info("Building graphical UI elements for License Usage Tab.")

        ctk.CTkLabel(self, text="Microsoft 365 Subscribed SKUs & Usage Overview", font=FONT_HEADER_MEDIUM, text_color=COLOR_TEXT_MAIN).pack(anchor="w", pady=(0, 5))
        ctk.CTkLabel(self, text="Connect your Microsoft Azure account to authenticate and audit tenant licensing bundle inventories and usage.", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB).pack(anchor="w", pady=(0, 15))

        self.inputs_frame = ctk.CTkFrame(self, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)
        self.inputs_frame.pack(fill="x", pady=5)

        inner_pad = ctk.CTkFrame(self.inputs_frame, fg_color="transparent")
        inner_pad.pack(fill="x", padx=15, pady=15)

        self._create_entry(inner_pad, "Tenant ID", self.lic_tenant_id)
        self._create_entry(inner_pad, "Client ID", self.lic_client_ids)
        self._create_entry(inner_pad, "Client Secret", self.lic_client_secrets, show="*")


        actions_frame = ctk.CTkFrame(self, fg_color="transparent")
        actions_frame.pack(fill="x", pady=(20, 10))

        self.btn_lic_submit = ctk.CTkButton(
            actions_frame, text="Submit", width=160, height=40, corner_radius=20,
            font=FONT_BODY_BOLD, fg_color=COLOR_PRIMARY, hover_color=COLOR_PRIMARY_HOVER,
            command=self.authenticate_licenses_tab,
        )
        self.btn_lic_submit.pack(side="left")

        self.lbl_lic_status = ctk.CTkLabel(actions_frame, text="", font=FONT_BODY_MEDIUM)
        self.lbl_lic_status.pack(side="left", padx=20)

        # ----------------------------------------------------
        # UI CONTAINERS
        # ----------------------------------------------------

        # 1. SKUs Section
        self.lic_section = ctk.CTkFrame(self, fg_color="transparent")
        self.lic_section.pack(fill="x", expand=True, pady=(15, 10))

        lic_header = ctk.CTkFrame(self.lic_section, fg_color="transparent")
        lic_header.pack(fill="x", pady=(0, 10))
        ctk.CTkLabel(lic_header, text="Subscribed SKUs Inventory Summary", font=FONT_HEADER_SMALL, text_color=COLOR_TEXT_MAIN).pack(side="left")
        ctk.CTkLabel(lic_header, text="* To view specific services offered, export the spreadsheet.", font=FONT_BODY_SMALL, text_color=COLOR_TEXT_SUB).pack(side="left", padx=(10, 0))

        self.btn_export_lic = ctk.CTkButton(lic_header, text="Export Spreadsheet", width=140, height=32, corner_radius=16, font=FONT_BODY_BOLD, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE, text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER, command=self.export_licenses_spreadsheet, state="disabled")
        self.btn_export_lic.pack(side="right")

        self.lic_state_frame = ctk.CTkFrame(self.lic_section, fg_color="transparent")
        self.lic_grid_frame = ctk.CTkFrame(self.lic_section, fg_color=COLOR_OUTLINE_LIGHT, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)

        # 2. O365 Usage Section
        self.o365_section = ctk.CTkFrame(self, fg_color="transparent")
        self.o365_section.pack(fill="x", expand=True, pady=(20, 10))
        ctk.CTkLabel(self.o365_section, text="O365 Active Users Usage", font=FONT_HEADER_SMALL, text_color=COLOR_TEXT_MAIN).pack(anchor="w", pady=(0, 10))
        self.o365_state_frame = ctk.CTkFrame(self.o365_section, fg_color="transparent")
        self.o365_grid_frame = ctk.CTkFrame(self.o365_section, fg_color=COLOR_OUTLINE_LIGHT, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)

        # 3. O365 Usage Trend Graph Section
        self.o365_trend_section = ctk.CTkFrame(self, fg_color="transparent")
        self.o365_trend_section.pack(fill="x", expand=True, pady=(20, 10))
        ctk.CTkLabel(self.o365_trend_section, text="O365 30-Day Active User Trend", font=FONT_HEADER_SMALL, text_color=COLOR_TEXT_MAIN).pack(anchor="w", pady=(0, 10))
        self.o365_trend_state_frame = ctk.CTkFrame(self.o365_trend_section, fg_color="transparent")
        self.o365_trend_grid_frame = ctk.CTkFrame(self.o365_trend_section, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)

        # 4. M365 Apps Usage Section
        self.m365_section = ctk.CTkFrame(self, fg_color="transparent")
        self.m365_section.pack(fill="x", expand=True, pady=(20, 10))
        ctk.CTkLabel(self.m365_section, text="M365 App Usage (180 Days)", font=FONT_HEADER_SMALL, text_color=COLOR_TEXT_MAIN).pack(anchor="w", pady=(0, 10))
        self.m365_state_frame = ctk.CTkFrame(self.m365_section, fg_color="transparent")
        self.m365_grid_frame = ctk.CTkFrame(self.m365_section, fg_color=COLOR_OUTLINE_LIGHT, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)

        # 5a. SharePoint Telemetry Section (Modular Integration)
        self.sharepoint_view = SharePointUsageFrame(
            master=self,
            log_callback=self.log_msg,
            credentials_callback=self._get_credentials,
            status_change_callback=self._check_all_done
        )

        # 5b. OneDrive Telemetry Section (Modular Integration)
        self.onedrive_view = OneDriveUsageFrame(
            master=self,
            log_callback=self.log_msg,
            credentials_callback=self._get_credentials,
            status_change_callback=self._check_all_done
        )

        # 6. Data Security & Governance Section (Modular Integration)
        self.security_gov_view = DataSecurityGovernanceFrame(
            master=self,
            log_callback=self.log_msg,
            credentials_callback=self._get_credentials,
            status_change_callback=self._check_all_done
        )

        # 7. Power Automate Section
        self.pa_section = ctk.CTkFrame(self, fg_color="transparent")
        self.pa_section.pack(fill="x", expand=True, pady=(20, 10))
        pa_header = ctk.CTkFrame(self.pa_section, fg_color="transparent")
        pa_header.pack(fill="x", pady=(0, 10))
        ctk.CTkLabel(pa_header, text="Power Automate", font=FONT_HEADER_SMALL, text_color=COLOR_TEXT_MAIN).pack(side="left")
        
        self.btn_export_pa = ctk.CTkButton(
            pa_header, text="Export Complex Flows", width=160, height=32, corner_radius=16,
            font=FONT_BODY_BOLD, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER,
            command=self.export_complex_flows, state="disabled"
        )
        self.btn_export_pa.pack(side="right")
        self.pa_state_frame = ctk.CTkFrame(self.pa_section, fg_color="transparent")
        self.pa_grid_frame = ctk.CTkFrame(self.pa_section, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)


        self._hide_all_grids()

    # -------------------------------------------------------------------------
    # STATE MANAGEMENT HELPERS
    # -------------------------------------------------------------------------
    def _hide_all_grids(self):
        self.lic_section.pack_forget()
        self.o365_section.pack_forget()
        self.o365_trend_section.pack_forget()
        self.m365_section.pack_forget()
        self.sharepoint_view.reset_view()
        self.onedrive_view.reset_view()
        self.security_gov_view.reset_view()
        self.pa_section.pack_forget()

        for grid in [self.lic_grid_frame, self.o365_grid_frame, self.o365_trend_grid_frame, self.m365_grid_frame, self.pa_grid_frame]:
            for w in grid.winfo_children():
                w.destroy()

    def _set_state_loading(self, state_frame, msg="Loading..."):
        for w in state_frame.winfo_children(): w.destroy()
        ctk.CTkLabel(state_frame, text=f"⏳ {msg}", text_color=COLOR_TEXT_SUB, font=FONT_BODY_MEDIUM).pack(pady=20)
        state_frame.pack(fill="x", expand=True)

    def _set_state_error(self, state_frame, error_msg, retry_command):
        for w in state_frame.winfo_children(): w.destroy()
        ctk.CTkLabel(state_frame, text=f"✖ {error_msg}", text_color=COLOR_ERROR, font=FONT_BODY_MEDIUM).pack(pady=(20, 10))
        ctk.CTkButton(state_frame, text="Try Again", command=retry_command, width=120, fg_color="transparent", border_width=1, text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER).pack(pady=(0, 20))
        state_frame.pack(fill="x", expand=True)

    def _get_credentials(self):
        tenant = self.lic_tenant_id.get().strip()
        client_str = self.lic_client_ids.get().strip()
        secret_str = self.lic_client_secrets.get().strip()

        if not tenant or not client_str or not secret_str:
            return None, None, None

        clients = [x.strip() for x in client_str.split(",") if x.strip()]
        secrets = [x.strip() for x in secret_str.split(",") if x.strip()]
        return tenant, clients, secrets

    def _check_all_done(self):
        """Checks if all sections have resolved (success or error) and updates main UI."""
        states = [
            self.status_sku, self.status_o365, self.status_o365_trend, self.status_m365,
            self.sharepoint_view.status, self.onedrive_view.status, self.security_gov_view.status,
            self.status_pa
        ]

        if "loading" in states:
            return

        self.btn_lic_submit.configure(state="normal", text="Submit")

        success = all(s == "success" for s in states)
        if success:
            self.lbl_lic_status.configure(text="✔ All Inventory and Usage Reports Pulled Successfully!", text_color=COLOR_SUCCESS)
        else:
            self.lbl_lic_status.configure(text="⚠ Some reports failed. Please retry individually.", text_color=COLOR_ERROR)

        if hasattr(self, "on_all_done_callback") and self.on_all_done_callback:
            self.on_all_done_callback(success)

    # -------------------------------------------------------------------------
    # MASTER SUBMIT LOGIC
    # -------------------------------------------------------------------------
    def authenticate_licenses_tab(self):
        """Master full parallel fetch."""
        async_logger.info("Master Submit triggered. Restarting all fetches.")

        tenant, clients, secrets = self._get_credentials()
        if not tenant:
            async_logger.warning("Authentication aborted: Missing credential parameters.")
            messagebox.showerror("Credential Error", "Please provide complete Tenant ID, Client ID, and Client Secret strings.", parent=self)
            return

        self.btn_lic_submit.configure(state="disabled", text="Submitting...")
        self.lbl_lic_status.configure(text="Querying Microsoft Graph APIs and Reports in parallel...", text_color=COLOR_TEXT_SUB)
        self.btn_export_lic.configure(state="disabled")

        self.retry_sku(clear_log=False)
        self.retry_o365(clear_log=False)
        self.retry_o365_trend(clear_log=False)
        self.retry_m365(clear_log=False)
        self.sharepoint_view.trigger_fetch(tenant, clients[0], secrets[0])
        self.onedrive_view.trigger_fetch(tenant, clients[0], secrets[0])
        self.security_gov_view.trigger_fetch(tenant, clients[0], secrets[0])
        self.retry_power_automate(clear_log=False)

    # -------------------------------------------------------------------------
    # INDIVIDUAL RETRY HANDLERS
    # -------------------------------------------------------------------------
    def retry_sku(self, clear_log=True):
        if clear_log: async_logger.info("User requested individual SKU retry.")
        tenant, clients, secrets = self._get_credentials()
        if not tenant: return

        self.status_sku = "loading"
        self.lic_section.pack(fill="x", expand=True, pady=(15, 10))
        self.lic_grid_frame.pack_forget()
        self._set_state_loading(self.lic_state_frame, "Fetching SKU inventories...")
        threading.Thread(target=self._execute_sku_worker, args=(tenant, clients, secrets), daemon=True).start()

    def retry_o365(self, clear_log=True):
        if clear_log: async_logger.info("User requested individual O365 Usage retry.")
        tenant, clients, secrets = self._get_credentials()
        if not tenant: return

        self.status_o365 = "loading"
        self.o365_section.pack(fill="x", expand=True, pady=(20, 10))
        self.o365_grid_frame.pack_forget()
        self._set_state_loading(self.o365_state_frame, "Downloading and parsing O365 Active User reports...")
        threading.Thread(target=self._execute_o365_worker, args=(tenant, clients[0], secrets[0]), daemon=True).start()

    def retry_o365_trend(self, clear_log=True):
        if clear_log: async_logger.info("User requested individual O365 Trend Chart retry.")
        tenant, clients, secrets = self._get_credentials()
        if not tenant: return

        self.status_o365_trend = "loading"
        self.o365_trend_section.pack(fill="x", expand=True, pady=(20, 10))
        self.o365_trend_grid_frame.pack_forget()
        self._set_state_loading(self.o365_trend_state_frame, "Downloading and generating O365 Trend report...")
        threading.Thread(target=self._execute_o365_trend_worker, args=(tenant, clients[0], secrets[0]), daemon=True).start()

    def retry_m365(self, clear_log=True):
        if clear_log: async_logger.info("User requested individual M365 Apps Usage retry.")
        tenant, clients, secrets = self._get_credentials()
        if not tenant: return

        self.status_m365 = "loading"
        self.m365_section.pack(fill="x", expand=True, pady=(20, 10))
        self.m365_grid_frame.pack_forget()
        self._set_state_loading(self.m365_state_frame, "Downloading and parsing M365 Apps Usage reports...")
        threading.Thread(target=self._execute_m365_worker, args=(tenant, clients[0], secrets[0]), daemon=True).start()

    # -------------------------------------------------------------------------
    # WORKER 1: SKU FETCHING LOGIC (NOW INDEPENDENT OF CONNECTORS.PY)
    # -------------------------------------------------------------------------
    def _execute_sku_worker(self, tenant: str, clients: List[str], secrets: List[str]):
        async_logger.info("Executing thread: _execute_sku_worker")
        try:
            client_id = clients[0]
            client_secret = secrets[0]
            
            self.log_msg(f"Authenticating app {client_id[:5]}...")
            
            client = GraphClient(
                tenant_id=tenant,
                client_ids=client_id,
                client_secrets=client_secret,
                concurrency=1,
                retries=self.retries.get(),
                backoff=self.backoff.get()
            )
            
            # Re-use the GraphClient parallel scope verification inside core service client
            required_scopes = ["Organization.Read.All", "Directory.Read.All"]
            client.authenticate(required_scopes=required_scopes)
            
            self.log_msg("Querying Graph API endpoint for SKUs...")
            dir_service = DirectoryService(client)
            sku_data = dir_service.get_subscribed_skus()
            client.close()
            
            async_logger.info("Successfully fetched SKU data.")
            self.after(0, self._render_skus_success, sku_data)
        except Exception as e:
            async_logger.error("Exception caught in _execute_sku_worker.", exc_info=True)
            self.after(0, self._render_skus_error, str(e))

    def _render_skus_success(self, sku_dict: Dict[str, Any]):
        async_logger.info("Executing UI render for SKU table.")
        self.lic_state_frame.pack_forget()

        for w in self.lic_grid_frame.winfo_children(): w.destroy()
        self.lic_grid_frame.pack(fill="x", expand=True)

        self.lic_grid_frame.grid_columnconfigure(0, weight=2)
        self.lic_grid_frame.grid_columnconfigure(1, weight=1)
        self.lic_grid_frame.grid_columnconfigure(2, weight=1)

        headers = ["SKU Part Number", "Units", "Consumed Units"]
        for col_idx, head_text in enumerate(headers):
            cell = ctk.CTkFrame(self.lic_grid_frame, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        items = sku_dict.get("value", [])
        if not items:
            empty_cell = ctk.CTkFrame(self.lic_grid_frame, fg_color="transparent")
            empty_cell.grid(row=1, column=0, columnspan=3, sticky="nsew", pady=15)
            ctk.CTkLabel(empty_cell, text="No subscribed product configurations found in scope.", text_color=COLOR_TEXT_SUB).pack()
        else:
            items.sort(key=lambda x: len(x.get("servicePlans", [])), reverse=True)
            self.last_licenses_items = items
            self.btn_export_lic.configure(state="normal")

            current_row = 1
            for item_idx, item in enumerate(items):
                bg_style = COLOR_SURFACE if item_idx % 2 == 0 else COLOR_SURFACE_VARIANT

                c0 = ctk.CTkFrame(self.lic_grid_frame, fg_color=bg_style, corner_radius=0)
                c0.grid(row=current_row, column=0, sticky="nsew", padx=0, pady=(0, 1))
                ctk.CTkLabel(c0, text=item.get("skuPartNumber", "UNKNOWN_SKU"), font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=8, anchor="nw")

                c1 = ctk.CTkFrame(self.lic_grid_frame, fg_color=bg_style, corner_radius=0)
                c1.grid(row=current_row, column=1, sticky="nsew", padx=0, pady=(0, 1))
                prepaid = item.get("prepaidUnits", {})
                p_str = f"Enabled: {prepaid.get('enabled', 0):,}"
                if prepaid.get('warning', 0) > 0: p_str += f"\nWarn: {prepaid.get('warning'):,}"
                if prepaid.get('suspended', 0) > 0: p_str += f"\nSusp: {prepaid.get('suspended'):,}"
                ctk.CTkLabel(c1, text=p_str, text_color=COLOR_TEXT_MAIN, justify="left").pack(padx=10, pady=8, anchor="nw")

                c2 = ctk.CTkFrame(self.lic_grid_frame, fg_color=bg_style, corner_radius=0)
                c2.grid(row=current_row, column=2, sticky="nsew", padx=0, pady=(0, 1))
                ctk.CTkLabel(c2, text=f"{item.get('consumedUnits', 0):,}", text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=8, anchor="nw")

                current_row += 1

        self.status_sku = "success"
        self._check_all_done()

    def _render_skus_error(self, err_msg):
        async_logger.warning(f"Rendering SKU table error state: {err_msg}")
        self._set_state_error(self.lic_state_frame, f"Failed to load SKUs: {err_msg}", self.retry_sku)
        self.btn_export_lic.configure(state="disabled")
        self.status_sku = "error"
        self._check_all_done()

    # -------------------------------------------------------------------------
    # WORKER 2: O365 USAGE LOGIC
    # -------------------------------------------------------------------------
    def _execute_o365_worker(self, tenant: str, client_id: str, client_secret: str):
        async_logger.info("Executing thread: _execute_o365_worker")
        try:
            o365_data = usage.run_o365_pipeline(client_id, client_secret, tenant)
            async_logger.info("Successfully completed O365 usage data fetch.")
            self.after(0, self._render_o365_success, o365_data)
        except Exception as e:
            async_logger.error("Exception caught in _execute_o365_worker.", exc_info=True)
            self.after(0, self._render_o365_error, str(e))

    def _render_o365_success(self, o365_data: list):
        async_logger.info("Rendering O365 Active Users Usage grid success state.")
        self.o365_state_frame.pack_forget()
        for w in self.o365_grid_frame.winfo_children(): w.destroy()

        self.o365_grid_frame.pack(fill="x", expand=True)

        self.o365_grid_frame.grid_columnconfigure(0, weight=2)
        self.o365_grid_frame.grid_columnconfigure(1, weight=1)
        self.o365_grid_frame.grid_columnconfigure(2, weight=1)
        self.o365_grid_frame.grid_columnconfigure(3, weight=1)

        headers_o365 = ["Service", "30 Days", "90 Days", "180 Days"]
        for col_idx, head_text in enumerate(headers_o365):
            cell = ctk.CTkFrame(self.o365_grid_frame, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        if not o365_data:
            empty_cell = ctk.CTkFrame(self.o365_grid_frame, fg_color="transparent")
            empty_cell.grid(row=1, column=0, columnspan=4, sticky="nsew", pady=15)
            ctk.CTkLabel(empty_cell, text="No O365 usage data found.", text_color=COLOR_TEXT_SUB).pack()
        else:
            for r_idx, row_data in enumerate(o365_data, start=1):
                bg_style = COLOR_SURFACE if r_idx % 2 == 0 else COLOR_SURFACE_VARIANT
                for c_idx, val in enumerate(row_data):
                    cell = ctk.CTkFrame(self.o365_grid_frame, fg_color=bg_style, corner_radius=0)
                    cell.grid(row=r_idx, column=c_idx, sticky="nsew", padx=0, pady=(0, 1))
                    fnt = FONT_BODY_BOLD if c_idx == 0 else FONT_BODY_MEDIUM
                    ctk.CTkLabel(cell, text=str(val), font=fnt, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=8, anchor="nw")

        self.status_o365 = "success"
        self._check_all_done()

    def _render_o365_error(self, err_msg):
        async_logger.warning(f"Rendering O365 Active Users Usage grid error state: {err_msg}")
        self._set_state_error(self.o365_state_frame, f"Failed to load O365 Usage: {err_msg}", self.retry_o365)
        self.status_o365 = "error"
        self._check_all_done()

    # -------------------------------------------------------------------------
    # WORKER 3: O365 TREND GRAPH LOGIC
    # -------------------------------------------------------------------------
    def _execute_o365_trend_worker(self, tenant: str, client_id: str, client_secret: str):
        async_logger.info("Executing thread: _execute_o365_trend_worker")
        try:
            trend_data = usage.run_o365_trend_pipeline(client_id, client_secret, tenant)
            async_logger.info("Successfully completed O365 trend data fetch.")
            self.after(0, self._render_o365_trend_success, trend_data)
        except Exception as e:
            async_logger.error("Exception caught in _execute_o365_trend_worker.", exc_info=True)
            self.after(0, self._render_o365_trend_error, str(e))

    def _render_o365_trend_success(self, trend_data: dict):
        async_logger.info("Rendering O365 Trend Chart success state.")
        self.o365_trend_state_frame.pack_forget()
        for w in self.o365_trend_grid_frame.winfo_children(): w.destroy()

        self.o365_trend_grid_frame.pack(fill="x", expand=True)

        if not MATPLOTLIB_AVAILABLE:
            empty_cell = ctk.CTkFrame(self.o365_trend_grid_frame, fg_color="transparent")
            empty_cell.pack(fill="x", expand=True, pady=15)
            ctk.CTkLabel(empty_cell, text="Matplotlib is required to render charts.\nPlease install it using 'pip install matplotlib'.", text_color=COLOR_ERROR).pack()
            self.status_o365_trend = "error"
            self._check_all_done()
            return

        if not trend_data or not trend_data.get("dates"):
            empty_cell = ctk.CTkFrame(self.o365_trend_grid_frame, fg_color="transparent")
            empty_cell.pack(fill="x", expand=True, pady=15)
            ctk.CTkLabel(empty_cell, text="No O365 trend data found.", text_color=COLOR_TEXT_SUB).pack()
        else:
            try:
                fig, ax = plt.subplots(figsize=(8, 4), dpi=100)
                fig.patch.set_facecolor(COLOR_SURFACE)
                ax.set_facecolor(COLOR_SURFACE)

                dates = trend_data["dates"]

                ax.plot(dates, trend_data["office365"], marker='o', label='Office 365')
                ax.plot(dates, trend_data["exchange"], marker='o', label='Exchange')
                ax.plot(dates, trend_data["onedrive"], marker='o', label='OneDrive')
                ax.plot(dates, trend_data["sharepoint"], marker='o', label='SharePoint')
                ax.plot(dates, trend_data["teams"], marker='o', label='Teams')

                ax.set_xlabel("Date", fontsize=10, color=COLOR_TEXT_SUB)
                ax.set_ylabel("Active Users", fontsize=10, color=COLOR_TEXT_SUB)

                ax.tick_params(axis='x', colors=COLOR_TEXT_SUB, rotation=45, labelsize=8)
                ax.tick_params(axis='y', colors=COLOR_TEXT_SUB)

                # Prevent crowding on the X-axis by plotting fewer labels
                if len(dates) > 10:
                    ax.set_xticks(dates[::max(1, len(dates)//10)])

                for spine in ax.spines.values():
                    spine.set_color(COLOR_OUTLINE_LIGHT)

                ax.legend(facecolor=COLOR_SURFACE, edgecolor=COLOR_OUTLINE_LIGHT, labelcolor=COLOR_TEXT_MAIN, fontsize=9)
                fig.tight_layout()

                canvas = FigureCanvasTkAgg(fig, master=self.o365_trend_grid_frame)
                canvas.draw()
                canvas.get_tk_widget().pack(fill="both", expand=True, padx=10, pady=10)
            except Exception as e:
                async_logger.error(f"Error drawing matplotlib plot: {e}", exc_info=True)
                empty_cell = ctk.CTkFrame(self.o365_trend_grid_frame, fg_color="transparent")
                empty_cell.pack(fill="x", expand=True, pady=15)
                ctk.CTkLabel(empty_cell, text="Failed to render trend graph (Matplotlib constraint).", text_color=COLOR_ERROR).pack()

        self.status_o365_trend = "success"
        self._check_all_done()

    def _render_o365_trend_error(self, err_msg):
        async_logger.warning(f"Rendering O365 Trend Chart error state: {err_msg}")
        self._set_state_error(self.o365_trend_state_frame, f"Failed to load O365 Trend: {err_msg}", self.retry_o365_trend)
        self.status_o365_trend = "error"
        self._check_all_done()

    # -------------------------------------------------------------------------
    # WORKER 4: M365 APPS USAGE LOGIC
    # -------------------------------------------------------------------------
    def _execute_m365_worker(self, tenant: str, client_id: str, client_secret: str):
        async_logger.info("Executing thread: _execute_m365_worker")
        try:
            m365_data = usage.run_m365_pipeline(client_id, client_secret, tenant)
            async_logger.info("Successfully completed M365 Apps usage data fetch.")
            self.after(0, self._render_m365_success, m365_data)
        except Exception as e:
            async_logger.error("Exception caught in _execute_m365_worker.", exc_info=True)
            self.after(0, self._render_m365_error, str(e))

    def _render_m365_success(self, m365_data: list):
        async_logger.info("Rendering M365 Apps Usage success state.")
        self.m365_state_frame.pack_forget()
        for w in self.m365_grid_frame.winfo_children(): w.destroy()

        self.m365_grid_frame.pack(fill="x", expand=True)

        for i in range(4): self.m365_grid_frame.grid_columnconfigure(i, weight=1)

        headers_m365 = ["App / Platform", "Users Count", "App / Platform", "Users Count"]
        for col_idx, head_text in enumerate(headers_m365):
            cell = ctk.CTkFrame(self.m365_grid_frame, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        if not m365_data:
            empty_cell = ctk.CTkFrame(self.m365_grid_frame, fg_color="transparent")
            empty_cell.grid(row=1, column=0, columnspan=4, sticky="nsew", pady=15)
            ctk.CTkLabel(empty_cell, text="No M365 App usage data found.", text_color=COLOR_TEXT_SUB).pack()
        else:
            half = (len(m365_data) + 1) // 2
            left_col = m365_data[:half]
            right_col = m365_data[half:]

            for r_idx in range(half):
                bg_style = COLOR_SURFACE if r_idx % 2 == 0 else COLOR_SURFACE_VARIANT
                row_items = []

                if r_idx < len(left_col): row_items.extend([left_col[r_idx][0], left_col[r_idx][1]])
                else: row_items.extend(["", ""])

                if r_idx < len(right_col): row_items.extend([right_col[r_idx][0], right_col[r_idx][1]])
                else: row_items.extend(["", ""])

                for c_idx, val in enumerate(row_items):
                    cell = ctk.CTkFrame(self.m365_grid_frame, fg_color=bg_style, corner_radius=0)
                    cell.grid(row=r_idx + 1, column=c_idx, sticky="nsew", padx=0, pady=(0, 1))
                    fnt = FONT_BODY_BOLD if c_idx in [0, 2] else FONT_BODY_MEDIUM
                    ctk.CTkLabel(cell, text=str(val), font=fnt, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=6, anchor="nw")

        self.status_m365 = "success"
        self._check_all_done()

    def _render_m365_error(self, err_msg):
        async_logger.warning(f"Rendering M365 Apps Usage error state: {err_msg}")
        self._set_state_error(self.m365_state_frame, f"Failed to load M365 Apps Usage: {err_msg}", self.retry_m365)
        self.status_m365 = "error"
        self._check_all_done()

    # -------------------------------------------------------------------------
    # WORKER 5: POWER AUTOMATE LOGIC
    # -------------------------------------------------------------------------
    def retry_power_automate(self, clear_log=True):
        if clear_log: async_logger.info("User requested individual Power Automate retry.")
        tenant, clients, secrets = self._get_credentials()
        if not tenant: return

        self.status_pa = "loading"
        self.pa_section.pack(fill="x", expand=True, pady=(20, 10))
        self.pa_grid_frame.pack_forget()
        self._set_state_loading(self.pa_state_frame, "Scanning Power Automate flows...")
        threading.Thread(target=self._execute_pa_worker, args=(tenant, clients[0], secrets[0]), daemon=True).start()

    def _execute_pa_worker(self, tenant: str, client_id: str, client_secret: str):
        async_logger.info("Executing thread: _execute_pa_worker")
        try:
            scanner = PowerAutomateScanner(tenant, client_id, client_secret)
            results = scanner.scan_flows()
            async_logger.info("Successfully completed Power Automate scan.")
            self.after(0, self._render_pa_success, results)
        except Exception as e:
            async_logger.error("Exception caught in _execute_pa_worker.", exc_info=True)
            self.after(0, self._render_pa_error, str(e))

    def _render_pa_success(self, results: dict):
        async_logger.info("Rendering Power Automate success state.")
        self.pa_state_frame.pack_forget()
        for w in self.pa_grid_frame.winfo_children(): w.destroy()

        self.pa_grid_frame.pack(fill="x", expand=True)

        if not results:
            empty_cell = ctk.CTkFrame(self.pa_grid_frame, fg_color="transparent")
            empty_cell.pack(fill="x", expand=True, pady=15)
            ctk.CTkLabel(empty_cell, text="No Power Automate data found.", text_color=COLOR_TEXT_SUB).pack()
            self.status_pa = "success"
            self._check_all_done()
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

        # 1. Summary Grid
        summary_frame = ctk.CTkFrame(self.pa_grid_frame, fg_color=COLOR_OUTLINE_LIGHT, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)
        summary_frame.pack(fill="x", pady=20)
        
        for i in range(2): summary_frame.grid_columnconfigure(i, weight=1)

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

        # Cache complex flows for export
        self.last_complex_flows = complex_flows
        if complex_flows:
            self.btn_export_pa.configure(state="normal")
        else:
            self.btn_export_pa.configure(state="disabled")

        # 2. Charts Section
        if total_flows > 0:
            charts_frame = ctk.CTkFrame(self.pa_grid_frame, fg_color="transparent")
            charts_frame.pack(fill="x", pady=20)
            
            if not MATPLOTLIB_AVAILABLE:
                ctk.CTkLabel(charts_frame, text="Matplotlib is required to render charts.\nPlease install it using 'pip install matplotlib'.", text_color=COLOR_ERROR).pack(pady=15)
            else:
                try:
                    # Increased size as requested
                    fig, ax = plt.subplots(figsize=(12, 7), dpi=100)
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
                    
                    complex_active = sum(1 for f in complex_flows if f.get("Active") == "Yes")
                    complex_inactive = len(complex_flows) - complex_active
                    
                    actives = [c_active, d_active, p_active, e_active, complex_active]
                    inactives = [c_inactive, d_inactive, p_inactive, e_inactive, complex_inactive]
                    
                    x = range(len(categories))
                    width = 0.15  # Reduced width of the bars
                    
                    color_active = COLOR_PRIMARY
                    color_inactive = COLOR_TONAL_BG
                    
                    rects1 = ax.bar(x, actives, width, label='Active', color=color_active)
                    rects2 = ax.bar([i + width for i in x], inactives, width, label='Inactive', color=color_inactive)
                    
                    ax.set_ylabel('Count', color=COLOR_TEXT_MAIN, fontsize=14, fontweight='bold')
                    ax.set_title('Power Automate Flows Breakdown', color=COLOR_TEXT_MAIN, fontsize=18, fontweight='bold')
                    ax.set_xticks([i + width/2 for i in x])
                    ax.set_xticklabels(categories, color=COLOR_TEXT_MAIN, fontsize=14, fontweight='bold')
                    ax.legend(facecolor=COLOR_SURFACE, edgecolor=COLOR_OUTLINE_LIGHT, labelcolor=COLOR_TEXT_MAIN, prop={'weight':'bold', 'size':12})
                    
                    ax.bar_label(rects1, padding=3, color=COLOR_TEXT_MAIN, fontsize=14, fontweight='bold')
                    ax.bar_label(rects2, padding=3, color=COLOR_TEXT_MAIN, fontsize=14, fontweight='bold')
                    
                    for spine in ax.spines.values():
                        spine.set_color(COLOR_OUTLINE_LIGHT)
                    
                    ax.tick_params(axis='y', colors=COLOR_TEXT_MAIN, labelsize=12)
                    for label in ax.get_yticklabels():
                        label.set_fontweight('bold')
                    
                    max_val = max(max(actives), max(inactives))
                    ax.set_ylim(0, max(max_val + 3, int(max_val * 1.3)))
                    
                    fig.tight_layout()
                    canvas = FigureCanvasTkAgg(fig, master=charts_frame)
                    canvas.draw()
                    # Allow it to expand more in width
                    canvas.get_tk_widget().pack(fill="x", padx=50, pady=10)
                    
                except Exception as e:
                    async_logger.error(f"Error drawing Power Automate charts: {e}", exc_info=True)

        self.status_pa = "success"
        self._check_all_done()

    def _render_pa_error(self, err_msg):
        async_logger.warning(f"Rendering Power Automate error state: {err_msg}")
        self._set_state_error(self.pa_state_frame, f"Failed to load Power Automate Telemetry: {err_msg}", self.retry_power_automate)
        self.status_pa = "error"
        self._check_all_done()

    # -------------------------------------------------------------------------
    # SPREADSHEET EXPORT LOGIC (SKUs)
    # -------------------------------------------------------------------------
    def export_licenses_spreadsheet(self):
        """Exports the SKUs inventory to a CSV formatted to mimic merged cells."""
        async_logger.info("Exporting licenses to local spreadsheet requested.")
        if not hasattr(self, "last_licenses_items") or not self.last_licenses_items:
            async_logger.warning("Export aborted: No cached license items available to export.")
            return

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        f = filedialog.asksaveasfilename(
            initialfile=f"licenses_inventory_{ts}.csv",
            defaultextension=".csv",
            filetypes=[("CSV Spreadsheet", "*.csv")]
        )

        if not f:
            async_logger.info("Export aborted by user (dialog cancelled).")
            return

        async_logger.info(f"Target export path established: {f}")
        headers = ["SKU Part Number", "Units", "Consumed Units", "Included Service Plans", "Applies To"]
        rows = []

        for item in self.last_licenses_items:
            sku_name = item.get("skuPartNumber", "UNKNOWN_SKU")
            prepaid = item.get("prepaidUnits", {})
            enabled_units = prepaid.get("enabled", 0)
            warn_units = prepaid.get("warning", 0)
            susp_units = prepaid.get("suspended", 0)

            prepaid_str = f"Enabled: {enabled_units:,}"
            if warn_units > 0: prepaid_str += f"\nWarn: {warn_units:,}"
            if susp_units > 0: prepaid_str += f"\nSusp: {susp_units:,}"
            consumed_str = f"{item.get('consumedUnits', 0):,}"

            plans = item.get("servicePlans", [])

            if not plans:
                rows.append([sku_name, prepaid_str, consumed_str, "None designated.", "-"])
            else:
                for idx, p in enumerate(plans):
                    p_name = p.get("servicePlanName", "UnnamedPlan")
                    p_scope = p.get("appliesTo", "Unknown")
                    if idx == 0:
                        rows.append([sku_name, prepaid_str, consumed_str, p_name, p_scope])
                    else:
                        rows.append(["", "", "", p_name, p_scope])

        try:
            with open(f, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(headers)
                writer.writerows(rows)
            async_logger.info("Spreadsheet exported successfully.")
            messagebox.showinfo("Export Successful", f"Spreadsheet successfully saved to:\n{f}", parent=self)
        except Exception as e:
            async_logger.error("Failed writing export spreadsheet to disk.", exc_info=True)
            messagebox.showerror("Export Error", f"Failed to save file:\n{e}", parent=self)

    def export_complex_flows(self):
        """Exports the complex flows to a CSV."""
        async_logger.info("Exporting complex flows to local spreadsheet requested.")
        if not hasattr(self, "last_complex_flows") or not self.last_complex_flows:
            async_logger.warning("Export aborted: No cached complex flows available to export.")
            return

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        f = filedialog.asksaveasfilename(
            initialfile=f"complex_flows_{ts}.csv",
            defaultextension=".csv",
            filetypes=[("CSV Spreadsheet", "*.csv")]
        )

        if not f:
            async_logger.info("Export aborted by user (dialog cancelled).")
            return

        headers = ["Environment", "Name", "Type", "Tier", "Active", "Reason"]
        rows = []

        for flow in self.last_complex_flows:
            rows.append([
                flow.get("Environment"),
                flow.get("Name"),
                flow.get("Type"),
                flow.get("Tier"),
                flow.get("Active"),
                flow.get("Reason")
            ])

        try:
            with open(f, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(headers)
                writer.writerows(rows)
            async_logger.info("Complex flows exported successfully.")
            messagebox.showinfo("Export Successful", f"Complex flows successfully saved to:\n{f}", parent=self)
        except Exception as e:
            async_logger.error("Failed writing export spreadsheet to disk.", exc_info=True)
            messagebox.showerror("Export Error", f"Failed to save file:\n{e}", parent=self)

    def is_descendant(self, parent, widget) -> bool:
        """Recursively checks if a widget (or its Tkinter path name) is a descendant of parent."""
        if not widget:
            return False
        if isinstance(widget, str):
            try:
                widget = self.nametowidget(widget)
            except Exception:
                return False
        if widget == parent:
            return True
        if hasattr(widget, "master") and widget.master is not None:
            return self.is_descendant(parent, widget.master)
        return False

    def _handle_global_mousewheel(self, event):
        """Redirects mousewheel scrolling to the tab's parent canvas if hovered."""
        try:
            widget = self.winfo_containing(event.x_root, event.y_root)
        except Exception:
            return

        if self.is_descendant(self, widget):
            if event.num == 4:  # Linux scroll up
                self._parent_canvas.yview("scroll", -1, "units")
            elif event.num == 5:  # Linux scroll down
                self._parent_canvas.yview("scroll", 1, "units")
            else:  # Windows / macOS
                if sys.platform == "darwin":
                    # macOS trackpad/mouse delta
                    self._parent_canvas.yview("scroll", -event.delta, "units")
                else:
                    # Windows delta (usually multiple of 120)
                    self._parent_canvas.yview("scroll", -int(event.delta / 120), "units")
