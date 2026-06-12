import customtkinter as ctk
import threading
import logging
import traceback
import webbrowser
from telemetry.styles import *

usage_logger = logging.getLogger("IntunePoliciesUI")

def fetch_intune_policies_data(client_id, client_secret, tenant_id) -> dict:
    """Fetch and parse Intune Configuration Policies and Device Configurations."""
    usage_logger.info("Starting Intune Policies fetch...")
    try:
        from core.graph.client import GraphClient
        client = GraphClient(
            tenant_id=tenant_id,
            client_ids=client_id,
            client_secrets=client_secret,
            concurrency=1,
            retries=3,
            backoff=2
        )
        client.authenticate()
        session = client.token_manager.get_session()
        
        slot = client.token_manager.get_valid_token_slot()
        access_token = slot["token"]
        headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
        
        device_configs = []
        try:
            res1 = session.get("https://graph.microsoft.com/beta/deviceManagement/deviceConfigurations", headers=headers)
            if res1.status_code == 200:
                device_configs = res1.json().get("value", [])
        except Exception as e:
            usage_logger.warning(f"Failed to fetch deviceConfigurations: {e}")

        config_policies = []
        try:
            res2 = session.get("https://graph.microsoft.com/beta/deviceManagement/configurationPolicies", headers=headers)
            if res2.status_code == 200:
                config_policies = res2.json().get("value", [])
        except Exception as e:
            usage_logger.warning(f"Failed to fetch configurationPolicies: {e}")
            
        client.token_manager.return_token_slot(slot)
        client.close()
        
        import re
        from collections import defaultdict
        counts = defaultdict(int)

        def extract_platform_and_type(odata_type):
            type_str = odata_type.replace("#microsoft.graph.", "")
            platform = "Unknown"
            
            if type_str.startswith("windows10"):
                platform = "Windows 10"
                policy_type = type_str.replace("windows10", "")
            elif type_str.startswith("windows"):
                platform = "Windows"
                policy_type = type_str.replace("windows", "")
            elif type_str.startswith("ios"):
                platform = "iOS"
                policy_type = type_str.replace("ios", "")
            elif type_str.startswith("android"):
                platform = "Android"
                policy_type = type_str.replace("android", "")
            elif type_str.startswith("macOS"):
                platform = "macOS"
                policy_type = type_str.replace("macOS", "")
            else:
                policy_type = type_str

            if not policy_type:
                policy_type = "Configuration"
                
            policy_type = re.sub(r"([A-Z])", r" \1", policy_type).strip()
            return platform, policy_type

        for dc in device_configs:
            platform, p_type = extract_platform_and_type(dc.get("@odata.type", ""))
            counts[(platform, p_type)] += 1
            
        for cp in config_policies:
            platform = cp.get("platforms", "Unknown")
            if platform == "windows10AndLater":
                platform = "Windows 10"
            elif platform == "windows81AndLater":
                platform = "Windows 8.1"
            elif platform == "macOS":
                platform = "macOS"
            else:
                platform = platform.capitalize()
            counts[(platform, "Settings Catalog")] += 1
            
        rows = []
        for (platform, p_type), count in sorted(counts.items()):
            rows.append((platform, p_type, str(count)))

        return {
            "total_device_configs": len(device_configs),
            "total_config_policies": len(config_policies),
            "table_rows": rows,
            "error": None
        }
    except Exception as e:
        usage_logger.error("Failed to fetch Intune Policies", exc_info=True)
        return {"error": str(e)}

class IntunePoliciesFrame(ctk.CTkFrame):
    """Component for rendering Intune Policies data."""
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
        self.grid_frame = ctk.CTkFrame(self.inner_pad, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)
        
        self.reset_view()

    def reset_view(self):
        self.pack_forget()
        self.state_frame.pack_forget()
        self.grid_frame.pack_forget()
        
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
        ctk.CTkLabel(self.state_frame, text=f"✖ {error_msg}", text_color=COLOR_ERROR, font=FONT_BODY_MEDIUM, justify="center").pack(pady=(20, 10))
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
        try:
            res = fetch_intune_policies_data(client_id, client_secret, tenant)
            self.after(0, self._handle_result, res)
        finally:
            if self.semaphore:
                self.semaphore.release()

    def _handle_result(self, result: dict):
        self.state_frame.pack_forget()
        for w in self.grid_frame.winfo_children():
            w.destroy()
            
        err = result.get("error")
        if err:
            self.status = "error"
            if "Authorization_RequestDenied" in err or "403" in err:
                self._set_state_error("Access Denied: Missing Intune or DeviceManagement permissions.")
            else:
                self._set_state_error(err)
        else:
            self.status = "success"
            self.grid_frame.pack(fill="x", expand=True)
            self._render_card(result)
            
        self.on_status_change()

    def _render_card(self, data: dict):
        summary_frame = ctk.CTkFrame(self.grid_frame, fg_color="transparent")
        summary_frame.pack(fill="x", padx=10, pady=10)
        
        tot_dc = data.get("total_device_configs", 0)
        tot_cp = data.get("total_config_policies", 0)
        ctk.CTkLabel(summary_frame, text=f"Total Extracted: {tot_dc} Device Configurations | {tot_cp} Configuration Policies", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB).pack(anchor="w")

        metrics_grid = ctk.CTkFrame(self.grid_frame, fg_color=COLOR_SURFACE, corner_radius=0)
        metrics_grid.pack(fill="x", padx=10, pady=(5, 15))
        
        headers = ["Platform", "Policy Type", "Number of Policies"]
        for i in range(3):
            metrics_grid.grid_columnconfigure(i, weight=1 if i == 2 else 2)
            
        for col_idx, head_text in enumerate(headers):
            cell = ctk.CTkFrame(metrics_grid, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")
            
        rows_data = data.get("table_rows", [])
        
        if not rows_data:
            c = ctk.CTkFrame(metrics_grid, fg_color=COLOR_SURFACE, corner_radius=0)
            c.grid(row=1, column=0, columnspan=3, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c, text="No policies detected.", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="center").pack(padx=10, pady=12)
            return
            
        for r_idx, (platform, p_type, count) in enumerate(rows_data, start=1):
            bg_style = COLOR_SURFACE if r_idx % 2 != 0 else COLOR_SURFACE_VARIANT
            
            vals = [platform, p_type, count]
            
            for c_idx, val in enumerate(vals):
                c = ctk.CTkFrame(metrics_grid, fg_color=bg_style, corner_radius=0)
                c.grid(row=r_idx, column=c_idx, sticky="nsew", padx=0, pady=(0, 1))
                ctk.CTkLabel(c, text=val, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="left", wraplength=450).pack(padx=10, pady=12, anchor="nw")
