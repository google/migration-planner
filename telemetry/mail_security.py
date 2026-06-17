import customtkinter as ctk
import time
import requests
import threading
import logging
from telemetry.styles import *

usage_logger = logging.getLogger("M365TelemetryAsyncLogger.MailSecurityUI")

class MailSecurityFrame(ctk.CTkFrame):
    def update_loading_text(self, text_msg):
        if hasattr(self, 'loading_label') and self.loading_label.winfo_exists():
            self.loading_label.configure(text=f"⏳ {text_msg}")
    def __init__(self, master, log_callback, credentials_callback, status_change_callback, **kwargs):
        self.semaphore = kwargs.pop("concurrency_semaphore", None)
        super().__init__(master, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=12, **kwargs)
        self.log_msg = log_callback
        self.get_credentials = credentials_callback
        self.on_status_change_cb = status_change_callback
        
        self.status = None
        self.loading = True
        self.error_msg = None
        
        self.total_eop_users = 0
        self.eop_skus = []
        
        self.total_defender_users = 0
        self.defender_skus = []
        
        self.last_data = {}
        
        self.inner_pad = ctk.CTkFrame(self, fg_color="transparent")
        self.inner_pad.pack(fill="both", expand=True, padx=20, pady=20)
        
        self.header = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.header.pack(fill="x", pady=(0, 10))
        ctk.CTkLabel(self.header, text="Mail Security", font=FONT_HEADER_SMALL, text_color=COLOR_TEXT_MAIN).pack(side="left")
        self.reload_btn = ctk.CTkButton(
            self.header, 
            state="disabled", text="↻ Reload", 
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
        
        self.grid_frame = ctk.CTkFrame(self.inner_pad, fg_color=COLOR_SURFACE, corner_radius=12, border_width=1, border_color=COLOR_OUTLINE_LIGHT)
        self.grid_frame.pack(fill="x", expand=True, pady=(0, 10))
        
        self.loading_label = None
        self.progress = None
        self.render_ui_state()

    def trigger_fetch(self, tenant, client_id, client_secret):
        self.pack(fill="x", expand=True, pady=(0, 5))
        self.status = "loading"
        self.loading = True
        self.render_ui_state()
        threading.Thread(target=self._fetch_data, args=(tenant, client_id, client_secret), daemon=True).start()

    def _fetch_data(self, tenant, c_id, c_secret):
        if self.semaphore:
            self.semaphore.acquire()
        try:
            if not tenant or not c_id or not c_secret:
                raise ValueError("Missing credentials.")

            from util.auth_manager import TokenManager
            tm = TokenManager(tenant_id=tenant, client_ids=[c_id], client_secrets=[c_secret], concurrency=1, retries=1, backoff=0)
            tm.authenticate_all()
            
            slot = tm.get_valid_token_slot()
            if not slot:
                raise ConnectionError("Authentication failed: No valid token.")

            token = slot["token"]
            headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
            
            url = "https://graph.microsoft.com/v1.0/subscribedSkus"
            res = requests.get(url, headers=headers, timeout=15)
            
            tm.return_token_slot(slot)
            
            if res.status_code != 200:
                raise ConnectionError(f"Graph API Error {res.status_code}: {res.text}")
                
            data = res.json().get("value", [])
            
            defender_skus_set = set()
            eop_skus_set = set()
            
            defender_users = 0
            eop_users = 0
            
            for sku in data:
                raw_part_num = sku.get("skuPartNumber", "Unknown")
                if isinstance(raw_part_num, list):
                    part_num = ", ".join([str(x) for x in raw_part_num])
                else:
                    part_num = str(raw_part_num)
                    
                consumed = int(sku.get("consumedUnits", 0))
                plans = sku.get("servicePlans", [])
                
                has_defender = False
                has_eop = False
                
                for p in plans:
                    if p.get("provisioningStatus") == "Success":
                        name = p.get("servicePlanName", "").upper()
                        if "DEFENDER_PLATFORM_FOR_OFFICE" in name or "ATP_ENTERPRISE" in name:
                            has_defender = True
                        elif "EXCHANGE_S_ENTERPRISE" in name or "EXCHANGE_S_STANDARD" in name or "EXCHANGE_S_FOUNDATION" in name:
                            has_eop = True
                            
                if has_defender:
                    defender_skus_set.add(part_num)
                    defender_users += consumed
                elif has_eop:
                    eop_skus_set.add(part_num)
                    eop_users += consumed
                    
            self.defender_skus = list(defender_skus_set)
            self.total_defender_users = defender_users
            
            self.eop_skus = list(eop_skus_set)
            self.total_eop_users = eop_users
            
            result_data = {
                "defender": {"skus": self.defender_skus, "users": self.total_defender_users},
                "eop": {"skus": self.eop_skus, "users": self.total_eop_users}
            }
            self.after(0, self._render_success, result_data)
            
        except Exception as e:
            usage_logger.error(f"Error fetching mail security SKUs: {e}", exc_info=True)
            self.after(0, self._render_error, str(e))
        finally:
            if self.semaphore:
                self.semaphore.release()

    def _render_success(self, data):
        self.last_data = data
        self.status = "success"
        self.loading = False
        self.on_status_change()

    def _render_error(self, err_msg):
        self.error_msg = err_msg
        self.status = "error"
        self.loading = False
        self.on_status_change()

    def reset_view(self):
        self.pack_forget()
        self.status = None
        self.error_msg = None

    def _retry_fetch(self):
        tenant, clients, secrets = self.get_credentials()
        if tenant and clients and secrets:
            if hasattr(self, 'reload_btn') and self.reload_btn.winfo_exists():
                self.reload_btn.configure(state="disabled")
            self.trigger_fetch(tenant, clients[0], secrets[0])

    def cancel(self):
        self.status = None
        self.loading = False
        self.error_msg = None
        self.reset_view()

    def on_status_change(self):
        self.render_ui_state()
        if hasattr(self, "on_status_change_cb") and self.on_status_change_cb:
            self.on_status_change_cb()

    def render_ui_state(self):
        for widget in self.grid_frame.winfo_children():
            widget.destroy()
            
        if not self.loading and hasattr(self, 'reload_btn') and self.reload_btn.winfo_exists():
            self.reload_btn.configure(state="normal")
            
        if self.loading:
            self.loading_label = ctk.CTkLabel(self.grid_frame, text="⏳ Loading Mail Security Data...", text_color="#6b7280", font=__import__("customtkinter").CTkFont(family="Segoe UI", size=13))
            self.loading_label.pack(pady=(20, 5))
            self.progress = ctk.CTkProgressBar(self.grid_frame, mode="indeterminate", width=250, fg_color=COLOR_SURFACE_VARIANT, progress_color=COLOR_PRIMARY)
            self.progress.pack(pady=(0, 20))
            self.progress.start()
            return
        if self.error_msg:
            ctk.CTkLabel(self.grid_frame, text=self.error_msg, text_color=COLOR_ERROR, font=FONT_BODY_MEDIUM).pack(pady=20)
            return
            
        metrics_grid = ctk.CTkFrame(self.grid_frame, fg_color=COLOR_SURFACE, corner_radius=0)
        metrics_grid.pack(fill="x", padx=10, pady=(5, 5))
        
        headers = ["Mail Security Configuration", "Detected SKUs", "Affected Users"]
        for i in range(3):
            metrics_grid.grid_columnconfigure(i, weight=1 if i == 2 else 2)
            
        for col_idx, head_text in enumerate(headers):
            cell = ctk.CTkFrame(metrics_grid, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")
            
        rows_data = []
        if self.defender_skus:
            rows_data.append(("Microsoft Defender for Office 365", ", ".join(self.defender_skus), str(self.total_defender_users)))
        if self.eop_skus:
            rows_data.append(("Exchange Online Protection (Baseline)", ", ".join(self.eop_skus), str(self.total_eop_users)))
            
        if not rows_data:
            c = ctk.CTkFrame(metrics_grid, fg_color=COLOR_SURFACE, corner_radius=0)
            c.grid(row=1, column=0, columnspan=3, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c, text="No Mail Security SKUs detected.", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="center").pack(padx=10, pady=12)
        else:
            for r_idx, vals in enumerate(rows_data, start=1):
                bg_style = COLOR_SURFACE if r_idx % 2 != 0 else COLOR_SURFACE_VARIANT
                for c_idx, val in enumerate(vals):
                    c = ctk.CTkFrame(metrics_grid, fg_color=bg_style, corner_radius=0)
                    c.grid(row=r_idx, column=c_idx, sticky="nsew", padx=0, pady=(0, 1))
                    ctk.CTkLabel(c, text=val, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="left", wraplength=450).pack(padx=10, pady=12, anchor="nw")
                    
        disclaimer = ctk.CTkLabel(self.grid_frame, text="Note: Users can track inbound connectors displayed below to identify 3rd-party security apps.", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB, anchor="w")
        disclaimer.pack(fill="x", padx=15, pady=(5, 15))
