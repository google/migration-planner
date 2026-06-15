import customtkinter as ctk
import threading
import logging
import traceback
import webbrowser
from telemetry.styles import *

usage_logger = logging.getLogger("ExchangeConnectorsUI")

def fetch_exchange_connectors_data(client_id, client_secret, tenant_id) -> dict:
    """Fetch Exchange Online Inbound and Outbound Connectors."""
    usage_logger.info("Starting Exchange Connectors fetch...")
    
    tenant_domain = tenant_id
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
        from core.graph.directory import DirectoryService
        dir_svc = DirectoryService(client)
        tenant_domain = dir_svc.get_tenant_primary_domain()
        usage_logger.info(f"Retrieved primary tenant domain for Connectors: {tenant_domain}")
    except Exception as e:
        usage_logger.warning(f"Could not retrieve tenant domain. Falling back to Tenant ID Guid: {e}")
    finally:
        try:
            client.close()
        except Exception:
            pass
            
    try:
        from core.powershell.client import PowerShellClient
        from core.powershell.exchange_connectors import ExchangeConnectorsService
        
        ps_client = PowerShellClient(tenant_id=tenant_domain, client_id=client_id, client_secret=client_secret, cert_tenant_id=tenant_id)
        connector_svc = ExchangeConnectorsService(ps_client)
        data = connector_svc.fetch_exchange_connectors()
        return {"connectors": data, "error": None}
    except Exception as e:
        usage_logger.error("Failed to fetch Exchange Connectors via PowerShell", exc_info=True)
        return {"connectors": None, "error": str(e)}

class ExchangeConnectorsFrame(ctk.CTkFrame):
    def update_loading_text(self, text_msg):
        if hasattr(self, 'loading_label') and self.loading_label.winfo_exists():
            self.loading_label.configure(text=f"⏳ {text_msg}")
    """Self-contained component for rendering Exchange Online Connectors routing logic matching ExchangeOnline UI standards."""
    def __init__(self, master, log_callback, credentials_callback, status_change_callback, **kwargs):
        self.semaphore = kwargs.pop("concurrency_semaphore", None)
        super().__init__(master, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=12, **kwargs)
        self.log_msg = log_callback
        self.get_credentials = credentials_callback
        self.on_status_change = status_change_callback
        self.status = None

        self.build_ui()

    def build_ui(self):
        """Creates card container for the tab."""
        self.pack(fill="x", expand=True, pady=10)
        
        self.inner_pad = ctk.CTkFrame(self, fg_color="transparent")
        self.inner_pad.pack(fill="both", expand=True, padx=20, pady=20)
        
        self.header_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.header_frame.pack(fill="x", pady=(0, 10))
        
        self.title_lbl = ctk.CTkLabel(
            self.header_frame,
            text="Exchange Connectors (Inbound & Outbound Routing)",
            font=FONT_HEADER_SMALL,
            text_color=COLOR_TEXT_MAIN
        )
        self.title_lbl.pack(side="left", anchor="w")
        
        # Link label redirecting to EAC Connectors
        self.link_lbl = ctk.CTkLabel(
            self.header_frame,
            text="Open Exchange Admin Center ↗",
            font=FONT_BODY_BOLD,
            text_color=COLOR_PRIMARY,
            cursor="hand2"
        )
        self.link_lbl.pack(side="left", anchor="w", padx=(15, 0))
        self.link_lbl.bind("<Button-1>", lambda e: webbrowser.open("https://admin.cloud.microsoft/exchange?#/connectors"))
        self.link_lbl.bind("<Enter>", lambda e: self.link_lbl.configure(text_color=COLOR_PRIMARY_HOVER))
        self.link_lbl.bind("<Leave>", lambda e: self.link_lbl.configure(text_color=COLOR_PRIMARY))
        
        self.reload_btn = ctk.CTkButton(
            self.header_frame, 
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
        if "is not installed or not in PATH" in error_msg.lower() or "pwsh" in error_msg.lower():
            display_msg = "PowerShell Core ('pwsh') is not installed or configured on this machine."
        elif "exchangeonlinemanagement" in error_msg.lower():
            display_msg = "ExchangeOnlineManagement PowerShell module is missing."

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
        usage_logger.info("Exchange Connectors trigger_fetch called. Spawning background thread...")
        self.status = "loading"
        self.on_status_change()
        
        self.pack(fill="x", expand=True, pady=10)
        self.grid_frame.pack_forget()
        
        self._set_state_loading("Retrieving Exchange Connectors routing configurations...")
        
        threading.Thread(
            target=self._execute_worker,
            args=(tenant, client_id, client_secret),
            daemon=True
        ).start()

    def _execute_worker(self, tenant: str, client_id: str, client_secret: str):
        usage_logger.info("Executing thread: _execute_connectors_worker")
        if self.semaphore:
            self.semaphore.acquire()
        try:
            res = fetch_exchange_connectors_data(client_id, client_secret, tenant)
            self.after(0, self._handle_result, res)
        finally:
            if self.semaphore:
                self.semaphore.release()

    def _handle_result(self, result: dict):
        if hasattr(self, 'reload_btn') and self.reload_btn.winfo_exists():
            self.reload_btn.configure(state="normal")
        self.state_frame.pack_forget()
        for w in self.grid_frame.winfo_children():
            w.destroy()
            
        connectors_data = result.get("connectors")
        err = result.get("error")
        
        if err:
            self.status = "error"
            self._set_state_error(err)
        else:
            ps_errs = connectors_data.get("Errors", {}) if connectors_data else {}
            if ps_errs:
                err_msg = "\n".join([f"{k}: {v}" for k,v in ps_errs.items()])
                self.status = "error"
                self._set_state_error(f"PowerShell Execution Error:\n{err_msg}")
            else:
                self.status = "success"
                self.grid_frame.pack(fill="x", expand=True)
                self._render_card(connectors_data)
                
        self.on_status_change()

    def _render_card(self, connectors_data: dict):
        if not connectors_data:
            return
            
        inbound = connectors_data.get("InboundConnectors", [])
        outbound = connectors_data.get("OutboundConnectors", [])
        
        if not inbound and not outbound:
            self.grid_frame.configure(fg_color=COLOR_SURFACE, border_width=1, border_color=COLOR_OUTLINE_LIGHT, corner_radius=8)
            ctk.CTkLabel(self.grid_frame, text="N/A (No Exchange Connectors configured)", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB).pack(padx=20, pady=20, anchor="w")
            return
            
        # Inbound Connectors Section
        if inbound:
            in_header = ctk.CTkFrame(self.grid_frame, fg_color="transparent")
            in_header.pack(fill="x", padx=10, pady=(10, 5))
            ctk.CTkLabel(in_header, text="Inbound Routing (On-Premises / Third-Party Filter to Exchange Online)", font=FONT_BODY_BOLD, text_color=COLOR_PRIMARY).pack(side="left")
            
            in_grid = ctk.CTkFrame(self.grid_frame, fg_color=COLOR_SURFACE, corner_radius=0)
            in_grid.pack(fill="x", padx=10, pady=(0, 15))
            
            headers_in = ["Connector Name", "Status", "Connector Type", "Sender Domains", "Require TLS"]
            for i in range(5):
                in_grid.grid_columnconfigure(i, weight=1)
                
            for col_idx, head_text in enumerate(headers_in):
                cell = ctk.CTkFrame(in_grid, fg_color=COLOR_TONAL_BG, corner_radius=0)
                cell.grid(row=0, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
                ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")
                
            for r_idx, conn in enumerate(inbound, start=1):
                bg_style = COLOR_SURFACE if r_idx % 2 != 0 else COLOR_SURFACE_VARIANT
                vals = [
                    conn.get("Name", "N/A"),
                    "🟢 Enabled" if conn.get("Enabled") else "🔴 Disabled",
                    conn.get("ConnectorType", "N/A"),
                    conn.get("SenderDomains", "N/A") or "N/A",
                    "Yes" if conn.get("RequireTls") else "No"
                ]
                for c_idx, val in enumerate(vals):
                    c = ctk.CTkFrame(in_grid, fg_color=bg_style, corner_radius=0)
                    c.grid(row=r_idx, column=c_idx, sticky="nsew", padx=0, pady=(0, 1))
                    ctk.CTkLabel(c, text=val, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="left", wraplength=180).pack(padx=10, pady=12, anchor="nw")

        # Outbound Connectors Section
        if outbound:
            out_header = ctk.CTkFrame(self.grid_frame, fg_color="transparent")
            out_header.pack(fill="x", padx=10, pady=(10, 5))
            ctk.CTkLabel(out_header, text="Outbound Routing (Exchange Online to On-Premises / Third-Party Gateway)", font=FONT_BODY_BOLD, text_color=COLOR_PRIMARY).pack(side="left")
            
            out_grid = ctk.CTkFrame(self.grid_frame, fg_color=COLOR_SURFACE, corner_radius=0)
            out_grid.pack(fill="x", padx=10, pady=(0, 10))
            
            headers_out = ["Connector Name", "Status", "Recipient Domains", "Smart Hosts", "Use MX Record"]
            for i in range(5):
                out_grid.grid_columnconfigure(i, weight=1)
                
            for col_idx, head_text in enumerate(headers_out):
                cell = ctk.CTkFrame(out_grid, fg_color=COLOR_TONAL_BG, corner_radius=0)
                cell.grid(row=0, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
                ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")
                
            for r_idx, conn in enumerate(outbound, start=1):
                bg_style = COLOR_SURFACE if r_idx % 2 != 0 else COLOR_SURFACE_VARIANT
                vals = [
                    conn.get("Name", "N/A"),
                    "🟢 Enabled" if conn.get("Enabled") else "🔴 Disabled",
                    conn.get("RecipientDomains", "N/A") or "N/A",
                    conn.get("SmartHosts", "N/A") or "N/A",
                    "Yes" if conn.get("UseMxRecord") else "No"
                ]
                for c_idx, val in enumerate(vals):
                    c = ctk.CTkFrame(out_grid, fg_color=bg_style, corner_radius=0)
                    c.grid(row=r_idx, column=c_idx, sticky="nsew", padx=0, pady=(0, 1))
                    ctk.CTkLabel(c, text=val, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="left", wraplength=180).pack(padx=10, pady=12, anchor="nw")
