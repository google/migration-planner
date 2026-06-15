import os
import logging
import threading
import customtkinter as ctk
from core.powershell.client import PowerShellClient
from telemetry.styles import *

calendar_logger = logging.getLogger("M365TelemetryAsyncLogger.CalendarTelemetry")

def run_calendar_telemetry_pipeline(client_id: str, client_secret: str, tenant_id: str) -> dict:
    """Consolidated orchestration pipeline to download and audit Exchange Calendar telemetry config via PowerShell client."""
    from core.graph.client import GraphClient
    from core.graph.directory import DirectoryService
    
    calendar_logger.info("Starting PowerShell Calendar Telemetry Pipeline...")
    
    # 1. Initialize GraphClient ONLY to get the tenant primary domain
    tenant_domain = tenant_id
    client = None
    try:
        client = GraphClient(
            tenant_id=tenant_id,
            client_ids=client_id,
            client_secrets=client_secret,
            concurrency=1,
            retries=5,
            backoff=2
        )
        client.authenticate()
        dir_svc = DirectoryService(client)
        tenant_domain = dir_svc.get_tenant_primary_domain()
        calendar_logger.info(f"Retrieved primary tenant domain: {tenant_domain}")
    except Exception as e:
        calendar_logger.warning(f"Could not retrieve tenant domain via Graph. Falling back to Tenant ID Guid: {e}")
    finally:
        if client:
            client.close()
            
    # 2. Connect to Exchange Online PowerShell for administrative metadata
    rooms_count = 0
    rooms_error = None
    rooms_naming = None
    equipment_count = 0
    equipment_error = None
    can_share_attachments = True
    owa_policy_error = None
    org_apps = []
    apps_error = None
    powershell_error = None
    
    try:
        calendar_logger.info("Connecting to Exchange Online PowerShell for calendar metadata...")
        ps_client = PowerShellClient(
            tenant_id=tenant_domain,
            client_id=client_id,
            client_secret=client_secret,
            cert_tenant_id=tenant_id
        )
        from core.powershell.calendar import CalendarStatsService
        cal_service = CalendarStatsService(ps_client)
        metadata = cal_service.fetch_calendar_attachments_policy()
        
        rooms_count = metadata.get("RoomsCount", 0)
        rooms_error = metadata.get("RoomsError")
        rooms_naming = metadata.get("RoomsNaming")
        equipment_count = metadata.get("EquipmentCount", 0)
        equipment_error = metadata.get("EquipmentError")
        can_share_attachments = metadata.get("CanShareAttachments", True)
        owa_policy_error = metadata.get("OwaPolicyError")
        org_apps = metadata.get("OrganizationApps", [])
        apps_error = metadata.get("AppsError")
             
    except Exception as e:
        calendar_logger.warning(f"Could not connect to Exchange Online PowerShell: {e}")
        powershell_error = str(e)
        
        if "pwsh" in str(e).lower() or "powershell" in str(e).lower():
            err_msg = "pwsh not available"
        elif "module" in str(e).lower():
            err_msg = "ExchangeOnlineManagement module not installed"
        else:
            err_msg = "Not Permitted (Exchange Permission Issue)"
            
        rooms_error = err_msg
        equipment_error = err_msg
        owa_policy_error = err_msg
        apps_error = err_msg

    # Log individual component errors
    if rooms_error:
        calendar_logger.error(f"Exchange PowerShell error querying Room Mailboxes: {rooms_error}")
    if equipment_error:
        calendar_logger.error(f"Exchange PowerShell error querying Equipment Mailboxes: {equipment_error}")
    if owa_policy_error:
        calendar_logger.error(f"Exchange PowerShell error querying OWA Mailbox Policy: {owa_policy_error}")
    if apps_error:
        calendar_logger.error(f"Exchange PowerShell error querying Organization Apps: {apps_error}")

    total_resources = rooms_count + equipment_count
    
    return {
        "CanUsersReserveRooms": rooms_error if rooms_error else (total_resources > 0),
        "TotalCalendarResources": total_resources,
        "RoomsCount": rooms_count,
        "EquipmentCount": equipment_count,
        "RoomsError": rooms_error,
        "DevicesError": equipment_error,
        "OrganizationApps": org_apps,
        "AppsError": apps_error,
        "NamingConvention": rooms_error if rooms_error else (rooms_naming if rooms_naming else "None found"),
        "CanShareAttachments": owa_policy_error if owa_policy_error else can_share_attachments,
        "powershell_error": powershell_error
    }


class CalendarTelemetryFrame(ctk.CTkFrame):
    """Self-contained customtkinter component wrapping Exchange Online Calendar Environment Telemetry UI."""
    
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
        
        
        self.header = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.header.pack(fill="x", pady=(0, 10))
        ctk.CTkLabel(self.header, text="Exchange Online Calendar Environment", font=FONT_HEADER_SMALL, text_color=COLOR_TEXT_MAIN).pack(side="left")
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
        
        self.state_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.warning_label = ctk.CTkLabel(self.inner_pad, text="", font=FONT_BODY_MEDIUM, text_color=COLOR_ERROR, justify="left", anchor="w", wraplength=750)
        self.grid_frame = ctk.CTkFrame(self.inner_pad, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)
        
        self.reset_view()

    def reset_view(self):
        """Resets and hides grids."""
        self.pack_forget()
        self.state_frame.pack_forget()
        self.grid_frame.pack_forget()
        if hasattr(self, "warning_label"):
            self.warning_label.pack_forget()
        
        for w in self.state_frame.winfo_children():
            w.destroy()
        for w in self.grid_frame.winfo_children():
            w.destroy()

    def _set_state_loading(self, msg="Loading..."):
        for w in self.state_frame.winfo_children():
            w.destroy()
        ctk.CTkLabel(self.state_frame, text=f"⏳ {msg}", text_color=COLOR_TEXT_SUB, font=FONT_BODY_MEDIUM).pack(pady=(20, 5))
        self.progress = __import__("customtkinter").CTkProgressBar(self.state_frame, mode="indeterminate", width=250, fg_color="#F3F4F6", progress_color="#2563EB")
        self.progress.pack(pady=(0, 20))
        self.progress.start()
        self.state_frame.pack(fill="x", expand=True)

    def _set_state_error(self, error_msg):
        for w in self.state_frame.winfo_children():
            w.destroy()

        ctk.CTkLabel(self.state_frame, text=f"✖ {error_msg}", text_color=COLOR_ERROR, font=FONT_BODY_MEDIUM, justify="center").pack(pady=(20, 10))
        ctk.CTkButton(self.state_frame, text="Try Again", command=self._retry_fetch, width=120, fg_color="transparent", border_width=1, text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER).pack(pady=(0, 20))
        self.state_frame.pack(fill="x", expand=True)

    def _retry_fetch(self):
        if hasattr(self, 'reload_btn') and self.reload_btn.winfo_exists():
            self.reload_btn.configure(state="disabled")
        tenant, clients, secrets = self.get_credentials()
        if tenant:
            self.trigger_fetch(tenant, clients[0], secrets[0])

    def trigger_fetch(self, tenant, client_id, client_secret):
        """Triggers background fetch thread."""
        calendar_logger.info("Calendar Telemetry trigger_fetch called. Spawning background thread...")
        self.status = "loading"
        self.on_status_change()
        
        self.pack(fill="x", expand=True, pady=10)
        self.grid_frame.pack_forget()
        self.warning_label.pack_forget()
        
        self._set_state_loading("Downloading and auditing Exchange Calendar configurations...")
        
        threading.Thread(
            target=self._execute_worker,
            args=(tenant, client_id, client_secret),
            daemon=True
        ).start()

    def _execute_worker(self, tenant: str, client_id: str, client_secret: str):
        calendar_logger.info("Executing thread: _execute_calendar_worker")
        if self.semaphore:
            self.semaphore.acquire()
        try:
            data = run_calendar_telemetry_pipeline(client_id, client_secret, tenant)
            calendar_logger.info("Successfully completed Calendar telemetry data fetch.")
            self.after(0, self._render_success, data)
        except Exception as e:
            calendar_logger.error("Exception caught in Calendar worker.", exc_info=True)
            self.after(0, self._render_error, str(e))
        finally:
            if self.semaphore:
                self.semaphore.release()

    def _render_success(self, data: dict):
        if hasattr(self, 'reload_btn') and self.reload_btn.winfo_exists():
            self.reload_btn.configure(state="normal")
        self.last_data = data
        calendar_logger.info("Calendar data successfully retrieved. Rendering UI grid.")
        self.state_frame.pack_forget()
        for w in self.grid_frame.winfo_children():
            w.destroy()

        if data.get("powershell_error"):
            friendly_msg = f"Exchange PowerShell query failed: {data['powershell_error']}"
            self.warning_label.configure(text=f"⚠️ Warning: {friendly_msg}")
            self.warning_label.pack(anchor="w", pady=(0, 10))
        else:
            self.warning_label.pack_forget()

        self.grid_frame.pack(fill="x", expand=True)

        self.grid_frame.grid_columnconfigure(0, weight=3)
        self.grid_frame.grid_columnconfigure(1, weight=2)

        headers_sp = ["Calendar Configuration / Metric", "Value / Configuration"]
        for col_idx, head_text in enumerate(headers_sp):
            cell = ctk.CTkFrame(self.grid_frame, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=1, pady=1)
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        def yes_no(val):
            if val is None:
                return "Unavailable"
            return "Yes" if val else "No"

        # Formulate resources display
        rooms_err = data.get("RoomsError")
        devs_err = data.get("DevicesError")
        rooms_count = data.get("RoomsCount", 0)
        equip_count = data.get("EquipmentCount", 0)
        
        if rooms_err and devs_err:
            res_val = rooms_err
        else:
            r_str = "Error" if rooms_err else str(rooms_count)
            e_str = "Error" if devs_err else str(equip_count)
            tot = "Error" if (rooms_err or devs_err) else str(rooms_count + equip_count)
            res_val = f"Total: {tot} ({r_str} Rooms, {e_str} Equipment)"

        # Reserve rooms display
        reserve_val = data.get("CanUsersReserveRooms")
        if isinstance(reserve_val, bool):
             reserve_val = "Yes" if reserve_val else "No"

        # Attachments display
        att_val = data.get("CanShareAttachments")
        if isinstance(att_val, bool):
            attachments_val = "Yes" if att_val else "No"
        else:
            attachments_val = att_val

        rows_data = [
            ("Room & Resource Reservation", reserve_val),
            ("Calendar Resources", res_val),
            ("Resource Naming Convention", data.get("NamingConvention") or "None found"),
            ("Calendar Attachments Enabled", attachments_val),
        ]

        for r_idx, (metric_name, val) in enumerate(rows_data, start=1):
            bg_style = "transparent" if r_idx % 2 != 0 else COLOR_SURFACE_VARIANT
            
            c0 = ctk.CTkFrame(self.grid_frame, fg_color=bg_style, corner_radius=0)
            c0.grid(row=r_idx, column=0, sticky="nsew", padx=1, pady=1)
            ctk.CTkLabel(c0, text=metric_name, font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN, wraplength=450, justify="left", anchor="w").pack(padx=10, pady=6, anchor="w")

            c1 = ctk.CTkFrame(self.grid_frame, fg_color=bg_style, corner_radius=0)
            c1.grid(row=r_idx, column=1, sticky="nsew", padx=1, pady=1)
            ctk.CTkLabel(c1, text=val, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, wraplength=300, justify="left", anchor="w").pack(padx=10, pady=6, anchor="w")

        self.status = "success"
        self.on_status_change()

    def _render_error(self, err_msg):
        calendar_logger.warning(f"Calendar Telemetry fetch failed: {err_msg}")
        if hasattr(self, 'reload_btn') and self.reload_btn.winfo_exists():
            self.reload_btn.configure(state="normal")
        self._set_state_error(err_msg)
        self.status = "error"
        self.on_status_change()
