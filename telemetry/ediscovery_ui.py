import customtkinter as ctk
import time
import threading
import logging
from telemetry.styles import *
from core.graph.delegated_auth import DelegatedAuthClient
from core.graph.ediscovery import EDiscoveryFetcher

logger = logging.getLogger("M365Telemetry.EDiscoveryUI")

class EDiscoveryFrame(ctk.CTkFrame):
    """Component for rendering Microsoft Purview eDiscovery Cases using Delegated Auth."""
    
    def __init__(self, master, log_callback, credentials_callback, status_change_callback, **kwargs):
        self.semaphore = kwargs.pop("concurrency_semaphore", None)
        self.get_delegated_auth = kwargs.pop("delegated_auth_callback", None)
        super().__init__(master, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=12, **kwargs)
        self.log_msg = log_callback
        self.get_credentials = credentials_callback
        self.on_status_change_cb = status_change_callback
        self.status = None
        self.error_msg = None
        self.loading = False
        
        self.page_index = 0
        self.page_size = 5
        self.last_data = []
        
        self.build_ui()

    def build_ui(self):
        self.pack(fill="x", expand=True, pady=10)
        
        self.inner_pad = ctk.CTkFrame(self, fg_color="transparent")
        self.inner_pad.pack(fill="both", expand=True, padx=20, pady=20)
        
        self.header = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.header.pack(fill="x", pady=(0, 10))
        
        self.title_lbl = ctk.CTkLabel(
            self.header,
            text="Microsoft Purview eDiscovery Cases",
            font=FONT_HEADER_SMALL,
            text_color=COLOR_TEXT_MAIN
        )
        self.title_lbl.pack(side="left")
        
        self.reload_btn = ctk.CTkButton(
            self.header, text="↻ Reload", width=80, height=28, corner_radius=6,
            fg_color="transparent", border_width=1, border_color=COLOR_PRIMARY,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER,
            command=self._retry_fetch
        )
        self.reload_btn.pack(side="right")
        
        self.state_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        
        self.grid_frame = ctk.CTkFrame(self.inner_pad, fg_color=COLOR_SURFACE, corner_radius=12, border_width=1, border_color=COLOR_OUTLINE_LIGHT)
        self.grid_frame.pack_forget()
        
        self.pagination_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.pagination_frame.pack_forget()
        
        self.loading_label = None
        self.progress = None
        self.render_ui_state()

    def trigger_fetch(self, tenant, client_id, secret, use_delegated_auth=False):
        if not use_delegated_auth:
            self.error_msg = "eDiscovery scanning requires Delegated Authentication. Please recreate the connection with Delegated Auth enabled."
            self.status = "error"
            self.render_ui_state()
            return

        self.pack(fill="x", expand=True, pady=(0, 5))
        self.status = "loading"
        self.loading = True
        self.error_msg = None
        self.render_ui_state()
        threading.Thread(target=self._fetch_data, args=(tenant, client_id, secret), daemon=True).start()

    def _fetch_data(self, tenant, client_id, secret):
        if self.semaphore:
            self.semaphore.acquire()
        try:
            if hasattr(self, 'loading_label') and self.loading_label.winfo_exists():
                self.loading_label.configure(text="⏳ Authenticating via MSAL...")
                
            auth_client = DelegatedAuthClient(tenant, client_id, secret)
            token = auth_client.get_token(scopes=["https://graph.microsoft.com/.default"])
            
            if not token:
                raise Exception("Failed to acquire delegated token. User may have cancelled or app is misconfigured.")
                
            if hasattr(self, 'loading_label') and self.loading_label.winfo_exists():
                self.loading_label.configure(text="⏳ Fetching eDiscovery Cases...")
                
            import os
            csv_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'reports', f"{tenant}_{client_id}")
            os.makedirs(csv_dir, exist_ok=True)
            self.csv_path = os.path.join(csv_dir, "ediscovery_cases.csv")
            self.total_items = 0
            
            def update_progress(items):
                self.total_items += len(items)
                if hasattr(self, 'loading_label') and self.loading_label.winfo_exists():
                    self.loading_label.configure(text=f"⏳ Fetched {self.total_items} eDiscovery cases...")
                    
            fetcher = EDiscoveryFetcher(token)
            res = fetcher.fetch_cases(csv_path=self.csv_path, on_page_callback=update_progress)
            
            if not res.get("success", False):
                raise Exception(res.get("error", "Unknown error fetching eDiscovery cases"))
                
            self.last_data = [] # Free memory
            self.after(0, self._render_success)
            
        except Exception as e:
            logger.error(f"Error fetching eDiscovery cases: {e}", exc_info=True)
            self.after(0, self._render_error, str(e))
        finally:
            if self.semaphore:
                self.semaphore.release()

    def _render_success(self):
        self.status = "success"
        self.loading = False
        self.page_index = 0
        self.render_ui_state()
        if self.on_status_change_cb:
            self.on_status_change_cb()

    def _render_error(self, err_msg):
        self.error_msg = err_msg
        self.status = "error"
        self.loading = False
        self.render_ui_state()
        if self.on_status_change_cb:
            self.on_status_change_cb()

    def reset_view(self):
        self.pack_forget()
        self.state_frame.pack_forget()
        self.grid_frame.pack_forget()
        self.pagination_frame.pack_forget()
        self.status = None
        self.error_msg = None

    def _retry_fetch(self):
        tenant, clients, secrets = self.get_credentials()
        use_delegated = self.get_delegated_auth() if self.get_delegated_auth else False
        if tenant and clients and secrets:
            if hasattr(self, 'reload_btn') and self.reload_btn.winfo_exists():
                self.reload_btn.configure(state="disabled")
            self.trigger_fetch(tenant, clients[0], secrets[0], use_delegated_auth=use_delegated)

    def render_ui_state(self):
        self.state_frame.pack_forget()
        self.grid_frame.pack_forget()
        self.pagination_frame.pack_forget()

        for widget in self.state_frame.winfo_children():
            widget.destroy()
        for widget in self.grid_frame.winfo_children():
            widget.destroy()
        for widget in self.pagination_frame.winfo_children():
            widget.destroy()
            
        if not self.loading and hasattr(self, 'reload_btn') and self.reload_btn.winfo_exists():
            self.reload_btn.configure(state="normal")
            
        if self.loading:
            self.loading_label = ctk.CTkLabel(self.state_frame, text="⏳ Initializing...", text_color="#6b7280", font=ctk.CTkFont(family="Segoe UI", size=13))
            self.loading_label.pack(pady=(20, 5))
            self.progress = ctk.CTkProgressBar(self.state_frame, mode="indeterminate", width=250, fg_color=COLOR_SURFACE_VARIANT, progress_color=COLOR_PRIMARY)
            self.progress.pack(pady=(0, 20))
            self.progress.start()
            self.state_frame.pack(fill="x", expand=True)
            return
            
        if self.error_msg:
            ctk.CTkLabel(self.state_frame, text=f"✖ {self.error_msg}", text_color=COLOR_ERROR, font=FONT_BODY_MEDIUM, justify="center", wraplength=700).pack(pady=(20, 5))
            ctk.CTkButton(self.state_frame, text="Try Again", command=self._retry_fetch, width=120, fg_color="transparent", border_width=1, text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER).pack(pady=(0, 20))
            self.state_frame.pack(fill="x", expand=True)
            return
            
        self.grid_frame.pack(fill="x", pady=(0, 10))
        self.pagination_frame.pack(fill="x", pady=(5, 0))
        self._update_ui_paginated()

    def _update_ui_paginated(self):
        for widget in self.grid_frame.winfo_children():
            widget.destroy()
        for widget in self.pagination_frame.winfo_children():
            widget.destroy()

        total_items = getattr(self, 'total_items', 0)

        if total_items == 0:
            c = ctk.CTkFrame(self.grid_frame, fg_color=COLOR_SURFACE, corner_radius=0)
            c.pack(fill="x", expand=True)
            ctk.CTkLabel(c, text="No eDiscovery Cases discovered.", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="center").pack(padx=10, pady=20)
            return

        metrics_grid = ctk.CTkFrame(self.grid_frame, fg_color=COLOR_SURFACE, corner_radius=0)
        metrics_grid.pack(fill="x", padx=10, pady=(5, 5))
        
        headers = ["Display Name", "Status", "Created DateTime", "Closed By"]
        for i in range(4):
            metrics_grid.grid_columnconfigure(i, weight=1 if i > 0 else 2)
            
        for col_idx, head_text in enumerate(headers):
            cell = ctk.CTkFrame(metrics_grid, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        start_idx = self.page_index * self.page_size
        end_idx = min(start_idx + self.page_size, total_items)
        
        page_items = []
        if getattr(self, 'csv_path', None):
            try:
                import csv
                with open(self.csv_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for i, row in enumerate(reader):
                        if i >= start_idx and i < end_idx:
                            page_items.append(row)
                        elif i >= end_idx:
                            break
            except Exception:
                pass

        for r_idx, case in enumerate(page_items, start=1):
            bg_style = COLOR_SURFACE if r_idx % 2 != 0 else COLOR_SURFACE_VARIANT
            
            c0 = ctk.CTkFrame(metrics_grid, fg_color=bg_style, corner_radius=0)
            c0.grid(row=r_idx, column=0, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c0, text=str(case.get("displayName", "-")), font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="left", wraplength=250).pack(padx=10, pady=12, anchor="w")
            
            c1 = ctk.CTkFrame(metrics_grid, fg_color=bg_style, corner_radius=0)
            c1.grid(row=r_idx, column=1, sticky="nsew", padx=0, pady=(0, 1))
            status_text = case.get("status", "-")
            status_color = "#16a34a" if status_text.lower() == "active" else COLOR_TEXT_SUB
            ctk.CTkLabel(c1, text=status_text, font=FONT_BODY_MEDIUM, text_color=status_color).pack(padx=10, pady=12, anchor="w")
            
            c2 = ctk.CTkFrame(metrics_grid, fg_color=bg_style, corner_radius=0)
            c2.grid(row=r_idx, column=2, sticky="nsew", padx=0, pady=(0, 1))
            dt = str(case.get("createdDateTime", "-")).split("T")[0]
            ctk.CTkLabel(c2, text=dt, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=12, anchor="w")
            
            c3 = ctk.CTkFrame(metrics_grid, fg_color=bg_style, corner_radius=0)
            c3.grid(row=r_idx, column=3, sticky="nsew", padx=0, pady=(0, 1))
            closed_by = case.get("closedBy", {})
            user_info = closed_by.get("user", {}) if closed_by else {}
            user_disp = user_info.get("displayName", "-")
            ctk.CTkLabel(c3, text=user_disp, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=12, anchor="w")

        if total_items >= 0:
            total_pages = max(1, (total_items + self.page_size - 1) // self.page_size)
            
            center_container = ctk.CTkFrame(self.pagination_frame, fg_color="transparent")
            center_container.pack(pady=(5, 10))
            
            prev_btn = ctk.CTkButton(center_container, text="◀ Prev", width=70, height=22, corner_radius=6,
                                     font=FONT_BODY_SMALL, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
                                     text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER,
                                     state="normal" if self.page_index > 0 else "disabled",
                                     command=self._prev_page)
            prev_btn.pack(side="left", padx=5)
            
            ctk.CTkLabel(center_container, text=f"Page {self.page_index + 1} of {total_pages}", 
                         font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB).pack(side="left", padx=15)
            
            next_btn = ctk.CTkButton(center_container, text="Next ▶", width=70, height=22, corner_radius=6,
                                     font=FONT_BODY_SMALL, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
                                     text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER,
                                     state="normal" if self.page_index < total_pages - 1 else "disabled",
                                     command=self._next_page)
            next_btn.pack(side="left", padx=5)

    def _prev_page(self):
        if self.page_index > 0:
            self.page_index -= 1
            self._update_ui_paginated()

    def _next_page(self):
        total_items = len(self.last_data)
        total_pages = max(1, (total_items + self.page_size - 1) // self.page_size)
        if self.page_index < total_pages - 1:
            self.page_index += 1
            self._update_ui_paginated()
