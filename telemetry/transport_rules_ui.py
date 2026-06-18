import customtkinter as ctk
import time
import threading
import logging
from telemetry.styles import *
from core.powershell.transport_rules import TransportRulesFetcher

usage_logger = logging.getLogger("M365TelemetryAsyncLogger.TransportRulesUI")

class TransportRulesFrame(ctk.CTkFrame):
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
        
        self.last_data = []
        self.page_index = 0
        self.page_size = 5
        
        self.inner_pad = ctk.CTkFrame(self, fg_color="transparent")
        self.inner_pad.pack(fill="both", expand=True, padx=20, pady=20)
        
        self.header = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.header.pack(fill="x", pady=(0, 10))
        ctk.CTkLabel(self.header, text="Exchange Transport Rules", font=FONT_HEADER_SMALL, text_color=COLOR_TEXT_MAIN).pack(side="left")
        
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
        
        self.grid_frame = ctk.CTkFrame(self.inner_pad, fg_color=COLOR_SURFACE, corner_radius=12, border_width=1, border_color=COLOR_OUTLINE_LIGHT)
        self.grid_frame.pack_forget()
        
        self.pagination_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        self.pagination_frame.pack_forget()
        
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

            self.update_loading_text("Fetching Exchange Transport Rules via PowerShell...")
            
            fetcher = TransportRulesFetcher(tenant, c_id, c_secret)
            res = fetcher.fetch_rules()
            
            if res.get("Errors") and not res.get("TransportRules"):
                errs = res["Errors"]
                first_err = list(errs.values())[0] if errs else "Unknown Script Error"
                raise ConnectionError(f"PowerShell Execution Error: {first_err}")
            
            data = res.get("TransportRules", [])
            
            # Export to CSV
            try:
                import os, csv
                script_dir = os.path.dirname(os.path.abspath(__file__))
                reports_dir = os.path.join(script_dir, "reports", f"{tenant}_{c_id}")
                os.makedirs(reports_dir, exist_ok=True)
                csv_path = os.path.join(reports_dir, "exchange_transport_rules.csv")
                
                with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                    if data:
                        writer = csv.DictWriter(f, fieldnames=data[0].keys())
                        writer.writeheader()
                        writer.writerows(data)
                    else:
                        writer = csv.writer(f)
                        writer.writerow(["No rules found"])
            except Exception as e:
                usage_logger.error(f"Failed to write transport rules CSV: {e}")

            self.after(0, self._render_success, data)
            
        except Exception as e:
            usage_logger.error(f"Error fetching transport rules: {e}", exc_info=True)
            self.after(0, self._render_error, str(e))
        finally:
            if self.semaphore:
                self.semaphore.release()

    def _render_success(self, data):
        self.last_data = data
        self.status = "success"
        self.loading = False
        self.page_index = 0
        self.on_status_change()

    def _render_error(self, err_msg):
        self._set_state_error(err_msg)

    def _set_state_error(self, error_msg):
        self.error_msg = error_msg
        self.status = "error"
        self.loading = False
        self.on_status_change()

    def reset_view(self):
        self.pack_forget()
        self.state_frame.pack_forget()
        self.grid_frame.pack_forget()
        self.pagination_frame.pack_forget()
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
            self.loading_label = ctk.CTkLabel(self.state_frame, text="⏳ Initializing...", text_color="#6b7280", font=__import__("customtkinter").CTkFont(family="Segoe UI", size=13))
            self.loading_label.pack(pady=(20, 5))
            self.progress = ctk.CTkProgressBar(self.state_frame, mode="indeterminate", width=250, fg_color=COLOR_SURFACE_VARIANT, progress_color=COLOR_PRIMARY)
            self.progress.pack(pady=(0, 20))
            self.progress.start()
            self.state_frame.pack(fill="x", expand=True)
            return
            
        if self.error_msg:
            ctk.CTkLabel(self.state_frame, text=f"✖ {self.error_msg}", text_color=COLOR_ERROR, font=FONT_BODY_MEDIUM, justify="center").pack(pady=(20, 5))
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

        if not self.last_data:
            c = ctk.CTkFrame(self.grid_frame, fg_color=COLOR_SURFACE, corner_radius=0)
            c.pack(fill="x", expand=True)
            ctk.CTkLabel(c, text="No Transport Rules discovered.", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="center").pack(padx=10, pady=20)
            return

        metrics_grid = ctk.CTkFrame(self.grid_frame, fg_color=COLOR_SURFACE, corner_radius=0)
        metrics_grid.pack(fill="x", padx=10, pady=(5, 5))
        
        headers = ["Rule Name", "State", "Priority", "Mode", "Rule Logic"]
        for i in range(5):
            if i == 0:
                weight = 2
            elif i == 4:
                weight = 4
            else:
                weight = 1
            metrics_grid.grid_columnconfigure(i, weight=weight)
            
        for col_idx, head_text in enumerate(headers):
            cell = ctk.CTkFrame(metrics_grid, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        total_items = len(self.last_data)
        start_idx = self.page_index * self.page_size
        end_idx = min(start_idx + self.page_size, total_items)
        page_items = self.last_data[start_idx:end_idx]

        for r_idx, rule in enumerate(page_items, start=1):
            bg_style = COLOR_SURFACE if r_idx % 2 != 0 else COLOR_SURFACE_VARIANT
            
            c0 = ctk.CTkFrame(metrics_grid, fg_color=bg_style, corner_radius=0)
            c0.grid(row=r_idx, column=0, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c0, text=str(rule.get("Name", "-")), font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="left", wraplength=350).pack(padx=10, pady=12, anchor="w")
            
            c1 = ctk.CTkFrame(metrics_grid, fg_color=bg_style, corner_radius=0)
            c1.grid(row=r_idx, column=1, sticky="nsew", padx=0, pady=(0, 1))
            status_text = rule.get("State", "-")
            status_color = "#16a34a" if status_text == "Enabled" else COLOR_TEXT_SUB
            ctk.CTkLabel(c1, text=status_text, font=FONT_BODY_MEDIUM, text_color=status_color).pack(padx=10, pady=12, anchor="w")
            
            c2 = ctk.CTkFrame(metrics_grid, fg_color=bg_style, corner_radius=0)
            c2.grid(row=r_idx, column=2, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c2, text=str(rule.get("Priority", "-")), font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=12, anchor="w")
            
            c3 = ctk.CTkFrame(metrics_grid, fg_color=bg_style, corner_radius=0)
            c3.grid(row=r_idx, column=3, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c3, text=str(rule.get("Mode", "-")), font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=12, anchor="w")

            c4 = ctk.CTkFrame(metrics_grid, fg_color=bg_style, corner_radius=0)
            c4.grid(row=r_idx, column=4, sticky="nsew", padx=0, pady=(0, 1))
            
            desc_text = rule.get("Description", "")
            if desc_text:
                desc_text = desc_text.strip()
            else:
                desc_text = "N/A"
                
            textbox = ctk.CTkTextbox(c4, height=85, fg_color="transparent", text_color=COLOR_TEXT_MAIN, font=FONT_BODY_MEDIUM, wrap="word")
            textbox.pack(fill="both", expand=True, padx=5, pady=5)
            textbox.insert("0.0", desc_text)
            textbox.configure(state="disabled")

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
        total_pages = (len(self.last_data) + self.page_size - 1) // self.page_size
        if self.page_index < total_pages - 1:
            self.page_index += 1
            self._update_ui_paginated()
