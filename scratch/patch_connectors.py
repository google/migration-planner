import re
import sys

def patch_file(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    # 1. Update __init__
    init_target = """        self.status = None

        self.build_ui()"""
    init_replacement = """        self.status = None
        self.current_page = 0
        self.ITEMS_PER_PAGE = 5
        self.last_data = []

        self.build_ui()"""
    content = content.replace(init_target, init_replacement)

    # 2. Extract unified data in _handle_result
    handle_result_target = """                self.status = "success"
                self.grid_frame.pack(fill="x", expand=True)
                self._render_card(connectors_data)"""
    handle_result_replacement = """                self.status = "success"
                self.grid_frame.pack(fill="x", expand=True)
                
                unified_data = []
                inbound = connectors_data.get("InboundConnectors", [])
                outbound = connectors_data.get("OutboundConnectors", [])
                
                for conn in inbound:
                    unified_data.append({
                        "Direction": "📥 Inbound",
                        "Name": conn.get("Name", "N/A"),
                        "Status": "🟢 Enabled" if conn.get("Enabled") else "🔴 Disabled",
                        "Domains": conn.get("SenderDomains", "N/A") or "N/A",
                        "Routing": f"Type: {conn.get('ConnectorType', 'N/A')}\\nRequire TLS: {'Yes' if conn.get('RequireTls') else 'No'}"
                    })
                    
                for conn in outbound:
                    unified_data.append({
                        "Direction": "📤 Outbound",
                        "Name": conn.get("Name", "N/A"),
                        "Status": "🟢 Enabled" if conn.get("Enabled") else "🔴 Disabled",
                        "Domains": conn.get("RecipientDomains", "N/A") or "N/A",
                        "Routing": f"SmartHosts: {conn.get('SmartHosts', 'N/A')}\\nUse MX: {'Yes' if conn.get('UseMxRecord') else 'No'}"
                    })
                
                self.last_data = unified_data
                self.current_page = 0
                self._update_ui_paginated(self.last_data)"""
    content = content.replace(handle_result_target, handle_result_replacement)

    # 3. Rename _render_card to _update_ui_paginated and implement pagination
    render_card_target = """    def _render_card(self, connectors_data: dict):
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
                    ctk.CTkLabel(c, text=val, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="left", wraplength=180).pack(padx=10, pady=12, anchor="nw")"""

    render_card_replacement = """    def _update_ui_paginated(self, data):
        for w in self.grid_frame.winfo_children():
            w.destroy()

        if not data:
            self.grid_frame.configure(fg_color=COLOR_SURFACE, border_width=1, border_color=COLOR_OUTLINE_LIGHT, corner_radius=8)
            ctk.CTkLabel(self.grid_frame, text="N/A (No Exchange Connectors configured)", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB).pack(padx=20, pady=20, anchor="w")
            return

        headers_in = ["Direction", "Connector Name", "Status", "Domains", "Routing Config"]
        for i in range(5):
            self.grid_frame.grid_columnconfigure(i, weight=1)

        for col_idx, head_text in enumerate(headers_in):
            cell = ctk.CTkFrame(self.grid_frame, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        total_count = len(data)
        start_idx = self.current_page * self.ITEMS_PER_PAGE
        end_idx = start_idx + self.ITEMS_PER_PAGE
        page_data = data[start_idx:end_idx]

        for r_idx, conn in enumerate(page_data, start=1):
            bg_style = COLOR_SURFACE if r_idx % 2 != 0 else COLOR_SURFACE_VARIANT
            vals = [
                conn["Direction"],
                conn["Name"],
                conn["Status"],
                conn["Domains"],
                conn["Routing"]
            ]
            for c_idx, val in enumerate(vals):
                c = ctk.CTkFrame(self.grid_frame, fg_color=bg_style, corner_radius=0)
                c.grid(row=r_idx, column=c_idx, sticky="nsew", padx=0, pady=(0, 1))
                ctk.CTkLabel(c, text=val, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="left", wraplength=180).pack(padx=10, pady=12, anchor="nw")

        if total_count > self.ITEMS_PER_PAGE:
            self._draw_pagination_controls(total_count, data)

    def _draw_pagination_controls(self, total_count, data):
        total_pages = (total_count + self.ITEMS_PER_PAGE - 1) // self.ITEMS_PER_PAGE
        
        control_frame = ctk.CTkFrame(self.grid_frame, fg_color="transparent")
        control_frame.grid(row=self.ITEMS_PER_PAGE + 2, column=0, columnspan=5, pady=(5, 10), sticky="ew")

        left_spacer = ctk.CTkFrame(control_frame, fg_color="transparent")
        left_spacer.pack(side="left", fill="x", expand=True)

        center_container = ctk.CTkFrame(control_frame, fg_color="transparent")
        center_container.pack(side="left")

        prev_state = "normal" if self.current_page > 0 else "disabled"
        btn_prev = ctk.CTkButton(
            center_container, text="◀ Prev", width=70, height=26, corner_radius=6,
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
            center_container, text="Next ▶", width=70, height=26, corner_radius=6,
            font=FONT_BODY_SMALL, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER, state=next_state,
            command=lambda: self._change_page(1, data)
        )
        btn_next.pack(side="left", padx=5)

        right_spacer = ctk.CTkFrame(control_frame, fg_color="transparent")
        right_spacer.pack(side="right", fill="x", expand=True)

    def _change_page(self, delta, data):
        self.current_page += delta
        self._update_ui_paginated(data)"""

    content = content.replace(render_card_target, render_card_replacement)

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)

if __name__ == "__main__":
    patch_file("/Users/srishtinegi/Desktop/Test/migration-planner/telemetry/exchange_connectors_ui.py")
