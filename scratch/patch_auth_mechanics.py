import re

def patch_auth_mechanics(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    # 1. Update reset_view
    if "self.auth_current_page = 0" not in content:
        content = content.replace(
            "self.labels_current_page = 0\n        self.retention_current_page = 0\n        self.last_labels_data = None\n        self.last_policies_data = None",
            "self.labels_current_page = 0\n        self.retention_current_page = 0\n        self.auth_current_page = 0\n        self.last_labels_data = None\n        self.last_policies_data = None\n        self.last_auth_data = None"
        )
    
    # 2. Replace _render_authentication_card completely
    target = """    def _render_authentication_card(self, auth_data: dict):
        self.auth_grid.configure(fg_color=COLOR_SURFACE, border_width=1, border_color=COLOR_OUTLINE_LIGHT, corner_radius=8)
        
        ca_policies = auth_data.get("ca_policies", [])
        
        headers = ["Policy Name", "State", "Target Users", "Target Apps", "Enforced Controls"]
        for i in range(5):
            self.auth_grid.grid_columnconfigure(i, weight=1)
            
        for col_idx, head_text in enumerate(headers):
            cell = ctk.CTkFrame(self.auth_grid, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        if not ca_policies:
            c0 = ctk.CTkFrame(self.auth_grid, fg_color=COLOR_SURFACE, corner_radius=0)
            c0.grid(row=1, column=0, columnspan=5, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c0, text="N/A (No Conditional Access Policies configured)", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=12, anchor="w")
        else:
            for r_idx, policy in enumerate(ca_policies, start=1):
                bg_style = COLOR_SURFACE if r_idx % 2 != 0 else COLOR_SURFACE_VARIANT
                
                vals = [
                    policy.get("name", "N/A"),
                    policy.get("state", "N/A"),
                    policy.get("users", "N/A"),
                    policy.get("apps", "N/A"),
                    policy.get("controls", "N/A")
                ]
                
                for c_idx, val in enumerate(vals):
                    c = ctk.CTkFrame(self.auth_grid, fg_color=bg_style, corner_radius=0)
                    c.grid(row=r_idx, column=c_idx, sticky="nsew", padx=0, pady=(0, 1))
                    ctk.CTkLabel(c, text=val, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="left", wraplength=180).pack(padx=10, pady=12, anchor="nw")"""

    replacement = """    def _render_authentication_card(self, auth_data: dict):
        self.auth_grid.configure(fg_color=COLOR_SURFACE, border_width=1, border_color=COLOR_OUTLINE_LIGHT, corner_radius=8)
        self.last_auth_data = auth_data.get("ca_policies", [])
        self.auth_current_page = 0
        self._update_auth_ui_paginated()

    def _update_auth_ui_paginated(self, data=None):
        if data is None:
            data = self.last_auth_data
            
        for w in self.auth_grid.winfo_children():
            info = w.grid_info()
            if "row" in info and int(info["row"]) > 0:
                w.destroy()

        headers = ["Policy Name", "State", "Target Users", "Target Apps", "Enforced Controls"]
        for i in range(5):
            self.auth_grid.grid_columnconfigure(i, weight=1)
            
        # Draw headers only if not present
        if not self.auth_grid.winfo_children():
            for col_idx, head_text in enumerate(headers):
                cell = ctk.CTkFrame(self.auth_grid, fg_color=COLOR_TONAL_BG, corner_radius=0)
                cell.grid(row=0, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
                ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        total_count = len(data) if data else 0
        start_idx = self.auth_current_page * self.ITEMS_PER_PAGE
        end_idx = start_idx + self.ITEMS_PER_PAGE
        page_data = data[start_idx:end_idx] if data else []

        if not data:
            c0 = ctk.CTkFrame(self.auth_grid, fg_color=COLOR_SURFACE, corner_radius=0)
            c0.grid(row=1, column=0, columnspan=5, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c0, text="N/A (No Conditional Access Policies configured)", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=12, anchor="w")
        else:
            for r_idx, policy in enumerate(page_data, start=1):
                bg_style = COLOR_SURFACE if r_idx % 2 != 0 else COLOR_SURFACE_VARIANT
                
                vals = [
                    policy.get("name", "N/A"),
                    policy.get("state", "N/A"),
                    policy.get("users", "N/A"),
                    policy.get("apps", "N/A"),
                    policy.get("controls", "N/A")
                ]
                
                for c_idx, val in enumerate(vals):
                    c = ctk.CTkFrame(self.auth_grid, fg_color=bg_style, corner_radius=0)
                    c.grid(row=r_idx, column=c_idx, sticky="nsew", padx=0, pady=(0, 1))
                    ctk.CTkLabel(c, text=val, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="left", wraplength=180).pack(padx=10, pady=12, anchor="nw")

        self._draw_auth_pagination_controls(total_count, data)

    def _draw_auth_pagination_controls(self, total_count, data):
        total_pages = max(1, (total_count + self.ITEMS_PER_PAGE - 1) // self.ITEMS_PER_PAGE)
        
        control_frame = ctk.CTkFrame(self.auth_grid, fg_color="transparent")
        control_frame.grid(row=self.ITEMS_PER_PAGE + 1, column=0, columnspan=5, pady=(5, 10), sticky="ew")
        
        left_spacer = ctk.CTkFrame(control_frame, fg_color="transparent")
        left_spacer.pack(side="left", fill="x", expand=True)
        center_container = ctk.CTkFrame(control_frame, fg_color="transparent")
        center_container.pack(side="left")

        prev_state = "normal" if self.auth_current_page > 0 else "disabled"
        btn_prev = ctk.CTkButton(
            center_container, text="◀ Prev", width=70, height=26, corner_radius=6,
            font=FONT_BODY_SMALL, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER, state=prev_state,
            command=lambda: self._change_auth_page(-1, data)
        )
        btn_prev.pack(side="left", padx=5)

        page_lbl = ctk.CTkLabel(center_container, text=f"Page {self.auth_current_page + 1} of {total_pages}", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB)
        page_lbl.pack(side="left", padx=15)

        next_state = "normal" if self.auth_current_page < total_pages - 1 else "disabled"
        btn_next = ctk.CTkButton(
            center_container, text="Next ▶", width=70, height=26, corner_radius=6,
            font=FONT_BODY_SMALL, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER, state=next_state,
            command=lambda: self._change_auth_page(1, data)
        )
        btn_next.pack(side="left", padx=5)

        right_spacer = ctk.CTkFrame(control_frame, fg_color="transparent")
        right_spacer.pack(side="right", fill="x", expand=True)

    def _change_auth_page(self, delta, data):
        self.auth_current_page += delta
        self._update_auth_ui_paginated(data)"""

    if "def _update_auth_ui_paginated" not in content:
        content = content.replace(target, replacement)
        
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)

if __name__ == "__main__":
    patch_auth_mechanics("/Users/srishtinegi/Desktop/Test/migration-planner/telemetry/data_security_governance.py")
