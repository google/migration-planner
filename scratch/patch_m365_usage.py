import re

def patch_m365_usage(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    # 1. Add ITEMS_PER_PAGE to init
    if "self.ITEMS_PER_PAGE = 5" not in content:
        content = content.replace(
            "self.status = None  # 'loading', 'success', 'error', None",
            "self.status = None  # 'loading', 'success', 'error', None\n        self.ITEMS_PER_PAGE = 5\n        self.current_page = 0\n        self.last_data = None"
        )
        
    # 2. Add self.current_page = 0 to reset_view
    if "self.current_page = 0" not in content.split("def reset_view")[1].split("def _set_state_loading")[0]:
        content = content.replace(
            "        for w in self.grid_frame.winfo_children():\n            w.destroy()",
            "        for w in self.grid_frame.winfo_children():\n            w.destroy()\n        self.current_page = 0\n        self.last_data = None"
        )

    # 3. Replace _render_success
    target_render = """    def _render_success(self, m365_data: list):
        self.m365_data = m365_data
        if hasattr(self, 'reload_btn') and self.reload_btn.winfo_exists():
            self.reload_btn.configure(state="normal")
        self.state_frame.pack_forget()
        for w in self.grid_frame.winfo_children():
            w.destroy()

        self.grid_frame.pack(fill="x", expand=True)

        for i in range(4):
            self.grid_frame.grid_columnconfigure(i, weight=1)

        headers_m365 = ["App / Platform", "Users Count", "App / Platform", "Users Count"]
        for col_idx, head_text in enumerate(headers_m365):
            cell = ctk.CTkFrame(self.grid_frame, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        if not m365_data:
            empty_cell = ctk.CTkFrame(self.grid_frame, fg_color="transparent")
            empty_cell.grid(row=1, column=0, columnspan=4, sticky="nsew", pady=15)
            ctk.CTkLabel(empty_cell, text="No M365 App usage data found.", text_color=COLOR_TEXT_SUB).pack()
        else:
            half = (len(m365_data) + 1) // 2
            left_col = m365_data[:half]
            right_col = m365_data[half:]

            for r_idx in range(half):
                bg_style = COLOR_SURFACE if r_idx % 2 == 0 else COLOR_SURFACE_VARIANT
                row_items = []

                if r_idx < len(left_col):
                    row_items.extend([left_col[r_idx][0], left_col[r_idx][1]])
                else:
                    row_items.extend(["", ""])

                if r_idx < len(right_col):
                    row_items.extend([right_col[r_idx][0], right_col[r_idx][1]])
                else:
                    row_items.extend(["", ""])

                for c_idx, val in enumerate(row_items):
                    cell = ctk.CTkFrame(self.grid_frame, fg_color=bg_style, corner_radius=0)
                    cell.grid(row=r_idx + 1, column=c_idx, sticky="nsew", padx=0, pady=(0, 1))
                    fnt = FONT_BODY_BOLD if c_idx in [0, 2] else FONT_BODY_MEDIUM
                    ctk.CTkLabel(cell, text=str(val), font=fnt, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=6, anchor="nw")

        self.status = "success"
        self.on_status_change()"""

    replace_render = """    def _render_success(self, m365_data: list):
        if hasattr(self, 'reload_btn') and self.reload_btn.winfo_exists():
            self.reload_btn.configure(state="normal")
        self.state_frame.pack_forget()
        self.grid_frame.pack(fill="x", expand=True)

        self.last_data = m365_data
        self.current_page = 0
        self._update_ui_paginated()

        self.status = "success"
        self.on_status_change()

    def _update_ui_paginated(self, data=None):
        if data is None:
            data = self.last_data

        for w in self.grid_frame.winfo_children():
            info = w.grid_info()
            if "row" in info and int(info["row"]) > 0:
                w.destroy()

        for i in range(2):
            self.grid_frame.grid_columnconfigure(i, weight=1)

        headers = ["App / Platform", "Users Count"]
        
        if not self.grid_frame.winfo_children():
            for col_idx, head_text in enumerate(headers):
                cell = ctk.CTkFrame(self.grid_frame, fg_color=COLOR_TONAL_BG, corner_radius=0)
                cell.grid(row=0, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
                ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        total_count = len(data) if data else 0
        start_idx = self.current_page * self.ITEMS_PER_PAGE
        end_idx = start_idx + self.ITEMS_PER_PAGE
        page_data = data[start_idx:end_idx] if data else []

        if not data:
            empty_cell = ctk.CTkFrame(self.grid_frame, fg_color="transparent")
            empty_cell.grid(row=1, column=0, columnspan=2, sticky="nsew", pady=15)
            ctk.CTkLabel(empty_cell, text="No M365 App usage data found.", text_color=COLOR_TEXT_SUB).pack()
        else:
            for r_idx, (platform, count) in enumerate(page_data, start=1):
                bg_style = COLOR_SURFACE if r_idx % 2 != 0 else COLOR_SURFACE_VARIANT
                vals = [platform, count]
                for c_idx, val in enumerate(vals):
                    cell = ctk.CTkFrame(self.grid_frame, fg_color=bg_style, corner_radius=0)
                    cell.grid(row=r_idx, column=c_idx, sticky="nsew", padx=0, pady=(0, 1))
                    fnt = FONT_BODY_BOLD if c_idx == 0 else FONT_BODY_MEDIUM
                    ctk.CTkLabel(cell, text=str(val), font=fnt, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=6, anchor="nw")

        self._draw_pagination_controls(total_count, data)

    def _draw_pagination_controls(self, total_count, data):
        total_pages = max(1, (total_count + self.ITEMS_PER_PAGE - 1) // self.ITEMS_PER_PAGE)
        
        control_frame = ctk.CTkFrame(self.grid_frame, fg_color="transparent")
        control_frame.grid(row=self.ITEMS_PER_PAGE + 1, column=0, columnspan=2, pady=(5, 10), sticky="ew")
        
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

        page_lbl = ctk.CTkLabel(center_container, text=f"Page {self.current_page + 1} of {total_pages}", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB)
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

    if "def _update_ui_paginated" not in content:
        content = content.replace(target_render, replace_render)
        
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)

if __name__ == "__main__":
    patch_m365_usage("/Users/srishtinegi/Desktop/Test/migration-planner/telemetry/active_users_usage.py")
