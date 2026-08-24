import re
import sys

def patch_file(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    # 1. Update __init__
    init_target = """        self.status = None
        self.last_data = {}

        self.build_ui()"""
    init_replacement = """        self.status = None
        self.last_data = {}
        self.current_page = 0
        self.ITEMS_PER_PAGE = 5

        self.build_ui()"""
    content = content.replace(init_target, init_replacement)

    # 2. Update reset_view and trigger_fetch
    trigger_fetch_target = """    def trigger_fetch(self, tenant, client_id, client_secret):
        if self.status == "loading":
            return
            
        self.status = "loading"
        self.is_cancelled = False"""
    trigger_fetch_replacement = """    def trigger_fetch(self, tenant, client_id, client_secret):
        if self.status == "loading":
            return
            
        self.status = "loading"
        self.is_cancelled = False
        self.current_page = 0"""
    content = content.replace(trigger_fetch_target, trigger_fetch_replacement)

    # 3. Rename _update_ui_lists calls
    content = content.replace("self._update_ui_lists(data)", "self._update_ui_lists_paginated(data)")

    # 4. _update_ui_lists rewrite
    update_ui_target = """        rows_data = data.get("table_rows", [])
        
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

        ctk.CTkLabel(self.grid_frame, text="* Based on sample data collected from Intune.", font=FONT_BODY_SMALL, text_color=COLOR_TEXT_SUB).pack(anchor="w", padx=10, pady=(0, 15))"""
    
    update_ui_replacement = """        rows_data = data.get("table_rows", [])
        
        if not rows_data:
            c = ctk.CTkFrame(metrics_grid, fg_color=COLOR_SURFACE, corner_radius=0)
            c.grid(row=1, column=0, columnspan=3, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c, text="No policies detected.", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="center").pack(padx=10, pady=12)
            return
            
        total_count = len(rows_data)
        start_idx = self.current_page * self.ITEMS_PER_PAGE
        end_idx = start_idx + self.ITEMS_PER_PAGE
        page_data = rows_data[start_idx:end_idx]

        for r_idx, (platform, p_type, count) in enumerate(page_data, start=1):
            bg_style = COLOR_SURFACE if r_idx % 2 != 0 else COLOR_SURFACE_VARIANT
            
            vals = [platform, p_type, count]
            
            for c_idx, val in enumerate(vals):
                c = ctk.CTkFrame(metrics_grid, fg_color=bg_style, corner_radius=0)
                c.grid(row=r_idx, column=c_idx, sticky="nsew", padx=0, pady=(0, 1))
                ctk.CTkLabel(c, text=val, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="left", wraplength=450).pack(padx=10, pady=12, anchor="nw")

        if total_count > self.ITEMS_PER_PAGE:
            self._draw_pagination_controls(total_count, data)

        ctk.CTkLabel(self.grid_frame, text="* Based on sample data collected from Intune.", font=FONT_BODY_SMALL, text_color=COLOR_TEXT_SUB).pack(anchor="w", padx=10, pady=(0, 15))

    def _draw_pagination_controls(self, total_count, data):
        total_pages = (total_count + self.ITEMS_PER_PAGE - 1) // self.ITEMS_PER_PAGE
        
        control_frame = ctk.CTkFrame(self.grid_frame, fg_color="transparent")
        control_frame.pack(fill="x", pady=(5, 10))

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
        self._update_ui_lists_paginated(data)"""

    content = content.replace("def _update_ui_lists(self, data: dict):", "def _update_ui_lists_paginated(self, data: dict):")
    content = content.replace(update_ui_target, update_ui_replacement)

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)

if __name__ == "__main__":
    patch_file("/Users/srishtinegi/Desktop/Test/migration-planner/telemetry/intune_policies.py")
