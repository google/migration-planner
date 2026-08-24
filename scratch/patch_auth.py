import re
import sys

def patch_file(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    # 1. Update __init__
    init_target = """        self.status = None
        self.last_data = []
        self.is_cancelled = False
        self.current_request_id = 0

        self.build_ui()"""
    init_replacement = """        self.status = None
        self.last_data = []
        self.is_cancelled = False
        self.current_request_id = 0
        self.current_page = 0
        self.ITEMS_PER_PAGE = 5

        self.build_ui()"""
    content = content.replace(init_target, init_replacement)

    # 2. Update trigger_fetch
    trigger_fetch_target = """    def trigger_fetch(self, tenant, client_id, client_secret):
        self.status = "loading"
        self.is_cancelled = False
        self.current_request_id += 1"""
    trigger_fetch_replacement = """    def trigger_fetch(self, tenant, client_id, client_secret):
        self.status = "loading"
        self.is_cancelled = False
        self.current_request_id += 1
        self.current_page = 0"""
    content = content.replace(trigger_fetch_target, trigger_fetch_replacement)

    # 3. Rename _update_ui calls
    content = content.replace("self._update_ui(data, is_partial=True)", "self._update_ui_paginated(data, is_partial=True)")
    content = content.replace("self._update_ui(data, is_partial=False)", "self._update_ui_paginated(data, is_partial=False)")
    content = content.replace("self._update_ui(self.last_data)", "self._update_ui_paginated(self.last_data)")

    # 4. Replace _update_ui implementation
    update_ui_target = """    def _update_ui(self, data, is_partial=False):
        for w in self.body_frame.winfo_children():
            w.destroy()

        if is_partial:
            progress_frame = ctk.CTkFrame(self.body_frame, fg_color=COLOR_TONAL_BG, height=26, corner_radius=6)
            progress_frame.pack(fill="x", pady=(0, 6))
            ctk.CTkLabel(
                progress_frame,
                text="⏳ Querying Authentication Methods in the background... UI will auto-refresh.",
                font=FONT_BODY_SMALL,
                text_color=COLOR_TONAL_TEXT
            ).pack(padx=10, pady=2, anchor="w")

        metrics_grid = ctk.CTkFrame(self.body_frame, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)
        metrics_grid.pack(fill="x", pady=(5, 10))

        headers = ["Authentication Method", "Success Activity Count"]
        for i in range(2):
            metrics_grid.grid_columnconfigure(i, weight=1)

        for col_idx, head_text in enumerate(headers):
            cell = ctk.CTkFrame(metrics_grid, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        if not data:
            c = ctk.CTkFrame(metrics_grid, fg_color=COLOR_SURFACE, corner_radius=0)
            c.grid(row=1, column=0, columnspan=2, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c, text="No authentication activity detected.", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="center").pack(padx=10, pady=12)
        else:
            for r_idx, (method, activity) in enumerate(data, start=1):
                bg_style = COLOR_SURFACE if r_idx % 2 != 0 else COLOR_SURFACE_VARIANT
                vals = [method, activity]
                for c_idx, val in enumerate(vals):
                    c = ctk.CTkFrame(metrics_grid, fg_color=bg_style, corner_radius=0)
                    c.grid(row=r_idx, column=c_idx, sticky="nsew", padx=0, pady=(0, 1))
                    ctk.CTkLabel(c, text=val, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="left").pack(padx=10, pady=8, anchor="nw")"""

    update_ui_replacement = """    def _update_ui_paginated(self, data, is_partial=False):
        for w in self.body_frame.winfo_children():
            w.destroy()

        if is_partial:
            progress_frame = ctk.CTkFrame(self.body_frame, fg_color=COLOR_TONAL_BG, height=26, corner_radius=6)
            progress_frame.pack(fill="x", pady=(0, 6))
            ctk.CTkLabel(
                progress_frame,
                text="⏳ Querying Authentication Methods in the background... UI will auto-refresh.",
                font=FONT_BODY_SMALL,
                text_color=COLOR_TONAL_TEXT
            ).pack(padx=10, pady=2, anchor="w")

        metrics_grid = ctk.CTkFrame(self.body_frame, fg_color=COLOR_SURFACE, border_color=COLOR_OUTLINE_LIGHT, border_width=1, corner_radius=8)
        metrics_grid.pack(fill="x", pady=(5, 10))

        headers = ["Authentication Method", "Success Activity Count"]
        for i in range(2):
            metrics_grid.grid_columnconfigure(i, weight=1)

        for col_idx, head_text in enumerate(headers):
            cell = ctk.CTkFrame(metrics_grid, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        if not data:
            c = ctk.CTkFrame(metrics_grid, fg_color=COLOR_SURFACE, corner_radius=0)
            c.grid(row=1, column=0, columnspan=2, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c, text="No authentication activity detected.", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="center").pack(padx=10, pady=12)
        else:
            total_count = len(data)
            start_idx = self.current_page * self.ITEMS_PER_PAGE
            end_idx = start_idx + self.ITEMS_PER_PAGE
            page_data = data[start_idx:end_idx]

            for r_idx, (method, activity) in enumerate(page_data, start=1):
                bg_style = COLOR_SURFACE if r_idx % 2 != 0 else COLOR_SURFACE_VARIANT
                vals = [method, activity]
                for c_idx, val in enumerate(vals):
                    c = ctk.CTkFrame(metrics_grid, fg_color=bg_style, corner_radius=0)
                    c.grid(row=r_idx, column=c_idx, sticky="nsew", padx=0, pady=(0, 1))
                    ctk.CTkLabel(c, text=val, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, justify="left").pack(padx=10, pady=8, anchor="nw")

            if total_count > self.ITEMS_PER_PAGE:
                self._draw_pagination_controls(total_count, data, is_partial)

    def _draw_pagination_controls(self, total_count, data, is_partial):
        total_pages = (total_count + self.ITEMS_PER_PAGE - 1) // self.ITEMS_PER_PAGE
        
        control_frame = ctk.CTkFrame(self.body_frame, fg_color="transparent")
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
            command=lambda: self._change_page(-1, data, is_partial)
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
            command=lambda: self._change_page(1, data, is_partial)
        )
        btn_next.pack(side="left", padx=5)

        right_spacer = ctk.CTkFrame(control_frame, fg_color="transparent")
        right_spacer.pack(side="right", fill="x", expand=True)

    def _change_page(self, delta, data, is_partial):
        self.current_page += delta
        self._update_ui_paginated(data, is_partial)"""

    content = content.replace(update_ui_target, update_ui_replacement)

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)

if __name__ == "__main__":
    patch_file("/Users/srishtinegi/Desktop/Test/migration-planner/telemetry/devices_apps_telemetry.py")
