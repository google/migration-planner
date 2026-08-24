import re

def fix_dsg(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    # Find where _update_retention_ui_paginated ends. It ends at `def export_labels_csv(self):`
    # We'll inject the missing call and the missing functions right before `def export_labels_csv(self):`

    target = """    def export_labels_csv(self):"""
    
    replacement = """        self._draw_retention_pagination_controls(total_count, data)
        self.retention_pagination_frame.pack(fill="x", pady=(5, 10))

    def _draw_retention_pagination_controls(self, total_count, data):
        total_pages = max(1, (total_count + self.ITEMS_PER_PAGE - 1) // self.ITEMS_PER_PAGE)
        
        left_spacer = ctk.CTkFrame(self.retention_pagination_frame, fg_color="transparent")
        left_spacer.pack(side="left", fill="x", expand=True)
        center_container = ctk.CTkFrame(self.retention_pagination_frame, fg_color="transparent")
        center_container.pack(side="left")

        prev_state = "normal" if self.retention_current_page > 0 else "disabled"
        btn_prev = ctk.CTkButton(
            center_container, text="◀ Prev", width=70, height=26, corner_radius=6,
            font=FONT_BODY_SMALL, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER, state=prev_state,
            command=lambda: self._change_retention_page(-1, data)
        )
        btn_prev.pack(side="left", padx=5)

        page_lbl = ctk.CTkLabel(center_container, text=f"Page {self.retention_current_page + 1} of {total_pages}", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB)
        page_lbl.pack(side="left", padx=15)

        next_state = "normal" if self.retention_current_page < total_pages - 1 else "disabled"
        btn_next = ctk.CTkButton(
            center_container, text="Next ▶", width=70, height=26, corner_radius=6,
            font=FONT_BODY_SMALL, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER, state=next_state,
            command=lambda: self._change_retention_page(1, data)
        )
        btn_next.pack(side="left", padx=5)

        right_spacer = ctk.CTkFrame(self.retention_pagination_frame, fg_color="transparent")
        right_spacer.pack(side="right", fill="x", expand=True)

    def _change_retention_page(self, delta, data):
        self.retention_current_page += delta
        self._update_retention_ui_paginated(data)

    def export_labels_csv(self):"""

    content = content.replace(target, replacement)

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)

if __name__ == "__main__":
    fix_dsg("/Users/srishtinegi/Desktop/Test/migration-planner/telemetry/data_security_governance.py")
