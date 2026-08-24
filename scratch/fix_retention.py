import re

def fix_dsg(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    # 1. Fix the top of _update_retention_ui_paginated
    bad_top = """    def _update_retention_ui_paginated(self, data):
        self._draw_retention_pagination_controls(len(data), data)
        self.retention_pagination_frame.pack(fill="x", pady=(5, 10))
        for w in self.retention_grid.winfo_children():
            w.destroy()
        for w in self.retention_pagination_frame.winfo_children():
            w.destroy()"""
            
    good_top = """    def _update_retention_ui_paginated(self, data):
        for w in self.retention_grid.winfo_children():
            w.destroy()
        for w in self.retention_pagination_frame.winfo_children():
            w.destroy()"""
            
    content = content.replace(bad_top, good_top)

    # 2. Fix the bottom of _update_retention_ui_paginated
    bad_bottom = """                ctk.CTkLabel(c4, text=status, font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=6, anchor="w")

        self._draw_retention_pagination_controls(total_count, data)
        self.retention_pagination_frame.pack(fill="x", pady=(5, 10))

    def _draw_retention_pagination_controls(self, total_count, data):"""
    
    good_bottom = """                ctk.CTkLabel(c4, text=status, font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=6, anchor="w")

        self._draw_retention_pagination_controls(total_count, data)
        self.retention_pagination_frame.pack(fill="x", pady=(5, 10))

    def _draw_retention_pagination_controls(self, total_count, data):"""
    
    # Wait, the bottom is actually correct now because of the second replace chunk?
    # Let's verify what the bottom looks like currently.

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)

if __name__ == "__main__":
    fix_dsg("/Users/srishtinegi/Desktop/Test/migration-planner/telemetry/data_security_governance.py")
