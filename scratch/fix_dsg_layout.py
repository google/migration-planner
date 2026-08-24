import re

def fix_dsg_layout(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    # 1. build_ui
    content = content.replace('        self.labels_pagination_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")', '')
    content = content.replace('        self.retention_pagination_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")', '')

    # 2. reset_view
    content = content.replace('        self.labels_pagination_frame.pack_forget()\n', '')
    
    # 3. labels pagination code
    target_labels_ui = """        for w in self.labels_pagination_frame.winfo_children():
            w.destroy()"""
    content = content.replace(target_labels_ui, '')
    
    target_labels_draw = """        self._draw_labels_pagination_controls(total_count, data)
        self.labels_pagination_frame.pack(fill="x", pady=(5, 10))

    def _draw_labels_pagination_controls(self, total_count, data):
        total_pages = max(1, (total_count + self.ITEMS_PER_PAGE - 1) // self.ITEMS_PER_PAGE)
        
        left_spacer = ctk.CTkFrame(self.labels_pagination_frame, fg_color="transparent")
        left_spacer.pack(side="left", fill="x", expand=True)
        center_container = ctk.CTkFrame(self.labels_pagination_frame, fg_color="transparent")
        center_container.pack(side="left")"""

    replace_labels_draw = """        self._draw_labels_pagination_controls(total_count, data)

    def _draw_labels_pagination_controls(self, total_count, data):
        total_pages = max(1, (total_count + self.ITEMS_PER_PAGE - 1) // self.ITEMS_PER_PAGE)
        
        control_frame = ctk.CTkFrame(self.labels_grid, fg_color="transparent")
        control_frame.grid(row=self.ITEMS_PER_PAGE + 1, column=0, columnspan=7, pady=(5, 10), sticky="ew")
        
        left_spacer = ctk.CTkFrame(control_frame, fg_color="transparent")
        left_spacer.pack(side="left", fill="x", expand=True)
        center_container = ctk.CTkFrame(control_frame, fg_color="transparent")
        center_container.pack(side="left")"""
    content = content.replace(target_labels_draw, replace_labels_draw)
    
    content = content.replace("right_spacer = ctk.CTkFrame(self.labels_pagination_frame", "right_spacer = ctk.CTkFrame(control_frame")

    # 4. retention pagination code
    target_retention_ui = """        for w in self.retention_pagination_frame.winfo_children():
            w.destroy()"""
    content = content.replace(target_retention_ui, '')
    
    target_retention_draw = """        self._draw_retention_pagination_controls(total_count, data)
        self.retention_pagination_frame.pack(fill="x", pady=(5, 10))

    def _draw_retention_pagination_controls(self, total_count, data):
        total_pages = max(1, (total_count + self.ITEMS_PER_PAGE - 1) // self.ITEMS_PER_PAGE)
        
        left_spacer = ctk.CTkFrame(self.retention_pagination_frame, fg_color="transparent")
        left_spacer.pack(side="left", fill="x", expand=True)
        center_container = ctk.CTkFrame(self.retention_pagination_frame, fg_color="transparent")
        center_container.pack(side="left")"""
        
    replace_retention_draw = """        self._draw_retention_pagination_controls(total_count, data)

    def _draw_retention_pagination_controls(self, total_count, data):
        total_pages = max(1, (total_count + self.ITEMS_PER_PAGE - 1) // self.ITEMS_PER_PAGE)
        
        control_frame = ctk.CTkFrame(self.retention_grid, fg_color="transparent")
        control_frame.grid(row=self.ITEMS_PER_PAGE + 1, column=0, columnspan=5, pady=(5, 10), sticky="ew")
        
        left_spacer = ctk.CTkFrame(control_frame, fg_color="transparent")
        left_spacer.pack(side="left", fill="x", expand=True)
        center_container = ctk.CTkFrame(control_frame, fg_color="transparent")
        center_container.pack(side="left")"""
    content = content.replace(target_retention_draw, replace_retention_draw)
    
    content = content.replace("right_spacer = ctk.CTkFrame(self.retention_pagination_frame", "right_spacer = ctk.CTkFrame(control_frame")
    
    # 5. Fix `for w in self.retention_grid.winfo_children(): w.destroy()` in `_update_retention_ui_paginated`
    # Currently it destroys ALL children. But it should ONLY destroy children if "row" in info and row > 0, to preserve headers!
    # Wait, does _update_retention_ui_paginated redraw the headers? Let's check.
    # It has:
    # headers = ["Policy Name", "Workloads", "Duration", "Distribution", "Status"]
    # for col_idx, head_text in enumerate(headers):
    # Yes, it redraws the headers every time! So w.destroy() for all children is perfectly fine.
    
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)

if __name__ == "__main__":
    fix_dsg_layout("/Users/srishtinegi/Desktop/Test/migration-planner/telemetry/data_security_governance.py")
