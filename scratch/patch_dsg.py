import re
import sys

def patch_file(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    # 1. Update __init__
    init_target = """        self.current_page = 0
        self.ITEMS_PER_PAGE = 8
        self.last_labels_data = None
        self.last_policies_data = None
        
        import tempfile
        import sqlite3
        import atexit
        self.db_fd, self.db_path = tempfile.mkstemp(suffix=".db")
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.cursor = self.conn.cursor()
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS labels 
                               (id INTEGER PRIMARY KEY AUTOINCREMENT, 
                                name TEXT, description TEXT, hasProtection INTEGER, 
                                applicationMode TEXT, priority INTEGER, 
                                applicableTo TEXT, isEnabled INTEGER, is_sublabel INTEGER)''')
        self.conn.commit()
        
        def cleanup_db():
            try:
                self.conn.close()
                import os
                os.close(self.db_fd)
                os.remove(self.db_path)
            except:
                pass
                
        atexit.register(cleanup_db)"""

    init_replacement = """        self.labels_current_page = 0
        self.retention_current_page = 0
        self.ITEMS_PER_PAGE = 5
        self.last_labels_data = None
        self.last_policies_data = None"""

    content = content.replace(init_target, init_replacement)

    # 2. Update build_ui
    build_ui_target = """        # Pagination controls frame (centered below the grid)
        self.pagination_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")
        
        self.btn_prev = ctk.CTkButton(
            self.pagination_frame,
            text="◀ Prev",
            command=self._prev_page,
            width=80,
            fg_color="transparent",
            border_width=1,
            text_color=COLOR_PRIMARY,
            border_color=COLOR_PRIMARY,
            hover_color=COLOR_SECONDARY_HOVER
        )
        self.btn_prev.pack(side="left", padx=10)
        
        self.lbl_page_info = ctk.CTkLabel(
            self.pagination_frame,
            text="Page 1 of 1",
            font=FONT_BODY_MEDIUM,
            text_color=COLOR_TEXT_MAIN
        )
        self.lbl_page_info.pack(side="left", padx=10)
        
        self.btn_next = ctk.CTkButton(
            self.pagination_frame,
            text="Next ▶",
            command=self._next_page,
            width=80,
            fg_color="transparent",
            border_width=1,
            text_color=COLOR_PRIMARY,
            border_color=COLOR_PRIMARY,
            hover_color=COLOR_SECONDARY_HOVER
        )
        self.btn_next.pack(side="left", padx=10)"""

    build_ui_replacement = """        # Pagination controls frame (centered below the grid)
        self.labels_pagination_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")"""
    content = content.replace(build_ui_target, build_ui_replacement)

    # Add retention_pagination_frame to build_ui
    retention_grid_target = """        self.retention_grid = ctk.CTkFrame(
            self.inner_pad,
            fg_color=COLOR_SURFACE,
            border_color=COLOR_OUTLINE_LIGHT,
            border_width=1,
            corner_radius=8
        )"""
    retention_grid_replacement = """        self.retention_grid = ctk.CTkFrame(
            self.inner_pad,
            fg_color=COLOR_SURFACE,
            border_color=COLOR_OUTLINE_LIGHT,
            border_width=1,
            corner_radius=8
        )
        
        self.retention_pagination_frame = ctk.CTkFrame(self.inner_pad, fg_color="transparent")"""
    content = content.replace(retention_grid_target, retention_grid_replacement)

    # 3. reset_view
    reset_target = """        self.labels_grid.pack_forget()
        self.pagination_frame.pack_forget()
        
        self.retention_header_frame.pack_forget()"""
    reset_replacement = """        self.labels_grid.pack_forget()
        self.labels_pagination_frame.pack_forget()
        
        self.retention_header_frame.pack_forget()"""
    content = content.replace(reset_target, reset_replacement)

    reset_target2 = """        for w in self.auth_grid.winfo_children():
            w.destroy()

            
        self.current_page = 0"""
    reset_replacement2 = """        for w in self.auth_grid.winfo_children():
            w.destroy()

            
        self.labels_current_page = 0
        self.retention_current_page = 0"""
    content = content.replace(reset_target2, reset_replacement2)

    # 4. _handle_labels_result
    handle_labels_target = """                import sqlite3
                self.cursor.execute("DELETE FROM labels")
                for parent in labels:
                    self.cursor.execute("INSERT INTO labels (name, description, hasProtection, applicationMode, priority, applicableTo, isEnabled, is_sublabel) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (parent.get("name", "N/A"),
                         parent.get("description", "") or parent.get("toolTip", "") or "N/A",
                         1 if parent.get("hasProtection", False) else 0,
                         parent.get("applicationMode", "N/A") or "N/A",
                         parent.get("priority", 0),
                         parent.get("applicableTo", ""),
                         1 if parent.get("isEnabled", True) else 0,
                         0))
                    sublabels = parent.get("sublabels", [])
                    if sublabels:
                        sublabels_sorted = sorted(sublabels, key=lambda x: x.get("priority", 0), reverse=True)
                        for sub in sublabels_sorted:
                            self.cursor.execute("INSERT INTO labels (name, description, hasProtection, applicationMode, priority, applicableTo, isEnabled, is_sublabel) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                                (f"    ↳  {sub.get('name', 'N/A')}",
                                 sub.get("description", "") or sub.get("toolTip", "") or "N/A",
                                 1 if sub.get("hasProtection", False) else 0,
                                 sub.get("applicationMode", "N/A") or "N/A",
                                 sub.get("priority", 0),
                                 sub.get("applicableTo", ""),
                                 1 if sub.get("isEnabled", True) else 0,
                                 1))
                self.conn.commit()
                self.current_page = 0
                self._display_current_page()
                
                self.cursor.execute("SELECT COUNT(*) FROM labels")
                total_items = self.cursor.fetchone()[0]
                if total_items > self.ITEMS_PER_PAGE:
                    self.pagination_frame.pack(fill="x", pady=(5, 10))
                else:
                    self.pagination_frame.pack_forget()"""

    handle_labels_replacement = """                flattened = []
                for parent in labels:
                    flattened.append({
                        "name": parent.get("name", "N/A"),
                        "description": parent.get("description", "") or parent.get("toolTip", "") or "N/A",
                        "hasProtection": 1 if parent.get("hasProtection", False) else 0,
                        "applicationMode": parent.get("applicationMode", "N/A") or "N/A",
                        "priority": parent.get("priority", 0),
                        "applicableTo": parent.get("applicableTo", ""),
                        "isEnabled": 1 if parent.get("isEnabled", True) else 0,
                        "is_sublabel": 0
                    })
                    sublabels = parent.get("sublabels", [])
                    if sublabels:
                        sublabels_sorted = sorted(sublabels, key=lambda x: x.get("priority", 0), reverse=True)
                        for sub in sublabels_sorted:
                            flattened.append({
                                "name": f"    ↳  {sub.get('name', 'N/A')}",
                                "description": sub.get("description", "") or sub.get("toolTip", "") or "N/A",
                                "hasProtection": 1 if sub.get("hasProtection", False) else 0,
                                "applicationMode": sub.get("applicationMode", "N/A") or "N/A",
                                "priority": sub.get("priority", 0),
                                "applicableTo": sub.get("applicableTo", ""),
                                "isEnabled": 1 if sub.get("isEnabled", True) else 0,
                                "is_sublabel": 1
                            })
                self.last_labels_data = flattened
                self.labels_current_page = 0
                self._update_labels_ui_paginated(self.last_labels_data)"""
    content = content.replace(handle_labels_target, handle_labels_replacement)
    
    content = content.replace("self.pagination_frame.pack_forget()", "if hasattr(self, 'labels_pagination_frame'): self.labels_pagination_frame.pack_forget()")

    # Remove old _display_current_page, _prev_page, _next_page
    pattern = re.compile(r"    def _display_current_page\(self\):.*?def _render_error\(self, err_msg\):", re.DOTALL)
    
    new_labels_paginated_code = """    def _update_labels_ui_paginated(self, data):
        for w in self.labels_grid.winfo_children():
            info = w.grid_info()
            if "row" in info and int(info["row"]) > 0:
                w.destroy()
        
        for w in self.labels_pagination_frame.winfo_children():
            w.destroy()

        if not data:
            return

        total_count = len(data)
        start_idx = self.labels_current_page * self.ITEMS_PER_PAGE
        end_idx = start_idx + self.ITEMS_PER_PAGE
        page_data = data[start_idx:end_idx]

        for offset, row_item in enumerate(page_data, start=1):
            r_idx = offset
            bg_style = COLOR_SURFACE if r_idx % 2 == 0 else COLOR_SURFACE_VARIANT
            
            name = row_item["name"]
            desc = row_item["description"]
            protection = "🛡️ Yes" if row_item["hasProtection"] else "🔓 No"
            mode = str(row_item["applicationMode"]).capitalize()
            priority = str(row_item["priority"])
            applicable = ", ".join([x.capitalize() for x in row_item["applicableTo"].split(",") if x.strip()]) or "N/A"
            status = "🟢 Enabled" if row_item["isEnabled"] else "🔴 Disabled"
            is_sublabel = bool(row_item["is_sublabel"])

            name_color = COLOR_TEXT_MAIN if not is_sublabel else COLOR_TEXT_SUB
            name_font = FONT_BODY_BOLD if not is_sublabel else FONT_BODY_MEDIUM

            c0 = ctk.CTkFrame(self.labels_grid, fg_color=bg_style, corner_radius=0)
            c0.grid(row=r_idx, column=0, sticky="nsew", padx=0, pady=(0, 1))
            lbl_name = ctk.CTkLabel(c0, text=name, font=name_font, text_color=name_color)
            lbl_name.pack(padx=10, pady=6, anchor="w")
            c0.bind("<Configure>", lambda e, l=lbl_name: l.configure(wraplength=e.width - 20))

            c1 = ctk.CTkFrame(self.labels_grid, fg_color=bg_style, corner_radius=0)
            c1.grid(row=r_idx, column=1, sticky="nsew", padx=0, pady=(0, 1))
            lbl_desc = ctk.CTkLabel(c1, text=desc, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN)
            lbl_desc.pack(padx=10, pady=6, anchor="w")
            c1.bind("<Configure>", lambda e, l=lbl_desc: l.configure(wraplength=e.width - 20))

            c2 = ctk.CTkFrame(self.labels_grid, fg_color=bg_style, corner_radius=0)
            c2.grid(row=r_idx, column=2, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c2, text=protection, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=6, anchor="w")

            c3 = ctk.CTkFrame(self.labels_grid, fg_color=bg_style, corner_radius=0)
            c3.grid(row=r_idx, column=3, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c3, text=mode, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=6, anchor="w")

            c4 = ctk.CTkFrame(self.labels_grid, fg_color=bg_style, corner_radius=0)
            c4.grid(row=r_idx, column=4, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c4, text=priority, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=6, anchor="w")

            c5 = ctk.CTkFrame(self.labels_grid, fg_color=bg_style, corner_radius=0)
            c5.grid(row=r_idx, column=5, sticky="nsew", padx=0, pady=(0, 1))
            lbl_app = ctk.CTkLabel(c5, text=applicable, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN)
            lbl_app.pack(padx=10, pady=6, anchor="w")
            c5.bind("<Configure>", lambda e, l=lbl_app: l.configure(wraplength=e.width - 20))

            c6 = ctk.CTkFrame(self.labels_grid, fg_color=bg_style, corner_radius=0)
            c6.grid(row=r_idx, column=6, sticky="nsew", padx=0, pady=(0, 1))
            ctk.CTkLabel(c6, text=status, font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN).pack(padx=10, pady=6, anchor="w")

        if total_count > self.ITEMS_PER_PAGE:
            self._draw_labels_pagination_controls(total_count, data)
            self.labels_pagination_frame.pack(fill="x", pady=(5, 10))
        else:
            self.labels_pagination_frame.pack_forget()

    def _draw_labels_pagination_controls(self, total_count, data):
        total_pages = (total_count + self.ITEMS_PER_PAGE - 1) // self.ITEMS_PER_PAGE
        
        left_spacer = ctk.CTkFrame(self.labels_pagination_frame, fg_color="transparent")
        left_spacer.pack(side="left", fill="x", expand=True)
        center_container = ctk.CTkFrame(self.labels_pagination_frame, fg_color="transparent")
        center_container.pack(side="left")

        prev_state = "normal" if self.labels_current_page > 0 else "disabled"
        btn_prev = ctk.CTkButton(
            center_container, text="◀ Prev", width=70, height=26, corner_radius=6,
            font=FONT_BODY_SMALL, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER, state=prev_state,
            command=lambda: self._change_labels_page(-1, data)
        )
        btn_prev.pack(side="left", padx=5)

        page_lbl = ctk.CTkLabel(center_container, text=f"Page {self.labels_current_page + 1} of {total_pages}", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB)
        page_lbl.pack(side="left", padx=15)

        next_state = "normal" if self.labels_current_page < total_pages - 1 else "disabled"
        btn_next = ctk.CTkButton(
            center_container, text="Next ▶", width=70, height=26, corner_radius=6,
            font=FONT_BODY_SMALL, fg_color="transparent", border_width=1, border_color=COLOR_OUTLINE,
            text_color=COLOR_PRIMARY, hover_color=COLOR_SECONDARY_HOVER, state=next_state,
            command=lambda: self._change_labels_page(1, data)
        )
        btn_next.pack(side="left", padx=5)

        right_spacer = ctk.CTkFrame(self.labels_pagination_frame, fg_color="transparent")
        right_spacer.pack(side="right", fill="x", expand=True)

    def _change_labels_page(self, delta, data):
        self.labels_current_page += delta
        self._update_labels_ui_paginated(data)

    def _render_error(self, err_msg):"""
    content = pattern.sub(new_labels_paginated_code, content)
    
    # 5. Fix retention pagination
    # The existing _render_retention_policies function draws the grid and lists all items.
    retention_target = """        else:
            self.btn_export_retention.configure(state="normal")
            # Configure grid columns
            self.retention_grid.grid_columnconfigure(0, weight=3)  # Policy Name
            self.retention_grid.grid_columnconfigure(1, weight=3)  # Workloads
            self.retention_grid.grid_columnconfigure(2, weight=2)  # Duration & Trigger
            self.retention_grid.grid_columnconfigure(3, weight=1)  # Distribution
            self.retention_grid.grid_columnconfigure(4, weight=1)  # Status

            headers = ["Policy Name", "Workloads", "Duration", "Distribution", "Status"]
            for col_idx, head_text in enumerate(headers):
                cell = ctk.CTkFrame(self.retention_grid, fg_color=COLOR_TONAL_BG, corner_radius=0)
                cell.grid(row=0, column=col_idx, sticky="nsew", padx=1, pady=1)
                ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

            # Handle case where policies is a single dict rather than a list
            policies_list = policies if isinstance(policies, list) else [policies]

            for r_idx, policy in enumerate(policies_list, start=1):
                bg_style = "transparent" if r_idx % 2 != 0 else COLOR_SURFACE_VARIANT"""

    retention_replacement = """        else:
            self.btn_export_retention.configure(state="normal")
            policies_list = policies if isinstance(policies, list) else [policies]
            self.last_policies_data = policies_list
            self.retention_current_page = 0
            self._update_retention_ui_paginated(self.last_policies_data)
            
    def _update_retention_ui_paginated(self, data):
        for w in self.retention_grid.winfo_children():
            w.destroy()
        for w in self.retention_pagination_frame.winfo_children():
            w.destroy()

        self.retention_grid.grid_columnconfigure(0, weight=3)  # Policy Name
        self.retention_grid.grid_columnconfigure(1, weight=3)  # Workloads
        self.retention_grid.grid_columnconfigure(2, weight=2)  # Duration & Trigger
        self.retention_grid.grid_columnconfigure(3, weight=1)  # Distribution
        self.retention_grid.grid_columnconfigure(4, weight=1)  # Status

        headers = ["Policy Name", "Workloads", "Duration", "Distribution", "Status"]
        for col_idx, head_text in enumerate(headers):
            cell = ctk.CTkFrame(self.retention_grid, fg_color=COLOR_TONAL_BG, corner_radius=0)
            cell.grid(row=0, column=col_idx, sticky="nsew", padx=1, pady=1)
            ctk.CTkLabel(cell, text=head_text, font=FONT_BODY_BOLD, text_color=COLOR_TONAL_TEXT).pack(padx=10, pady=8, anchor="w")

        total_count = len(data)
        start_idx = self.retention_current_page * self.ITEMS_PER_PAGE
        end_idx = start_idx + self.ITEMS_PER_PAGE
        page_data = data[start_idx:end_idx]

        for offset, policy in enumerate(page_data, start=1):
            r_idx = offset
            bg_style = "transparent" if r_idx % 2 != 0 else COLOR_SURFACE_VARIANT"""

    content = content.replace(retention_target, retention_replacement)

    # 6. Pagination controls for retention
    # In `_render_retention_policies`, there is a big loop `for r_idx, policy in enumerate(policies_list, start=1):`.
    # We need to replace the end of that loop with pagination logic.
    retention_loop_end = """                ctk.CTkLabel(c4, text=status_text, font=FONT_BODY_BOLD, text_color=status_color).pack(padx=10, pady=8, anchor="w")"""
    retention_loop_replacement = """                ctk.CTkLabel(c4, text=status_text, font=FONT_BODY_BOLD, text_color=status_color).pack(padx=10, pady=8, anchor="w")

        if total_count > self.ITEMS_PER_PAGE:
            self._draw_retention_pagination_controls(total_count, data)
            self.retention_pagination_frame.pack(fill="x", pady=(5, 10))
        else:
            self.retention_pagination_frame.pack_forget()

    def _draw_retention_pagination_controls(self, total_count, data):
        total_pages = (total_count + self.ITEMS_PER_PAGE - 1) // self.ITEMS_PER_PAGE
        
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
        self._update_retention_ui_paginated(data)"""

    content = content.replace(retention_loop_end, retention_loop_replacement)

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)

if __name__ == "__main__":
    patch_file("/Users/srishtinegi/Desktop/Test/migration-planner/telemetry/data_security_governance.py")
