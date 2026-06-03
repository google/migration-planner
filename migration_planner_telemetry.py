# Copyright 2026 Google LLC
"""Standalone application for the License Usage and Telemetry view."""

import os
import customtkinter as ctk
from telemetry.license_usage import LicenseUsageTab, async_logger
import logging
from telemetry.power_automate import PowerAutomateScanner

# Orchestrator logging
logger = logging.getLogger("TelemetryOrchestrator")


class TelemetryApp(ctk.CTk):
    """Standalone application for the License Usage and Telemetry view."""

    def __init__(self):
        super().__init__()
        self.title("Migration Planner - Telemetry")  # CITATION: self.title("Migration Planner - Telemetry")
        self.geometry("1230x950")  # Expanded window width to support increased sidebar dimensions

        # FIX: Bind the window close button to a custom exit handler
        # to prevent CustomTkinter 'after script' errors when closing.
        self.protocol("WM_DELETE_WINDOW", self.on_closing)  # CITATION: self.protocol("WM_DELETE_WINDOW", self.on_closing)

        # Initialize variables required by the LicenseUsageTab
        self.retries = ctk.IntVar(value=30)  # CITATION: self.retries = ctk.IntVar(value=30)
        self.backoff = ctk.IntVar(value=2)  # CITATION: self.backoff = ctk.IntVar(value=2)

        # Stage 1: In-memory variables to store connection credentials
        self.stored_tenant = ""
        self.stored_client = ""
        self.stored_secret = ""

        # Page containers
        self.auth_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.report_frame = ctk.CTkFrame(self, fg_color="transparent")

        # Render Page 1 (Authentication screen)
        self.setup_auth_ui()

        # Render Page 2 (Reports Dashboard with Sidebar)
        self.setup_report_ui()

        # Initial view
        self.show_auth_page()

    def setup_auth_ui(self):
        """Builds a modern, polished Connection interface for Page 1."""
        # Welcome Branding Card
        self.brand_card = ctk.CTkFrame(
            self.auth_frame,
            fg_color="white",
            corner_radius=15,
            border_width=1,
            border_color="#E2E8F0"
        )
        self.brand_card.pack(fill="x", padx=40, pady=(60, 20))

        self.brand_title = ctk.CTkLabel(
            self.brand_card,
            text="Migration Planner - Telemetry Connection",
            font=ctk.CTkFont(size=22, weight="bold"),
            text_color="#1E3A8A"
        )
        self.brand_title.pack(pady=(25, 5))

        self.brand_subtitle = ctk.CTkLabel(
            self.brand_card,
            text="Connect your Azure Active Directory credentials to begin auditing your tenant.",
            font=ctk.CTkFont(size=13),
            text_color="#4B5563"
        )
        self.brand_subtitle.pack(pady=(0, 25))

        # Credentials Form Box
        self.credentials_card = ctk.CTkFrame(
            self.auth_frame,
            fg_color="white",
            corner_radius=15,
            border_width=1,
            border_color="#E2E8F0"
        )
        self.credentials_card.pack(fill="both", expand=True, padx=40, pady=(0, 40))

        self.form_container = ctk.CTkFrame(self.credentials_card, fg_color="transparent")
        self.form_container.pack(pady=40, padx=50, fill="both", expand=True)

        # Tenant ID Input
        self.tenant_lbl = ctk.CTkLabel(self.form_container, text="Tenant ID", font=ctk.CTkFont(size=14, weight="bold"), text_color="#374151")
        self.tenant_lbl.pack(anchor="w", pady=(10, 5))
        self.tenant_entry = ctk.CTkEntry(self.form_container, width=500, height=40, placeholder_text="Enter Tenant ID")
        self.tenant_entry.pack(fill="x", pady=(0, 15))

        # Client ID Input
        self.client_lbl = ctk.CTkLabel(self.form_container, text="Client ID", font=ctk.CTkFont(size=14, weight="bold"), text_color="#374151")
        self.client_lbl.pack(anchor="w", pady=(10, 5))
        self.client_entry = ctk.CTkEntry(self.form_container, width=500, height=40, placeholder_text="Enter Client ID")
        self.client_entry.pack(fill="x", pady=(0, 15))

        # Client Secret Input
        self.secret_lbl = ctk.CTkLabel(self.form_container, text="Client Secret", font=ctk.CTkFont(size=14, weight="bold"), text_color="#374151")
        self.secret_lbl.pack(anchor="w", pady=(10, 5))
        self.secret_entry = ctk.CTkEntry(self.form_container, width=500, height=40, show="*", placeholder_text="Enter Client Secret")
        self.secret_entry.pack(fill="x", pady=(0, 30))

        # Status & Feedback Display
        self.auth_status_lbl = ctk.CTkLabel(self.form_container, text="", font=ctk.CTkFont(size=13), text_color="red")
        self.auth_status_lbl.pack(pady=(0, 15))

        # Action Submission Trigger
        self.connect_btn = ctk.CTkButton(
            self.form_container,
            text="Connect & Continue",  # Updated to denote transitioning without immediate fetch
            command=self.on_connect_clicked,
            height=45,
            fg_color="#1E3A8A",
            hover_color="#172554",
            font=ctk.CTkFont(size=14, weight="bold")
        )
        self.connect_btn.pack(fill="x", pady=(0, 10))

    def setup_report_ui(self):
        """Instantiates the ReportsPage frame which contains the collapsible navigation structure."""
        self.reports_page = ReportsPage(
            master=self.report_frame,
            controller=self,
            retries_var=self.retries,
            backoff_var=self.backoff
        )
        self.reports_page.pack(fill="both", expand=True)

    def on_connect_clicked(self):
        """Validates inputs, caches credentials in-memory, and transitions to Page 2."""
        tenant = self.tenant_entry.get().strip()
        client = self.client_entry.get().strip()
        secret = self.secret_entry.get().strip()

        if not tenant or not client or not secret:
            self.auth_status_lbl.configure(text="Error: Tenant ID, Client ID, and Client Secret are required.", text_color="red")
            return

        self.auth_status_lbl.configure(text="")

        # Cache connection details safely in memory (Stage 1)
        self.stored_tenant = tenant
        self.stored_client = client
        self.stored_secret = secret

        # Switch view to Page 2 (Reports screen) without executing backend fetching threads
        self.show_reports_page()

    def on_disconnect_clicked(self):
        """Clears all session properties and safely resets screens back to Page 1."""
        # Wipes local entry buffers
        self.tenant_entry.delete(0, "end")
        self.client_entry.delete(0, "end")
        self.secret_entry.delete(0, "end")
        self.auth_status_lbl.configure(text="")

        # Wipes stored in-memory configurations
        self.stored_tenant = ""
        self.stored_client = ""
        self.stored_secret = ""

        # Clears variables inside nested dashboards
        self.reports_page.clear_session_data()

        # Shifts screen orientation
        self.show_auth_page()

    def show_auth_page(self):
        """Transitions view port to Page 1 (Authentication screen)."""
        self.report_frame.pack_forget()
        self.auth_frame.pack(fill="both", expand=True)

    def show_reports_page(self):
        """Transitions view port to Page 2 (Reports Dashboard)."""
        self.auth_frame.pack_forget()
        self.report_frame.pack(fill="both", expand=True)

    def log_msg(self, text):  # CITATION: def log_msg(self, text):
        """Simple callback handler for telemetry UI logs. Pipes to log file instead of stdout."""
        async_logger.info(text)  # CITATION: async_logger.info(text)

    def on_closing(self):  # CITATION: def on_closing(self):
        """Trigger an OS-level exit to cleanly bypass Tkinter background tasks."""
        self.destroy()  # CITATION: self.destroy()
        os._exit(0)  # CITATION: os._exit(0)


class ReportsPage(ctk.CTkFrame):
    """Page 2 Content Host. Organizes the Left Collapsible Panel and the Main Data Panel side-by-side."""

    def __init__(self, master, controller, retries_var, backoff_var):
        super().__init__(master, fg_color="transparent")
        self.controller = controller

        # 1. Left Collapsible Navigation Sidebar
        self.sidebar = SidebarFrame(self, disconnect_callback=self.controller.on_disconnect_clicked)
        self.sidebar.pack(side="left", fill="y", padx=(0, 10))

        # 2. Right-hand Main Dashboard Container
        self.dashboard_container = ctk.CTkFrame(self, fg_color="transparent")
        self.dashboard_container.pack(side="right", fill="both", expand=True)

        # Top Header Bar mimicking Google Workspace Deal Assistant details
        self.nav_header = ctk.CTkFrame(
            self.dashboard_container,
            fg_color="white",
            height=70,
            corner_radius=10,
            border_width=1,
            border_color="#E2E8F0"
        )
        self.nav_header.pack(fill="x", pady=(0, 15))
        self.nav_header.pack_propagate(False)

        # Text container frame to keep alignment clean next to the Action Button
        self.header_text_frame = ctk.CTkFrame(self.nav_header, fg_color="transparent")
        self.header_text_frame.pack(side="left", padx=(20, 10), pady=(12, 0), anchor="w")

        self.nav_title = ctk.CTkLabel(
            self.header_text_frame,
            text="Usage and adoption data",  # Match screenshot header text precisely
            font=ctk.CTkFont(size=20, weight="bold"),
            text_color="#111827"
        )
        self.nav_title.pack(anchor="w")

        self.nav_subtitle = ctk.CTkLabel(
            self.header_text_frame,
            text="Overview of your current plans and how people are using applications.",
            font=ctk.CTkFont(size=12),
            text_color="#6B7280"
        )
        self.nav_subtitle.pack(anchor="w", pady=(2, 0))

        # 3. Fetch Report button on the right side of the header panel (Stage 2)
        self.fetch_btn = ctk.CTkButton(
            self.nav_header,
            text="Fetch Report",
            command=self.on_fetch_report_clicked,
            width=150,
            height=36,
            fg_color="#1E3A8A",
            hover_color="#172554",
            font=ctk.CTkFont(size=13, weight="bold")
        )
        self.fetch_btn.pack(side="right", padx=20, pady=17)

        # Initialize the telemetry view (No TabView layout)
        self.license_usage_view = LicenseUsageTab(  # CITATION: self.license_usage_view = LicenseUsageTab(
            master=self.dashboard_container, 
            log_callback=controller.log_msg, 
            retries_var=retries_var, 
            backoff_var=backoff_var
        )
        self.license_usage_view.pack(fill="both", expand=True)  # CITATION: self.license_usage_view.pack(fill="both", expand=True, padx=15, pady=15)
        self.license_usage_view.on_all_done_callback = self.on_telemetry_fetch_completed

        # Adapt layout recursively to hide original inputs from view
        self.adapt_embedded_view()

    def adapt_embedded_view(self):
        """Traverses LicenseUsageTab to identify and hide native login components."""
        self.embedded_entries = []
        self.embedded_submit_btn = None
        self.embedded_labels = []

        def find_widgets_recursive(widget):
            if isinstance(widget, ctk.CTkEntry):
                self.embedded_entries.append(widget)
            elif isinstance(widget, ctk.CTkButton):
                btn_txt = str(widget.cget("text")).lower()
                if "submit" in btn_txt or btn_txt == "":
                    self.embedded_submit_btn = widget
            elif isinstance(widget, ctk.CTkLabel):
                lbl_txt = str(widget.cget("text")).lower()
                if any(kw in lbl_txt for kw in ["tenant id", "client id", "client secret", "connect your", "authenticate and audit"]):
                    self.embedded_labels.append(widget)

            for child in widget.winfo_children():
                find_widgets_recursive(child)

        find_widgets_recursive(self.license_usage_view)

        # Remove the target widgets from layout grids/packs programmatically
        for entry in self.embedded_entries:
            entry.pack_forget()
            entry.grid_forget()

        for label in self.embedded_labels:
            label.pack_forget()
            label.grid_forget()

        if self.embedded_submit_btn:
            self.embedded_submit_btn.pack_forget()
            self.embedded_submit_btn.grid_forget()

        if hasattr(self.license_usage_view, "inputs_frame"):
            self.license_usage_view.inputs_frame.pack_forget()
            self.license_usage_view.inputs_frame.grid_forget()

    def on_fetch_report_clicked(self):
        """Stage 2: Migrates stored variables into the hidden entries and clicks the submit thread."""
        tenant = self.controller.stored_tenant
        client = self.controller.stored_client
        secret = self.controller.stored_secret

        if not tenant or not client or not secret:
            return

        # Disable the trigger button and update visual text during background thread audit
        self.fetch_btn.configure(state="disabled", text="Fetching...", fg_color="#64748B")

        # Map credentials into the hidden layout entries dynamically
        if len(self.embedded_entries) >= 3:
            self.embedded_entries[0].delete(0, "end")
            self.embedded_entries[0].insert(0, tenant)

            self.embedded_entries[1].delete(0, "end")
            self.embedded_entries[1].insert(0, client)

            self.embedded_entries[2].delete(0, "end")
            self.embedded_entries[2].insert(0, secret)

        # Programmatically trigger the hidden connection submit button
        if self.embedded_submit_btn:
            self.embedded_submit_btn.invoke()

    def on_telemetry_fetch_completed(self, success: bool):
        """Callback from LicenseUsageTab when all parallel reports complete."""
        self.fetch_btn.configure(state="normal", text="Fetch Report", fg_color="#1E3A8A")

    def clear_session_data(self):
        """Wipes the cached parameters from telemetry objects and resets the Fetch button."""
        for entry in self.embedded_entries:
            entry.delete(0, "end")
        
        # Reset Fetch Report button state
        self.fetch_btn.configure(state="normal", text="Fetch Report", fg_color="#1E3A8A")


class SidebarFrame(ctk.CTkFrame):
    """Collapsible Left Navigation Sidebar, matching Workspace Deal Assistant styling."""

    def __init__(self, master, disconnect_callback, **kwargs):
        # Increased initial width to 300px to avoid text truncation of longer menu items
        super().__init__(master, width=300, fg_color="white", corner_radius=12, border_width=1, border_color="#E2E8F0", **kwargs)
        self.pack_propagate(False)  # Lock sidebar panel dimensions
        self.disconnect_callback = disconnect_callback
        self.is_expanded = True

        # Header Branding Section
        self.header_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.header_frame.pack(fill="x", padx=15, pady=(25, 30))

        # Brand Icon symbolizing colorful Workspace shape
        self.logo_label = ctk.CTkLabel(
            self.header_frame,
            text="✦",
            font=ctk.CTkFont(size=24, weight="bold"),
            text_color="#2563EB"
        )
        self.logo_label.pack(side="left", padx=(5, 5))

        self.brand_text_area = ctk.CTkFrame(self.header_frame, fg_color="transparent")
        self.brand_text_area.pack(side="left", fill="both", expand=True)

        self.brand_title = ctk.CTkLabel(
            self.brand_text_area,
            text="Workspace",
            font=ctk.CTkFont(size=14, weight="bold"),
            text_color="#1E293B",
            anchor="w"
        )
        self.brand_title.pack(fill="x")

        self.brand_subtitle = ctk.CTkLabel(
            self.brand_text_area,
            text="Deal Assistant",
            font=ctk.CTkFont(size=10),
            text_color="#64748B",
            anchor="w"
        )
        self.brand_subtitle.pack(fill="x")

        # Collapse / Expand control button
        self.toggle_btn = ctk.CTkButton(
            self,
            text="⏴",
            command=self.toggle_sidebar,
            width=26,
            height=28,
            corner_radius=13,
            fg_color="#F1F5F9",
            hover_color="#E2E8F0",
            text_color="#475569",
            font=ctk.CTkFont(size=11, weight="bold")
        )
        self.toggle_btn.place(relx=1.0, x=-22, y=26, anchor="center")

        # Vertical menu container
        self.menu_items_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.menu_items_frame.pack(fill="both", expand=True, padx=10)

        # Definitions for active/inactive routes
        self.menu_buttons = []
        self.menu_data = [
            ("Usage and adoption", "📊", True),
            ("Workforce analysis", "👥", False),
            ("Cost savings plan", "💰", False),
            ("Migration planner", "🚀", False)
        ]

        self.render_navigation_menu()

        # Session closure button pinned safely at bottom
        self.disconnect_row = ctk.CTkFrame(self, fg_color="transparent")
        self.disconnect_row.pack(fill="x", side="bottom", padx=10, pady=25)

        self.disconnect_symbol = ctk.CTkLabel(
            self.disconnect_row,
            text="🚪",
            font=ctk.CTkFont(size=14),
            text_color="#EF4444"
        )
        self.disconnect_symbol.pack(side="left", padx=(12, 6))

        self.disconnect_btn = ctk.CTkButton(
            self.disconnect_row,
            text="Disconnect",
            command=self.disconnect_callback,
            anchor="w",
            height=38,
            corner_radius=8,
            fg_color="transparent",
            text_color="#EF4444",
            hover_color="#FEF2F2",
            font=ctk.CTkFont(size=13, weight="normal")  # CITATION: _tkinter.TclError: bad -weight value "medium": must be normal, or bold
        )
        self.disconnect_btn.pack(side="left", fill="x", expand=True)

    def render_navigation_menu(self):
        """Builds clean selection widgets mapping Workspace items."""
        for item in self.menu_buttons:
            item[0].destroy()
            item[1].destroy()
        self.menu_buttons.clear()

        for label, icon, is_active in self.menu_data:
            row_frame = ctk.CTkFrame(self.menu_items_frame, fg_color="transparent")
            row_frame.pack(fill="x", pady=4)

            # Highlighting indicators mapping screenshot
            if is_active:
                btn_fg = "#EFF6FF"
                text_color = "#1D4ED8"
                hover_color = "#DBEAFE"
                weight = "bold"
            else:
                btn_fg = "transparent"
                text_color = "#475569"
                hover_color = "#F8FAFC"
                weight = "normal"

            icon_lbl = ctk.CTkLabel(
                row_frame,
                text=icon,
                font=ctk.CTkFont(size=15),
                text_color=text_color
            )
            icon_lbl.pack(side="left", padx=(12, 8))

            btn = ctk.CTkButton(
                row_frame,
                text=label if self.is_expanded else "",
                anchor="w",
                height=38,
                corner_radius=8,
                fg_color=btn_fg,
                text_color=text_color,
                hover_color=hover_color,
                state="normal" if is_active else "disabled",
                font=ctk.CTkFont(size=13, weight=weight)
            )
            btn.pack(side="left", fill="x", expand=True)
            self.menu_buttons.append((row_frame, btn, icon_lbl))

    def toggle_sidebar(self):
        """Performs layout adjustments to expand/collapse panel width dynamically."""
        if self.is_expanded:
            # Shift width configuration to compact state (72px)
            self.configure(width=72)
            self.brand_text_area.pack_forget()
            self.logo_label.pack(side="top", pady=10)
            self.is_expanded = False
            self.toggle_btn.configure(text="⏵")

            # Wipe text arrays inside panel items
            for row, btn, icon in self.menu_buttons:
                btn.configure(text="")
            self.disconnect_btn.configure(text="")
        else:
            # Return layout to expanded parameters (300px)
            self.configure(width=300)
            self.logo_label.pack(side="left", padx=(5, 5))
            self.brand_text_area.pack(side="left", fill="both", expand=True)
            self.is_expanded = True
            self.toggle_btn.configure(text="⏴")

            # Restore original strings dynamically
            for idx, (row, btn, icon) in enumerate(self.menu_buttons):
                btn.configure(text=self.menu_data[idx][0])
            self.disconnect_btn.configure(text="Disconnect")


def collect_power_automate_telemetry(tenant_id, client_id, client_secret, env_url):  # CITATION: def collect_power_automate_telemetry(tenant_id, client_id, client_secret, env_url):
    """Integrates the Power Automate scan into the telemetry execution flow."""
    logger.info("--- Power Automate Telemetry Phase Initiated ---")  # CITATION: logger.info("--- Power Automate Telemetry Phase Initiated ---")

    if not env_url:  # CITATION: if not env_url:
        logger.warning("Skipping Power Automate: Environment URL not provided.")  # CITATION: logger.warning("Skipping Power Automate: Environment URL not provided.")
        return {}

    try:
        scanner = PowerAutomateScanner(tenant_id, client_id, client_secret, env_url)  # CITATION: scanner = PowerAutomateScanner(tenant_id, client_id, client_secret, env_url)
        results = scanner.scan_flows()  # CITATION: results = scanner.scan_flows()

        if results:  # CITATION: if results:
            logger.info(f"Telemetry Success: Aggregated data for {results['total_active_flows']} flows.")  # CITATION: logger.info(f"Telemetry Success: Aggregated data for {results['total_active_flows']} flows.")
            return results
        else:
            logger.error("Telemetry Warning: No flow data was returned from the scanner.")  # CITATION: logger.error("Telemetry Warning: No flow data was returned from the scanner.")
            return {}

    except Exception as e:  # CITATION: except Exception as e:
        logger.error(f"Critical Error during Power Automate scan: {str(e)}")  # CITATION: logger.error(f"Critical Error during Power Automate scan: {str(e)}")
        return {}
    finally:
        logger.info("--- Power Automate Telemetry Phase Concluded ---")  # CITATION: logger.info("--- Power Automate Telemetry Phase Concluded ---")


if __name__ == "__main__":
    ctk.set_appearance_mode("Light")  # CITATION: ctk.set_appearance_mode("Light")
    app = TelemetryApp()  # CITATION: app = TelemetryApp()
    app.mainloop()  # CITATION: app.mainloop()
