# Copyright 2026 Google LLC
"""Standalone application for the License Usage and Telemetry view."""

import os
import customtkinter as ctk
from telemetry.m365_telemetry import M365TelemetryTab, async_logger
import logging
from telemetry.power_automate import PowerAutomateScanner
from telemetry.styles import *

import queue
import threading
import ui.exchange_online_ui
import ui.chats_ui
import ui.files_ui

# Create custom helper base class to embed CTk windows as CTkFrames
class EmbeddedCTkFrameHelper(ctk.CTkFrame):
    _current_master = None

    def __init__(self, master=None, *args, **kwargs):
        actual_master = getattr(EmbeddedCTkFrameHelper, "_current_master", master)
        super().__init__(actual_master, *args, **kwargs)

    def title(self, *args, **kwargs):
        pass

    def geometry(self, *args, **kwargs):
        pass

    def protocol(self, *args, **kwargs):
        pass

    def attributes(self, *args, **kwargs):
        pass


# Patch estimator tool base classes in-memory
ui.exchange_online_ui.MigrationEstimatorTool.__bases__ = (EmbeddedCTkFrameHelper,)
ui.chats_ui.ChatMigrationEstimatorTool.__bases__ = (EmbeddedCTkFrameHelper,)


class EmbeddedExchangeOnlineTool(ui.exchange_online_ui.MigrationEstimatorTool):
    def __init__(self, master, controller, *args, **kwargs):
        EmbeddedCTkFrameHelper._current_master = master
        self.controller = controller
        super().__init__()
        
        # Populate credentials
        self.tenant_id.set(self.controller.stored_tenant)
        self.client_ids.set(self.controller.stored_client)
        self.client_secrets.set(self.controller.stored_secret)

    def create_entry(self, parent, label, var, show=None):
        if label in ["Tenant ID", "Client ID", "Client Secret"]:
            if parent.winfo_exists():
                parent.pack_forget()
                if parent.master and parent.master.winfo_exists():
                    parent.master.pack_forget()
            return
        super().create_entry(parent, label, var, show)

    def go_back_to_selector(self):
        if hasattr(self, "_back_callback") and self._back_callback:
            self._back_callback()


class EmbeddedChatTool(ui.chats_ui.ChatMigrationEstimatorTool):
    def __init__(self, master, controller, *args, **kwargs):
        EmbeddedCTkFrameHelper._current_master = master
        self.controller = controller
        super().__init__()
        
        # Populate credentials
        self.tenant_id.set(self.controller.stored_tenant)
        self.client_ids.set(self.controller.stored_client)
        self.client_secrets.set(self.controller.stored_secret)

    def create_entry(self, parent, label, var, show=None):
        if label in ["Tenant ID", "Client ID", "Client Secret"]:
            if parent.winfo_exists():
                parent.pack_forget()
                if parent.master and parent.master.winfo_exists():
                    parent.master.pack_forget()
            return
        super().create_entry(parent, label, var, show)

    def go_back_to_selector(self):
        if hasattr(self, "_back_callback") and self._back_callback:
            self._back_callback()


class EmbeddedFilesTool(ui.files_ui.FileMigrationEstimatorTool):
    def __init__(self, master, controller, *args, **kwargs):
        EmbeddedCTkFrameHelper._current_master = master
        self.controller = controller
        super().__init__()
        
        # Populate credentials
        self.tenant_id.set(self.controller.stored_tenant)
        self.client_ids.set(self.controller.stored_client)
        self.client_secrets.set(self.controller.stored_secret)

    def create_entry(self, parent, label, var, show=None):
        if label in ["Tenant ID", "Client ID", "Client Secret"]:
            if parent.winfo_exists():
                parent.pack_forget()
                if parent.master and parent.master.winfo_exists():
                    parent.master.pack_forget()
            return
        super().create_entry(parent, label, var, show)

    def go_back_to_selector(self):
        if hasattr(self, "_back_callback") and self._back_callback:
            self._back_callback()


class MigrationPlannerView(ctk.CTkFrame):
    """Container for the Migration Planner workload selector and tool views."""

    def __init__(self, master, controller, **kwargs):
        super().__init__(master, fg_color="transparent", **kwargs)
        self.controller = controller

        # Workload selector frame
        self.selector_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.selector_frame.pack(fill="both", expand=True)

        self.setup_selector_ui()

        # Active tool frame container
        self.active_tool_frame = None

    def setup_selector_ui(self):
        # Title of selector
        ctk.CTkLabel(
            self.selector_frame,
            text="Select Workload to Estimate",
            font=FONT_HEADER_MEDIUM,
            text_color=COLOR_TEXT_MAIN,
        ).pack(pady=(40, 10), anchor="w", padx=40)

        ctk.CTkLabel(
            self.selector_frame,
            text="Plan your migration by estimating data size and timelines for your workloads.",
            font=FONT_BODY_MEDIUM,
            text_color=COLOR_TEXT_SUB,
        ).pack(pady=(0, 30), anchor="w", padx=40)

        # Card container grid
        self.cards_container = ctk.CTkFrame(self.selector_frame, fg_color="transparent")
        self.cards_container.pack(fill="x", padx=40)

        self.cards_container.grid_columnconfigure((0, 1, 2), weight=1, uniform="equal")

        # Card 1: Exchange
        self.create_workload_card(
            parent=self.cards_container,
            column=0,
            icon="📩",
            title="Exchange Online",
            desc="Estimate migration time and resource sizes for mailboxes, calendars, and contacts.",
            callback=lambda: self.launch_tool("Exchange")
        )

        # Card 2: Chat
        self.create_workload_card(
            parent=self.cards_container,
            column=1,
            icon="💬",
            title="Chat (Teams)",
            desc="Plan Teams private chats, group channels, and message history migration.",
            callback=lambda: self.launch_tool("Chat")
        )

        # Card 3: Files
        self.create_workload_card(
            parent=self.cards_container,
            column=2,
            icon="📁",
            title="Files (SharePoint/OneDrive)",
            desc="Analyze OneDrive personal sites and SharePoint team site collections.",
            callback=lambda: self.launch_tool("Files")
        )

    def create_workload_card(self, parent, column, icon, title, desc, callback):
        card = ctk.CTkFrame(
            parent,
            fg_color=COLOR_SURFACE,
            corner_radius=12,
            border_width=1,
            border_color=COLOR_OUTLINE_LIGHT
        )
        card.grid(row=0, column=column, padx=10, pady=10, sticky="nsew")

        # Icon
        ctk.CTkLabel(
            card,
            text=icon,
            font=ctk.CTkFont(size=36),
            text_color=COLOR_PRIMARY
        ).pack(pady=(25, 10))

        # Title
        ctk.CTkLabel(
            card,
            text=title,
            font=FONT_BODY_BOLD,
            text_color=COLOR_TEXT_MAIN
        ).pack(pady=(0, 10))

        # Description
        ctk.CTkLabel(
            card,
            text=desc,
            font=FONT_BODY_MEDIUM,
            text_color=COLOR_TEXT_SUB,
            wraplength=220,
            justify="center"
        ).pack(fill="both", expand=True, padx=20, pady=(0, 20))

        # Launch Button
        btn = ctk.CTkButton(
            card,
            text="Open Planner",
            command=callback,
            height=36,
            corner_radius=8,
            fg_color=COLOR_PRIMARY,
            hover_color=COLOR_PRIMARY_HOVER,
            font=FONT_BODY_BOLD
        )
        btn.pack(pady=(0, 25), padx=20, fill="x")

    def launch_tool(self, workload):
        # Hide selector
        self.selector_frame.pack_forget()

        # Clean old tool if any
        if self.active_tool_frame:
            self.active_tool_frame.destroy()
            self.active_tool_frame = None

        # Instantiate the embedded tool
        if workload == "Exchange":
            self.active_tool_frame = EmbeddedExchangeOnlineTool(self, self.controller)
        elif workload == "Chat":
            self.active_tool_frame = EmbeddedChatTool(self, self.controller)
        elif workload == "Files":
            self.active_tool_frame = EmbeddedFilesTool(self, self.controller)

        if self.active_tool_frame:
            self.active_tool_frame._back_callback = self.show_selector
            self.active_tool_frame.pack(fill="both", expand=True)

    def show_selector(self):
        if self.active_tool_frame:
            self.active_tool_frame.pack_forget()
            self.active_tool_frame.destroy()
            self.active_tool_frame = None
        self.selector_frame.pack(fill="both", expand=True)

# Orchestrator logging
logger = logging.getLogger("M365TelemetryAsyncLogger.TelemetryOrchestrator")


class TelemetryApp(ctk.CTk):
    """Standalone application for the License Usage and Telemetry view."""

    def __init__(self):
        super().__init__()
        logger.info("Initializing TelemetryApp application...")
        self.title("Deal Assistant")  # CITATION: self.title("Deal Assistant")
        self.geometry("1230x950")  # Expanded window width to support increased sidebar dimensions

        # FIX: Bind the window close button to a custom exit handler
        # to prevent CustomTkinter 'after script' errors when closing.
        self.protocol("WM_DELETE_WINDOW", self.on_closing)  # CITATION: self.protocol("WM_DELETE_WINDOW", self.on_closing)

        # Initialize variables required by the M365TelemetryTab
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
        logger.info("TelemetryApp UI initialized successfully.")

    def setup_auth_ui(self):
        """Builds a modern, polished Connection interface for Page 1."""
        # Welcome Branding Card
        self.brand_card = ctk.CTkFrame(
            self.auth_frame,
            fg_color=COLOR_SURFACE,
            corner_radius=12,
            border_width=1,
            border_color=COLOR_OUTLINE_LIGHT
        )
        self.brand_card.pack(fill="x", padx=40, pady=(60, 20))

        self.brand_title = ctk.CTkLabel(
            self.brand_card,
            text="Deal Assistant",
            font=FONT_HEADER_MEDIUM,
            text_color=COLOR_PRIMARY
        )
        self.brand_title.pack(pady=(25, 5))

        self.brand_subtitle = ctk.CTkLabel(
            self.brand_card,
            text="Connect your Azure App Credentials to begin auditing your tenant.",
            font=FONT_BODY_MEDIUM,
            text_color=COLOR_TEXT_SUB
        )
        self.brand_subtitle.pack(pady=(0, 25))

        # Credentials Form Box
        self.credentials_card = ctk.CTkFrame(
            self.auth_frame,
            fg_color=COLOR_SURFACE,
            corner_radius=12,
            border_width=1,
            border_color=COLOR_OUTLINE_LIGHT
        )
        self.credentials_card.pack(fill="both", expand=True, padx=40, pady=(0, 40))

        self.form_container = ctk.CTkFrame(self.credentials_card, fg_color="transparent")
        self.form_container.pack(pady=40, anchor="center")

        # Tenant ID Input
        self.tenant_lbl = ctk.CTkLabel(self.form_container, text="Tenant ID", font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN)
        self.tenant_lbl.pack(anchor="w", pady=(10, 5))
        import tkinter as tk

        # Tenant ID Input Wrapper Frame (using native tk.Frame to ensure full X11 border rendering)
        self.tenant_border = tk.Frame(
            self.form_container, width=850, height=42,
            highlightbackground=COLOR_OUTLINE, highlightcolor=COLOR_PRIMARY, highlightthickness=1,
            bd=0, background=COLOR_SURFACE
        )
        self.tenant_border.pack(pady=(0, 15))
        self.tenant_border.pack_propagate(False)

        self.tenant_entry = ctk.CTkEntry(
            self.tenant_border, border_width=0, fg_color="transparent", text_color=COLOR_TEXT_MAIN,
            placeholder_text="Enter Tenant ID"
        )
        self.tenant_entry.pack(fill="both", expand=True, padx=10, pady=2)

        # Client ID Input
        self.client_lbl = ctk.CTkLabel(self.form_container, text="Client ID", font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN)
        self.client_lbl.pack(anchor="w", pady=(10, 5))
        
        self.client_border = tk.Frame(
            self.form_container, width=850, height=42,
            highlightbackground=COLOR_OUTLINE, highlightcolor=COLOR_PRIMARY, highlightthickness=1,
            bd=0, background=COLOR_SURFACE
        )
        self.client_border.pack(pady=(0, 15))
        self.client_border.pack_propagate(False)

        self.client_entry = ctk.CTkEntry(
            self.client_border, border_width=0, fg_color="transparent", text_color=COLOR_TEXT_MAIN,
            placeholder_text="Enter Client ID"
        )
        self.client_entry.pack(fill="both", expand=True, padx=10, pady=2)

        # Client Secret Input
        self.secret_lbl = ctk.CTkLabel(self.form_container, text="Client Secret", font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN)
        self.secret_lbl.pack(anchor="w", pady=(10, 5))
        
        self.secret_border = tk.Frame(
            self.form_container, width=850, height=42,
            highlightbackground=COLOR_OUTLINE, highlightcolor=COLOR_PRIMARY, highlightthickness=1,
            bd=0, background=COLOR_SURFACE
        )
        self.secret_border.pack(pady=(0, 30))
        self.secret_border.pack_propagate(False)

        self.secret_entry = ctk.CTkEntry(
            self.secret_border, show="*", border_width=0, fg_color="transparent", text_color=COLOR_TEXT_MAIN,
            placeholder_text="Enter Client Secret"
        )
        self.secret_entry.pack(fill="both", expand=True, padx=10, pady=2)

        # Status & Feedback Display
        self.auth_status_lbl = ctk.CTkLabel(self.form_container, text="", font=FONT_BODY_MEDIUM, text_color=COLOR_ERROR)
        self.auth_status_lbl.pack(pady=(0, 15))

        # Action Submission Trigger
        self.connect_btn = ctk.CTkButton(
            self.form_container,
            text="Connect & Continue",
            command=self.on_connect_clicked,
            height=44,
            corner_radius=8,
            fg_color=COLOR_PRIMARY,
            hover_color=COLOR_PRIMARY_HOVER,
            font=FONT_BODY_BOLD
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
        """Validates inputs, caches credentials in-memory, checks certificate status, and transitions/generates cert."""
        tenant = self.tenant_entry.get().strip()
        client = self.client_entry.get().strip()
        secret = self.secret_entry.get().strip()

        logger.info("Connect & Continue clicked. Verifying connection credentials...")
        if not tenant or not client or not secret:
            logger.warning("Connection failed: Missing one or more required credential fields.")
            self.auth_status_lbl.configure(text="Error: Tenant ID, Client ID, and Client Secret are required.", text_color="red")
            return

        self.auth_status_lbl.configure(text="")

        # Cache connection details safely in memory
        self.stored_tenant = tenant
        self.stored_client = client
        self.stored_secret = secret

        logger.info("Credentials validated and cached in memory. Updating log directories...")

        # Update log directory to use sub-folder based on tenant and client
        from telemetry.m365_telemetry import update_log_directory as update_license_log_dir
        from core.cert_auth import update_log_directory as update_cert_log_dir
        
        update_license_log_dir(tenant, client)
        update_cert_log_dir(tenant, client)

        from core.cert_auth import check_certificate_exists, generate_certificate, load_certificate

        if check_certificate_exists(tenant_id=tenant, client_id=client):
            try:
                # Decrypt the PFX certificate using the client secret
                load_certificate(secret, tenant_id=tenant, client_id=client)
            except Exception as e:
                from tkinter import messagebox
                messagebox.showerror(
                    "Certificate Decryption Error",
                    f"Unable to unlock certificate with Client Secret. Proceeding with standard Client Secret authentication fallback.\n\nError: {e}",
                    parent=self
                )
            # Proceed to reports page in either case
            self.show_reports_page()
        else:
            try:
                # Generate new certificate and pfx encrypted with the client secret
                pem_path, _ = generate_certificate(secret, tenant_id=tenant, client_id=client)
                # Setup instructions UI requesting the user to upload it to Entra
                self.setup_cert_instructions_ui(pem_path)
            except Exception as e:
                from tkinter import messagebox
                messagebox.showerror(
                    "Certificate Generation Error",
                    f"Unable to generate certificate. Proceeding with standard Client Secret authentication fallback.\n\nError: {e}",
                    parent=self
                )
                self.show_reports_page()

    def setup_cert_instructions_ui(self, pem_path):
        """Displays certificate upload instructions screen when a new certificate is generated."""
        self.form_container.pack_forget()

        if hasattr(self, "cert_container") and self.cert_container:
            self.cert_container.destroy()

        self.cert_container = ctk.CTkFrame(self.credentials_card, fg_color="transparent")
        self.cert_container.pack(pady=30, padx=50, fill="both", expand=True)

        ctk.CTkLabel(
            self.cert_container,
            text="Certificate Upload Required",
            font=ctk.CTkFont(size=18, weight="bold"),
            text_color="#1E3A8A"
        ).pack(anchor="w", pady=(0, 15))

        instructions_text = (
            "A new security certificate has been generated for hybrid authentication.\n\n"
            f"1. Locate the certificate file at:\n   {pem_path}\n\n"
            "2. Upload this 'certificate.pem' file to your App Registration in the Microsoft Entra ID portal.\n"
            "   (App Registration -> Certificates & secrets -> Certificates -> Upload certificate)\n\n"
            "3. Once you have successfully uploaded the certificate, click the 'Continue' button below."
        )

        self.cert_instr_lbl = ctk.CTkLabel(
            self.cert_container,
            text=instructions_text,
            font=ctk.CTkFont(size=13),
            text_color="#374151",
            justify="left"
        )
        self.cert_instr_lbl.pack(anchor="w", pady=(0, 25))

        self.cert_continue_btn = ctk.CTkButton(
            self.cert_container,
            text="I have uploaded the certificate. Continue",
            command=self.on_cert_continue_clicked,
            height=45,
            fg_color="#1E3A8A",
            hover_color="#172554",
            font=ctk.CTkFont(size=14, weight="bold")
        )
        self.cert_continue_btn.pack(fill="x")

    def on_cert_continue_clicked(self):
        """Validates certificate after user claims to have uploaded it and transitions to reports."""
        from core.cert_auth import load_certificate
        try:
            # Verify we can unlock/read the cert successfully
            load_certificate(self.stored_secret, tenant_id=self.stored_tenant, client_id=self.stored_client)
        except Exception as e:
            from tkinter import messagebox
            messagebox.showerror(
                "Certificate Verification Error",
                f"Unable to verify certificate. Proceeding with standard Client Secret authentication fallback.\n\nError: {e}",
                parent=self
            )
        self.show_reports_page()
        
        # Reset UI form in case of subsequent logins
        if hasattr(self, "cert_container") and self.cert_container:
            self.cert_container.pack_forget()
        self.form_container.pack(pady=40, padx=50, fill="both", expand=True)

    def on_disconnect_clicked(self):
        """Clears all session properties and safely resets screens back to Page 1."""
        logger.info("Disconnect clicked. Clearing cached session and credentials...")
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

        # Revert log directories to default
        from telemetry.m365_telemetry import update_log_directory as update_license_log_dir
        from core.cert_auth import update_log_directory as update_cert_log_dir
        
        update_license_log_dir()
        update_cert_log_dir()

        # Clean up cert screen UI and restore normal entry layout
        if hasattr(self, "cert_container") and self.cert_container:
            try:
                self.cert_container.pack_forget()
            except Exception:
                pass
        self.form_container.pack(pady=40, padx=50, fill="both", expand=True)

        # Shifts screen orientation
        self.show_auth_page()
        logger.info("Session successfully disconnected. Returned to Auth page.")

    def show_auth_page(self):
        """Transitions view port to Page 1 (Authentication screen)."""
        logger.info("Showing Authentication Page.")
        self.report_frame.pack_forget()
        self.auth_frame.pack(fill="both", expand=True)

    def show_reports_page(self):
        """Transitions view port to Page 2 (Reports Dashboard)."""
        logger.info("Showing Reports Dashboard Page.")
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
        self.sidebar = SidebarFrame(
            self,
            disconnect_callback=self.controller.on_disconnect_clicked,
            selection_callback=self.on_sidebar_selection_changed
        )
        self.sidebar.pack(side="left", fill="y", padx=(0, 10))

        # 2. Right-hand Main Dashboard Container
        self.dashboard_container = ctk.CTkFrame(self, fg_color="transparent")
        self.dashboard_container.pack(side="right", fill="both", expand=True)

        # Top Header Bar mimicking Google Workspace Deal Assistant details
        self.nav_header = ctk.CTkFrame(
            self.dashboard_container,
            fg_color=COLOR_SURFACE,
            height=70,
            corner_radius=12,
            border_width=1,
            border_color=COLOR_OUTLINE_LIGHT
        )
        self.nav_header.pack(fill="x", pady=(0, 15))
        self.nav_header.pack_propagate(False)

        # Text container frame to keep alignment clean next to the Action Button
        self.header_text_frame = ctk.CTkFrame(self.nav_header, fg_color="transparent")
        self.header_text_frame.pack(side="left", padx=20)

        self.nav_title = ctk.CTkLabel(
            self.header_text_frame,
            text="Usage Report",
            font=ctk.CTkFont(family="Segoe UI", size=20, weight="bold"),
            text_color=COLOR_TEXT_MAIN
        )
        self.nav_title.pack(anchor="w")



        # 3. Fetch Report button on the right side of the header panel (Stage 2)
        self.fetch_btn = ctk.CTkButton(
            self.nav_header,
            text="Fetch Report",
            command=self.on_fetch_report_clicked,
            width=150,
            height=36,
            corner_radius=8,
            fg_color=COLOR_PRIMARY,
            hover_color=COLOR_PRIMARY_HOVER,
            font=FONT_BODY_BOLD
        )
        self.fetch_btn.pack(side="right", padx=20, pady=17)

        # 4. Download PDF button next to Fetch Report
        self.pdf_btn = ctk.CTkButton(
            self.nav_header,
            text="Download PDF",
            command=self.on_download_pdf_clicked,
            width=150,
            height=36,
            corner_radius=8,
            fg_color="transparent",
            border_width=1,
            border_color=COLOR_PRIMARY,
            text_color=COLOR_PRIMARY,
            hover_color=COLOR_SECONDARY_HOVER,
            font=FONT_BODY_BOLD,
            state="disabled"
        )
        self.pdf_btn.pack(side="right", padx=(0, 20), pady=17)


        # Initialize the telemetry view (No TabView layout)
        self.m365_telemetry_view = M365TelemetryTab(
            master=self.dashboard_container, 
            log_callback=controller.log_msg, 
            retries_var=retries_var, 
            backoff_var=backoff_var
        )
        self.m365_telemetry_view.pack(fill="both", expand=True)
        self.m365_telemetry_view.on_all_done_callback = self.on_telemetry_fetch_completed

        # Initialize the migration planner view (initially hidden)
        self.migration_planner_view = MigrationPlannerView(
            master=self.dashboard_container,
            controller=self.controller
        )

        # Adapt layout recursively to hide original inputs from view
        self.adapt_embedded_view()

    def on_sidebar_selection_changed(self, label):
        if label == "Usage and adoption":
            # Show Telemetry, Hide Migration Planner
            self.migration_planner_view.pack_forget()
            self.nav_title.configure(text="Usage Report")
            self.fetch_btn.pack(side="right", padx=20, pady=17)
            self.pdf_btn.pack(side="right", padx=(0, 20), pady=17)
            self.m365_telemetry_view.pack(fill="both", expand=True)
        elif label == "Migration planner":
            # Show Migration Planner, Hide Telemetry
            self.m365_telemetry_view.pack_forget()
            self.fetch_btn.pack_forget()
            self.pdf_btn.pack_forget()
            self.nav_title.configure(text="Migration Planner")
            self.migration_planner_view.pack(fill="both", expand=True)

    def adapt_embedded_view(self):
        """Traverses M365TelemetryTab to identify and hide native login components."""
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

        find_widgets_recursive(self.m365_telemetry_view)

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

        if hasattr(self.m365_telemetry_view, "inputs_frame"):
            self.m365_telemetry_view.inputs_frame.pack_forget()
            self.m365_telemetry_view.inputs_frame.grid_forget()

    def on_fetch_report_clicked(self):
        """Stage 2: Migrates stored variables into the hidden entries and clicks the submit thread."""
        tenant = self.controller.stored_tenant
        client = self.controller.stored_client
        secret = self.controller.stored_secret

        if not tenant or not client or not secret:
            logger.warning("Fetch Report triggered, but connection credentials are empty.")
            return

        logger.info("Fetch Report triggered. Invoking background parallel audits...")
        # Disable the trigger button and update visual text during background thread audit
        self.fetch_btn.configure(state="disabled", text="Fetching...", fg_color="#64748B")
        self.pdf_btn.configure(state="disabled")

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
        """Callback from M365TelemetryTab when all parallel reports complete."""
        self.fetch_btn.configure(state="normal", text="Fetch Report", fg_color="#1E3A8A")
        if success:
            self.pdf_btn.configure(state="normal")
        else:
            self.pdf_btn.configure(state="disabled")

    def on_download_pdf_clicked(self):
        """Prompts the user to save the M365 usage report as a detailed PDF file."""
        from tkinter import filedialog, messagebox
        import datetime
        from telemetry.pdf_report import generate_pdf_report
        
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        f = filedialog.asksaveasfilename(
            initialfile=f"m365_usage_report_{ts}.pdf",
            defaultextension=".pdf",
            filetypes=[("PDF Documents", "*.pdf"), ("All Files", "*.*")],
            parent=self
        )
        if not f:
            return
            
        data = self.m365_telemetry_view.get_all_telemetry_data()
        
        try:
            generate_pdf_report(data, f)
            messagebox.showinfo("Export Successful", f"PDF report successfully saved to:\n{f}", parent=self)
        except Exception as e:
            logger.error("Failed to generate PDF report", exc_info=True)
            messagebox.showerror("Export Failed", f"Failed to generate PDF report: {e}", parent=self)

    def clear_session_data(self):
        """Wipes the cached parameters from telemetry objects and resets the Fetch button."""
        logger.info("Clearing session data in ReportsPage.")
        for entry in self.embedded_entries:
            entry.delete(0, "end")
        
        # Reset Fetch Report button state
        self.fetch_btn.configure(state="normal", text="Fetch Report", fg_color="#1E3A8A")
        self.pdf_btn.configure(state="disabled")

        # Reset the telemetry coordinator tab and hide all grids
        self.m365_telemetry_view.reset_tab()

        # Reset migration planner view back to selector screen
        self.migration_planner_view.show_selector()

        # Reset the sidebar selection state
        self.sidebar.reset_selection()
        # Switch back UI elements to default Usage report view
        self.on_sidebar_selection_changed("Usage and adoption")


class SidebarFrame(ctk.CTkFrame):
    """Collapsible Left Navigation Sidebar, matching Workspace Deal Assistant styling."""

    def __init__(self, master, disconnect_callback, selection_callback, **kwargs):
        # Increased initial width to 380px to avoid text truncation of longer menu items
        super().__init__(master, width=380, fg_color=COLOR_SURFACE, corner_radius=12, border_width=1, border_color=COLOR_OUTLINE_LIGHT, **kwargs)
        self.pack_propagate(False)  # Lock sidebar panel dimensions
        self.disconnect_callback = disconnect_callback
        self.selection_callback = selection_callback
        self.is_expanded = True

        # Header Branding Section
        self.header_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.header_frame.pack(fill="x", padx=20, pady=(25, 30))

        # Brand Icon representing a deal helper (Handshake 🤝)
        self.logo_label = ctk.CTkLabel(
            self.header_frame,
            text="🤝",
            font=ctk.CTkFont(family="Segoe UI", size=24),
            text_color=COLOR_PRIMARY
        )
        self.logo_label.pack(side="left", padx=(5, 5))

        self.brand_text_area = ctk.CTkFrame(self.header_frame, fg_color="transparent")
        self.brand_text_area.pack(side="left", fill="both", expand=True)

        self.brand_title = ctk.CTkLabel(
            self.brand_text_area,
            text="Deal Assistant",
            font=FONT_HEADER_SMALL,
            text_color=COLOR_TEXT_MAIN,
            anchor="w"
        )
        self.brand_title.pack(fill="x", pady=2)

        # Collapse / Expand control button
        self.toggle_btn = ctk.CTkButton(
            self,
            text="⏴",
            command=self.toggle_sidebar,
            width=26,
            height=28,
            corner_radius=13,
            fg_color=COLOR_SURFACE_VARIANT,
            hover_color=COLOR_SECONDARY_HOVER,
            text_color=COLOR_TEXT_SUB,
            font=FONT_BODY_BOLD
        )
        self.toggle_btn.place(relx=1.0, x=-22, y=26, anchor="center")

        # Vertical menu container (increased padding for larger sidebar)
        self.menu_items_frame = ctk.CTkFrame(self, fg_color="transparent")
        self.menu_items_frame.pack(fill="both", expand=True, padx=20)

        # Definitions for active/inactive routes
        self.menu_buttons = []
        self.menu_data = [
            ("Usage and adoption", "📊", True),
            ("Migration planner", "🚀", False)
        ]

        self.render_navigation_menu()

        # Session closure button pinned safely at bottom
        self.disconnect_row = ctk.CTkFrame(self, fg_color="transparent")
        self.disconnect_row.pack(fill="x", side="bottom", padx=20, pady=25)

        self.disconnect_symbol = ctk.CTkLabel(
            self.disconnect_row,
            text="🚪",
            font=ctk.CTkFont(family="Segoe UI", size=14),
            text_color=COLOR_ERROR
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
            text_color=COLOR_ERROR,
            hover_color="#FCE8E6",
            font=FONT_BODY_MEDIUM
        )
        self.disconnect_btn.pack(side="left", fill="x", expand=True)

        # RAM Usage Display Row (packed second with side="bottom", placing it directly above disconnect_row)
        self.ram_row = ctk.CTkFrame(self, fg_color="transparent")
        self.ram_row.pack(fill="x", side="bottom", padx=20, pady=(0, 10))

        self.ram_symbol = ctk.CTkLabel(
            self.ram_row,
            text="💾",
            font=ctk.CTkFont(family="Segoe UI", size=14),
            text_color=COLOR_TEXT_SUB
        )
        self.ram_symbol.pack(side="left", padx=(12, 6))

        self.ram_lbl = ctk.CTkLabel(
            self.ram_row,
            text="RAM: Checking...",
            anchor="w",
            height=38,
            text_color=COLOR_TEXT_SUB,
            font=FONT_BODY_MEDIUM
        )
        self.ram_lbl.pack(side="left", fill="x", expand=True)

        self._last_ram_log_time = 0
        # Start periodic RAM usage updates
        self.update_ram_usage()

    def reset_selection(self):
        self.menu_data = [
            ("Usage and adoption", "📊", True),
            ("Migration planner", "🚀", False)
        ]
        self.render_navigation_menu()

    def on_menu_item_clicked(self, label):
        for i, (m_label, m_icon, m_active) in enumerate(self.menu_data):
            self.menu_data[i] = (m_label, m_icon, m_label == label)
        self.render_navigation_menu()
        self.selection_callback(label)



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
                btn_fg = COLOR_TONAL_BG
                text_color = COLOR_PRIMARY
                hover_color = "#D2E3FC"
                weight = "bold"
            else:
                btn_fg = "transparent"
                text_color = COLOR_TEXT_SUB
                hover_color = COLOR_SURFACE_VARIANT
                weight = "normal"

            icon_lbl = ctk.CTkLabel(
                row_frame,
                text=icon,
                font=ctk.CTkFont(family="Segoe UI", size=15),
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
                state="normal",
                font=ctk.CTkFont(family="Segoe UI", size=13, weight=weight),
                command=lambda l=label: self.on_menu_item_clicked(l)
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
            self.ram_lbl.configure(text="")
            logger.info("Sidebar collapsed.")
        else:
            # Return layout to expanded parameters (380px)
            self.configure(width=380)
            self.logo_label.pack(side="left", padx=(5, 5))
            self.brand_text_area.pack(side="left", fill="both", expand=True)
            self.is_expanded = True
            self.toggle_btn.configure(text="⏴")

            # Restore original strings dynamically
            for idx, (row, btn, icon) in enumerate(self.menu_buttons):
                btn.configure(text=self.menu_data[idx][0])
            self.disconnect_btn.configure(text="Disconnect")
            self.update_ram_label_immediate()
            logger.info("Sidebar expanded.")

    def update_ram_label_immediate(self):
        """Updates the RAM label text immediately without waiting for the timer."""
        try:
            import psutil
            process = psutil.Process(os.getpid())
            ram_mb = process.memory_info().rss / (1024 * 1024)
            if self.is_expanded:
                self.ram_lbl.configure(text=f"RAM: {ram_mb:.1f} MB")
            else:
                self.ram_lbl.configure(text="")

            # Log RAM usage to log file every 10 seconds
            import time
            now = time.time()
            if now - self._last_ram_log_time >= 10:
                logger.info(f"App memory consumption: {ram_mb:.1f} MB")
                self._last_ram_log_time = now
        except Exception as e:
            logger.error(f"Error checking RAM usage: {e}")
            if self.is_expanded:
                self.ram_lbl.configure(text="RAM: N/A")
            else:
                self.ram_lbl.configure(text="")


    def update_ram_usage(self):
        """Periodically updates the displayed RAM usage of the current process."""
        self.update_ram_label_immediate()
        self._ram_timer_id = self.after(2000, self.update_ram_usage)



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
