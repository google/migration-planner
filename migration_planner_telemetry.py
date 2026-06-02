# Copyright 2026 Google LLC
"""Standalone application for the License Usage and Telemetry view."""

import os
import customtkinter as ctk
from telemetry.license_usage import LicenseUsageTab, async_logger
import logging
from telemetry.power_automate import PowerAutomateScanner

class TelemetryApp(ctk.CTk):
    """Standalone application for the License Usage and Telemetry view."""

    def __init__(self):
        super().__init__()
        self.title("Migration Planner - Telemetry")
        self.geometry("950x900")
        
        # FIX: Bind the window close button to a custom exit handler
        # to prevent CustomTkinter 'after script' errors when closing.
        self.protocol("WM_DELETE_WINDOW", self.on_closing)
        
        # Initialize variables required by the LicenseUsageTab
        self.retries = ctk.IntVar(value=30)
        self.backoff = ctk.IntVar(value=2)
        
        # Initialize the telemetry view using the main window as the master (No TabView)
        self.license_usage_view = LicenseUsageTab(
            master=self, 
            log_callback=self.log_msg, 
            retries_var=self.retries, 
            backoff_var=self.backoff
        )
        self.license_usage_view.pack(fill="both", expand=True, padx=15, pady=15)

    def log_msg(self, text):
        """Simple callback handler for telemetry UI logs. Pipes to log file instead of stdout."""
        async_logger.info(text)

    def on_closing(self):
        """Trigger an OS-level exit to cleanly bypass Tkinter background tasks."""
        self.destroy()
        os._exit(0)

    def collect_power_automate_telemetry(tenant_id, client_id, client_secret, env_url):
        """Integrates the Power Automate scan into the telemetry execution flow."""
        # Orchestrator logging
        logger = logging.getLogger("TelemetryOrchestrator")
        logger.info("--- Power Automate Telemetry Phase Initiated ---")
        
        if not env_url:
            logger.warning("Skipping Power Automate: Environment URL not provided.")
            return {}

        try:
            scanner = PowerAutomateScanner(tenant_id, client_id, client_secret, env_url)
            results = scanner.scan_flows()
        
            if results:
                logger.info(f"Telemetry Success: Aggregated data for {results['total_active_flows']} flows.")
                return results
            else:
                logger.error("Telemetry Warning: No flow data was returned from the scanner.")
                return {}
            
        except Exception as e:
            logger.error(f"Critical Error during Power Automate scan: {str(e)}")
            return {}
        finally:
            logger.info("--- Power Automate Telemetry Phase Concluded ---")

if __name__ == "__main__":
    ctk.set_appearance_mode("Light")
    app = TelemetryApp()
    app.mainloop()
