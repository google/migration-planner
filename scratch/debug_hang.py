import os
import sys
import time
import threading
import traceback
from unittest.mock import MagicMock

# Add workspace root to sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Mock filedialog and messagebox
import tkinter.filedialog
import tkinter.messagebox
tkinter.filedialog.asksaveasfilename = lambda *a, **k: "/usr/local/google/home/projjalkundu/.gemini/jetski/scratch/splash_one/scratch/temp_report.pdf"
tkinter.messagebox.showinfo = lambda *a, **k: None
tkinter.messagebox.showerror = lambda *a, **k: None

# Mock the backend telemetry data
import telemetry.active_users_usage
telemetry.active_users_usage.run_o365_pipeline = lambda *a, **k: [("Exchange Online", 10, 20, 30)]
telemetry.active_users_usage.run_o365_trend_pipeline = lambda *a, **k: {
    "dates": ["2026-06-01", "2026-06-02"], "office365": [10, 15], "exchange": [10, 12], "onedrive": [10, 11], "sharepoint": [10, 14], "teams": [10, 13]
}
telemetry.active_users_usage.run_m365_pipeline = lambda *a, **k: [("Word", 10)]

import telemetry.data_security_governance
telemetry.data_security_governance.fetch_sensitivity_labels_data = lambda *a, **k: {
    "labels": [{"name": "Public", "priority": 0, "hasProtection": False, "applicableTo": "all", "isEnabled": True}], "error": None
}
telemetry.data_security_governance.fetch_retention_policies_data = lambda *a, **k: {
    "policies": [{"Name": "Policy 1", "Workload": "Exchange", "Duration": "Unlimited", "DistributionStatus": "Success", "Enabled": True}], "error": None
}

import telemetry.calendar_telemetry
telemetry.calendar_telemetry.run_calendar_telemetry_pipeline = lambda *a, **k: {
    "CanUsersReserveRooms": True, "TotalCalendarResources": 5, "RoomsCount": 3, "EquipmentCount": 2,
    "RoomsError": None, "DevicesError": None, "IntegratedCalendarApps": "None", "NamingConvention": "None",
    "CanShareAttachments": True, "powershell_error": None
}

import telemetry.mailbox_usage
telemetry.mailbox_usage.run_mailbox_usage_pipeline = lambda *a, **k: {
    "total_mailboxes": 10, "total_storage_bytes": 1024*1024, "total_storage_formatted": "1.00 MB",
    "average_mailbox_size_bytes": 1024, "average_mailbox_size_formatted": "1.00 KB",
    "total_emails": 100, "average_emails": 10, "powershell_error": None
}

import telemetry.sharepoint_onedrive_usage
telemetry.sharepoint_onedrive_usage.run_sharepoint_pipeline = lambda *a, **k: {
    "total_sites": 5, "total_storage_bytes": 2048*1024, "total_storage_formatted": "2.00 MB",
    "average_site_size_bytes": 2048, "average_site_size_formatted": "2.00 KB",
    "inactive_sites": 1, "active_sites": 4, "external_sharing_enabled": True
}
telemetry.sharepoint_onedrive_usage.run_onedrive_pipeline = lambda *a, **k: {
    "total_accounts": 5, "total_storage_bytes": 2048*1024, "total_storage_formatted": "2.00 MB",
    "average_account_size_bytes": 2048, "average_account_size_formatted": "2.00 KB",
    "inactive_accounts": 1, "active_accounts": 4
}

import telemetry.power_automate
telemetry.power_automate.PowerAutomateScanner = MagicMock()
scanner_mock = telemetry.power_automate.PowerAutomateScanner.return_value
scanner_mock.scan_flows.return_value = {
    "total_environments": 1,
    "counts": {"Cloud Flows": 2, "Desktop Flows": 1},
    "active_counts": {"Cloud Flows": 1, "Desktop Flows": 1},
    "tier_counts": {"Personal Productivity": 2, "Enterprise/Departmental": 1},
    "active_tier_counts": {"Personal Productivity": 1, "Enterprise/Departmental": 1},
    "premium_connectors": ["shared_sql"],
    "custom_connectors": [],
    "complex_logic_flows": []
}

import core.graph.client
core.graph.client.GraphClient = MagicMock()

import deal_assistant
from deal_assistant import TelemetryApp

def dump_stacks():
    print("\n==================== THREAD SLOTS STACK TRACES ====================")
    id_to_name = {th.ident: th.name for th in threading.enumerate()}
    for thread_id, stack in sys._current_frames().items():
        thread_name = id_to_name.get(thread_id, "Unknown Thread")
        print(f"\nThread: {thread_name} (ID: {thread_id})")
        traceback.print_stack(stack)
    print("==================================================================\n")

def run_test():
    print("Launching TelemetryApp for deadlock analysis...")
    app = TelemetryApp()
    
    app.stored_tenant = "dummy-tenant"
    app.stored_client = "dummy-client"
    app.stored_secret = "dummy-secret"
    
    app.show_reports_page()
    reports_page = app.reports_page
    
    # Schedule thread stack dump 15 seconds from now
    app.after(15000, dump_stacks)
    # Schedule exit 20 seconds from now
    app.after(20000, app.on_closing)
    
    def test_sequence():
        try:
            time.sleep(1)
            
            # --- FIRST FETCH ---
            print("Triggering First Fetch...")
            app.after(0, reports_page.on_fetch_report_clicked)
            
            while True:
                time.sleep(0.5)
                if reports_page.fetch_btn.cget("state") == "normal":
                    break
            print("First Fetch Done!")
            
            time.sleep(1)
            
            # --- SECOND FETCH ---
            print("Triggering Second Fetch...")
            app.after(0, reports_page.on_fetch_report_clicked)
            
        except Exception as e:
            print(f"Error: {e}")

    threading.Thread(target=test_sequence, daemon=True).start()
    app.mainloop()

if __name__ == "__main__":
    run_test()
