import sys
import os
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

sys.path.insert(0, os.path.abspath("."))
from core.graph.files.sharepoint_data_types import run_sharepoint_data_types_pipeline

def main():
    print("=== SharePoint Data Types Pipeline Debugger ===")
    tenant_id = input("Enter Tenant ID (e.g., contoso.onmicrosoft.com): ").strip()
    client_id = input("Enter Client ID (Application ID): ").strip()
    client_secret = input("Enter Client Secret: ").strip()

    if not all([tenant_id, client_id, client_secret]):
        print("All fields are required.")
        return

    print("\nStarting pipeline...\n")
    try:
        data = run_sharepoint_data_types_pipeline(client_id, client_secret, tenant_id)
        print("\n=== SUCCESS ===")
        print(data)
    except Exception as e:
        print("\n=== ERROR CAUGHT ===")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
