import sys
import os
import logging
import json

# Add the project root to sys.path so we can import modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.graph.delegated_auth import DelegatedAuthClient
from core.graph.ediscovery import EDiscoveryFetcher

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def main():
    print("--- eDiscovery Delegated Auth Debug Script ---")
    print("This script will attempt to authenticate via Delegated Auth and fetch eDiscovery cases.\n")

    tenant_id = input("Enter Tenant ID: ").strip()
    client_id = input("Enter Client ID: ").strip()
    client_secret = input("Enter Client Secret: ").strip()

    if not tenant_id or not client_id or not client_secret:
        print("Error: Tenant ID, Client ID, and Client Secret are all required.")
        sys.exit(1)

    print("\n[1] Initializing DelegatedAuthClient...")
    try:
        auth_client = DelegatedAuthClient(tenant_id, client_id, client_secret)
        
        print("[2] Requesting Token for eDiscovery.Read.All...")
        # This will open the browser if no cached token is valid
        token = auth_client.get_token(scopes=["https://graph.microsoft.com/eDiscovery.Read.All"])
        
        if not token:
            print("Failed to acquire token.")
            sys.exit(1)
            
        print("[SUCCESS] Acquired Delegated Token successfully!\n")
        
        print("[3] Fetching eDiscovery Cases via API...")
        fetcher = EDiscoveryFetcher(token)
        cases = fetcher.fetch_cases()
        
        print("\n--- API Response ---")
        print(json.dumps(cases, indent=2))
        
        print(f"\n[SUCCESS] Successfully fetched {len(cases)} cases!")

    except Exception as e:
        print(f"\n[ERROR] An error occurred: {e}")

if __name__ == "__main__":
    main()
