import os
import sys

# Add the root directory to sys.path so we can import modules correctly
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from telemetry.data_security_governance import fetch_dlp_policies_data

CLIENT_ID = "16e96d46-0e64-481a-872f-c538c1a99311"
CLIENT_SECRET = "j_L8Q~DsaImyAmDqU4u~kjz~43b15N1PzllASbNW"
TENANT_ID = "a8a95e4b-10a6-4526-a2cb-db7682eba2e6"

print("Fetching DLP policies...")
res = fetch_dlp_policies_data(CLIENT_ID, CLIENT_SECRET, TENANT_ID)

print("\n--- Result ---")
if res.get("error"):
    print("ERROR:", res["error"])
else:
    policies = res.get("policies", {})
    if isinstance(policies, dict) and "value" in policies:
        policies = policies["value"]
    print(f"Success! Found {len(policies)} policies.")
    if policies:
        print("First policy:", policies[0])
