import json
import sqlite3
import os
from core.powershell.dlp import DLPService

def debug_sits():
    db_path = "telemetry_results.db"
    if not os.path.exists(db_path):
        print(f"DB not found at {db_path}")
        return
        
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("SELECT * FROM config WHERE is_active = 1")
    active_tenant = c.fetchone()
    conn.close()
    
    if not active_tenant:
        print("No active tenant config found in DB.")
        return
        
    org = active_tenant['organization']
    client_id = active_tenant['client_id']
    cert_path = active_tenant['cert_path']
    cert_password = active_tenant['cert_password']
    
    print(f"Active tenant: {org}")
    
    dlp_svc = DLPService(None)
    args = [
        "-AppId", client_id,
        "-Organization", org,
        "-CertificatePath", cert_path
    ]
    if cert_password:
        args.extend(["-CertificatePassword", cert_password])
        
    print(f"Executing get_sensitive_info_types.ps1...")
    raw_output = dlp_svc.ps_client.execute_script("scripts/get_sensitive_info_types.ps1", args)
    
    with open("debug_raw_output.txt", "w", encoding='utf-8') as f:
        f.write(str(raw_output))
        
    print(f"Raw output saved to debug_raw_output.txt. Length: {len(raw_output) if raw_output else 0}")
    
    if not raw_output:
        print("Raw output is empty!")
        return

    print("\nAttempting JSON decode...")
    try:
        data = json.loads(raw_output)
        print("Standard JSON loaded successfully.")
    except json.JSONDecodeError as e:
        print(f"Standard JSON decode failed: {e}")
        lines = raw_output.strip().split('\n')
        json_str = ""
        for line in lines:
            if line.startswith("[") or line.startswith("{") or json_str:
                json_str += line
        if json_str:
            try:
                data = json.loads(json_str)
                print("Fallback JSON loaded successfully.")
            except Exception as e2:
                print(f"Fallback JSON decode failed: {e2}")
                return
        else:
            print("No JSON structure found.")
            return

    print("\n--- Parsing Analysis ---")
    print(f"Type of parsed data: {type(data)}")
    if isinstance(data, dict):
        print(f"Keys in parsed data: {list(data.keys())}")
        
        custom_sits = data.get("CustomRulePackages", [])
        print(f"\nCustomRulePackages Type: {type(custom_sits)}")
        if isinstance(custom_sits, list):
            print(f"CustomRulePackages Count: {len(custom_sits)}")
            if custom_sits:
                print(f"First element: {json.dumps(custom_sits[0], indent=2)[:500]}")
        elif isinstance(custom_sits, dict):
            print("WARNING: CustomRulePackages returned as a single dictionary.")
            print(f"Content: {json.dumps(custom_sits, indent=2)[:500]}")
        else:
            print(f"Content: {custom_sits}")
            
        edm_schemas = data.get("EdmSchemas", [])
        print(f"\nEdmSchemas Type: {type(edm_schemas)}")
        if isinstance(edm_schemas, list):
            print(f"EdmSchemas Count: {len(edm_schemas)}")
            if edm_schemas:
                print(f"First element: {json.dumps(edm_schemas[0], indent=2)[:500]}")
        elif isinstance(edm_schemas, dict):
            print("WARNING: EdmSchemas returned as a single dictionary.")
            print(f"Content: {json.dumps(edm_schemas, indent=2)[:500]}")
        else:
            print(f"Content: {edm_schemas}")
            
    print("\nDebug complete.")

if __name__ == "__main__":
    debug_sits()
