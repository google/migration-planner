import os
import sys
import json
import logging

# Add the root directory to sys.path so we can import modules correctly
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from core.powershell.client import PowerShellClient

CLIENT_ID = "16e96d46-0e64-481a-872f-c538c1a99311"
CLIENT_SECRET = "j_L8Q~DsaImyAmDqU4u~kjz~43b15N1PzllASbNW"
TENANT_ID = "a8a95e4b-10a6-4526-a2cb-db7682eba2e6"

def debug_sensitive_info():
    print("Initializing PowerShellClient...")
    ps_client = PowerShellClient("acme1000.onmicrosoft.com", CLIENT_ID, CLIENT_SECRET, cert_tenant_id=TENANT_ID)
    
    try:
        cert_path = ps_client.locate_certificate()
    except Exception as e:
        print(f"Failed to locate certificate: {e}")
        return

    script_path = os.path.join(os.path.dirname(__file__), "get_sensitive_info_types.ps1")
    
    args = [
        "-AppId", ps_client.client_id,
        "-Organization", ps_client.tenant_id,
        "-CertificatePath", cert_path
    ]
    if ps_client.cert_password:
        args += ["-CertificatePassword", ps_client.cert_password]

    print("Executing script: get_sensitive_info_types.ps1 ...")
    try:
        raw_output = ps_client.execute_script(script_path, args)
        if not raw_output or not raw_output.strip():
            print("No output returned.")
            return
            
        print("\n--- RAW JSON OUTPUT ---\n")
        print(raw_output)
        
        try:
            data = json.loads(raw_output)
            print("\n--- PARSED DATA PREVIEW ---\n")
            print(json.dumps(data, indent=2))
        except json.JSONDecodeError:
            # Maybe there are some warnings before JSON
            lines = raw_output.strip().split('\n')
            json_str = ""
            for line in lines:
                if line.startswith("{") or line.startswith("[") or json_str:
                    json_str += line
            if json_str:
                data = json.loads(json_str)
                print("\n--- PARSED DATA PREVIEW (Extracted) ---\n")
                print(json.dumps(data, indent=2))
            else:
                print("Failed to parse JSON.")
            
    except Exception as e:
        print(f"Execution failed: {e}")

if __name__ == "__main__":
    debug_sensitive_info()
