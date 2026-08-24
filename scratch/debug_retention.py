from core.auth import get_stored_credentials
from telemetry.data_security_governance import fetch_retention_policies_data

tenant, clients, secrets = get_stored_credentials()
if tenant and clients:
    res = fetch_retention_policies_data(clients[0], secrets[0], tenant)
    policies = res.get("policies")
    print(f"Policies type: {type(policies)}")
    if isinstance(policies, list):
        print(f"Length: {len(policies)}")
    else:
        print("Not a list.")
else:
    print("No credentials found.")
