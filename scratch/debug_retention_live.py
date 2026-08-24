import os
import sys
import json
sys.path.append(os.path.abspath("."))

from core.powershell.client import PowerShellClient
from core.powershell.retention import RetentionService
from core.graph.client import GraphClient
from core.graph.directory import DirectoryService

TENANT_ID = "a8a95e4b-10a6-4526-a2cb-db7682eba2e6"
CLIENT_ID = "16e96d46-0e64-481a-872f-c538c1a99311"
CLIENT_SECRET = "j_L8Q~DsaImyAmDqU4u~kjz~43b15N1PzllASbNW"

try:
    client = GraphClient(TENANT_ID, CLIENT_ID, CLIENT_SECRET)
    client.authenticate()
    dir_svc = DirectoryService(client)
    tenant_domain = dir_svc.get_tenant_primary_domain()
    print(f"Resolved tenant domain: {tenant_domain}")

    ps_client = PowerShellClient(tenant_id=tenant_domain, client_id=CLIENT_ID, client_secret=CLIENT_SECRET, cert_tenant_id=TENANT_ID)
    retention_service = RetentionService(ps_client)
    policies = retention_service.fetch_retention_policies()
    
    if isinstance(policies, dict) and "value" in policies:
        policies = policies["value"]
    elif not isinstance(policies, list):
        policies = [policies]
        
    for p in policies:
        print(p.get("Name", "Unknown"))
except Exception as e:
    print(f"Error fetching: {e}")
