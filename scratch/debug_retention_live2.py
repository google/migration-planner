import os
import sys
import json
sys.path.append(os.path.abspath("."))

from core.powershell.client import PowerShellClient
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
    
    script = """
    param($AppId, $Organization, $CertificatePath, $CertificatePassword)
    Import-Module ExchangeOnlineManagement
    $secPassword = ConvertTo-SecureString -String $CertificatePassword -AsPlainText -Force
    Connect-IPPSSession -CertificateFilePath $CertificatePath -CertificatePassword $secPassword -AppId $AppId -Organization $Organization -ShowBanner:$false -WarningAction SilentlyContinue
    
    $policies = Get-RetentionCompliancePolicy
    $count = @($policies).Count
    Write-Output "TOTAL POLICIES IN POWERSHELL: $count"
    
    foreach ($p in @($policies)) {
        Write-Output $p.Name
    }
    
    Disconnect-ExchangeOnline -Confirm:$false -WarningAction SilentlyContinue
    """
    
    with open("scratch/temp_ps.ps1", "w") as f:
        f.write(script)
        
    raw_output = ps_client.execute_script("../../scratch/temp_ps.ps1", [])
    print(raw_output)
except Exception as e:
    print(f"Error fetching: {e}")
