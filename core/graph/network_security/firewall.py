# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Intune Firewall and Proxy configurations data pipeline."""

import logging
import pandas as pd
from core.graph.client import GraphClient

logger = logging.getLogger(__name__)

def run_firewall_pipeline(
    client_id: str,
    client_secret: str,
    tenant_id: str,
    csv_path: str = None,
    on_page_callback=None,
    is_cancelled_callback=None
):
    """Fetch Intune Firewall and Proxy configurations."""
    client = GraphClient(
        tenant_id=tenant_id,
        client_ids=client_id,
        client_secrets=client_secret,
        concurrency=1,
        retries=5,
        backoff=2
    )
    client.authenticate()
    
    url = "https://graph.microsoft.com/v1.0/deviceManagement/deviceConfigurations"
    token_slot = client.get_active_token()
    session = client.get_session()
    
    headers = {
        "Authorization": f"Bearer {token_slot['token']}",
        "Accept": "application/json"
    }
    
    rows = []
    try:
        logger.info("Querying Intune Device Configurations for Firewall/Proxy...")
        while url:
            if is_cancelled_callback and is_cancelled_callback(): break
            resp = session.get(url, headers=headers)
            if resp.status_code == 200:
                data = resp.json()
                value_list = data.get("value", [])
                
                for p in value_list:
                    p["name"] = p.get("displayName") or p.get("name") or "N/A"
                    p["description"] = p.get("description") or "N/A"
                    
                    odata_type = p.get("@odata.type", "")
                    policy_type = odata_type.replace("#microsoft.graph.", "")
                    p["policy_type"] = policy_type
                    
                    is_firewall = "Windows10EndpointProtection" in policy_type or "Firewall" in p["name"] or "Firewall" in p["description"] or "firewall" in policy_type.lower()
                    firewall_status = "Configured" if is_firewall else "Not Configured"
                    
                    if "Windows10EndpointProtectionConfiguration" in odata_type:
                        fw_enable = p.get("firewallEnableDomainProfile") or p.get("firewallEnablePrivateProfile") or p.get("firewallEnablePublicProfile")
                        if fw_enable is not None:
                            firewall_status = "Enabled" if fw_enable else "Disabled"
                    p["firewall_status"] = firewall_status
                    
                    is_proxy = "GeneralConfiguration" in policy_type or "Proxy" in p["name"] or "Proxy" in p["description"] or "proxy" in policy_type.lower()
                    proxy_status = "Not Configured"
                    if is_proxy:
                        proxy_status = "Configured"
                        p_srv = p.get("proxyServer") or p.get("proxyAutomaticConfigurationUrl")
                        if p_srv:
                            proxy_status = f"Configured ({p_srv})"
                    p["proxy_status"] = proxy_status
                    
                    rows.append(p)
                        
                if on_page_callback:
                    on_page_callback(value_list)
                url = data.get("@odata.nextLink")
            elif resp.status_code in [401, 403]:
                logger.error("DeviceConfigurations endpoint permission error: %d %s", resp.status_code, resp.text)
                raise PermissionError("DeviceManagementConfiguration.Read.All permission required for Intune Device Configurations.")
            else:
                logger.error("DeviceConfigurations endpoint failed: %d %s", resp.status_code, resp.text)
                raise ConnectionError(f"API request failed with status {resp.status_code}")
                
        if csv_path:
            df = pd.DataFrame(rows) if rows else pd.DataFrame()
            df.to_csv(csv_path, index=False, encoding='utf-8')
    finally:
        client.release_token(token_slot)
        client.close()
