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

"""Service for querying Microsoft Graph Global Secure Access filtering, Conditional Access, and firewall configurations."""

import logging
import pandas as pd
from typing import Dict, Any, List
from core.graph.client import GraphClient

logger = logging.getLogger(__name__)

class NetworkSecurityService:
    """Service to interact with Network Security configurations."""

    def __init__(self, client: GraphClient) -> None:
        self.client = client

    def fetch_filtering_policies(self, csv_path: str = None, on_page_callback=None, is_cancelled_callback=None) -> None:
        """Fetches Entra Global Secure Access Filtering Policies (Beta) and saves full details via Pandas."""
        url = "https://graph.microsoft.com/beta/networkAccess/filteringPolicies"
        token_slot = self.client.get_active_token()
        session = self.client.get_session()
        
        headers = {
            "Authorization": f"Bearer {token_slot['token']}",
            "Accept": "application/json"
        }
        
        rows = []
        try:
            logger.info("Querying Entra Global Secure Access filtering policies...")
            while url:
                if is_cancelled_callback and is_cancelled_callback(): break
                resp = session.get(url, headers=headers)
                if resp.status_code == 200:
                    data = resp.json()
                    value_list = data.get("value", [])
                    
                    for p in value_list:
                        # Append display helpers to the raw object to keep full details
                        p["name"] = p.get("name") or p.get("displayName") or "N/A"
                        p["description"] = p.get("description") or "N/A"
                        p["version"] = p.get("version") or "N/A"
                        p["action"] = p.get("action") or "N/A"
                        
                        rules = p.get("policyRules", [])
                        p["rules_count"] = len(rules) if isinstance(rules, list) else 0
                        rows.append(p)
                            
                    if on_page_callback:
                        on_page_callback(value_list)
                    url = data.get("@odata.nextLink")
                elif resp.status_code in [401, 403]:
                    logger.error("Filtering policies endpoint permission error: %d %s", resp.status_code, resp.text)
                    raise PermissionError("NetworkAccess.Read.All permission required for beta network Access GSA.")
                else:
                    logger.error("Filtering policies endpoint failed: %d %s", resp.status_code, resp.text)
                    raise ConnectionError(f"API request failed with status {resp.status_code}")
                    
            if csv_path:
                df = pd.DataFrame(rows) if rows else pd.DataFrame()
                df.to_csv(csv_path, index=False, encoding='utf-8')
        finally:
            self.client.release_token(token_slot)

    def fetch_conditional_access_policies(self, csv_path: str = None, on_page_callback=None, is_cancelled_callback=None) -> None:
        """Fetches the Entra ID Conditional Access policies and saves full details via Pandas."""
        url = "https://graph.microsoft.com/v1.0/identity/conditionalAccess/policies"
        token_slot = self.client.get_active_token()
        session = self.client.get_session()
        
        headers = {
            "Authorization": f"Bearer {token_slot['token']}",
            "Accept": "application/json"
        }
        
        rows = []
        try:
            logger.info("Querying Conditional Access policies for Network Security...")
            while url:
                if is_cancelled_callback and is_cancelled_callback(): break
                resp = session.get(url, headers=headers)
                if resp.status_code == 200:
                    data = resp.json()
                    value_list = data.get("value", [])
                    
                    for p in value_list:
                        p["name"] = p.get("displayName") or p.get("name") or "N/A"
                        p["state"] = p.get("state") or "N/A"
                        
                        conds = p.get("conditions", {})
                        users_cond = conds.get("users", {})
                        apps_cond = conds.get("applications", {})
                        
                        inc_users = users_cond.get("includeUsers", [])
                        inc_groups = users_cond.get("includeGroups", [])
                        p["target_users"] = "All Users" if "All" in inc_users else f"Specific ({len(inc_users)} users, {len(inc_groups)} groups)"
                        
                        inc_apps = apps_cond.get("includeApplications", [])
                        p["target_apps"] = "All Apps" if "All" in inc_apps else f"Specific ({len(inc_apps)} apps)"
                        
                        controls = p.get("grantControls", {}).get("builtInControls", [])
                        p["controls"] = ", ".join(controls) if controls else "Block/None"
                        rows.append(p)
                        
                    if on_page_callback:
                        on_page_callback(value_list)
                    url = data.get("@odata.nextLink")
                else:
                    logger.error("Conditional Access endpoint failed: %d %s", resp.status_code, resp.text)
                    raise ConnectionError(f"API request failed with status {resp.status_code}")
                    
            if csv_path:
                df = pd.DataFrame(rows) if rows else pd.DataFrame()
                df.to_csv(csv_path, index=False, encoding='utf-8')
        finally:
            self.client.release_token(token_slot)

    def fetch_firewall_policies(self, csv_path: str = None, on_page_callback=None, is_cancelled_callback=None) -> None:
        """Fetches device configurations, filters Firewall & Proxy configs, and saves full details via Pandas."""
        url = "https://graph.microsoft.com/v1.0/deviceManagement/deviceConfigurations"
        token_slot = self.client.get_active_token()
        session = self.client.get_session()
        
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
                else:
                    logger.error("DeviceConfigurations endpoint failed: %d %s", resp.status_code, resp.text)
                    raise ConnectionError(f"API request failed with status {resp.status_code}")
                    
            if csv_path:
                df = pd.DataFrame(rows) if rows else pd.DataFrame()
                df.to_csv(csv_path, index=False, encoding='utf-8')
        finally:
            self.client.release_token(token_slot)
