import requests
import json
import logging
import os
import time
from datetime import datetime

class PowerAutomateScanner:
    def __init__(self, tenant_id, client_id, client_secret):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        
        # Setup logging directory and file inside telemetry/logs
        self.log_dir = os.path.join("telemetry", "logs")
        if not os.path.exists(self.log_dir):
            os.makedirs(self.log_dir)
            
        self.log_file = os.path.join(self.log_dir, "power_automate_log.txt")
        self._setup_logger()
        self.access_token = None

    def _setup_logger(self):
        """Configures logging to propagate to the central LicenseUsageAsyncLogger."""
        self.logger = logging.getLogger("LicenseUsageAsyncLogger.PowerAutomateScanner")

    def _get_access_token(self, scope):
        """Fetches OAuth 2.0 token for a given scope."""
        self.logger.info(f"Step Start: Fetching access token for scope {scope} from Microsoft Identity Platform.")
        url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        payload = {
            'grant_type': 'client_credentials',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'scope': scope
        }
        
        try:
            response = requests.post(url, headers=headers, data=payload)
            response.raise_for_status()
            token = response.json().get('access_token')
            self.logger.info(f"Step End: Successfully retrieved access token for {scope}.")
            return token
        except Exception as e:
            self.logger.error(f"Step Error: Failed to fetch access token for {scope}. Error: {str(e)}")
            return None

    def fetch_all_pages(self, url, headers, context_name="API"):
        """Helper to cleanly handle Power Platform API pagination & Throttling limits."""
        results = []
        while url:
            res = requests.get(url, headers=headers)
            
            if res.status_code == 429:
                retry_after = int(res.headers.get("Retry-After", 2))
                self.logger.warning(f"[!] Rate limited on {context_name}. Waiting {retry_after} seconds...")
                time.sleep(retry_after)
                continue
                
            if res.status_code == 200:
                data = res.json()
                results.extend(data.get("value", []))
                url = data.get("nextLink") or data.get("@odata.nextLink")
            else:
                self.logger.error(f"[X] {context_name} Request Failed | HTTP {res.status_code}: {res.text}")
                break
                
        return results

    def fetch_single_resource(self, url, headers, context_name="API"):
        """Helper to cleanly fetch a single resource handling Throttling limits for loop logic."""
        while True:
            res = requests.get(url, headers=headers)
            if res.status_code == 429:
                retry_after = int(res.headers.get("Retry-After", 2))
                self.logger.warning(f"[!] Rate limited on {context_name}. Waiting {retry_after} seconds...")
                time.sleep(retry_after)
                continue
            if res.status_code == 200:
                return res.json()
            self.logger.error(f"[X] {context_name} Request Failed | HTTP {res.status_code}: {res.text}")
            return None

    def scan_flows(self):
        """Scans Power Automate flows across all environments in the tenant."""
        self.logger.info("Main Process Start: Initiating Power Automate Flow Scan.")
        
        try:
            bap_token = self._get_access_token("https://api.bap.microsoft.com/.default")
            flow_token = self._get_access_token("https://service.flow.microsoft.com/.default")
        except Exception as e:
            self.logger.error(f"Auth Error: {e}")
            return None

        if not bap_token or not flow_token:
            self.logger.error("Main Process Failure: Aborting scan due to missing access tokens.")
            return None

        bap_headers = {"Authorization": f"Bearer {bap_token}", "Accept": "application/json"}
        flow_headers = {"Authorization": f"Bearer {flow_token}", "Accept": "application/json"}

        self.logger.info("Step Start: Fetching all environments in the tenant.")
        env_api_url = "https://api.bap.microsoft.com/providers/Microsoft.BusinessAppPlatform/scopes/admin/environments?api-version=2023-06-01"
        environments = self.fetch_all_pages(env_api_url, bap_headers, context_name="Environment Discovery")
        
        if not environments:
            self.logger.error("[X] No environments found or failed to fetch.")
            return None

        self.logger.info(f"[+] Successfully retrieved {len(environments)} environments.")

        counts = {"Cloud Flows": 0, "Desktop Flows": 0}
        active_counts = {"Cloud Flows": 0, "Desktop Flows": 0}
        tier_counts = {"Personal Productivity": 0, "Enterprise/Departmental": 0}
        active_tier_counts = {"Personal Productivity": 0, "Enterprise/Departmental": 0}
        premium_connectors_found = set()
        custom_connectors_found = set()
        complex_logic_flows = []
        
        PREMIUM_KEYWORDS = ['shared_sql', 'shared_httpaction', 'shared_salesforce', 'shared_oracle', 'shared_sap']

        for env in environments:
            env_name = env.get("name")
            env_props = env.get("properties", {})
            env_display = env_props.get("displayName", env_name)
            is_default = env_props.get("isDefault", False)
            
            self.logger.info(f"[*] Scanning Environment: {env_display}")
            
            # ==========================================
            # 1. FETCH CLOUD FLOWS
            # ==========================================
            flows_url = f"https://api.flow.microsoft.com/providers/Microsoft.ProcessSimple/scopes/admin/environments/{env_name}/v2/flows?api-version=2016-11-01"
            cloud_flows = self.fetch_all_pages(flows_url, flow_headers, context_name="Cloud Flows Admin API (V2)")
            
            counts["Cloud Flows"] += len(cloud_flows)
            active_cloud_flows = [f for f in cloud_flows if f.get("properties", {}).get("state") == "Started"]
            active_counts["Cloud Flows"] += len(active_cloud_flows)
            
            self.logger.info(f"    -> Cloud Flows: {len(cloud_flows)} total found, {len(active_cloud_flows)} are currently active.")
            
            for flow_summary in cloud_flows:
                state = flow_summary.get("properties", {}).get("state")
                is_active = (state == "Started")
                
                flow_id = flow_summary.get("name")
                
                detail_url = f"https://api.flow.microsoft.com/providers/Microsoft.ProcessSimple/scopes/admin/environments/{env_name}/flows/{flow_id}?api-version=2016-11-01"
                flow_detail = self.fetch_single_resource(detail_url, flow_headers, context_name=f"Get Flow Details ({flow_id})")
                
                if not flow_detail:
                    continue

                props = flow_detail.get("properties", {})
                name = props.get("displayName", "Unnamed Flow")
                
                is_managed = "workflowEntityId" in props or not is_default
                if is_managed:
                    tier_counts["Enterprise/Departmental"] += 1
                    tier = "Enterprise"
                    if is_active:
                        active_tier_counts["Enterprise/Departmental"] += 1
                else:
                    tier_counts["Personal Productivity"] += 1
                    tier = "Personal"
                    if is_active:
                        active_tier_counts["Personal Productivity"] += 1

                conn_refs = props.get("connectionReferences", {})
                for conn_key, conn_val in conn_refs.items():
                    api_obj = conn_val.get("api", {})
                    api_id = api_obj.get("id", "")
                    conn_name = api_id.split("/")[-1] if "/" in api_id else api_id
                    
                    if api_obj.get("tier") == "Premium" or any(kw in conn_name.lower() for kw in PREMIUM_KEYWORDS):
                        premium_connectors_found.add(conn_name)
                        self.logger.info(f"      [!] Premium connector found: {conn_name} in flow {name}")
                    if "custom" in api_id.lower() or api_obj.get("type") == "Microsoft.PowerApps/apis/custom":
                        custom_connectors_found.add(conn_name)
                        self.logger.info(f"      [!] Custom connector found: {conn_name} in flow {name}")

                actions_str = json.dumps(props)
                has_nested_loops = actions_str.count('"type": "Foreach"') > 0 or actions_str.count('"type": "Until"') > 0
                has_multi_approvals = "shared_approvals" in actions_str or "Approval" in actions_str
                has_advanced_expressions = "@" in actions_str and any(exp in actions_str for exp in ["concat(", "split(", "base64("])

                if has_nested_loops or has_multi_approvals or has_advanced_expressions:
                    self.logger.info(f"      [!] Complex logic detected in Cloud Flow: {name}")
                    reasons = []
                    if has_nested_loops: reasons.append("Nested Loops")
                    if has_multi_approvals: reasons.append("Multi Approvals")
                    if has_advanced_expressions: reasons.append("Advanced Expressions")
                    
                    complex_logic_flows.append({
                        "Environment": env_display, "Name": name, "Type": "Cloud Flow", "Tier": tier,
                        "Active": "Yes" if is_active else "No",
                        "Reason": ", ".join(reasons)
                    })

            # ==========================================
            # 2. FETCH DESKTOP FLOWS
            # ==========================================
            instance_url = env_props.get("linkedEnvironmentMetadata", {}).get("instanceApiUrl")
            
            if instance_url:
                dv_url = instance_url.rstrip("/")
                try:
                    dv_token = self._get_access_token(f"{dv_url}/.default")
                    if not dv_token:
                         self.logger.warning(f"      [X] Failed to acquire token for Dataverse instance {dv_url}")
                         continue
                         
                    headers_dv = {
                        "Authorization": f"Bearer {dv_token}",
                        "Accept": "application/json",
                        "OData-MaxVersion": "4.0", "OData-Version": "4.0"
                    }
                    
                    dv_api_url = f"{dv_url}/api/data/v9.2/workflows?$filter=category eq 6&$select=name,clientdata,ismanaged,statecode,_ownerid_value&$expand=ownerid"
                    desktop_flows = self.fetch_all_pages(dv_api_url, headers_dv, context_name="Dataverse Desktop Flows API")
                    
                    counts["Desktop Flows"] += len(desktop_flows)
                    active_desktop_flows = [f for f in desktop_flows if f.get("statecode") == 1]
                    active_counts["Desktop Flows"] += len(active_desktop_flows)
                    
                    self.logger.info(f"    -> Desktop Flows: {len(desktop_flows)} total found, {len(active_desktop_flows)} are active.")
                    
                    for flow in desktop_flows:
                        name = flow.get("name", "Unnamed Desktop Flow")
                        is_managed = flow.get("ismanaged", False)
                        owner_name = flow.get("ownerid", {}).get("fullname", "Unknown / System")
                        statecode = flow.get("statecode")
                        is_active = (statecode == 1)
                        
                        if is_managed or "system" in owner_name.lower() or not is_default:
                            tier_counts["Enterprise/Departmental"] += 1
                            tier = "Enterprise"
                            if is_active:
                                active_tier_counts["Enterprise/Departmental"] += 1
                        else:
                            tier_counts["Personal Productivity"] += 1
                            tier = "Personal"
                            if is_active:
                                active_tier_counts["Personal Productivity"] += 1
                            
                        client_data_str = flow.get("clientdata", "")
                        if client_data_str:
                            try:
                                has_nested_loops = client_data_str.lower().count("foreach") > 1
                                has_multi_approvals = client_data_str.count("shared_approvals") > 1
                                has_advanced_expressions = "@" in client_data_str and any(exp in client_data_str for exp in ["concat(", "split(", "base64("])

                                if has_nested_loops or has_multi_approvals or has_advanced_expressions:
                                    self.logger.info(f"      [!] Complex logic detected in Desktop Flow: {name}")
                                    reasons = []
                                    if has_nested_loops: reasons.append("Nested Loops")
                                    if has_multi_approvals: reasons.append("Multi Approvals")
                                    if has_advanced_expressions: reasons.append("Advanced Expressions")
                                    
                                    complex_logic_flows.append({
                                        "Environment": env_display, "Name": name, "Type": "Desktop Flow", "Tier": tier,
                                        "Active": "Yes" if is_active else "No",
                                        "Reason": ", ".join(reasons)
                                    })
                            except Exception:
                                pass
                except Exception as e:
                    self.logger.error(f"      [X] Failed to authenticate against Dataverse instance {dv_url}: {e}")

        results = {
            "total_environments": len(environments),
            "counts": counts,
            "active_counts": active_counts,
            "tier_counts": tier_counts,
            "active_tier_counts": active_tier_counts,
            "premium_connectors": list(premium_connectors_found),
            "custom_connectors": list(custom_connectors_found),
            "complex_logic_flows": complex_logic_flows
        }

        self.logger.info("Step End: Analysis complete.")
        self.logger.info("Main Process End: Power Automate Telemetry scan finished.")
        return results
