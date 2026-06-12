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

import csv
import logging
import re
import time
from core.graph.client import GraphClient

logger = logging.getLogger("core.graph.intune")

class IntuneService:
    def __init__(self, client: GraphClient):
        self.client = client

    def fetch_configuration_records(
        self,
        endpoint_name: str,
        csv_path: str,
        max_rows: int = 10000,
        on_page_callback=None,
        is_cancelled_callback=None
    ) -> None:
        """Fetches Intune configuration settings from Microsoft Graph beta endpoints
        and streams them directly to a CSV file.
        
        Args:
            endpoint_name: 'deviceConfigurations' or 'configurationPolicies'
            csv_path: Absolute path to the CSV file where data will be written
            max_rows: Maximum rows to fetch (default 10000)
            on_page_callback: Callback invoked as on_page_callback(parsed_list) for each page
            is_cancelled_callback: Callback returning boolean if cancelled
        """
        token_slot = self.client.get_active_token()
        session = self.client.get_session()
        
        headers = {
            "Authorization": f"Bearer {token_slot['token']}",
            "Accept": "application/json"
        }
        
        base_url = f"https://graph.microsoft.com/beta/deviceManagement/{endpoint_name}"
        next_url = f"{base_url}?$top=100"
        rows_written = 0
        page_number = 1
        
        try:
            logger.info("Fetching Intune configurations from endpoint %s...", endpoint_name)
            with open(csv_path, 'a', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                
                while next_url and rows_written < max_rows:
                    if is_cancelled_callback and is_cancelled_callback():
                        logger.info("Fetch cancelled in-flight for Intune %s. Aborting.", endpoint_name)
                        break
                        
                    retries_left = 2
                    resp = None
                    while retries_left > 0:
                        logger.info("Querying Intune endpoint (Endpoint: %s, Page: %d, Rows so far: %d, Attempt: %d)...", 
                                    endpoint_name, page_number, rows_written, 3 - retries_left)
                        try:
                            resp = session.get(next_url, headers=headers, timeout=40.0)
                            if resp.status_code == 200:
                                break
                            elif resp.status_code in [401, 403]:
                                break
                        except Exception as get_err:
                            logger.warning("Intune query attempt failed: %s", get_err)
                        
                        retries_left -= 1
                        if retries_left > 0:
                            time.sleep(2)
                            
                    page_number += 1
                    if resp and resp.status_code == 200:
                        data = resp.json()
                        value_list = data.get("value", [])
                        parsed_records = []
                        
                        for item in value_list:
                            item_id = item.get("id", "")
                            display_name = item.get("displayName", "")
                            
                            # Parse platform & policyType
                            if endpoint_name == "deviceConfigurations":
                                odata_type = item.get("@odata.type", "")
                                type_str = odata_type.replace("#microsoft.graph.", "")
                                platform = "Unknown"
                                
                                if type_str.startswith("windows10"):
                                    platform = "Windows 10"
                                    policy_type = type_str.replace("windows10", "")
                                elif type_str.startswith("windows"):
                                    platform = "Windows"
                                    policy_type = type_str.replace("windows", "")
                                elif type_str.startswith("ios"):
                                    platform = "iOS"
                                    policy_type = type_str.replace("ios", "")
                                elif type_str.startswith("android"):
                                    platform = "Android"
                                    policy_type = type_str.replace("android", "")
                                elif type_str.startswith("macOS"):
                                    platform = "macOS"
                                    policy_type = type_str.replace("macOS", "")
                                else:
                                    policy_type = type_str
                                    
                                if not policy_type:
                                    policy_type = "Configuration"
                                policy_type = re.sub(r"([A-Z])", r" \1", policy_type).strip()
                            else: # configurationPolicies
                                raw_platform = item.get("platforms", "Unknown")
                                if raw_platform == "windows10AndLater":
                                    platform = "Windows 10"
                                elif raw_platform == "windows81AndLater":
                                    platform = "Windows 8.1"
                                elif raw_platform == "macOS":
                                    platform = "macOS"
                                else:
                                    platform = raw_platform.capitalize()
                                policy_type = "Settings Catalog"
                                
                            writer.writerow([item_id, display_name, platform, policy_type])
                            parsed_records.append({
                                "id": item_id,
                                "displayName": display_name,
                                "platform": platform,
                                "policyType": policy_type
                            })
                            rows_written += 1
                            
                        if on_page_callback:
                            on_page_callback(parsed_records)
                            
                        if rows_written >= max_rows:
                            break
                        next_url = data.get("@odata.nextLink")
                    else:
                        if resp and resp.status_code in [401, 403]:
                            logger.error("Intune endpoint access denied: %d %s", resp.status_code, resp.text)
                            raise PermissionError("DeviceManagementConfiguration.Read.All permission required.")
                        else:
                            status_str = f"status {resp.status_code}" if resp else "connection/timeout error"
                            logger.warning("Intune query failed after 2 attempts (%s). Stopping pagination.", status_str)
                            break
            logger.info("Successfully fetched %d records for Intune %s", rows_written, endpoint_name)
        finally:
            self.client.release_token(token_slot)
