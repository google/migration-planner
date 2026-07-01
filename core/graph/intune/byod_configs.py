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

"""Microsoft Intune Mobile BYOD Configurations data pipeline."""

import logging
import pandas as pd
from core.graph.client import GraphClient

logger = logging.getLogger(__name__)

def format_restriction(restriction: dict) -> str:
    """Helper to format a restriction dict into a single line readable string."""
    if not restriction or not isinstance(restriction, dict):
        return "N/A"
    blocked = "Yes" if restriction.get("platformBlocked") else "No"
    personal_blocked = "Yes" if restriction.get("personalDeviceEnrollmentBlocked") else "No"
    min_ver = restriction.get("osMinimumVersion") or "None"
    max_ver = restriction.get("osMaximumVersion") or "None"
    return f"Blocked: {blocked}, Personal Blocked: {personal_blocked}, Min OS: {min_ver}, Max OS: {max_ver}"

def run_byod_configs_pipeline(
    client_id: str,
    client_secret: str,
    tenant_id: str,
    csv_path: str = None,
    max_rows: int = 1000,
    is_cancelled_callback=None
) -> list:
    """Fetch Mobile BYOD Configurations from deviceManagement/deviceEnrollmentConfigurations."""
    client = GraphClient(
        tenant_id=tenant_id,
        client_ids=client_id,
        client_secrets=client_secret,
        concurrency=1,
        retries=5,
        backoff=2
    )
    client.authenticate()
    
    url = "https://graph.microsoft.com/v1.0/deviceManagement/deviceEnrollmentConfigurations"
    token_slot = client.get_active_token()
    session = client.get_session()
    
    headers = {
        "Authorization": f"Bearer {token_slot['token']}",
        "Accept": "application/json"
    }
    
    rows = []
    try:
        logger.info("Querying Intune Device Enrollment Configurations for Mobile BYOD...")
        while url and len(rows) < max_rows:
            if is_cancelled_callback and is_cancelled_callback():
                logger.info("Mobile BYOD configurations scan cancelled. Aborting.")
                break
            resp = session.get(url, headers=headers)
            if resp.status_code == 200:
                data = resp.json()
                value_list = data.get("value", [])
                
                for item in value_list:
                    if len(rows) >= max_rows:
                        break
                    
                    # Filter for platform restrictions configuration type
                    odata_type = item.get("@odata.type")
                    if odata_type != "#microsoft.graph.deviceEnrollmentPlatformRestrictionsConfiguration":
                        continue
                        
                    ios_rest = format_restriction(item.get("iosRestriction"))
                    win_mob_rest = format_restriction(item.get("windowsMobileRestriction"))
                    android_rest = format_restriction(item.get("androidRestriction"))
                    
                    rows.append({
                        "displayName": item.get("displayName") or "N/A",
                        "description": item.get("description") or "N/A",
                        "priority": item.get("priority", 0),
                        "lastModifiedDateTime": item.get("lastModifiedDateTime") or "N/A",
                        "iosRestrictions": ios_rest,
                        "windowsMobileRestrictions": win_mob_rest,
                        "androidRestrictions": android_rest
                    })
                    
                url = data.get("@odata.nextLink")
            elif resp.status_code in [401, 403]:
                logger.error("BYOD configs access denied: %d %s", resp.status_code, resp.text)
                raise PermissionError("DeviceManagementServiceConfig.Read.All permission required.")
            else:
                logger.error("BYOD configs endpoint failed: %d %s", resp.status_code, resp.text)
                raise ConnectionError(f"API request failed with status {resp.status_code}")
       
        if csv_path and rows:
            df = pd.DataFrame(rows)
            df.to_csv(csv_path, index=False, encoding='utf-8')
        return rows
    finally:
        client.release_token(token_slot)
        client.close()
