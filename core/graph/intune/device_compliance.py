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

"""Microsoft Intune Device Compliance Policies telemetry scanner data pipeline."""

import logging
import pandas as pd
from core.graph.client import GraphClient

logger = logging.getLogger(__name__)

def run_device_compliance_pipeline(
    client_id: str,
    client_secret: str,
    tenant_id: str,
    csv_path: str = None,
    filter_type: str = None,
    max_rows: int = 10000,
    is_cancelled_callback=None
) -> list:
    """Fetch device compliance policies from Microsoft Graph /deviceManagement/deviceCompliancePolicies."""
    client = GraphClient(
        tenant_id=tenant_id,
        client_ids=client_id,
        client_secrets=client_secret,
        concurrency=1,
        retries=5,
        backoff=2
    )
    client.authenticate()
    
    if filter_type:
        url = f"https://graph.microsoft.com/v1.0/deviceManagement/deviceCompliancePolicies?$filter=isof('{filter_type}')"
    else:
        url = "https://graph.microsoft.com/v1.0/deviceManagement/deviceCompliancePolicies"
        
    token_slot = client.get_active_token()
    session = client.get_session()
    
    headers = {
        "Authorization": f"Bearer {token_slot['token']}",
        "Accept": "application/json"
    }
    
    rows = []
    try:
        logger.info(f"Querying Intune Device Compliance Policies (filter: {filter_type or 'None'})...")
        while url and len(rows) < max_rows:
            if is_cancelled_callback and is_cancelled_callback():
                logger.info("Device compliance scan cancelled. Aborting.")
                break
            resp = session.get(url, headers=headers)
            if resp.status_code == 200:
                data = resp.json()
                value_list = data.get("value", [])
                
                for item in value_list:
                    if len(rows) >= max_rows:
                        break
                    rows.append(item)
                    
                url = data.get("@odata.nextLink")
            elif resp.status_code in [401, 403]:
                logger.error("Device compliance access denied: %d %s", resp.status_code, resp.text)
                raise PermissionError("DeviceManagementConfiguration.Read.All permission required for compliance policies.")
            else:
                logger.error("Device compliance endpoint failed: %d %s", resp.status_code, resp.text)
                raise ConnectionError(f"API request failed with status {resp.status_code}")
       
        if csv_path and rows:
            df = pd.DataFrame(rows)
            df.to_csv(csv_path, index=False, encoding='utf-8')
        return rows
    finally:
        client.release_token(token_slot)
        client.close()
