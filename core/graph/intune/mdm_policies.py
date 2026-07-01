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

"""Microsoft Intune Mobile Device Management (MDM) Policies telemetry scanner data pipeline."""

import logging
import requests
import pandas as pd
from core.graph.client import GraphClient

logger = logging.getLogger(__name__)

def run_mdm_policies_pipeline(
    client_id: str,
    client_secret: str,
    tenant_id: str,
    csv_path: str = None,
    delegated_token: str = None,
    max_rows: int = 1000,
    is_cancelled_callback=None
) -> list:
    """Fetch Mobile Device Management Policies from beta/policies/mobileDeviceManagementPolicies."""
    client = None
    token = None
    session = None
    
    if delegated_token:
        token = delegated_token
        session = requests.Session()
    else:
        client = GraphClient(
            tenant_id=tenant_id,
            client_ids=client_id,
            client_secrets=client_secret,
            concurrency=1,
            retries=5,
            backoff=2
        )
        client.authenticate()
        token_slot = client.get_active_token()
        token = token_slot['token']
        session = client.get_session()
        
    url = "https://graph.microsoft.com/beta/policies/mobileDeviceManagementPolicies?$filter=isValid eq true"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }
    
    rows = []
    try:
        logger.info("Querying Intune Mobile Device Management Policies...")
        while url and len(rows) < max_rows:
            if is_cancelled_callback and is_cancelled_callback():
                logger.info("MDM policies scan cancelled. Aborting.")
                break
            resp = session.get(url, headers=headers)
            if resp.status_code == 200:
                data = resp.json()
                value_list = data.get("value", [])
                
                for item in value_list:
                    if len(rows) >= max_rows:
                        break
                    # Map appliesTo to a readable Auto-Enroll value (e.g. 'all', 'selected', 'none')
                    applies_to = item.get("appliesTo")
                    if isinstance(applies_to, str):
                        applies_to_str = applies_to.capitalize()
                    else:
                        applies_to_str = "None"
                        
                    rows.append({
                        "displayName": item.get("displayName") or "N/A",
                        "description": item.get("description") or "N/A",
                        "appliesTo": applies_to_str,
                        "discoveryUrl": item.get("discoveryUrl") or "N/A",
                        "termsOfUseUrl": item.get("termsOfUseUrl") or "N/A",
                        "complianceUrl": item.get("complianceUrl") or "N/A"
                    })
                    
                url = data.get("@odata.nextLink")
            elif resp.status_code in [401, 403]:
                logger.error("MDM policies access denied: %d %s", resp.status_code, resp.text)
                raise PermissionError("Policy.Read.All or Policy.ReadWrite.ConditionalAccess permission required.")
            else:
                logger.error("MDM policies endpoint failed: %d %s", resp.status_code, resp.text)
                raise ConnectionError(f"API request failed with status {resp.status_code}")
       
        if csv_path and rows:
            df = pd.DataFrame(rows)
            df.to_csv(csv_path, index=False, encoding='utf-8')
        return rows
    finally:
        if client:
            client.release_token(token_slot)
            client.close()
        elif session:
            session.close()
