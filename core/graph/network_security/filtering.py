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

"""Microsoft Entra Global Secure Access filtering policies data pipeline."""

import logging
import pandas as pd
from core.graph.client import GraphClient

logger = logging.getLogger(__name__)

def run_filtering_pipeline(
    client_id: str,
    client_secret: str,
    tenant_id: str,
    csv_path: str = None,
    on_page_callback=None,
    is_cancelled_callback=None
):
    """Fetch Entra Global Secure Access Filtering Policies (Beta)."""
    client = GraphClient(
        tenant_id=tenant_id,
        client_ids=client_id,
        client_secrets=client_secret,
        concurrency=1,
        retries=5,
        backoff=2
    )
    client.authenticate()
    
    url = "https://graph.microsoft.com/beta/networkAccess/filteringPolicies"
    token_slot = client.get_active_token()
    session = client.get_session()
    
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
        client.release_token(token_slot)
        client.close()
