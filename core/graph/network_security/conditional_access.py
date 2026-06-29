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

"""Microsoft Entra ID Conditional Access policies for Network Security data pipeline."""

import logging
import pandas as pd
from core.graph.client import GraphClient

logger = logging.getLogger(__name__)

def run_conditional_access_pipeline(
    client_id: str,
    client_secret: str,
    tenant_id: str,
    csv_path: str = None,
    on_page_callback=None,
    is_cancelled_callback=None
):
    """Fetch Conditional Access policies."""
    client = GraphClient(
        tenant_id=tenant_id,
        client_ids=client_id,
        client_secrets=client_secret,
        concurrency=1,
        retries=5,
        backoff=2
    )
    client.authenticate()
    
    url = "https://graph.microsoft.com/v1.0/identity/conditionalAccess/policies"
    token_slot = client.get_active_token()
    session = client.get_session()
    
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
                data = resp.json() or {}
                value_list = data.get("value", [])
                
                for p in value_list:
                    if not p:
                        continue
                    p["name"] = p.get("displayName") or p.get("name") or "N/A"
                    p["state"] = p.get("state") or "N/A"
                    
                    conds = p.get("conditions") or {}
                    users_cond = conds.get("users") or {}
                    apps_cond = conds.get("applications") or {}
                    
                    inc_users = users_cond.get("includeUsers") or []
                    inc_groups = users_cond.get("includeGroups") or []
                    p["target_users"] = "All Users" if "All" in inc_users else f"Specific ({len(inc_users)} users, {len(inc_groups)} groups)"
                    
                    inc_apps = apps_cond.get("includeApplications") or []
                    p["target_apps"] = "All Apps" if "All" in inc_apps else f"Specific ({len(inc_apps)} apps)"
                    
                    grant_controls = p.get("grantControls") or {}
                    controls = grant_controls.get("builtInControls") or []
                    p["controls"] = ", ".join(controls) if controls else "Block/None"
                    rows.append(p)
                    
                if on_page_callback:
                    on_page_callback(value_list)
                url = data.get("@odata.nextLink")
            elif resp.status_code in [401, 403]:
                logger.error("Conditional Access endpoint permission error: %d %s", resp.status_code, resp.text)
                raise PermissionError("Policy.Read.All permission required for Conditional Access policies.")
            else:
                logger.error("Conditional Access endpoint failed: %d %s", resp.status_code, resp.text)
                raise ConnectionError(f"API request failed with status {resp.status_code}")
                
        if csv_path:
            df = pd.DataFrame(rows) if rows else pd.DataFrame()
            df.to_csv(csv_path, index=False, encoding='utf-8')
    finally:
        client.release_token(token_slot)
        client.close()
