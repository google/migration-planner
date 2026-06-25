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

"""Entra ID Conditional Access policies scanner data pipeline."""

import logging
import csv
from core.graph.client import GraphClient

logger = logging.getLogger(__name__)

def run_authentication_pipeline(
    client_id: str,
    client_secret: str,
    tenant_id: str,
    csv_path: str = None,
    on_page_callback=None,
    is_cancelled_callback=None
):
    """Fetch Conditional Access policies and stream to CSV."""
    client = GraphClient(
        tenant_id=tenant_id,
        client_ids=client_id,
        client_secrets=client_secret,
        concurrency=1,
        retries=2,
        backoff=1
    )
    client.authenticate()
    
    url = "https://graph.microsoft.com/v1.0/identity/conditionalAccess/policies"
    token_slot = client.get_active_token()
    session = client.get_session()
    
    headers = {
        "Authorization": f"Bearer {token_slot['token']}",
        "Accept": "application/json"
    }
    
    f = None
    writer = None
    try:
        logger.info("Querying Entra ID Conditional Access policies...")
        if csv_path:
            f = open(csv_path, 'w', encoding='utf-8', newline='')
            writer = csv.writer(f)
            writer.writerow(["name", "state", "target_users", "target_apps", "controls"])
            
        while url:
            if is_cancelled_callback and is_cancelled_callback(): break
            resp = session.get(url, headers=headers)
            if resp.status_code == 200:
                data = resp.json()
                value_list = data.get("value", [])
                if writer:
                    for p in value_list:
                        name = p.get("displayName", "N/A")
                        state = p.get("state", "N/A")
                        
                        conds = p.get("conditions") or {}
                        users_cond = conds.get("users") or {}
                        apps_cond = conds.get("applications") or {}
                        
                        inc_users = users_cond.get("includeUsers") or []
                        inc_groups = users_cond.get("includeGroups") or []
                        user_target = "All Users" if "All" in inc_users else f"Specific ({len(inc_users)} users, {len(inc_groups)} groups)"
                        
                        inc_apps = apps_cond.get("includeApplications") or []
                        app_target = "All Apps" if "All" in inc_apps else f"Specific ({len(inc_apps)} apps)"
                        
                        grant_controls = p.get("grantControls") or {}
                        controls = grant_controls.get("builtInControls") or []
                        ctrl_str = ", ".join(controls) if controls else "Block/None"
                        
                        writer.writerow([name, state, user_target, app_target, ctrl_str])
                if on_page_callback:
                    on_page_callback(value_list)
                url = data.get("@odata.nextLink")
            elif resp.status_code in [401, 403]:
                logger.error("Conditional Access endpoint permission error: %d %s", resp.status_code, resp.text)
                raise PermissionError("Policy.Read.All or Policy.Read permission required.")
            else:
                logger.error("Conditional Access endpoint failed with status %d: %s", resp.status_code, resp.text)
                raise ConnectionError(f"Microsoft Graph API request failed with status {resp.status_code}")
    finally:
        if f: f.close()
        client.release_token(token_slot)
        client.close()
