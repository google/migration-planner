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

"""Service Principals SSO configuration settings scanner data pipeline."""

import logging
import csv
from core.graph.client import GraphClient

logger = logging.getLogger(__name__)

def run_service_principals_sso_pipeline(
    client_id: str,
    client_secret: str,
    tenant_id: str,
    csv_path: str = None,
    on_page_callback=None,
    is_cancelled_callback=None
):
    """Fetch Enterprise SSO service principal configurations."""
    client = GraphClient(
        tenant_id=tenant_id,
        client_ids=client_id,
        client_secrets=client_secret,
        concurrency=1,
        retries=3,
        backoff=2
    )
    client.authenticate()
    
    url = "https://graph.microsoft.com/v1.0/servicePrincipals?$select=id,appDisplayName,preferredSingleSignOnMode&$top=100"
    token_slot = client.get_active_token()
    session = client.get_session()
    
    headers = {
        "Authorization": f"Bearer {token_slot['token']}",
        "Accept": "application/json"
    }
    
    f = None
    writer = None
    try:
        logger.info("Querying Entra ID Service Principals for Single Sign-On modes...")
        if csv_path:
            f = open(csv_path, 'a', encoding='utf-8', newline='')
            writer = csv.writer(f)
            
        while url:
            if is_cancelled_callback and is_cancelled_callback(): break
            resp = session.get(url, headers=headers)
            if resp.status_code == 200:
                data = resp.json()
                value_list = data.get("value", [])
                if writer:
                    for sp in value_list:
                        writer.writerow([sp.get("appDisplayName", ""), sp.get("preferredSingleSignOnMode", "")])
                if on_page_callback:
                    on_page_callback(value_list)
                url = data.get("@odata.nextLink")
            elif resp.status_code in [401, 403]:
                logger.error("Service Principals endpoint permission error: %d %s", resp.status_code, resp.text)
                raise PermissionError("Application.Read.All permission required.")
            else:
                logger.error("Service Principals endpoint failed with status %d: %s", resp.status_code, resp.text)
                raise ConnectionError(f"Microsoft Graph API request failed with status {resp.status_code}")
    finally:
        if f: f.close()
        client.release_token(token_slot)
        client.close()
