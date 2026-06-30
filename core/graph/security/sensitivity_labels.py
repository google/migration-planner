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

"""Sensitivity Labels query pipeline."""

import logging
import csv
from core.graph.client import GraphClient

logger = logging.getLogger(__name__)

def run_sensitivity_labels_pipeline(
    client_id: str,
    client_secret: str,
    tenant_id: str,
    csv_path: str = None,
    on_page_callback=None,
    is_cancelled_callback=None
):
    """Fetch sensitivity labels and stream to CSV."""
    client = GraphClient(
        tenant_id=tenant_id,
        client_ids=client_id,
        client_secrets=client_secret,
        concurrency=1,
        retries=3,
        backoff=2
    )
    client.authenticate()
    
    url = "https://graph.microsoft.com/v1.0/security/dataSecurityAndGovernance/sensitivityLabels"
    token_slot = client.get_active_token()
    session = client.get_session()
    
    headers = {
        "Authorization": f"Bearer {token_slot['token']}",
        "Accept": "application/json"
    }
    
    f = None
    writer = None
    try:
        logger.info("Querying Microsoft Graph information protection sensitivity labels...")
        if csv_path:
            f = open(csv_path, 'w', encoding='utf-8', newline='')
            writer = csv.writer(f)
            writer.writerow(["name", "description", "hasProtection", "applicationMode", "priority", "applicableTo", "isEnabled", "is_sublabel"])
        
        while url:
            if is_cancelled_callback and is_cancelled_callback(): break
            resp = session.get(url, headers=headers)
            if resp.status_code == 200:
                data = resp.json()
                value_list = data.get("value", [])
                if writer:
                    for parent in value_list:
                        writer.writerow([
                            parent.get("name", "N/A"),
                            parent.get("description", "") or parent.get("toolTip", "") or "N/A",
                            1 if parent.get("hasProtection", False) else 0,
                            parent.get("applicationMode", "N/A") or "N/A",
                            parent.get("priority", 0),
                            parent.get("applicableTo", ""),
                            1 if parent.get("isEnabled", True) else 0,
                            0
                        ])
                        sublabels = parent.get("sublabels", [])
                        if sublabels:
                            sublabels_sorted = sorted(sublabels, key=lambda x: x.get("priority", 0), reverse=True)
                            for sub in sublabels_sorted:
                                writer.writerow([
                                    f"    ↳  {sub.get('name', 'N/A')}",
                                    sub.get("description", "") or sub.get("toolTip", "") or "N/A",
                                    1 if sub.get("hasProtection", False) else 0,
                                    sub.get("applicationMode", "N/A") or "N/A",
                                    sub.get("priority", 0),
                                    sub.get("applicableTo", ""),
                                    1 if sub.get("isEnabled", True) else 0,
                                    1
                                ])
                if on_page_callback:
                    on_page_callback(value_list)
                url = data.get("@odata.nextLink")
            else:
                logger.error("Graph sensitivityLabels endpoint failed with status %d: %s", resp.status_code, resp.text)
                raise ConnectionError(f"Microsoft Graph API request failed with status {resp.status_code}")
    finally:
        if f: f.close()
        client.release_token(token_slot)
        client.close()
