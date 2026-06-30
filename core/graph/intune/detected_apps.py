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

"""Microsoft Intune Detected Apps telemetry scanner data pipeline."""

import logging
import pandas as pd
from core.graph.client import GraphClient

logger = logging.getLogger(__name__)

def run_detected_apps_pipeline(
    client_id: str,
    client_secret: str,
    tenant_id: str,
    csv_path: str = None,
    max_rows: int = 10000,
    is_cancelled_callback=None
) -> list:
    """Fetch detected apps from Microsoft Graph /deviceManagement/detectedApps."""
    client = GraphClient(
        tenant_id=tenant_id,
        client_ids=client_id,
        client_secrets=client_secret,
        concurrency=1,
        retries=5,
        backoff=2
    )
    client.authenticate()
    
    url = "https://graph.microsoft.com/v1.0/deviceManagement/detectedApps"
    token_slot = client.get_active_token()
    session = client.get_session()
    
    headers = {
        "Authorization": f"Bearer {token_slot['token']}",
        "Accept": "application/json"
    }
    
    rows = []
    try:
        logger.info("Querying Intune Detected Apps...")
        while url and len(rows) < max_rows:
            if is_cancelled_callback and is_cancelled_callback(): break
            resp = session.get(url, headers=headers)
            if resp.status_code == 200:
                data = resp.json()
                value_list = data.get("value", [])
                
                for item in value_list:
                    if len(rows) >= max_rows:
                        break
                    rows.append({
                        "displayName": item.get("displayName") or "N/A",
                        "version": item.get("version") or "N/A",
                        "publisher": item.get("publisher") or "N/A",
                        "platform": item.get("platform") or "unknown",
                        "deviceCount": item.get("deviceCount") or 0
                    })
                    
                url = data.get("@odata.nextLink")
            else:
                logger.error("Detected apps endpoint failed: %d %s", resp.status_code, resp.text)
                raise ConnectionError(f"API request failed with status {resp.status_code}")
       
        if csv_path and rows:
            df = pd.DataFrame(rows)
            df.to_csv(csv_path, index=False, encoding='utf-8')
        return rows
    finally:
        client.release_token(token_slot)
        client.close()
