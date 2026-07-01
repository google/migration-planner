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

"""Microsoft Intune Mobile Apps telemetry scanner data pipeline."""

import csv
import logging
import time
from core.graph.client import GraphClient

logger = logging.getLogger(__name__)

def run_mobile_apps_pipeline(
    client_id: str,
    client_secret: str,
    tenant_id: str,
    csv_path: str,
    max_rows: int = 5000,
    on_page_callback=None,
    is_cancelled_callback=None
):
    """Fetch mobile apps from Microsoft Graph /deviceAppManagement/mobileApps."""
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
    session = client.get_session()
    
    headers = {
        "Authorization": f"Bearer {token_slot['token']}",
        "Accept": "application/json"
    }
    
    base_url = "https://graph.microsoft.com/beta/deviceAppManagement/mobileApps"
    next_url = f"{base_url}?$select=displayName&$top=100"
    rows_written = 0
    page_number = 1
    
    try:
        logger.info("Fetching Intune mobile apps...")
        with open(csv_path, 'a', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            
            while next_url and rows_written < max_rows:
                if is_cancelled_callback and is_cancelled_callback():
                    logger.info("Fetch cancelled in-flight for Intune mobile apps. Aborting.")
                    break
                    
                retries_left = 2
                resp = None
                while retries_left > 0:
                    try:
                        resp = session.get(next_url, headers=headers, timeout=40.0)
                        if resp.status_code == 200:
                            break
                        elif resp.status_code in [401, 403]:
                            break
                    except Exception as get_err:
                        logger.warning("Intune apps query attempt failed: %s", get_err)
                    
                    retries_left -= 1
                    if retries_left > 0:
                        time.sleep(2)
                        
                page_number += 1
                if resp and resp.status_code == 200:
                    data = resp.json()
                    value_list = data.get("value", [])
                    parsed_records = []
                    
                    for item in value_list:
                        display_name = item.get("displayName", "")
                        writer.writerow([display_name])
                        parsed_records.append({
                            "displayName": display_name
                        })
                        rows_written += 1
                        
                    if on_page_callback:
                        on_page_callback(parsed_records)
                        
                    if rows_written >= max_rows:
                        break
                    next_url = data.get("@odata.nextLink")
                else:
                    if resp and resp.status_code in [401, 403]:
                        logger.error("Intune apps endpoint access denied: %d %s", resp.status_code, resp.text)
                        raise PermissionError("DeviceManagementApps.Read.All permission required.")
                    else:
                        status_str = f"status {resp.status_code}" if resp else "connection/timeout error"
                        logger.warning("Intune apps query failed after 2 attempts (%s). Stopping pagination.", status_str)
                        break
        logger.info("Successfully fetched %d mobile apps", rows_written)
    finally:
        client.release_token(token_slot)
        client.close()
