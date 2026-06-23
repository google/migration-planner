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

"""Service for querying Entra ID user creation/deletion logs."""

import logging
import csv
import json
from core.graph.client import GraphClient

logger = logging.getLogger(__name__)

class UserLogsService:
    def __init__(self, client: GraphClient) -> None:
        self.client = client

    def fetch_user_creation_logs(self, csv_path: str, max_rows: int = 50, on_page_callback=None, is_cancelled_callback=None) -> None:
        """Queries Microsoft Graph API /auditLogs/directoryAudits to fetch successful Add user and Delete user logs,
        flattens, and appends/saves to CSV.
        """
        token_slot = self.client.get_active_token()
        session = self.client.get_session()
        
        headers = {
            "Authorization": f"Bearer {token_slot['token']}",
            "Accept": "application/json"
        }
        
        urls = [
            ("Add user", "https://graph.microsoft.com/v1.0/auditLogs/directoryAudits?$select=activityDisplayName,initiatedBy&$filter=activityDisplayName eq 'Add user' and result eq 'success'&$top=50"),
            ("Delete user", "https://graph.microsoft.com/v1.0/auditLogs/directoryAudits?$select=activityDisplayName,initiatedBy&$filter=activityDisplayName eq 'Delete user' and result eq 'success'&$top=50")
        ]
        
        rows_written = 0
        try:
            logger.info("Starting User Creation logs fetch...")
            # Initialize/overwrite CSV
            with open(csv_path, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["Activity", "Initiated By"])
                
            with open(csv_path, 'a', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                
                try:
                    for activity_type, url in urls:
                        if is_cancelled_callback and is_cancelled_callback():
                            logger.info("User Creation logs fetch cancelled in-flight.")
                            break
                            
                        next_url = url
                        activity_rows_written = 0
                        
                        while next_url and activity_rows_written < max_rows:
                            if is_cancelled_callback and is_cancelled_callback():
                                break
                                
                            logger.info("Querying MSFT Graph directory audits for '%s'...", activity_type)
                            try:
                                resp = session.get(next_url, headers=headers, timeout=60.0)
                            except Exception as get_err:
                                logger.warning("Query attempt for '%s' failed with exception: %s. Displaying data obtained till now.", activity_type, get_err)
                                break
                                
                            if not resp or resp.status_code != 200:
                                if resp and resp.status_code in [401, 403]:
                                    logger.error("Directory audits endpoint permission error: %d %s", resp.status_code, resp.text)
                                    raise PermissionError("AuditLog.Read.All permission required. Please ensure this permission is granted to the application registration in Microsoft Entra ID.")
                                else:
                                    status_str = f"status {resp.status_code}" if resp else "connection/timeout error"
                                    logger.warning("Directory audits query for '%s' failed (%s). Displaying data obtained till now.", activity_type, status_str)
                                    break
                                    
                            data = resp.json()
                            value_list = data.get("value", [])
                            
                            page_rows = []
                            for log in value_list:
                                activity = log.get("activityDisplayName") or ""
                                initiated_by_obj = log.get("initiatedBy") or {}
                                
                                initiated_by_str = json.dumps(initiated_by_obj)
                                    
                                writer.writerow([activity, initiated_by_str])
                                activity_rows_written += 1
                                rows_written += 1
                                
                                page_rows.append({
                                    "activity": activity,
                                    "initiatedBy": initiated_by_str
                                })
                                
                                if activity_rows_written >= max_rows:
                                    break
                                    
                            if on_page_callback:
                                try:
                                    on_page_callback(page_rows)
                                except Exception as cb_err:
                                    logger.warning("Error in User Creation logs page callback: %s", cb_err)
                                    
                            if activity_rows_written >= max_rows:
                                break
                                
                            next_url = data.get("@odata.nextLink")
                except PermissionError as pe:
                    logger.error("Permission error during User Creation logs query: %s", pe)
                    with open(csv_path, 'w', encoding='utf-8', newline='') as f_err:
                        w_err = csv.writer(f_err)
                        w_err.writerow(["Activity", "Initiated By"])
                        w_err.writerow(["ERROR", str(pe)])
                    if on_page_callback:
                        on_page_callback([{"activity": "ERROR", "initiatedBy": str(pe)}])
                except Exception as ex:
                    logger.error("Unexpected error during User Creation logs query: %s", ex)
                    with open(csv_path, 'w', encoding='utf-8', newline='') as f_err:
                        w_err = csv.writer(f_err)
                        w_err.writerow(["Activity", "Initiated By"])
                        w_err.writerow(["ERROR", f"Failed to query User Creation logs: {ex}"])
                    if on_page_callback:
                        on_page_callback([{"activity": "ERROR", "initiatedBy": f"Failed to query User Creation logs: {ex}"}])
                        
            logger.info("Finished fetching User Creation logs. Rows written: %d", rows_written)
        finally:
            self.client.release_token(token_slot)
