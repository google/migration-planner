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

"""Service for querying Entra ID provisioning logs."""

import logging
import csv
import json
from core.graph.client import GraphClient

logger = logging.getLogger(__name__)

class ProvisioningLogsService:
    def __init__(self, client: GraphClient) -> None:
        self.client = client

    def fetch_provisioning_logs(self, csv_path: str, max_rows: int = 200, on_page_callback=None, is_cancelled_callback=None) -> None:
        """Queries Microsoft Graph API /auditLogs/provisioning to fetch provisioning logs,
        flattens, and appends/saves to CSV.
        """
        token_slot = self.client.get_active_token()
        session = self.client.get_session()
        
        headers = {
            "Authorization": f"Bearer {token_slot['token']}",
            "Accept": "application/json"
        }
        
        url = "https://graph.microsoft.com/v1.0/auditLogs/provisioning?$select=initiatedBy,provisioningAction,provisioningSteps,servicePrincipal,sourceSystem,targetSystem,tenantId,provisioningStatusInfo&$top=100"
        
        def to_raw_str(val):
            if val is None:
                return ""
            if isinstance(val, (dict, list)):
                return json.dumps(val)
            return str(val)
            
        rows_written = 0
        try:
            logger.info("Starting Provisioning logs fetch...")
            # Initialize/overwrite CSV
            with open(csv_path, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["initiatedBy", "provisioningAction", "provisioningSteps", "servicePrincipal", "sourceSystem", "targetSystem", "tenantId", "provisioningStatusInfo"])
                
            with open(csv_path, 'a', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                
                next_url = url
                while next_url and rows_written < max_rows:
                    if is_cancelled_callback and is_cancelled_callback():
                        logger.info("Provisioning logs fetch cancelled in-flight.")
                        break
                        
                    logger.info("Querying MSFT Graph directory provisioning logs...")
                    try:
                        resp = session.get(next_url, headers=headers, timeout=60.0)
                    except Exception as get_err:
                        logger.warning("Query attempt failed with exception: %s. Displaying data obtained till now.", get_err)
                        break
                        
                    if not resp or resp.status_code != 200:
                        if resp and resp.status_code in [401, 403]:
                            logger.error("Provisioning audits endpoint permission error: %d %s", resp.status_code, resp.text)
                            raise PermissionError("AuditLog.Read.All permission required. Please ensure this permission is granted to the application registration in Microsoft Entra ID.")
                        else:
                            status_str = f"status {resp.status_code}" if resp else "connection/timeout error"
                            logger.warning("Provisioning audits query failed (%s). Displaying data obtained till now.", status_str)
                            break
                            
                    data = resp.json()
                    value_list = data.get("value", [])
                    
                    page_rows = []
                    for log in value_list:
                        initiated_by = to_raw_str(log.get("initiatedBy"))
                        action = to_raw_str(log.get("provisioningAction"))
                        steps = to_raw_str(log.get("provisioningSteps"))
                        service_principal = to_raw_str(log.get("servicePrincipal"))
                        source_system = to_raw_str(log.get("sourceSystem"))
                        target_system = to_raw_str(log.get("targetSystem"))
                        tenant_id = to_raw_str(log.get("tenantId"))
                        status_info = to_raw_str(log.get("provisioningStatusInfo"))
                        
                        writer.writerow([initiated_by, action, steps, service_principal, source_system, target_system, tenant_id, status_info])
                        rows_written += 1
                        
                        page_rows.append({
                            "initiatedBy": initiated_by,
                            "provisioningAction": action,
                            "provisioningSteps": steps,
                            "servicePrincipal": service_principal,
                            "sourceSystem": source_system,
                            "targetSystem": target_system,
                            "tenantId": tenant_id,
                            "provisioningStatusInfo": status_info
                        })
                        
                        if rows_written >= max_rows:
                            break
                            
                    if on_page_callback:
                        try:
                            on_page_callback(page_rows)
                        except Exception as cb_err:
                            logger.warning("Error in Provisioning logs page callback: %s", cb_err)
                            
                    if rows_written >= max_rows:
                        break
                        
                    next_url = data.get("@odata.nextLink")
        except PermissionError as pe:
            logger.error("Permission error during Provisioning logs query: %s", pe)
            with open(csv_path, 'w', encoding='utf-8', newline='') as f_err:
                w_err = csv.writer(f_err)
                w_err.writerow(["initiatedBy", "provisioningAction", "provisioningSteps", "servicePrincipal", "sourceSystem", "targetSystem", "tenantId", "provisioningStatusInfo"])
                w_err.writerow(["ERROR", str(pe), "", "", "", "", "", ""])
            if on_page_callback:
                on_page_callback([{
                    "initiatedBy": "ERROR",
                    "provisioningAction": str(pe),
                    "provisioningSteps": "",
                    "servicePrincipal": "",
                    "sourceSystem": "",
                    "targetSystem": "",
                    "tenantId": "",
                    "provisioningStatusInfo": ""
                }])
        except Exception as ex:
            logger.error("Unexpected error during Provisioning logs query: %s", ex)
            with open(csv_path, 'w', encoding='utf-8', newline='') as f_err:
                w_err = csv.writer(f_err)
                w_err.writerow(["initiatedBy", "provisioningAction", "provisioningSteps", "servicePrincipal", "sourceSystem", "targetSystem", "tenantId", "provisioningStatusInfo"])
                w_err.writerow(["ERROR", f"Failed to query Provisioning logs: {ex}", "", "", "", "", "", ""])
            if on_page_callback:
                on_page_callback([{
                    "initiatedBy": "ERROR",
                    "provisioningAction": f"Failed to query Provisioning logs: {ex}",
                    "provisioningSteps": "",
                    "servicePrincipal": "",
                    "sourceSystem": "",
                    "targetSystem": "",
                    "tenantId": "",
                    "provisioningStatusInfo": ""
                }])
                
        logger.info("Finished fetching Provisioning logs. Rows written: %d", rows_written)
        self.client.release_token(token_slot)
