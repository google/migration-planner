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

"""SecurityService encapsulating Microsoft Graph security and governance policy queries."""

import logging
from core.graph.client import GraphClient

logger = logging.getLogger(__name__)

class SecurityService:
    """Service to interact with M365 Security and Information Protection configurations."""

    def __init__(self, client: GraphClient) -> None:
        self.client = client

    def fetch_sensitivity_labels(self) -> list[dict]:
        """Fetches the sensitivity labels configured for the tenant in JSON format."""
        url = "https://graph.microsoft.com/v1.0/security/dataSecurityAndGovernance/sensitivityLabels"
        token_slot = self.client.get_active_token()
        session = self.client.get_session()
        
        headers = {
            "Authorization": f"Bearer {token_slot['token']}",
            "Accept": "application/json"
        }
        try:
            logger.info("Querying Microsoft Graph information protection sensitivity labels...")
            resp = session.get(url, headers=headers)
            if resp.status_code == 200:
                data = resp.json()
                return data.get("value", [])
            else:
                logger.error("Graph sensitivityLabels endpoint failed with status %d: %s", resp.status_code, resp.text)
                raise ConnectionError(f"Microsoft Graph API request failed with status {resp.status_code}")
        finally:
            self.client.release_token(token_slot)

    def fetch_conditional_access_policies(self) -> list[dict]:
        """Fetches the Microsoft Entra Conditional Access policies configured for the tenant."""
        url = "https://graph.microsoft.com/v1.0/identity/conditionalAccess/policies"
        token_slot = self.client.get_active_token()
        session = self.client.get_session()
        
        headers = {
            "Authorization": f"Bearer {token_slot['token']}",
            "Accept": "application/json"
        }
        try:
            logger.info("Querying Entra ID Conditional Access policies...")
            resp = session.get(url, headers=headers)
            if resp.status_code == 200:
                data = resp.json()
                return data.get("value", [])
            elif resp.status_code in [401, 403]:
                logger.error("Conditional Access endpoint permission error: %d %s", resp.status_code, resp.text)
                raise PermissionError("Policy.Read.All or Policy.Read permission required.")
            else:
                logger.error("Conditional Access endpoint failed with status %d: %s", resp.status_code, resp.text)
                raise ConnectionError(f"Microsoft Graph API request failed with status {resp.status_code}")
        finally:
            self.client.release_token(token_slot)

    def fetch_sso_service_principals(self) -> list[dict]:
        """Fetches Microsoft Entra Enterprise Applications (Service Principals) to analyze Single Sign-On."""
        url = "https://graph.microsoft.com/v1.0/servicePrincipals?$select=id,appDisplayName,preferredSingleSignOnMode&$top=100"
        token_slot = self.client.get_active_token()
        session = self.client.get_session()
        
        headers = {
            "Authorization": f"Bearer {token_slot['token']}",
            "Accept": "application/json"
        }
        try:
            logger.info("Querying Entra ID Service Principals for Single Sign-On modes...")
            resp = session.get(url, headers=headers)
            if resp.status_code == 200:
                data = resp.json()
                return data.get("value", [])
            elif resp.status_code in [401, 403]:
                logger.error("Service Principals endpoint permission error: %d %s", resp.status_code, resp.text)
                raise PermissionError("Application.Read.All permission required.")
            else:
                logger.error("Service Principals endpoint failed with status %d: %s", resp.status_code, resp.text)
                raise ConnectionError(f"Microsoft Graph API request failed with status {resp.status_code}")
        finally:
            self.client.release_token(token_slot)

    def search_cloud_pst_files(self) -> dict:
        """Queries Microsoft Graph Search API to locate cloud-stored PST archive files."""
        url = "https://graph.microsoft.com/v1.0/search/query"
        token_slot = self.client.get_active_token()
        session = self.client.get_session()
        
        headers = {
            "Authorization": f"Bearer {token_slot['token']}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        payload = {
            "requests": [
                {
                    "entityTypes": ["driveItem"],
                    "query": {"queryString": "fileextension:pst"},
                    "from": 0,
                    "size": 50
                }
            ]
        }
        try:
            logger.info("Executing Graph Search query for cloud PST archives...")
            resp = session.post(url, json=payload, headers=headers)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code in [401, 403]:
                logger.warning("Graph Search permission error: %d %s", resp.status_code, resp.text)
                return {}
            else:
                logger.warning("Graph Search query failed with status %d: %s", resp.status_code, resp.text)
                return {}
        except Exception as e:
            logger.warning("Exception during Graph Search query: %s", e)
            return {}
        finally:
            self.client.release_token(token_slot)

    def fetch_signin_activities(self, event_type: str, csv_path: str, max_rows: int = 10000, on_page_callback=None, is_cancelled_callback=None) -> None:
        """Fetches successful sign-in logs from Microsoft Graph beta auditLogs endpoint for a specific signInEventType
        and appends them to a CSV file.
        
        Args:
            event_type: 'interactiveUser' or 'nonInteractiveUser'
            csv_path: Absolute path to the CSV file where logs will be appended
            max_rows: Maximum number of rows to retrieve (default 10000)
            on_page_callback: Optional callback invoked as on_page_callback(value_list) for each page fetched
            is_cancelled_callback: Optional callable returning boolean indicating if operation is cancelled
        """
        base_url = "https://graph.microsoft.com/beta/auditLogs/signIns"
        filter_str = f"status/errorCode eq 0 and signInEventTypes/any(t: t eq '{event_type}')"
        select_str = "appDisplayName,deviceDetail"
        
        token_slot = self.client.get_active_token()
        session = self.client.get_session()
        
        headers = {
            "Authorization": f"Bearer {token_slot['token']}",
            "Accept": "application/json"
        }
        
        next_url = f"{base_url}?$filter={filter_str}&$select={select_str}&$top=100"
        rows_written = 0
        page_number = 1
        import csv
        
        try:
            logger.info("Fetching successful sign-in activities for %s...", event_type)
            with open(csv_path, 'a', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                
                while next_url and rows_written < max_rows:
                    if is_cancelled_callback and is_cancelled_callback():
                        logger.info("Fetch cancelled in-flight for %s. Aborting pagination loop.", event_type)
                        break
                    import time
                    retries_left = 2
                    resp = None
                    while retries_left > 0:
                        logger.info("Querying MSFT Graph sign-ins endpoint (Event: %s, Page: %d, Rows so far: %d, Attempt: %d)...", 
                                    event_type, page_number, rows_written, 3 - retries_left)
                        try:
                            resp = session.get(next_url, headers=headers, timeout=40.0)
                            if resp.status_code == 200:
                                break
                            elif resp.status_code in [401, 403]:
                                break
                        except Exception as get_err:
                            logger.warning("Query attempt failed: %s", get_err)
                        
                        retries_left -= 1
                        if retries_left > 0:
                            time.sleep(2)

                    page_number += 1
                    if resp and resp.status_code == 200:
                        data = resp.json()
                        value_list = data.get("value", [])
                        
                        for log in value_list:
                            if rows_written >= max_rows:
                                break
                            app_name = log.get("appDisplayName") or ""
                            device = log.get("deviceDetail") or {}
                            os_name = device.get("operatingSystem") or ""
                            browser_name = device.get("browser") or ""
                            writer.writerow([app_name, os_name, browser_name, event_type])
                            rows_written += 1
                        
                        if on_page_callback:
                            try:
                                on_page_callback(value_list)
                            except Exception as cb_err:
                                logger.warning("Error in sign-ins page callback: %s", cb_err)
                        
                        if rows_written >= max_rows:
                            break
                            
                        next_url = data.get("@odata.nextLink")
                    else:
                        if resp and resp.status_code in [401, 403]:
                            logger.error("Sign-ins endpoint permission error: %d %s", resp.status_code, resp.text)
                            raise PermissionError("AuditLog.Read.All permission required.")
                        else:
                            status_str = f"status {resp.status_code}" if resp else "connection/timeout error"
                            logger.warning("Sign-ins endpoint query failed after 2 attempts (%s). Stopping pagination.", status_str)
                            break
            
            logger.info("Successfully fetched and appended %d sign-in records for %s", rows_written, event_type)
        finally:
            self.client.release_token(token_slot)




