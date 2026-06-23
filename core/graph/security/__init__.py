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

"""Facade exposing security & governance pipelines and compatibility SecurityService."""

import logging
import csv

from core.graph.client import GraphClient
from core.graph.security.sensitivity_labels import run_sensitivity_labels_pipeline
from core.graph.security.retention_policies import run_retention_policies_pipeline
from core.graph.security.dlp_policies import run_dlp_policies_pipeline
from core.graph.security.sensitive_info_types import run_sensitive_info_types_pipeline
from core.graph.security.authentication import run_authentication_pipeline
from core.graph.security.service_principals_sso import run_service_principals_sso_pipeline

logger = logging.getLogger(__name__)

class SecurityService:
    """Service to interact with M365 Security and Information Protection configurations (backward compatibility)."""

    def __init__(self, client: GraphClient) -> None:
        self.client = client

    def fetch_sensitivity_labels(self, csv_path: str = None, on_page_callback=None, is_cancelled_callback=None) -> list:
        """Fetches sensitivity labels."""
        labels = []
        def on_page(items):
            labels.extend(items)
            if on_page_callback:
                on_page_callback(items)
                
        run_sensitivity_labels_pipeline(
            client_id=self.client.client_ids[0] if isinstance(self.client.client_ids, list) else self.client.client_ids,
            client_secret=self.client.client_secrets[0] if isinstance(self.client.client_secrets, list) else self.client.client_secrets,
            tenant_id=self.client.tenant_id,
            csv_path=csv_path,
            on_page_callback=on_page,
            is_cancelled_callback=is_cancelled_callback
        )
        return labels

    def fetch_conditional_access_policies(self, csv_path: str = None, on_page_callback=None, is_cancelled_callback=None) -> None:
        """Fetches conditional access policies."""
        run_authentication_pipeline(
            client_id=self.client.client_ids[0] if isinstance(self.client.client_ids, list) else self.client.client_ids,
            client_secret=self.client.client_secrets[0] if isinstance(self.client.client_secrets, list) else self.client.client_secrets,
            tenant_id=self.client.tenant_id,
            csv_path=csv_path,
            on_page_callback=on_page_callback,
            is_cancelled_callback=is_cancelled_callback
        )

    def fetch_sso_service_principals(self, csv_path: str = None, on_page_callback=None, is_cancelled_callback=None) -> None:
        """Fetches service principals SSO configurations."""
        run_service_principals_sso_pipeline(
            client_id=self.client.client_ids[0] if isinstance(self.client.client_ids, list) else self.client.client_ids,
            client_secret=self.client.client_secrets[0] if isinstance(self.client.client_secrets, list) else self.client.client_secrets,
            tenant_id=self.client.tenant_id,
            csv_path=csv_path,
            on_page_callback=on_page_callback,
            is_cancelled_callback=is_cancelled_callback
        )

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
            else:
                return {}
        except Exception as e:
            logger.warning("Exception during Graph Search query: %s", e)
            return {}
        finally:
            self.client.release_token(token_slot)

    def fetch_signin_activities(self, event_type: str, csv_path: str, max_rows: int = 10000, on_page_callback=None, is_cancelled_callback=None) -> None:
        """Fetches successful sign-in logs (legacy utility)."""
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
        
        try:
            logger.info("Fetching successful sign-in activities for %s...", event_type)
            with open(csv_path, 'a', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                while next_url and rows_written < max_rows:
                    if is_cancelled_callback and is_cancelled_callback():
                        break
                    resp = session.get(next_url, headers=headers)
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
                            on_page_callback(value_list)
                        next_url = data.get("@odata.nextLink")
                    else:
                        break
        finally:
            self.client.release_token(token_slot)
