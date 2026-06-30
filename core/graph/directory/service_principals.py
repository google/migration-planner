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

"""Service for querying Service Principals (Enterprise Apps) and SSO configuration details."""

import logging
import csv
from core.graph.client import GraphClient

logger = logging.getLogger(__name__)

class ServicePrincipalsService:
    def __init__(self, client: GraphClient) -> None:
        self.client = client

    def fetch_service_principals_sso(self, csv_path: str = None, on_page_callback=None, is_cancelled_callback=None) -> None:
        """Fetches all Service Principals (Enterprise Apps) and their SSO modes, streaming to CSV."""
        logger.info("Fetching Service Principals and SSO modes...")
        token_slot = self.client.get_active_token()
        session = self.client.get_session()
        headers = {
            "Authorization": f"Bearer {token_slot['token']}",
            "ConsistencyLevel": "eventual"
        }
        
        url = "https://graph.microsoft.com/v1.0/servicePrincipals?$select=id,appId,displayName,preferredSingleSignOnMode"
        
        try:
            if csv_path:
                f = open(csv_path, 'w', encoding='utf-8', newline='')
                writer = csv.writer(f)
                writer.writerow(["displayName", "preferredSingleSignOnMode"])
            else:
                f = None
                writer = None
            
            while url:
                if is_cancelled_callback and is_cancelled_callback(): break
                resp = session.get(url, headers=headers, timeout=30.0)
                resp.raise_for_status()
                data = resp.json()
                value_list = data.get("value", [])
                
                if writer:
                    for sp in value_list:
                        writer.writerow([
                            sp.get("displayName", ""), 
                            sp.get("preferredSingleSignOnMode", "")
                        ])
                
                if on_page_callback:
                    on_page_callback(value_list)
                    
                url = data.get("@odata.nextLink")
        finally:
            if 'f' in locals() and f: f.close()
            self.client.release_token(token_slot)
