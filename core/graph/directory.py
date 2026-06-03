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

"""DirectoryService for querying tenant details and subscribed SKUs config."""

import logging
from typing import Dict, Any
from core.graph.client import GraphClient

logger = logging.getLogger(__name__)

class DirectoryService:
    """Service to query Entra ID directory configuration details."""

    def __init__(self, client: GraphClient) -> None:
        self.client = client

    def get_subscribed_skus(self) -> Dict[str, Any]:
        """Queries the Microsoft Graph /subscribedSkus endpoint with active retries."""
        token_slot = self.client.get_active_token()
        session = self.client.get_session()

        headers = {
            "Authorization": f"Bearer {token_slot['token']}",
            "ConsistencyLevel": "eventual"
        }
        try:
            url = "https://graph.microsoft.com/v1.0/subscribedSkus"
            logger.info("Querying Graph API configuration endpoint: %s", url)
            resp = session.get(url, headers=headers, timeout=30.0)
            resp.raise_for_status()
            return resp.json()
        finally:
            self.client.release_token(token_slot)

    def get_tenant_primary_domain(self) -> str:
        """Queries the /organization endpoint to retrieve the tenant's default or initial domain name."""
        token_slot = self.client.get_active_token()
        session = self.client.get_session()

        headers = {
            "Authorization": f"Bearer {token_slot['token']}"
        }
        try:
            url = "https://graph.microsoft.com/v1.0/organization"
            logger.info("Querying Graph API organization endpoint: %s", url)
            resp = session.get(url, headers=headers, timeout=30.0)
            resp.raise_for_status()
            data = resp.json()
            values = data.get("value", [])
            if not values:
                raise ValueError("No organization details found in Microsoft Graph.")
            
            domains = values[0].get("verifiedDomains", [])
            
            # 1. Find default domain
            for d in domains:
                if d.get("isDefault"):
                    return d.get("name")
                    
            # 2. Fallback to initial domain (.onmicrosoft.com)
            for d in domains:
                if d.get("isInitial"):
                    return d.get("name")
            
            # 3. Fallback to first verified domain
            if domains:
                return domains[0].get("name")
                
            raise ValueError("No verified domains found in organization details.")
        finally:
            self.client.release_token(token_slot)

