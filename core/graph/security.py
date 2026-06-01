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
