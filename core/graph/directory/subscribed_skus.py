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

"""Service for querying Entra ID subscribed SKUs."""

import logging
from typing import Dict, Any
from core.graph.client import GraphClient

logger = logging.getLogger(__name__)

class SubscribedSKUsService:
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
