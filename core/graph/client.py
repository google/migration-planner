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

"""Unified client managing connection pools, authentication, and sessions for Microsoft Graph."""

import logging
from typing import List, Dict, Any, Union, Optional
import requests
from util.auth_manager import TokenManager
from util.connectors import UrlInvoker

logger = logging.getLogger(__name__)

class GraphClient:
    """Unified client managing credentials validation and connection slots to Microsoft Graph."""

    def __init__(
        self,
        tenant_id: str,
        client_ids: Union[str, List[str]],
        client_secrets: Union[str, List[str]],
        concurrency: int = 1,
        retries: int = 5,
        backoff: int = 2
    ) -> None:
        self.tenant_id = tenant_id
        
        # Normalize inputs to lists to perfectly match TokenManager parameters
        self.client_ids = [client_ids] if isinstance(client_ids, str) else client_ids
        self.client_secrets = [client_secrets] if isinstance(client_secrets, str) else client_secrets
        
        self.token_manager = TokenManager(
            tenant_id=self.tenant_id,
            client_ids=self.client_ids,
            client_secrets=self.client_secrets,
            concurrency=concurrency,
            retries=retries,
            backoff=backoff
        )

        self.url_invoker = UrlInvoker(
            token_manager=self.token_manager,
            batch_retry_count=retries,
            batch_backoff=backoff,
            initial_delay=1,
            jitter=0.5
        )

    def authenticate(self, required_scopes: Optional[List[str]] = None) -> None:
        """Validates Entra ID scopes and fetches active tokens."""
        self.token_manager.authenticate_all(required_scopes=required_scopes)

    def get_session(self) -> requests.Session:
        """Returns the unified requests Session pool."""
        return self.token_manager.get_session()

    def get_active_token(self) -> Dict[str, Any]:
        """Acquires an active, refreshed token slot from TokenManager lease queue."""
        return self.token_manager.get_valid_token_slot()

    def release_token(self, token_slot: Dict[str, Any]) -> None:
        """Returns token slot to the queue, completing the lease cycle."""
        self.token_manager.return_token_slot(token_slot)

    def close(self) -> None:
        """Releases all connection pool and socket resources."""
        self.token_manager.close()

    def __enter__(self) -> "GraphClient":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
