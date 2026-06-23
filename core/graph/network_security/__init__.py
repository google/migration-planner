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

"""Facade exposing Network Security pipelines and compatibility service."""

from core.graph.client import GraphClient
from core.graph.network_security.filtering import run_filtering_pipeline
from core.graph.network_security.conditional_access import run_conditional_access_pipeline
from core.graph.network_security.firewall import run_firewall_pipeline

class NetworkSecurityService:
    """Service to interact with Network Security configurations (backward compatibility)."""

    def __init__(self, client: GraphClient) -> None:
        self.client = client

    def fetch_filtering_policies(self, csv_path: str = None, on_page_callback=None, is_cancelled_callback=None) -> None:
        """Fetches Entra Global Secure Access Filtering Policies (Beta)."""
        run_filtering_pipeline(
            client_id=self.client.client_ids[0] if isinstance(self.client.client_ids, list) else self.client.client_ids,
            client_secret=self.client.client_secrets[0] if isinstance(self.client.client_secrets, list) else self.client.client_secrets,
            tenant_id=self.client.tenant_id,
            csv_path=csv_path,
            on_page_callback=on_page_callback,
            is_cancelled_callback=is_cancelled_callback
        )

    def fetch_conditional_access_policies(self, csv_path: str = None, on_page_callback=None, is_cancelled_callback=None) -> None:
        """Fetches Entra ID Conditional Access policies."""
        run_conditional_access_pipeline(
            client_id=self.client.client_ids[0] if isinstance(self.client.client_ids, list) else self.client.client_ids,
            client_secret=self.client.client_secrets[0] if isinstance(self.client.client_secrets, list) else self.client.client_secrets,
            tenant_id=self.client.tenant_id,
            csv_path=csv_path,
            on_page_callback=on_page_callback,
            is_cancelled_callback=is_cancelled_callback
        )

    def fetch_firewall_policies(self, csv_path: str = None, on_page_callback=None, is_cancelled_callback=None) -> None:
        """Fetches Intune device configurations and filters Firewall & Proxy configs."""
        run_firewall_pipeline(
            client_id=self.client.client_ids[0] if isinstance(self.client.client_ids, list) else self.client.client_ids,
            client_secret=self.client.client_secrets[0] if isinstance(self.client.client_secrets, list) else self.client.client_secrets,
            tenant_id=self.client.tenant_id,
            csv_path=csv_path,
            on_page_callback=on_page_callback,
            is_cancelled_callback=is_cancelled_callback
        )
