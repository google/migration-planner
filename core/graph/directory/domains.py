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

"""Service for querying Entra ID domains list and federation configurations."""

import logging
from typing import List, Dict, Any
from core.graph.client import GraphClient

logger = logging.getLogger(__name__)

class DomainsService:
    def __init__(self, client: GraphClient) -> None:
        self.client = client

    def get_domains(self, log_callback=None) -> List[Dict[str, Any]]:
        """Queries /domains and resolves federation configurations for federated domains."""
        if log_callback:
            log_callback("Querying domains from Microsoft Graph...")
        token_slot = self.client.get_active_token()
        session = self.client.get_session()

        headers = {
            "Authorization": f"Bearer {token_slot['token']}"
        }
        try:
            url = "https://graph.microsoft.com/v1.0/domains"
            logger.info("Querying Graph API domains endpoint: %s", url)
            resp = session.get(url, headers=headers, timeout=30.0)
            resp.raise_for_status()
            domains_list = resp.json().get("value", [])
        finally:
            self.client.release_token(token_slot)

        # Fetch federation configuration details for federated domains
        federated_domains = [d for d in domains_list if str(d.get("authenticationType", "")).lower() == "federated"]
        if federated_domains:
            token_slot = self.client.get_active_token()
            session = self.client.get_session()
            headers = {
                "Authorization": f"Bearer {token_slot['token']}",
                "Accept": "application/json"
            }
            try:
                for domain in federated_domains:
                    domain_id = domain.get("id")
                    fed_url = f"https://graph.microsoft.com/v1.0/domains/{domain_id}/federationConfiguration"
                    logger.info("Fetching federation configuration for domain: %s", domain_id)
                    try:
                        fed_resp = session.get(fed_url, headers=headers, timeout=30.0)
                        if fed_resp.status_code == 200:
                            fed_vals = fed_resp.json().get("value", [])
                            if fed_vals:
                                domain["federationDisplayName"] = fed_vals[0].get("displayName") or "N/A"
                                domain["federationIssuerUri"] = fed_vals[0].get("issuerUri") or "N/A"
                            else:
                                domain["federationDisplayName"] = "N/A"
                                domain["federationIssuerUri"] = "N/A"
                        else:
                            try:
                                err_msg = fed_resp.json().get("error", {}).get("message", f"HTTP {fed_resp.status_code}")
                            except Exception:
                                err_msg = f"HTTP {fed_resp.status_code}"
                            if len(err_msg) > 50:
                                err_msg = err_msg[:47] + "..."
                            domain["federationDisplayName"] = err_msg
                            domain["federationIssuerUri"] = err_msg
                    except Exception as err:
                        logger.warning("Error fetching federation configuration for %s: %s", domain_id, err)
                        err_msg = str(err)
                        if "timeout" in err_msg.lower():
                            err_msg = "Timeout Error"
                        elif "connection" in err_msg.lower():
                            err_msg = "Connection Error"
                        else:
                            err_msg = "Request Error"
                        domain["federationDisplayName"] = err_msg
                        domain["federationIssuerUri"] = err_msg
            finally:
                self.client.release_token(token_slot)

        return domains_list
