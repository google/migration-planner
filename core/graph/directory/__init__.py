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

"""DirectoryService backward compatibility facade and sub-module exports."""

import logging
from typing import Dict, Any, List

from core.graph.client import GraphClient
from core.graph.directory.organization import OrganizationService
from core.graph.directory.domains import DomainsService
from core.graph.directory.user_logs import UserLogsService
from core.graph.directory.provisioning_logs import ProvisioningLogsService
from core.graph.directory.users_groups import UsersGroupsService
from core.graph.directory.subscribed_skus import SubscribedSKUsService
from core.graph.directory.service_principals import ServicePrincipalsService

logger = logging.getLogger(__name__)

class DirectoryService:
    """Facade for querying Entra ID directory configuration details (for backward compatibility)."""

    def __init__(self, client: GraphClient) -> None:
        self.client = client

    def get_subscribed_skus(self) -> Dict[str, Any]:
        return SubscribedSKUsService(self.client).get_subscribed_skus()

    def get_tenant_primary_domain(self) -> str:
        return OrganizationService(self.client).get_tenant_primary_domain()

    def get_directory_telemetry(self, log_callback=None) -> Dict[str, Any]:
        """Queries Microsoft Graph API in a single batch to fetch both domains list, user counts, and group counts.
        Maintained for exact backward compatibility with test suites and batch invocations.
        """
        logger.info("Fetching directory telemetry data using Graph API batch...")
        if log_callback:
            log_callback("Querying Microsoft Graph API for directory domains, users, and groups...")

        batch_requests = [
            {
                "id": "organization",
                "method": "GET",
                "url": "/organization",
            },
            {
                "id": "domains",
                "method": "GET",
                "url": "/domains",
            },
            {
                "id": "total",
                "method": "GET",
                "url": "/groups?$count=true&$top=1&$select=id",
                "headers": {"ConsistencyLevel": "eventual"}
            },
            {
                "id": "security",
                "method": "GET",
                "url": "/groups?$filter=securityEnabled eq true and mailEnabled eq false&$count=true&$top=1&$select=id",
                "headers": {"ConsistencyLevel": "eventual"}
            },
            {
                "id": "distribution",
                "method": "GET",
                "url": "/groups?$filter=mailEnabled eq true and securityEnabled eq false and NOT groupTypes/any(c:c eq 'Unified')&$count=true&$top=1&$select=id",
                "headers": {"ConsistencyLevel": "eventual"}
            },
            {
                "id": "mail_enabled_security",
                "method": "GET",
                "url": "/groups?$filter=mailEnabled eq true and securityEnabled eq true and NOT groupTypes/any(c:c eq 'Unified')&$count=true&$top=1&$select=id",
                "headers": {"ConsistencyLevel": "eventual"}
            },
            {
                "id": "m365",
                "method": "GET",
                "url": "/groups?$filter=groupTypes/any(c:c eq 'Unified')&$count=true&$top=1&$select=id",
                "headers": {"ConsistencyLevel": "eventual"}
            },
            {
                "id": "dynamic",
                "method": "GET",
                "url": "/groups?$filter=groupTypes/any(s:s eq 'DynamicMembership')&$count=true&$top=1&$select=id",
                "headers": {"ConsistencyLevel": "eventual"}
            },
            {
                "id": "users_total",
                "method": "GET",
                "url": "/users?$count=true&$top=1&$select=id",
                "headers": {"ConsistencyLevel": "eventual"}
            },
            {
                "id": "users_enabled",
                "method": "GET",
                "url": "/users?$filter=accountEnabled eq true&$count=true&$top=1&$select=id",
                "headers": {"ConsistencyLevel": "eventual"}
            },
            {
                "id": "users_disabled",
                "method": "GET",
                "url": "/users?$filter=accountEnabled eq false&$count=true&$top=1&$select=id",
                "headers": {"ConsistencyLevel": "eventual"}
            },
            {
                "id": "users_member",
                "method": "GET",
                "url": "/users?$filter=userType eq 'Member'&$count=true&$top=1&$select=id",
                "headers": {"ConsistencyLevel": "eventual"}
            },
            {
                "id": "users_guest",
                "method": "GET",
                "url": "/users?$filter=userType eq 'Guest'&$count=true&$top=1&$select=id",
                "headers": {"ConsistencyLevel": "eventual"}
            }
        ]

        # Invoke the batch query via client's UrlInvoker
        responses = self.client.url_invoker.invoke(
            url="https://graph.microsoft.com/v1.0",
            batch=batch_requests,
            logger=log_callback or (lambda x: None),
            context="DirectoryTelemetry"
        )

        organization_list = []
        domains_list = []
        counts = {
            "total": 0,
            "security": 0,
            "distribution": 0,
            "mail_enabled_security": 0,
            "m365": 0,
            "dynamic": 0
        }
        
        user_counts = {
            "users_total": 0,
            "users_enabled": 0,
            "users_disabled": 0,
            "users_member": 0,
            "users_guest": 0
        }

        for resp in responses:
            resp_id = resp.get("id")
            if resp.get("status", 0) != 200:
                error_msg = resp.get("body", {}).get("error", {}).get("message", "Unknown error")
                logger.error("Failed to fetch directory telemetry for %s: status %s, message: %s", resp_id, resp.get("status"), error_msg)
                raise Exception(f"Failed to fetch directory telemetry for '{resp_id}': {error_msg}")

            body = resp.get("body", {})
            if resp_id == "organization":
                organization_list = body.get("value", [])
            elif resp_id == "domains":
                domains_list = body.get("value", [])
            elif resp_id in counts:
                count_val = body.get("@odata.count", 0)
                counts[resp_id] = count_val
            elif resp_id in user_counts:
                count_val = body.get("@odata.count", 0)
                user_counts[resp_id] = count_val

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

        # Normalize user counts dictionary keys for the UI
        normalized_user_counts = {
            "total": user_counts["users_total"],
            "enabled": user_counts["users_enabled"],
            "disabled": user_counts["users_disabled"],
            "member": user_counts["users_member"],
            "guest": user_counts["users_guest"]
        }

        return {
            "organization": organization_list,
            "domains": domains_list,
            "group_counts": counts,
            "user_counts": normalized_user_counts
        }

    def fetch_service_principals_sso(self, csv_path: str = None, on_page_callback=None, is_cancelled_callback=None) -> None:
        ServicePrincipalsService(self.client).fetch_service_principals_sso(
            csv_path=csv_path,
            on_page_callback=on_page_callback,
            is_cancelled_callback=is_cancelled_callback
        )

    def fetch_user_creation_logs(self, csv_path: str, max_rows: int = 50, on_page_callback=None, is_cancelled_callback=None) -> None:
        UserLogsService(self.client).fetch_user_creation_logs(
            csv_path=csv_path,
            max_rows=max_rows,
            on_page_callback=on_page_callback,
            is_cancelled_callback=is_cancelled_callback
        )

    def fetch_provisioning_logs(self, csv_path: str, max_rows: int = 200, on_page_callback=None, is_cancelled_callback=None) -> None:
        ProvisioningLogsService(self.client).fetch_provisioning_logs(
            csv_path=csv_path,
            max_rows=max_rows,
            on_page_callback=on_page_callback,
            is_cancelled_callback=is_cancelled_callback
        )
