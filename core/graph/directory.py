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

    def get_directory_telemetry(self, log_callback=None) -> Dict[str, Any]:
        """Queries Microsoft Graph API in a single batch to fetch both domains list, user counts, and group counts."""
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
            import csv, os
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
