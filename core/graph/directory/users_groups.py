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

"""Service for querying Entra ID Users & Groups counts."""

import logging
from typing import Dict, Any
from core.graph.client import GraphClient

logger = logging.getLogger(__name__)

class UsersGroupsService:
    def __init__(self, client: GraphClient) -> None:
        self.client = client

    def get_users_groups_counts(self, log_callback=None) -> Dict[str, Any]:
        """Queries Microsoft Graph API in batch to fetch group counts and user counts."""
        if log_callback:
            log_callback("Querying users and groups counts from Microsoft Graph...")
        logger.info("Fetching users & groups count telemetry data using Graph API batch...")

        batch_requests = [
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

        responses = self.client.url_invoker.invoke(
            url="https://graph.microsoft.com/v1.0",
            batch=batch_requests,
            logger=log_callback or (lambda x: None),
            context="UsersGroupsTelemetry"
        )

        group_counts = {
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
                logger.error("Failed to fetch Users/Groups count for %s: status %s, message: %s", resp_id, resp.get("status"), error_msg)
                raise Exception(f"Failed to fetch Users/Groups count for '{resp_id}': {error_msg}")

            body = resp.get("body", {})
            if resp_id in group_counts:
                count_val = body.get("@odata.count", 0)
                group_counts[resp_id] = count_val
            elif resp_id in user_counts:
                count_val = body.get("@odata.count", 0)
                user_counts[resp_id] = count_val

        # Normalize keys
        normalized_user_counts = {
            "total": user_counts["users_total"],
            "enabled": user_counts["users_enabled"],
            "disabled": user_counts["users_disabled"],
            "member": user_counts["users_member"],
            "guest": user_counts["users_guest"]
        }

        return {
            "group_counts": group_counts,
            "user_counts": normalized_user_counts
        }
