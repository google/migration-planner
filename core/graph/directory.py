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

import os
import csv
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

    def fetch_user_creation_logs(self, csv_path: str, max_rows: int = 50, on_page_callback=None, is_cancelled_callback=None) -> None:
        """Queries Microsoft Graph API /auditLogs/directoryAudits to fetch successful Add user and Delete user logs,
        flattens, and appends/saves to CSV.
        """
        token_slot = self.client.get_active_token()
        session = self.client.get_session()
        
        headers = {
            "Authorization": f"Bearer {token_slot['token']}",
            "Accept": "application/json"
        }
        
        urls = [
            ("Add user", "https://graph.microsoft.com/v1.0/auditLogs/directoryAudits?$select=activityDisplayName,initiatedBy&$filter=activityDisplayName eq 'Add user' and result eq 'success'&$top=50"),
            ("Delete user", "https://graph.microsoft.com/v1.0/auditLogs/directoryAudits?$select=activityDisplayName,initiatedBy&$filter=activityDisplayName eq 'Delete user' and result eq 'success'&$top=50")
        ]
        
        import csv
        
        rows_written = 0
        try:
            logger.info("Starting User Creation logs fetch...")
            # We want to overwrite or initialize the CSV file with headers if it's the start
            with open(csv_path, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["Activity", "Initiated By"])
                
            with open(csv_path, 'a', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                
                try:
                    for activity_type, url in urls:
                        if is_cancelled_callback and is_cancelled_callback():
                            logger.info("User Creation logs fetch cancelled in-flight.")
                            break
                            
                        next_url = url
                        activity_rows_written = 0
                        
                        while next_url and activity_rows_written < max_rows:
                            if is_cancelled_callback and is_cancelled_callback():
                                break
                                
                            logger.info("Querying MSFT Graph directory audits for '%s'...", activity_type)
                            try:
                                resp = session.get(next_url, headers=headers, timeout=60.0)
                            except Exception as get_err:
                                logger.warning("Query attempt for '%s' failed with exception: %s. Displaying data obtained till now.", activity_type, get_err)
                                break
                                
                            if not resp or resp.status_code != 200:
                                if resp and resp.status_code in [401, 403]:
                                    logger.error("Directory audits endpoint permission error: %d %s", resp.status_code, resp.text)
                                    raise PermissionError("AuditLog.Read.All permission required. Please ensure this permission is granted to the application registration in Microsoft Entra ID.")
                                else:
                                    status_str = f"status {resp.status_code}" if resp else "connection/timeout error"
                                    logger.warning("Directory audits query for '%s' failed (%s). Displaying data obtained till now.", activity_type, status_str)
                                    break
                                    
                            data = resp.json()
                            value_list = data.get("value", [])
                            
                            page_rows = []
                            for log in value_list:
                                activity = log.get("activityDisplayName") or ""
                                initiated_by_obj = log.get("initiatedBy") or {}
                                
                                import json
                                initiated_by_str = json.dumps(initiated_by_obj)
                                    
                                writer.writerow([activity, initiated_by_str])
                                activity_rows_written += 1
                                rows_written += 1
                                
                                page_rows.append({
                                    "activity": activity,
                                    "initiatedBy": initiated_by_str
                                })
                                
                                if activity_rows_written >= max_rows:
                                    break
                                    
                            if on_page_callback:
                                try:
                                    on_page_callback(page_rows)
                                except Exception as cb_err:
                                    logger.warning("Error in User Creation logs page callback: %s", cb_err)
                                    
                            if activity_rows_written >= max_rows:
                                break
                                
                            next_url = data.get("@odata.nextLink")
                except PermissionError as pe:
                    logger.error("Permission error during User Creation logs query: %s", pe)
                    with open(csv_path, 'w', encoding='utf-8', newline='') as f_err:
                        w_err = csv.writer(f_err)
                        w_err.writerow(["Activity", "Initiated By"])
                        w_err.writerow(["ERROR", str(pe)])
                    if on_page_callback:
                        on_page_callback([{"activity": "ERROR", "initiatedBy": str(pe)}])
                except Exception as ex:
                    logger.error("Unexpected error during User Creation logs query: %s", ex)
                    with open(csv_path, 'w', encoding='utf-8', newline='') as f_err:
                        w_err = csv.writer(f_err)
                        w_err.writerow(["Activity", "Initiated By"])
                        w_err.writerow(["ERROR", f"Failed to query User Creation logs: {ex}"])
                    if on_page_callback:
                        on_page_callback([{"activity": "ERROR", "initiatedBy": f"Failed to query User Creation logs: {ex}"}])
                        
            logger.info("Finished fetching User Creation logs. Rows written: %d", rows_written)
        finally:
            self.client.release_token(token_slot)

    def fetch_provisioning_logs(self, csv_path: str, max_rows: int = 200, on_page_callback=None, is_cancelled_callback=None) -> None:
        """Queries Microsoft Graph API /auditLogs/provisioning to fetch provisioning logs,
        flattens, and appends/saves to CSV.
        """
        token_slot = self.client.get_active_token()
        session = self.client.get_session()
        
        headers = {
            "Authorization": f"Bearer {token_slot['token']}",
            "Accept": "application/json"
        }
        
        url = "https://graph.microsoft.com/v1.0/auditLogs/provisioning?$select=initiatedBy,provisioningAction,provisioningSteps,servicePrincipal,sourceSystem,targetSystem,tenantId,provisioningStatusInfo&$top=100"
        
        import csv
        import json
        
        def to_raw_str(val):
            if val is None:
                return ""
            if isinstance(val, (dict, list)):
                return json.dumps(val)
            return str(val)
            
        rows_written = 0
        try:
            logger.info("Starting Provisioning logs fetch...")
            # We want to overwrite or initialize the CSV file with headers if it's the start
            with open(csv_path, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["initiatedBy", "provisioningAction", "provisioningSteps", "servicePrincipal", "sourceSystem", "targetSystem", "tenantId", "provisioningStatusInfo"])
                
            with open(csv_path, 'a', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                
                next_url = url
                while next_url and rows_written < max_rows:
                    if is_cancelled_callback and is_cancelled_callback():
                        logger.info("Provisioning logs fetch cancelled in-flight.")
                        break
                        
                    logger.info("Querying MSFT Graph directory provisioning logs...")
                    try:
                        resp = session.get(next_url, headers=headers, timeout=60.0)
                    except Exception as get_err:
                        logger.warning("Query attempt failed with exception: %s. Displaying data obtained till now.", get_err)
                        break
                        
                    if not resp or resp.status_code != 200:
                        if resp and resp.status_code in [401, 403]:
                            logger.error("Provisioning audits endpoint permission error: %d %s", resp.status_code, resp.text)
                            raise PermissionError("AuditLog.Read.All permission required. Please ensure this permission is granted to the application registration in Microsoft Entra ID.")
                        else:
                            status_str = f"status {resp.status_code}" if resp else "connection/timeout error"
                            logger.warning("Provisioning audits query failed (%s). Displaying data obtained till now.", status_str)
                            break
                            
                    data = resp.json()
                    value_list = data.get("value", [])
                    
                    page_rows = []
                    for log in value_list:
                        initiated_by = to_raw_str(log.get("initiatedBy"))
                        action = to_raw_str(log.get("provisioningAction"))
                        steps = to_raw_str(log.get("provisioningSteps"))
                        service_principal = to_raw_str(log.get("servicePrincipal"))
                        source_system = to_raw_str(log.get("sourceSystem"))
                        target_system = to_raw_str(log.get("targetSystem"))
                        tenant_id = to_raw_str(log.get("tenantId"))
                        status_info = to_raw_str(log.get("provisioningStatusInfo"))
                        
                        writer.writerow([initiated_by, action, steps, service_principal, source_system, target_system, tenant_id, status_info])
                        rows_written += 1
                        
                        page_rows.append({
                            "initiatedBy": initiated_by,
                            "provisioningAction": action,
                            "provisioningSteps": steps,
                            "servicePrincipal": service_principal,
                            "sourceSystem": source_system,
                            "targetSystem": target_system,
                            "tenantId": tenant_id,
                            "provisioningStatusInfo": status_info
                        })
                        
                        if rows_written >= max_rows:
                            break
                            
                    if on_page_callback:
                        try:
                            on_page_callback(page_rows)
                        except Exception as cb_err:
                            logger.warning("Error in Provisioning logs page callback: %s", cb_err)
                            
                    if rows_written >= max_rows:
                        break
                        
                    next_url = data.get("@odata.nextLink")
        except PermissionError as pe:
            logger.error("Permission error during Provisioning logs query: %s", pe)
            with open(csv_path, 'w', encoding='utf-8', newline='') as f_err:
                w_err = csv.writer(f_err)
                w_err.writerow(["initiatedBy", "provisioningAction", "provisioningSteps", "servicePrincipal", "sourceSystem", "targetSystem", "tenantId", "provisioningStatusInfo"])
                w_err.writerow(["ERROR", str(pe), "", "", "", "", "", ""])
            if on_page_callback:
                on_page_callback([{
                    "initiatedBy": "ERROR",
                    "provisioningAction": str(pe),
                    "provisioningSteps": "",
                    "servicePrincipal": "",
                    "sourceSystem": "",
                    "targetSystem": "",
                    "tenantId": "",
                    "provisioningStatusInfo": ""
                }])
        except Exception as ex:
            logger.error("Unexpected error during Provisioning logs query: %s", ex)
            with open(csv_path, 'w', encoding='utf-8', newline='') as f_err:
                w_err = csv.writer(f_err)
                w_err.writerow(["initiatedBy", "provisioningAction", "provisioningSteps", "servicePrincipal", "sourceSystem", "targetSystem", "tenantId", "provisioningStatusInfo"])
                w_err.writerow(["ERROR", f"Failed to query Provisioning logs: {ex}", "", "", "", "", "", ""])
            if on_page_callback:
                on_page_callback([{
                    "initiatedBy": "ERROR",
                    "provisioningAction": f"Failed to query Provisioning logs: {ex}",
                    "provisioningSteps": "",
                    "servicePrincipal": "",
                    "sourceSystem": "",
                    "targetSystem": "",
                    "tenantId": "",
                    "provisioningStatusInfo": ""
                }])
                
        logger.info("Finished fetching Provisioning logs. Rows written: %d", rows_written)
        self.client.release_token(token_slot)
