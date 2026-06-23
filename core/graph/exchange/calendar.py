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

"""Exchange Online Calendar environment telemetry scanner data pipeline."""

import logging
from core.graph.client import GraphClient
from core.graph.directory import DirectoryService
from core.powershell.client import PowerShellClient
from core.powershell.calendar import CalendarStatsService

logger = logging.getLogger(__name__)

def run_calendar_telemetry_pipeline(client_id: str, client_secret: str, tenant_id: str) -> dict:
    """Consolidated orchestration pipeline to download and audit Exchange Calendar telemetry config."""
    logger.info("Starting PowerShell Calendar Telemetry Pipeline...")
    
    tenant_domain = tenant_id
    client = None
    try:
        client = GraphClient(
            tenant_id=tenant_id,
            client_ids=client_id,
            client_secrets=client_secret,
            concurrency=1,
            retries=5,
            backoff=2
        )
        client.authenticate()
        dir_svc = DirectoryService(client)
        tenant_domain = dir_svc.get_tenant_primary_domain()
        logger.info(f"Retrieved primary tenant domain: {tenant_domain}")
    except Exception as e:
        logger.warning(f"Could not retrieve tenant domain via Graph. Falling back to Tenant ID Guid: {e}")
    finally:
        if client:
            client.close()
            
    rooms_count = 0
    rooms_error = None
    rooms_naming = None
    equipment_count = 0
    equipment_error = None
    can_share_attachments = True
    owa_policy_error = None
    org_apps = []
    apps_error = None
    powershell_error = None
    
    try:
        logger.info("Connecting to Exchange Online PowerShell for calendar metadata...")
        ps_client = PowerShellClient(
            tenant_id=tenant_domain,
            client_id=client_id,
            client_secret=client_secret,
            cert_tenant_id=tenant_id
        )
        cal_service = CalendarStatsService(ps_client)
        metadata = cal_service.fetch_calendar_attachments_policy()
        
        rooms_count = metadata.get("RoomsCount", 0)
        rooms_error = metadata.get("RoomsError")
        rooms_naming = metadata.get("RoomsNaming")
        equipment_count = metadata.get("EquipmentCount", 0)
        equipment_error = metadata.get("EquipmentError")
        can_share_attachments = metadata.get("CanShareAttachments", True)
        owa_policy_error = metadata.get("OwaPolicyError")
        org_apps = metadata.get("OrganizationApps", [])
        apps_error = metadata.get("AppsError")
              
    except Exception as e:
        logger.warning(f"Could not connect to Exchange Online PowerShell: {e}")
        powershell_error = str(e)
        
        if "pwsh" in str(e).lower() or "powershell" in str(e).lower():
            err_msg = "pwsh not available"
        elif "module" in str(e).lower():
            err_msg = "ExchangeOnlineManagement module not installed"
        else:
            err_msg = "Not Permitted (Exchange Permission Issue)"
            
        rooms_error = err_msg
        equipment_error = err_msg
        owa_policy_error = err_msg
        apps_error = err_msg

    if rooms_error:
        logger.error(f"Exchange PowerShell error querying Room Mailboxes: {rooms_error}")
    if equipment_error:
        logger.error(f"Exchange PowerShell error querying Equipment Mailboxes: {equipment_error}")
    if owa_policy_error:
        logger.error(f"Exchange PowerShell error querying OWA Mailbox Policy: {owa_policy_error}")
    if apps_error:
        logger.error(f"Exchange PowerShell error querying Organization Apps: {apps_error}")

    total_resources = rooms_count + equipment_count
    
    return {
        "CanUsersReserveRooms": rooms_error if rooms_error else (total_resources > 0),
        "TotalCalendarResources": total_resources,
        "RoomsCount": rooms_count,
        "EquipmentCount": equipment_count,
        "RoomsError": rooms_error,
        "DevicesError": equipment_error,
        "OrganizationApps": org_apps,
        "AppsError": apps_error,
        "NamingConvention": rooms_error if rooms_error else (rooms_naming if rooms_naming else "None found"),
        "CanShareAttachments": owa_policy_error if owa_policy_error else can_share_attachments,
        "powershell_error": powershell_error
    }
