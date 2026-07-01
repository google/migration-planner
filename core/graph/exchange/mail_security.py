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

"""Exchange Online Mail Security telemetry scanner data pipeline."""

import logging
import requests
from util.auth_manager import TokenManager

logger = logging.getLogger(__name__)

def run_mail_security_pipeline(client_id: str, client_secret: str, tenant_id: str) -> dict:
    """Standalone pipeline to query Graph API for mail security SKUs and user counts."""
    if not tenant_id or not client_id or not client_secret:
        raise ValueError("Missing credentials.")

    tm = TokenManager(tenant_id=tenant_id, client_ids=[client_id], client_secrets=[client_secret], concurrency=1, retries=1, backoff=0)
    tm.authenticate_all()
    
    slot = tm.get_valid_token_slot()
    if not slot:
        raise ConnectionError("Authentication failed: No valid token.")

    token = slot["token"]
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    url = "https://graph.microsoft.com/v1.0/subscribedSkus"
    data = []
    
    while url:
        res = requests.get(url, headers=headers, timeout=15)
        if res.status_code != 200:
            tm.return_token_slot(slot)
            raise ConnectionError(f"Graph API Error {res.status_code}: {res.text}")
            
        json_data = res.json()
        data.extend(json_data.get("value", []))
        url = json_data.get("@odata.nextLink")
        
    tm.return_token_slot(slot)
    
    defender_skus_set = set()
    eop_skus_set = set()
    
    defender_users = 0
    eop_users = 0
    
    for sku in data:
        raw_part_num = sku.get("skuPartNumber", "Unknown")
        if isinstance(raw_part_num, list):
            part_num = ", ".join([str(x) for x in raw_part_num])
        else:
            part_num = str(raw_part_num)
            
        consumed = int(sku.get("consumedUnits", 0))
        plans = sku.get("servicePlans", [])
        
        has_defender = False
        has_eop = False
        
        for p in plans:
            if p.get("provisioningStatus") == "Success":
                name = p.get("servicePlanName", "").upper()
                if "DEFENDER_PLATFORM_FOR_OFFICE" in name or "ATP_ENTERPRISE" in name:
                    has_defender = True
                elif "EXCHANGE_S_ENTERPRISE" in name or "EXCHANGE_S_STANDARD" in name or "EXCHANGE_S_FOUNDATION" in name:
                    has_eop = True
                    
        if has_defender:
            defender_skus_set.add(part_num)
            defender_users += consumed
        elif has_eop:
            eop_skus_set.add(part_num)
            eop_users += consumed
            
    return {
        "defender": {"skus": list(defender_skus_set), "users": defender_users},
        "eop": {"skus": list(eop_skus_set), "users": eop_users}
    }
