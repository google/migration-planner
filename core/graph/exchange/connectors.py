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

"""Exchange Online Inbound and Outbound Connectors telemetry scanner data pipeline."""

import logging
from core.graph.client import GraphClient
from core.graph.directory import DirectoryService
from core.powershell.client import PowerShellClient
from core.powershell.exchange_connectors import ExchangeConnectorsService

logger = logging.getLogger(__name__)

def fetch_exchange_connectors_data(client_id: str, client_secret: str, tenant_id: str) -> dict:
    """Fetch Exchange Online Inbound and Outbound Connectors."""
    logger.info("Starting Exchange Connectors fetch...")
    
    tenant_domain = tenant_id
    try:
        client = GraphClient(
            tenant_id=tenant_id,
            client_ids=client_id,
            client_secrets=client_secret,
            concurrency=1,
            retries=3,
            backoff=2
        )
        client.authenticate()
        dir_svc = DirectoryService(client)
        tenant_domain = dir_svc.get_tenant_primary_domain()
        logger.info(f"Retrieved primary tenant domain for Connectors: {tenant_domain}")
    except Exception as e:
        logger.warning(f"Could not retrieve tenant domain. Falling back to Tenant ID Guid: {e}")
    finally:
        try:
            client.close()
        except Exception:
            pass
            
    try:
        ps_client = PowerShellClient(tenant_id=tenant_domain, client_id=client_id, client_secret=client_secret, cert_tenant_id=tenant_id)
        connector_svc = ExchangeConnectorsService(ps_client)
        data = connector_svc.fetch_exchange_connectors()
        return {"connectors": data, "error": None}
    except Exception as e:
        logger.error("Failed to fetch Exchange Connectors via PowerShell", exc_info=True)
        return {"connectors": None, "error": str(e)}
