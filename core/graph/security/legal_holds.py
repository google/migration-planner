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

"""Mailboxes on Legal Hold PowerShell scanner data pipeline."""

import logging
from core.graph.client import GraphClient
from core.graph.directory.organization import OrganizationService
from core.powershell.client import PowerShellClient
from core.powershell.mailbox import MailboxStatsService

logger = logging.getLogger(__name__)


def run_legal_holds_pipeline(
    client_id: str,
    client_secret: str,
    tenant_id: str,
) -> dict:
    """Fetch mailboxes on legal hold via Exchange Online PowerShell."""
    tenant_domain = tenant_id
    client = GraphClient(
        tenant_id=tenant_id,
        client_ids=client_id,
        client_secrets=client_secret,
        concurrency=1,
    )
    try:
        client.authenticate()
        org_svc = OrganizationService(client)
        tenant_domain = org_svc.get_tenant_primary_domain()
        logger.info(f"Retrieved primary tenant domain for Legal Holds: {tenant_domain}")
    except Exception as e:
        logger.warning(f"Could not retrieve tenant domain for Legal Holds. Falling back to Tenant ID: {e}")
    finally:
        client.close()

    ps_client = PowerShellClient(
        tenant_id=tenant_domain,
        client_id=client_id,
        client_secret=client_secret,
        cert_tenant_id=tenant_id,
    )
    mbx_service = MailboxStatsService(ps_client)
    return mbx_service.fetch_legal_holds()
