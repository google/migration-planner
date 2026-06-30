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

"""Exchange Online PST files telemetry scanner data pipeline."""

import logging
from core.graph.client import GraphClient
from core.graph.reports import ReportsService

logger = logging.getLogger(__name__)

def run_pst_discovery_pipeline(client_id: str, client_secret: str, tenant_id: str) -> dict:
    """Executes search discovery of cloud stored PST files."""
    client = GraphClient(tenant_id=tenant_id, client_ids=client_id, client_secrets=client_secret, concurrency=1, retries=3, backoff=2)
    client.authenticate()
    service = ReportsService(client)
    
    pst_cloud = {}
    pst_error = None
    try:
        pst_cloud = service.search_cloud_pst_files()
    except Exception as e:
        pst_error = str(e)
    client.close()
    return {"pst_cloud_data": pst_cloud, "pst_error": pst_error}
