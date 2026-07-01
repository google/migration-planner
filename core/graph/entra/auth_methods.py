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

"""Microsoft Entra Authentication Methods telemetry scanner data pipeline."""

import logging
from core.graph.client import GraphClient
from core.graph.reports import ReportsService

logger = logging.getLogger(__name__)

def run_auth_methods_pipeline(
    client_id: str,
    client_secret: str,
    tenant_id: str,
    csv_path: str,
    period: str = "D7",
    max_rows: int = 5000,
    on_page_callback=None,
    is_cancelled_callback=None
):
    """Fetch authentication methods usage activity summary."""
    client = GraphClient(
        tenant_id=tenant_id,
        client_ids=client_id,
        client_secrets=client_secret,
        concurrency=1,
        retries=5,
        backoff=2
    )
    client.authenticate()
    reports_service = ReportsService(client)
    try:
        reports_service.fetch_auth_methods_summary(
            csv_path=csv_path,
            period=period,
            max_rows=max_rows,
            on_page_callback=on_page_callback,
            is_cancelled_callback=is_cancelled_callback
        )
    finally:
        client.close()
